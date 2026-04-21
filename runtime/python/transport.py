# The scheme in PEPPER_BUS_URL selects the transport:
#   mula://host:port   — Pepper's pure-Go TCP bus (default for single-node)
#   tcp://host:port    — alias for mula://
#   redis://host:port  — Redis pub/sub + BRPOP queue (distributed mode)
#   nats://host:port   — NATS core pub/sub + queue groups (distributed mode)
#
# Go passes PEPPER_BUS_URL with the full scheme so the runtime selects
# the right transport without guessing. Mula is used when no WithCoord is
# set. Redis or NATS is used when WithCoord(coord.NewRedis/NewNATS) is set.
from __future__ import annotations

import socket
import threading
import time
import os
import sys
import struct
import logging
import queue as _queue

log = logging.getLogger("pepper.transport")


class BusTransport:
    """Pluggable bus transport. Scheme in PEPPER_BUS_URL selects implementation."""

    SUPPORTED = ("mula", "tcp", "redis", "nats")

    def __init__(self, url: str):
        self.url = url.strip()
        self.scheme, self.host, self.port = self._parse(self.url)
        self._impl: "_TransportImpl | None" = None

    @staticmethod
    def _parse(url: str) -> tuple[str, str, int]:
        if "://" not in url:
            host, _, port = url.rpartition(":")
            return "mula", host or "127.0.0.1", int(port) if port else 7731
        scheme, rest = url.split("://", 1)
        scheme = scheme.lower()
        host, _, port_str = rest.rpartition(":")
        return scheme, host or "127.0.0.1", int(port_str) if port_str else _default_port(scheme)

    def connect(self, retries: int = 10) -> None:
        if self.scheme in ("mula", "tcp"):
            self._impl = _MulaTransport(self.host, self.port)
        elif self.scheme == "redis":
            self._impl = _RedisTransport(self.host, self.port)
        elif self.scheme == "nats":
            self._impl = _NATSTransport(self.host, self.port)
        else:
            raise NotImplementedError(f"unsupported bus scheme: {self.scheme!r}")
        self._impl.connect(retries)
        log.info("bus connected [scheme=%s addr=%s:%s]", self.scheme, self.host, self.port)

    def send_frame(self, data: bytes) -> None:
        assert self._impl is not None, "not connected"
        self._impl.send_frame(data)

    def recv_frame(self) -> bytes | None:
        assert self._impl is not None, "not connected"
        return self._impl.recv_frame()

    def subscribe(self, topics: list[str]) -> None:
        """Register interest in topics. Mula uses frame protocol; coord uses native sub."""
        if self._impl is not None:
            self._impl.subscribe(topics)

    @property
    def raw_conn(self) -> socket.socket | None:
        if isinstance(self._impl, _MulaTransport):
            return self._impl._conn
        return None

    def close(self) -> None:
        if self._impl:
            self._impl.close()
            self._impl = None


def _default_port(scheme: str) -> int:
    return {"redis": 6379, "nats": 4222, "mula": 7731, "tcp": 7731}.get(scheme, 7731)


# ── Internal transport implementations ────────────────────────────────────────

class _TransportImpl:
    def connect(self, retries: int) -> None: ...
    def send_frame(self, data: bytes) -> None: ...
    def recv_frame(self) -> bytes | None: ...
    def subscribe(self, topics: list[str]) -> None: ...
    def close(self) -> None: ...


class _MulaTransport(_TransportImpl):
    """Pepper's custom TCP framing: 4-byte big-endian length prefix."""

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._conn: socket.socket | None = None
        self._send_lock = threading.Lock()

    def connect(self, retries: int = 10) -> None:
        for attempt in range(retries):
            try:
                conn = socket.create_connection((self._host, self._port), timeout=5)
                conn.settimeout(None)
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self._conn = conn
                return
            except OSError as exc:
                log.debug("mula connect attempt %d failed: %s", attempt + 1, exc)
                time.sleep(0.1 * (2 ** attempt))
        log.error("mula: could not connect to %s:%s after %d attempts", self._host, self._port, retries)
        sys.exit(1)

    def subscribe(self, topics: list[str]) -> None:
        # Mula subscription: send pipe-separated topics as first frame.
        frame = "|".join(topics).encode("utf-8")
        self.send_frame(frame)

    def send_frame(self, data: bytes) -> None:
        assert self._conn is not None
        with self._send_lock:
            self._conn.sendall(len(data).to_bytes(4, "big") + data)

    def recv_frame(self) -> bytes | None:
        hdr = _recv_exact(self._conn, 4)
        if hdr is None:
            raise ConnectionResetError("router closed connection")
        size = int.from_bytes(hdr, "big")
        if size == 0:
            return None  # keepalive ping
        if size > 64 * 1024 * 1024:
            raise ValueError(f"frame too large: {size} bytes")
        return _recv_exact(self._conn, size)

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except OSError:
                pass
            self._conn = None


class _RedisTransport(_TransportImpl):
    """
    Redis transport for distributed mode.

    Send path:  PUBLISH to the response/control channel.
    Receive path: BRPOP from the worker's push queue (pepper.push.{group})
                  and PSUBSCRIBE for broadcast/control channels.

    Uses two connections:
      - _pub_conn: PUBLISH commands (protected by send_lock)
      - _sub_conn: PSUBSCRIBE for control/broadcast topics
    Queue polling runs in a background thread and pushes into _inbox.
    """

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._pub_conn: socket.socket | None = None
        self._sub_conn: socket.socket | None = None
        self._send_lock = threading.Lock()
        self._inbox: _queue.Queue[bytes] = _queue.Queue(maxsize=512)
        self._queues: list[str] = []
        self._sub_patterns: list[str] = []
        self._stop = threading.Event()

    def _dial(self) -> socket.socket:
        conn = socket.create_connection((self._host, self._port), timeout=5)
        conn.settimeout(None)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return conn

    def connect(self, retries: int = 10) -> None:
        for attempt in range(retries):
            try:
                self._pub_conn = self._dial()
                self._sub_conn = self._dial()
                return
            except OSError as exc:
                log.debug("redis connect attempt %d: %s", attempt + 1, exc)
                time.sleep(0.1 * (2 ** attempt))
        log.error("redis: could not connect to %s:%s", self._host, self._port)
        sys.exit(1)

    def subscribe(self, topics: list[str]) -> None:
        # Split topics: queues (pepper.push.*) go to BRPOP loop,
        # pub/sub topics (pepper.pub.*, pepper.control*, pepper.broadcast) go to PSUBSCRIBE.
        self._queues = [t for t in topics if t.startswith("pepper.push.")]
        sub_topics = [t for t in topics if not t.startswith("pepper.push.")]

        if sub_topics:
            # Store patterns so _pubsub_loop can re-subscribe after reconnect.
            self._sub_patterns = [t if t.endswith("*") else t + "*" for t in sub_topics]
            # Issue the initial PSUBSCRIBE on the already-connected _sub_conn.
            cmd = _resp_encode("PSUBSCRIBE", *self._sub_patterns)
            self._sub_conn.sendall(cmd)
            t = threading.Thread(target=self._pubsub_loop, daemon=True)
            t.start()

        if self._queues:
            t = threading.Thread(target=self._brpop_loop, daemon=True)
            t.start()

    def _brpop_loop(self) -> None:
        """Background thread: BRPOP from worker push queues."""
        while not self._stop.is_set():
            try:
                conn = self._dial()
                while not self._stop.is_set():
                    cmd = _resp_encode("BRPOP", *self._queues, "2")
                    conn.sendall(cmd)
                    reply = _resp_read(conn)
                    if reply is None or not isinstance(reply, list) or len(reply) < 2:
                        continue
                    data = reply[1]
                    if isinstance(data, str):
                        data = bytes.fromhex(data)
                    # Wrap as a Mula-style frame: 2-byte topic-len=0 + payload
                    # so the existing Worker._message_loop can decode it unchanged.
                    self._inbox.put(_wrap_payload(data), timeout=1)
                conn.close()
            except Exception as exc:
                if not self._stop.is_set():
                    log.warning("redis brpop error: %s — retrying", exc)
                    time.sleep(0.5)

    def _pubsub_loop(self) -> None:
        """Background thread: read PSUBSCRIBE messages.

        Reconnects and re-issues PSUBSCRIBE on every outer-loop entry so that
        a broken socket (e.g. from an i/o timeout under concurrent load) never
        gets reused. This mirrors _brpop_loop's dial-per-outer-iteration pattern.
        """
        while not self._stop.is_set():
            conn: socket.socket | None = None
            try:
                # Dial a fresh connection and re-subscribe on each (re)entry.
                conn = self._dial()
                cmd = _resp_encode("PSUBSCRIBE", *self._sub_patterns)
                conn.sendall(cmd)
                # Keep the shared _sub_conn reference current so close() can
                # tear it down cleanly on shutdown.
                self._sub_conn = conn
                while not self._stop.is_set():
                    conn.settimeout(2)
                    try:
                        reply = _resp_read(conn)
                    except (TimeoutError, socket.timeout):
                        continue
                    if not isinstance(reply, list) or len(reply) < 4:
                        continue
                    kind = reply[0]
                    if kind not in ("pmessage", b"pmessage"):
                        continue
                    data = reply[3]
                    if isinstance(data, str):
                        data = bytes.fromhex(data)
                    self._inbox.put(_wrap_payload(data), timeout=1)
            except Exception as exc:
                if not self._stop.is_set():
                    log.warning("redis pubsub error: %s — retrying", exc)
                    time.sleep(0.5)
            finally:
                # Always close the per-iteration connection on exit; a new one
                # will be dialled at the top of the next outer-loop iteration.
                if conn is not None:
                    try:
                        conn.close()
                    except OSError:
                        pass

    def send_frame(self, data: bytes) -> None:
        # In distributed mode the worker sends responses back to the router
        # by publishing on the res/control channels.
        # The channel is embedded in the envelope — we PUBLISH to it.
        # For simplicity we decode the topic from the Mula-wrapped frame.
        topic, payload = _unwrap_frame(data)
        if not topic:
            return
        cmd = _resp_encode("PUBLISH", topic, payload.hex())
        with self._send_lock:
            self._pub_conn.sendall(cmd)

    def recv_frame(self) -> bytes | None:
        try:
            return self._inbox.get(timeout=2)
        except _queue.Empty:
            return None

    def close(self) -> None:
        self._stop.set()
        for c in (self._pub_conn, self._sub_conn):
            if c:
                try:
                    c.close()
                except OSError:
                    pass


class _NATSTransport(_TransportImpl):
    """
    NATS transport for distributed mode.

    Send path:  PUB to the response/control subject.
    Receive path: queue-group SUB on push subjects (pepper.push.*) for
                  exactly-once delivery, plain SUB for broadcast/control.

    Uses one persistent connection per direction (pub / sub).
    """

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._pub_conn: socket.socket | None = None
        self._sub_conn: socket.socket | None = None
        self._send_lock = threading.Lock()
        self._inbox: _queue.Queue[bytes] = _queue.Queue(maxsize=512)
        self._stop = threading.Event()
        self._sid = 0

    def _dial(self) -> socket.socket:
        conn = socket.create_connection((self._host, self._port), timeout=5)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # Drain lines until INFO is seen.
        buf = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                raise ConnectionResetError("nats: server closed on connect")
            buf += chunk
            if b"\r\n" in buf:
                line, buf = buf.split(b"\r\n", 1)
                if line.startswith(b"INFO"):
                    break
        # Send CONNECT.
        conn.sendall(b"CONNECT {}\r\n")
        # Drain the server's immediate post-CONNECT PING and respond.
        # NATS sends PING right after CONNECT to verify liveness; an unanswered
        # PING causes the server to close the connection within ~2 seconds, which
        # means worker_hello (sent over this same connection) is silently dropped.
        conn.settimeout(0.5)
        try:
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\r\n" in buf:
                    line, buf = buf.split(b"\r\n", 1)
                    line_s = line.decode("utf-8", errors="replace").strip()
                    if line_s == "PING":
                        conn.sendall(b"PONG\r\n")
                    # Stop after the first non-empty, non-PING line (+OK, -ERR, etc.)
                    elif line_s:
                        raise StopIteration
        except (TimeoutError, socket.timeout, StopIteration, OSError):
            pass
        conn.settimeout(None)
        return conn

    def connect(self, retries: int = 10) -> None:
        for attempt in range(retries):
            try:
                self._pub_conn = self._dial()
                self._sub_conn = self._dial()
                # Keep both connections alive by responding to server PINGs.
                # _recv_loop handles _sub_conn; start a mirror thread for _pub_conn.
                t = threading.Thread(target=self._pub_pong_loop, daemon=True)
                t.start()
                return
            except OSError as exc:
                log.debug("nats connect attempt %d: %s", attempt + 1, exc)
                time.sleep(0.1 * (2 ** attempt))
        log.error("nats: could not connect to %s:%s", self._host, self._port)
        sys.exit(1)

    def _pub_pong_loop(self) -> None:
        """Background thread: respond to PING on _pub_conn to keep it alive."""
        buf = b""
        while not self._stop.is_set():
            try:
                self._pub_conn.settimeout(2)
                chunk = self._pub_conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\r\n" in buf:
                    line, buf = buf.split(b"\r\n", 1)
                    if line.strip() == b"PING":
                        try:
                            with self._send_lock:
                                self._pub_conn.sendall(b"PONG\r\n")
                        except OSError:
                            return
            except (TimeoutError, socket.timeout):
                continue
            except OSError:
                break

    def subscribe(self, topics: list[str]) -> None:
        push_topics = [t for t in topics if t.startswith("pepper.push.")]
        other_topics = [t for t in topics if not t.startswith("pepper.push.")]

        for topic in push_topics:
            self._sid += 1
            # Queue-group SUB: exactly one subscriber across all nodes receives each msg.
            sub_cmd = f"SUB {topic} pepper-workers {self._sid}\r\n"
            self._sub_conn.sendall(sub_cmd.encode())

        for topic in other_topics:
            self._sid += 1
            subj = topic if topic.endswith(">") else topic.rstrip("*") + ">"
            sub_cmd = f"SUB {subj} {self._sid}\r\n"
            self._sub_conn.sendall(sub_cmd.encode())

        t = threading.Thread(target=self._recv_loop, daemon=True)
        t.start()

    def _recv_loop(self) -> None:
        buf = b""
        while not self._stop.is_set():
            try:
                self._sub_conn.settimeout(2)
                chunk = self._sub_conn.recv(65536)
                if not chunk:
                    break
                buf += chunk
                while b"\r\n" in buf:
                    line, buf = buf.split(b"\r\n", 1)
                    line = line.decode("utf-8", errors="replace")
                    # NATS sends PING frames to check client liveness; we must
                    # respond with PONG or the server will close the connection.
                    # Without this, worker_hello is never delivered and startup
                    # times out with "timed out waiting for a worker to become ready".
                    if line == "PING":
                        try:
                            self._sub_conn.sendall(b"PONG\r\n")
                        except OSError:
                            pass
                        continue
                    if not line.startswith("MSG"):
                        continue
                    parts = line.split()
                    if len(parts) < 4:
                        continue
                    n = int(parts[3])
                    while len(buf) < n + 2:
                        more = self._sub_conn.recv(65536)
                        if not more:
                            return
                        buf += more
                    payload_hex = buf[:n].decode("utf-8", errors="replace")
                    buf = buf[n + 2:]
                    try:
                        data = bytes.fromhex(payload_hex.strip())
                    except ValueError:
                        continue
                    self._inbox.put(_wrap_payload(data), timeout=1)
            except (TimeoutError, socket.timeout):
                continue
            except Exception as exc:
                if not self._stop.is_set():
                    log.warning("nats recv error: %s", exc)
                break

    def send_frame(self, data: bytes) -> None:
        topic, payload = _unwrap_frame(data)
        if not topic:
            return
        encoded = payload.hex()
        cmd = f"PUB {topic} {len(encoded)}\r\n{encoded}\r\n"
        with self._send_lock:
            self._pub_conn.sendall(cmd.encode())

    def recv_frame(self) -> bytes | None:
        try:
            return self._inbox.get(timeout=2)
        except _queue.Empty:
            return None

    def close(self) -> None:
        self._stop.set()
        for c in (self._pub_conn, self._sub_conn):
            if c:
                try:
                    c.close()
                except OSError:
                    pass


# ── Wire helpers ──────────────────────────────────────────────────────────────

def _recv_exact(conn: socket.socket | None, n: int) -> bytes | None:
    if conn is None:
        return None
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def _resp_encode(*args: str) -> bytes:
    """Encode a RESP command."""
    out = f"*{len(args)}\r\n"
    for a in args:
        out += f"${len(a)}\r\n{a}\r\n"
    return out.encode()


def _resp_read(conn: socket.socket) -> object:
    """Read one RESP value from conn (synchronous, blocking)."""
    buf = b""
    while b"\r\n" not in buf:
        chunk = conn.recv(4096)
        if not chunk:
            raise ConnectionResetError("connection closed")
        buf += chunk
    line, rest = buf.split(b"\r\n", 1)
    line = line.decode()
    if line[0] == "+":
        return line[1:]
    if line[0] == "-":
        raise RuntimeError(f"redis error: {line[1:]}")
    if line[0] == ":":
        return int(line[1:])
    if line[0] == "$":
        n = int(line[1:])
        if n < 0:
            return None
        while len(rest) < n + 2:
            rest += conn.recv(4096)
        return rest[:n].decode()
    if line[0] == "*":
        n = int(line[1:])
        if n < 0:
            return None
        items = []
        # Re-feed rest as a fake connection is not possible; use recursive read.
        # For arrays we need to read sequentially — put rest back by creating
        # a small helper that drains rest first then reads from conn.
        remaining = rest
        for _ in range(n):
            item, remaining = _resp_read_from_buf(remaining, conn)
            items.append(item)
        return items
    return None


def _resp_read_from_buf(buf: bytes, conn: socket.socket) -> tuple[object, bytes]:
    while b"\r\n" not in buf:
        buf += conn.recv(4096)
    line, rest = buf.split(b"\r\n", 1)
    line = line.decode()
    if line[0] == "$":
        n = int(line[1:])
        if n < 0:
            return None, rest
        while len(rest) < n + 2:
            rest += conn.recv(4096)
        return rest[:n].decode(), rest[n + 2:]
    if line[0] == "*":
        n = int(line[1:])
        items = []
        for _ in range(n):
            item, rest = _resp_read_from_buf(rest, conn)
            items.append(item)
        return items, rest
    if line[0] == ":":
        return int(line[1:]), rest
    if line[0] == "+":
        return line[1:], rest
    return None, rest


def _wrap_payload(payload: bytes) -> bytes:
    """
    Wrap a raw payload as a Mula-style received frame so Worker._message_loop
    can decode it without modification.

    Mula frame layout (as sent by router to worker):
        2 bytes: topic length (big-endian)
        N bytes: topic
        M bytes: msgpack/json payload

    For coord transport the topic is not embedded in the frame — the router
    publishes to the topic implicitly. We set topic_len=0 so the worker
    skips topic parsing and reads the payload directly.
    """
    return b"\x00\x00" + payload


def _unwrap_frame(data: bytes) -> tuple[str, bytes]:
    """
    Extract topic and payload from a frame about to be sent by the worker.

    Workers call _send_envelope which calls _send_frame with a frame
    containing the response envelope. In Mula mode this is sent raw over
    TCP. In coord mode we need to PUBLISH to the correct topic.

    The response envelope contains reply_to / origin_id from which the
    topic can be derived. We decode the first few bytes to extract it.
    For simplicity we look for the reply_to field in the msgpack envelope.

    Returns ("", payload) if the topic cannot be determined — caller skips.
    """
    if len(data) < 2:
        return "", data
    topic_len = int.from_bytes(data[:2], "big")
    if topic_len == 0:
        # No topic embedded — payload only (received frame being echoed back).
        # This shouldn't happen on send path but guard anyway.
        return "", data[2:]
    if len(data) < 2 + topic_len:
        return "", data
    topic = data[2:2 + topic_len].decode("utf-8", errors="replace")
    payload = data[2 + topic_len:]
    return topic, payload