"""
transport.py — Pepper Python worker bus transport.

One file, three backends:
  mula  — raw TCP framing (Pepper's custom protocol, no library available)
  redis — redis-py (RESP handled by the library)
  nats  — nats-py (core NATS + JetStream handled by the library)

The Go side defines the Store interface; this is the Python equivalent.
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue
import socket
import sys
import threading
import time
from typing import Callable

log = logging.getLogger("pepper.transport")

SEND_TIMEOUT_S: float = 5.0

class BusTransport:
    """Pluggable bus transport. Scheme in PEPPER_BUS_URL selects backend."""

    SUPPORTED = ("mula", "tcp", "redis", "nats")

    def __init__(self, url: str) -> None:
        self.url = url.strip()
        self.scheme, self.host, self.port = self._parse(self.url)
        self._impl: _TransportImpl | None = None

    @staticmethod
    def _parse(url: str) -> tuple[str, str, int]:
        if "://" not in url:
            host, _, port = url.rpartition(":")
            return "mula", host or "127.0.0.1", int(port) if port else 7731
        scheme, rest = url.split("://", 1)
        scheme = scheme.lower()
        if ":" in rest:
            host, _, port_str = rest.rpartition(":")
            port = int(port_str)
        else:
            host, port = rest, _default_port(scheme)
        return scheme, host or "127.0.0.1", port

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

class _TransportImpl:
    def connect(self, retries: int) -> None: ...
    def send_frame(self, data: bytes) -> None: ...
    def recv_frame(self) -> bytes | None: ...
    def subscribe(self, topics: list[str]) -> None: ...
    def close(self) -> None: ...

# Mula (raw TCP — no library exists)

class _MulaTransport(_TransportImpl):
    """Pepper's custom TCP transport. Frame: 4-byte BE length + payload."""

    def __init__(self, host: str, port: int) -> None:
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
                time.sleep(min(0.1 * (2 ** attempt), 4.0))
        log.error("mula: could not connect to %s:%s after %d attempts", self._host, self._port, retries)
        sys.exit(1)

    def subscribe(self, topics: list[str]) -> None:
        self.send_frame("|".join(topics).encode("utf-8"))

    def send_frame(self, data: bytes) -> None:
        assert self._conn is not None
        with self._send_lock:
            self._conn.sendall(len(data).to_bytes(4, "big") + data)

    def recv_frame(self) -> bytes | None:
        hdr = _recv_exact(self._conn, 4)
        if hdr is None:
            raise ConnectionResetError("mula: router closed connection")
        size = int.from_bytes(hdr, "big")
        if size == 0:
            return None
        if size > 64 * 1024 * 1024:
            raise ValueError(f"mula: frame too large: {size} bytes")
        return _recv_exact(self._conn, size)

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except OSError:
                pass
            self._conn = None

# Redis (redis-py)

class _RedisTransport(_TransportImpl):
    """Redis transport backed by redis-py. Thread-safe, connection-pooled."""

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        # _client: used for PUBLISH and pubsub (socket_timeout ok for non-blocking ops)
        self._client: "redis.Redis" | None = None
        # _brpop_client: dedicated no-socket-timeout connection for BRPOP.
        # BRPOP blocks server-side for up to `timeout` seconds; if socket_timeout
        # is set shorter than that the Python socket fires first, the reply is
        # discarded, and recv always times out.
        self._brpop_client: "redis.Redis" | None = None
        self._pubsub: "redis.client.PubSub" | None = None
        self._pubsub_thread: threading.Thread | None = None
        self._inbox: queue.Queue[bytes] = queue.Queue(maxsize=512)
        self._queues: list[str] = []
        self._sub_topics: list[str] = []
        self._stop = threading.Event()

    def connect(self, retries: int = 10) -> None:
        import redis

        for attempt in range(retries):
            try:
                self._client = redis.Redis(
                    host=self._host, port=self._port,
                    socket_connect_timeout=5, socket_timeout=5,
                )
                self._client.ping()
                # Dedicated client for BRPOP — socket_timeout=None so the
                # blocking read is never cut short by a Python-level timeout.
                self._brpop_client = redis.Redis(
                    host=self._host, port=self._port,
                    socket_connect_timeout=5, socket_timeout=None,
                )
                return
            except Exception as exc:
                log.debug("redis connect attempt %d: %s", attempt + 1, exc)
                time.sleep(min(0.1 * (2 ** attempt), 4.0))
        log.error("redis: could not connect to %s:%s", self._host, self._port)
        sys.exit(1)

    def subscribe(self, topics: list[str]) -> None:
        import redis

        self._queues = [t for t in topics if t.startswith("pepper.push.")]
        self._sub_topics = [t for t in topics if not t.startswith("pepper.push.")]

        if self._sub_topics and self._client:
            self._pubsub = self._client.pubsub()
            self._pubsub.subscribe(**{t: self._on_pubsub for t in self._sub_topics})
            self._pubsub_thread = self._pubsub.run_in_thread(sleep_time=0.001)

        if self._queues:
            ready = threading.Event()
            threading.Thread(target=self._brpop_loop, args=(ready,), daemon=True, name="redis-brpop").start()
            if not ready.wait(timeout=10.0):
                log.warning("redis: BRPOP did not become ready in 10 s")

    def _on_pubsub(self, message: dict) -> None:
        if message.get("type") == "message":
            self._enqueue(message["data"])

    def _brpop_loop(self, ready: threading.Event) -> None:
        ready.set()
        while not self._stop.is_set():
            try:
                assert self._brpop_client
                # Use the dedicated no-socket-timeout client so the 2s server-side
                # block is never cut short by a Python socket timeout.
                result = self._brpop_client.brpop(self._queues, timeout=2)
                if result is not None and len(result) == 2:
                    self._enqueue(result[1])
            except Exception as exc:
                if not self._stop.is_set():
                    log.warning("redis: brpop error: %s — retrying", exc)
                    time.sleep(0.5)

    def send_frame(self, data: bytes) -> None:
        topic, payload = _unwrap_frame(data)
        if not topic or self._client is None:
            return
        try:
            self._client.publish(topic, payload)
        except Exception as exc:
            log.warning("redis: publish failed: %s", exc)

    def recv_frame(self) -> bytes | None:
        try:
            return self._inbox.get(timeout=0.05)
        except queue.Empty:
            return None

    def close(self) -> None:
        self._stop.set()
        if self._pubsub_thread:
            self._pubsub_thread.stop()  # type: ignore[attr-defined]
        if self._pubsub:
            self._pubsub.close()
        if self._brpop_client:
            self._brpop_client.close()
        if self._client:
            self._client.close()

    def _enqueue(self, data: bytes) -> None:
        while not self._stop.is_set():
            try:
                self._inbox.put(_wrap_payload(data), timeout=1)
                return
            except queue.Full:
                continue

# NATS (nats-py)

class _NATSTransport(_TransportImpl):
    """NATS transport backed by nats-py. Asyncio bridge for JetStream pull."""

    def __init__(self, host: str, port: int) -> None:
        self._url = f"nats://{host}:{port}"
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="nats-asyncio")
        self._nc: "nats.NATS" | None = None
        self._js: "nats.js.JetStreamContext" | None = None
        self._inbox: queue.Queue[bytes] = queue.Queue(maxsize=512)
        self._stop = threading.Event()
        self._push_topics: list[str] = []
        self._other_topics: list[str] = []
        self._subs: list = []

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def connect(self, retries: int = 10) -> None:
        import nats

        self._thread.start()
        for attempt in range(retries):
            try:
                self._nc = self._run(nats.connect(self._url))
                self._js = self._nc.jetstream()
                return
            except Exception as exc:
                log.debug("nats connect attempt %d: %s", attempt + 1, exc)
                time.sleep(min(0.1 * (2 ** attempt), 4.0))
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=2.0)
        log.error("nats: could not connect to %s", self._url)
        sys.exit(1)

    def subscribe(self, topics: list[str]) -> None:
        import nats.js.api as jsapi

        push = [t for t in topics if t.startswith("pepper.push.")]
        other = [t for t in topics if not t.startswith("pepper.push.")]
        self._push_topics = push
        self._other_topics = other

        # Ensure work-queue stream exists.
        async def _ensure_stream() -> None:
            assert self._js
            try:
                await self._js.add_stream(jsapi.StreamConfig(
                    name="pepper-work",
                    subjects=["pepper.push.>"],
                    retention="workqueue",
                    storage="file",
                ))
            except Exception:
                pass  # already exists

        self._run(_ensure_stream())

        for subject in push:
            ready = threading.Event()
            asyncio.run_coroutine_threadsafe(self._jspull_coro(subject, ready), self._loop)
            if not ready.wait(timeout=15.0):
                log.warning("nats: jspull not ready in 15 s [subject=%s]", subject)

        for subject in other:
            sub = self._run(self._nc.subscribe(subject, cb=self._on_core_message_async))
            self._subs.append(sub)

    async def _on_core_message_async(self, msg) -> None:
        """Async callback required by nats-py for subscriptions."""
        self._enqueue(msg.data)

    async def _jspull_coro(self, subject: str, ready: threading.Event) -> None:
        import nats.js.api as jsapi

        assert self._js
        cname = _js_consumer_name(subject)

        try:
            await self._js.add_consumer("pepper-work", jsapi.ConsumerConfig(
                durable_name=cname,
                filter_subject=subject,
                ack_policy="explicit",
                deliver_policy="all",
                max_ack_pending=1,
                ack_wait=30.0,  # seconds — nats-py multiplies by 1e9 before sending
                max_deliver=5,
            ))
        except Exception:
            pass  # already exists — consumer is shared between workers

        # pull_subscribe checks consumer_info first; if the durable already
        # exists (created by add_consumer above, or by a peer worker) it skips
        # creation and calls pull_subscribe_bind directly — no hang.
        sub = await self._js.pull_subscribe(subject, durable=cname,
                                            stream="pepper-work")
        ready.set()  # signal only after the pull subscriber is bound

        log.info("nats: jspull ready [subject=%s consumer=%s]", subject, cname)

        while not self._stop.is_set():
            try:
                msgs = await sub.fetch(batch=1, timeout=2.5)
                for msg in msgs:
                    self._enqueue(msg.data)
                    await msg.ack()
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                if not self._stop.is_set():
                    log.warning("nats: jspull error: %s [subject=%s] — retrying", exc, subject)
                    await asyncio.sleep(0.5)

    def send_frame(self, data: bytes) -> None:
        topic, payload = _unwrap_frame(data)
        if not topic or self._nc is None:
            return
        try:
            self._run(self._nc.publish(topic, payload))
        except Exception as exc:
            log.warning("nats: publish failed: %s", exc)

    def recv_frame(self) -> bytes | None:
        try:
            return self._inbox.get(timeout=0.05)
        except queue.Empty:
            return None

    def close(self) -> None:
        self._stop.set()
        for sub in self._subs:
            try:
                self._run(sub.unsubscribe())
            except Exception:
                pass
        if self._nc:
            try:
                self._run(self._nc.close())
            except Exception:
                pass
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=2.0)

    def _enqueue(self, data: bytes) -> None:
        while not self._stop.is_set():
            try:
                self._inbox.put(_wrap_payload(data), timeout=1)
                return
            except queue.Full:
                continue

# Shared helpers

def _recv_exact(conn: socket.socket | None, n: int) -> bytes | None:
    if conn is None:
        return None
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)

def _wrap_payload(payload: bytes) -> bytes:
    """Prepend zero-length topic header for Worker._message_loop compatibility."""
    return b"\x00\x00" + payload

def _unwrap_frame(data: bytes) -> tuple[str, bytes]:
    """Extract (topic, payload) from a Worker._send_envelope frame."""
    if len(data) < 2:
        return "", data
    topic_len = int.from_bytes(data[:2], "big")
    if topic_len == 0:
        return "", data[2:]
    if len(data) < 2 + topic_len:
        return "", data
    return (
        data[2 : 2 + topic_len].decode("utf-8", errors="replace"),
        data[2 + topic_len :],
    )

def _js_consumer_name(subject: str) -> str:
    """Mirror Go's consumerName()."""
    return "c-" + subject.replace(".", "_")

# Minimal RESP helpers (used by test_runtime integration tests)

def _resp_cmd(*args: str | bytes) -> bytes:
    """Encode a RESP array command, e.g. _resp_cmd('PSUBSCRIBE', 'pepper.res.*')."""
    parts = [f"*{len(args)}\r\n".encode()]
    for arg in args:
        if isinstance(arg, str):
            arg = arg.encode("utf-8")
        parts.append(f"${len(arg)}\r\n".encode())
        parts.append(arg)
        parts.append(b"\r\n")
    return b"".join(parts)

class _BufReader:
    """Buffered line reader over a raw socket for RESP parsing."""

    def __init__(self, conn: socket.socket) -> None:
        self._conn = conn
        self._buf = bytearray()

    def readline(self) -> bytes:
        while b"\r\n" not in self._buf:
            chunk = self._conn.recv(4096)
            if not chunk:
                raise ConnectionResetError("RESP: connection closed")
            self._buf.extend(chunk)
        idx = self._buf.index(b"\r\n")
        line = bytes(self._buf[: idx + 2])
        del self._buf[: idx + 2]
        return line

    def read_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self._conn.recv(4096)
            if not chunk:
                raise ConnectionResetError("RESP: connection closed")
            self._buf.extend(chunk)
        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

def _resp_read(reader: _BufReader):
    """Read one RESP value from *reader*. Returns str/int/bytes/list/None."""
    line = reader.readline().rstrip(b"\r\n")
    prefix = chr(line[0])
    rest = line[1:]
    if prefix == "+":
        return rest.decode("utf-8", errors="replace")
    if prefix == "-":
        raise RuntimeError(f"RESP error: {rest.decode()}")
    if prefix == ":":
        return int(rest)
    if prefix == "$":
        length = int(rest)
        if length == -1:
            return None
        data = reader.read_exact(length + 2)  # +2 for CRLF
        return data[:length]
    if prefix == "*":
        count = int(rest)
        if count == -1:
            return None
        return [_resp_read(reader) for _ in range(count)]
    raise ValueError(f"RESP: unknown prefix {prefix!r} in {line!r}")
