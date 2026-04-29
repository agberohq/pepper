"""
Pepper bus transport layer.

Three backends:
  mula  — raw TCP framing (Pepper's custom protocol, no library available)
  redis — redis-py (RESP handled by the library)
  nats  — nats-py (JetStream push consumers for pepper.push.* work queues;
          core NATS subscribe for control/broadcast channels)

The NATS backend uses JetStream queue-group push consumers for pepper.push.*
because coord/nats.go Push() uses js.Publish(), which lands messages in the
"pepper-work" JetStream stream. A plain nc.subscribe() would never see those
messages — JetStream intercepts them. Queue-group push consumers load-balance
work across all worker instances while retaining at-least-once delivery
semantics (each message is ACKed after the capability executes).
"""

SEND_TIMEOUT_S: float = 5.0  # socket send timeout used by Mula and Redis transports

import asyncio
import os
import queue
import socket
import sys
import threading
import time

log = __import__("logging").getLogger("pepper.transport")

class BusTransport:
    """Unified transport façade. Selects backend from PEPPER_BUS_URL."""

    SUPPORTED = ("mula", "tcp", "redis", "nats")

    def __init__(self, url: str) -> None:
        self.url = url
        self.scheme, self.host, self.port = self._parse(self.url)
        self._impl: "_TransportImpl" = self._make()

    def _parse(self, url: str):
        if "://" not in url:
            host, _, port = url.partition(":")
            return "mula", host or "127.0.0.1", int(port) if port else 7731
        scheme, rest = url.split("://", 1)
        scheme = scheme.lower()
        host, _, port_str = rest.partition(":")
        try:
            port = int(port_str) if port_str else _default_port(scheme)
        except ValueError:
            port = _default_port(scheme)
        return scheme, host or "127.0.0.1", port

    def _make(self) -> "_TransportImpl":
        if self.scheme in ("mula", "tcp"):
            return _MulaTransport(self.host, self.port)
        elif self.scheme == "redis":
            return _RedisTransport(self.host, self.port)
        elif self.scheme == "nats":
            return _NATSTransport(self.host, self.port)
        else:
            return _UnknownTransport(self.scheme)

    def connect(self, retries: int = 10) -> None:
        self._impl.connect(retries=retries)
        log.info("bus connected [scheme=%s addr=%s:%s]", self.scheme, self.host, self.port)

    @property
    def raw_conn(self):
        return getattr(self._impl, "_conn", None)

    def subscribe(self, topics: list[str]) -> None:
        self._impl.subscribe(topics)

    def send_frame(self, data: bytes) -> None:
        self._impl.send_frame(data)

    def recv_frame(self) -> bytes | None:
        return self._impl.recv_frame()

    def close(self) -> None:
        self._impl.close()

def _default_port(scheme: str) -> int:
    return {"redis": 6379, "nats": 4222, "mula": 7731, "tcp": 7731}.get(scheme, 7731)

# abstract base

class _TransportImpl:
    def connect(self, retries: int = 10) -> None: ...
    def subscribe(self, topics: list[str]) -> None: ...
    def send_frame(self, data: bytes) -> None: ...
    def recv_frame(self) -> bytes | None: ...
    def close(self) -> None: ...

# Mula (raw TCP)

class _MulaTransport(_TransportImpl):
    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._conn: socket.socket | None = None

    def connect(self, retries: int = 10) -> None:
        for attempt in range(retries):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self._host, self._port))
                self._conn = s
                return
            except OSError as exc:
                log.debug("mula connect attempt %d failed: %s", attempt + 1, exc)
                time.sleep(min(0.1 * (2 ** attempt), 4.0))
        log.error("mula: could not connect to %s:%s after %d attempts", self._host, self._port, retries)
        sys.exit(1)

    def subscribe(self, topics: list[str]) -> None:
        frame = "|".join(topics).encode("utf-8")
        self.send_frame(frame)

    def send_frame(self, data: bytes) -> None:
        if self._conn is None:
            return
        length = len(data).to_bytes(4, "big")
        try:
            self._conn.sendall(length + data)
        except OSError as exc:
            log.warning("mula: send failed: %s", exc)

    def recv_frame(self) -> bytes | None:
        header = _recv_exact(self._conn, 4)
        if header is None:
            raise ConnectionResetError("mula: router closed connection")
        n = int.from_bytes(header, "big")
        return _recv_exact(self._conn, n)

    def close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except OSError:
                pass

class _UnknownTransport(_TransportImpl):
    """Placeholder for unsupported schemes — raises NotImplementedError on connect()."""
    def __init__(self, scheme: str) -> None:
        self._scheme = scheme

    def connect(self, retries: int = 10) -> None:
        raise NotImplementedError(f"unsupported bus scheme: {self._scheme!r}")

# Redis

class _RedisTransport(_TransportImpl):
    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._client: "redis.Redis" | None = None
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
        log.debug("redis: brpop loop started [queues=%s]", self._queues)
        dequeued = 0
        timeouts = 0
        while not self._stop.is_set():
            try:
                assert self._brpop_client
                result = self._brpop_client.brpop(self._queues, timeout=2)
                if result is not None and len(result) == 2:
                    dequeued += 1
                    timeouts = 0  # reset timeout counter on success
                    queue_name = result[0].decode() if isinstance(result[0], bytes) else result[0]
                    log.info("redis: brpop dequeued item #%d [queue=%s size=%d]",
                             dequeued, queue_name, len(result[1]))
                    self._enqueue(result[1])
                else:
                    timeouts += 1
                    if timeouts <= 5:  # log first few timeouts to see if loop is alive
                        log.info("redis: brpop timeout #%d (no items, queues=%s)",
                                 timeouts, self._queues)
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
                log.debug("redis: enqueued to inbox [inbox_size=%d]", self._inbox.qsize())
                return
            except queue.Full:
                log.warning("redis: inbox full — retrying")
                continue
        log.warning("redis: _enqueue exited early due to _stop (item lost!)")

# NATS (nats-py)

class _NATSTransport(_TransportImpl):
    """NATS transport backed by nats-py.

    Work queues (pepper.push.*):
        Uses JetStream queue-group push consumers. Go's coord/nats.Push() calls
        js.Publish(), which lands the message in the "pepper-work" JetStream
        stream. A plain nc.subscribe() would never see those messages because
        JetStream intercepts publishes to matching stream subjects. Queue-group
        push consumers distribute work across worker instances with ACK-based
        at-least-once delivery (matching the workqueue retention policy on the
        stream).

    Control/broadcast channels (everything else):
        Uses plain nc.subscribe() — these are core NATS pub/sub topics that
        Go publishes via nc.Publish(), not through JetStream.
    """

    def __init__(self, host: str, port: int) -> None:
        self._url = f"nats://{host}:{port}"
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="nats-asyncio")
        self._nc: "nats.NATS" | None = None
        self._js: "nats.js.JetStreamContext" | None = None
        self._inbox: queue.Queue[bytes] = queue.Queue(maxsize=512)
        self._stop = threading.Event()
        self._subs: list = []                    # all subscriptions (core + JetStream)
        self._js_push_subs: list = []            # JetStream push sub handles for close()

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
        push_topics = [t for t in topics if t.startswith("pepper.push.")]
        core_topics = [t for t in topics if not t.startswith("pepper.push.")]

        # Core NATS subscribe for control/broadcast channels.
        # Go publishes these via nc.Publish() (not JetStream), so plain SUB works.
        for subject in core_topics:
            sub = self._run(self._nc.subscribe(subject, cb=self._on_core_msg))
            self._subs.append(sub)

        # JetStream queue-group push consumers for work queues.
        # Go publishes requests via js.Publish() → "pepper-work" stream.
        # Queue group = subject name (sanitised) so multiple worker instances
        # load-balance; each message goes to exactly one worker.
        for subject in push_topics:
            ready = threading.Event()
            asyncio.run_coroutine_threadsafe(
                self._js_push_subscribe(subject, ready), self._loop
            )
            if not ready.wait(timeout=15.0):
                log.warning("nats: js push consumer not ready in 15 s [subject=%s]", subject)

    async def _js_push_subscribe(self, subject: str, ready: threading.Event) -> None:
        """Create a durable JetStream queue-group push consumer for subject."""
        assert self._js
        # Ephemeral JetStream push consumer with queue group.
        # - No durable name: ephemeral consumers are auto-cleaned up when all
        # subscribers disconnect, avoiding leftover consumer state between test runs.
        # - queue= enables load balancing: NATS delivers each message to exactly
        # one subscriber in the group (queue-group semantics).
        # - durable + queue is rejected by NATS when the consumer was previously
        # created without a deliver group, so we keep it ephemeral.
        queue_group = _js_consumer_name(subject)

        sub = None
        try:
            for attempt in range(3):
                try:
                    sub = await asyncio.wait_for(
                        self._js.subscribe(
                            subject,
                            queue=queue_group,
                            stream="pepper-work",
                            manual_ack=True,
                            cb=self._on_js_msg,
                        ),
                        timeout=10.0,
                    )
                    log.info(
                        "nats: js push consumer ready [subject=%s queue=%s]",
                        subject, queue_group,
                    )
                    self._js_push_subs.append(sub)
                    break
                except Exception as exc:
                    log.warning(
                        "nats: js subscribe attempt %d failed [subject=%s]: %s",
                        attempt + 1, subject, exc,
                    )
                    if attempt < 2:
                        await asyncio.sleep(0.5)
            else:
                log.error("nats: js push consumer failed after 3 attempts [subject=%s]", subject)
        finally:
            ready.set()

    async def _on_js_msg(self, msg) -> None:
        """Callback for JetStream push consumer messages."""
        self._enqueue(msg.data)
        try:
            await msg.ack()
        except Exception as exc:
            log.debug("nats: ack failed: %s", exc)

    async def _on_core_msg(self, msg) -> None:
        """Callback for core NATS subscribe messages."""
        self._enqueue(msg.data)

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
        # Drain JetStream push subscriptions.
        for sub in self._js_push_subs:
            try:
                self._run(sub.drain())
            except Exception:
                pass
        # Unsubscribe core subs.
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
                log.debug("redis: enqueued to inbox [inbox_size=%d]", self._inbox.qsize())
                return
            except queue.Full:
                log.warning("redis: inbox full — retrying")
                continue
        log.warning("redis: _enqueue exited early due to _stop (item lost!)")

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

    def __init__(self, conn) -> None:
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
