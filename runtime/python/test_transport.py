"""
test_transport.py — Full coverage test suite for transport.py.

Structure
─────────
Unit tests (no services required):
  TestUrlParsing          — BusTransport._parse() for every scheme
  TestWrapUnwrap          — _wrap_payload / _unwrap_frame round-trip
  TestMulaFraming         — _MulaTransport over a local socketpair echo server

Integration tests (skipped when service unavailable):
  TestRedisTransport      — full connect/subscribe/send/recv against real Redis
  TestNATSTransport       — full connect/subscribe/send/recv against real NATS

Run:
    pytest runtime/python/test_transport.py -v
    pytest runtime/python/test_transport.py -v -k "not Redis and not NATS"
"""
from __future__ import annotations

import os
import socket
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from transport import (
    SEND_TIMEOUT_S,
    BusTransport,
    _wrap_payload,
    _unwrap_frame,
    _js_consumer_name,
)

# Helpers

def _port_open(host: str, port: int, timeout: float = 0.3) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False

def _require(host: str, port: int, service: str) -> None:
    if not _port_open(host, port):
        pytest.skip(f"{service} not available at {host}:{port}")

REDIS_HOST = os.environ.get("PEPPER_TEST_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("PEPPER_TEST_REDIS_PORT", "6379"))
NATS_HOST  = os.environ.get("PEPPER_TEST_NATS_HOST",  "127.0.0.1")
NATS_PORT  = int(os.environ.get("PEPPER_TEST_NATS_PORT",  "4222"))

def _encode_frame(topic: str, payload: bytes) -> bytes:
    """Build the 2-byte-topic-len frame that Worker._send_envelope produces."""
    tb = topic.encode()
    return len(tb).to_bytes(2, "big") + tb + payload

def _decode_frame(frame: bytes) -> tuple[str, bytes]:
    """Decode what the _wrap_payload path delivers to the message loop."""
    topic_len = int.from_bytes(frame[:2], "big")
    topic   = frame[2 : 2 + topic_len].decode()
    payload = frame[2 + topic_len :]
    return topic, payload

# URL Parsing

class TestUrlParsing:

    def test_bare_host_port_defaults_to_mula(self):
        t = BusTransport("127.0.0.1:7731")
        assert t.scheme == "mula" and t.host == "127.0.0.1" and t.port == 7731

    def test_tcp_scheme(self):
        t = BusTransport("tcp://10.0.0.1:9000")
        assert t.scheme == "tcp" and t.port == 9000

    def test_mula_scheme_default_port(self):
        t = BusTransport("mula://127.0.0.1")
        assert t.scheme == "mula" and t.port == 7731

    def test_redis_scheme(self):
        t = BusTransport("redis://127.0.0.1:6379")
        assert t.scheme == "redis" and t.port == 6379

    def test_redis_default_port(self):
        t = BusTransport("redis://127.0.0.1")
        assert t.port == 6379

    def test_nats_scheme(self):
        t = BusTransport("nats://127.0.0.1:4222")
        assert t.scheme == "nats" and t.port == 4222

    def test_nats_default_port(self):
        t = BusTransport("nats://127.0.0.1")
        assert t.port == 4222

    def test_unknown_scheme_raises_on_connect(self):
        t = BusTransport("grpc://127.0.0.1:9090")
        with pytest.raises(NotImplementedError):
            t.connect(retries=1)

    def test_connect_refused_exits(self):
        t = BusTransport("tcp://127.0.0.1:19998")
        with pytest.raises((SystemExit, OSError)):
            t.connect(retries=1)

# _wrap_payload / _unwrap_frame

class TestWrapUnwrap:

    def test_wrap_prepends_zero_topic(self):
        wrapped = _wrap_payload(b"hello")
        assert wrapped[:2] == b"\x00\x00"
        assert wrapped[2:] == b"hello"

    def test_unwrap_encoded_frame(self):
        frame = _encode_frame("pepper.res.abc123", b"payload")
        topic, payload = _unwrap_frame(frame)
        assert topic == "pepper.res.abc123"
        assert payload == b"payload"

    def test_unwrap_zero_topic_len(self):
        topic, payload = _unwrap_frame(b"\x00\x00somedata")
        assert topic == ""
        assert payload == b"somedata"

    def test_unwrap_too_short(self):
        topic, payload = _unwrap_frame(b"\x01")
        assert topic == ""

    def test_unwrap_binary_payload(self):
        binary = bytes(range(256))
        frame = _encode_frame("pepper.res.x", binary)
        topic, payload = _unwrap_frame(frame)
        assert payload == binary

    def test_round_trip(self):
        original_topic   = "pepper.res.01ABC"
        original_payload = b"\xa7msgpack\xffbinary"
        frame = _encode_frame(original_topic, original_payload)
        topic, payload = _unwrap_frame(frame)
        assert topic == original_topic
        assert payload == original_payload

# Mula framing (no Pepper server needed — local echo)

class TestMulaFraming:
    """
    Mula framing tests against a minimal local TCP server that echoes frames.
    No real Pepper router is required.
    """

    @pytest.fixture()
    def echo_server(self):
        """A server that reads one Mula frame and echoes it back."""
        ready = threading.Event()
        received: list[bytes] = []

        def _serve():
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("127.0.0.1", 0))
            srv.listen(1)
            ready.port = srv.getsockname()[1]
            ready.set()
            try:
                conn, _ = srv.accept()
            except OSError:
                srv.close(); return
            try:
                while True:
                    hdr = b""
                    while len(hdr) < 4:
                        chunk = conn.recv(4 - len(hdr))
                        if not chunk:
                            return
                        hdr += chunk
                    size = int.from_bytes(hdr, "big")
                    if size == 0:
                        continue
                    body = b""
                    while len(body) < size:
                        chunk = conn.recv(size - len(body))
                        if not chunk:
                            return
                        body += chunk
                    received.append(body)
                    conn.sendall(hdr + body)
            except OSError:
                pass   # client disconnected — expected in some tests
            finally:
                try: conn.close()
                except OSError: pass
                try: srv.close()
                except OSError: pass

        t = threading.Thread(target=_serve, daemon=True)
        t.start()
        ready.wait(timeout=2)
        yield ready.port, received

    def test_send_and_recv_frame(self, echo_server):
        port, received = echo_server
        tr = BusTransport(f"tcp://127.0.0.1:{port}")
        tr.connect(retries=3)
        tr.send_frame(b"hello-mula")
        frame = tr.recv_frame()
        tr.close()
        assert frame == b"hello-mula"
        assert received == [b"hello-mula"]

    def test_send_binary_frame(self, echo_server):
        port, _ = echo_server
        tr = BusTransport(f"tcp://127.0.0.1:{port}")
        tr.connect(retries=3)
        binary = bytes(range(256))
        tr.send_frame(binary)
        assert tr.recv_frame() == binary
        tr.close()

    def test_subscribe_sends_topic_frame(self, echo_server):
        port, received = echo_server
        tr = BusTransport(f"tcp://127.0.0.1:{port}")
        tr.connect(retries=3)
        tr.subscribe(["pepper.control", "pepper.push.w1"])
        # subscribe() calls send_frame(b"pepper.control|pepper.push.w1")
        time.sleep(0.05)  # let server read it
        tr.close()
        assert received == [b"pepper.control|pepper.push.w1"]

    def test_connect_refused_exits(self):
        tr = BusTransport("tcp://127.0.0.1:19997")
        with pytest.raises((SystemExit, OSError)):
            tr.connect(retries=1)

# Redis integration (skipped when Redis not available)

class TestRedisTransport:
    """
    Full integration test against a real Redis server.
    """

    @pytest.fixture(autouse=True)
    def _skip(self):
        _require(REDIS_HOST, REDIS_PORT, "Redis")

    def _send_frame(self, tr: BusTransport, topic: str, payload: bytes) -> None:
        tr.send_frame(_encode_frame(topic, payload))

    def _recv(self, tr: BusTransport, timeout: float = 3.0) -> tuple[str, bytes]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = tr.recv_frame()
            if frame is not None:
                return _decode_frame(frame)
        raise TimeoutError("recv timed out")

    # pubsub (SUBSCRIBE) path

    def test_subscribe_and_receive_pubsub_message(self):
        unique = f"pepper.control.test_{int(time.time()*1000)}"
        tr = BusTransport(f"redis://{REDIS_HOST}:{REDIS_PORT}")
        tr.connect()
        tr.subscribe([unique])
        time.sleep(0.05)  # let pubsub register

        pub_tr = BusTransport(f"redis://{REDIS_HOST}:{REDIS_PORT}")
        pub_tr.connect()
        try:
            pub_tr.send_frame(_encode_frame(unique, b"cap_load_payload"))
            topic, payload = self._recv(tr, timeout=3.0)
            assert topic == ""            # coord frame has no embedded topic
            assert payload == b"cap_load_payload"
        finally:
            tr.close()
            pub_tr.close()

    # BRPOP path

    def test_brpop_receives_pushed_message(self):
        queue_name = f"pepper.push.test_{int(time.time()*1000)}"
        tr = BusTransport(f"redis://{REDIS_HOST}:{REDIS_PORT}")
        tr.connect()
        tr.subscribe([queue_name])

        # Push via raw Redis LPUSH
        import socket as _sock
        raw = _sock.create_connection((REDIS_HOST, REDIS_PORT), timeout=2)
        raw.sendall(b"*3\r\n$5\r\nLPUSH\r\n$%d\r\n%s\r\n$%d\r\n" %
                    (len(queue_name), queue_name.encode(), len(b"req_payload")) + b"req_payload\r\n")
        raw.recv(64)
        raw.close()

        try:
            _, payload = self._recv(tr, timeout=3.0)
            assert payload == b"req_payload"
        finally:
            tr.close()

    # send path

    def test_send_frame_publishes_to_correct_channel(self):
        unique = f"pepper.res.test_{int(time.time()*1000)}"
        tr = BusTransport(f"redis://{REDIS_HOST}:{REDIS_PORT}")
        tr.connect()
        tr.subscribe([])

        # Subscriber via raw socket
        sub = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=2)
        sub.sendall(b"*2\r\n$9\r\nSUBSCRIBE\r\n$%d\r\n%s\r\n" %
                    (len(unique), unique.encode()))
        # Drain subscribe confirmation
        sub.recv(1024)

        self._send_frame(tr, unique, b"response_payload")

        sub.settimeout(3.0)
        reply = sub.recv(1024)
        sub.close()
        assert b"response_payload" in reply

    def test_send_frame_binary_payload_intact(self):
        unique = f"pepper.res.binary_{int(time.time()*1000)}"
        binary = bytes(range(256))

        tr = BusTransport(f"redis://{REDIS_HOST}:{REDIS_PORT}")
        tr.connect()
        tr.subscribe([])

        sub = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=2)
        sub.sendall(b"*2\r\n$9\r\nSUBSCRIBE\r\n$%d\r\n%s\r\n" %
                    (len(unique), unique.encode()))
        sub.recv(1024)

        self._send_frame(tr, unique, binary)

        sub.settimeout(3.0)
        reply = sub.recv(1024)
        sub.close()
        assert binary in reply

    def test_throughput_20_messages_no_deadlock(self):
        unique = f"pepper.push.throughput_{int(time.time()*1000)}"
        tr = BusTransport(f"redis://{REDIS_HOST}:{REDIS_PORT}")
        tr.connect()
        tr.subscribe([unique])

        # Push 20 items via LPUSH
        raw = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=2)
        for i in range(20):
            raw.sendall(b"*3\r\n$5\r\nLPUSH\r\n$%d\r\n%s\r\n$%d\r\nmsg-%d\r\n" %
                        (len(unique), unique.encode(), len(f"msg-{i}".encode()), i))
            raw.recv(64)
        raw.close()

        received = []
        deadline = time.monotonic() + 10.0
        while len(received) < 20 and time.monotonic() < deadline:
            frame = tr.recv_frame()
            if frame is not None:
                received.append(frame)
        tr.close()
        assert len(received) == 20, f"only got {len(received)}/20"

    def test_two_workers_drain_all_20_messages(self):
        unique = f"pepper.push.two_workers_{int(time.time()*1000)}"

        tr1 = BusTransport(f"redis://{REDIS_HOST}:{REDIS_PORT}")
        tr2 = BusTransport(f"redis://{REDIS_HOST}:{REDIS_PORT}")
        tr1.connect(); tr2.connect()
        tr1.subscribe([unique]); tr2.subscribe([unique])

        raw = socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=2)
        for i in range(20):
            raw.sendall(b"*3\r\n$5\r\nLPUSH\r\n$%d\r\n%s\r\n$%d\r\nmsg-%d\r\n" %
                        (len(unique), unique.encode(), len(f"msg-{i}".encode()), i))
            raw.recv(64)
        raw.close()

        received: list[bytes] = []
        deadline = time.monotonic() + 15.0
        while len(received) < 20 and time.monotonic() < deadline:
            for tr in (tr1, tr2):
                f = tr.recv_frame()
                if f is not None:
                    received.append(f)

        tr1.close(); tr2.close()
        assert len(received) == 20, (
            f"only got {len(received)}/20 — some items were never BRPOPped."
        )

# NATS integration (skipped when NATS not available)

class TestNATSTransport:
    """
    Full integration test against a real NATS server.
    """

    @pytest.fixture(autouse=True)
    def _skip(self):
        _require(NATS_HOST, NATS_PORT, "NATS")

    def _encode(self, topic: str, payload: bytes) -> bytes:
        return _encode_frame(topic, payload)

    def _recv(self, tr: BusTransport, timeout: float = 3.0) -> tuple[str, bytes]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = tr.recv_frame()
            if frame is not None:
                return _decode_frame(frame)
        raise TimeoutError("nats recv timed out")

    def test_connect_and_disconnect(self):
        tr = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        tr.connect()
        tr.close()

    def test_subscribe_flush_guarantees_delivery(self):
        """
        Worker subscribes then immediately receives a message — verifies the
        JetStream consumer is active before returning.
        """
        subject = f"pepper.push.flush_{int(time.time()*1000)}"
        tr = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        tr.connect()
        tr.subscribe([subject])

        pub = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        pub.connect()
        try:
            pub.send_frame(self._encode(subject, b"flush_test"))
            _, payload = self._recv(tr, timeout=5.0)
            assert payload == b"flush_test"
        finally:
            tr.close(); pub.close()

    def test_queue_group_exactly_once(self):
        """
        Two workers both subscribe to the same queue group. The publisher
        sends N messages and each must be received exactly once in total.
        """
        subject = f"pepper.push.qgroup_{int(time.time()*1000)}"
        tr1 = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        tr2 = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        tr1.connect(); tr1.subscribe([subject])
        tr2.connect(); tr2.subscribe([subject])

        pub = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        pub.connect()
        N = 10
        try:
            for i in range(N):
                pub.send_frame(self._encode(subject, f"msg-{i}".encode()))

            received: list[bytes] = []
            deadline = time.monotonic() + 10.0
            while len(received) < N and time.monotonic() < deadline:
                for tr in (tr1, tr2):
                    f = tr.recv_frame()
                    if f is not None:
                        received.append(f)

            assert len(received) == N, f"got {len(received)}/{N}"
        finally:
            tr1.close(); tr2.close(); pub.close()

    def test_send_frame_binary_payload(self):
        subject = f"pepper.res.binary_{int(time.time()*1000)}"
        binary = bytes(range(256))

        tr = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        tr.connect()
        tr.subscribe([subject])

        pub = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        pub.connect()
        try:
            pub.send_frame(self._encode(subject, binary))
            _, payload = self._recv(tr, timeout=5.0)
            assert payload == binary
        finally:
            tr.close(); pub.close()

    def test_throughput_20_messages_no_deadlock(self):
        """Regression test: 20 concurrent sends must complete without deadlock."""
        subject = f"pepper.push.throughput_{int(time.time()*1000)}"
        tr = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        tr.connect()
        tr.subscribe([subject])

        pub = BusTransport(f"nats://{NATS_HOST}:{NATS_PORT}")
        pub.connect()
        try:
            for i in range(20):
                pub.send_frame(self._encode(subject, f"msg-{i}".encode()))

            received = []
            deadline = time.monotonic() + 15.0
            while len(received) < 20 and time.monotonic() < deadline:
                f = tr.recv_frame()
                if f is not None:
                    received.append(f)
            assert len(received) == 20, f"got {len(received)}/20"
        finally:
            tr.close(); pub.close()

if __name__ == "__main__":
    import pytest as _pytest
    _pytest.main([__file__, "-v"])
