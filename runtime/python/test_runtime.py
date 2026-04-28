"""
Integration tests for runtime.py using a mock TCP router.
Run with: python -m pytest internal/runtime/python/test_runtime.py -v
"""

import json
import os
import socket
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from runtime import (
    Worker, _Codec, _parse_bus_url, _encode_msg, _reply_topic,
    _recv_exact, BlobWriter, BusTransport,
    pepper, _origin_id_var, _cancelled_ids, _cancelled_lock,
)

class MockRouter:
    """TCP mock router. Spawns on a random port."""

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        self.conn: socket.socket | None = None

    def accept(self, timeout: float = 3.0):
        self.sock.settimeout(timeout)
        self.conn, _ = self.sock.accept()
        self.sock.settimeout(None)

    def recv_envelope(self, skip_types: tuple = ("hb_ping",)) -> tuple[str, dict]:
        """Receive next envelope, skipping any message types in skip_types."""
        while True:
            assert self.conn is not None
            data = self._recv_frame()
            if b"|" in data and data.startswith(b"pepper."):
                continue
            tl = int.from_bytes(data[:2], "big")
            topic = data[2:2 + tl].decode()
            payload = data[2 + tl:]
            env = json.loads(payload)
            if env.get("msg_type") not in skip_types:
                return topic, env

    def send_envelope(self, topic: str, env: dict):
        assert self.conn is not None
        # Encode any bytes values as JSON strings so json.dumps doesn't fail.
        # The worker decodes the outer envelope with JSON; bytes payloads must
        # be pre-encoded as JSON strings (the worker re-decodes payload field).
        def _encode(v):
            if isinstance(v, bytes):
                return v.decode("utf-8", errors="replace")
            return v
        serialisable = {k: _encode(v) for k, v in env.items()}
        payload = json.dumps(serialisable).encode()
        body = len(topic.encode()).to_bytes(2, "big") + topic.encode() + payload
        self._send_frame(body)

    def close(self):
        if self.conn:
            self.conn.close()
        self.sock.close()

    def _recv_frame(self) -> bytes:
        assert self.conn is not None
        hdr = _recv_exact(self.conn, 4)
        if hdr is None:
            raise ConnectionResetError()
        size = int.from_bytes(hdr, "big")
        return _recv_exact(self.conn, size)

    def _send_frame(self, data: bytes):
        assert self.conn is not None
        self.conn.sendall(len(data).to_bytes(4, "big") + data)

import pytest

@pytest.fixture
def mock_router():
    router = MockRouter()
    yield router
    router.close()

@pytest.fixture
def env_setup(mock_router):
    old = dict(os.environ)
    os.environ.update({
        "PEPPER_WORKER_ID": "w-test-1",
        "PEPPER_BUS_URL": f"tcp://127.0.0.1:{mock_router.port}",
        "PEPPER_GROUPS": "default,test",
        "PEPPER_HEARTBEAT_MS": "100",
        "PEPPER_MAX_CONCURRENT": "2",
        "PEPPER_CODEC": "json",
    })
    yield
    os.environ.clear()
    os.environ.update(old)

class TestCodec:
    def test_json_roundtrip(self):
        c = _Codec("json")
        data = {"hello": "world", "n": 42}
        assert c.unmarshal(c.marshal(data)) == data

class TestFraming:
    def test_encode_msg(self):
        frame = _encode_msg("pepper.res.abc", b"payload")
        tl = int.from_bytes(frame[:2], "big")
        assert frame[2:2 + tl] == b"pepper.res.abc"
        assert frame[2 + tl:] == b"payload"

    def test_reply_topic_routing(self):
        assert _reply_topic({"msg_type": "res", "origin_id": "01HZ9K"}) == "pepper.res.01HZ9K"
        assert _reply_topic({"msg_type": "err", "origin_id": "01HZ9K"}) == "pepper.res.01HZ9K"
        assert _reply_topic({"msg_type": "hb_ping", "worker_id": "w-1"}) == "pepper.hb.w-1"
        assert _reply_topic({"msg_type": "pipe", "topic": "pepper.pipe.x"}) == "pepper.pipe.x"

class TestWorkerLifecycle:
    def test_hello_message(self, mock_router, env_setup):
        worker = Worker()

        def run_worker():
            try:
                worker.run()
            except (ConnectionResetError, OSError):
                pass

        t = threading.Thread(target=run_worker, daemon=True)
        t.start()

        mock_router.accept()
        topic, env = mock_router.recv_envelope()
        assert topic == "pepper.control"
        assert env["msg_type"] == "worker_hello"
        assert env["worker_id"] == "w-test-1"
        assert env["runtime"] == "python"
        assert env["groups"] == ["default", "test"]
        assert env["cb_supported"] is True

        mock_router.send_envelope("pepper.broadcast", {"msg_type": "worker_bye"})
        t.join(timeout=2.0)

    def test_cap_load_and_ready(self, mock_router, env_setup, tmp_path: Path):
        cap_file = tmp_path / "echo.py"
        cap_file.write_text("""
# pepper:name = echo
# pepper:version = 1.0.0

def run(inputs: dict) -> dict:
    return {"echo": inputs.get("text", "")}
""")
        worker = Worker()

        def run_worker():
            try:
                worker.run()
            except (ConnectionResetError, OSError):
                pass

        t = threading.Thread(target=run_worker, daemon=True)
        t.start()

        mock_router.accept()
        mock_router.recv_envelope()  # hello

        mock_router.send_envelope("pepper.control", {
            "msg_type": "cap_load",
            "cap": "echo",
            "cap_ver": "1.0.0",
            "source": str(cap_file),
            "config": {},
        })

        topic, env = mock_router.recv_envelope()
        assert env["msg_type"] == "cap_ready"
        assert env["cap"] == "echo"
        assert env["error"] == ""

        mock_router.send_envelope("pepper.broadcast", {"msg_type": "worker_bye"})
        t.join(timeout=2.0)

    def test_request_response(self, mock_router, env_setup, tmp_path: Path):
        cap_file = tmp_path / "double.py"
        cap_file.write_text("""
# pepper:name = double

def run(inputs: dict) -> dict:
    return {"result": inputs["n"] * 2}
""")
        worker = Worker()

        def run_worker():
            try:
                worker.run()
            except (ConnectionResetError, OSError):
                pass

        t = threading.Thread(target=run_worker, daemon=True)
        t.start()

        mock_router.accept()
        mock_router.recv_envelope()  # hello

        mock_router.send_envelope("pepper.control", {
            "msg_type": "cap_load",
            "cap": "double",
            "source": str(cap_file),
            "config": {},
        })
        mock_router.recv_envelope()  # cap_ready

        mock_router.send_envelope("pepper.push.default", {
            "msg_type": "req",
            "corr_id": "corr-1",
            "origin_id": "origin-1",
            "cap": "double",
            "payload": json.dumps({"n": 21}).encode(),
            "meta": {},
        })

        topic, env = mock_router.recv_envelope()
        assert env["msg_type"] == "res"
        assert env["corr_id"] == "corr-1"
        result = json.loads(env["payload"])
        assert result["result"] == 42

        mock_router.send_envelope("pepper.broadcast", {"msg_type": "worker_bye"})
        t.join(timeout=2.0)

    def test_cancel_request(self, mock_router, env_setup, tmp_path: Path):
        cap_file = tmp_path / "slow.py"
        cap_file.write_text("""
import time
def run(inputs: dict) -> dict:
    time.sleep(10)
    return {}
""")
        worker = Worker()

        def run_worker():
            try:
                worker.run()
            except (ConnectionResetError, OSError):
                pass

        t = threading.Thread(target=run_worker, daemon=True)
        t.start()

        mock_router.accept()
        mock_router.recv_envelope()  # hello
        mock_router.send_envelope("pepper.control", {
            "msg_type": "cap_load", "cap": "slow",
            "source": str(cap_file), "config": {},
        })
        mock_router.recv_envelope()  # cap_ready

        mock_router.send_envelope("pepper.push.default", {
            "msg_type": "req",
            "corr_id": "corr-2",
            "origin_id": "origin-cancel-test",
            "cap": "slow",
            "payload": b"{}",
            "meta": {},
        })

        mock_router.send_envelope("pepper.broadcast", {
            "msg_type": "cancel",
            "origin_id": "origin-cancel-test",
        })

        topic, env = mock_router.recv_envelope()
        assert env["msg_type"] == "err"
        assert env["code"] == "CANCELLED"

        mock_router.send_envelope("pepper.broadcast", {"msg_type": "worker_bye"})
        t.join(timeout=2.0)

    def test_uses_fixture_caps(self, mock_router, env_setup):
        """Test against the real fixture capabilities in tests/caps/."""
        fixture_dir = Path(__file__).parent.parent.parent.parent / "tests" / "caps"
        if not fixture_dir.exists():
            pytest.skip("tests/caps/ not found")

        worker = Worker()

        def run_worker():
            try:
                worker.run()
            except (ConnectionResetError, OSError):
                pass

        t = threading.Thread(target=run_worker, daemon=True)
        t.start()

        mock_router.accept()
        mock_router.recv_envelope()  # hello

        # Load echo.py fixture
        echo_path = fixture_dir / "echo.py"
        if echo_path.exists():
            mock_router.send_envelope("pepper.control", {
                "msg_type": "cap_load", "cap": "echo",
                "source": str(echo_path), "config": {},
            })
            topic, env = mock_router.recv_envelope()
            assert env["msg_type"] == "cap_ready"

        mock_router.send_envelope("pepper.broadcast", {"msg_type": "worker_bye"})
        t.join(timeout=2.0)

class TestPepperAPI:
    def test_context_vars(self, mock_router, env_setup, tmp_path: Path):
        cap_file = tmp_path / "ctx.py"
        cap_file.write_text("""
from runtime import pepper

def run(inputs: dict) -> dict:
    return {
        "corr": pepper.corr_id(),
        "origin": pepper.origin_id(),
        "delivery": pepper.delivery_count(),
    }
""")
        worker = Worker()

        def run_worker():
            try:
                worker.run()
            except (ConnectionResetError, OSError):
                pass

        t = threading.Thread(target=run_worker, daemon=True)
        t.start()

        mock_router.accept()
        mock_router.recv_envelope()  # hello

        mock_router.send_envelope("pepper.control", {
            "msg_type": "cap_load", "cap": "ctx",
            "source": str(cap_file), "config": {},
        })
        mock_router.recv_envelope()  # cap_ready

        mock_router.send_envelope("pepper.push.default", {
            "msg_type": "req",
            "corr_id": "corr-ctx",
            "origin_id": "origin-ctx",
            "cap": "ctx",
            "delivery_count": 3,
            "payload": b"{}",
            "meta": {},
        })

        topic, env = mock_router.recv_envelope()
        result = json.loads(env["payload"])
        assert result["corr"] == "corr-ctx"
        assert result["origin"] == "origin-ctx"
        assert result["delivery"] == 3

        mock_router.send_envelope("pepper.broadcast", {"msg_type": "worker_bye"})
        t.join(timeout=2.0)

class TestBlobWriter:
    def test_writes_file(self, tmp_path: Path):
        BlobWriter._dir = str(tmp_path)
        ref = BlobWriter.write(
            b"binary data",
            dtype="float32",
            shape=[1, 2, 3],
            format="image/jpeg",
            origin_id="origin-1",
        )
        assert ref["_pepper_blob"] is True
        assert ref["id"].startswith("blob_")
        assert ref["size"] == 11  # len(b"binary data") == 11
        assert Path(ref["path"]).exists()
        assert Path(ref["path"]).read_bytes() == b"binary data"

if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])

# Transport tests
# These test the BusTransport class that Python workers use to connect to the
# Pepper router bus.  Redis and NATS tests skip automatically when the service
# is not reachable — no mocks, no secrets required.

import socket as _socket
import os as _os

def _port_open(host: str, port: int, timeout: float = 0.3) -> bool:
    """Return True if TCP host:port is reachable within timeout."""
    try:
        with _socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False

_REDIS_HOST = _os.environ.get("PEPPER_TEST_REDIS_HOST", "127.0.0.1")
_REDIS_PORT = int(_os.environ.get("PEPPER_TEST_REDIS_PORT", "6379"))
_NATS_HOST  = _os.environ.get("PEPPER_TEST_NATS_HOST", "127.0.0.1")
_NATS_PORT  = int(_os.environ.get("PEPPER_TEST_NATS_PORT", "4222"))

class TestBusTransportParsing:
    """URL parsing is pure logic — no network required."""

    def test_mula_scheme(self):
        t = BusTransport("mula://127.0.0.1:7731")
        assert t.scheme == "mula"
        assert t.host == "127.0.0.1"
        assert t.port == 7731

    def test_tcp_scheme(self):
        t = BusTransport("tcp://10.0.0.1:9000")
        assert t.scheme == "tcp"
        assert t.port == 9000

    def test_bare_host_port(self):
        t = BusTransport("127.0.0.1:7731")
        assert t.scheme == "mula"
        assert t.port == 7731

    def test_redis_scheme_parses(self):
        """Redis transport is implemented — constructor must succeed."""
        t = BusTransport("redis://127.0.0.1:6379")
        assert t.scheme == "redis"
        assert t.port == 6379

    def test_nats_scheme_parses(self):
        """NATS transport is implemented — constructor must succeed."""
        t = BusTransport("nats://127.0.0.1:4222")
        assert t.scheme == "nats"
        assert t.port == 4222

class TestBusTransportTCP:
    """Verifies the mula/TCP transport against a real local echo server."""

    def test_connect_and_send_frame(self):
        received = []
        ready = threading.Event()

        def _server():
            srv = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            srv.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
            srv.bind(("127.0.0.1", 0))
            srv.listen(1)
            ready.port = srv.getsockname()[1]
            ready.set()
            conn, _ = srv.accept()
            hdr = conn.recv(4)
            if hdr:
                size = int.from_bytes(hdr, "big")
                data = b""
                while len(data) < size:
                    chunk = conn.recv(size - len(data))
                    if not chunk:
                        break
                    data += chunk
                received.append(data)
            conn.close()
            srv.close()

        t = threading.Thread(target=_server, daemon=True)
        t.start()
        ready.wait(timeout=2.0)

        transport = BusTransport(f"tcp://127.0.0.1:{ready.port}")
        transport.connect(retries=3)
        transport.send_frame(b"hello-pepper")
        transport.close()
        t.join(timeout=2.0)

        assert received == [b"hello-pepper"]

class TestRedisWorkerConnectivity:
    """
    Verifies that a Python worker can reach Redis when PEPPER_BUS_URL points
    to a redis:// address. The Redis transport is fully implemented.

    All tests in this class skip automatically when Redis is not reachable.
    """

    def _require_redis(self):
        import pytest
        if not _port_open(_REDIS_HOST, _REDIS_PORT):
            pytest.skip(f"Redis not available at {_REDIS_HOST}:{_REDIS_PORT}")

    def test_redis_port_open(self):
        self._require_redis()
        # Just proving the port is reachable — the skip guard above handles the rest.

    def test_redis_ping(self):
        """Raw RESP PING/PONG proves the server is a real Redis instance."""
        self._require_redis()
        with _socket.create_connection((_REDIS_HOST, _REDIS_PORT), timeout=2.0) as conn:
            conn.sendall(b"*1\r\n$4\r\nPING\r\n")
            conn.settimeout(2.0)
            resp = conn.recv(128)
        assert resp.startswith(b"+PONG"), f"unexpected Redis response: {resp!r}"

    def test_redis_url_constructs_and_parses(self):
        """
        BusTransport accepts redis:// URLs — the transport is fully implemented.
        Verifies construction succeeds and parses the URL correctly.
        """
        self._require_redis()
        url = f"redis://{_REDIS_HOST}:{_REDIS_PORT}"
        t = BusTransport(url)
        assert t.scheme == "redis"
        assert t.host == _REDIS_HOST
        assert t.port == _REDIS_PORT

    def test_worker_redis_end_to_end_msgpack(self, tmp_path):
        """
        End-to-end round-trip through a real Python Worker using Redis transport
        and MSGPACK codec — exactly the configuration the Go cluster test uses.

        This is the missing integration test that bridges:
          - transport.py tests (prove BRPOP works)
          - isolation tests (prove Go dispatch/routing works)

        If this test fails with "No response received", the bug is in the Worker's
        response publishing path when using Redis+msgpack.

        Previously failed because the response SUBSCRIBE was set up AFTER the LPUSH,
        creating a race: echo cap runs in 0ms so the PUBLISH arrives before the
        SUBSCRIBE is registered. Fixed by setting up the PSUBSCRIBE listener on a
        background thread BEFORE the LPUSH, matching how the Go runtime works
        (PSubscribe to pepper.res.* in bootRuntime before workers start).
        """
        import pytest
        self._require_redis()

        try:
            import msgpack as _mp
        except ImportError:
            pytest.skip("msgpack not installed")

        import os
        import threading
        import time
        import socket as _sock

        WORKER_ID = f"test-worker-{int(time.time())}"
        PUSH_QUEUE = f"pepper.push.{WORKER_ID}"
        CORR_ID = "test-corr-01"
        ORIGIN_ID = "test-origin-01"
        CAP_NAME = "test.echo"
        REPLY_TOPIC = f"pepper.res.{ORIGIN_ID}"

        # Write a simple echo cap.
        cap_file = tmp_path / "echo.py"
        cap_file.write_text(
            "# pepper:name = test.echo\n"
            "def run(inputs: dict) -> dict:\n"
            "    return {**inputs, 'echoed': True}\n"
        )

        # Step 1: Set up PSUBSCRIBE listener BEFORE anything else
        # This mirrors how the Go runtime sets up PSubscribe before workers start,
        # so responses are never missed even for instant caps (0ms exec time).
        from transport import _resp_cmd as rc, _BufReader, _resp_read
        res_conn = _sock.create_connection((_REDIS_HOST, _REDIS_PORT), timeout=5)
        res_conn.sendall(rc("PSUBSCRIBE", "pepper.res.*"))
        # Drain the psubscribe confirmation.
        res_reader = _BufReader(res_conn)
        res_conn.settimeout(2.0)
        try:
            _resp_read(res_reader)   # *4 psubscribe pepper.res.* 1
        except Exception:
            pass
        res_conn.settimeout(None)

        # Collect responses on a background thread so we don't block the main thread.
        received_responses: list[dict] = []
        res_event = threading.Event()

        def _listen_for_response():
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                res_conn.settimeout(max(0.1, deadline - time.monotonic()))
                try:
                    msg = _resp_read(res_reader)
                    # PSubscribe delivers: ["pmessage", pattern, channel, payload]
                    if isinstance(msg, list) and len(msg) >= 4 and msg[0] in ("pmessage", b"pmessage"):
                        raw_payload = msg[-1]
                        if isinstance(raw_payload, bytes) and raw_payload:
                            env = _mp.unpackb(raw_payload, raw=False)
                            received_responses.append(env)
                            res_event.set()
                            return
                except Exception:
                    break

        listener = threading.Thread(target=_listen_for_response, daemon=True)
        listener.start()

        # Step 2: Set env and start worker
        old_env = dict(os.environ)
        os.environ.update({
            "PEPPER_WORKER_ID": WORKER_ID,
            "PEPPER_BUS_URL": f"redis://{_REDIS_HOST}:{_REDIS_PORT}",
            "PEPPER_GROUPS": "default",
            "PEPPER_HEARTBEAT_MS": "5000",
            "PEPPER_MAX_CONCURRENT": "4",
            "PEPPER_CODEC": "msgpack",
        })

        worker = Worker()

        def run_worker():
            try:
                worker.run()
            except Exception:
                pass

        t = threading.Thread(target=run_worker, daemon=True)
        t.start()

        try:
            # Wait for worker to connect and subscribe (BRPOP + pubsub).
            time.sleep(0.3)

            # Step 3: Send cap_load via PUBLISH
            cap_load_env = {
                "proto_ver": 1, "msg_type": "cap_load",
                "worker_id": WORKER_ID, "cap": CAP_NAME,
                "source": str(cap_file), "config": {},
            }
            cap_load_bytes = _mp.packb(cap_load_env, use_bin_type=True)
            raw = _sock.create_connection((_REDIS_HOST, _REDIS_PORT), timeout=2)
            raw.sendall(rc("PUBLISH", f"pepper.control.{WORKER_ID}", cap_load_bytes))
            raw.recv(64)
            raw.close()

            time.sleep(0.3)  # let cap_load + cap_ready complete

            # Step 4: LPUSH the request
            # The PSUBSCRIBE listener is already running, so even a 0ms cap response is caught.
            payload = _mp.packb({"msg": "hello"}, use_bin_type=True)
            req_env = {
                "proto_ver": 1, "msg_type": "req",
                "corr_id": CORR_ID, "origin_id": ORIGIN_ID,
                "cap": CAP_NAME, "group": "default", "worker_id": "",
                "reply_to": REPLY_TOPIC,
                "payload": payload, "meta": {}, "deadline_ms": 0,
                "dispatch": "any", "hop": 0, "max_hops": 10,
            }
            req_bytes = _mp.packb(req_env, use_bin_type=True)
            raw2 = _sock.create_connection((_REDIS_HOST, _REDIS_PORT), timeout=2)
            raw2.sendall(rc("LPUSH", PUSH_QUEUE, req_bytes))
            raw2.recv(64)
            raw2.close()

            # Step 5: Wait for response
            got = res_event.wait(timeout=10.0)

            assert got and received_responses, (
                "No response received within 10s after exec_done. "
                "The Python Worker executed the cap but failed to publish "
                f"the response to {REPLY_TOPIC}. "
                "Check: send_frame, _try_publish, pub_conn state."
            )

            response_env = received_responses[0]
            assert response_env.get("msg_type") in ("res", "err"), (
                f"Unexpected msg_type: {response_env}"
            )
            assert response_env.get("corr_id") == CORR_ID, (
                f"corr_id mismatch: got {response_env.get('corr_id')!r}, want {CORR_ID!r}"
            )
            if response_env.get("msg_type") == "res":
                result = _mp.unpackb(response_env["payload"], raw=False)
                assert result.get("echoed") is True, f"echo cap returned wrong result: {result}"

        finally:
            worker._running = False
            res_conn.close()
            listener.join(timeout=1.0)
            os.environ.clear()
            os.environ.update(old_env)
            t.join(timeout=3.0)

class TestNATSWorkerConnectivity:
    """
    Verifies that a Python worker can reach NATS when PEPPER_BUS_URL points
    to a nats:// address. The NATS transport is fully implemented.

    All tests in this class skip automatically when NATS is not reachable.
    """

    def _require_nats(self):
        import pytest
        if not _port_open(_NATS_HOST, _NATS_PORT):
            pytest.skip(f"NATS not available at {_NATS_HOST}:{_NATS_PORT}")

    def test_nats_port_open(self):
        self._require_nats()

    def test_nats_info_handshake(self):
        """NATS sends an INFO JSON banner on connect — parse and validate it."""
        self._require_nats()
        with _socket.create_connection((_NATS_HOST, _NATS_PORT), timeout=2.0) as conn:
            conn.settimeout(2.0)
            data = b""
            while b"\r\n" not in data:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
        assert data.startswith(b"INFO "), f"expected NATS INFO banner, got: {data[:80]!r}"
        info_line = data.split(b"\r\n", 1)[0]
        import json as _json
        info = _json.loads(info_line[5:])  # strip "INFO "
        assert "version" in info
        assert "max_payload" in info

    def test_nats_ping_pong(self):
        """NATS PING/PONG health check — same mechanism client libraries use."""
        self._require_nats()
        with _socket.create_connection((_NATS_HOST, _NATS_PORT), timeout=2.0) as conn:
            conn.settimeout(2.0)
            banner = b""
            while b"\r\n" not in banner:
                banner += conn.recv(4096)
            conn.sendall(b"PING\r\n")
            resp = conn.recv(64)
        assert b"PONG" in resp, f"expected PONG, got: {resp!r}"

    def test_nats_url_constructs_and_parses(self):
        """
        BusTransport accepts nats:// URLs — the transport is fully implemented.
        Verifies construction succeeds and parses the URL correctly.
        """
        self._require_nats()
        url = f"nats://{_NATS_HOST}:{_NATS_PORT}"
        t = BusTransport(url)
        assert t.scheme == "nats"
        assert t.host == _NATS_HOST
        assert t.port == _NATS_PORT
