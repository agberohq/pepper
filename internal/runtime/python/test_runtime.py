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
    _recv_exact, BlobWriter,
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