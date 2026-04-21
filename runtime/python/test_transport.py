"""
test_transport.py — Integration tests for pluggable bus transports.

Redis and NATS tests are skipped automatically when the service is not
reachable on localhost.  No mocks, no CI secrets required: if the port is
open the test runs; if not it is marked as skipped with a clear reason.

Run all:
    pytest internal/runtime/python/test_transport.py -v

Run only what is available:
    pytest internal/runtime/python/test_transport.py -v -k "not nats"
"""

from __future__ import annotations

import os
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Generator

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from runtime import BusTransport, _parse_bus_url

# ── helpers ───────────────────────────────────────────────────────────────────


def _port_open(host: str, port: int, timeout: float = 0.3) -> bool:
    """Return True if a TCP connection to host:port succeeds within timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _require_port(host: str, port: int, service: str) -> None:
    """Skip the test if the service port is not reachable."""
    if not _port_open(host, port):
        pytest.skip(
            f"{service} not available at {host}:{port} — "
            f"start the service or set the appropriate env var to run this test"
        )


# ── fixtures ──────────────────────────────────────────────────────────────────

REDIS_HOST = os.environ.get("PEPPER_TEST_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("PEPPER_TEST_REDIS_PORT", "6379"))

NATS_HOST = os.environ.get("PEPPER_TEST_NATS_HOST", "127.0.0.1")
NATS_PORT = int(os.environ.get("PEPPER_TEST_NATS_PORT", "4222"))


# ── BusTransport unit tests (no real service needed) ─────────────────────────


class TestBusTransportParsing:
    """URL parsing is pure logic — no network required."""

    def test_bare_host_port(self):
        t = BusTransport("127.0.0.1:7731")
        assert t.scheme == "mula"
        assert t.host == "127.0.0.1"
        assert t.port == 7731

    def test_tcp_scheme(self):
        t = BusTransport("tcp://10.0.0.1:9000")
        assert t.scheme == "tcp"
        assert t.host == "10.0.0.1"
        assert t.port == 9000

    def test_mula_scheme(self):
        t = BusTransport("mula://127.0.0.1:7731")
        assert t.scheme == "mula"
        assert t.port == 7731

    def test_nanomsg_scheme(self):
        t = BusTransport("nanomsg://127.0.0.1:5555")
        assert t.scheme == "nanomsg"
        assert t.port == 5555

    def test_redis_scheme_parses(self):
        """Redis transport is implemented — constructor must succeed and parse the URL."""
        t = BusTransport("redis://127.0.0.1:6379")
        assert t.scheme == "redis"
        assert t.host == "127.0.0.1"
        assert t.port == 6379

    def test_nats_scheme_parses(self):
        """NATS transport is implemented — constructor must succeed and parse the URL."""
        t = BusTransport("nats://127.0.0.1:4222")
        assert t.scheme == "nats"
        assert t.host == "127.0.0.1"
        assert t.port == 4222

    def test_legacy_parse_bus_url(self):
        host, port = _parse_bus_url("tcp://192.168.1.5:8080")
        assert host == "192.168.1.5"
        assert port == 8080


class TestBusTransportTCP:
    """Verifies the mula/TCP transport against a local echo server — no Pepper needed."""

    def test_connect_and_send_frame(self):
        """Connect to a minimal TCP listener and send one frame."""
        received: list[bytes] = []
        ready = threading.Event()

        def _server():
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("127.0.0.1", 0))
            srv.listen(1)
            ready.port = srv.getsockname()[1]  # type: ignore[attr-defined]
            ready.set()
            conn, _ = srv.accept()
            # Read one 4-byte length-prefixed frame
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

        transport = BusTransport(f"tcp://127.0.0.1:{ready.port}")  # type: ignore[attr-defined]
        transport.connect(retries=3)
        transport.send_frame(b"hello-pepper")
        transport.close()
        t.join(timeout=2.0)

        assert received == [b"hello-pepper"]

    def test_connect_refused_gives_up(self):
        """connect() to a dead port must not hang — exits or raises quickly."""
        transport = BusTransport("tcp://127.0.0.1:19999")
        # retries=1 → max 1 attempt, so this should complete in < 2 s
        with pytest.raises((SystemExit, OSError, ConnectionRefusedError)):
            transport.connect(retries=1)


# ── Redis reachability test ───────────────────────────────────────────────────


class TestRedisReachability:
    """
    Checks that the Redis port is open and responds to a raw PING.

    This is intentionally a lightweight connectivity test, not a full
    storage-layer test (that lives in internal/storage/redis_test.go on the
    Go side).  The purpose here is to confirm that a Pepper Python worker
    *could* reach the Redis service it would be pointed at.

    Skipped automatically when Redis is not running on REDIS_HOST:REDIS_PORT.
    Override via env vars:
        PEPPER_TEST_REDIS_HOST=my-redis-host
        PEPPER_TEST_REDIS_PORT=6380
    """

    def test_redis_port_open(self):
        _require_port(REDIS_HOST, REDIS_PORT, "Redis")
        # Port is open — basic sanity passed.

    def test_redis_ping(self):
        """Send a raw RESP PING and expect +PONG back."""
        _require_port(REDIS_HOST, REDIS_PORT, "Redis")
        with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=2.0) as conn:
            conn.sendall(b"*1\r\n$4\r\nPING\r\n")
            conn.settimeout(2.0)
            resp = conn.recv(128)
        assert resp.startswith(b"+PONG"), f"unexpected Redis response: {resp!r}"

    def test_redis_set_get_round_trip(self):
        """
        Validates the raw RESP SET/GET round trip that the Go storage layer
        and any future Python Redis transport would depend on.
        """
        _require_port(REDIS_HOST, REDIS_PORT, "Redis")

        def _send(conn: socket.socket, *args: str) -> str:
            cmd = f"*{len(args)}\r\n"
            for a in args:
                cmd += f"${len(a)}\r\n{a}\r\n"
            conn.sendall(cmd.encode())
            conn.settimeout(2.0)
            return conn.recv(256).decode(errors="replace")

        key = "pepper:transport:test"
        val = "hello-from-test"

        with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=2.0) as conn:
            r = _send(conn, "SET", key, val, "EX", "10")
            assert "+OK" in r, f"SET failed: {r!r}"

        with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=2.0) as conn:
            r = _send(conn, "GET", key)
            assert val in r, f"GET returned unexpected: {r!r}"

        # Cleanup
        with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=2.0) as conn:
            _send(conn, "DEL", key)

    def test_redis_url_constructs_and_parses(self):
        """
        Validates that a redis:// URL in PEPPER_BUS_URL is accepted by BusTransport.
        The transport is fully implemented — construction must succeed.
        """
        url = f"redis://{REDIS_HOST}:{REDIS_PORT}"
        t = BusTransport(url)
        assert t.scheme == "redis"
        assert t.host == REDIS_HOST
        assert t.port == REDIS_PORT


# ── NATS reachability test ────────────────────────────────────────────────────


class TestNATSReachability:
    """
    Checks that the NATS server port is open and speaks the NATS INFO protocol.

    Like the Redis tests above, this is a connectivity gate test — not a full
    publish/subscribe integration test.  Full NATS transport integration will
    live here once the Python runtime implements nats:// in BusTransport.

    Skipped automatically when NATS is not running on NATS_HOST:NATS_PORT.
    Override via env vars:
        PEPPER_TEST_NATS_HOST=my-nats-host
        PEPPER_TEST_NATS_PORT=4222
    """

    def test_nats_port_open(self):
        _require_port(NATS_HOST, NATS_PORT, "NATS")

    def test_nats_info_handshake(self):
        """
        NATS servers send an INFO JSON blob immediately on connect.
        Verify we receive it and it looks sane.
        """
        _require_port(NATS_HOST, NATS_PORT, "NATS")
        with socket.create_connection((NATS_HOST, NATS_PORT), timeout=2.0) as conn:
            conn.settimeout(2.0)
            data = b""
            deadline = time.monotonic() + 2.0
            while b"\r\n" not in data and time.monotonic() < deadline:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk

        assert data.startswith(b"INFO "), (
            f"Expected NATS INFO banner, got: {data[:80]!r}"
        )
        # Parse the JSON payload after "INFO "
        import json
        info_line = data.split(b"\r\n", 1)[0]
        info_json = info_line[5:]  # strip "INFO "
        info = json.loads(info_json)
        assert "version" in info, f"INFO missing 'version': {info}"
        assert "max_payload" in info, f"INFO missing 'max_payload': {info}"

    def test_nats_ping_pong(self):
        """
        NATS core protocol: send PING, expect PONG back.
        This is the health check the client library uses internally.
        """
        _require_port(NATS_HOST, NATS_PORT, "NATS")
        with socket.create_connection((NATS_HOST, NATS_PORT), timeout=2.0) as conn:
            conn.settimeout(2.0)
            # Drain the INFO banner first
            banner = b""
            while b"\r\n" not in banner:
                banner += conn.recv(4096)
            # Now send PING
            conn.sendall(b"PING\r\n")
            resp = conn.recv(64)
        assert b"PONG" in resp, f"Expected PONG, got: {resp!r}"

    def test_nats_url_constructs_and_parses(self):
        """
        Validates that a nats:// URL in PEPPER_BUS_URL is accepted by BusTransport.
        The transport is fully implemented — construction must succeed.
        """
        url = f"nats://{NATS_HOST}:{NATS_PORT}"
        t = BusTransport(url)
        assert t.scheme == "nats"
        assert t.host == NATS_HOST
        assert t.port == NATS_PORT


# ── Future: full NATS pub/sub transport test ─────────────────────────────────
# When runtime.py implements nats:// in BusTransport, add a test here that:
#   1. Creates a NATSTransport("nats://127.0.0.1:4222")
#   2. Subscribes to "pepper.res.test"
#   3. Publishes a frame to "pepper.push.default"
#   4. Asserts the frame is received on the other side
#
# Pattern mirrors TestBusTransportTCP.test_connect_and_send_frame above.


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
