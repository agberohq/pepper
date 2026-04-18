"""
runtime.py — Pepper Python worker runtime.
"""
from __future__ import annotations

import contextvars
import importlib.util
import inspect
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Generator
import socket

log = logging.getLogger("pepper.runtime")

# ── Context variables ─────────────────────────────────────────────────────────
_corr_id_var:    contextvars.ContextVar[str] = contextvars.ContextVar("corr_id", default="")
_origin_id_var:  contextvars.ContextVar[str] = contextvars.ContextVar("origin_id", default="")
_meta_var:       contextvars.ContextVar[dict] = contextvars.ContextVar("meta", default={})
_config_var:     contextvars.ContextVar[dict] = contextvars.ContextVar("config", default={})
_delivery_var:   contextvars.ContextVar[int]  = contextvars.ContextVar("delivery", default=0)
_session_var:    contextvars.ContextVar[dict] = contextvars.ContextVar("session", default={})
_cancelled_ids:  set[str] = set()
_cancelled_lock: threading.Lock = threading.Lock()

# ── Retryable errors per spec §4.4 ────────────────────────────────────────────
_RETRYABLE_CODES = {
    "DEADLINE_EXCEEDED", "WORKER_OOM", "NO_WORKERS",
    "WORKER_SHUTTING_DOWN", "DISPATCH_TIMEOUT", "MODEL_OOM", "MODEL_TIMEOUT",
}

# ── Public Python API ─────────────────────────────────────────────────────────
class pepper:
    @staticmethod
    def is_cancelled() -> bool:
        origin = _origin_id_var.get()
        with _cancelled_lock:
            if origin in _cancelled_ids:
                return True
        # Also check deadline — spec §5.9 says is_cancelled() returns True
        # if either deadline passed OR cancel message received.
        # We rely on the router's reaper for deadline; here we do a quick check.
        return False  # deadline is enforced by router reaper

    @staticmethod
    def corr_id() -> str:
        return _corr_id_var.get()

    @staticmethod
    def origin_id() -> str:
        return _origin_id_var.get()

    @staticmethod
    def meta() -> dict:
        return dict(_meta_var.get())

    @staticmethod
    def config() -> dict:
        return dict(_config_var.get())

    @staticmethod
    def delivery_count() -> int:
        return _delivery_var.get()

    @staticmethod
    def session() -> "SessionProxy":
        return SessionProxy(_session_var.get())

    @staticmethod
    def forward(topic: str, payload: dict) -> None:
        _PipeForwarder.forward(topic, payload)

    @staticmethod
    def call(cap: str, inputs: dict, timeout: float = 30.0) -> dict:
        return _CallbackDispatcher.call(cap, inputs, timeout)

    @staticmethod
    def gather(*pipe_calls) -> list[dict]:
        return _PipeForwarder.gather(*pipe_calls)

    @staticmethod
    def pipe(topic: str, payload: dict) -> "_PipeCall":
        return _PipeCall(topic=topic, payload=payload)

    @staticmethod
    def new_blob(data: bytes, *, dtype: str = "", shape: list | None = None, format: str = "") -> dict:
        return BlobWriter.write(data, dtype=dtype, shape=shape or [], format=format,
                                origin_id=_origin_id_var.get())


class SessionProxy:
    def __init__(self, data: dict): self._data = data
    def get(self, key: str, default=None): return self._data.get(key, default)
    def set(self, key: str, value: Any) -> None:
        updates = _meta_var.get().setdefault("_session_updates", {})
        updates[key] = value
        self._data[key] = value


class _PipeCall:
    def __init__(self, topic: str, payload: dict): self.topic, self.payload = topic, payload


class _PipeForwarder:
    _sock: socket.socket | None = None
    _send_lock = threading.Lock()

    @classmethod
    def init(cls, conn: socket.socket):
        cls._sock = conn

    @classmethod
    def forward(cls, topic: str, payload: dict) -> None:
        if cls._sock is None:
            raise RuntimeError("pepper.forward: worker not initialized")
        env = {"msg_type": "pipe", "topic": topic, "payload": payload,
               "corr_id": _corr_id_var.get(), "origin_id": _origin_id_var.get()}
        data = _codec.marshal(env)
        frame = _encode_msg(topic, data)
        with cls._send_lock:
            cls._sock.sendall(frame)

    @classmethod
    def gather(cls, *pipe_calls: _PipeCall) -> list[dict]:
        results: list[dict] = []
        out_lock = threading.Lock()

        def _send_and_wait(call: _PipeCall):
            # Subscribe to a temporary gather topic would require SUB socket.
            # Simplified: forward and collect responses via a shared gather map.
            # For full implementation, worker needs a SUB socket per pipe topic.
            cls.forward(call.topic, call.payload)
            # Placeholder: in production, this correlates on a gather topic.
            with out_lock:
                results.append({"topic": call.topic, "status": "dispatched"})

        threads = []
        for call in pipe_calls:
            t = threading.Thread(target=_send_and_wait, args=(call,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        return results


class _CallbackDispatcher:
    _pending: Dict[str, threading.Event] = {}
    _results: Dict[str, Any] = {}
    _errors: Dict[str, str] = {}
    _lock = threading.Lock()
    _worker_ref: Worker | None = None

    @classmethod
    def init(cls, worker: Worker):
        cls._worker_ref = worker

    @classmethod
    def handle_response(cls, cb_id: str, payload: dict, error: str | None = None):
        with cls._lock:
            ev = cls._pending.get(cb_id)
            if ev is None:
                return
            if error:
                cls._errors[cb_id] = error
            else:
                cls._results[cb_id] = payload
            ev.set()

    @classmethod
    def call(cls, cap: str, inputs: dict, timeout: float = 30.0) -> dict:
        if cls._worker_ref is None:
            raise RuntimeError("pepper.call: runtime not initialized")

        cb_id = f"cb_{int(time.time()*1000)}_{threading.current_thread().ident}"
        origin = _origin_id_var.get()

        req_env = {
            "proto_ver": 1, "msg_type": "cb_req",
            "corr_id": _corr_id_var.get(), "origin_id": origin, "cb_id": cb_id,
            "cap": cap, "payload": cls._worker_ref._codec.marshal(inputs),
            "deadline_ms": int((time.time() + timeout) * 1000),
        }

        ev = threading.Event()
        with cls._lock:
            cls._pending[cb_id] = ev

        cls._worker_ref._send_envelope(req_env)

        if not ev.wait(timeout=timeout):
            with cls._lock:
                cls._pending.pop(cb_id, None)
            raise TimeoutError(f"pepper.call: timeout waiting for {cap}")

        with cls._lock:
            err = cls._errors.pop(cb_id, None)
            result = cls._results.pop(cb_id, None)
            cls._pending.pop(cb_id, None)

        if err:
            raise RuntimeError(f"pepper.call: {err}")
        return result if result is not None else {}


class BlobWriter:
    _dir: str = os.environ.get("PEPPER_BLOB_DIR", "/dev/shm")
    _lock = threading.Lock()

    @classmethod
    def write(cls, data: bytes, *, dtype: str, shape: list, format: str, origin_id: str) -> dict:
        import uuid
        blob_id = f"blob_{uuid.uuid4().hex}"
        path = os.path.join(cls._dir, f"pepper_blob_{blob_id}.dat")
        with cls._lock:
            with open(path, "wb") as f:
                f.write(data)
        ref = {
            "_pepper_blob": True,
            "id": blob_id,
            "path": path,
            "size": len(data),
            "dtype": dtype,
            "shape": shape,
            "format": format,
        }
        if origin_id:
            meta = _meta_var.get()
            meta.setdefault("_blobs", []).append(blob_id)
        return ref


# ── Codec ─────────────────────────────────────────────────────────────────────
class _Codec:
    def __init__(self, name: str): self.name = name
    def marshal(self, v: Any) -> bytes:
        if self.name == "msgpack":
            return msgpack.packb(v, use_bin_type=True)
        return json.dumps(v).encode()
    def unmarshal(self, data: bytes) -> Any:
        if self.name == "msgpack":
            return msgpack.unpackb(data, raw=False)
        return json.loads(data)

try:
    import msgpack
except ImportError:
    msgpack = None  # type: ignore

_codec = _Codec(os.environ.get("PEPPER_CODEC", "msgpack"))


def _make_codec() -> "_Codec":
    """Create a codec from the current environment. Called at Worker init time."""
    return _Codec(os.environ.get("PEPPER_CODEC", "msgpack"))


# ── Form A parser — defined in cap.py, imported here for runtime use ──────────
from cap import parse_form_a  # noqa: E402


class LoadedCap:
    def __init__(self, spec: dict, runner: Any, is_class: bool):
        self.spec = spec
        self.runner = runner
        self.is_class = is_class
        self.semaphore = threading.Semaphore(spec.get("max_concurrent", 4))

    def run(self, inputs: dict) -> Any:
        if self.is_class:
            return self.runner.run(**inputs)
        fn = getattr(self.runner, "run", None)
        if fn:
            return fn(inputs)
        raise AttributeError(f"capability {self.spec['cap']} has no run()")

    def teardown(self):
        if self.is_class and hasattr(self.runner, "teardown"):
            self.runner.teardown()
        elif not self.is_class and hasattr(self.runner, "teardown"):
            self.runner.teardown()


def load_capability(cap_load: dict) -> LoadedCap:
    source_path = cap_load.get("source", "")
    if not source_path or not Path(source_path).exists():
        raise FileNotFoundError(f"capability source not found: {source_path}")

    spec = importlib.util.spec_from_file_location(cap_load["cap"], source_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Parse Form A header from source before importing
    source_text = Path(source_path).read_text()
    form_a_meta = parse_form_a(source_text)

    form_b_class = None
    for name in dir(module):
        obj = getattr(module, name)
        if inspect.isclass(obj) and hasattr(obj, "_pepper_capability"):
            form_b_class = obj
            break

    if form_b_class is not None:
        instance = form_b_class()
        if hasattr(instance, "setup"):
            instance.setup(cap_load.get("config", {}))
        # Merge Form A metadata as fallback
        if "name" not in (form_b_class.__dict__ if hasattr(form_b_class, '__dict__') else {}):
            pass  # Form B decorator takes precedence
        return LoadedCap({**form_a_meta, **cap_load}, instance, is_class=True)
    else:
        if hasattr(module, "setup"):
            module.setup(cap_load.get("config", {}))
        return LoadedCap({**form_a_meta, **cap_load}, module, is_class=False)


# ── Worker ────────────────────────────────────────────────────────────────────
class Worker:
    def __init__(self):
        self.worker_id = os.environ["PEPPER_WORKER_ID"]
        self.bus_url = os.environ["PEPPER_BUS_URL"]
        self.groups = os.environ.get("PEPPER_GROUPS", "default").split(",")
        self.heartbeat_ms = int(os.environ.get("PEPPER_HEARTBEAT_MS", "5000"))
        self.max_concurrent = int(os.environ.get("PEPPER_MAX_CONCURRENT", "8"))
        self.caps: dict = {}
        self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent)
        self.requests_served = 0
        self.started_at = time.time()
        self._running = True
        self._conn: socket.socket | None = None
        self._send_lock = threading.Lock()
        self._codec = _make_codec()  # read PEPPER_CODEC at construction time

    def run(self):
        log.info("pepper worker %s starting, bus=%s", self.worker_id, self.bus_url)
        host, port = _parse_bus_url(self.bus_url)

        for attempt in range(10):
            try:
                self._conn = socket.create_connection((host, port), timeout=5)
                self._conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                break
            except OSError:
                time.sleep(0.1 * (2 ** attempt))
        else:
            log.error("worker %s: could not connect to router", self.worker_id)
            sys.exit(1)

        _PipeForwarder.init(self._conn)
        _CallbackDispatcher.init(self)

        # Send hello with empty caps (will be populated after cap_load)
        self._send_envelope(self._make_hello())

        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb_thread.start()
        self._message_loop()

    def _message_loop(self):
        while self._running:
            try:
                data = self._recv_frame()
                if data is None:
                    continue
                # Frame format: [2-byte topic len][topic][payload]
                if len(data) < 2:
                    continue
                topic_len = int.from_bytes(data[:2], "big")
                payload = data[2 + topic_len:]
                env = self._codec.unmarshal(payload) if payload else {}
            except (OSError, ConnectionResetError) as e:
                if self._running:
                    log.error("worker %s: connection lost: %s", self.worker_id, e)
                break
            except Exception as e:
                log.warning("worker %s: decode error: %s", self.worker_id, e)
                continue

            msg_type = env.get("msg_type", "")
            if msg_type == "cap_load":
                self._handle_cap_load(env)
            elif msg_type == "req":
                self.executor.submit(self._handle_request_and_reply, env)
            elif msg_type == "worker_bye":
                self._running = False
                break
            elif msg_type == "cancel":
                origin_id = env.get("origin_id", "")
                if origin_id:
                    with _cancelled_lock:
                        _cancelled_ids.add(origin_id)
                    # Auto-expire after 2x max deadline to prevent unbounded growth
                    threading.Thread(
                        target=lambda oid=origin_id: (time.sleep(300), _cancelled_ids.discard(oid)),
                        daemon=True,
                    ).start()
            elif msg_type == "cb_res":
                cb_id = env.get("cb_id")
                payload = env.get("payload")
                error = env.get("error")
                if cb_id:
                    _CallbackDispatcher.handle_response(cb_id, payload, error)

        self.executor.shutdown(wait=True)
        for cap in self.caps.values():
            cap.teardown()
        from cap import close_resources
        close_resources()
        if self._conn:
            self._conn.close()

    def _handle_cap_load(self, env: dict):
        cap_name = env.get("cap", "")
        error_msg = ""
        try:
            self.caps[cap_name] = load_capability(env)
        except Exception as exc:
            error_msg = str(exc)
            log.exception("cap_load %s failed", cap_name)

        self._send_envelope({
            "proto_ver": 1, "msg_type": "cap_ready", "worker_id": self.worker_id,
            "cap": cap_name, "cap_ver": env.get("cap_ver", ""),
            "setup_ms": 0, "error": error_msg,
        })

    def _handle_request_and_reply(self, env: dict):
        result_env = self._handle_request(env)
        if result_env:
            self._send_envelope(result_env)

    def _handle_request(self, env: dict) -> dict | None:
        cap_name = env.get("cap", "")
        loaded = self.caps.get(cap_name)
        if not loaded:
            return self._err(env, "CAP_NOT_FOUND", f"capability {cap_name!r} not loaded")

        origin_id = env.get("origin_id", "")
        with _cancelled_lock:
            if origin_id in _cancelled_ids:
                return self._err(env, "CANCELLED", "request cancelled")

        _corr_id_var.set(env.get("corr_id", ""))
        _origin_id_var.set(origin_id)
        _meta_var.set(dict(env.get("meta") or {}))
        _config_var.set(loaded.spec.get("config") or {})
        _delivery_var.set(env.get("delivery_count", 0))
        _session_var.set(env.get("_session_data") or {})

        try:
            payload_bytes = env.get("payload") or b""
            inputs = self._codec.unmarshal(payload_bytes) if payload_bytes else {}
            with loaded.semaphore:
                result = loaded.run(inputs)
            self.requests_served += 1

            # Handle pipe forward: if forward_to is set, publish there and return None
            forward_to = env.get("forward_to", "")
            if forward_to:
                fwd_env = {
                    "proto_ver": 1, "msg_type": "pipe",
                    "corr_id": env["corr_id"], "origin_id": origin_id,
                    "worker_id": self.worker_id, "cap": cap_name,
                    "hop": env.get("hop", 0) + 1,
                    "payload": self._codec.marshal(result) if result is not None else b"",
                    "meta": _meta_var.get(),
                }
                self._send_envelope(fwd_env)
                return None

            if result is None:
                return {}

            return {
                "proto_ver": 1, "msg_type": "res", "corr_id": env["corr_id"],
                "origin_id": origin_id, "worker_id": self.worker_id,
                "cap": cap_name, "cap_ver": loaded.spec.get("cap_ver", ""),
                "payload": self._codec.marshal(result), "meta": _meta_var.get(),
            }
        except Exception as exc:
            log.exception("exec error in %s", cap_name)
            return self._err(env, "EXEC_ERROR", str(exc))

    def _heartbeat_loop(self):
        interval = self.heartbeat_ms / 1000.0
        while self._running:
            time.sleep(interval)
            if not self._running:
                break
            try:
                self._send_envelope({
                    "proto_ver": 1, "msg_type": "hb_ping", "worker_id": self.worker_id,
                    "runtime": "python", "load": 0, "groups": self.groups,
                    "requests_served": self.requests_served,
                    "uptime_ms": int((time.time() - self.started_at) * 1000),
                })
            except OSError:
                break

    def _send_envelope(self, env: dict):
        if not env:
            return
        topic = _reply_topic(env)
        data = self._codec.marshal(env)
        frame = _encode_msg(topic, data)
        self._send_frame(frame)

    def _send_frame(self, data: bytes):
        with self._send_lock:
            self._conn.sendall(len(data).to_bytes(4, "big") + data)

    def _recv_frame(self) -> bytes | None:
        hdr = _recv_exact(self._conn, 4)
        if hdr is None:
            raise ConnectionResetError("router closed connection")
        size = int.from_bytes(hdr, "big")
        if size == 0:
            return None  # keepalive ping
        if size > 64 * 1024 * 1024:
            raise ValueError(f"frame too large: {size} bytes")
        return _recv_exact(self._conn, size)

    def _make_hello(self) -> dict:
        return {
            "proto_ver": 1, "msg_type": "worker_hello", "worker_id": self.worker_id,
            "runtime": "python", "pid": os.getpid(), "codec": _codec.name,
            "groups": self.groups, "caps": list(self.caps.keys()),
            "pipe_subs": [], "pipe_pubs": [],
            "cb_supported": True, "stream_supported": True,
        }

    def _err(self, env: dict, code: str, message: str) -> dict:
        return {
            "proto_ver": 1, "msg_type": "err", "corr_id": env.get("corr_id", ""),
            "origin_id": env.get("origin_id", ""), "worker_id": self.worker_id,
            "cap": env.get("cap", ""), "code": code, "message": message,
            "retryable": code in _RETRYABLE_CODES,
        }


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


def _encode_msg(topic: str, data: bytes) -> bytes:
    tb = topic.encode()
    return len(tb).to_bytes(2, "big") + tb + data


def _reply_topic(env: dict) -> str:
    msg_type = env.get("msg_type", "")
    if msg_type in ("res", "err", "res_chunk", "res_end"):
        return f"pepper.res.{env.get('origin_id', '')}"
    if msg_type == "hb_ping":
        return f"pepper.hb.{env.get('worker_id', '')}"
    if msg_type == "pipe":
        return env.get("topic", "pepper.pipe.unknown")
    return "pepper.control"


def _parse_bus_url(url: str) -> tuple[str, int]:
    url = url.strip()
    if url.startswith("tcp://"):
        addr = url[6:]
        host, _, port = addr.rpartition(":")
        return host or "127.0.0.1", int(port)
    if url.startswith("ipc://"):
        return "127.0.0.1", 7731
    raise ValueError(f"unsupported bus URL: {url!r}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    Worker().run()