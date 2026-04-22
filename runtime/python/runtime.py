"""
runtime.py — Pepper Python worker runtime.
"""
from __future__ import annotations

import sys
if __name__ == "__main__":
    sys.modules["runtime"] = sys.modules[__name__]
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
from typing import Any, Dict
import socket

# ── Logging — structured JSON to fd 3 (Go reads it), fallback to stderr ──────
# Python MUST NOT write human-readable text to stderr when stdout is used by
# a CLI capability (e.g. ffmpeg pipe). Go opens fd 3 on the subprocess for
# structured log lines; if fd 3 is not available we fall back to stderr.
# Format: one JSON object per line with fields: ts, level, logger, msg, **extra

class _StructuredHandler(logging.Handler):
    """Emits one JSON line per log record to fd 3 or stderr."""

    def __init__(self):
        super().__init__()
        try:
            self._fd = open(3, "w", buffering=1, closefd=False)  # fd 3 = Go log pipe
        except OSError:
            self._fd = sys.stderr  # fallback: fd 3 not provided (local dev / tests)

    def emit(self, record: logging.LogRecord) -> None:  # type: ignore[override]
        try:
            entry = {
                "ts": record.created,
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
            }
            if record.exc_info:
                entry["exc"] = self.formatException(record.exc_info)
            print(json.dumps(entry, separators=(",", ":")), file=self._fd, flush=True)
        except Exception:  # noqa: BLE001
            pass  # never crash the worker because of a log failure


_handler = _StructuredHandler()
logging.root.addHandler(_handler)
logging.root.setLevel(logging.DEBUG if os.environ.get("PEPPER_LOG_DEBUG") else logging.INFO)

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

    @staticmethod
    def kv(namespace: str = "default") -> "_KVStore":
        """Return a shared key-value store scoped to a namespace.

        The KV store is process-local (all caps on this worker share it).
        Use it to cache expensive objects (models, tokenizers, connections)
        across requests without re-loading them each time.

        Example::

            # In setup():
            pepper.kv("models").set("whisper", WhisperModel("tiny"))

            # In run():
            model = pepper.kv("models").get("whisper")
        """
        return _KVStore.for_namespace(namespace)

    @staticmethod
    def cache_dir() -> str:
        """Return the canonical Pepper model/data cache directory.

        Resolves in priority order:
          1. ``PEPPER_CACHE_DIR`` environment variable
          2. ``HF_HOME`` environment variable (HuggingFace compatibility)
          3. ``~/.cache/pepper``

        Capabilities should use this instead of hardcoding cache paths so
        that operators can redirect the cache with a single env var.

        Example::

            model = WhisperModel("tiny", download_root=pepper.cache_dir())
        """
        return _resolve_cache_dir()


def _resolve_cache_dir() -> str:
    """Resolve the canonical Pepper cache directory (module-level helper)."""
    d = (
        os.environ.get("PEPPER_CACHE_DIR")
        or os.environ.get("HF_HOME")
        or os.path.join(os.path.expanduser("~"), ".cache", "pepper")
    )
    os.makedirs(d, exist_ok=True)
    return d


class _KVStore:
    """Process-local key-value store. One instance per namespace, shared across all caps.

    Keys are stored under the composite key  ``{worker_id}:{namespace}:{user_key}``
    so that each worker process has a clearly isolated KV space. This prevents
    silent key collisions when multiple workers run on the same host (e.g. during
    tests) and makes it trivial to audit or evict a single worker's state.
    """

    _stores: dict[str, "_KVStore"] = {}
    _lock = threading.Lock()
    # Populated at Worker.run() time so the prefix is available before any cap
    # calls pepper.kv(). Falls back to "" in unit-test contexts without a worker.
    _worker_id: str = os.environ.get("PEPPER_WORKER_ID", "")

    def __init__(self, namespace: str) -> None:
        self._namespace = namespace
        self._data: dict[str, Any] = {}
        self._rw = threading.RLock()

    def _full_key(self, key: str) -> str:
        """Return the internal storage key, prefixed with worker_id and namespace."""
        prefix = self._worker_id
        return f"{prefix}:{self._namespace}:{key}" if prefix else f"{self._namespace}:{key}"

    @classmethod
    def for_namespace(cls, namespace: str) -> "_KVStore":
        # Incorporate the worker_id into the store-map key so that if (in a
        # future scenario) two workers share a process, they each get an
        # independent store object.
        store_key = f"{cls._worker_id}:{namespace}" if cls._worker_id else namespace
        with cls._lock:
            if store_key not in cls._stores:
                cls._stores[store_key] = cls(namespace)
            return cls._stores[store_key]

    def get(self, key: str, default: Any = None) -> Any:
        with self._rw:
            return self._data.get(self._full_key(key), default)

    def set(self, key: str, value: Any) -> None:
        with self._rw:
            self._data[self._full_key(key)] = value

    def delete(self, key: str) -> None:
        with self._rw:
            self._data.pop(self._full_key(key), None)

    def get_or_set(self, key: str, factory) -> Any:
        """Return cached value, or call factory() once and cache the result.

        Thread-safe: factory is called at most once per key even under
        concurrent requests. Ideal for lazy model loading::

            model = pepper.kv("models").get_or_set(
                "whisper-tiny",
                lambda: WhisperModel("tiny", download_root=pepper.cache_dir()),
            )
        """
        fk = self._full_key(key)
        with self._rw:
            if fk in self._data:
                return self._data[fk]
            value = factory()
            self._data[fk] = value
            return value

    def keys(self) -> list[str]:
        """Return user-visible keys (without the internal worker:namespace: prefix)."""
        prefix = self._full_key("")  # "worker_id:namespace:"
        with self._rw:
            return [k[len(prefix):] for k in self._data if k.startswith(prefix)]

    def clear(self) -> None:
        """Remove all keys belonging to this namespace on this worker."""
        prefix = self._full_key("")
        with self._rw:
            for k in [k for k in self._data if k.startswith(prefix)]:
                del self._data[k]

    def __repr__(self) -> str:
        with self._rw:
            return f"_KVStore(namespace={self._namespace!r}, keys={list(self._data)!r})"

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
        def _default(o):
            if isinstance(o, bytes):
                return o.decode("utf-8", errors="replace")
            raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")
        return json.dumps(v, default=_default).encode()
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
from transport import BusTransport  # noqa: E402


def _parse_bus_url(url: str) -> tuple[str, int]:
    """Parse a bus URL into (host, port). Thin wrapper over BusTransport._parse."""
    _, host, port = BusTransport._parse(url)
    return host, port


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
    # Inject the pepper API into the module's namespace before executing it so
    # that top-level code and setup() can call pepper.kv(), pepper.cache_dir(),
    # pepper.config(), etc. Without this, any cap that references `pepper` at
    # module load time raises NameError: name 'pepper' is not defined.
    module.__dict__["pepper"] = pepper
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


# ── Stream registry ───────────────────────────────────────────────────────────
# Tracks active bidirectional streams opened via pp.OpenStream().
# Each entry is a queue that the message loop feeds with incoming chunks;
# the capability reads from it via pepper.stream_chunks() (or a generator).

class _StreamState:
    def __init__(self):
        self.queue: queue.Queue = queue.Queue()
        self.closed = threading.Event()

    def feed(self, payload: bytes) -> None:
        self.queue.put(payload)

    def close_input(self) -> None:
        self.closed.set()
        self.queue.put(None)  # sentinel to unblock readers

    def chunks(self):
        """Yield decoded payloads until the input stream is closed."""
        while True:
            item = self.queue.get()
            if item is None:
                return
            yield item


class _StreamRegistry:
    _streams: Dict[str, _StreamState] = {}
    _lock = threading.Lock()

    @classmethod
    def open(cls, stream_id: str) -> _StreamState:
        state = _StreamState()
        with cls._lock:
            cls._streams[stream_id] = state
        return state

    @classmethod
    def feed(cls, stream_id: str, payload: bytes) -> None:
        with cls._lock:
            state = cls._streams.get(stream_id)
        if state:
            state.feed(payload)

    @classmethod
    def close(cls, stream_id: str) -> None:
        with cls._lock:
            state = cls._streams.pop(stream_id, None)
        if state:
            state.close_input()

    @classmethod
    def remove(cls, stream_id: str) -> None:
        with cls._lock:
            cls._streams.pop(stream_id, None)


import queue  # noqa: E402 — placed here to keep top imports clean


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
        self._transport: BusTransport | None = None
        self._codec = _make_codec()  # read PEPPER_CODEC at construction time

    def run(self):
        log.info("pepper worker %s starting, bus=%s", self.worker_id, self.bus_url)
        self._transport = BusTransport(self.bus_url)
        self._transport.connect()
        # raw_conn is only set for Mula; None in distributed mode.
        self._conn = self._transport.raw_conn

        _PipeForwarder.init(self._conn)
        _CallbackDispatcher.init(self)

        # Build the list of topics this worker cares about.
        # BusTransport.subscribe() handles the protocol difference:
        #   Mula:  sends a pipe-separated topic list as the first frame,
        #          which Mula's handleConn uses to set up push queues.
        #   Redis: PSUBSCRIBE for control/broadcast, BRPOP loop for push topics.
        #   NATS:  queue-group SUB for push topics, plain SUB for others.
        topics = ["pepper.control", "pepper.broadcast"]
        if self.worker_id:
            topics.append(f"pepper.control.{self.worker_id}")
            topics.append(f"pepper.push.{self.worker_id}")
        for g in self.groups:
            topics.append(f"pepper.push.{g}")
            topics.append(f"pepper.pub.{g}")

        self._transport.subscribe(topics)

        # Send worker_hello so the router registers this worker.
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
                log.debug("req received [cap=%s corr_id=%s worker=%s hop=%s]",
                          env.get("cap", ""), env.get("corr_id", ""),
                          self.worker_id, env.get("hop", 0))
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
            elif msg_type == "stream_open":
                # A new bidirectional stream has been opened (§11.2).
                # Dispatch the capability in streaming mode; the cap returns a
                # generator and we forward each yielded value as res_chunk.
                self.executor.submit(self._handle_stream_open, env)
            elif msg_type == "stream_chunk":
                # An input chunk arriving on an existing stream.
                stream_id = env.get("stream_id", "")
                if stream_id:
                    _StreamRegistry.feed(stream_id, env.get("payload") or b"")
            elif msg_type == "stream_close":
                stream_id = env.get("stream_id", "")
                if stream_id:
                    _StreamRegistry.close(stream_id)

        self.executor.shutdown(wait=True)
        for cap in self.caps.values():
            cap.teardown()
        from cap import close_resources
        close_resources()
        if hasattr(self, "_transport"):
            self._transport.close()

    def _handle_cap_load(self, env: dict):
        cap_name = env.get("cap", "")
        error_msg = ""
        t_start = time.monotonic()
        log.info("cap_load start [cap=%s worker=%s]", cap_name, self.worker_id)
        try:
            self.caps[cap_name] = load_capability(env)
        except Exception as exc:
            error_msg = str(exc)
            log.exception("cap_load failed [cap=%s worker=%s error=%s]", cap_name, self.worker_id, exc)

        setup_ms = int((time.monotonic() - t_start) * 1000)
        if not error_msg:
            log.info("cap_load done [cap=%s worker=%s setup_ms=%d]", cap_name, self.worker_id, setup_ms)

        self._send_envelope({
            "proto_ver": 1, "msg_type": "cap_ready", "worker_id": self.worker_id,
            "cap": cap_name, "cap_ver": env.get("cap_ver", ""),
            "setup_ms": setup_ms, "error": error_msg,
        })

    def _handle_request_and_reply(self, env: dict):
        result_env = self._handle_request(env)
        if result_env:
            self._send_envelope(result_env)

    def _handle_stream_open(self, env: dict):
        """Handle stream_open: run the capability in streaming mode (§11.2).

        If the capability's run() returns a generator, each yielded value is
        forwarded as a res_chunk. A final res_end closes the stream.
        If run() returns a plain dict, it is sent as a single res_chunk + res_end.
        """
        cap_name = env.get("cap", "")
        stream_id = env.get("stream_id", "")
        corr_id = env.get("corr_id", "")
        origin_id = env.get("origin_id", "")

        loaded = self.caps.get(cap_name)
        if not loaded:
            self._send_envelope(self._err(env, "CAP_NOT_FOUND",
                                          f"capability {cap_name!r} not loaded"))
            return

        _corr_id_var.set(corr_id)
        _origin_id_var.set(origin_id)
        _meta_var.set(dict(env.get("meta") or {}))
        _config_var.set(loaded.spec.get("config") or {})

        # Register the stream so incoming stream_chunk messages are queued.
        stream_state = _StreamRegistry.open(stream_id)

        payload_bytes = env.get("payload") or b""
        inputs = self._codec.unmarshal(payload_bytes) if payload_bytes else {}
        # Inject stream chunk iterator so the cap can consume input chunks.
        inputs["_stream_chunks"] = stream_state.chunks()

        def _send_chunk(result: dict):
            self._send_envelope({
                "proto_ver": 1, "msg_type": "res_chunk",
                "corr_id": corr_id, "origin_id": origin_id,
                "worker_id": self.worker_id, "cap": cap_name,
                "stream_id": stream_id,
                "payload": self._codec.marshal(result),
            })

        def _send_end():
            self._send_envelope({
                "proto_ver": 1, "msg_type": "res_end",
                "corr_id": corr_id, "origin_id": origin_id,
                "worker_id": self.worker_id, "cap": cap_name,
                "stream_id": stream_id,
            })

        try:
            with loaded.semaphore:
                result = loaded.run(inputs)

            if inspect.isgenerator(result):
                for chunk in result:
                    with _cancelled_lock:
                        if origin_id in _cancelled_ids:
                            break
                    if isinstance(chunk, dict):
                        _send_chunk(chunk)
                    else:
                        _send_chunk({"value": chunk})
            elif isinstance(result, dict):
                _send_chunk(result)

            _send_end()
        except Exception as exc:
            log.exception("stream exec error in %s", cap_name)
            self._send_envelope(self._err(env, "EXEC_ERROR", str(exc)))
        finally:
            _StreamRegistry.remove(stream_id)

    def _handle_request(self, env: dict) -> dict | None:
        cap_name = env.get("cap", "")
        origin_id = env.get("origin_id", "")
        corr_id = env.get("corr_id", "")

        # ── pepper.inline: raw snippet execution (§16.3) ──────────────────────
        # The router sends cap="pepper.inline" with _snippet in the payload.
        # We handle it here natively — it is never registered via cap_load.
        if cap_name == "pepper.inline":
            payload_bytes = env.get("payload") or b""
            inputs = self._codec.unmarshal(payload_bytes) if payload_bytes else {}
            snippet = inputs.pop("_snippet", "")
            _corr_id_var.set(corr_id)
            _origin_id_var.set(origin_id)
            _meta_var.set(dict(env.get("meta") or {}))
            try:
                local_scope = dict(inputs)
                exec(snippet, {}, local_scope)  # noqa: S102
                result = local_scope.get("result", {k: v for k, v in local_scope.items()
                                                    if not k.startswith("_")})
                if not isinstance(result, dict):
                    result = {"result": result}
                return {
                    "proto_ver": 1, "msg_type": "res",
                    "corr_id": corr_id,
                    "origin_id": origin_id, "worker_id": self.worker_id,
                    "cap": cap_name, "cap_ver": "",
                    "payload": self._codec.marshal(result),
                    "meta": _meta_var.get(),
                }
            except Exception as exc:
                log.exception("pepper.inline exec error")
                return self._err(env, "EXEC_ERROR", str(exc))

        loaded = self.caps.get(cap_name)
        if not loaded:
            log.error("cap not found [cap=%s corr_id=%s worker=%s]", cap_name, corr_id, self.worker_id)
            return self._err(env, "CAP_NOT_FOUND", f"capability {cap_name!r} not loaded")

        with _cancelled_lock:
            if origin_id in _cancelled_ids:
                log.info("request cancelled before exec [cap=%s corr_id=%s]", cap_name, corr_id)
                return self._err(env, "CANCELLED", "request cancelled")

        _corr_id_var.set(corr_id)
        _origin_id_var.set(origin_id)
        _meta_var.set(dict(env.get("meta") or {}))
        _config_var.set(loaded.spec.get("config") or {})
        _delivery_var.set(env.get("delivery_count", 0))
        # Session data is injected by the Go runtime into envelope meta under
        # "_session_data" (not at the top level) so workers can read prior state.
        _session_var.set((env.get("meta") or {}).get("_session_data") or {})

        t_start = time.monotonic()
        log.info("exec start [cap=%s corr_id=%s worker=%s hop=%s]",
                 cap_name, corr_id, self.worker_id, env.get("hop", 0))

        try:
            payload_bytes = env.get("payload") or b""
            inputs = self._codec.unmarshal(payload_bytes) if payload_bytes else {}
            with loaded.semaphore:
                result = loaded.run(inputs)
            self.requests_served += 1
            duration_ms = int((time.monotonic() - t_start) * 1000)

            # If cancelled while running, discard result and return CANCELLED.
            with _cancelled_lock:
                if origin_id in _cancelled_ids:
                    log.info("request cancelled after exec [cap=%s corr_id=%s duration_ms=%d]",
                             cap_name, corr_id, duration_ms)
                    return self._err(env, "CANCELLED", "request cancelled")

            # Handle pipe forward: if forward_to is set, publish there and return None
            forward_to = env.get("forward_to", "")
            if forward_to:
                log.info("exec done — forwarding [cap=%s corr_id=%s duration_ms=%d forward_to=%s]",
                         cap_name, corr_id, duration_ms, forward_to)
                fwd_env = {
                    "proto_ver": 1, "msg_type": "pipe",
                    "corr_id": corr_id, "origin_id": origin_id,
                    "worker_id": self.worker_id, "cap": cap_name,
                    "hop": env.get("hop", 0) + 1,
                    "forward_to": forward_to,
                    "topic": forward_to,
                    "payload": self._codec.marshal(result) if result is not None else b"",
                    "meta": _meta_var.get(),
                }
                self._send_envelope(fwd_env)
                return None

            if result is None:
                log.info("exec done — no result [cap=%s corr_id=%s duration_ms=%d]",
                         cap_name, corr_id, duration_ms)
                return {}

            log.info("exec done [cap=%s corr_id=%s worker=%s duration_ms=%d]",
                     cap_name, corr_id, self.worker_id, duration_ms)
            return {
                "proto_ver": 1, "msg_type": "res", "corr_id": corr_id,
                "origin_id": origin_id, "worker_id": self.worker_id,
                "cap": cap_name, "cap_ver": loaded.spec.get("cap_ver", ""),
                "payload": self._codec.marshal(result),
                # Echo session_id back so Go's routeResponse can find and merge
                # the _session_updates written by pepper.session().set().
                "session_id": env.get("session_id", ""),
                "meta": _meta_var.get(),
            }
        except Exception as exc:
            duration_ms = int((time.monotonic() - t_start) * 1000)
            log.exception("exec error [cap=%s corr_id=%s worker=%s duration_ms=%d error=%s]",
                          cap_name, corr_id, self.worker_id, duration_ms, exc)
            return self._err(env, "EXEC_ERROR", str(exc))

    def _heartbeat_loop(self):
        interval = self.heartbeat_ms / 1000.0
        while self._running:
            time.sleep(interval)
            if not self._running:
                break
            self._send_envelope({
                "proto_ver": 1, "msg_type": "hb_ping", "worker_id": self.worker_id,
                "runtime": "python", "load": 0, "groups": self.groups,
                "requests_served": self.requests_served,
                "uptime_ms": int((time.time() - self.started_at) * 1000),
            })

    def _send_envelope(self, env: dict):
        if not env:
            return
        topic = _reply_topic(env)
        data = self._codec.marshal(env)
        frame = _encode_msg(topic, data)
        self._send_frame(frame)

    def _send_frame(self, data: bytes):
        self._transport.send_frame(data)

    def _recv_frame(self) -> bytes | None:
        return self._transport.recv_frame()

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

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    Worker().run()