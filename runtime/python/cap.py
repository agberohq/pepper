"""
pepper.cap — Python capability contract helpers.

Covers:
  - Input[T] / Output[T] type wrappers
  - @capability decorator (Form B)
  - @resource decorator and lifecycle
  - parse_form_a() — Form A header parser (spec §10.1)
  - read_blob() — zero-copy blob access (spec §12.4)
  - JSON schema extraction from run() signatures
"""
from __future__ import annotations

import functools
import re
import threading
import typing
from typing import Generator, Generic, TypeVar, get_type_hints, get_origin, get_args

T = TypeVar("T")

_resources: dict = {}
_resources_lock = threading.Lock()
_resource_gens: dict = {}


class Input(Generic[T]):
    """Annotate a run() parameter as a typed capability input."""
    pass


class Output(Generic[T]):
    """Annotate a run() return type."""
    pass


def capability(
        *,
        name: str,
        version: str = "1.0.0",
        deps: list | None = None,
        timeout: str = "30s",
        max_concurrent: int = 4,
        groups: list | None = None,
        pipe_pub: str | list | None = None,
        pipe_sub: str | list | None = None,
        resources: list | None = None,
        description: str = "",
):
    def decorator(cls):
        cls._pepper_capability = True
        cls._pepper_name = name
        cls._pepper_version = version
        cls._pepper_deps = deps or []
        cls._pepper_timeout = timeout
        cls._pepper_max_concurrent = max_concurrent
        cls._pepper_groups = groups or ["default"]
        cls._pepper_pipe_pub = ([pipe_pub] if isinstance(pipe_pub, str) else pipe_pub) or []
        cls._pepper_pipe_sub = ([pipe_sub] if isinstance(pipe_sub, str) else pipe_sub) or []
        cls._pepper_resources = resources or []
        cls._pepper_description = description
        cls._pepper_input_schema = _extract_input_schema(cls)
        cls._pepper_output_schema = _extract_output_schema(cls)

        # Inject declared resources as instance attributes before run()
        original_init = cls.__init__ if "__init__" in cls.__dict__ else None

        def __init__(self, *args, **kwargs):
            if original_init:
                original_init(self, *args, **kwargs)
            for res_name in cls._pepper_resources:
                setattr(self, res_name, get_resource(res_name))

        cls.__init__ = __init__
        return cls
    return decorator


def resource(*, name: str):
    """
    Decorator for shared connection pools. The decorated generator is started
    once per worker at boot. Yield returns the resource; cleanup after yield
    runs on worker shutdown via close_resource(name).
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(config: dict):
            gen = fn(config)
            instance = next(gen)
            with _resources_lock:
                _resources[name] = instance
                _resource_gens[name] = gen
            return instance

        wrapper._pepper_resource = True
        wrapper._pepper_resource_name = name
        return wrapper
    return decorator


def get_resource(name: str):
    with _resources_lock:
        return _resources.get(name)


def close_resources():
    """Call cleanup code for all resources. Invoked on worker_bye."""
    with _resources_lock:
        for name, gen in list(_resource_gens.items()):
            try:
                next(gen)
            except StopIteration:
                pass
            except Exception:
                pass
        _resource_gens.clear()
        _resources.clear()


# ── Form A parser (spec §10.1) ────────────────────────────────────────────────

_HEADER_RE = re.compile(r"^#\s*pepper:(\w+)\s*=\s*(.+)$")


def parse_form_a(source: str) -> dict:
    """
    Parse Form A magic comment header from a Python source string.

    Reads # pepper:key = value lines from the contiguous header block at the
    top of the file — stops at the first non-comment, non-whitespace,
    non-shebang line (def, class, import, etc.). This prevents misreading
    # pepper: strings inside function bodies or docstrings.
    """
    meta = {}
    for line in source.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#!"):
            continue
        m = _HEADER_RE.match(stripped)
        if m:
            meta[m.group(1)] = m.group(2).strip()
        elif stripped.startswith("#"):
            continue
        else:
            break
    return meta


# ── Blob reader (spec §12.4) ──────────────────────────────────────────────────

class BlobRef:
    """
    Zero-copy access to a pepper blob. Workers receive a blob reference dict
    with _pepper_blob=True and call read_blob(ref) to get this object.
    """

    def __init__(self, ref: dict):
        self._ref = ref
        self._path: str = ref.get("path", "")
        self._size: int = ref.get("size", 0)
        self._dtype: str = ref.get("dtype", "")
        self._shape: list = ref.get("shape", [])
        self._format: str = ref.get("format", "")

    @property
    def id(self) -> str:
        return self._ref.get("id", "")

    @property
    def path(self) -> str:
        return self._path

    @property
    def size(self) -> int:
        return self._size

    @property
    def dtype(self) -> str:
        return self._dtype

    @property
    def shape(self) -> list:
        return self._shape

    @property
    def format(self) -> str:
        return self._format

    def as_bytes(self) -> bytes:
        """Read blob as raw bytes (one copy)."""
        with open(self._path, "rb") as f:
            return f.read()

    def as_numpy(self):
        """Return numpy array backed by mmap — zero copy when dtype/shape set."""
        import numpy as np
        if self._dtype and self._shape:
            return np.memmap(self._path, dtype=self._dtype, mode="r",
                             shape=tuple(self._shape))
        return np.frombuffer(self.as_bytes(), dtype=self._dtype or "uint8")

    def as_cv2(self):
        """Return cv2 image (BGR). Requires opencv-python."""
        import cv2
        import numpy as np
        data = np.frombuffer(self.as_bytes(), dtype=np.uint8)
        return cv2.imdecode(data, cv2.IMREAD_COLOR)

    def as_torch(self):
        """Return torch tensor. Requires torch."""
        import torch
        import numpy as np
        return torch.from_numpy(np.array(self.as_numpy()))


def read_blob(ref: dict) -> BlobRef:
    """
    Convert a blob reference dict (with _pepper_blob=True) to a BlobRef.

    Usage:
        img = read_blob(inputs["image"]).as_numpy()
    """
    if not isinstance(ref, dict) or not ref.get("_pepper_blob"):
        raise ValueError("read_blob: argument is not a pepper blob reference")
    return BlobRef(ref)


# ── Schema extraction ─────────────────────────────────────────────────────────

def _extract_input_schema(cls) -> dict:
    run = getattr(cls, "run", None)
    if run is None:
        return {"type": "object", "properties": {}}

    try:
        hints = get_type_hints(run)
    except Exception:
        return {"type": "object", "properties": {}}

    import inspect
    params = list(inspect.signature(run).parameters.items())
    params = [(n, p) for n, p in params if n != "self"]

    if len(params) == 1:
        _, p = params[0]
        ann = hints.get(p.name)
        if ann is not None and _is_pydantic(ann):
            return _pydantic_to_schema(ann)

    properties: dict[str, dict] = {}
    required: list[str] = []
    for param_name, param in params:
        ann = hints.get(param_name)
        if ann is None:
            continue
        prop_schema = _annotation_to_json_schema(ann)
        properties[param_name] = prop_schema
        if param.default is inspect.Parameter.empty:
            required.append(param_name)

    schema: dict = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _extract_output_schema(cls) -> dict:
    run = getattr(cls, "run", None)
    if run is None:
        return {}
    try:
        hints = get_type_hints(run)
        ret = hints.get("return")
        if ret is not None:
            return _annotation_to_json_schema(ret)
    except Exception:
        pass
    return {}


def _unwrap_generic(ann):
    """Unwrap Input[T], Output[T], Optional[T], List[T] to inner type."""
    origin = get_origin(ann)
    if origin is not None:
        args = get_args(ann)
        if origin is Input and args:
            return args[0]
        if origin is Output and args:
            return args[0]
        if origin is typing.Union and len(args) == 2 and type(None) in args:
            # Optional[T]
            return args[0] if args[1] is type(None) else args[1]
        if origin in (list, typing.List) and args:
            return {"type": "array", "items": _annotation_to_json_schema(args[0])}
        if origin in (dict, typing.Dict) and len(args) == 2:
            return {"type": "object"}
    return ann


def _annotation_to_json_schema(ann) -> dict:
    ann = _unwrap_generic(ann)

    if isinstance(ann, dict):
        return ann

    if ann is bytes:
        return {"type": "string", "format": "binary"}
    if ann is str:
        return {"type": "string"}
    if ann is int:
        return {"type": "integer"}
    if ann is float:
        return {"type": "number"}
    if ann is bool:
        return {"type": "boolean"}

    if isinstance(ann, type):
        if issubclass(ann, dict):
            return {"type": "object"}
        if issubclass(ann, list):
            return {"type": "array"}
        if issubclass(ann, bytes):
            return {"type": "string", "format": "binary"}
        if issubclass(ann, str):
            return {"type": "string"}
        if issubclass(ann, int):
            return {"type": "integer"}
        if issubclass(ann, float):
            return {"type": "number"}
        if issubclass(ann, bool):
            return {"type": "boolean"}

    if _is_pydantic(ann):
        return _pydantic_to_schema(ann)

    return {}


def _is_pydantic(ann) -> bool:
    try:
        from pydantic import BaseModel
        return isinstance(ann, type) and issubclass(ann, BaseModel)
    except ImportError:
        return False


def _pydantic_to_schema(model) -> dict:
    try:
        return model.model_json_schema()
    except Exception:
        try:
            return model.schema()
        except Exception:
            return {"type": "object"}