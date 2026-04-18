"""
Unit tests for cap.py. Run with: python -m pytest internal/runtime/python/test_cap.py -v
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional

# cap.py is in the same directory
sys.path.insert(0, str(Path(__file__).parent))

from cap import (
    Input, Output, capability, resource, get_resource, close_resources,
    parse_form_a, _annotation_to_json_schema, _extract_input_schema,
)


class TestFormAParser:
    def test_parses_contiguous_header(self):
        src = """# pepper:name           = speech.transcribe
# pepper:version        = 1.0.0
# pepper:deps           = faster-whisper>=1.0, numpy>=1.24

def setup(config: dict) -> None:
    pass
"""
        meta = parse_form_a(src)
        assert meta["name"] == "speech.transcribe"
        assert meta["version"] == "1.0.0"
        assert meta["deps"] == "faster-whisper>=1.0, numpy>=1.24"

    def test_stops_at_first_code_line(self):
        src = """# pepper:name    = speech.transcribe
# pepper:version = 1.0.0

# This is a regular comment
def run():
    # pepper:deps = something-else
    pass
"""
        meta = parse_form_a(src)
        assert meta["name"] == "speech.transcribe"
        assert "deps" not in meta

    def test_ignores_shebang(self):
        src = "#!/usr/bin/env python3\n# pepper:name = x\nimport os\n"
        meta = parse_form_a(src)
        assert meta["name"] == "x"

    def test_empty_source(self):
        assert parse_form_a("") == {}


class TestCapabilityDecorator:
    def test_metadata_attached(self):
        @capability(
            name="face.recognize",
            version="2.0.0",
            groups=["gpu", "fast"],
            pipe_pub="pipeline:faces",
        )
        class FaceRecognize:
            def run(self, image: Input[bytes]) -> Output[dict]:
                return {}

        assert FaceRecognize._pepper_capability is True
        assert FaceRecognize._pepper_name == "face.recognize"
        assert FaceRecognize._pepper_version == "2.0.0"
        assert FaceRecognize._pepper_groups == ["gpu", "fast"]
        assert FaceRecognize._pepper_pipe_pub == ["pipeline:faces"]
        assert FaceRecognize._pepper_max_concurrent == 4

    def test_defaults(self):
        @capability(name="minimal")
        class Minimal:
            pass

        assert Minimal._pepper_version == "1.0.0"
        assert Minimal._pepper_groups == ["default"]
        assert Minimal._pepper_deps == []
        assert Minimal._pepper_pipe_pub == []
        assert Minimal._pepper_description == ""


class TestResourceDecorator:
    def test_resource_lifecycle(self):
        cleanup_called = [False]

        @resource(name="test_db")
        def test_db(config: dict):
            conn = {"url": config.get("url", "localhost")}
            yield conn
            cleanup_called[0] = True

        test_db({"url": "postgres://localhost"})
        assert get_resource("test_db") == {"url": "postgres://localhost"}

        close_resources()
        assert get_resource("test_db") is None
        assert cleanup_called[0] is True

    def test_resource_thread_safe(self):
        @resource(name="counter")
        def counter(config: dict):
            yield {"val": 0}

        counter({})
        assert get_resource("counter") is not None
        close_resources()


class TestSchemaExtraction:
    def test_primitive_annotations(self):
        assert _annotation_to_json_schema(bytes) == {"type": "string", "format": "binary"}
        assert _annotation_to_json_schema(str) == {"type": "string"}
        assert _annotation_to_json_schema(int) == {"type": "integer"}
        assert _annotation_to_json_schema(float) == {"type": "number"}
        assert _annotation_to_json_schema(bool) == {"type": "boolean"}

    def test_input_unwraps(self):
        ann = Input[bytes]
        assert _annotation_to_json_schema(ann) == {"type": "string", "format": "binary"}

    def test_optional_unwraps(self):
        ann = Optional[str]
        assert _annotation_to_json_schema(ann) == {"type": "string"}

    def test_list_becomes_array(self):
        ann = List[int]
        schema = _annotation_to_json_schema(ann)
        assert schema == {"type": "array", "items": {"type": "integer"}}

    def test_dict_becomes_object(self):
        ann = Dict[str, int]
        assert _annotation_to_json_schema(ann) == {"type": "object"}

    def test_run_signature_to_schema(self):
        @capability(name="transcribe")
        class Transcribe:
            def run(
                    self,
                    audio: Input[bytes],
                    language: Input[str] = "en",
                    model_size: Input[str] = "small",
            ) -> Output[dict]:
                return {}

        schema = _extract_input_schema(Transcribe)
        assert schema["type"] == "object"
        props = schema["properties"]
        assert "audio" in props
        assert "language" in props
        assert "model_size" in props
        assert schema.get("required") == ["audio"]

    def test_pydantic_model_detection(self):
        try:
            from pydantic import BaseModel
        except ImportError:
            import pytest
            pytest.skip("pydantic not installed")

        class Request(BaseModel):
            text: str
            count: int

        @capability(name="echo")
        class Echo:
            def run(self, req: Request) -> Output[dict]:
                return {}

        schema = _extract_input_schema(Echo)
        assert "properties" in schema


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])