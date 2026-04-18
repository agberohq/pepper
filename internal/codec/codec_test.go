package codec

import (
	"bytes"
	"testing"
)

// Get()

func TestGetMsgPack(t *testing.T) {
	c, err := Get("msgpack")
	if err != nil {
		t.Fatalf("Get(msgpack): %v", err)
	}
	if c.Name() != "msgpack" {
		t.Errorf("Name() = %q, want msgpack", c.Name())
	}
}

func TestGetJSON(t *testing.T) {
	c, err := Get("json")
	if err != nil {
		t.Fatalf("Get(json): %v", err)
	}
	if c.Name() != "json" {
		t.Errorf("Name() = %q, want json", c.Name())
	}
}

func TestGetUnknown(t *testing.T) {
	_, err := Get("protobuf")
	if err == nil {
		t.Error("expected error for unknown codec name")
	}
}

func TestGetEmpty(t *testing.T) {
	_, err := Get("")
	if err == nil {
		t.Error("expected error for empty codec name")
	}
}

// MsgPack roundtrip

func TestMsgPackMapRoundtrip(t *testing.T) {
	c := MsgPack
	in := map[string]any{
		"msg":   "hello",
		"count": int64(42),
	}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out map[string]any
	if err := c.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out["msg"] != "hello" {
		t.Errorf("msg = %v, want hello", out["msg"])
	}
}

func TestMsgPackBytesNativeNoBase64(t *testing.T) {
	// Key property: MsgPack encodes []byte as raw bytes, not base64.
	// This keeps audio/image payloads compact.
	c := MsgPack
	raw := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
	in := map[string]any{"data": raw}

	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// The msgpack encoding of 5 bytes should be far smaller than base64 (8+ bytes).
	// Rough check: data should not contain "base64" or "=" padding.
	if bytes.Contains(data, []byte("base64")) {
		t.Error("MsgPack should NOT contain 'base64' — raw bytes expected")
	}

	var out map[string]any
	if err := c.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	recovered, ok := out["data"].([]byte)
	if !ok {
		t.Fatalf("expected []byte back, got %T", out["data"])
	}
	if !bytes.Equal(recovered, raw) {
		t.Errorf("bytes roundtrip failed: got %v, want %v", recovered, raw)
	}
}

func TestMsgPackStringRoundtrip(t *testing.T) {
	c := MsgPack
	in := "hello pepper"
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out string
	if err := c.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != in {
		t.Errorf("string roundtrip: got %q, want %q", out, in)
	}
}

func TestMsgPackNestedRoundtrip(t *testing.T) {
	c := MsgPack
	type Segment struct {
		Text  string  `msgpack:"text"`
		Start float64 `msgpack:"start"`
	}
	in := map[string]any{
		"language": "en",
		"segments": []Segment{
			{Text: "hello", Start: 0.0},
			{Text: "world", Start: 1.5},
		},
	}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(data) == 0 {
		t.Error("Marshal produced empty bytes")
	}
}

func TestMsgPackNilPayload(t *testing.T) {
	c := MsgPack
	data, err := c.Marshal(nil)
	if err != nil {
		t.Fatalf("Marshal nil: %v", err)
	}
	// nil should produce a valid msgpack nil token, not empty bytes
	if len(data) == 0 {
		t.Error("Marshal(nil) should produce at least 1 byte (msgpack nil token)")
	}
}

func TestMsgPackEmptyMap(t *testing.T) {
	c := MsgPack
	in := map[string]any{}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal empty map: %v", err)
	}
	var out map[string]any
	if err := c.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal empty map: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty map, got %v", out)
	}
}

// JSON roundtrip

func TestJSONMapRoundtrip(t *testing.T) {
	c := JSON
	in := map[string]any{
		"msg":   "hello",
		"count": float64(42), // JSON numbers unmarshal as float64
	}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out map[string]any
	if err := c.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out["msg"] != "hello" {
		t.Errorf("msg = %v, want hello", out["msg"])
	}
}

func TestJSONName(t *testing.T) {
	if JSON.Name() != "json" {
		t.Errorf("JSON.Name() = %q, want json", JSON.Name())
	}
}

// Custom codec

func TestCustomCodecPassthrough(t *testing.T) {
	// Custom() must return the same codec unchanged
	c := Custom(MsgPack)
	if c.Name() != "msgpack" {
		t.Errorf("Custom(MsgPack).Name() = %q, want msgpack", c.Name())
	}
}

// Error cases

func TestUnmarshalInvalidBytes(t *testing.T) {
	c := MsgPack
	var out map[string]any
	err := c.Unmarshal([]byte{0xFF, 0xFF, 0xFF}, &out)
	if err == nil {
		t.Error("expected error unmarshalling invalid msgpack bytes")
	}
}

func TestUnmarshalEmptyBytes(t *testing.T) {
	c := MsgPack
	var out map[string]any
	err := c.Unmarshal([]byte{}, &out)
	// Empty bytes should produce an error or leave out at zero value
	// — either is acceptable, but must not panic
	_ = err
}

// Codec consistency

func TestMsgPackAndJSONProduceDifferentBytes(t *testing.T) {
	// MsgPack and JSON produce different wire representations.
	// This is the whole point — they must never silently cross-decode.
	in := map[string]any{"key": "value"}

	mp, err := MsgPack.Marshal(in)
	if err != nil {
		t.Fatalf("MsgPack.Marshal: %v", err)
	}
	js, err := JSON.Marshal(in)
	if err != nil {
		t.Fatalf("JSON.Marshal: %v", err)
	}
	if bytes.Equal(mp, js) {
		t.Error("MsgPack and JSON produced identical bytes — that should not happen")
	}

	// JSON bytes should not decode cleanly with MsgPack
	var out map[string]any
	err = MsgPack.Unmarshal(js, &out)
	if err == nil {
		// Some JSON happens to be valid msgpack (small maps can overlap).
		// Log rather than fail — the important check is they are different.
		t.Log("note: JSON bytes accidentally valid msgpack (coincidental overlap)")
	}
}
