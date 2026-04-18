package codec

import "github.com/goccy/go-json"

// JSON is available for debugging or tooling integration.
// It base64-encodes []byte fields, making it unsuitable for large binary payloads.
// Use MsgPack in production.
var JSON Codec = jsonCodec{}

type jsonCodec struct{}

func (jsonCodec) Name() string { return "json" }

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
