package codec

import "github.com/vmihailenco/msgpack/v5"

// MsgPack is the default Pepper codec.
// It handles []byte natively without base64, making it 2-10× smaller than JSON
// for AI payloads (audio, tensors, images).
var MsgPack Codec = msgpackCodec{}

type msgpackCodec struct{}

func (msgpackCodec) Name() string { return "msgpack" }

func (msgpackCodec) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (msgpackCodec) Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}
