package codec

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgPack is the default Pepper codec.
// It handles []byte natively without base64, making it 2-10× smaller than JSON
// for AI payloads (audio, tensors, images).
//
// # Struct encoding: maps, not arrays
//
// vmihailenco/msgpack/v5 encodes Go structs as positional msgpack arrays by
// default. That format is opaque to any receiver that is not also using the
// same Go struct definition — in particular the Python worker, which decodes
// every message into a plain dict and accesses fields by name
// (env.get("msg_type", "")). A positional array causes all Go→Python messages
// (req, cap_load, worker_bye, …) to be silently discarded: msg_type evaluates
// to "" and no handler branch fires.
//
// UseCustomStructTag("msgpack") switches the encoder to named-field map format,
// keying each field by its `msgpack:"…"` tag value. The decoder mirror is set
// symmetrically so struct round-trips remain correct on the Go side.
var MsgPack Codec = msgpackCodec{}

type msgpackCodec struct{}

func (msgpackCodec) Name() string { return "msgpack" }

func (msgpackCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	// UseCustomStructTag encodes struct fields as a msgpack MAP keyed by the
	// named struct tag rather than a positional array. This is the setting
	// that makes Go-originated envelopes (envelope.Envelope, etc.) parseable
	// by the Python worker's plain-dict decoder.
	enc.SetCustomStructTag("msgpack")
	enc.UseCompactInts(true)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (msgpackCodec) Unmarshal(data []byte, v any) error {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.SetCustomStructTag("msgpack")
	return dec.Decode(v)
}
