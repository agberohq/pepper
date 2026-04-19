package pepper

import (
	"time"

	"github.com/agberohq/pepper/internal/codec"
)

// Result is the response from a capability invocation.
type Result struct {
	payload []byte
	codec   codec.Codec

	WorkerID      string
	Runtime       string
	Latency       time.Duration
	WorkerMs      time.Duration
	Cap           string
	CapVer        string
	Hop           uint8
	CbDepth       uint8
	DeliveryCount uint8
	Meta          map[string]any
	Err           error
}

// AsString decodes the result payload as a UTF-8 string.
func (r Result) AsString() string {
	var s string
	_ = r.codec.Unmarshal(r.payload, &s)
	return s
}

// AsBytes returns the raw payload bytes.
func (r Result) AsBytes() []byte { return r.payload }

// AsJSON decodes the result payload as map[string]any.
func (r Result) AsJSON() map[string]any {
	var m map[string]any
	_ = r.codec.Unmarshal(r.payload, &m)
	return m
}

// AsFloat decodes the result payload as float64.
func (r Result) AsFloat() float64 {
	var f float64
	_ = r.codec.Unmarshal(r.payload, &f)
	return f
}

// AsInt decodes the result payload as int64.
func (r Result) AsInt() int64 {
	var i int64
	_ = r.codec.Unmarshal(r.payload, &i)
	return i
}

// AsBool decodes the result payload as bool.
func (r Result) AsBool() bool {
	var b bool
	_ = r.codec.Unmarshal(r.payload, &b)
	return b
}

// Into decodes the result payload into v using the active codec.
func (r Result) Into(v any) error {
	return r.codec.Unmarshal(r.payload, v)
}

// AsBlob extracts a BlobRef from the result payload.
// Use this when a worker returns a blob (e.g. a denoise worker returning
// cleaned audio as a /dev/shm reference). Returns nil if the payload is
// not a blob reference.
func (r Result) AsBlob() *BlobRef {
	var m map[string]any
	if err := r.codec.Unmarshal(r.payload, &m); err != nil {
		return nil
	}
	if _, ok := m["_pepper_blob"]; !ok {
		return nil
	}
	ref := &BlobRef{}
	ref.IsPepperBlob = true
	if id, ok := m["id"].(string); ok {
		ref.ID = id
	}
	if p, ok := m["path"].(string); ok {
		ref.Path = p
	}
	if s, ok := m["size"].(int64); ok {
		ref.Size = s
	}
	if d, ok := m["dtype"].(string); ok {
		ref.DType = d
	}
	if f, ok := m["format"].(string); ok {
		ref.Format = f
	}
	if shape, ok := m["shape"].([]any); ok {
		for _, v := range shape {
			if n, ok := v.(int64); ok {
				ref.Shape = append(ref.Shape, int(n))
			}
		}
	}
	return ref
}
