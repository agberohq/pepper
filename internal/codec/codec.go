package codec

import (
	"fmt"
)

// Codec is the pluggable serialisation interface.
// Implementations must be safe for concurrent use.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	// Name returns the codec identifier exchanged in worker_hello.
	// Must match the Python runtime's codec name.
	Name() string
}

// Custom wraps any Codec implementation registered by the user.
func Custom(c Codec) Codec { return c }

// Get returns a built-in codec by name, or an error if unknown.
// Used during worker_hello codec validation.
func Get(name string) (Codec, error) {
	switch name {
	case "msgpack":
		return MsgPack, nil
	case "json":
		return JSON, nil
	default:
		return nil, fmt.Errorf("pepper/codec: unknown codec %q — must be msgpack or json", name)
	}
}
