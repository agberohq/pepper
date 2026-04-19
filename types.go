package pepper

import (
	"github.com/agberohq/pepper/internal/blob"
	"github.com/agberohq/pepper/internal/dlq"
	"github.com/agberohq/pepper/internal/storage"
)

// In is the map-based capability input used at the wire level.
// All internal routing, encoding, and Python/CLI workers use this type.
// For type-safe Go calls use pepper.Do[O] or pepper.All[O].
type In = map[string]any

// BlobRef is the wire representation of a zero-copy blob.
// Embed in In when passing large binary payloads (images, audio, tensors).
// Workers mmap the file directly — zero copies into numpy/torch/cv2.
type BlobRef = blob.Ref

// SessionStore is the pluggable session persistence backend.
type SessionStore = storage.Store

// DLQBackend is the dead-letter queue storage interface.
// Receives poison pill entries for investigation and replay.
type DLQBackend = dlq.Backend

// DLQEntry is one poison pill record written to the DLQ.
type DLQEntry = dlq.Entry

// MetricsSink is the pluggable metrics backend.
// All methods must be safe for concurrent use.
type MetricsSink interface {
	Counter(name string, val int64, tags map[string]string)
	Gauge(name string, val float64, tags map[string]string)
	Histogram(name string, val float64, tags map[string]string)
}
