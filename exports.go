package pepper

import (
	"time"

	"github.com/agberohq/pepper/internal/blob"
	"github.com/agberohq/pepper/internal/dlq"
	"github.com/agberohq/pepper/internal/hooks"
	"github.com/agberohq/pepper/internal/metrics"
	"github.com/agberohq/pepper/internal/storage"
)

// Metrics

// Prometheus returns a MetricsSink backed by the default Prometheus registry.
func Prometheus() metrics.Sink { return metrics.Prometheus() }

// Noop returns a zero-overhead MetricsSink that discards all observations.
func Noop() metrics.Sink { return metrics.Noop() }

// DLQ

// FileDLQ returns a DLQBackend that persists poison pill entries to dir.
func FileDLQ(dir string) dlq.Backend { return dlq.NewFile(dir) }

// NopDLQ returns a DLQBackend that discards all entries.
func NopDLQ() dlq.Backend { return dlq.Nop{} }

// Session

// InMemoryStore returns an in-memory SessionStore with TTL managed by jack.Lifetime.
func InMemoryStore(ttl time.Duration) storage.Store { return storage.NewMemory(ttl) }

// Hook helpers

// ShortCircuit returns a special sentinel error from a BeforeHook that bypasses
// the capability call and returns r as the result immediately.
//
//	pp.Hooks().Global().Before("cache", func(ctx context.Context, env any, in In) (In, error) {
//	    if hit, ok := cache.Get(key); ok {
//	        return nil, pepper.ShortCircuit(pepper.CachedResult(hit))
//	    }
//	    return in, nil
//	})
var ShortCircuit = hooks.ShortCircuit

// CachedResult constructs a hooks.Result from raw payload bytes for ShortCircuit.
func CachedResult(payload []byte) hooks.Result {
	return hooks.Result{Payload: payload}
}

// BlobRef is the wire representation of a zero-copy blob.
// Embed in In when passing large binary payloads (images, audio, tensors).
// WithWorkers mmap the file directly — zero copies into numpy/torch/cv2.
type BlobRef = blob.Ref

// DLQBackend is the dead-letter queue storage interface.
// Receives poison pill entries for investigation and replay.
type DLQBackend = dlq.Backend

// DLQEntry is one poison pill record written to the DLQ.
type DLQEntry = dlq.Entry

// Storage is the pluggable session persistence backend.
type Storage = storage.Store
