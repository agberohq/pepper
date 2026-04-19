// for common constructors.
//
//	pepper.New(
//	    pepper.Metrics(pepper.Prometheus()),
//	    pepper.DLQ(pepper.FileDLQ("/var/log/pepper/dlq")),
//	    pepper.SessionStore(pepper.InMemoryStore(24*time.Hour)),
//	)
//
// Hook short-circuit:
//
//	return nil, pepper.ShortCircuit(pepper.CachedResult(payload))
package pepper

import (
	"time"

	"github.com/agberohq/pepper/internal/dlq"
	"github.com/agberohq/pepper/internal/hooks"
	"github.com/agberohq/pepper/internal/metrics"
	"github.com/agberohq/pepper/internal/storage"
)

// Metrics

// Prometheus returns a MetricsSink backed by the default Prometheus registry.
func Prometheus() MetricsSink { return metrics.Prometheus() }

// Noop returns a zero-overhead MetricsSink that discards all observations.
func Noop() MetricsSink { return metrics.Noop() }

// DLQ

// FileDLQ returns a DLQBackend that persists poison pill entries to dir.
func FileDLQ(dir string) DLQBackend { return dlq.NewFile(dir) }

// NopDLQ returns a DLQBackend that discards all entries.
func NopDLQ() DLQBackend { return dlq.Nop{} }

// Session

// InMemoryStore returns an in-memory SessionStore with TTL managed by jack.Lifetime.
func InMemoryStore(ttl time.Duration) SessionStore { return storage.NewMemory(ttl) }

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
