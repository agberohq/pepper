package pepper

import (
	"time"

	"github.com/agberohq/pepper/internal/blob"
	"github.com/agberohq/pepper/internal/coord"
	"github.com/agberohq/pepper/internal/dlq"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/hooks"
	"github.com/agberohq/pepper/internal/metrics"
	"github.com/agberohq/pepper/internal/session"
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
func InMemoryStore(ttl time.Duration) session.Store { return session.NewMemory(ttl) }

// Hook helpers

// Envelope is the wire message that carries one call. Exposed so Before and
// After hooks can be authored without importing an internal package.
type Envelope = envelope.Envelope

// Hook is a named before and/or after interceptor pair.
type Hook = hooks.Hook

// BeforeFunc runs before a capability. Returning a non-nil error aborts the call.
type BeforeFunc = hooks.BeforeFunc

// AfterFunc runs after a capability, in reverse registration order.
type AfterFunc = hooks.AfterFunc

// HookResult is the payload handed to After hooks and returned by them.
// Build one from raw payload bytes with CachedResult.
type HookResult = hooks.Result

// ShortCircuit returns a special sentinel error from a BeforeHook that bypasses
// the capability call and returns r as the result immediately.
//
//	pp.Hooks().Global().Before("cache", func(ctx context.Context, env *pepper.Envelope, in pepper.In) (pepper.In, error) {
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
type Storage = session.Store

// Coord is the pluggable cluster coordination store used for registry
// replication, distributed sessions and cross-node pub/sub.
// Build one with Redis, NATS, Mula or MulaCluster, then pass it to WithCoord
// together with the same URL on WithTransportURL.
type Coord = coord.Store

// Redis returns a coordination store backed by Redis.
//
//	c, err := pepper.Redis("redis://127.0.0.1:6379")
//	pp, err := pepper.New(pepper.WithCoord(c),
//	    pepper.WithTransportURL("redis://127.0.0.1:6379"))
func Redis(url string) (Coord, error) { return coord.NewRedis(url) }

// NATS returns a coordination store backed by NATS (JetStream enabled).
//
//	c, err := pepper.NATS("nats://127.0.0.1:4222")
func NATS(url string) (Coord, error) { return coord.NewNATS(url) }

// Mula returns the built-in single-node coordination store.
// url is "tcp://host:port"; empty means a random free port on 127.0.0.1.
// A single-node Pepper configures this for you — use it only when you need to
// share one bus between several Pepper instances on the same host.
func Mula(url string) (Coord, error) { return coord.NewMula(url) }

// MulaCluster returns the built-in coordination store joined to a memberlist
// gossip cluster. seeds are existing members as "host:gossipPort".
func MulaCluster(bindAddr string, gossipPort, rpcPort int, seeds []string) (Coord, error) {
	return coord.NewMulaCluster(bindAddr, gossipPort, rpcPort, seeds)
}
