package pepper

import (
	"crypto/tls"
	"time"

	"github.com/agberohq/pepper/internal/core"
	"github.com/olekukonko/ll"
)

// Serializer identifies the wire codec.
type Serializer string

const (
	MsgPack Serializer = "msgpack"
	JSON    Serializer = "json"
)

// Transport identifies the IPC mechanism.
type Transport string

const (
	NanoMsg     Transport = "nanomsg"
	UnixSocket  Transport = "unix"
	NamedPipe   Transport = "pipe"
	TCPLoopback Transport = "tcp"
	Auto        Transport = "auto"
)

// DebugLevel controls wire logging verbosity.
type DebugLevel int

const (
	DebugOff      DebugLevel = iota
	DebugEnvelope            // envelope fields only
	DebugPayload             // + decoded payload — never in production
	DebugWire                // + raw bytes
)

// Config holds all Pepper runtime configuration.
type Config struct {
	// Wire protocol
	Serializer   Serializer
	Transport    Transport
	TransportURL string

	// Workers
	Workers       []WorkerConfig
	MaxConcurrent int
	MemoryBudget  string

	// Timeouts
	DefaultTimeout    time.Duration
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration

	// Retry & limits
	MaxRetries int
	MaxHops    uint8
	MaxCbDepth uint8

	// Blobs (zero-copy)
	BlobDir       string
	BlobOrphanTTL time.Duration

	// Streaming
	StreamCredits uint16

	// Poison pill
	PoisonPillThreshold int
	PoisonPillTTL       time.Duration

	// Security
	TLS *tls.Config

	// Debug & observability
	Debug         DebugLevel
	InspectorPath string

	// Storage backends
	SessionStore SessionStore
	DLQ          DLQBackend
	// Metrics is the pluggable sink — field renamed from MetricsSink.
	Metrics core.MetricsSink

	// Resources for Python @resource decorator
	Resources map[string]map[string]any

	// Strategy controls worker selection for DispatchAny. Default: CapAffinity.
	Strategy StrategyType

	// Logger
	logger *ll.Logger
}

// WorkerConfig describes a single worker process or goroutine.
type WorkerConfig struct {
	ID          string
	Groups      []string
	MaxRequests uint64        // recycle after N requests served
	MaxUptime   time.Duration // recycle after this duration
}

// defaultConfig returns safe production defaults.
func defaultConfig() Config {
	return Config{
		Serializer:          MsgPack,
		Transport:           Auto,
		MaxConcurrent:       8,
		DefaultTimeout:      30 * time.Second,
		HeartbeatInterval:   5 * time.Second,
		ShutdownTimeout:     30 * time.Second,
		MaxRetries:          2,
		MaxHops:             10,
		MaxCbDepth:          5,
		BlobOrphanTTL:       24 * time.Hour,
		StreamCredits:       10,
		PoisonPillThreshold: 2,
		PoisonPillTTL:       1 * time.Hour,
		Resources:           make(map[string]map[string]any),
	}
}

// Option is a functional configuration option for pepper.New().
type Option func(*Config)

// WithTransport sets the IPC transport. Kept With-prefixed to avoid collision
// with the Transport type name.
func WithTransport(t Transport) Option {
	return func(c *Config) { c.Transport = t }
}

// TransportURL sets an explicit address (e.g. "tls+tcp://0.0.0.0:7731").
func TransportURL(url string) Option {
	return func(c *Config) { c.TransportURL = url }
}

// WithSerializer sets the wire codec. Kept With-prefixed to avoid collision
// with the Serializer type name. Default: MsgPack.
func WithSerializer(s Serializer) Option {
	return func(c *Config) { c.Serializer = s }
}

// WithTLS sets mutual WithTLS for cross-node deployments.
// Not needed for same-machine ipc:// or tcp://127.0.0.1.
func WithTLS(cfg *tls.Config) Option {
	return func(c *Config) { c.TLS = cfg }
}

func WithLogger(logger *ll.Logger) Option {
	return func(c *Config) {
		c.logger = logger
	}
}

// WorkerArg is satisfied by WorkerConfig and *WorkerBuilder.
// Allows Workers() to accept both without requiring .Build():
//
//	pepper.Workers(
//	    pepper.Worker("w-1").Groups("gpu"),   // *WorkerBuilder
//	    someConfig,                            // WorkerConfig
//	)
type WorkerArg interface{ workerConfig() WorkerConfig }

func (w WorkerConfig) workerConfig() WorkerConfig   { return w }
func (b *WorkerBuilder) workerConfig() WorkerConfig { return b.cfg }

// Workers registers worker configurations.
// Accepts *WorkerBuilder values directly (no .Build() needed) or plain WorkerConfig.
//
//	pepper.Workers(
//	    pepper.NewWorker("w-1").Groups("gpu"),   // *WorkerBuilder
//	    someConfig,                               // WorkerConfig
//	)
func Workers(workers ...WorkerArg) Option {
	return func(c *Config) {
		for _, w := range workers {
			c.Workers = append(c.Workers, w.workerConfig())
		}
	}
}

// MaxConcurrent sets the maximum simultaneous in-flight requests.
func MaxConcurrent(n int) Option {
	return func(c *Config) { c.MaxConcurrent = n }
}

// MemoryBudget sets an advisory memory limit string (e.g. "8gb").
func MemoryBudget(budget string) Option {
	return func(c *Config) { c.MemoryBudget = budget }
}

// DefaultTimeout sets the default request deadline when none is specified.
func DefaultTimeout(d time.Duration) Option {
	return func(c *Config) { c.DefaultTimeout = d }
}

// HeartbeatInterval sets the worker heartbeat period.
func HeartbeatInterval(d time.Duration) Option {
	return func(c *Config) { c.HeartbeatInterval = d }
}

// ShutdownTimeout sets how long Stop() waits for workers to drain.
func ShutdownTimeout(d time.Duration) Option {
	return func(c *Config) { c.ShutdownTimeout = d }
}

// MaxRetries sets the automatic retry count for retryable error codes.
func MaxRetries(n int) Option {
	return func(c *Config) { c.MaxRetries = n }
}

// MaxHops sets the pipeline hop depth limit. Default: 10.
func MaxHops(n uint8) Option {
	return func(c *Config) { c.MaxHops = n }
}

// MaxCbDepth sets the callback nesting depth limit. Default: 5.
func MaxCbDepth(n uint8) Option {
	return func(c *Config) { c.MaxCbDepth = n }
}

// BlobDir sets the zero-copy blob storage directory.
// Default: /dev/shm/pepper on Linux, os.TempDir()/pepper elsewhere.
func BlobDir(dir string) Option {
	return func(c *Config) { c.BlobDir = dir }
}

// StreamCredits sets the initial flow-control credit window. Default: 10.
func StreamCredits(n uint16) Option {
	return func(c *Config) { c.StreamCredits = n }
}

// PoisonPillThreshold sets how many worker crashes declare a poison pill.
// Default: 2.
func PoisonPillThreshold(n int) Option {
	return func(c *Config) { c.PoisonPillThreshold = n }
}

// PoisonPillTTL sets how long a poison pill origin_id stays blacklisted.
// Default: 1h.
func PoisonPillTTL(d time.Duration) Option {
	return func(c *Config) { c.PoisonPillTTL = d }
}

// Debug sets the wire logging verbosity.
func Debug(level DebugLevel) Option {
	return func(c *Config) { c.Debug = level }
}

// Inspector sets the Unix socket path for the live wire inspector.
func Inspector(path string) Option {
	return func(c *Config) { c.InspectorPath = path }
}

// SessionStore sets the session persistence backend.
// Default: in-memory with 24h TTL managed by jack.Lifetime.
func WithSessionStore(store SessionStore) Option {
	return func(c *Config) { c.SessionStore = store }
}

// DLQ sets the dead-letter queue backend for poison pill payloads.
// Default: nop (discard).
func DLQ(dlq DLQBackend) Option {
	return func(c *Config) { c.DLQ = dlq }
}

// Metrics sets the pluggable metrics sink.
// Default: noop (zero overhead).
func Metrics(sink core.MetricsSink) Option {
	return func(c *Config) { c.Metrics = sink }
}

// Resource registers a named resource config for Python @resource workers.
//
//	pepper.Resource("milvus", map[string]any{"milvus_uri": "localhost:19530"})
func Resource(name string, config map[string]any) Option {
	return func(c *Config) {
		if c.Resources == nil {
			c.Resources = make(map[string]map[string]any)
		}
		c.Resources[name] = config
	}
}

// NewWorker creates a WorkerConfig builder.
//
//	pepper.NewWorker("w-1").Groups("gpu", "asr").MaxRequests(10000).MaxUptime(24*time.Hour)
func NewWorker(id string) *WorkerBuilder {
	return &WorkerBuilder{cfg: WorkerConfig{ID: id}}
}

// WorkerBuilder builds a WorkerConfig with method chaining.
type WorkerBuilder struct {
	cfg WorkerConfig
}

func (b *WorkerBuilder) Groups(groups ...string) *WorkerBuilder {
	b.cfg.Groups = groups
	return b
}

func (b *WorkerBuilder) MaxRequests(n uint64) *WorkerBuilder {
	b.cfg.MaxRequests = n
	return b
}

func (b *WorkerBuilder) MaxUptime(d time.Duration) *WorkerBuilder {
	b.cfg.MaxUptime = d
	return b
}

func (b *WorkerBuilder) Build() WorkerConfig {
	return b.cfg
}

// StrategyType selects the worker selection algorithm for DispatchAny.
type StrategyType string

const (
	// CapAffinity (default) routes to workers that have already loaded the
	// requested capability, minimising cold-start overhead.
	CapAffinity StrategyType = "cap_affinity"
	// LeastLoaded always routes to the worker reporting the lowest load.
	LeastLoaded StrategyType = "least_loaded"
	// RoundRobin cycles through available workers in registration order.
	RoundRobin StrategyType = "round_robin"
)

// Strategy sets the worker selection strategy used for DispatchAny.
// Default: CapAffinity.
func Strategy(s StrategyType) Option {
	return func(c *Config) { c.Strategy = s }
}

// Convenience aliases matching spec call sites
// The spec uses pepper.New(pepper.Serializer(pepper.MsgPack)) and
// pepper.New(pepper.Transport(pepper.NanoMsg)).
// In Go, a function cannot share a name with a type in the same package,
// so these are exposed as WithSerializer/WithTransport. Callers who want the
// spec spelling can use the With-prefixed names or the aliases below which
// use distinct function names.
