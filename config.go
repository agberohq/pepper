package pepper

import (
	"crypto/tls"
	"time"

	"github.com/agberohq/pepper/internal/dlq"
	"github.com/agberohq/pepper/internal/metrics"
	"github.com/agberohq/pepper/internal/storage"
	"github.com/olekukonko/ll"
)

// Config holds all Pepper runtime configuration.
type Config struct {
	// Wire protocol
	Codec        Codec
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
	Storage storage.Store
	DLQ     dlq.Backend
	Metrics metrics.Sink

	// Resources for Python @resource decorator
	Resources map[string]map[string]any

	// Strategy controls worker selection for DispatchAny. Default: StrategyCapAffinity.
	Strategy Strategy

	// Tracking enables the built-in ProcessTracker for Execute/Process[Out].
	Tracking bool

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

func defaultConfig() Config {
	return Config{
		Codec:               CodecMsgPack,
		Transport:           TransportAuto,
		MaxConcurrent:       DefaultMaxConcurrent,
		DefaultTimeout:      DefaultTimeout,
		HeartbeatInterval:   DefaultHeartbeatInterval,
		ShutdownTimeout:     DefaultShutdownTimeout,
		MaxRetries:          DefaultMaxRetries,
		MaxHops:             DefaultMaxHops,
		MaxCbDepth:          DefaultMaxCbDepth,
		BlobOrphanTTL:       DefaultBlobOrphanTTL,
		StreamCredits:       DefaultStreamCredits,
		PoisonPillThreshold: DefaultPoisonPillThreshold,
		PoisonPillTTL:       DefaultPoisonPillTTL,
		Resources:           make(map[string]map[string]any),
	}
}

// Option is a functional configuration option for pepper.New().
type Option func(*Config)

func WithCodec(c Codec) Option           { return func(cfg *Config) { cfg.Codec = c } }
func WithTransport(t Transport) Option   { return func(cfg *Config) { cfg.Transport = t } }
func WithTransportURL(url string) Option { return func(cfg *Config) { cfg.TransportURL = url } }
func WithTLS(c *tls.Config) Option       { return func(cfg *Config) { cfg.TLS = c } }
func WithLogger(l *ll.Logger) Option     { return func(cfg *Config) { cfg.logger = l } }

func WithMaxConcurrent(n int) Option            { return func(cfg *Config) { cfg.MaxConcurrent = n } }
func WithMemoryBudget(b string) Option          { return func(cfg *Config) { cfg.MemoryBudget = b } }
func WithDefaultTimeout(d time.Duration) Option { return func(cfg *Config) { cfg.DefaultTimeout = d } }
func WithHeartbeatInterval(d time.Duration) Option {
	return func(cfg *Config) { cfg.HeartbeatInterval = d }
}
func WithShutdownTimeout(d time.Duration) Option {
	return func(cfg *Config) { cfg.ShutdownTimeout = d }
}
func WithMaxRetries(n int) Option       { return func(cfg *Config) { cfg.MaxRetries = n } }
func WithMaxHops(n uint8) Option        { return func(cfg *Config) { cfg.MaxHops = n } }
func WithMaxCbDepth(n uint8) Option     { return func(cfg *Config) { cfg.MaxCbDepth = n } }
func WithBlobDir(dir string) Option     { return func(cfg *Config) { cfg.BlobDir = dir } }
func WithStreamCredits(n uint16) Option { return func(cfg *Config) { cfg.StreamCredits = n } }

func WithPoisonPillThreshold(n int) Option {
	return func(cfg *Config) { cfg.PoisonPillThreshold = n }
}
func WithPoisonPillTTL(d time.Duration) Option { return func(cfg *Config) { cfg.PoisonPillTTL = d } }
func WithDebug(level DebugLevel) Option        { return func(cfg *Config) { cfg.Debug = level } }
func WithInspector(path string) Option         { return func(cfg *Config) { cfg.InspectorPath = path } }
func WithStorage(s storage.Store) Option       { return func(cfg *Config) { cfg.Storage = s } }
func WithDLQ(d dlq.Backend) Option             { return func(cfg *Config) { cfg.DLQ = d } }
func WithMetrics(s metrics.Sink) Option        { return func(cfg *Config) { cfg.Metrics = s } }
func WithTracking(enabled bool) Option         { return func(cfg *Config) { cfg.Tracking = enabled } }
func WithStrategy(s Strategy) Option           { return func(cfg *Config) { cfg.Strategy = s } }

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

// WorkerArg is satisfied by WorkerConfig and *WorkerBuilder.
type WorkerArg interface{ workerConfig() WorkerConfig }

func (w WorkerConfig) workerConfig() WorkerConfig   { return w }
func (b *WorkerBuilder) workerConfig() WorkerConfig { return b.cfg }

// NewWorker creates a WorkerConfig builder.
//
//	pepper.NewWorker("w-1").Groups("gpu", "asr").MaxRequests(10000).MaxUptime(24*time.Hour)
func NewWorker(id string) *WorkerBuilder {
	return &WorkerBuilder{cfg: WorkerConfig{ID: id}}
}

// WorkerBuilder builds a WorkerConfig with method chaining.
type WorkerBuilder struct{ cfg WorkerConfig }

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
func (b *WorkerBuilder) Build() WorkerConfig { return b.cfg }

// WithWorkers registers worker configurations.
// Accepts *WorkerBuilder or plain WorkerConfig — no .Build() needed.
func WithWorkers(workers ...WorkerArg) Option {
	return func(c *Config) {
		for _, w := range workers {
			c.Workers = append(c.Workers, w.workerConfig())
		}
	}
}
