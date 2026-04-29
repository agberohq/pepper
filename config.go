package pepper

import (
	"crypto/tls"
	"time"

	"github.com/agberohq/pepper/internal/blob"
	"github.com/agberohq/pepper/internal/coord"
	"github.com/agberohq/pepper/internal/dlq"
	"github.com/agberohq/pepper/internal/metrics"
	"github.com/agberohq/pepper/internal/session"
	"github.com/olekukonko/ll"
)

// Config holds all Pepper runtime configuration.
type Config struct {
	// Wire protocol
	Codec     Codec
	Transport Transport
	// TransportURL has two roles depending on mode:
	//   single-node (no Coord): Mula TCP address, e.g. "tcp://127.0.0.1:7731"
	//                           defaults to a random free port if empty.
	//   distributed (with Coord): the coord URL, e.g. "redis://host:6379"
	//                             or "nats://host:4222". Must match the URL
	//                             passed to coord.NewRedis / coord.NewNATS.
	//                             Required when WithCoord is set.
	TransportURL string

	// Workers
	Workers       []WorkerConfig
	MaxConcurrent int
	MemoryBudget  string

	// Timeouts
	DefaultTimeout    time.Duration
	BootTimeout       time.Duration // max wait for first worker ready; 0 = use DefaultTimeout
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration

	// Retry & limits
	MaxRetries int
	MaxHops    uint8
	MaxCbDepth uint8

	// Blobs (zero-copy local, or remote object store in distributed mode)
	BlobDir       string
	BlobOrphanTTL time.Duration
	// BlobStore is the remote object store backend used in distributed mode.
	// When set, blobs are uploaded here so workers on other nodes can fetch
	// them. Ref.Path will be "s3://..." instead of a local filesystem path.
	// If nil in distributed mode, large file transfer falls back to the
	// router's HTTP relay (not yet implemented — set this for production).
	BlobStore blob.RemoteBackend

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

	// Session backends
	Session session.Store
	DLQ     dlq.Backend
	Metrics metrics.Sink

	// Resources for Python @resource decorator
	Resources map[string]map[string]any

	// Strategy controls worker selection for DispatchAny. Default: StrategyCapAffinity.
	Strategy Strategy

	// Tracking enables the built-in ProcessTracker for Execute/Process[Out].
	Tracking bool

	// Coord is the coordination store for distributed deployments.
	//
	// When set:
	//   - capability registrations are published to all nodes
	//   - session storage uses the coord backend automatically (unless
	//     WithSession is also set, which takes precedence)
	//   - the bus uses coord pub/sub + Push/Pull instead of Mula TCP
	//   - TransportURL must be set to the same URL as the coord backend
	//
	// When nil (default): single-node in-memory mode, Mula bus.
	Coord coord.Store

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
		BootTimeout:         0, // 0 = inherit DefaultTimeout at runtime
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

// WithBootTimeout sets how long Start() waits for at least one worker to become
// ready (i.e. all cap setup() calls to complete). Use this when capabilities
// have heavy setup — model downloads, database connections, etc. — that take
// longer than the default per-request timeout.
// A value of 0 (the default) falls back to DefaultTimeout.
func WithBootTimeout(d time.Duration) Option { return func(cfg *Config) { cfg.BootTimeout = d } }
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
func WithDLQ(d dlq.Backend) Option             { return func(cfg *Config) { cfg.DLQ = d } }
func WithMetrics(s metrics.Sink) Option        { return func(cfg *Config) { cfg.Metrics = s } }
func WithTracking(enabled bool) Option         { return func(cfg *Config) { cfg.Tracking = enabled } }
func WithStrategy(s Strategy) Option           { return func(cfg *Config) { cfg.Strategy = s } }

// WithSession sets the session persistence backend.
// If not set and WithCoord is used, a coord-backed session store is created
// automatically. If neither is set, an in-memory store is used.
func WithSession(s session.Store) Option { return func(cfg *Config) { cfg.Session = s } }

// WithBlobStore sets the remote object store backend for distributed blob access.
// Required in distributed mode (WithCoord) when workers on different machines
// need to share large binary payloads (audio, video, images, model outputs).
//
//	pp, _ := pepper.New(
//	    pepper.WithCoord(coord.NewRedis("redis://host:6379")),
//	    pepper.WithTransportURL("redis://host:6379"),
//	    pepper.WithBlobStore(&blob.S3Backend{
//	        Bucket:  "my-bucket",
//	        BaseURL: "https://s3.amazonaws.com",
//	    }),
//	)
func WithBlobStore(b blob.RemoteBackend) Option { return func(cfg *Config) { cfg.BlobStore = b } }

// WithCoord attaches a coordination store for distributed deployments.
//
// Also set WithTransportURL to the same URL so Python workers know where
// to connect:
//
//	c, _ := coord.NewRedis("redis://host:6379")
//	pp, _ := pepper.New(
//	    pepper.WithCoord(c),
//	    pepper.WithTransportURL("redis://host:6379"),
//	)
func WithCoord(c coord.Store) Option { return func(cfg *Config) { cfg.Coord = c } }

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
