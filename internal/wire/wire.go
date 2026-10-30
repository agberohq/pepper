// Package wire is the single source of truth for every string that crosses a
// process, package or network boundary: bus topic prefixes, URL schemes, worker
// environment variables and reserved names.
//
// It imports nothing from Pepper, so the root package and every internal package
// can share it without an import cycle. Anything protocol-facing that would
// otherwise be a bare string literal belongs here — building topics or URLs by
// hand ("pepper.res."+id, "tcp://"+addr) is how the two ends of a protocol drift
// apart.
package wire

// Bus topic prefixes. Every topic Pepper publishes starts with TopicRoot.
// Build topics with the internal/bus helpers rather than concatenating these.
const (
	TopicRoot = "pepper."

	TopicPushPrefix    = TopicRoot + "push."
	TopicPubPrefix     = TopicRoot + "pub."
	TopicResPrefix     = TopicRoot + "res."
	TopicPipePrefix    = TopicRoot + "pipe."
	TopicStreamPrefix  = TopicRoot + "stream."
	TopicCbPrefix      = TopicRoot + "cb."
	TopicHBPrefix      = TopicRoot + "hb."
	TopicControlPrefix = TopicRoot + "control."

	// TopicControlExact is the control topic without a worker id: a control
	// message published here reaches every worker.
	TopicControlExact = TopicRoot + "control"

	// TopicBroadcast reaches every worker on the bus.
	TopicBroadcast = TopicRoot + "broadcast"
)

// PipeFinalSuffix is the pseudo-stage topic a pipeline uses to receive the
// result of its last worker stage.
const PipeFinalSuffix = "final"

// URL schemes and separators.
const (
	SchemeFile  = "file"
	SchemeTCP   = "tcp"
	SchemeRedis = "redis"
	SchemeNATS  = "nats"
	SchemeS3    = "s3"

	// SchemeSep separates a scheme from the rest of a URL.
	SchemeSep = "://"

	// TCPPrefix is "tcp://", the prefix Mula addresses carry.
	TCPPrefix = SchemeTCP + SchemeSep
)

// SchemeURL joins a scheme and an address, e.g. ("tcp", "127.0.0.1:7731").
func SchemeURL(scheme, addr string) string { return scheme + SchemeSep + addr }

// HasScheme reports whether url begins with scheme://.
func HasScheme(url, scheme string) bool {
	return len(url) > len(scheme) && url[:len(scheme)+len(SchemeSep)] == scheme+SchemeSep
}

// Worker environment variable names. Written by internal/bus, read by every
// bundled runtime (runtime/python, runtime/julia, runtime/node).
const (
	EnvWorkerID      = "PEPPER_WORKER_ID"
	EnvBusURL        = "PEPPER_BUS_URL"
	EnvCodec         = "PEPPER_CODEC"
	EnvGroups        = "PEPPER_GROUPS"
	EnvHeartbeatMS   = "PEPPER_HEARTBEAT_MS"
	EnvMaxConcurrent = "PEPPER_MAX_CONCURRENT"
	EnvBlobDir       = "PEPPER_BLOB_DIR"
	EnvResources     = "PEPPER_RESOURCES"

	// Runtime directory hints, set per script runtime so a capability can find
	// its own cap.* helper module.
	EnvJuliaRuntimeDir = "PEPPER_JULIA_RUNTIME_DIR"
	EnvNodeRuntimeDir  = "PEPPER_NODE_RUNTIME_DIR"
	EnvRustRuntimeDir  = "PEPPER_RUST_RUNTIME_DIR"

	// EnvRustWorkerBin points at a prebuilt pepper-worker binary, skipping the
	// cargo build of the runtime crate itself.
	EnvRustWorkerBin = "PEPPER_RUST_WORKER_BIN"

	// EnvCacheDir redirects the compiled-artifact cache used by the Rust
	// runtime (and by the Python runtime for model caches).
	EnvCacheDir = "PEPPER_CACHE_DIR"
)

// Reserved names.
const (
	// CapInline is the capability name used for pepper.Run snippets. The Python
	// runtime recognises it by name (see runtime/python/runtime.py).
	CapInline = TopicRoot + "inline"

	// GroupDefault is used when a capability or worker declares no group.
	GroupDefault = "default"

	// GroupBroadcast routes a request to every worker.
	GroupBroadcast = "*"
)
