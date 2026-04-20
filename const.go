// Package pepper — named constants for all magic values.
//
// Every string literal, numeric default, and status value that appears in
// user-facing code lives here. Internal packages retain their own constants;
// this file covers the public API surface.
package pepper

import "time"

// Core types

// In is the wire-level capability input map.
// Used for dynamic/untyped calls. For type-safe calls use pepper.Call[Out]
// with a typed struct as Input.
type In = map[string]any

// Dispatch modes

// Dispatch controls how requests are routed to workers.
type Dispatch string

const (
	// DispatchAny routes to the single best available worker (default).
	DispatchAny Dispatch = "any"
	// DispatchAll broadcasts to every worker in the group and collects all responses.
	DispatchAll Dispatch = "all"
	// DispatchFirst broadcasts and returns the first response received.
	DispatchFirst Dispatch = "first"
	// DispatchVote broadcasts and returns the majority response.
	DispatchVote Dispatch = "vote"
	// DispatchQuorum broadcasts and waits for a quorum of responses.
	DispatchQuorum Dispatch = "quorum"
)

// Process / stage statuses

// Status represents the lifecycle state of a process or stage.
type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

// Serialization formats

// Codec identifies the wire serialisation format.
type Codec string

const (
	CodecMsgPack Codec = "msgpack"
	CodecJSON    Codec = "json"
)

// Transport types

// Transport identifies the IPC mechanism between the Go runtime and workers.
type Transport string

const (
	TransportNanoMsg     Transport = "nanomsg"
	TransportUnixSocket  Transport = "unix"
	TransportNamedPipe   Transport = "pipe"
	TransportTCPLoopback Transport = "tcp"
	TransportAuto        Transport = "auto"
)

// Debug verbosity

// DebugLevel controls wire-level logging verbosity.
type DebugLevel int

const (
	DebugOff      DebugLevel = iota
	DebugEnvelope            // log envelope fields
	DebugPayload             // log decoded payloads — never in production
	DebugWire                // log raw bytes
)

// Worker selection strategy

// Strategy controls which worker is selected for DispatchAny.
type Strategy string

const (
	// StrategyCapAffinity routes to workers that have already loaded the
	// requested capability, minimising cold-start overhead. Default.
	StrategyCapAffinity Strategy = "cap_affinity"
	// StrategyLeastLoaded always routes to the worker reporting the lowest load.
	StrategyLeastLoaded Strategy = "least_loaded"
	// StrategyRoundRobin cycles through available workers in registration order.
	StrategyRoundRobin Strategy = "round_robin"
)

// Timeouts & limits

const (
	// DefaultTimeout is the default per-request deadline when none is specified.
	DefaultTimeout = 30 * time.Second

	// DefaultHeartbeatInterval is the worker heartbeat period.
	DefaultHeartbeatInterval = 5 * time.Second

	// DefaultShutdownTimeout is how long Stop() waits for workers to drain.
	DefaultShutdownTimeout = 30 * time.Second

	// DefaultMaxRetries is the automatic retry count for retryable errors.
	DefaultMaxRetries = 2

	// DefaultMaxHops is the maximum number of pipeline hops.
	DefaultMaxHops uint8 = 10

	// DefaultMaxCbDepth is the maximum callback nesting depth.
	DefaultMaxCbDepth uint8 = 5

	// DefaultMaxConcurrent is the maximum simultaneous in-flight requests.
	DefaultMaxConcurrent = 8

	// DefaultStreamCredits is the initial flow-control credit window.
	DefaultStreamCredits uint16 = 10

	// DefaultPoisonPillThreshold is how many worker crashes declare a poison pill.
	DefaultPoisonPillThreshold = 2

	// DefaultBlobOrphanTTL is how long orphaned blobs persist before cleanup.
	DefaultBlobOrphanTTL = 24 * time.Hour

	// DefaultSessionTTL is the default idle expiry for in-memory sessions.
	DefaultSessionTTL = 24 * time.Hour

	// DefaultStreamChanBuffer is the output channel buffer size for streaming calls.
	DefaultStreamChanBuffer = 64

	// DefaultAdapterTimeout is the default HTTP/MCP adapter request timeout.
	DefaultAdapterTimeout = 120 * time.Second

	// DefaultPoisonPillTTL is how long a poison pill origin_id stays blacklisted.
	DefaultPoisonPillTTL = 1 * time.Hour

	// defaultCapVersion is the semver assigned to capabilities with no explicit version.
	defaultCapVersion = "0.0.0"

	// protoVer is the wire envelope protocol version.
	protoVer = uint8(1)
)

// groupBroadcast is the group name that routes a request to all workers via pub/sub.
const groupBroadcast = "*"

// Meta key prefixes

// MetaKeyPrefix is the reserved prefix for internal envelope Meta keys.
// User-defined Meta keys must not start with this prefix.
const MetaKeyPrefix = "_"

// Internal meta keys injected by the framework.
// Use these constants instead of raw string literals.
const (
	MetaKeyProcessID   = "_process_id"
	MetaKeyTraceParent = "_traceparent"
	MetaKeyTraceState  = "_tracestate"
	MetaKeyBlobs       = "_blobs"
	MetaKeyCostUSD     = "_cost_usd"
)
