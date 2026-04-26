// Package envelope defines the Pepper wire protocol message types.
// Every message — request, response, error, heartbeat, pipeline control —
// uses the same Envelope struct. Every field is always present.
package envelope

// Envelope is the universal Pepper wire message.
// Every field is always present — zero values are explicit, never omitted.
// This guarantees forward compatibility: future versions add fields;
// old parsers silently ignore unknown keys.
type Envelope struct {
	// Protocol
	ProtoVer uint8 `msgpack:"proto_ver" json:"proto_ver"`
	MsgType  Type  `msgpack:"msg_type"  json:"msg_type"`

	// Correlation
	CorrID   string `msgpack:"corr_id"   json:"corr_id"`   // ULID — changes on retry
	OriginID string `msgpack:"origin_id" json:"origin_id"` // ULID — never changes end-to-end
	ParentID string `msgpack:"parent_id" json:"parent_id"` // immediate parent, empty for Go-originated

	// Routing
	WorkerID string   `msgpack:"worker_id" json:"worker_id"` // pin to specific worker, empty = router chooses
	Group    string   `msgpack:"group"     json:"group"`     // target group, empty = "default"
	Dispatch Dispatch `msgpack:"dispatch"  json:"dispatch"`  // routing strategy
	Quorum   uint8    `msgpack:"quorum"    json:"quorum"`    // for vote/quorum dispatch

	// Capability
	Cap    string `msgpack:"cap"     json:"cap"`     // capability name
	CapVer string `msgpack:"cap_ver" json:"cap_ver"` // semver constraint, empty = any

	// Timing
	DeadlineMs int64 `msgpack:"deadline_ms" json:"deadline_ms"` // unix epoch ms, shared across pipeline

	// Pipeline routing (set by router, read by runtime — never set by user code)
	ReplyTo        string `msgpack:"reply_to"        json:"reply_to"`        // where final response goes
	ForwardTo      string `msgpack:"forward_to"      json:"forward_to"`      // next pipe topic, empty = reply_to
	GatherAt       string `msgpack:"gather_at"       json:"gather_at"`       // scatter gather convergence topic
	GatherStrategy string `msgpack:"gather_strategy" json:"gather_strategy"` // all|first|quorum|timeout
	GatherQuorum   uint8  `msgpack:"gather_quorum"   json:"gather_quorum"`
	BranchTable    string `msgpack:"branch_table"    json:"branch_table"` // JSON: condition → pipe topic
	ParallelID     string `msgpack:"parallel_id"     json:"parallel_id"`  // shared ID for parallel stages

	// Depth tracking
	Hop        uint8 `msgpack:"hop"          json:"hop"`          // pipeline depth, incremented on pipe forward
	MaxHops    uint8 `msgpack:"max_hops"     json:"max_hops"`     // pipeline depth limit, default 10
	CbDepth    uint8 `msgpack:"cb_depth"     json:"cb_depth"`     // callback nesting depth
	MaxCbDepth uint8 `msgpack:"max_cb_depth" json:"max_cb_depth"` // callback depth limit, default 5

	// Idempotency
	DeliveryCount uint8 `msgpack:"delivery_count" json:"delivery_count"` // 0 = first, >0 = retry

	// Session
	SessionID string `msgpack:"session_id" json:"session_id"` // empty = no session

	// Streaming
	StreamID string `msgpack:"stream_id" json:"stream_id"` // empty = not a stream
	Credits  uint16 `msgpack:"credits"   json:"credits"`   // stream flow control credits

	// Payload
	Payload []byte `msgpack:"payload" json:"payload"` // codec-encoded inputs or result

	// Metadata — propagated unchanged through all hops
	// Reserved keys prefixed with "_": _traceparent, _tracestate, _blobs, _cost_usd
	Meta map[string]any `msgpack:"meta" json:"meta"`
}

type Error struct {
	ProtoVer  uint8          `msgpack:"proto_ver" json:"proto_ver"`
	MsgType   Type           `msgpack:"msg_type"  json:"msg_type"`
	CorrID    string         `msgpack:"corr_id"   json:"corr_id"`
	OriginID  string         `msgpack:"origin_id" json:"origin_id"`
	WorkerID  string         `msgpack:"worker_id" json:"worker_id"`
	Cap       string         `msgpack:"cap"       json:"cap"`
	Code      Code           `msgpack:"code"      json:"code"`
	Message   string         `msgpack:"message"   json:"message"`
	Retryable bool           `msgpack:"retryable" json:"retryable"`
	Meta      map[string]any `msgpack:"meta"      json:"meta"`
}
