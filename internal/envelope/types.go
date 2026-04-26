// Package envelope defines the Pepper wire protocol message types.
// Every message — request, response, error, heartbeat, pipeline control —
// uses the same Envelope struct. Every field is always present.
package envelope

// Hello is the first message sent by every worker at boot.
type Hello struct {
	ProtoVer        uint8          `msgpack:"proto_ver"         json:"proto_ver"`
	MsgType         Type           `msgpack:"msg_type"          json:"msg_type"`
	WorkerID        string         `msgpack:"worker_id"         json:"worker_id"`
	Runtime         string         `msgpack:"runtime"           json:"runtime"` // "python"|"go"|"http"|"cli"|custom
	PID             int            `msgpack:"pid"               json:"pid"`
	Codec           string         `msgpack:"codec"             json:"codec"`
	Groups          []string       `msgpack:"groups"            json:"groups"`
	Caps            []string       `msgpack:"caps"              json:"caps"`
	PipeSubs        []string       `msgpack:"pipe_subs"         json:"pipe_subs"`
	PipePubs        []string       `msgpack:"pipe_pubs"         json:"pipe_pubs"`
	CbSupported     bool           `msgpack:"cb_supported"      json:"cb_supported"`
	StreamSupported bool           `msgpack:"stream_supported"  json:"stream_supported"`
	Meta            map[string]any `msgpack:"meta"              json:"meta"`
}

// HbPing is the heartbeat sent by workers every HeartbeatInterval.
type HbPing struct {
	ProtoVer       uint8    `msgpack:"proto_ver"       json:"proto_ver"`
	MsgType        Type     `msgpack:"msg_type"        json:"msg_type"`
	WorkerID       string   `msgpack:"worker_id"       json:"worker_id"`
	Runtime        string   `msgpack:"runtime"         json:"runtime"`
	Load           uint8    `msgpack:"load"            json:"load"` // 0=idle, 100=saturated
	Groups         []string `msgpack:"groups"          json:"groups"`
	Caps           []string `msgpack:"caps"            json:"caps"`
	RequestsServed uint64   `msgpack:"requests_served" json:"requests_served"`
	UptimeMs       int64    `msgpack:"uptime_ms"       json:"uptime_ms"`
}

// CapLoad is sent from router to worker for each registered capability at boot.
type CapLoad struct {
	ProtoVer       uint8          `msgpack:"proto_ver"       json:"proto_ver"`
	MsgType        Type           `msgpack:"msg_type"        json:"msg_type"`
	Cap            string         `msgpack:"cap"             json:"cap"`
	CapVer         string         `msgpack:"cap_ver"         json:"cap_ver"`
	Source         string         `msgpack:"source"          json:"source"`
	Deps           []string       `msgpack:"deps"            json:"deps"`
	TimeoutMs      int64          `msgpack:"timeout_ms"      json:"timeout_ms"`
	MaxConcurrent  int            `msgpack:"max_concurrent"  json:"max_concurrent"`
	Groups         []string       `msgpack:"groups"          json:"groups"`
	PipePublishes  []string       `msgpack:"pipe_publishes"  json:"pipe_publishes"`
	PipeSubscribes []string       `msgpack:"pipe_subscribes" json:"pipe_subscribes"`
	Config         map[string]any `msgpack:"config"          json:"config"`
}

// CapReady is sent from worker to router after setup() completes.
type CapReady struct {
	ProtoVer uint8  `msgpack:"proto_ver" json:"proto_ver"`
	MsgType  Type   `msgpack:"msg_type"  json:"msg_type"`
	WorkerID string `msgpack:"worker_id" json:"worker_id"`
	Cap      string `msgpack:"cap"       json:"cap"`
	CapVer   string `msgpack:"cap_ver"   json:"cap_ver"`
	SetupMs  int64  `msgpack:"setup_ms"  json:"setup_ms"`
	Error    string `msgpack:"error"     json:"error"` // empty = success
}

// CancelMsg is broadcast when a Go context is cancelled.
type CancelMsg struct {
	ProtoVer uint8  `msgpack:"proto_ver" json:"proto_ver"`
	MsgType  Type   `msgpack:"msg_type"  json:"msg_type"`
	OriginID string `msgpack:"origin_id" json:"origin_id"`
}

// WorkerBye is sent by the router to gracefully shut down a worker.
type WorkerBye struct {
	ProtoVer uint8  `msgpack:"proto_ver" json:"proto_ver"`
	MsgType  Type   `msgpack:"msg_type"  json:"msg_type"`
	WorkerID string `msgpack:"worker_id" json:"worker_id"`
}

// CbRes is the callback response sent back to the originating worker.
type CbRes struct {
	ProtoVer uint8  `msgpack:"proto_ver"        json:"proto_ver"`
	MsgType  Type   `msgpack:"msg_type"         json:"msg_type"`
	OriginID string `msgpack:"origin_id"        json:"origin_id"`
	CbID     string `msgpack:"cb_id"            json:"cb_id"`
	Payload  []byte `msgpack:"payload,omitempty" json:"payload,omitempty"`
	Error    string `msgpack:"error,omitempty"   json:"error,omitempty"`
}

// StreamChunk is a single chunk sent from the Go side into a bidi stream.
type StreamChunk struct {
	ProtoVer uint8  `msgpack:"proto_ver" json:"proto_ver"`
	MsgType  Type   `msgpack:"msg_type"  json:"msg_type"`
	StreamID string `msgpack:"stream_id" json:"stream_id"`
	CorrID   string `msgpack:"corr_id"   json:"corr_id"`
	Seq      uint64 `msgpack:"seq"       json:"seq"`
	Payload  []byte `msgpack:"payload"   json:"payload"`
}

// StreamClose signals end-of-input on a bidi stream.
type StreamClose struct {
	ProtoVer uint8  `msgpack:"proto_ver" json:"proto_ver"`
	MsgType  Type   `msgpack:"msg_type"  json:"msg_type"`
	StreamID string `msgpack:"stream_id" json:"stream_id"`
	CorrID   string `msgpack:"corr_id"   json:"corr_id"`
}
