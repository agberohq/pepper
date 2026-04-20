// Package envelope defines the Pepper wire protocol message types.
// Every message — request, response, error, heartbeat, pipeline control —
// uses the same Envelope struct. Every field is always present.
package envelope

// ProtoVer is the wire envelope protocol version.
const ProtoVer = uint8(1)

// Type identifies the kind of message on the wire.
type Type string

const (
	MsgReq          Type = "req"
	MsgRes          Type = "res"
	MsgResChunk     Type = "res_chunk"
	MsgResEnd       Type = "res_end"
	MsgErr          Type = "err"
	MsgPipe         Type = "pipe"
	MsgCancel       Type = "cancel"
	MsgCbReq        Type = "cb_req"
	MsgCbRes        Type = "cb_res"
	MsgStreamOpen   Type = "stream_open"
	MsgStreamChunk  Type = "stream_chunk"
	MsgStreamClose  Type = "stream_close"
	MsgStreamReady  Type = "stream_ready"
	MsgStreamCredit Type = "stream_credit"
	MsgStreamPause  Type = "stream_pause"
	MsgStreamResume Type = "stream_resume"
	MsgHbPing       Type = "hb_ping"
	MsgHbPong       Type = "hb_pong"
	MsgWorkerHello  Type = "worker_hello"
	MsgWorkerBye    Type = "worker_bye"
	MsgCapLoad      Type = "cap_load"
	MsgCapReady     Type = "cap_ready"
	MsgGroupJoin    Type = "group_join"
	MsgGroupLeave   Type = "group_leave"
	MsgBroadcast    Type = "broadcast"
)

// Dispatch controls how a request is routed to workers in a group.
type Dispatch string

const (
	DispatchAny    Dispatch = "any"
	DispatchAll    Dispatch = "all"
	DispatchFirst  Dispatch = "first"
	DispatchVote   Dispatch = "vote"
	DispatchQuorum Dispatch = "quorum"
)

// Code is a typed error returned in error envelopes.
type Code string

const (
	ErrCapNotFound      Code = "CAP_NOT_FOUND"
	ErrCapVerMismatch   Code = "CAP_VER_MISMATCH"
	ErrDeadlineExceeded Code = "DEADLINE_EXCEEDED"
	ErrWorkerOOM        Code = "WORKER_OOM"
	ErrPayloadInvalid   Code = "PAYLOAD_INVALID"
	ErrExecError        Code = "EXEC_ERROR"
	ErrModelOOM         Code = "MODEL_OOM"
	ErrModelTimeout     Code = "MODEL_TIMEOUT"
	ErrModelError       Code = "MODEL_ERROR"
	ErrProtoVer         Code = "PROTO_VER"
	ErrNoWorkers        Code = "NO_WORKERS"
	ErrWorkerShutdown   Code = "WORKER_SHUTTING_DOWN"
	ErrHopLimit         Code = "HOP_LIMIT"
	ErrGroupNotFound    Code = "GROUP_NOT_FOUND"
	ErrQuorumFailed     Code = "QUORUM_FAILED"
	ErrDispatchTimeout  Code = "DISPATCH_TIMEOUT"
	ErrCodecMismatch    Code = "CODEC_MISMATCH"
	ErrStreamError      Code = "STREAM_ERROR"
	ErrCbFailed         Code = "CB_FAILED"
	ErrSessionNotFound  Code = "SESSION_NOT_FOUND"
	ErrPoisonPill       Code = "POISON_PILL"
	ErrCancelled        Code = "CANCELLED"
)

// retryable returns true if the error code is safe to retry.
var retryable = map[Code]bool{
	ErrDeadlineExceeded: true,
	ErrWorkerOOM:        true,
	ErrNoWorkers:        true,
	ErrWorkerShutdown:   true,
	ErrDispatchTimeout:  true,
	ErrModelOOM:         true,
	ErrModelTimeout:     true,
}

// Retryable reports whether this error code should be retried.
func (e Code) Retryable() bool { return retryable[e] }
