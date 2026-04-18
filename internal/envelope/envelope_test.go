package envelope

import (
	"testing"
	"time"
)

// MsgType constants

func TestMsgTypeValues(t *testing.T) {
	cases := []struct {
		name string
		got  Type
		want string
	}{
		{"req", MsgReq, "req"},
		{"res", MsgRes, "res"},
		{"res_chunk", MsgResChunk, "res_chunk"},
		{"res_end", MsgResEnd, "res_end"},
		{"err", MsgErr, "err"},
		{"pipe", MsgPipe, "pipe"},
		{"cancel", MsgCancel, "cancel"},
		{"cb_req", MsgCbReq, "cb_req"},
		{"cb_res", MsgCbRes, "cb_res"},
		{"stream_open", MsgStreamOpen, "stream_open"},
		{"stream_chunk", MsgStreamChunk, "stream_chunk"},
		{"stream_close", MsgStreamClose, "stream_close"},
		{"stream_ready", MsgStreamReady, "stream_ready"},
		{"stream_credit", MsgStreamCredit, "stream_credit"},
		{"stream_pause", MsgStreamPause, "stream_pause"},
		{"stream_resume", MsgStreamResume, "stream_resume"},
		{"hb_ping", MsgHbPing, "hb_ping"},
		{"hb_pong", MsgHbPong, "hb_pong"},
		{"worker_hello", MsgWorkerHello, "worker_hello"},
		{"worker_bye", MsgWorkerBye, "worker_bye"},
		{"cap_load", MsgCapLoad, "cap_load"},
		{"cap_ready", MsgCapReady, "cap_ready"},
		{"group_join", MsgGroupJoin, "group_join"},
		{"group_leave", MsgGroupLeave, "group_leave"},
		{"broadcast", MsgBroadcast, "broadcast"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Errorf("MsgType %s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

// Dispatch constants

func TestDispatchValues(t *testing.T) {
	cases := []struct {
		got  Dispatch
		want string
	}{
		{DispatchAny, "any"},
		{DispatchAll, "all"},
		{DispatchFirst, "first"},
		{DispatchVote, "vote"},
		{DispatchQuorum, "quorum"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Errorf("Dispatch = %q, want %q", tc.got, tc.want)
		}
	}
}

// Error code constants

func TestErrorCodeValues(t *testing.T) {
	cases := []struct {
		got  Code
		want string
	}{
		{ErrCapNotFound, "CAP_NOT_FOUND"},
		{ErrCapVerMismatch, "CAP_VER_MISMATCH"},
		{ErrDeadlineExceeded, "DEADLINE_EXCEEDED"},
		{ErrWorkerOOM, "WORKER_OOM"},
		{ErrPayloadInvalid, "PAYLOAD_INVALID"},
		{ErrExecError, "EXEC_ERROR"},
		{ErrModelOOM, "MODEL_OOM"},
		{ErrModelTimeout, "MODEL_TIMEOUT"},
		{ErrModelError, "MODEL_ERROR"},
		{ErrProtoVer, "PROTO_VER"},
		{ErrNoWorkers, "NO_WORKERS"},
		{ErrWorkerShutdown, "WORKER_SHUTTING_DOWN"},
		{ErrHopLimit, "HOP_LIMIT"},
		{ErrGroupNotFound, "GROUP_NOT_FOUND"},
		{ErrQuorumFailed, "QUORUM_FAILED"},
		{ErrDispatchTimeout, "DISPATCH_TIMEOUT"},
		{ErrCodecMismatch, "CODEC_MISMATCH"},
		{ErrStreamError, "STREAM_ERROR"},
		{ErrCbFailed, "CB_FAILED"},
		{ErrSessionNotFound, "SESSION_NOT_FOUND"},
		{ErrPoisonPill, "POISON_PILL"},
		{ErrCancelled, "CANCELLED"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Errorf("Code = %q, want %q", tc.got, tc.want)
		}
	}
}

// Retryable()

func TestRetryable(t *testing.T) {
	shouldRetry := []Code{
		ErrDeadlineExceeded,
		ErrWorkerOOM,
		ErrNoWorkers,
		ErrWorkerShutdown,
		ErrDispatchTimeout,
		ErrModelOOM,
		ErrModelTimeout,
	}
	for _, code := range shouldRetry {
		if !code.Retryable() {
			t.Errorf("expected %q to be retryable", code)
		}
	}

	shouldNotRetry := []Code{
		ErrCapNotFound,
		ErrCapVerMismatch,
		ErrPayloadInvalid,
		ErrExecError,
		ErrModelError,
		ErrProtoVer,
		ErrHopLimit,
		ErrGroupNotFound,
		ErrQuorumFailed,
		ErrCodecMismatch,
		ErrStreamError,
		ErrCbFailed,
		ErrSessionNotFound,
		ErrPoisonPill,
		ErrCancelled,
	}
	for _, code := range shouldNotRetry {
		if code.Retryable() {
			t.Errorf("expected %q to NOT be retryable", code)
		}
	}
}

// DefaultEnvelope()

func TestDefaultEnvelopeFields(t *testing.T) {
	env := DefaultEnvelope()

	if env.ProtoVer != 1 {
		t.Errorf("ProtoVer = %d, want 1", env.ProtoVer)
	}
	if env.Dispatch != DispatchAny {
		t.Errorf("Dispatch = %q, want %q", env.Dispatch, DispatchAny)
	}
	if env.MaxHops != 10 {
		t.Errorf("MaxHops = %d, want 10", env.MaxHops)
	}
	if env.MaxCbDepth != 5 {
		t.Errorf("MaxCbDepth = %d, want 5", env.MaxCbDepth)
	}
	if env.Meta == nil {
		t.Error("Meta must not be nil — zero-value map causes marshal issues")
	}
}

func TestDefaultEnvelopeZeroFields(t *testing.T) {
	// Fields not set by DefaultEnvelope must be zero/empty — callers set them
	env := DefaultEnvelope()

	if env.CorrID != "" {
		t.Errorf("CorrID should be empty, got %q", env.CorrID)
	}
	if env.OriginID != "" {
		t.Errorf("OriginID should be empty, got %q", env.OriginID)
	}
	if env.Cap != "" {
		t.Errorf("Cap should be empty, got %q", env.Cap)
	}
	if env.DeadlineMs != 0 {
		t.Errorf("DeadlineMs should be zero, got %d", env.DeadlineMs)
	}
	if env.Hop != 0 {
		t.Errorf("Hop should be 0, got %d", env.Hop)
	}
}

// Envelope struct coverage

func TestEnvelopeFieldAssignment(t *testing.T) {
	env := DefaultEnvelope()
	env.CorrID = "01HTEST123"
	env.OriginID = "01HORIGIN"
	env.ParentID = "01HPARENT"
	env.Cap = "face.recognize"
	env.CapVer = ">=1.0.0"
	env.Group = "gpu"
	env.WorkerID = "w-1"
	env.Dispatch = DispatchAll
	env.Quorum = 3
	env.DeadlineMs = time.Now().Add(30 * time.Second).UnixMilli()
	env.ReplyTo = "pepper.res.01HORIGIN"
	env.ForwardTo = "pepper.pipe.asr.stage.1"
	env.Hop = 2
	env.CbDepth = 1
	env.DeliveryCount = 1
	env.SessionID = "user-123"
	env.StreamID = "stream-abc"
	env.Credits = 10
	env.Payload = []byte(`{"audio":"..."}`)
	env.Meta["_traceparent"] = "00-abc-def-01"

	if env.CorrID != "01HTEST123" {
		t.Errorf("CorrID roundtrip failed")
	}
	if env.Dispatch != DispatchAll {
		t.Errorf("Dispatch roundtrip failed")
	}
	if env.Hop != 2 {
		t.Errorf("Hop roundtrip failed")
	}
	if string(env.Payload) != `{"audio":"..."}` {
		t.Errorf("Payload roundtrip failed")
	}
	if env.Meta["_traceparent"] != "00-abc-def-01" {
		t.Errorf("Meta roundtrip failed")
	}
}

// Depth limit semantics

func TestHopAndCbDepthAreIndependent(t *testing.T) {
	// spec §0.5.0: hop and cb_depth are independent counters with independent limits
	// A deeply pipelined request (hop=9) can still make callbacks (cb_depth starts at 0)
	env := DefaultEnvelope()
	env.Hop = 9
	env.MaxHops = 10
	env.CbDepth = 0
	env.MaxCbDepth = 5

	// hop is within limit
	if env.Hop >= env.MaxHops {
		t.Error("hop should not have hit limit yet at 9/10")
	}
	// cb_depth is within limit
	if env.CbDepth >= env.MaxCbDepth {
		t.Error("cb_depth should not have hit limit at 0/5")
	}

	// Simulate one more hop hitting the limit
	env.Hop++
	if env.Hop < env.MaxHops {
		t.Error("hop should now be at limit")
	}
	// cb_depth still fine
	if env.CbDepth >= env.MaxCbDepth {
		t.Error("cb_depth should still be within limit")
	}
}

// CancelMsg

func TestCancelMsg(t *testing.T) {
	msg := CancelMsg{
		ProtoVer: 1,
		MsgType:  MsgCancel,
		OriginID: "01HORIGIN",
	}
	if msg.ProtoVer != 1 {
		t.Errorf("ProtoVer = %d, want 1", msg.ProtoVer)
	}
	if msg.MsgType != MsgCancel {
		t.Errorf("MsgType = %q, want %q", msg.MsgType, MsgCancel)
	}
	if msg.OriginID != "01HORIGIN" {
		t.Errorf("OriginID = %q, want 01HORIGIN", msg.OriginID)
	}
}

// Hello

func TestHelloFields(t *testing.T) {
	h := Hello{
		ProtoVer:        1,
		MsgType:         MsgWorkerHello,
		WorkerID:        "w-1",
		Runtime:         "python",
		PID:             12345,
		Codec:           "msgpack",
		Groups:          []string{"gpu", "asr"},
		Caps:            []string{"speech.transcribe"},
		CbSupported:     true,
		StreamSupported: true,
		Meta:            map[string]any{},
	}
	if h.WorkerID != "w-1" {
		t.Errorf("WorkerID = %q", h.WorkerID)
	}
	if len(h.Groups) != 2 {
		t.Errorf("Groups len = %d, want 2", len(h.Groups))
	}
	if !h.CbSupported {
		t.Error("CbSupported should be true")
	}
}

// HbPing

func TestHbPingFields(t *testing.T) {
	ping := HbPing{
		ProtoVer:       1,
		MsgType:        MsgHbPing,
		WorkerID:       "w-1",
		Runtime:        "go",
		Load:           42,
		Groups:         []string{"cpu"},
		RequestsServed: 1247,
		UptimeMs:       86400000,
	}
	if ping.Load != 42 {
		t.Errorf("Load = %d, want 42", ping.Load)
	}
	if ping.RequestsServed != 1247 {
		t.Errorf("RequestsServed = %d, want 1247", ping.RequestsServed)
	}
}

// CapLoad / CapReady

func TestCapLoadFields(t *testing.T) {
	cl := CapLoad{
		ProtoVer:      1,
		MsgType:       MsgCapLoad,
		Cap:           "speech.transcribe",
		CapVer:        "1.0.0",
		Source:        "./caps/speech/transcribe.py",
		Deps:          []string{"faster-whisper>=1.0"},
		TimeoutMs:     60000,
		MaxConcurrent: 4,
		Groups:        []string{"gpu", "asr"},
		Config:        map[string]any{"model_size": "small"},
	}
	if cl.Cap != "speech.transcribe" {
		t.Errorf("Cap = %q", cl.Cap)
	}
	if cl.MaxConcurrent != 4 {
		t.Errorf("MaxConcurrent = %d, want 4", cl.MaxConcurrent)
	}
}

func TestCapReadySuccess(t *testing.T) {
	cr := CapReady{
		ProtoVer: 1,
		MsgType:  MsgCapReady,
		WorkerID: "w-1",
		Cap:      "speech.transcribe",
		CapVer:   "1.0.0",
		SetupMs:  142,
		Error:    "", // empty = success
	}
	if cr.Error != "" {
		t.Errorf("Error should be empty for success, got %q", cr.Error)
	}
}

func TestCapReadyFailure(t *testing.T) {
	cr := CapReady{
		ProtoVer: 1,
		MsgType:  MsgCapReady,
		WorkerID: "w-1",
		Cap:      "speech.transcribe",
		Error:    "model file not found",
	}
	if cr.Error == "" {
		t.Error("Error should be non-empty for failure")
	}
}

// Error struct

func TestErrorStruct(t *testing.T) {
	e := Error{
		ProtoVer:  1,
		MsgType:   MsgErr,
		CorrID:    "01HCORR",
		OriginID:  "01HORIGIN",
		WorkerID:  "w-2",
		Cap:       "doc.parse",
		Code:      ErrExecError,
		Message:   "unhandled exception in run()",
		Retryable: false,
		Meta:      map[string]any{},
	}
	if e.Code != ErrExecError {
		t.Errorf("Code = %q, want ErrExecError", e.Code)
	}
	if e.Retryable {
		t.Error("EXEC_ERROR should not be retryable")
	}
}

// Deadline semantics

func TestDeadlineMsIsUnixEpochMs(t *testing.T) {
	// DeadlineMs must be unix epoch milliseconds, not nanoseconds or seconds
	deadline := time.Now().Add(30 * time.Second)
	ms := deadline.UnixMilli()

	// sanity: must be > year 2020 in ms
	year2020Ms := int64(1577836800000)
	if ms < year2020Ms {
		t.Errorf("DeadlineMs %d looks wrong — expected epoch ms > 2020", ms)
	}

	// sanity: converting back must be within 1ms of original
	recovered := time.UnixMilli(ms)
	diff := deadline.Sub(recovered)
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Millisecond {
		t.Errorf("DeadlineMs roundtrip diff = %v, want < 1ms", diff)
	}
}
