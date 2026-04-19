package goruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/olekukonko/ll"
)

// Test doubles

type echoWorker struct {
	setupCalled atomic.Int32
	runDelay    time.Duration
	runErr      error
}

func (w *echoWorker) Setup(cap string, config map[string]any) error {
	w.setupCalled.Add(1)
	return nil
}

func (w *echoWorker) Run(ctx context.Context, cap string, in map[string]any) (map[string]any, error) {
	if w.runDelay > 0 {
		select {
		case <-time.After(w.runDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if w.runErr != nil {
		return nil, w.runErr
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	out["cap"] = cap
	return out, nil
}

func (w *echoWorker) Capabilities() []CapSpec {
	return []CapSpec{
		{Name: "echo", Version: "1.0.0", Groups: []string{"default"}, MaxConcurrent: 4},
	}
}

type teardownWorker struct {
	echoWorker
	teardownCalled atomic.Int32
	teardownErr    error
}

func (w *teardownWorker) Teardown(cap string) error {
	w.teardownCalled.Add(1)
	return w.teardownErr
}

func (w *teardownWorker) Stream(_ context.Context, _ string, _ map[string]any) (<-chan map[string]any, error) {
	return nil, nil
}

// jsonCodec is a simple JSON codec for tests. Satisfies codec.Codec.
type jsonCodec struct{}

func (jsonCodec) Name() string                       { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func newRuntime(t *testing.T, worker Worker) (*GoWorkerRuntime, *bus.Mock) {
	t.Helper()
	b := bus.NewMock()
	logger := ll.New("test")
	rt := New("w-test", worker, []string{"default"}, b, jsonCodec{}, logger)
	return rt, b
}

func makeEnvelope(cap string, payload map[string]any) ([]byte, error) {
	p, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	env := envelope.DefaultEnvelope()
	env.MsgType = envelope.MsgReq
	env.CorrID = "corr-001"
	env.OriginID = "origin-001"
	env.Cap = cap
	env.DeadlineMs = time.Now().Add(5 * time.Second).UnixMilli()
	env.Payload = p
	return json.Marshal(env)
}

// New / capabilities

func TestNewCreatesRuntime(t *testing.T) {
	rt, _ := newRuntime(t, &echoWorker{})
	if rt.id != "w-test" {
		t.Errorf("id = %q, want w-test", rt.id)
	}
	if len(rt.caps) != 1 || rt.caps[0].Name != "echo" {
		t.Errorf("caps = %v", rt.caps)
	}
	if rt.semaphores["echo"] == nil {
		t.Error("semaphore for echo should be created")
	}
}

func TestNewDefaultMaxConcurrent(t *testing.T) {
	b := bus.NewMock()
	logger := ll.New("test")
	rt := New("w-1", &zeroCapWorker{}, []string{"default"}, b, jsonCodec{}, logger)
	if cap(rt.semaphores["zero.cap"]) != 8 {
		t.Errorf("semaphore cap = %d, want 8", cap(rt.semaphores["zero.cap"]))
	}
}

type zeroCapWorker struct{}

func (w *zeroCapWorker) Setup(cap string, config map[string]any) error { return nil }
func (w *zeroCapWorker) Run(ctx context.Context, cap string, in map[string]any) (map[string]any, error) {
	return in, nil
}
func (w *zeroCapWorker) Capabilities() []CapSpec {
	return []CapSpec{{Name: "zero.cap", MaxConcurrent: 0}}
}

// Start

func TestStartCallsSetup(t *testing.T) {
	worker := &echoWorker{}
	rt, _ := newRuntime(t, worker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if worker.setupCalled.Load() != 1 {
		t.Errorf("Setup called %d times, want 1", worker.setupCalled.Load())
	}
}

func TestStartSetupError(t *testing.T) {
	worker := &failSetupWorker{}
	rt, _ := newRuntime(t, worker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := rt.Start(ctx, nil)
	if err == nil {
		t.Error("expected error from Setup failure")
	}
}

type failSetupWorker struct{}

func (w *failSetupWorker) Setup(cap string, config map[string]any) error {
	return errors.New("setup failed: GPU unavailable")
}
func (w *failSetupWorker) Run(ctx context.Context, cap string, in map[string]any) (map[string]any, error) {
	return nil, nil
}
func (w *failSetupWorker) Capabilities() []CapSpec {
	return []CapSpec{{Name: "fail.cap", MaxConcurrent: 1}}
}

// handleMessage

func TestHandleMessageEcho(t *testing.T) {
	rt, b := newRuntime(t, &echoWorker{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resCh, _ := b.Subscribe(ctx, bus.TopicRes("origin-001"))
	time.Sleep(20 * time.Millisecond)

	data, err := makeEnvelope("echo", map[string]any{"hello": "world"})
	if err != nil {
		t.Fatalf("makeEnvelope: %v", err)
	}
	b.Publish(bus.TopicPub("default"), data)

	select {
	case msg := <-resCh:
		var resp map[string]any
		unmarshalErr := json.Unmarshal(msg.Data, &resp)
		if unmarshalErr != nil {
			t.Fatalf("unmarshal response: %v", unmarshalErr)
		}
		if resp["msg_type"] != string(envelope.MsgRes) {
			t.Errorf("msg_type = %v, want res", resp["msg_type"])
		}
		if resp["worker_id"] != "w-test" {
			t.Errorf("worker_id = %v, want w-test", resp["worker_id"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for response")
	}
}

func TestHandleMessageDeadlineExceeded(t *testing.T) {
	rt, b := newRuntime(t, &echoWorker{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resCh, _ := b.Subscribe(ctx, bus.TopicRes("origin-dl"))
	time.Sleep(20 * time.Millisecond)

	// Expired deadline
	p, _ := json.Marshal(map[string]any{"x": 1})
	env := envelope.DefaultEnvelope()
	env.MsgType = envelope.MsgReq
	env.CorrID = "corr-dl"
	env.OriginID = "origin-dl"
	env.Cap = "echo"
	env.DeadlineMs = time.Now().Add(-1 * time.Second).UnixMilli() // already expired
	env.Payload = p
	data, _ := json.Marshal(env)

	b.Publish(bus.TopicPub("default"), data)

	select {
	case msg := <-resCh:
		var resp map[string]any
		json.Unmarshal(msg.Data, &resp)
		if resp["msg_type"] != string(envelope.MsgErr) {
			t.Errorf("msg_type = %v, want err", resp["msg_type"])
		}
		if resp["code"] != string(envelope.ErrDeadlineExceeded) {
			t.Errorf("code = %v, want DEADLINE_EXCEEDED", resp["code"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for error response")
	}
}

func TestHandleMessageHopLimit(t *testing.T) {
	rt, b := newRuntime(t, &echoWorker{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resCh, _ := b.Subscribe(ctx, bus.TopicRes("origin-hop"))
	time.Sleep(20 * time.Millisecond)

	p, _ := json.Marshal(map[string]any{})
	env := envelope.DefaultEnvelope()
	env.MsgType = envelope.MsgReq
	env.CorrID = "corr-hop"
	env.OriginID = "origin-hop"
	env.Cap = "echo"
	env.DeadlineMs = time.Now().Add(5 * time.Second).UnixMilli()
	env.Hop = 10 // at limit
	env.MaxHops = 10
	env.Payload = p
	data, _ := json.Marshal(env)

	b.Publish(bus.TopicPub("default"), data)

	select {
	case msg := <-resCh:
		var resp map[string]any
		json.Unmarshal(msg.Data, &resp)
		if resp["code"] != string(envelope.ErrHopLimit) {
			t.Errorf("code = %v, want HOP_LIMIT", resp["code"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestHandleMessageExecError(t *testing.T) {
	worker := &echoWorker{runErr: errors.New("model crashed")}
	rt, b := newRuntime(t, worker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resCh, _ := b.Subscribe(ctx, bus.TopicRes("origin-err"))
	time.Sleep(20 * time.Millisecond)

	data, _ := makeEnvelope("echo", map[string]any{})
	// patch origin
	env := envelope.DefaultEnvelope()
	env.MsgType = envelope.MsgReq
	env.CorrID = "corr-err"
	env.OriginID = "origin-err"
	env.Cap = "echo"
	env.DeadlineMs = time.Now().Add(5 * time.Second).UnixMilli()
	p, _ := json.Marshal(map[string]any{})
	env.Payload = p
	data, _ = json.Marshal(env)

	b.Publish(bus.TopicPub("default"), data)

	select {
	case msg := <-resCh:
		var resp map[string]any
		json.Unmarshal(msg.Data, &resp)
		if resp["msg_type"] != string(envelope.MsgErr) {
			t.Errorf("msg_type = %v, want err", resp["msg_type"])
		}
		if resp["code"] != string(envelope.ErrExecError) {
			t.Errorf("code = %v, want EXEC_ERROR", resp["code"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

// Stop / Teardown

func TestStopCallsTeardown(t *testing.T) {
	worker := &teardownWorker{}
	rt, _ := newRuntime(t, worker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.Start(ctx, nil)

	rt.Stop()
	if worker.teardownCalled.Load() != 1 {
		t.Errorf("Teardown called %d times, want 1", worker.teardownCalled.Load())
	}
}

func TestStopSetsStoppedFlag(t *testing.T) {
	rt, _ := newRuntime(t, &echoWorker{})
	rt.Stop()
	if !rt.stopped.Load() {
		t.Error("stopped flag should be set after Stop")
	}
}

// Broadcast cancel

func TestBroadcastCancelStopsLongRunningWork(t *testing.T) {
	worker := &echoWorker{runDelay: 5 * time.Second}
	rt, b := newRuntime(t, worker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resCh, _ := b.Subscribe(ctx, bus.TopicRes("origin-cancel"))
	time.Sleep(20 * time.Millisecond)

	// Dispatch a slow request
	p, _ := json.Marshal(map[string]any{})
	env := envelope.DefaultEnvelope()
	env.MsgType = envelope.MsgReq
	env.CorrID = "corr-cancel"
	env.OriginID = "origin-cancel"
	env.Cap = "echo"
	env.DeadlineMs = time.Now().Add(10 * time.Second).UnixMilli()
	env.Payload = p
	data, _ := json.Marshal(env)
	b.Publish(bus.TopicPub("default"), data)

	time.Sleep(50 * time.Millisecond)

	// Send cancel broadcast
	cancelMsg := map[string]any{
		"msg_type":  "cancel",
		"origin_id": "origin-cancel",
	}
	cancelData, _ := json.Marshal(cancelMsg)
	b.Publish(bus.TopicBroadcast, cancelData)

	select {
	case <-resCh:
		// Got an error response — cancelled
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: cancel did not interrupt slow worker")
	}
}

// Concurrent execution

func TestConcurrentRequests(t *testing.T) {
	rt, b := newRuntime(t, &echoWorker{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	n := 10
	results := make(chan struct{}, n)
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		origID := fmt.Sprintf("origin-%02d", i)
		corrID := fmt.Sprintf("corr-%02d", i)
		resCh, _ := b.Subscribe(ctx, bus.TopicRes(origID))

		wg.Add(1)
		go func(ch <-chan bus.Message) {
			defer wg.Done()
			select {
			case <-ch:
				results <- struct{}{}
			case <-time.After(3 * time.Second):
			}
		}(resCh)

		p, _ := json.Marshal(map[string]any{"i": i})
		env := envelope.DefaultEnvelope()
		env.MsgType = envelope.MsgReq
		env.CorrID = corrID
		env.OriginID = origID
		env.Cap = "echo"
		env.DeadlineMs = time.Now().Add(5 * time.Second).UnixMilli()
		env.Payload = p
		data, _ := json.Marshal(env)
		b.Publish(bus.TopicPub("default"), data)
	}

	time.Sleep(20 * time.Millisecond)
	wg.Wait()

	if len(results) != n {
		t.Errorf("received %d/%d responses", len(results), n)
	}
}

// chanCap helper

func TestChanCap(t *testing.T) {
	ch := make(chan struct{}, 7)
	if chanCap(ch) != 7 {
		t.Errorf("chanCap = %d, want 7", chanCap(ch))
	}
}

// jsonCodec roundtrip

func TestJsonCodecRoundtrip(t *testing.T) {
	c := jsonCodec{}
	in := map[string]any{"key": "value", "num": float64(42)}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out map[string]any
	if err := c.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out["key"] != "value" {
		t.Errorf("key = %v", out["key"])
	}
}
