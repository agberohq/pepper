package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/dlq"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/pending"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
)

func newRouter(t *testing.T) (*Router, *bus.Mock, *pending.Map) {
	t.Helper()
	b := bus.NewMock()
	p := pending.New()
	reaper := jack.NewReaper(0,
		jack.ReaperWithShards(4),
		jack.ReaperWithHandler(func(_ context.Context, id string) {
			p.Fail(id, errors.New("deadline exceeded"))
		}),
	)
	t.Cleanup(reaper.Stop)
	logger := ll.New("test")
	cfg := DefaultConfig()
	cfg.Codec = codec.MsgPack
	r := New(b, p, reaper, cfg, logger)
	return r, b, p
}

func helloMsg(id string, groups []string, caps []string) envelope.Hello {
	return envelope.Hello{
		ProtoVer: 1,
		MsgType:  envelope.MsgWorkerHello,
		WorkerID: id,
		Runtime:  "go",
		Codec:    "msgpack",
		Groups:   groups,
		Caps:     caps,
		Meta:     map[string]any{},
	}
}

// DefaultConfig

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxRetries != 2 {
		t.Errorf("MaxRetries = %d, want 2", cfg.MaxRetries)
	}
	if cfg.PoisonThreshold != 2 {
		t.Errorf("PoisonThreshold = %d, want 2", cfg.PoisonThreshold)
	}
	if cfg.PoisonPillTTL != time.Hour {
		t.Errorf("PoisonPillTTL = %v, want 1h", cfg.PoisonPillTTL)
	}
}

// RegisterWorker

func TestRegisterWorker(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu", "asr"}, []string{"speech.transcribe"}))

	if r.WorkerCountInGroup("gpu") != 1 {
		t.Errorf("gpu count = %d, want 1", r.WorkerCountInGroup("gpu"))
	}
	if r.WorkerCountInGroup("asr") != 1 {
		t.Errorf("asr count = %d, want 1", r.WorkerCountInGroup("asr"))
	}
}

func TestRegisterMultipleWorkers(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))
	r.RegisterWorker(helloMsg("w-2", []string{"gpu"}, nil))
	r.RegisterWorker(helloMsg("w-3", []string{"cpu"}, nil))

	if r.WorkerCountInGroup("gpu") != 2 {
		t.Errorf("gpu count = %d, want 2", r.WorkerCountInGroup("gpu"))
	}
	if r.WorkerCountInGroup("cpu") != 1 {
		t.Errorf("cpu count = %d, want 1", r.WorkerCountInGroup("cpu"))
	}
}

func TestWorkersInGroup(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))
	r.RegisterWorker(helloMsg("w-2", []string{"gpu"}, nil))

	ids := r.WorkersInGroup("gpu")
	if len(ids) != 2 {
		t.Errorf("WorkersInGroup = %v, want 2", ids)
	}
}

func TestWorkersInGroupEmpty(t *testing.T) {
	r, _, _ := newRouter(t)
	if len(r.WorkersInGroup("nonexistent")) != 0 {
		t.Error("nonexistent group should return empty slice")
	}
}

// MarkCapReady

func TestMarkCapReady(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, []string{"face.recognize"}))
	r.MarkCapReady("w-1", "face.recognize")

	// affinityWorker should now prefer w-1 for face.recognize
	best := r.affinityWorker("face.recognize", "gpu")
	if best != "w-1" {
		t.Errorf("affinity worker = %q, want w-1", best)
	}
}

// UpdateHeartbeat

func TestUpdateHeartbeat(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))

	ping := envelope.HbPing{
		WorkerID:       "w-1",
		Load:           42,
		RequestsServed: 100,
		UptimeMs:       5000,
	}
	r.UpdateHeartbeat(ping)

	r.mu.RLock()
	ws := r.workers["w-1"]
	r.mu.RUnlock()

	if ws.Load.Load() != 42 {
		t.Errorf("Load = %d, want 42", ws.Load.Load())
	}
	if ws.RequestsServed.Load() != 100 {
		t.Errorf("RequestsServed = %d, want 100", ws.RequestsServed.Load())
	}
}

func TestUpdateHeartbeatUnknownWorkerIsNoop(t *testing.T) {
	r, _, _ := newRouter(t)
	r.UpdateHeartbeat(envelope.HbPing{WorkerID: "unknown"})
}

// WorkerAlive

func TestWorkerAliveAfterRecentHeartbeat(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))
	r.UpdateHeartbeat(envelope.HbPing{WorkerID: "w-1"})
	if !r.WorkerAlive("w-1") {
		t.Error("worker should be alive after recent heartbeat")
	}
}

func TestWorkerAliveUnknown(t *testing.T) {
	r, _, _ := newRouter(t)
	if r.WorkerAlive("unknown") {
		t.Error("unknown worker should not be alive")
	}
}

// MarkWorkerDead

func TestMarkWorkerDead(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))
	r.MarkWorkerDead("w-1")

	if r.WorkerCountInGroup("gpu") != 0 {
		t.Errorf("gpu count = %d after dead, want 0", r.WorkerCountInGroup("gpu"))
	}
}

func TestMarkWorkerDeadUnknownIsNoop(t *testing.T) {
	r, _, _ := newRouter(t)
	r.MarkWorkerDead("nonexistent") // must not panic
}

// RemoveWorker

func TestRemoveWorker(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))
	r.RegisterWorker(helloMsg("w-2", []string{"gpu"}, nil))

	r.RemoveWorker("w-1")
	if r.WorkerCountInGroup("gpu") != 1 {
		t.Errorf("gpu count = %d, want 1 after remove", r.WorkerCountInGroup("gpu"))
	}
}

func TestRemoveWorkerUnknownIsNoop(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RemoveWorker("nonexistent")
}

// Dispatch

func makeEnv(cap, group string, dispatch envelope.Dispatch) envelope.Envelope {
	env := envelope.DefaultEnvelope()
	env.CorrID = "corr-001"
	env.OriginID = "origin-001"
	env.Cap = cap
	env.Group = group
	env.Dispatch = dispatch
	env.DeadlineMs = time.Now().Add(5 * time.Second).UnixMilli()
	return env
}

func TestDispatchAnyRequiresWorker(t *testing.T) {
	r, _, _ := newRouter(t)
	env := makeEnv("echo", "gpu", envelope.DispatchAny)
	err := r.Dispatch(context.Background(), env)
	if !errors.Is(err, ErrNoWorkers) {
		t.Errorf("err = %v, want ErrNoWorkers", err)
	}
}

func TestDispatchAnyWithWorker(t *testing.T) {
	r, b, p := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, []string{"echo"}))
	r.MarkCapReady("w-1", "echo")

	ch, _ := p.Register("corr-001")
	env := makeEnv("echo", "gpu", envelope.DispatchAny)
	err := r.Dispatch(context.Background(), env)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// Message should be on push queue
	data, ok := b.PopPush(bus.TopicPush("gpu"))
	if !ok {
		t.Fatal("expected message on push queue")
	}
	if data == nil {
		t.Error("message data should not be nil")
	}
	_ = ch
}

func TestDispatchAllPublishes(t *testing.T) {
	r, b, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	subCh, _ := b.Subscribe(ctx, bus.TopicPub("gpu"))

	env := makeEnv("echo", "gpu", envelope.DispatchAll)
	err := r.Dispatch(context.Background(), env)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	select {
	case <-subCh:
		// received on pub topic
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for pub message")
	}
}

func TestDispatchCancelledContext(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	env := makeEnv("echo", "gpu", envelope.DispatchAny)
	err := r.Dispatch(ctx, env)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestDispatchZeroDeadlineSafe(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, []string{"echo"}))
	r.MarkCapReady("w-1", "echo")

	env := makeEnv("echo", "gpu", envelope.DispatchAny)
	env.DeadlineMs = 0 // zero deadline — must not crash the reaper
	err := r.Dispatch(context.Background(), env)
	if err != nil {
		t.Fatalf("Dispatch with zero deadline: %v", err)
	}
}

// Poison pill

func TestPoisonPillAfterThresholdCrashes(t *testing.T) {
	r, _, p := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, []string{"echo"}))

	// Register a pending entry for originID
	ch, _ := p.Register("origin-pill")

	// Simulate threshold crashes
	r.recordCrash("origin-pill", "w-1", "echo")
	r.recordCrash("origin-pill", "w-1", "echo") // threshold=2 → poison declared

	// Pending should be failed
	select {
	case resp := <-ch:
		if resp.Err == nil {
			t.Error("expected error from poison pill")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for poison pill failure")
	}
}

func TestPoisonPillBlocksDispatch(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, []string{"echo"}))
	r.MarkCapReady("w-1", "echo")

	r.recordCrash("origin-blocked", "w-1", "echo")
	r.recordCrash("origin-blocked", "w-1", "echo") // declared

	env := makeEnv("echo", "gpu", envelope.DispatchAny)
	env.OriginID = "origin-blocked"
	err := r.Dispatch(context.Background(), env)
	if !errors.Is(err, ErrPoisonPill) {
		t.Errorf("err = %v, want ErrPoisonPill", err)
	}
}

func TestPoisonPillExpires(t *testing.T) {
	r, _, _ := newRouter(t)
	r.poisonPillTTL = 10 * time.Millisecond // very short TTL
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, []string{"echo"}))
	r.MarkCapReady("w-1", "echo")

	r.recordCrash("origin-expire", "w-1", "echo")
	r.recordCrash("origin-expire", "w-1", "echo")

	time.Sleep(50 * time.Millisecond)

	// After TTL, dispatch should succeed
	env := makeEnv("echo", "gpu", envelope.DispatchAny)
	env.OriginID = "origin-expire"
	err := r.Dispatch(context.Background(), env)
	if errors.Is(err, ErrPoisonPill) {
		t.Error("poison pill should have expired")
	}
}

// Poison pill with DLQ

func TestPoisonPillWritesToDLQ(t *testing.T) {
	dir := t.TempDir()
	fileDLQ := dlq.NewFile(dir)

	b := bus.NewMock()
	p := pending.New()
	reaper := jack.NewReaper(0,
		jack.ReaperWithShards(4),
		jack.ReaperWithHandler(func(_ context.Context, id string) {}),
	)
	t.Cleanup(reaper.Stop)

	cfg := DefaultConfig()
	cfg.Codec = codec.MsgPack
	cfg.DLQ = fileDLQ
	r := New(b, p, reaper, cfg, ll.New("test"))

	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, []string{"audio.denoise"}))
	p.Register("origin-dlq")

	r.recordCrash("origin-dlq", "w-1", "audio.denoise")
	r.recordCrash("origin-dlq", "w-1", "audio.denoise")

	ids, err := fileDLQ.List()
	if err != nil {
		t.Fatalf("DLQ.List: %v", err)
	}
	if len(ids) != 1 {
		t.Errorf("DLQ has %d entries, want 1", len(ids))
	}
}

// BroadcastCancel

func TestBroadcastCancel(t *testing.T) {
	r, b, _ := newRouter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	subCh, _ := b.Subscribe(ctx, bus.TopicBroadcast)

	r.BroadcastCancel("origin-001")

	select {
	case msg := <-subCh:
		if msg.Topic != bus.TopicBroadcast {
			t.Errorf("topic = %q, want broadcast", msg.Topic)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for broadcast cancel")
	}
}

// leastBusy affinity

func TestLeastBusyPicksLowestLoad(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))
	r.RegisterWorker(helloMsg("w-2", []string{"gpu"}, nil))

	r.mu.RLock()
	r.workers["w-1"].Load.Store(80)
	r.workers["w-2"].Load.Store(20)
	r.mu.RUnlock()

	best := r.leastBusy([]string{"w-1", "w-2"}, "gpu")
	if best != "w-2" {
		t.Errorf("leastBusy = %q, want w-2 (lower load)", best)
	}
}

func TestLeastBusySkipsDeadWorkers(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))
	r.RegisterWorker(helloMsg("w-2", []string{"gpu"}, nil))
	r.MarkWorkerDead("w-1")

	best := r.leastBusy([]string{"w-1", "w-2"}, "gpu")
	if best != "w-2" {
		t.Errorf("leastBusy = %q, want w-2 (w-1 is dead)", best)
	}
}

func TestLeastBusyNoReadyWorkers(t *testing.T) {
	r, _, _ := newRouter(t)
	best := r.leastBusy([]string{}, "gpu")
	if best != "" {
		t.Errorf("leastBusy = %q, want empty (no workers)", best)
	}
}

// ResolveInflight

func TestResolveInflight(t *testing.T) {
	r, _, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"gpu"}, nil))

	env := makeEnv("echo", "gpu", envelope.DispatchAny)
	env.WorkerID = "w-1"
	r.trackInFlight(env)

	r.ResolveInflight("corr-001", "origin-001", "w-1")
	// Must not panic; inflight removed
}

// containsGroup helper

func TestContainsGroup(t *testing.T) {
	if !containsGroup([]string{"gpu", "asr"}, "gpu") {
		t.Error("should contain gpu")
	}
	if containsGroup([]string{"gpu", "asr"}, "cpu") {
		t.Error("should not contain cpu")
	}
	if containsGroup(nil, "gpu") {
		t.Error("nil slice should not contain anything")
	}
}
