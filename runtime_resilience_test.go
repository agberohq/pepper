package pepper

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/envelope"
)

// TestRecycleTargetsOneWorker pins the fix for a recycle shutting down the fleet:
// worker_bye must go to the worker's own control topic, and carry its id.
func TestRecycleTargetsOneWorker(t *testing.T) {
	pp, m := newCapLoadTestPepper(t, nil)
	pp.cfg.Codec = CodecMsgPack

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch, err := m.Subscribe(ctx, bus.TopicControl("w-7"))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	pp.checkRecycle("w-7", WorkerConfig{ID: "w-7", MaxRequests: 2}, 2, 0)

	select {
	case msg := <-ch:
		var env envelope.Envelope
		if err := codec.MsgPack.Unmarshal(msg.Data, &env); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if env.MsgType != envelope.MsgWorkerBye {
			t.Errorf("msg_type = %q, want %q", env.MsgType, envelope.MsgWorkerBye)
		}
		if env.WorkerID != "w-7" {
			t.Errorf("worker_id = %q, want w-7", env.WorkerID)
		}
	case <-ctx.Done():
		t.Fatal("no worker_bye published on the worker's control topic")
	}
}

// TestRecycleSkippedBelowLimit guards the inverse: no bye before the threshold.
func TestRecycleSkippedBelowLimit(t *testing.T) {
	pp, m := newCapLoadTestPepper(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ch, err := m.Subscribe(ctx, bus.TopicControl("w-8"))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	pp.checkRecycle("w-8", WorkerConfig{ID: "w-8", MaxRequests: 10}, 3, 0)

	select {
	case <-ch:
		t.Fatal("worker_bye published before MaxRequests was reached")
	case <-ctx.Done():
	}
}

// TestScriptRuntimeRejectsJSONCodec verifies the fail-fast: a JSON codec cannot
// serve script workers, so registration must refuse instead of letting every
// request time out.
func TestScriptRuntimeRejectsJSONCodec(t *testing.T) {
	pp, err := New(WithCodec(CodecJSON))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = pp.Register(Script("echo", "./caps/echo.py"))
	if err == nil {
		t.Fatal("expected Register to reject a script capability with CodecJSON")
	}
	if !strings.Contains(err.Error(), "msgpack only") {
		t.Errorf("error should explain the codec mismatch, got: %v", err)
	}

	// Non-script runtimes are unaffected: Go functions never touch the wire codec.
	if err := pp.Register(Func("math.sum", func(_ context.Context, in SumInTest) (SumOutTest, error) {
		return SumOutTest{}, nil
	})); err != nil {
		t.Errorf("Register(Func) with CodecJSON should be allowed: %v", err)
	}
}

type SumInTest struct{}
type SumOutTest struct{}

// TestComposeRejectsUnexecutedStages verifies that a pipeline the router cannot
// run is rejected at registration instead of half-running at request time.
func TestComposeRejectsUnexecutedStages(t *testing.T) {
	pp, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = pp.Compose("bad.pipeline",
		Pipe("text.upper"),
		PipeScatter("fan.out").Gather("gather.results", "all"),
	)
	if err == nil {
		t.Fatal("expected Compose to reject a scatter stage")
	}
	if !strings.Contains(err.Error(), "not executable yet") {
		t.Errorf("error should name the unsupported stage, got: %v", err)
	}

	// The supported combination still composes.
	if err := pp.Compose("good.pipeline",
		Pipe("text.upper"),
		Transform(func(in map[string]any) (map[string]any, error) { return in, nil }),
		PipeReturn(map[string]any{"ok": true}),
	); err != nil {
		t.Errorf("Compose with Pipe/Transform/PipeReturn failed: %v", err)
	}
}

// TestCircuitBreakerOptionReachesConfig ensures the option is wired (it used to be
// silently dropped, leaving the breaker permanently disabled).
func TestCircuitBreakerOptionReachesConfig(t *testing.T) {
	pp, err := New(WithCircuitBreaker(7, 45*time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if pp.cfg.CircuitBreakerThreshold != 7 {
		t.Errorf("threshold = %d, want 7", pp.cfg.CircuitBreakerThreshold)
	}
	if pp.cfg.CircuitBreakerTimeout != 45*time.Second {
		t.Errorf("timeout = %v, want 45s", pp.cfg.CircuitBreakerTimeout)
	}

	// Off by default.
	def, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if def.cfg.CircuitBreakerThreshold != 0 {
		t.Errorf("default threshold = %d, want 0 (disabled)", def.cfg.CircuitBreakerThreshold)
	}
}
