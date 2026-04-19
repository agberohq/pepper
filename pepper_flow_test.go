package pepper

import (
	"context"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/core"
	"github.com/olekukonko/ll"
)

var (
	testLogger = ll.New("test").Enable()
)

// TestFlowEchoRoundTrip verifies the full Go→Python→Go request path.
func TestFlowEchoRoundTrip(t *testing.T) {
	if !pythonAvailable() {
		t.Skip("python3 not available")
	}
	if !msgpackAvailable() {
		t.Skip("pip install msgpack required")
	}

	pp, err := New(
		WithWorkers(NewWorker("w-test-1").Groups("default")),
		WithSerializer(MsgPack),
		WithTransport(TCPLoopback),
		WithShutdownTimeout(3*time.Second),
		WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register("echo", "./testdata/caps/echo.py"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	result, err := pp.Do(ctx, "echo", core.In{"msg": "hello"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	out := result.AsJSON()
	msg, ok := out["msg"].(string)
	if !ok {
		t.Fatalf("expected 'msg' string in result, got: %#v", out)
	}
	if msg != "hello" {
		t.Fatalf("expected msg='hello', got %q", msg)
	}
	t.Logf("round trip ok — worker=%s latency=%s", result.WorkerID, result.Latency)
}

// TestFlowTypedDo verifies the generic Do[O] free function compiles and decodes.
func TestFlowTypedDo(t *testing.T) {
	if !pythonAvailable() {
		t.Skip("python3 not available")
	}
	if !msgpackAvailable() {
		t.Skip("pip install msgpack required")
	}

	pp, err := New(
		WithWorkers(NewWorker("w-typed").Groups("default")),
		WithShutdownTimeout(3*time.Second),
		WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register("echo", "./testdata/caps/echo.py"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	type EchoResult struct {
		Msg string `msgpack:"msg"`
	}
	out, err := Do[EchoResult](ctx, pp, "echo", core.In{"msg": "typed"})
	if err != nil {
		t.Fatalf("Do[EchoResult]: %v", err)
	}
	if out.Msg != "typed" {
		t.Fatalf("expected Msg='typed', got %q", out.Msg)
	}
	t.Logf("typed Do ok — msg=%s", out.Msg)
}
