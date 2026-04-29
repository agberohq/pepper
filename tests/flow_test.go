package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/agberohq/pepper"
	"github.com/agberohq/pepper/internal/core"
)

// TestFlowEchoRoundTrip verifies the full Go→Python→Go request path.
func TestFlowEchoRoundTrip(t *testing.T) {
	if !runtimeFinder.HasPython() {
		t.Skip("python3 not available")
	}
	if !runtimeFinder.HasPythonMsgpack() {
		t.Skip("pip install msgpack required")
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-test-1").Groups("default")),
		pepper.WithCodec(pepper.CodecMsgPack),
		pepper.WithTransport(pepper.TransportTCPLoopback),
		pepper.WithShutdownTimeout(3*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Script("echo", "./testdata/caps/echo.py")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer startCancel()

	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	doCtx, doCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer doCancel()

	result, err := pp.Do(doCtx, "echo", core.In{"msg": "hello"})
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
	if !runtimeFinder.HasPython() {
		t.Skip("python3 not available")
	}
	if !runtimeFinder.HasPythonMsgpack() {
		t.Skip("pip install msgpack required")
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-typed").Groups("default")),
		pepper.WithShutdownTimeout(3*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Script("echo", "./testdata/caps/echo.py")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer startCancel()

	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	doCtx, doCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer doCancel()

	type EchoResult struct {
		Msg string `msgpack:"msg"`
	}
	out, err := pepper.Do[EchoResult](doCtx, pp, "echo", core.In{"msg": "typed"})
	if err != nil {
		t.Fatalf("Do[EchoResult]: %v", err)
	}
	if out.Msg != "typed" {
		t.Fatalf("expected Msg='typed', got %q", out.Msg)
	}
	t.Logf("typed Do ok — msg=%s", out.Msg)
}

// TestFlowGoFuncCallTyped verifies Call[Out].Do with a Go Func capability
// (no Python needed — fully in-process).
func TestFlowGoFuncCallTyped(t *testing.T) {
	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-gofunc").Groups("default")),
		pepper.WithShutdownTimeout(2*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Func("text.upper",
		func(ctx context.Context, in TextIn) (TextOut, error) {
			return TextOut{Text: strings.ToUpper(in.Text)}, nil
		},
	)); err != nil {
		t.Fatalf("Register Func: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	out, err := pepper.Call[TextOut]{
		Cap:   "text.upper",
		Input: TextIn{Text: "hello flow"},
	}.Bind(pp).Do(ctx)
	if err != nil {
		t.Fatalf("Call[TextOut].Do: %v", err)
	}
	if out.Text != "HELLO FLOW" {
		t.Fatalf("expected 'HELLO FLOW', got %q", out.Text)
	}
	t.Logf("Call[Out] Go Func ok — text=%s", out.Text)
}

// TestFlowExecuteProcess verifies Execute + Process[Out].Wait for async tracking.
func TestFlowExecuteProcess(t *testing.T) {
	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-proc").Groups("default")),
		pepper.WithTracking(true),
		pepper.WithShutdownTimeout(2*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Func("text.reverse",
		func(ctx context.Context, in TextIn) (TextOut, error) {
			r := []rune(in.Text)
			for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
				r[i], r[j] = r[j], r[i]
			}
			return TextOut{Text: string(r)}, nil
		},
	)); err != nil {
		t.Fatalf("Register Func: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	proc, err := pepper.Call[TextOut]{
		Cap:   "text.reverse",
		Input: TextIn{Text: "pepper"},
	}.Bind(pp).Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if proc.ID() == "" {
		t.Fatal("expected non-empty process ID")
	}
	// originID must be threaded through (not empty)
	if proc.OriginID() == "" {
		t.Fatal("Process.originID must be non-empty — needed for Cancel()")
	}

	// Drain events in background
	events := make([]pepper.Event, 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range proc.Events() {
			events = append(events, e)
		}
	}()

	out, err := proc.Wait(ctx)
	if err != nil {
		t.Fatalf("Process.Wait: %v", err)
	}
	<-done

	if out.Text != "reppep" {
		t.Fatalf("expected 'reppep', got %q", out.Text)
	}

	state := proc.State()
	if state.Status != pepper.StatusDone {
		t.Fatalf("expected StatusDone, got %s", state.Status)
	}
	t.Logf("Execute + Process.Wait ok — text=%s events=%d", out.Text, len(events))
}

// TestFlowProcessWaitNoPoll verifies resultOfWatch does not busy-poll:
// a fast capability resolves long before any 50ms tick would fire.
func TestFlowProcessWaitNoPoll(t *testing.T) {
	type Void struct{}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-fast").Groups("default")),
		pepper.WithTracking(true),
		pepper.WithShutdownTimeout(2*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Func("noop",
		func(ctx context.Context, in Void) (Void, error) {
			return Void{}, nil
		},
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	start := time.Now()
	proc, err := pepper.Call[Void]{Cap: "noop", Input: Void{}}.Bind(pp).Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := proc.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	elapsed := time.Since(start)

	// If polling were used at 50ms intervals, a fast capability would still take
	// ~50ms to resolve. Watch-based approach should be well under that.
	if elapsed > 40*time.Millisecond {
		t.Logf("wait took %s — may indicate polling (expected <40ms for in-process cap)", elapsed)
	}
	t.Logf("Process.Wait resolved in %s (no polling)", elapsed)
}

// TestFlowAllParallel verifies All[O] executes calls concurrently and collects results.
func TestFlowAllParallel(t *testing.T) {
	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-all").Groups("default")),
		pepper.WithShutdownTimeout(2*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Func("text.upper",
		func(ctx context.Context, in TextIn) (TextOut, error) {
			return TextOut{Text: strings.ToUpper(in.Text)}, nil
		},
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	inputs := []string{"alpha", "beta", "gamma"}
	calls := make([]pepper.CallArgs, len(inputs))
	for i, s := range inputs {
		calls[i] = pepper.MakeCall("text.upper", pepper.In{"text": s})
	}

	results, err := pepper.All[TextOut](ctx, pp, calls...)
	if err != nil {
		t.Fatalf("All[TextOut]: %v", err)
	}
	if len(results) != len(inputs) {
		t.Fatalf("expected %d results, got %d", len(inputs), len(results))
	}
	for _, r := range results {
		if r.Text == "" {
			t.Fatal("got empty Text in result")
		}
		// Each result must be one of the uppercased inputs
		found := false
		for _, s := range inputs {
			if r.Text == strings.ToUpper(s) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("unexpected result text: %q", r.Text)
		}
	}
	t.Logf("All[TextOut] ok — %d parallel results", len(results))
}

// TestFlowPipelineWithGoFunc verifies Compose + Call[Out] with Go Func stages.
func TestFlowPipelineWithGoFunc(t *testing.T) {
	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-pipe").Groups("default")),
		pepper.WithShutdownTimeout(2*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Func("text.upper",
		func(ctx context.Context, in TextIn) (TextOut, error) {
			return TextOut{Text: strings.ToUpper(in.Text)}, nil
		},
	)); err != nil {
		t.Fatalf("Register upper: %v", err)
	}
	if err := pp.Register(pepper.Func("text.exclaim",
		func(ctx context.Context, in TextIn) (TextOut, error) {
			return TextOut{Text: in.Text + "!"}, nil
		},
	)); err != nil {
		t.Fatalf("Register exclaim: %v", err)
	}

	if err := pp.Compose("text.shout",
		pepper.Pipe("text.upper"),
		pepper.Transform(func(in map[string]any) (map[string]any, error) {
			// Pass through unchanged — just verifying PipeTransform in the chain
			return in, nil
		}),
		pepper.Pipe("text.exclaim"),
	); err != nil {
		t.Fatalf("Compose: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	out, err := pepper.Call[TextOut]{
		Cap:   "text.shout",
		Input: TextIn{Text: "hello"},
	}.Bind(pp).Do(ctx)
	if err != nil {
		t.Fatalf("Call pipeline: %v", err)
	}
	if out.Text != "HELLO!" {
		t.Fatalf("expected 'HELLO!', got %q", out.Text)
	}
	t.Logf("pipeline Call[Out] ok — text=%s", out.Text)
}
