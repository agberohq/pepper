// Package pepper integration tests.
//
// Run:
//
//	go test ./... -run TestEchoRoundTrip -v -timeout 30s
//
// Requires Python 3 with msgpack:
//
//	pip install msgpack
package pepper

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/core"
)

func writeTestCap(path, src string) {
	if _, err := os.Stat(path); err == nil {
		return
	}
	_ = os.WriteFile(path, []byte(src), 0644)
}

const echoCapSource = `# pepper:name    = echo
# pepper:version = 1.0.0
# pepper:groups  = default

def run(inputs):
    return {"msg": inputs.get("msg", "")}
`

const reverseCapSource = `# pepper:name    = reverse
# pepper:version = 1.0.0
# pepper:groups  = default

def run(inputs):
    text = inputs.get("text", "")
    return {"text": text[::-1]}
`

// TestMain writes test capability sources to testdata/caps/ before running tests.
func TestMain(m *testing.M) {
	if err := os.MkdirAll("testdata/caps", 0755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir testdata/caps: %v\n", err)
		os.Exit(1)
	}
	writeTestCap("testdata/caps/echo.py", echoCapSource)
	writeTestCap("testdata/caps/reverse.py", reverseCapSource)
	os.Exit(m.Run())
}

// TestMultipleCapabilities registers two capabilities and calls both.
func TestMultipleCapabilities(t *testing.T) {
	if !pythonAvailable() {
		t.Skip("python3 not available")
	}
	if !msgpackAvailable() {
		t.Skip("pip install msgpack required")
	}

	pp, err := New(
		WithWorkers(NewWorker("w-multi").Groups("default")),
		WithShutdownTimeout(3*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(Script("echo", "./testdata/caps/echo.py")); err != nil {
		t.Fatalf("Register echo: %v", err)
	}
	if err := pp.Register(Script("reverse", "./testdata/caps/reverse.py")); err != nil {
		t.Fatalf("Register reverse: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	r1, err := pp.Do(ctx, "echo", core.In{"msg": "hello"})
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	if r1.AsJSON()["msg"] != "hello" {
		t.Fatalf("echo unexpected: %v", r1.AsJSON())
	}

	r2, err := pp.Do(ctx, "reverse", core.In{"text": "hello"})
	if err != nil {
		t.Fatalf("reverse: %v", err)
	}
	if r2.AsJSON()["text"] != "olleh" {
		t.Fatalf("reverse unexpected: %v", r2.AsJSON())
	}
	t.Log("both capabilities ok")
}

// TestContextCancellation verifies that cancelling the Go context returns ctx.Err().
func TestContextCancellation(t *testing.T) {
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Do

	_, err = pp.Do(ctx, "any.cap", core.In{})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	t.Logf("context cancel propagated: %v", err)
}

// TestStreamChunksCompiles verifies the streaming API compiles correctly.
func TestStreamChunksCompiles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &Stream{}
	ch := s.Chunks(ctx)
	for range ch {
	}
	t.Log("stream API compiles correctly")
}

// TestWorkerBuilderInterface verifies WithWorkers() accepts *WorkerBuilder directly.
func TestWorkerBuilderInterface(t *testing.T) {
	pp, err := New(
		WithWorkers(
			NewWorker("w-1").Groups("gpu"),
			NewWorker("w-2").Groups("cpu"),
			NewWorker("w-3").Groups("gpu", "asr").MaxRequests(10000).MaxUptime(24*time.Hour),
		),
		WithShutdownTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if len(pp.cfg.Workers) != 3 {
		t.Fatalf("expected 3 workers, got %d", len(pp.cfg.Workers))
	}
	if pp.cfg.Workers[0].ID != "w-1" {
		t.Fatalf("expected w-1, got %s", pp.cfg.Workers[0].ID)
	}
	t.Log("WorkerBuilder interface ok")
}

// TestCapabilityRegistration verifies pepper.Script registers and appears in Capabilities.
func TestCapabilityRegistration(t *testing.T) {
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	// Python via new unified Register
	if err := pp.Register(Script("echo", "./testdata/caps/echo.py")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	schemas := pp.Capabilities(context.Background())
	if len(schemas) == 0 {
		t.Fatal("expected at least one capability after Register")
	}

	found := false
	for _, s := range schemas {
		if s.Name == "echo" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("echo not found in capabilities")
	}
	t.Logf("registration ok — %d capabilities", len(schemas))
}

// TestFuncRegistration verifies pepper.Func registers a Go capability inline.
func TestFuncRegistration(t *testing.T) {
	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}

	pp, err := New(
		WithWorkers(NewWorker("w-func").Groups("default")),
		WithShutdownTimeout(time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(Func("text.upper",
		func(ctx context.Context, in TextIn) (TextOut, error) {
			return TextOut{Text: strings.ToUpper(in.Text)}, nil
		},
	)); err != nil {
		t.Fatalf("Register Func: %v", err)
	}

	schemas := pp.Capabilities(context.Background())
	found := false
	for _, s := range schemas {
		if s.Name == "text.upper" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("text.upper not found in capabilities")
	}
	t.Log("Func registration ok")
}

// TestCallTyped verifies Call[Out].Do round-trips a Go capability with typed output.
func TestCallTyped(t *testing.T) {
	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}

	pp, err := New(
		WithWorkers(NewWorker("w-call").Groups("default")),
		WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(Func("text.upper",
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

	// Typed struct input
	out, err := Call[TextOut]{
		Cap:   "text.upper",
		Input: TextIn{Text: "hello world"},
	}.Bind(pp).Do(ctx)
	if err != nil {
		t.Fatalf("Call[TextOut].Do: %v", err)
	}
	if out.Text != "HELLO WORLD" {
		t.Fatalf("expected 'HELLO WORLD', got %q", out.Text)
	}

	// Wire-map input also works
	out2, err := Call[TextOut]{
		Cap:   "text.upper",
		Input: In{"text": "wire input"},
	}.Bind(pp).Do(ctx)
	if err != nil {
		t.Fatalf("Call[TextOut].Do (wire): %v", err)
	}
	if out2.Text != "WIRE INPUT" {
		t.Fatalf("expected 'WIRE INPUT', got %q", out2.Text)
	}
	t.Log("Call[Out] typed round-trip ok")
}

// TestExec verifies Exec.Do dispatches without returning output.
func TestExec(t *testing.T) {
	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}

	pp, err := New(
		WithWorkers(NewWorker("w-exec").Groups("default")),
		WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(Func("text.noop",
		func(ctx context.Context, in TextIn) (TextOut, error) {
			return TextOut{Text: in.Text}, nil
		},
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := (Exec{Cap: "text.noop", Input: In{"text": "fire"}}.Bind(pp).Do(ctx)); err != nil {
		t.Fatalf("Exec.Do: %v", err)
	}
	t.Log("Exec ok")
}

// TestMakeCallArgs verifies MakeCall returns CallArgs (not the old Call type).
func TestMakeCallArgs(t *testing.T) {
	args := MakeCall("my.cap", In{"key": "val"}, WithCallGroup("gpu"))
	if args.Cap != "my.cap" {
		t.Fatalf("expected cap='my.cap', got %q", args.Cap)
	}
	if args.In["key"] != "val" {
		t.Fatalf("expected in.key='val'")
	}
	if len(args.Opts) != 1 {
		t.Fatalf("expected 1 opt, got %d", len(args.Opts))
	}
	t.Log("MakeCall / CallArgs ok")
}

// TestToWireInputAliasNoBranch verifies that core.In (a map[string]any alias)
// passes through toWireInput without the JSON round-trip path.
func TestToWireInputAliasNoBranch(t *testing.T) {
	in := In{"a": "1", "b": float64(2)}
	out, err := toWireInput(in)
	if err != nil {
		t.Fatalf("toWireInput: %v", err)
	}
	if out["a"] != "1" || out["b"] != float64(2) {
		t.Fatalf("unexpected values: %v", out)
	}

	// nil → empty map
	empty, err := toWireInput(nil)
	if err != nil {
		t.Fatalf("toWireInput(nil): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty map for nil input")
	}
	t.Log("toWireInput alias handling ok")
}

// TestPipelineNoInternalImports verifies that pipeline helpers compile from the
// root package alone (no internal/compose imports needed by callers).
func TestPipelineNoInternalImports(t *testing.T) {
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	type In2 = map[string]any

	err = pp.Compose("test.pipeline",
		Pipe("stage.one"),
		Transform(func(in In2) (In2, error) {
			return In2{"result": "transformed"}, nil
		}),
		PipeTransformWithEnv(func(env EnvelopeInfo, in In2) (In2, error) {
			// Can read CorrID, SessionID, Meta — no internal imports
			_ = env.CorrID
			_ = env.SessionID
			return in, nil
		}),
		PipeReturn(In2{"done": true}),
	)
	if err != nil {
		// Compose may fail validation (no registered stages), that's fine —
		// we're only testing that the API compiles and accepts these stage types.
		t.Logf("Compose validation (expected for unregistered caps): %v", err)
	}
	t.Log("pipeline API compiles with no internal imports")
}

// TestProcessCancelFields verifies Process[Out] stores originID correctly.
func TestProcessCancelFields(t *testing.T) {
	proc := &Process[struct{}]{
		id:       "proc-123",
		originID: "origin-456",
		pp:       &Pepper{}, // nil rt — Cancel() must guard against nil
	}
	// Cancel should not panic even when rt is nil (not started)
	proc.Cancel()
	t.Log("Process.Cancel nil-rt guard ok")
}

// TestDeprecatedTrackStillWorks verifies the deprecated Track method still compiles
// and is wired through to the internal track() implementation.
func TestExecuteAndProcess(t *testing.T) {
	pp, err := New(
		WithWorkers(NewWorker("w-dep").Groups("default")),
		WithTracking(true),
		WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}
	if err := pp.Register(Func("text.id",
		func(ctx context.Context, in TextIn) (TextOut, error) {
			return TextOut{Text: in.Text}, nil
		},
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	proc, err := Call[TextOut]{Cap: "text.id", Input: TextIn{Text: "dep"}}.
		Bind(pp).Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if proc.ID() == "" {
		t.Fatal("expected non-empty process ID")
	}

	out, err := proc.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if out.Text != "dep" {
		t.Fatalf("expected Text='dep', got %q", out.Text)
	}
	t.Logf("Execute/Wait ok — processID=%s", proc.ID())
}

// TestSessionTouch verifies Touch() replaces the old TTL(d) method.
func TestSessionTouch(t *testing.T) {
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	sess := pp.Session("s-touch")
	if err := sess.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// Touch must not panic or error
	if err := sess.Touch(); err != nil {
		t.Fatalf("Touch: %v", err)
	}
	if v, ok := sess.Get("k"); !ok || v != "v" {
		t.Fatalf("expected k=v after Touch, got %v %v", v, ok)
	}
	t.Log("Session.Touch ok")
}

// TestBoundCallGroupCompiles verifies BoundCall[O].Group compiles.
func TestBoundCallGroupCompiles(t *testing.T) {
	type Out struct {
		V string `json:"v"`
	}
	// Just confirm it compiles and returns an error (no workers running)
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = Call[Out]{Cap: "noop", Input: In{}}.Bind(pp).Group(ctx, "default", DispatchAny)
	t.Log("BoundCall.Group compiles")
}

// TestBoundCallBroadcastCompiles verifies BoundCall[O].Broadcast compiles.
func TestBoundCallBroadcastCompiles(t *testing.T) {
	type Out struct {
		V string `json:"v"`
	}
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = Call[Out]{Cap: "noop", Input: In{}}.Bind(pp).Broadcast(ctx)
	t.Log("BoundCall.Broadcast compiles")
}

// TestSessionDo verifies Call[Out].Session(sess).Do(ctx) injects session ID and decodes result.
func TestSessionDo(t *testing.T) {
	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}

	pp, err := New(
		WithWorkers(NewWorker("w-sess").Groups("default")),
		WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(Func("text.upper",
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

	sess := pp.Session("user-99")
	out, err := Call[TextOut]{
		Cap:   "text.upper",
		Input: TextIn{Text: "hello session"},
	}.Session(sess).Do(ctx)
	if err != nil {
		t.Fatalf("DoSession: %v", err)
	}
	if out.Text != "HELLO SESSION" {
		t.Fatalf("expected 'HELLO SESSION', got %q", out.Text)
	}
	t.Logf("Call[Out].Session(sess).Do(ctx) ok — text=%s", out.Text)
}

// TestBoundCallStreamCompiles verifies BoundCall[O].Stream compiles correctly.
func TestBoundCallStreamCompiles(t *testing.T) {
	type TokenChunk struct {
		Token string `json:"token"`
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()
	// Will fail with context cancelled — we just want compile + no panic
	_, _ = Call[TokenChunk]{Cap: "noop.stream", Input: In{}}.Bind(pp).Stream(ctx)
	t.Log("BoundCall.Stream compiles")
}

// TestOpenStreamCompiles verifies OpenStream[In, Out] compiles.
func TestOpenStreamCompiles(t *testing.T) {
	type AudioIn struct {
		Data []byte `json:"data"`
	}
	type TranscriptOut struct {
		Text string `json:"text"`
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()
	// Will fail — we just verify it compiles
	_, _ = OpenStream[AudioIn, TranscriptOut](ctx, pp, "speech.transcribe", AudioIn{})
	t.Log("OpenStream[In,Out] compiles")
}

// TestFilterByRuntimeTyped verifies FilterByRuntime accepts registry.Runtime.
func TestFilterByRuntimeTyped(t *testing.T) {
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}
	_ = pp.Register(Func("text.upper", func(_ context.Context, in TextIn) (TextOut, error) {
		return TextOut{Text: in.Text}, nil
	}))

	schemas := pp.Capabilities(context.Background(), FilterByRuntime("go"))
	if len(schemas) == 0 {
		t.Fatal("expected at least one Go capability")
	}
	for _, s := range schemas {
		if s.Runtime != "go" {
			t.Fatalf("FilterByRuntime(go) returned runtime=%q", s.Runtime)
		}
	}
	t.Logf("FilterByRuntime ok — %d Go capabilities", len(schemas))
}

// TestToolsOpenAI verifies Tools[T] with FormatOpenAI produces typed structs.
func TestToolsOpenAI(t *testing.T) {
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}
	if err := pp.Register(Func("text.upper",
		func(_ context.Context, in TextIn) (TextOut, error) {
			return TextOut{}, nil
		},
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	schemas := pp.Capabilities(context.Background())
	if len(schemas) == 0 {
		t.Fatal("expected at least one capability")
	}

	// Typed output — no map[string]any assertions
	tools := Tools(schemas, FormatOpenAI)
	if len(tools) != len(schemas) {
		t.Fatalf("expected %d tools, got %d", len(schemas), len(tools))
	}
	for _, tool := range tools {
		if tool.Type != "function" {
			t.Fatalf("expected type=function, got %q", tool.Type)
		}
		if tool.Function.Name == "" {
			t.Fatal("expected non-empty function name")
		}
	}
	t.Logf("Tools[OpenAITool] ok — %d tools, first=%s", len(tools), tools[0].Function.Name)
}

// TestToolsAnthropic verifies Tools[T] with FormatAnthropic produces typed structs.
func TestToolsAnthropic(t *testing.T) {
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}
	if err := pp.Register(Func("text.upper",
		func(_ context.Context, in TextIn) (TextOut, error) {
			return TextOut{}, nil
		},
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	schemas := pp.Capabilities(context.Background())
	tools := Tools(schemas, FormatAnthropic)
	for _, tool := range tools {
		if tool.Name == "" {
			t.Fatal("expected non-empty tool name")
		}
		// InputSchema defaults to empty object when nil — never nil
		if tool.InputSchema == nil {
			t.Fatal("expected non-nil InputSchema")
		}
	}
	t.Logf("Tools[AnthropicTool] ok — %d tools", len(tools))
}

// TestToolsCustomFormatter verifies an inline custom formatter works.
func TestToolsCustomFormatter(t *testing.T) {
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	type TextIn struct {
		Text string `json:"text"`
	}
	type TextOut struct {
		Text string `json:"text"`
	}
	if err := pp.Register(Func("text.upper",
		func(_ context.Context, in TextIn) (TextOut, error) {
			return TextOut{}, nil
		},
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	type CustomTool struct {
		ID     string
		Schema map[string]any
	}

	schemas := pp.Capabilities(context.Background())
	tools := Tools(schemas, func(s Schema) CustomTool {
		return CustomTool{ID: s.Name, Schema: s.InputSchema}
	})
	if len(tools) == 0 {
		t.Fatal("expected at least one custom tool")
	}
	if tools[0].ID == "" {
		t.Fatal("expected non-empty ID")
	}
	t.Logf("Tools[CustomTool] ok — %d tools", len(tools))
}

// TestSessionFromContext verifies that SessionFromContext retrieves the session ID
// inside a Go Func capability invoked through a Session.
func TestSessionFromContext(t *testing.T) {
	type In struct{}
	type Out struct {
		SessionID string `json:"session_id"`
	}

	pp, err := New(
		WithWorkers(NewWorker("w-sessctx").Groups("default")),
		WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(Func("echo.session",
		func(ctx context.Context, in In) (Out, error) {
			return Out{SessionID: SessionFromContext(ctx)}, nil
		},
	)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pp.Start(cctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	sess := pp.NewSession()
	if sess.ID() == "" {
		t.Fatal("NewSession should produce a non-empty ID")
	}

	out, err := Call[Out]{Cap: "echo.session", Input: In{}}.
		Session(sess).
		Do(cctx)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if out.SessionID != sess.ID() {
		t.Fatalf("expected session ID %q in ctx, got %q", sess.ID(), out.SessionID)
	}
	t.Logf("SessionFromContext ok — capability saw session=%s", out.SessionID)
}

// TestNewSession verifies NewSession generates a non-empty unique ID.
func TestNewSession(t *testing.T) {
	pp, err := New(WithShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	s1 := pp.NewSession()
	s2 := pp.NewSession()

	if s1.ID() == "" {
		t.Fatal("NewSession: empty ID")
	}
	if s1.ID() == s2.ID() {
		t.Fatal("NewSession: duplicate IDs")
	}

	// Resume by ID
	resumed := pp.Session(s1.ID())
	if resumed.ID() != s1.ID() {
		t.Fatalf("Session(id): expected %s, got %s", s1.ID(), resumed.ID())
	}
	t.Logf("NewSession ok — id=%s", s1.ID())
}

// helpers

func pythonAvailable() bool {
	_, err := exec.LookPath("python3")
	return err == nil
}

func msgpackAvailable() bool {
	return exec.Command("python3", "-c", "import msgpack").Run() == nil
}
