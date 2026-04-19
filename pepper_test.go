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
	"testing"
	"time"
)

// TestEchoRoundTrip verifies the full Go→Python→Go request path.
func TestEchoRoundTrip(t *testing.T) {
	if !pythonAvailable() {
		t.Skip("python3 not available")
	}
	if !msgpackAvailable() {
		t.Skip("pip install msgpack required")
	}

	pp, err := New(
		Workers(NewWorker("w-test-1").Groups("default")),
		WithSerializer(MsgPack),
		WithTransport(TCPLoopback),
		ShutdownTimeout(3*time.Second),
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

	result, err := pp.Do(ctx, "echo", In{"msg": "hello"})
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

// TestTypedDo verifies the generic Do[O] free function compiles and decodes.
func TestTypedDo(t *testing.T) {
	if !pythonAvailable() {
		t.Skip("python3 not available")
	}
	if !msgpackAvailable() {
		t.Skip("pip install msgpack required")
	}

	pp, err := New(
		Workers(NewWorker("w-typed").Groups("default")),
		ShutdownTimeout(3*time.Second),
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
	out, err := Do[EchoResult](ctx, pp, "echo", In{"msg": "typed"})
	if err != nil {
		t.Fatalf("Do[EchoResult]: %v", err)
	}
	if out.Msg != "typed" {
		t.Fatalf("expected Msg='typed', got %q", out.Msg)
	}
	t.Logf("typed Do ok — msg=%s", out.Msg)
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
		Workers(NewWorker("w-multi").Groups("default")),
		ShutdownTimeout(3*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	pp.Register("echo", "./testdata/caps/echo.py")
	pp.Register("reverse", "./testdata/caps/reverse.py")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	r1, err := pp.Do(ctx, "echo", In{"msg": "hello"})
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	if r1.AsJSON()["msg"] != "hello" {
		t.Fatalf("echo unexpected: %v", r1.AsJSON())
	}

	r2, err := pp.Do(ctx, "reverse", In{"text": "hello"})
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
	pp, err := New(ShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Do

	_, err = pp.Do(ctx, "any.cap", In{})
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

// TestWorkerBuilderInterface verifies Workers() accepts *WorkerBuilder directly.
func TestWorkerBuilderInterface(t *testing.T) {
	pp, err := New(
		Workers(
			NewWorker("w-1").Groups("gpu"),
			NewWorker("w-2").Groups("cpu"),
			NewWorker("w-3").Groups("gpu", "asr").MaxRequests(10000).MaxUptime(24*time.Hour),
		),
		ShutdownTimeout(time.Second),
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

// TestCapabilityRegistration verifies Register, Include, Compose compile.
func TestCapabilityRegistration(t *testing.T) {
	pp, err := New(ShutdownTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	// Python
	if err := pp.Register("echo", "./testdata/caps/echo.py"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Check it's in the registry
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

// helpers

func pythonAvailable() bool {
	_, err := exec.LookPath("python3")
	return err == nil
}

func msgpackAvailable() bool {
	return exec.Command("python3", "-c", "import msgpack").Run() == nil
}

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
