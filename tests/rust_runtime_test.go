package tests

// Rust runtime tests.
//
// These compile a real Rust capability on first run (cached by content hash in
// PEPPER_CACHE_DIR) and then exercise it through the normal bus, so they cover
// the same path a production deployment takes: pepper-worker → cargo → binary →
// envelopes → router.
//
// Skip conditions (checked at runtime, no build tags):
//
//	cargo not in PATH                      → the whole file skips
//	runtime/rust missing                   → the whole file skips
//
// The first run needs the crates serde/rmp-serde/serde_json/memmap2 available to
// cargo (registry cache or network); later runs reuse the compiled binary.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agberohq/pepper"
)

type rustEchoOut struct {
	Msg    string `msgpack:"msg"`
	Upper  string `msgpack:"upper"`
	Length int    `msgpack:"length"`
	Worker string `msgpack:"worker"`
}

type rustWordCountOut struct {
	Words   int    `msgpack:"words"`
	Longest string `msgpack:"longest"`
	Chars   int    `msgpack:"chars"`
	Worker  string `msgpack:"worker"`
}

// TestRustCapabilityRoundTrip runs two Rust capabilities on ONE worker: they share
// a worker process exactly like several .py files share a Python worker, so this
// also pins that the runtime does not quietly require one worker per capability.
func TestRustCapabilityRoundTrip(t *testing.T) {
	if !runtimeFinder.HasCargo() {
		t.Skip("cargo not available")
	}
	if runtimeFinder.RustRuntime(nil) == "" {
		t.Skip("runtime/rust not found (set PEEPER_HOME)")
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-rust-1").Groups("default")),
		// Deliberately small: compilation happens before this budget starts, so
		// a cold cargo build must not be able to blow the boot deadline.
		pepper.WithBootTimeout(20*time.Second),
		pepper.WithShutdownTimeout(10*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Script("rust.echo", "../runtime/rust/caps/echo.rs").
		Groups("default")); err != nil {
		t.Fatalf("Register echo: %v", err)
	}
	if err := pp.Register(pepper.Script("rust.wordcount", "../runtime/rust/caps/wordcount.rs").
		Groups("default")); err != nil {
		t.Fatalf("Register wordcount: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 11*time.Minute)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := pp.WaitWorkerReady(ctx, "rust.echo"); err != nil {
		t.Fatalf("WaitWorkerReady echo: %v", err)
	}
	if err := pp.WaitWorkerReady(ctx, "rust.wordcount"); err != nil {
		t.Fatalf("WaitWorkerReady wordcount: %v", err)
	}

	out, err := pepper.Call[rustEchoOut]{
		Cap:   "rust.echo",
		Input: pepper.In{"msg": "hello rust"},
	}.Bind(pp).Do(ctx)
	if err != nil {
		t.Fatalf("Call echo: %v", err)
	}
	if out.Upper != "HELLO RUST" {
		t.Errorf("upper = %q, want HELLO RUST", out.Upper)
	}
	if out.Length != 10 {
		t.Errorf("length = %d, want 10", out.Length)
	}
	if out.Worker != "w-rust-1" {
		t.Errorf("worker = %q, want w-rust-1", out.Worker)
	}
	if out.Msg != "hello rust" {
		t.Errorf("msg = %q, want %q", out.Msg, "hello rust")
	}

	// The second capability is served by the same worker process.
	wc, err := pepper.Call[rustWordCountOut]{
		Cap:   "rust.wordcount",
		Input: pepper.In{"text": "pepper runs rust capabilities"},
	}.Bind(pp).Do(ctx)
	if err != nil {
		t.Fatalf("Call wordcount: %v", err)
	}
	if wc.Words != 4 {
		t.Errorf("words = %d, want 4", wc.Words)
	}
	if wc.Longest != "capabilities" {
		t.Errorf("longest = %q, want capabilities", wc.Longest)
	}
	// Both capabilities answered from the same worker process — that is the
	// symmetry with Python (N caps, one worker), not an accident of the test.
	if wc.Worker != out.Worker {
		t.Errorf("capabilities served by different workers: %q vs %q", wc.Worker, out.Worker)
	}

	// Warm path: no respawn, no rebuild.
	out2, err := pepper.Call[rustEchoOut]{
		Cap:   "rust.echo",
		Input: pepper.In{"msg": "again"},
	}.Bind(pp).Do(ctx)
	if err != nil {
		t.Fatalf("second Call: %v", err)
	}
	if out2.Upper != "AGAIN" {
		t.Errorf("second call upper = %q, want AGAIN", out2.Upper)
	}
}

// TestRustBuildFailureIsReported pins the failure contract: a capability that does
// not compile must fail Start() with the compiler's message, not hang until the
// boot timeout or surface later as "worker died".
func TestRustBuildFailureIsReported(t *testing.T) {
	if !runtimeFinder.HasCargo() {
		t.Skip("cargo not available")
	}
	if runtimeFinder.RustRuntime(nil) == "" {
		t.Skip("runtime/rust not found (set PEEPER_HOME)")
	}

	dir := t.TempDir()
	broken := filepath.Join(dir, "broken.rs")
	source := []byte("use pepper_runtime::prelude::*;\n\nderive_broken!();\n")
	if err := os.WriteFile(broken, source, 0o644); err != nil {
		t.Fatalf("write cap: %v", err)
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-rust-bad").Groups("default")),
		pepper.WithBootTimeout(20*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Script("rust.broken", broken).Groups("default")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	err = pp.Start(ctx)
	if err == nil {
		t.Fatal("Start succeeded for a capability that cannot compile")
	}
	msg := err.Error()
	if !strings.Contains(msg, "cargo build failed") && !strings.Contains(msg, "error") {
		t.Errorf("error should carry compiler output, got: %s", msg)
	}
	if !strings.Contains(msg, "broken") {
		t.Errorf("error should name the offending capability, got: %s", msg)
	}
}
