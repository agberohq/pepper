package tests

// Rust parity tests: streaming and real concurrency.
//
// They compile the Rust capabilities in runtime/rust/caps on first run (cached),
// so they need cargo; they self-skip otherwise.

import (
	"context"
	"testing"
	"time"

	"github.com/agberohq/pepper"
)

type rustTokenChunk struct {
	Index int    `msgpack:"index"`
	Token string `msgpack:"token"`
}

// TestRustStreaming exercises server streaming: the Rust cap overrides
// run_stream and emits one chunk per word.
func TestRustStreaming(t *testing.T) {
	if !runtimeFinder.HasCargo() {
		t.Skip("cargo not available")
	}
	if runtimeFinder.RustRuntime(nil) == "" {
		t.Skip("runtime/rust not found (set PEEPER_HOME)")
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-rust-stream").Groups("default")),
		pepper.WithBootTimeout(20*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Script("rust.tokens", "../runtime/rust/caps/tokens.rs").
		Groups("default")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := pp.WaitWorkerReady(ctx, "rust.tokens"); err != nil {
		t.Fatalf("WaitWorkerReady: %v", err)
	}

	ch, err := pepper.Call[rustTokenChunk]{
		Cap:   "rust.tokens",
		Input: pepper.In{"prompt": "stream me now"},
	}.Bind(pp).Stream(ctx)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	var got []string
	for chunk := range ch {
		got = append(got, chunk.Token)
	}
	want := []string{"stream", "me", "now"}
	if len(got) != len(want) {
		t.Fatalf("chunks = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("chunk %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestRustRequestsRunInParallel verifies `run(&self)` actually buys concurrency:
// four 250 ms calls on one worker must finish in roughly one call's time, not four.
func TestRustRequestsRunInParallel(t *testing.T) {
	if !runtimeFinder.HasCargo() {
		t.Skip("cargo not available")
	}
	if runtimeFinder.RustRuntime(nil) == "" {
		t.Skip("runtime/rust not found (set PEEPER_HOME)")
	}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-rust-par").Groups("default")),
		pepper.WithBootTimeout(20*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Script("rust.slow", "../runtime/rust/caps/slow.rs").
		Groups("default")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := pp.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := pp.WaitWorkerReady(ctx, "rust.slow"); err != nil {
		t.Fatalf("WaitWorkerReady: %v", err)
	}

	const sleepMs = 250
	const calls = 4
	start := time.Now()
	results, err := pepper.All[struct {
		SleptMs int64 `msgpack:"slept_ms"`
	}](ctx, pp,
		pepper.MakeCall("rust.slow", pepper.In{"sleep_ms": sleepMs}),
		pepper.MakeCall("rust.slow", pepper.In{"sleep_ms": sleepMs}),
		pepper.MakeCall("rust.slow", pepper.In{"sleep_ms": sleepMs}),
		pepper.MakeCall("rust.slow", pepper.In{"sleep_ms": sleepMs}),
	)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(results) != calls {
		t.Fatalf("got %d results, want %d", len(results), calls)
	}

	serial := time.Duration(calls) * sleepMs * time.Millisecond
	if elapsed > serial/2 {
		t.Errorf("4 x %dms calls took %s — expected closer to %s, so requests are serialized",
			sleepMs, elapsed.Round(time.Millisecond), (sleepMs * time.Millisecond))
	}
	t.Logf("parallelism: %d calls in %s (serial would be ≥%s)", calls, elapsed.Round(time.Millisecond), serial)
}

// TestRustPrebuiltCacheRunsWithoutCargo proves the deployment story: once a
// capability is compiled, the worker starts from the cache with no toolchain at
// all. That is what a multi-stage Docker build or an air-gapped host relies on.
func TestRustPrebuiltCacheRunsWithoutCargo(t *testing.T) {
	if !runtimeFinder.HasCargo() {
		t.Skip("cargo not available")
	}
	if runtimeFinder.RustRuntime(nil) == "" {
		t.Skip("runtime/rust not found (set PEEPER_HOME)")
	}

	callOnce := func() {
		t.Helper()
		pp, err := pepper.New(
			pepper.WithWorkers(pepper.NewWorker("w-rust-cache").Groups("default")),
			pepper.WithBootTimeout(20*time.Second),
			pepper.WithLogger(testLogger),
		)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer pp.Stop()

		if err := pp.Register(pepper.Script("rust.echo", "../runtime/rust/caps/echo.rs").
			Groups("default")); err != nil {
			t.Fatalf("Register: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := pp.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := pp.WaitWorkerReady(ctx, "rust.echo"); err != nil {
			t.Fatalf("WaitWorkerReady: %v", err)
		}
		out, err := pepper.Call[rustEchoOut]{
			Cap:   "rust.echo",
			Input: pepper.In{"msg": "cached"},
		}.Bind(pp).Do(ctx)
		if err != nil {
			t.Fatalf("Call: %v", err)
		}
		if out.Upper != "CACHED" {
			t.Errorf("upper = %q, want CACHED", out.Upper)
		}
	}

	// Warm the cache with a real toolchain.
	callOnce()

	// Now make cargo unusable: a cache hit must not need it.
	t.Setenv("CARGO", "/nonexistent/cargo-that-does-not-exist")
	callOnce()
}
