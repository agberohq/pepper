package hooks

import (
	"context"
	"errors"
	"testing"

	"github.com/agberohq/pepper/internal/envelope"
)

func baseEnv() *envelope.Envelope {
	env := envelope.DefaultEnvelope()
	env.Cap = "test.cap"
	env.Group = "default"
	return &env
}

// Chain

func TestChainLen(t *testing.T) {
	c := &Chain{}
	if c.Len() != 0 {
		t.Errorf("Len = %d, want 0", c.Len())
	}
	c.Add(Hook{Name: "a"})
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1", c.Len())
	}
}

func TestChainBeforeAdd(t *testing.T) {
	c := &Chain{}
	c.Before("auth", func(_ context.Context, _ *envelope.Envelope, in In) (In, error) {
		return nil, nil
	})
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1 after Before", c.Len())
	}
}

func TestChainAfterAdd(t *testing.T) {
	c := &Chain{}
	c.After("log", func(_ context.Context, _ *envelope.Envelope, _ In, r Result) (Result, error) {
		return r, nil
	})
	if c.Len() != 1 {
		t.Errorf("Len = %d, want 1 after After", c.Len())
	}
}

func TestChainRunBeforePassthrough(t *testing.T) {
	c := &Chain{}
	// Hook returns nil → original input passed through
	c.Before("noop", func(_ context.Context, _ *envelope.Envelope, in In) (In, error) {
		return nil, nil
	})
	in := In{"key": "value"}
	out, err := c.RunBefore(context.Background(), baseEnv(), in)
	if err != nil {
		t.Fatalf("RunBefore: %v", err)
	}
	if out["key"] != "value" {
		t.Errorf("key = %v, want value", out["key"])
	}
}

func TestChainRunBeforeMutates(t *testing.T) {
	c := &Chain{}
	c.Before("inject", func(_ context.Context, _ *envelope.Envelope, in In) (In, error) {
		return In{"injected": true, "original": in["original"]}, nil
	})
	out, err := c.RunBefore(context.Background(), baseEnv(), In{"original": "yes"})
	if err != nil {
		t.Fatalf("RunBefore: %v", err)
	}
	if out["injected"] != true {
		t.Error("injected key missing")
	}
}

func TestChainRunBeforeError(t *testing.T) {
	c := &Chain{}
	sentinel := errors.New("auth failed")
	c.Before("auth", func(_ context.Context, _ *envelope.Envelope, _ In) (In, error) {
		return nil, sentinel
	})
	_, err := c.RunBefore(context.Background(), baseEnv(), In{})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want sentinel", err)
	}
}

func TestChainRunBeforeStopsOnError(t *testing.T) {
	c := &Chain{}
	called := false
	c.Before("fail", func(_ context.Context, _ *envelope.Envelope, _ In) (In, error) {
		return nil, errors.New("stop here")
	})
	c.Before("second", func(_ context.Context, _ *envelope.Envelope, _ In) (In, error) {
		called = true
		return nil, nil
	})
	c.RunBefore(context.Background(), baseEnv(), In{})
	if called {
		t.Error("second hook should not run after first returns error")
	}
}

func TestChainRunAfterReverseOrder(t *testing.T) {
	c := &Chain{}
	order := []string{}
	c.After("first", func(_ context.Context, _ *envelope.Envelope, _ In, r Result) (Result, error) {
		order = append(order, "first")
		return r, nil
	})
	c.After("second", func(_ context.Context, _ *envelope.Envelope, _ In, r Result) (Result, error) {
		order = append(order, "second")
		return r, nil
	})
	c.RunAfter(context.Background(), baseEnv(), In{}, Result{})
	// After hooks run in reverse: second, first
	if len(order) != 2 || order[0] != "second" || order[1] != "first" {
		t.Errorf("order = %v, want [second first]", order)
	}
}

func TestChainRunAfterMutatesResult(t *testing.T) {
	c := &Chain{}
	c.After("enrich", func(_ context.Context, _ *envelope.Envelope, _ In, r Result) (Result, error) {
		r.Cap = "enriched"
		return r, nil
	})
	res, err := c.RunAfter(context.Background(), baseEnv(), In{}, Result{Cap: "original"})
	if err != nil {
		t.Fatalf("RunAfter: %v", err)
	}
	if res.Cap != "enriched" {
		t.Errorf("Cap = %q, want enriched", res.Cap)
	}
}

func TestChainNilBeforeSkipped(t *testing.T) {
	c := &Chain{}
	c.Add(Hook{Name: "only-after", After: func(_ context.Context, _ *envelope.Envelope, _ In, r Result) (Result, error) {
		return r, nil
	}})
	_, err := c.RunBefore(context.Background(), baseEnv(), In{})
	if err != nil {
		t.Errorf("RunBefore with nil Before should not error: %v", err)
	}
}

func TestChainNilAfterSkipped(t *testing.T) {
	c := &Chain{}
	c.Add(Hook{Name: "only-before", Before: func(_ context.Context, _ *envelope.Envelope, in In) (In, error) {
		return nil, nil
	}})
	_, err := c.RunAfter(context.Background(), baseEnv(), In{}, Result{})
	if err != nil {
		t.Errorf("RunAfter with nil After should not error: %v", err)
	}
}

// ShortCircuit

func TestShortCircuit(t *testing.T) {
	cached := Result{Payload: []byte("cached"), Cap: "test.cap"}
	err := ShortCircuit(cached)
	if err == nil {
		t.Fatal("ShortCircuit should return non-nil error")
	}
	if !IsShortCircuit(err) {
		t.Error("IsShortCircuit should return true")
	}
	r, ok := ShortCircuitResult(err)
	if !ok {
		t.Error("ShortCircuitResult should return ok=true")
	}
	if string(r.Payload) != "cached" {
		t.Errorf("Payload = %q", r.Payload)
	}
}

func TestIsShortCircuitFalseForOtherErrors(t *testing.T) {
	if IsShortCircuit(errors.New("other")) {
		t.Error("IsShortCircuit should be false for regular errors")
	}
	if IsShortCircuit(nil) {
		t.Error("IsShortCircuit should be false for nil")
	}
}

func TestShortCircuitResultFalseForOtherErrors(t *testing.T) {
	_, ok := ShortCircuitResult(errors.New("nope"))
	if ok {
		t.Error("ShortCircuitResult should return ok=false for non-ShortCircuit error")
	}
}

// Built-in hooks

func TestLoggerHook(t *testing.T) {
	logged := []string{}
	logger := &testLogger{msgs: &logged}
	h := Logger(logger, false)

	if h.Name != "pepper.logger" {
		t.Errorf("Name = %q", h.Name)
	}

	env := baseEnv()
	env.CorrID = "corr-123"
	in := In{"x": 1}

	_, err := h.Before(context.Background(), env, in)
	if err != nil {
		t.Fatalf("Before: %v", err)
	}
	_, err = h.After(context.Background(), env, in, Result{WorkerID: "w-1"})
	if err != nil {
		t.Fatalf("After: %v", err)
	}
	if len(logged) != 2 {
		t.Errorf("logged %d messages, want 2", len(logged))
	}
}

func TestLoggerHookDebugPayload(t *testing.T) {
	logged := []string{}
	logger := &testLogger{msgs: &logged}
	h := Logger(logger, true) // debugPayload=true

	env := baseEnv()
	h.Before(context.Background(), env, In{"secret": "data"})
	if len(logged) == 0 {
		t.Error("expected debug log with payload")
	}
}

func TestMetaInjectHook(t *testing.T) {
	h := MetaInject(map[string]any{"tenant": "acme", "version": 2})
	env := baseEnv()

	_, err := h.Before(context.Background(), env, In{})
	if err != nil {
		t.Fatalf("Before: %v", err)
	}
	if env.Meta["tenant"] != "acme" {
		t.Errorf("tenant = %v", env.Meta["tenant"])
	}
	if env.Meta["version"] != 2 {
		t.Errorf("version = %v", env.Meta["version"])
	}
}

func TestMetaInjectHookNilMeta(t *testing.T) {
	h := MetaInject(map[string]any{"key": "val"})
	env := baseEnv()
	env.Meta = nil // deliberately nil
	_, err := h.Before(context.Background(), env, In{})
	if err != nil {
		t.Fatalf("Before with nil Meta: %v", err)
	}
	if env.Meta["key"] != "val" {
		t.Error("Meta should be initialised and populated")
	}
}

func TestInputTransformHook(t *testing.T) {
	h := InputTransform("normalise", func(in In) In {
		return In{"normalised": true, "original": in["x"]}
	})
	out, err := h.Before(context.Background(), baseEnv(), In{"x": 42})
	if err != nil {
		t.Fatalf("Before: %v", err)
	}
	if out["normalised"] != true {
		t.Error("normalised key missing")
	}
	if out["original"] != 42 {
		t.Errorf("original = %v, want 42", out["original"])
	}
}

func TestInputTransformNilReturn(t *testing.T) {
	h := InputTransform("noop", func(in In) In { return nil })
	original := In{"x": 1}
	out, err := h.Before(context.Background(), baseEnv(), original)
	if err != nil {
		t.Fatalf("Before: %v", err)
	}
	// nil return from fn → nil returned from Before → chain passes original through
	if out != nil {
		t.Errorf("expected nil, got %v", out)
	}
}

func TestOutputTransformHook(t *testing.T) {
	h := OutputTransform("redact", func(r Result) Result {
		r.Payload = []byte("redacted")
		return r
	})
	res, err := h.After(context.Background(), baseEnv(), In{}, Result{Payload: []byte("secret")})
	if err != nil {
		t.Fatalf("After: %v", err)
	}
	if string(res.Payload) != "redacted" {
		t.Errorf("Payload = %q", res.Payload)
	}
}

// Registry

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	if r.Global() == nil {
		t.Error("Global chain should not be nil")
	}
}

func TestRegistryGlobal(t *testing.T) {
	r := NewRegistry()
	r.Global().Before("g", func(_ context.Context, _ *envelope.Envelope, in In) (In, error) {
		return nil, nil
	})
	if r.Global().Len() != 1 {
		t.Errorf("Global chain len = %d, want 1", r.Global().Len())
	}
}

func TestRegistryForGroup(t *testing.T) {
	r := NewRegistry()
	c1 := r.ForGroup("gpu")
	c2 := r.ForGroup("gpu")
	if c1 != c2 {
		t.Error("ForGroup should return the same chain for the same group")
	}
	r.ForGroup("cpu") // different group
}

func TestRegistryForCap(t *testing.T) {
	r := NewRegistry()
	c1 := r.ForCap("face.recognize")
	c2 := r.ForCap("face.recognize")
	if c1 != c2 {
		t.Error("ForCap should return same chain for same cap")
	}
}

func TestRegistryResolveOrder(t *testing.T) {
	r := NewRegistry()
	order := []string{}

	r.Global().Before("global", func(_ context.Context, _ *envelope.Envelope, in In) (In, error) {
		order = append(order, "global")
		return nil, nil
	})
	r.ForGroup("gpu").Before("group", func(_ context.Context, _ *envelope.Envelope, in In) (In, error) {
		order = append(order, "group")
		return nil, nil
	})
	r.ForCap("face.recognize").Before("cap", func(_ context.Context, _ *envelope.Envelope, in In) (In, error) {
		order = append(order, "cap")
		return nil, nil
	})

	env := baseEnv()
	env.Cap = "face.recognize"
	env.Group = "gpu"
	r.RunBefore(context.Background(), env, In{})

	if len(order) != 3 {
		t.Fatalf("order = %v, want 3 entries", order)
	}
	if order[0] != "global" || order[1] != "group" || order[2] != "cap" {
		t.Errorf("order = %v, want [global group cap]", order)
	}
}

func TestRegistryResolveEmptyChainNotIncluded(t *testing.T) {
	r := NewRegistry()
	// ForGroup creates a chain but we don't add hooks — should not appear in Resolve
	r.ForGroup("gpu")

	env := baseEnv()
	env.Group = "gpu"
	chains := r.Resolve(env.Cap, env.Group)
	// Only non-empty chains included: global (0 hooks) excluded, group (0 hooks) excluded
	if len(chains) != 0 {
		t.Errorf("Resolve returned %d chains for empty registry, want 0", len(chains))
	}
}

func TestRegistryRunBeforeShortCircuit(t *testing.T) {
	r := NewRegistry()
	cached := Result{Payload: []byte("from-cache")}
	r.Global().Before("cache", func(_ context.Context, _ *envelope.Envelope, _ In) (In, error) {
		return nil, ShortCircuit(cached)
	})

	env := baseEnv()
	_, err := r.RunBefore(context.Background(), env, In{})
	if !IsShortCircuit(err) {
		t.Errorf("expected ShortCircuit error, got %v", err)
	}
	res, ok := ShortCircuitResult(err)
	if !ok || string(res.Payload) != "from-cache" {
		t.Error("ShortCircuit result not preserved through registry")
	}
}

func TestRegistryRunAfterReverseChainOrder(t *testing.T) {
	r := NewRegistry()
	order := []string{}

	r.Global().After("global-after", func(_ context.Context, _ *envelope.Envelope, _ In, res Result) (Result, error) {
		order = append(order, "global")
		return res, nil
	})
	r.ForCap("test.cap").After("cap-after", func(_ context.Context, _ *envelope.Envelope, _ In, res Result) (Result, error) {
		order = append(order, "cap")
		return res, nil
	})

	env := baseEnv()
	r.RunAfter(context.Background(), env, In{}, Result{})

	// After: chains resolved as [global, cap], run in reverse: cap then global
	if len(order) != 2 || order[0] != "cap" || order[1] != "global" {
		t.Errorf("after order = %v, want [cap global]", order)
	}
}

// test helpers

type testLogger struct{ msgs *[]string }

func (l *testLogger) Debug(msg string, args ...any) { *l.msgs = append(*l.msgs, msg) }
