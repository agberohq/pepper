package session

import (
	"testing"
	"time"
)

// helpers

func newStore(ttl time.Duration) Store {
	return NewMemory(ttl)
}

// Set / Get

func TestSetAndGet(t *testing.T) {
	s := newStore(time.Minute)

	if err := s.Set("sess-1", "name", "alice"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, ok := s.Get("sess-1", "name")
	if !ok {
		t.Fatal("Get returned not-found")
	}
	if v != "alice" {
		t.Errorf("Get = %v, want alice", v)
	}
}

func TestGetMissingKeyReturnsFalse(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-1", "key", "value")

	_, ok := s.Get("sess-1", "missing-key")
	if ok {
		t.Error("Get for missing key should return false")
	}
}

func TestGetMissingSessionReturnsFalse(t *testing.T) {
	s := newStore(time.Minute)
	_, ok := s.Get("no-such-session", "key")
	if ok {
		t.Error("Get for missing session should return false")
	}
}

func TestSetOverwritesExistingKey(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-1", "counter", 1)
	s.Set("sess-1", "counter", 2)

	v, _ := s.Get("sess-1", "counter")
	if v != 2 {
		t.Errorf("counter = %v, want 2 (overwrite)", v)
	}
}

func TestSetMultipleKeys(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-1", "a", "alpha")
	s.Set("sess-1", "b", "beta")
	s.Set("sess-1", "c", "gamma")

	for key, want := range map[string]string{"a": "alpha", "b": "beta", "c": "gamma"} {
		v, ok := s.Get("sess-1", key)
		if !ok {
			t.Errorf("Get(%q) returned not-found", key)
			continue
		}
		if v != want {
			t.Errorf("Get(%q) = %v, want %v", key, v, want)
		}
	}
}

// GetAll

func TestGetAll(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-2", "x", 1)
	s.Set("sess-2", "y", 2)

	all, ok := s.GetAll("sess-2")
	if !ok {
		t.Fatal("GetAll returned not-found")
	}
	if len(all) != 2 {
		t.Errorf("GetAll returned %d keys, want 2", len(all))
	}
	if all["x"] != 1 || all["y"] != 2 {
		t.Errorf("GetAll values wrong: %v", all)
	}
}

func TestGetAllMissingSessionReturnsFalse(t *testing.T) {
	s := newStore(time.Minute)
	_, ok := s.GetAll("no-such-session")
	if ok {
		t.Error("GetAll for missing session should return false")
	}
}

func TestGetAllReturnsCopy(t *testing.T) {
	// Mutating the returned map must not affect the stored session
	s := newStore(time.Minute)
	s.Set("sess-copy", "key", "original")

	all, _ := s.GetAll("sess-copy")
	all["key"] = "mutated"

	v, _ := s.Get("sess-copy", "key")
	if v == "mutated" {
		t.Error("GetAll should return a copy, not a reference")
	}
}

// Merge

func TestMerge(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-m", "existing", "keep")

	err := s.Merge("sess-m", map[string]any{
		"new-key":  "new-val",
		"existing": "overwritten",
	})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	v, _ := s.Get("sess-m", "new-key")
	if v != "new-val" {
		t.Errorf("new-key = %v, want new-val", v)
	}
	v, _ = s.Get("sess-m", "existing")
	if v != "overwritten" {
		t.Errorf("existing = %v, want overwritten", v)
	}
}

func TestMergeCreatesSession(t *testing.T) {
	s := newStore(time.Minute)
	err := s.Merge("new-sess", map[string]any{"key": "val"})
	if err != nil {
		t.Fatalf("Merge on new session: %v", err)
	}
	if !s.Exists("new-sess") {
		t.Error("Merge should create session if it doesn't exist")
	}
}

func TestMergeEmpty(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-em", "key", "val")
	if err := s.Merge("sess-em", map[string]any{}); err != nil {
		t.Fatalf("Merge empty: %v", err)
	}
	// Existing key must still be present
	_, ok := s.Get("sess-em", "key")
	if !ok {
		t.Error("existing key lost after empty Merge")
	}
}

// Touch

func TestTouch(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-t", "key", "val")

	if err := s.Touch("sess-t"); err != nil {
		t.Errorf("Touch: %v", err)
	}
}

func TestTouchMissingSessionReturnsError(t *testing.T) {
	s := newStore(time.Minute)
	if err := s.Touch("no-such-session"); err == nil {
		t.Error("expected error touching missing session")
	}
}

// Clear

func TestClear(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-c", "a", 1)
	s.Set("sess-c", "b", 2)

	if err := s.Clear("sess-c"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if s.Exists("sess-c") {
		t.Error("session should not exist after Clear")
	}
	_, ok := s.Get("sess-c", "a")
	if ok {
		t.Error("Get after Clear should return not-found")
	}
}

func TestClearMissingSessionIsNoop(t *testing.T) {
	s := newStore(time.Minute)
	// Must not error or panic
	if err := s.Clear("no-such-session"); err != nil {
		t.Errorf("Clear on missing session returned error: %v", err)
	}
}

// Exists

func TestExistsFalseBeforeSet(t *testing.T) {
	s := newStore(time.Minute)
	if s.Exists("empty") {
		t.Error("Exists should return false before any Set")
	}
}

func TestExistsTrueAfterSet(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-e", "k", "v")
	if !s.Exists("sess-e") {
		t.Error("Exists should return true after Set")
	}
}

func TestExistsFalseAfterClear(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("sess-ec", "k", "v")
	s.Clear("sess-ec")
	if s.Exists("sess-ec") {
		t.Error("Exists should return false after Clear")
	}
}

// TTL expiry

func TestSessionExpiresAfterTTL(t *testing.T) {
	// Use a very short TTL to test expiry without long sleeps.
	// jack.Lifetime processes expiry at its minimum tick (10ms by default),
	// so 100ms TTL + 200ms sleep is sufficient.
	s := newStore(100 * time.Millisecond)

	s.Set("expiring-sess", "key", "value")
	if !s.Exists("expiring-sess") {
		t.Fatal("session should exist immediately after Set")
	}

	time.Sleep(350 * time.Millisecond)

	if s.Exists("expiring-sess") {
		t.Error("session should have expired after TTL")
	}
	_, ok := s.Get("expiring-sess", "key")
	if ok {
		t.Error("Get after TTL expiry should return not-found")
	}
}

func TestTouchResetsExpiry(t *testing.T) {
	// Set a 150ms TTL. Touch at 100ms. Session should still exist at 200ms.
	s := newStore(150 * time.Millisecond)
	s.Set("touch-sess", "key", "value")

	time.Sleep(100 * time.Millisecond)
	s.Touch("touch-sess") // reset TTL

	time.Sleep(100 * time.Millisecond)
	// 200ms elapsed since Set, but Touch reset the TTL — still alive
	if !s.Exists("touch-sess") {
		t.Error("session should still exist after Touch reset the TTL")
	}
}

// Multiple sessions isolation

func TestSessionsAreIsolated(t *testing.T) {
	s := newStore(time.Minute)
	s.Set("user-1", "name", "alice")
	s.Set("user-2", "name", "bob")

	v1, _ := s.Get("user-1", "name")
	v2, _ := s.Get("user-2", "name")

	if v1 != "alice" {
		t.Errorf("user-1 name = %v, want alice", v1)
	}
	if v2 != "bob" {
		t.Errorf("user-2 name = %v, want bob", v2)
	}

	s.Clear("user-1")
	if s.Exists("user-1") {
		t.Error("user-1 should be cleared")
	}
	if !s.Exists("user-2") {
		t.Error("user-2 should still exist")
	}
}

// Concurrent safety

func TestConcurrentSetGet(t *testing.T) {
	s := newStore(time.Minute)
	done := make(chan struct{}, 40)

	for i := 0; i < 20; i++ {
		sessID := "sess-concurrent-" + string(rune('a'+i))
		go func(id string) {
			s.Set(id, "k", id)
			done <- struct{}{}
		}(sessID)
		go func(id string) {
			s.Get(id, "k")
			done <- struct{}{}
		}(sessID)
	}
	for i := 0; i < 40; i++ {
		<-done
	}
}
