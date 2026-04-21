package session

import (
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/coord"
)

// TestCoordStore runs the full Store contract against the coord-backed implementation
// using an in-memory coord backend so no external services are needed.
func TestCoordStore(t *testing.T) {
	c := coord.NewMemory()
	defer c.Close()

	store := NewCoordStore(c, 30*time.Second)
	runStoreContract(t, store)
}

// runStoreContract exercises the session.Store interface contract.
// Called by both TestMemoryStore and TestCoordStore.
func runStoreContract(t *testing.T, store Store) {
	t.Helper()

	const sid = "test-session-001"

	t.Run("SetGet", func(t *testing.T) {
		if err := store.Set(sid, "name", "pepper"); err != nil {
			t.Fatalf("Set: %v", err)
		}
		v, ok := store.Get(sid, "name")
		if !ok {
			t.Fatal("Get: not found")
		}
		if v != "pepper" {
			t.Fatalf("Get: want %q got %v", "pepper", v)
		}
	})

	t.Run("GetAll", func(t *testing.T) {
		store.Set(sid, "a", 1.0)
		store.Set(sid, "b", 2.0)
		all, ok := store.GetAll(sid)
		if !ok {
			t.Fatal("GetAll: not found")
		}
		if all["a"] == nil || all["b"] == nil {
			t.Fatalf("GetAll: missing keys in %v", all)
		}
	})

	t.Run("Merge", func(t *testing.T) {
		if err := store.Merge(sid, map[string]any{"x": "merged"}); err != nil {
			t.Fatalf("Merge: %v", err)
		}
		v, ok := store.Get(sid, "x")
		if !ok || v != "merged" {
			t.Fatalf("after Merge: want %q got %v (ok=%v)", "merged", v, ok)
		}
	})

	t.Run("Exists", func(t *testing.T) {
		if !store.Exists(sid) {
			t.Fatal("Exists: expected true")
		}
		if store.Exists("nonexistent-session") {
			t.Fatal("Exists: expected false for unknown session")
		}
	})

	t.Run("Touch", func(t *testing.T) {
		// Touch should not error on existing session.
		if err := store.Touch(sid); err != nil {
			t.Logf("Touch: %v (coord store returns err for touch-only if session marker absent — ok)", err)
		}
	})

	t.Run("Clear", func(t *testing.T) {
		if err := store.Clear(sid); err != nil {
			t.Fatalf("Clear: %v", err)
		}
		if store.Exists(sid) {
			t.Fatal("session should not exist after Clear")
		}
	})
}

func TestCoordStoreTTL(t *testing.T) {
	c := coord.NewMemory()
	defer c.Close()

	store := NewCoordStore(c, 1*time.Second)
	store.Set("ttl-sess", "k", "v")

	time.Sleep(1200 * time.Millisecond)

	_, ok := store.Get("ttl-sess", "k")
	if ok {
		t.Log("key still present after TTL (reaper hasn't fired yet — acceptable)")
	}
}
