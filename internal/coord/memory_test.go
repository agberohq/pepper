package coord

import (
	"context"
	"testing"
	"time"
)

func TestMemory(t *testing.T) {
	s := &Suite{
		Name:     "Memory",
		NewStore: func(t *testing.T) Store { return NewMemory() },
	}
	s.Run(t)
}

func TestMemoryReaper(t *testing.T) {
	store := NewMemory()
	defer store.Close()

	ctx := context.Background()
	if err := store.Set(ctx, "expire-me", []byte("val"), 1); err != nil {
		t.Fatal(err)
	}
	_, ok, _ := store.Get(ctx, "expire-me")
	if !ok {
		t.Fatal("key should exist immediately after Set")
	}
	time.Sleep(1100 * time.Millisecond)
	_, ok, _ = store.Get(ctx, "expire-me")
	if ok {
		t.Fatal("key should be gone after TTL")
	}
}

func TestMemorySubscribeOnSet(t *testing.T) {
	store := NewMemory()
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := store.Subscribe(ctx, "pepper:worker:")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	if err := store.Set(ctx, "pepper:worker:w-1", []byte(`{"id":"w-1"}`), 0); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-ch:
		if ev.Key != "pepper:worker:w-1" {
			t.Errorf("expected key pepper:worker:w-1, got %q", ev.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event received")
	}
}

func TestMemorySubscribeOnDelete(t *testing.T) {
	store := NewMemory()
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	key := "pepper:worker:w-del"
	store.Set(ctx, key, []byte("present"), 0)

	ch, err := store.Subscribe(ctx, "pepper:worker:")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	store.Delete(ctx, key)

	select {
	case ev := <-ch:
		if !ev.Deleted {
			t.Errorf("expected Deleted=true, got key=%q deleted=%v", ev.Key, ev.Deleted)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no delete event received")
	}
}

func TestMemoryCloseIdempotent(t *testing.T) {
	store := NewMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, _ := store.Subscribe(ctx, "pepper:")
	store.Close()
	store.Close() // must not panic

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after store Close")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after store.Close()")
	}
}

func BenchmarkMemorySetGet(b *testing.B) {
	store := NewMemory()
	defer store.Close()
	runBenchSetGet(b, store)
}
