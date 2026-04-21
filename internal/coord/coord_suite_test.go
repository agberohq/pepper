package coord

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Suite is a reusable compliance test suite for any coord.Store implementation.
type Suite struct {
	Name     string
	NewStore func(t *testing.T) Store
}

func (s *Suite) Run(t *testing.T) {
	t.Helper()
	t.Run(s.Name, func(t *testing.T) {
		t.Run("SetGet", s.testSetGet)
		t.Run("GetMissing", s.testGetMissing)
		t.Run("Overwrite", s.testOverwrite)
		t.Run("Delete", s.testDelete)
		t.Run("List", s.testList)
		t.Run("PubSubBasic", s.testPubSubBasic)
		t.Run("PushPullFIFO", s.testPushPullFIFO)
		t.Run("PushPullExactlyOnce", s.testPushPullExactlyOnce)
		t.Run("PullBlocksUntilPush", s.testPullBlocksUntilPush)
		t.Run("PullCancelledContext", s.testPullCancelledContext)
	})
}

func (s *Suite) newStore(t *testing.T) Store {
	t.Helper()
	store := s.NewStore(t)
	t.Cleanup(func() { store.Close() })
	return store
}

func (s *Suite) testSetGet(t *testing.T) {
	store := s.newStore(t)
	ctx := context.Background()

	if err := store.Set(ctx, "suite:setget", []byte("hello"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, ok, err := store.Get(ctx, "suite:setget")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("Get: key not found")
	}
	if string(val) != "hello" {
		t.Fatalf("Get: got %q, want %q", val, "hello")
	}
}

func (s *Suite) testGetMissing(t *testing.T) {
	store := s.newStore(t)
	_, ok, err := store.Get(context.Background(), "suite:does-not-exist")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatal("Get: expected missing, got found")
	}
}

func (s *Suite) testOverwrite(t *testing.T) {
	store := s.newStore(t)
	ctx := context.Background()

	_ = store.Set(ctx, "suite:overwrite", []byte("first"), 0)
	_ = store.Set(ctx, "suite:overwrite", []byte("second"), 0)
	val, _, _ := store.Get(ctx, "suite:overwrite")
	if string(val) != "second" {
		t.Fatalf("Overwrite: got %q, want second", val)
	}
}

func (s *Suite) testDelete(t *testing.T) {
	store := s.newStore(t)
	ctx := context.Background()

	_ = store.Set(ctx, "suite:delete", []byte("v"), 0)
	if err := store.Delete(ctx, "suite:delete"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _ := store.Get(ctx, "suite:delete")
	if ok {
		t.Fatal("Delete: key still present")
	}
}

func (s *Suite) testList(t *testing.T) {
	store := s.newStore(t)
	ctx := context.Background()

	prefix := fmt.Sprintf("suite:list:%d:", time.Now().UnixNano())
	for i := 0; i < 3; i++ {
		_ = store.Set(ctx, fmt.Sprintf("%s%d", prefix, i), []byte("v"), 0)
	}
	_ = store.Set(ctx, "suite:unrelated:key", []byte("v"), 0)

	keys, err := store.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("List: got %d keys, want 3 (keys=%v)", len(keys), keys)
	}
	for _, k := range keys {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			t.Fatalf("List: unexpected key %q", k)
		}
	}
}

func (s *Suite) testPubSubBasic(t *testing.T) {
	store := s.newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := store.Subscribe(ctx, "suite:pub:")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	want := []byte("pubsub-payload")
	if err := store.Publish(ctx, "suite:pub:x", want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case ev := <-ch:
		if string(ev.Value) != string(want) {
			t.Fatalf("PubSub: got %q, want %q", ev.Value, want)
		}
	case <-ctx.Done():
		t.Fatal("PubSub: timed out waiting for event")
	}
}

func (s *Suite) testPushPullFIFO(t *testing.T) {
	store := s.newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	q := fmt.Sprintf("suite.push.fifo-%d", time.Now().UnixNano())
	items := []string{"first", "second", "third"}
	for _, item := range items {
		if err := store.Push(ctx, q, []byte(item)); err != nil {
			t.Fatalf("Push %q: %v", item, err)
		}
	}
	for i, want := range items {
		got, err := store.Pull(ctx, q)
		if err != nil {
			t.Fatalf("Pull[%d]: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("Pull[%d]: got %q, want %q", i, got, want)
		}
	}
}

func (s *Suite) testPushPullExactlyOnce(t *testing.T) {
	store := s.newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	q := fmt.Sprintf("suite.push.once-%d", time.Now().UnixNano())

	var mu sync.Mutex
	var received int

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := store.Pull(ctx, q)
			if err != nil {
				return
			}
			if string(data) == "payload" {
				mu.Lock()
				received++
				mu.Unlock()
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	if err := store.Push(ctx, q, []byte("payload")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	cancel()
	wg.Wait()

	mu.Lock()
	n := received
	mu.Unlock()
	if n != 1 {
		t.Fatalf("ExactlyOnce: %d receivers got the item, want exactly 1", n)
	}
}

func (s *Suite) testPullBlocksUntilPush(t *testing.T) {
	store := s.newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	q := fmt.Sprintf("suite.push.block-%d", time.Now().UnixNano())
	result := make(chan []byte, 1)

	go func() {
		data, err := store.Pull(ctx, q)
		if err == nil {
			result <- data
		}
	}()

	time.Sleep(100 * time.Millisecond)
	_ = store.Push(ctx, q, []byte("delayed"))

	select {
	case got := <-result:
		if string(got) != "delayed" {
			t.Fatalf("got %q, want delayed", got)
		}
	case <-ctx.Done():
		t.Fatal("Pull did not unblock after Push")
	}
}

func (s *Suite) testPullCancelledContext(t *testing.T) {
	store := s.newStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	q := fmt.Sprintf("suite.push.cancel-%d", time.Now().UnixNano())
	done := make(chan error, 1)
	go func() {
		_, err := store.Pull(ctx, q)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Pull should have returned an error after cancel")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Pull did not return after ctx cancel")
	}
}

// runBenchSetGet is a shared benchmark helper for any coord.Store backend.
func runBenchSetGet(b *testing.B, store Store) {
	b.Helper()
	ctx := context.Background()
	key := "bench:setget"
	val := []byte("bench-value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Set(ctx, key, val, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
		if _, _, err := store.Get(ctx, key); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}
