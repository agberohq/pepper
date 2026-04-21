package bus

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/coord"
)

// storeFactory returns a Store and a cleanup func.
type storeFactory func(t *testing.T) (coord.Store, func())

// memFactory always works — no external deps.
func memFactory(t *testing.T) (coord.Store, func()) {
	t.Helper()
	s := coord.NewMemory()
	return s, func() { _ = s.Close() }
}

// redisFactory skips if PEPPER_TEST_REDIS is not set.
func redisFactory(t *testing.T) (coord.Store, func()) {
	t.Helper()
	url := os.Getenv("PEPPER_TEST_REDIS")
	if url == "" {
		t.Skip("PEPPER_TEST_REDIS not set")
	}
	s, err := coord.NewRedis(url)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	return s, func() { _ = s.Close() }
}

// natsFactory skips if PEPPER_TEST_NATS is not set.
func natsFactory(t *testing.T) (coord.Store, func()) {
	t.Helper()
	url := os.Getenv("PEPPER_TEST_NATS")
	if url == "" {
		t.Skip("PEPPER_TEST_NATS not set")
	}
	s, err := coord.NewNATS(url)
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	return s, func() { _ = s.Close() }
}

var backends = []struct {
	name    string
	factory storeFactory
}{
	{"memory", memFactory},
	{"redis", redisFactory},
	{"nats", natsFactory},
}

// Key-value

func TestStore_SetGet(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx := context.Background()

			if err := s.Set(ctx, "pepper:test:key1", []byte("hello"), 0); err != nil {
				t.Fatalf("Set: %v", err)
			}
			val, ok, err := s.Get(ctx, "pepper:test:key1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !ok {
				t.Fatal("Get: key not found")
			}
			if string(val) != "hello" {
				t.Fatalf("Get: got %q, want %q", val, "hello")
			}
		})
	}
}

func TestStore_GetMissing(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx := context.Background()

			_, ok, err := s.Get(ctx, "pepper:test:nonexistent")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if ok {
				t.Fatal("Get: expected missing, got found")
			}
		})
	}
}

func TestStore_Delete(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx := context.Background()

			_ = s.Set(ctx, "pepper:test:del", []byte("v"), 0)
			if err := s.Delete(ctx, "pepper:test:del"); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			_, ok, _ := s.Get(ctx, "pepper:test:del")
			if ok {
				t.Fatal("Delete: key still present after delete")
			}
		})
	}
}

func TestStore_TTL(t *testing.T) {
	// Only memory backend can test TTL expiry reliably without sleeping long.
	t.Run("memory", func(t *testing.T) {
		s, cleanup := memFactory(t)
		defer cleanup()
		ctx := context.Background()

		_ = s.Set(ctx, "pepper:test:ttl", []byte("expires"), 1) // 1 second TTL
		val, ok, _ := s.Get(ctx, "pepper:test:ttl")
		if !ok {
			t.Fatal("TTL: key should exist immediately after set")
		}
		if string(val) != "expires" {
			t.Fatalf("TTL: got %q", val)
		}
		time.Sleep(1100 * time.Millisecond)
		_, ok, _ = s.Get(ctx, "pepper:test:ttl")
		if ok {
			t.Fatal("TTL: key should have expired")
		}
	})
}

func TestStore_List(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx := context.Background()

			prefix := fmt.Sprintf("pepper:test:list:%d:", time.Now().UnixNano())
			for i := 0; i < 3; i++ {
				_ = s.Set(ctx, prefix+fmt.Sprintf("%d", i), []byte("v"), 0)
			}
			// Unrelated key — should not appear.
			_ = s.Set(ctx, "pepper:other:key", []byte("v"), 0)

			keys, err := s.List(ctx, prefix)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(keys) != 3 {
				t.Fatalf("List: got %d keys, want 3 (keys=%v)", len(keys), keys)
			}
		})
	}
}

// Pub/sub

func TestStore_PubSub(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			ch, err := s.Subscribe(ctx, "pepper:test:pub:")
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			// Small delay so the subscription is active before publish.
			time.Sleep(20 * time.Millisecond)

			want := []byte("hello from pubsub")
			if err := s.Publish(ctx, "pepper:test:pub:x", want); err != nil {
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
		})
	}
}

func TestStore_PubSub_PrefixFilter(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		s, cleanup := memFactory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		ch, _ := s.Subscribe(ctx, "pepper:cap:")
		time.Sleep(10 * time.Millisecond)

		_ = s.Publish(ctx, "pepper:cap:echo", []byte("cap-data"))
		_ = s.Publish(ctx, "pepper:worker:w1", []byte("worker-data")) // should not arrive

		select {
		case ev := <-ch:
			if string(ev.Value) != "cap-data" {
				t.Fatalf("expected cap-data, got %q", ev.Value)
			}
		case <-ctx.Done():
			t.Fatal("timed out")
		}

		// Confirm worker event did not bleed through.
		select {
		case ev := <-ch:
			t.Fatalf("unexpected event: %v", ev)
		case <-time.After(50 * time.Millisecond):
			// Good — nothing leaked.
		}
	})
}

// Push / Pull

func TestStore_PushPull_Basic(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			queue := fmt.Sprintf("pepper.push.test-basic-%d", time.Now().UnixNano())
			want := []byte("work-item-1")

			if err := s.Push(ctx, queue, want); err != nil {
				t.Fatalf("Push: %v", err)
			}

			got, err := s.Pull(ctx, queue)
			if err != nil {
				t.Fatalf("Pull: %v", err)
			}
			if string(got) != string(want) {
				t.Fatalf("Pull: got %q, want %q", got, want)
			}
		})
	}
}

func TestStore_PushPull_ExactlyOnce(t *testing.T) {
	// Push one item; two concurrent Pull callers — exactly one should receive it.
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			queue := fmt.Sprintf("pepper.push.test-once-%d", time.Now().UnixNano())

			var (
				mu       sync.Mutex
				received []string
			)

			// Start two consumers.
			for i := 0; i < 2; i++ {
				go func(id int) {
					data, err := s.Pull(ctx, queue)
					if err != nil {
						return // ctx cancelled after test ends — expected
					}
					mu.Lock()
					received = append(received, fmt.Sprintf("consumer-%d:%s", id, data))
					mu.Unlock()
				}(i)
			}

			time.Sleep(50 * time.Millisecond) // let consumers block on Pull
			if err := s.Push(ctx, queue, []byte("payload")); err != nil {
				t.Fatalf("Push: %v", err)
			}

			// Give time for delivery.
			time.Sleep(300 * time.Millisecond)
			cancel()
			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			n := len(received)
			mu.Unlock()

			if n != 1 {
				t.Fatalf("ExactlyOnce: %d consumers received the item, want exactly 1 (got: %v)", n, received)
			}
		})
	}
}

func TestStore_PushPull_Order(t *testing.T) {
	// Items pushed in order should be pulled in the same order (FIFO).
	// Memory and Redis use LPUSH+BRPOP (right end = oldest), NATS is ordered per subject.
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			queue := fmt.Sprintf("pepper.push.test-order-%d", time.Now().UnixNano())
			items := []string{"first", "second", "third"}
			for _, item := range items {
				if err := s.Push(ctx, queue, []byte(item)); err != nil {
					t.Fatalf("Push %q: %v", item, err)
				}
			}

			for i, want := range items {
				got, err := s.Pull(ctx, queue)
				if err != nil {
					t.Fatalf("Pull[%d]: %v", i, err)
				}
				if string(got) != want {
					t.Fatalf("Pull[%d]: got %q, want %q", i, got, want)
				}
			}
		})
	}
}

func TestStore_Pull_ContextCancel(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

			queue := fmt.Sprintf("pepper.push.test-cancel-%d", time.Now().UnixNano())
			done := make(chan error, 1)
			go func() {
				_, err := s.Pull(ctx, queue) // will block — nothing pushed
				done <- err
			}()

			time.Sleep(50 * time.Millisecond)
			cancel() // trigger cancellation

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("Pull: expected error after cancel, got nil")
				}
			case <-time.After(4 * time.Second):
				t.Fatal("Pull did not return after context cancel")
			}
		})
	}
}

func TestStore_PushPull_MultipleQueues(t *testing.T) {
	// Items on different queues must not bleed into each other.
	t.Run("memory", func(t *testing.T) {
		s, cleanup := memFactory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		nano := time.Now().UnixNano()
		qa := fmt.Sprintf("pepper.push.qa-%d", nano)
		qb := fmt.Sprintf("pepper.push.qb-%d", nano)

		_ = s.Push(ctx, qa, []byte("for-a"))
		_ = s.Push(ctx, qb, []byte("for-b"))

		gotA, _ := s.Pull(ctx, qa)
		gotB, _ := s.Pull(ctx, qb)

		if string(gotA) != "for-a" {
			t.Fatalf("queue A: got %q, want for-a", gotA)
		}
		if string(gotB) != "for-b" {
			t.Fatalf("queue B: got %q, want for-b", gotB)
		}
	})
}

// Interface compliance

func TestStore_InterfaceCompliance(t *testing.T) {
	// Compile-time check that all backends satisfy the interface.
	var _ coord.Store = coord.NewMemory()
}
