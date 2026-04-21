package coord

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// storeFactory returns a fresh Store and a cleanup func.
type storeFactory func(t *testing.T) (Store, func())

func memFactory(t *testing.T) (Store, func()) {
	t.Helper()
	s := NewMemory()
	return s, func() { _ = s.Close() }
}

func redisFactory(t *testing.T) (Store, func()) {
	t.Helper()
	url := os.Getenv("PEPPER_TEST_REDIS")
	if url == "" {
		t.Skip("PEPPER_TEST_REDIS not set (e.g. redis://127.0.0.1:6379)")
	}
	s, err := NewRedis(url)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	return s, func() { _ = s.Close() }
}

func natsFactory(t *testing.T) (Store, func()) {
	t.Helper()
	url := os.Getenv("PEPPER_TEST_NATS")
	if url == "" {
		t.Skip("PEPPER_TEST_NATS not set (e.g. nats://127.0.0.1:4222)")
	}
	s, err := NewNATS(url)
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

			if err := s.Set(ctx, "pepper:test:setget", []byte("hello"), 0); err != nil {
				t.Fatalf("Set: %v", err)
			}
			val, ok, err := s.Get(ctx, "pepper:test:setget")
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

			_, ok, err := s.Get(context.Background(), "pepper:test:does-not-exist")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if ok {
				t.Fatal("Get: expected missing, got found")
			}
		})
	}
}

func TestStore_Overwrite(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx := context.Background()

			_ = s.Set(ctx, "pepper:test:overwrite", []byte("first"), 0)
			_ = s.Set(ctx, "pepper:test:overwrite", []byte("second"), 0)
			val, _, _ := s.Get(ctx, "pepper:test:overwrite")
			if string(val) != "second" {
				t.Fatalf("Overwrite: got %q, want second", val)
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

			_ = s.Set(ctx, "pepper:test:delete", []byte("v"), 0)
			if err := s.Delete(ctx, "pepper:test:delete"); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			_, ok, _ := s.Get(ctx, "pepper:test:delete")
			if ok {
				t.Fatal("Delete: key still present")
			}
		})
	}
}

func TestStore_TTL_Expiry(t *testing.T) {
	// Only memory can test TTL expiry reliably without a long sleep.
	t.Run("memory", func(t *testing.T) {
		s, cleanup := memFactory(t)
		defer cleanup()
		ctx := context.Background()

		_ = s.Set(ctx, "pepper:test:ttl", []byte("expires"), 1)
		_, ok, _ := s.Get(ctx, "pepper:test:ttl")
		if !ok {
			t.Fatal("TTL: key should exist immediately after set")
		}
		time.Sleep(1100 * time.Millisecond)
		_, ok, _ = s.Get(ctx, "pepper:test:ttl")
		if ok {
			t.Fatal("TTL: key should have expired after 1s")
		}
	})
}

func TestStore_List(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx := context.Background()

			// Use a unique prefix to avoid collisions with other test runs.
			prefix := fmt.Sprintf("pepper:test:list:%d:", time.Now().UnixNano())
			for i := 0; i < 3; i++ {
				_ = s.Set(ctx, fmt.Sprintf("%s%d", prefix, i), []byte("v"), 0)
			}
			_ = s.Set(ctx, "pepper:other:unrelated", []byte("v"), 0)

			keys, err := s.List(ctx, prefix)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(keys) != 3 {
				t.Fatalf("List: got %d keys, want 3 (keys=%v)", len(keys), keys)
			}
			for _, k := range keys {
				if k[:len(prefix)] != prefix {
					t.Fatalf("List: unexpected key %q", k)
				}
			}
		})
	}
}

// Pub/sub

func TestStore_PubSub_Basic(t *testing.T) {
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
			time.Sleep(20 * time.Millisecond) // let subscription register

			want := []byte("pubsub-payload")
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

func TestStore_PubSub_PrefixIsolation(t *testing.T) {
	// Events published to a different prefix must not arrive on the subscriber.
	t.Run("memory", func(t *testing.T) {
		s, cleanup := memFactory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		ch, _ := s.Subscribe(ctx, "pepper:cap:")
		time.Sleep(10 * time.Millisecond)

		_ = s.Publish(ctx, "pepper:cap:echo", []byte("cap-event"))
		_ = s.Publish(ctx, "pepper:worker:w1", []byte("worker-event")) // must not arrive

		select {
		case ev := <-ch:
			if string(ev.Value) != "cap-event" {
				t.Fatalf("got %q, want cap-event", ev.Value)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for cap event")
		}

		select {
		case ev := <-ch:
			t.Fatalf("received unexpected event: channel=%q value=%q", ev.Channel, ev.Value)
		case <-time.After(60 * time.Millisecond):
			// nothing leaked — good
		}
	})
}

func TestStore_Subscribe_ClosedOnCtxDone(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		s, cleanup := memFactory(t)
		defer cleanup()
		ctx, cancel := context.WithCancel(context.Background())

		ch, _ := s.Subscribe(ctx, "pepper:test:close:")
		cancel()

		// Channel must close promptly after ctx is cancelled.
		select {
		case _, ok := <-ch:
			if ok {
				// received a value before close — acceptable if something was buffered
			}
			// channel closed
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Subscribe channel not closed after ctx cancel")
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

			q := fmt.Sprintf("pepper.push.test-basic-%d", time.Now().UnixNano())
			want := []byte("work-item")

			if err := s.Push(ctx, q, want); err != nil {
				t.Fatalf("Push: %v", err)
			}
			got, err := s.Pull(ctx, q)
			if err != nil {
				t.Fatalf("Pull: %v", err)
			}
			if string(got) != string(want) {
				t.Fatalf("Pull: got %q, want %q", got, want)
			}
		})
	}
}

func TestStore_PushPull_FIFO(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			q := fmt.Sprintf("pepper.push.test-fifo-%d", time.Now().UnixNano())
			items := []string{"first", "second", "third"}
			for _, item := range items {
				if err := s.Push(ctx, q, []byte(item)); err != nil {
					t.Fatalf("Push %q: %v", item, err)
				}
			}
			for i, want := range items {
				got, err := s.Pull(ctx, q)
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

func TestStore_PushPull_ExactlyOnce(t *testing.T) {
	// Push one item; two concurrent Pull callers — exactly one should receive it.
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			q := fmt.Sprintf("pepper.push.test-once-%d", time.Now().UnixNano())

			var mu sync.Mutex
			var received int

			// Start two concurrent pullers.
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					data, err := s.Pull(ctx, q)
					if err != nil {
						return // ctx cancelled after test ends — expected
					}
					if string(data) == "payload" {
						mu.Lock()
						received++
						mu.Unlock()
					}
				}()
			}

			time.Sleep(50 * time.Millisecond) // let both block on Pull
			if err := s.Push(ctx, q, []byte("payload")); err != nil {
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
		})
	}
}

func TestStore_Pull_BlocksUntilPush(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			q := fmt.Sprintf("pepper.push.test-block-%d", time.Now().UnixNano())
			result := make(chan []byte, 1)

			go func() {
				data, err := s.Pull(ctx, q)
				if err == nil {
					result <- data
				}
			}()

			time.Sleep(100 * time.Millisecond) // confirm Pull is blocking
			_ = s.Push(ctx, q, []byte("delayed"))

			select {
			case got := <-result:
				if string(got) != "delayed" {
					t.Fatalf("got %q, want delayed", got)
				}
			case <-ctx.Done():
				t.Fatal("Pull did not unblock after Push")
			}
		})
	}
}

func TestStore_Pull_CancelledContext(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithCancel(context.Background())

			q := fmt.Sprintf("pepper.push.test-cancel-%d", time.Now().UnixNano())
			done := make(chan error, 1)
			go func() {
				_, err := s.Pull(ctx, q) // nothing pushed — will block
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
		})
	}
}

func TestStore_PushPull_QueueIsolation(t *testing.T) {
	// Items on different queues must not bleed into each other.
	t.Run("memory", func(t *testing.T) {
		s, cleanup := memFactory(t)
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		n := time.Now().UnixNano()
		qa := fmt.Sprintf("pepper.push.qa-%d", n)
		qb := fmt.Sprintf("pepper.push.qb-%d", n)

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

// Binary payload correctness

func TestStore_BinaryPayloads(t *testing.T) {
	// msgpack-encoded envelopes are binary — verify round-trip fidelity.
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s, cleanup := b.factory(t)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			payload := make([]byte, 256)
			for i := range payload {
				payload[i] = byte(i)
			}

			// KV round-trip
			_ = s.Set(ctx, "pepper:test:binary", payload, 0)
			got, ok, _ := s.Get(ctx, "pepper:test:binary")
			if !ok {
				t.Fatal("binary KV: key not found")
			}
			if len(got) != len(payload) {
				t.Fatalf("binary KV: length mismatch: got %d, want %d", len(got), len(payload))
			}
			for i := range payload {
				if got[i] != payload[i] {
					t.Fatalf("binary KV: byte %d: got %x, want %x", i, got[i], payload[i])
				}
			}

			// Queue round-trip
			q := fmt.Sprintf("pepper.push.test-binary-%d", time.Now().UnixNano())
			_ = s.Push(ctx, q, payload)
			got2, err := s.Pull(ctx, q)
			if err != nil {
				t.Fatalf("binary queue Pull: %v", err)
			}
			for i := range payload {
				if got2[i] != payload[i] {
					t.Fatalf("binary queue: byte %d: got %x, want %x", i, got2[i], payload[i])
				}
			}
		})
	}
}

// Interface compliance

func TestStore_InterfaceCompliance(t *testing.T) {
	var _ Store = NewMemory()
	// Redis and NATS are verified at runtime via var _ Store = (*Xstore)(nil)
	// in their respective files.
}
