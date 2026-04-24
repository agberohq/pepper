package coord

import (
	"context"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func skipIfNoRedis(t *testing.T) {
	t.Helper()
	host := os.Getenv("PEPPER_TEST_REDIS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	conn, err := net.DialTimeout("tcp", host+":"+port, 200*time.Millisecond)
	if err != nil {
		t.Skipf("Redis not available at %s:%s: %v", host, port, err)
	}
	conn.Close()
}

func redisURL() string {
	if u := os.Getenv("PEPPER_TEST_REDIS_URL"); u != "" {
		return u
	}
	if u := os.Getenv("PEPPER_TEST_REDIS"); u != "" {
		return u
	}
	host := os.Getenv("PEPPER_TEST_REDIS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	return "redis://" + host + ":" + port
}

func newRedisStore(t *testing.T) Store {
	t.Helper()
	skipIfNoRedis(t)
	s, err := NewRedis(redisURL())
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

var redisKeySeq atomic.Int64

func redisUniqueKey(prefix string) string {
	return prefix + "." + strconv.FormatInt(redisKeySeq.Add(1), 10) + "." + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// Bootstrap

func TestRedis_Bootstrap(t *testing.T) {
	_ = newRedisStore(t)
}

// Key-Value

func TestRedis_SetGet(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()

	key := redisUniqueKey("redis:sg")
	if err := s.Set(ctx, key, []byte("hello"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, ok, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("Get: key not found")
	}
	if string(val) != "hello" {
		t.Fatalf("Get: got %q, want hello", val)
	}
}

func TestRedis_GetMissing(t *testing.T) {
	s := newRedisStore(t)
	_, ok, err := s.Get(context.Background(), redisUniqueKey("redis:missing"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatal("Get: expected miss, got hit")
	}
}

func TestRedis_Overwrite(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()
	key := redisUniqueKey("redis:ow")
	_ = s.Set(ctx, key, []byte("first"), 0)
	_ = s.Set(ctx, key, []byte("second"), 0)
	val, _, _ := s.Get(ctx, key)
	if string(val) != "second" {
		t.Fatalf("Overwrite: got %q, want second", val)
	}
}

func TestRedis_Delete(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()
	key := redisUniqueKey("redis:del")
	_ = s.Set(ctx, key, []byte("v"), 0)
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _ := s.Get(ctx, key)
	if ok {
		t.Fatal("Delete: key still present")
	}
}

func TestRedis_List(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()

	prefix := redisUniqueKey("redis:list:") + ":"
	for i := 0; i < 3; i++ {
		_ = s.Set(ctx, prefix+string(rune('a'+i)), []byte("v"), 0)
	}
	_ = s.Set(ctx, redisUniqueKey("redis:other"), []byte("v"), 0)

	keys, err := s.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("List: got %d keys, want 3 (keys=%v)", len(keys), keys)
	}
	for _, k := range keys {
		if !hasPrefix(k, prefix) {
			t.Fatalf("List: unexpected key %q", k)
		}
	}
}

func TestRedis_TTL_Expiry(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()
	key := redisUniqueKey("redis:ttl")
	_ = s.Set(ctx, key, []byte("expires"), 1)
	_, ok, _ := s.Get(ctx, key)
	if !ok {
		t.Fatal("TTL: key should exist immediately after set")
	}
	time.Sleep(1100 * time.Millisecond)
	_, ok, _ = s.Get(ctx, key)
	if ok {
		t.Fatal("TTL: key should have expired after 1s")
	}
}

func TestRedis_BinaryPayload(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	key := redisUniqueKey("redis:bin")
	_ = s.Set(ctx, key, payload, 0)
	got, ok, err := s.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get binary: ok=%v err=%v", ok, err)
	}
	if len(got) != len(payload) {
		t.Fatalf("binary: length %d != %d", len(got), len(payload))
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("binary: byte %d: got %x, want %x", i, got[i], payload[i])
		}
	}
}

// Pub/Sub

func TestRedis_PubSub_Basic(t *testing.T) {
	s := newRedisStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := s.Subscribe(ctx, "redis.test.pub.")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	want := []byte("pubsub-payload")
	if err := s.Publish(ctx, "redis.test.pub.x", want); err != nil {
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

func TestRedis_PubSub_PrefixIsolation(t *testing.T) {
	s := newRedisStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, _ := s.Subscribe(ctx, "redis:cap:")
	time.Sleep(50 * time.Millisecond)

	_ = s.Publish(ctx, "redis:cap:echo", []byte("cap-event"))
	_ = s.Publish(ctx, "redis:worker:w1", []byte("worker-event"))

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
}

func TestRedis_Subscribe_ClosedOnCtxDone(t *testing.T) {
	s := newRedisStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	ch, _ := s.Subscribe(ctx, "redis:test:close:")
	cancel()

	select {
	case _, ok := <-ch:
		_ = ok
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Subscribe channel not closed after ctx cancel")
	}
}

func TestRedis_Subscribe_ExactAndPrefix(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		subscribeTo  string
		publishTo    []string
		wantReceived []string
	}{
		{
			name:         "exact topic",
			subscribeTo:  "redis.control.w-1",
			publishTo:    []string{"redis.control.w-1"},
			wantReceived: []string{"redis.control.w-1"},
		},
		{
			name:         "prefix subscribe",
			subscribeTo:  "redis.control",
			publishTo:    []string{"redis.control.w-1", "redis.control.w-2"},
			wantReceived: []string{"redis.control.w-1", "redis.control.w-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subCtx, subCancel := context.WithCancel(ctx)
			defer subCancel()

			ch, err := s.Subscribe(subCtx, tt.subscribeTo)
			if err != nil {
				t.Fatalf("Subscribe(%q): %v", tt.subscribeTo, err)
			}
			time.Sleep(50 * time.Millisecond)

			var mu sync.Mutex
			var received []string
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case ev, ok := <-ch:
						if !ok {
							return
						}
						mu.Lock()
						received = append(received, ev.Channel)
						n := len(received)
						mu.Unlock()
						if n >= len(tt.wantReceived) {
							return
						}
					case <-time.After(2 * time.Second):
						return
					}
				}
			}()

			for _, topic := range tt.publishTo {
				if err := s.Publish(ctx, topic, []byte("msg")); err != nil {
					t.Errorf("Publish(%q): %v", topic, err)
				}
			}
			wg.Wait()

			wantSet := make(map[string]bool)
			for _, w := range tt.wantReceived {
				wantSet[w] = true
			}
			mu.Lock()
			for _, r := range received {
				delete(wantSet, r)
			}
			mu.Unlock()
			for missing := range wantSet {
				t.Errorf("Subscribe(%q) missed topic %q", tt.subscribeTo, missing)
			}
		})
	}
}

// Push / Pull

// TestRedis_PushPull_ExactTopic is the regression test: Push fires before any
// Pull is active. Redis BRPOP ensures the message is retained and delivered.
func TestRedis_PushPull_ExactTopic(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()

	queue := "pepper.push.redis-default"
	payload := []byte("redis-test-payload")

	if err := s.Push(ctx, queue, payload); err != nil {
		t.Fatalf("Push: %v", err)
	}

	pullCtx, pullCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pullCancel()

	got, err := s.Pull(pullCtx, queue)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("Pull got %q, want %q", got, payload)
	}
}

func TestRedis_PushPull_PullBeforePush(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()

	queue := redisUniqueKey("pepper.push.redis-block")
	result := make(chan []byte, 1)

	pullCtx, pullCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pullCancel()

	go func() {
		data, err := s.Pull(pullCtx, queue)
		if err == nil {
			result <- data
		}
	}()

	time.Sleep(100 * time.Millisecond)
	if err := s.Push(ctx, queue, []byte("delayed")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	select {
	case got := <-result:
		if string(got) != "delayed" {
			t.Fatalf("got %q, want delayed", got)
		}
	case <-pullCtx.Done():
		t.Fatal("Pull did not unblock after Push")
	}
}

func TestRedis_PushPull_ExactlyOnce(t *testing.T) {
	s := newRedisStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queue := redisUniqueKey("pepper.push.redis-once")
	var received atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := s.Pull(ctx, queue)
			if err != nil {
				return
			}
			if string(data) == "payload" {
				received.Add(1)
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	if err := s.Push(ctx, queue, []byte("payload")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	cancel()
	wg.Wait()

	if n := received.Load(); n != 1 {
		t.Fatalf("ExactlyOnce: %d receivers got the item, want exactly 1", n)
	}
}

func TestRedis_PushPull_FIFO(t *testing.T) {
	s := newRedisStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queue := redisUniqueKey("pepper.push.redis-fifo")
	items := []string{"alpha", "beta", "gamma"}
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
}

func TestRedis_PushPull_QueueIsolation(t *testing.T) {
	s := newRedisStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	qa := redisUniqueKey("pepper.push.redis-qa")
	qb := redisUniqueKey("pepper.push.redis-qb")

	_ = s.Push(ctx, qa, []byte("for-a"))
	_ = s.Push(ctx, qb, []byte("for-b"))

	gotA, errA := s.Pull(ctx, qa)
	gotB, errB := s.Pull(ctx, qb)

	if errA != nil || string(gotA) != "for-a" {
		t.Errorf("queue A: err=%v got=%q", errA, gotA)
	}
	if errB != nil || string(gotB) != "for-b" {
		t.Errorf("queue B: err=%v got=%q", errB, gotB)
	}
}

func TestRedis_PushPull_CancelledContext(t *testing.T) {
	s := newRedisStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	queue := redisUniqueKey("pepper.push.redis-cancel")
	done := make(chan error, 1)
	go func() {
		_, err := s.Pull(ctx, queue)
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Pull should return an error after ctx cancel")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Pull did not return after ctx cancel")
	}
}

func TestRedis_PushPull_BinaryPayload(t *testing.T) {
	s := newRedisStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	queue := redisUniqueKey("pepper.push.redis-bin")
	if err := s.Push(ctx, queue, payload); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got, err := s.Pull(ctx, queue)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("length: got %d, want %d", len(got), len(payload))
	}
	for i := range payload {
		if got[i] != payload[i] {
			t.Fatalf("byte %d: got %x, want %x", i, got[i], payload[i])
		}
	}
}

// PSUBSCRIBE-specific

func TestRedis_Subscribe_PSUBSCRIBE_Pattern(t *testing.T) {
	s := newRedisStore(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		subscribeTo  string
		publishTo    []string
		wantReceived []string
	}{
		{
			name:         "prefix subscribe via PSUBSCRIBE",
			subscribeTo:  "redis.control",
			publishTo:    []string{"redis.control.w-1", "redis.control.w-2"},
			wantReceived: []string{"redis.control.w-1", "redis.control.w-2"},
		},
		{
			name:         "wildcard * appended for PSUBSCRIBE",
			subscribeTo:  "redis.test",
			publishTo:    []string{"redis.test.a", "redis.test.b"},
			wantReceived: []string{"redis.test.a", "redis.test.b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subCtx, subCancel := context.WithCancel(ctx)
			defer subCancel()

			ch, err := s.Subscribe(subCtx, tt.subscribeTo)
			if err != nil {
				t.Fatalf("Subscribe(%q): %v", tt.subscribeTo, err)
			}
			time.Sleep(50 * time.Millisecond)

			var mu sync.Mutex
			var received []string
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case ev, ok := <-ch:
						if !ok {
							return
						}
						mu.Lock()
						received = append(received, ev.Channel)
						n := len(received)
						mu.Unlock()
						if n >= len(tt.wantReceived) {
							return
						}
					case <-time.After(2 * time.Second):
						return
					}
				}
			}()

			for _, topic := range tt.publishTo {
				if err := s.Publish(ctx, topic, []byte("msg")); err != nil {
					t.Errorf("Publish(%q): %v", topic, err)
				}
			}
			wg.Wait()

			wantSet := make(map[string]bool)
			for _, w := range tt.wantReceived {
				wantSet[w] = true
			}
			mu.Lock()
			for _, r := range received {
				delete(wantSet, r)
			}
			mu.Unlock()
			for missing := range wantSet {
				t.Errorf("Subscribe(%q) missed topic %q", tt.subscribeTo, missing)
			}
		})
	}
}

func TestRedis_Subscribe_ExactTopic(t *testing.T) {
	s := newRedisStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	ch, err := s.Subscribe(subCtx, "redis.control.w-1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := s.Publish(ctx, "redis.control.w-1", []byte("exact-msg")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Channel != "redis.control.w-1" {
			t.Errorf("Got channel %q, want redis.control.w-1", ev.Channel)
		}
	case <-ctx.Done():
		t.Error("Timeout waiting for exact topic message")
	}
}
