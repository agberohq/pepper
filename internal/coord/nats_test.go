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

// skipIfNoNATS skips the test when no NATS server is reachable.
// Set PEPPER_TEST_NATS_URL (or PEPPER_TEST_NATS_HOST / PEPPER_TEST_NATS_PORT)
// to point at a live nats-server -js instance.
func skipIfNoNATS(t *testing.T) {
	t.Helper()
	host := os.Getenv("PEPPER_TEST_NATS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_NATS_PORT")
	if port == "" {
		port = "4222"
	}
	conn, err := net.DialTimeout("tcp", host+":"+port, 200*time.Millisecond)
	if err != nil {
		t.Skipf("NATS not available at %s:%s: %v", host, port, err)
	}
	conn.Close()
}

func natsURL() string {
	if u := os.Getenv("PEPPER_TEST_NATS_URL"); u != "" {
		return u
	}
	host := os.Getenv("PEPPER_TEST_NATS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_NATS_PORT")
	if port == "" {
		port = "4222"
	}
	return "nats://" + host + ":" + port
}

func newNATSStore(t *testing.T) Store {
	t.Helper()
	skipIfNoNATS(t)
	s, err := NewNATS(natsURL())
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// Bootstrap

// TestNATS_Bootstrap verifies that NewNATS succeeds and the returned store
// satisfies the Store interface. This is the minimal smoke test: if the
// server rejects CONNECT or stream creation, it will fail here, not
// silently during a later operation.
func TestNATS_Bootstrap(t *testing.T) {
	_ = newNATSStore(t)
}

// CONNECT header negotiation

// TestNATS_Delete_HeadersNegotiated verifies that Delete (which uses HPUB)
// works without the server logging "message headers not supported".
// Previously the CONNECT frame sent "{}" which did not negotiate headers=true,
// causing every HPUB to be rejected.
func TestNATS_Delete_HeadersNegotiated(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()

	key := uniqueKey("nats:delete:hdr")
	if err := s.Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete (HPUB): %v — did headers=true get negotiated?", err)
	}
	_, ok, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if ok {
		t.Fatal("key still present after Delete")
	}
}

// KV bucket creation

// TestNATS_KVBucket_UnknownField verifies that NewNATS does not include the
// "history" field in its STREAM.CREATE payload. NATS 2.11 rejects that field
// with "json: unknown field \"history\"". max_msgs_per_subject=1 is the
// correct wire representation for KV history=1.
func TestNATS_KVBucket_UnknownField(t *testing.T) {
	// If NewNATS returns successfully the stream was created without error.
	// A second call must also succeed (stream already exists case).
	s1 := newNATSStore(t)
	_ = s1

	s2, err := NewNATS(natsURL())
	if err != nil {
		t.Fatalf("NewNATS (idempotent second call): %v", err)
	}
	_ = s2.Close()
}

// Key-Value

func TestNATS_SetGet(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()

	key := uniqueKey("nats:sg")
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
		t.Fatalf("Get: got %q, want %q", val, "hello")
	}
}

func TestNATS_GetMissing(t *testing.T) {
	s := newNATSStore(t)
	_, ok, err := s.Get(context.Background(), uniqueKey("nats:missing"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatal("Get: expected miss, got hit")
	}
}

func TestNATS_Overwrite(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()
	key := uniqueKey("nats:ow")
	_ = s.Set(ctx, key, []byte("first"), 0)
	_ = s.Set(ctx, key, []byte("second"), 0)
	val, _, _ := s.Get(ctx, key)
	if string(val) != "second" {
		t.Fatalf("Overwrite: got %q, want second", val)
	}
}

func TestNATS_Delete(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()
	key := uniqueKey("nats:del")
	_ = s.Set(ctx, key, []byte("v"), 0)
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _ := s.Get(ctx, key)
	if ok {
		t.Fatal("Delete: key still present")
	}
}

func TestNATS_List(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()

	prefix := uniqueKey("nats:list:") + ":"
	for i := 0; i < 3; i++ {
		_ = s.Set(ctx, prefix+string(rune('a'+i)), []byte("v"), 0)
	}
	_ = s.Set(ctx, uniqueKey("nats:other"), []byte("v"), 0)

	// Allow the KV stream index to settle.
	time.Sleep(100 * time.Millisecond)

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

func TestNATS_BinaryPayload(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	key := uniqueKey("nats:bin")
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

func TestNATS_PubSub_Basic(t *testing.T) {
	s := newNATSStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := s.Subscribe(ctx, "nats.test.pub.")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	want := []byte("pubsub-payload")
	if err := s.Publish(ctx, "nats.test.pub.x", want); err != nil {
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

func TestNATS_Subscribe_ExactAndPrefix(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()

	tests := []struct {
		name         string
		subscribeTo  string
		publishTo    []string
		wantReceived []string
	}{
		{
			name:         "exact topic",
			subscribeTo:  "pepper.control.w-1",
			publishTo:    []string{"pepper.control.w-1"},
			wantReceived: []string{"pepper.control.w-1"},
		},
		{
			name:         "prefix subscribe",
			subscribeTo:  "pepper.control",
			publishTo:    []string{"pepper.control.w-1", "pepper.control.w-2"},
			wantReceived: []string{"pepper.control.w-1", "pepper.control.w-2"},
		},
		{
			name:         "wildcard * converted to >",
			subscribeTo:  "pepper.test.*",
			publishTo:    []string{"pepper.test.a", "pepper.test.b"},
			wantReceived: []string{"pepper.test.a", "pepper.test.b"},
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
			time.Sleep(100 * time.Millisecond)

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
				t.Errorf("Subscribe(%q) missed expected topic %q", tt.subscribeTo, missing)
			}
		})
	}
}

func TestNATS_Subscribe_UniqueSIDs(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()

	subCtx1, cancel1 := context.WithCancel(ctx)
	subCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel1()
	defer cancel2()

	ch1, err := s.Subscribe(subCtx1, "pepper.control.w-1")
	if err != nil {
		t.Fatalf("Subscribe #1: %v", err)
	}
	ch2, err := s.Subscribe(subCtx2, "pepper.control.w-1")
	if err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := s.Publish(ctx, "pepper.control.w-1", []byte("msg")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var got1, got2 bool
	for _, ch := range []<-chan Event{ch1, ch2} {
		select {
		case <-ch:
			if ch == ch1 {
				got1 = true
			} else {
				got2 = true
			}
		case <-time.After(500 * time.Millisecond):
		}
	}
	if !got1 || !got2 {
		t.Errorf("both subscriptions should receive; got1=%v got2=%v", got1, got2)
	}
}

// Push / Pull (JetStream)

// TestNATS_PushPull_ExactTopic is the regression test for the original fire-and-
// forget race: Push fires before Pull has subscribed and the message is lost.
// With JetStream the message is retained, so Pull can arrive arbitrarily late.
func TestNATS_PushPull_ExactTopic(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()

	queue := "pepper.push.default"
	payload := []byte("test-payload")

	// Push FIRST — no puller is active yet.
	if err := s.Push(ctx, queue, payload); err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Pull AFTER — JetStream must retain the message.
	pullCtx, pullCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pullCancel()

	got, err := s.Pull(pullCtx, queue)
	if err != nil {
		t.Fatalf("Pull: %v — message was lost (fire-and-forget regression)", err)
	}
	if string(got) != string(payload) {
		t.Errorf("Pull got %q, want %q", got, payload)
	}
}

// TestNATS_PushPull_PullBeforePush verifies that Pull blocks when the queue
// is empty and unblocks when Push is called later.
func TestNATS_PushPull_PullBeforePush(t *testing.T) {
	s := newNATSStore(t)
	ctx := context.Background()

	queue := uniqueKey("pepper.push.block")
	result := make(chan []byte, 1)

	pullCtx, pullCancel := context.WithTimeout(ctx, 15*time.Second)
	defer pullCancel()

	go func() {
		data, err := s.Pull(pullCtx, queue)
		if err == nil {
			result <- data
		}
	}()

	// Give Pull time to register its consumer.
	time.Sleep(200 * time.Millisecond)

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

// TestNATS_PushPull_ExactlyOnce verifies that a single queued item is
// delivered to exactly one of two concurrent Pull callers — the JetStream
// workqueue retention policy with max_ack_pending=1 enforces this.
func TestNATS_PushPull_ExactlyOnce(t *testing.T) {
	s := newNATSStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queue := uniqueKey("pepper.push.once")
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

	time.Sleep(100 * time.Millisecond)
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

// TestNATS_PushPull_FIFO verifies ordering: items pushed in sequence are
// pulled in the same sequence (JetStream per-subject ordering guarantee).
func TestNATS_PushPull_FIFO(t *testing.T) {
	s := newNATSStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queue := uniqueKey("pepper.push.fifo")
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

// TestNATS_PushPull_QueueIsolation verifies that items on different queue
// subjects do not bleed into each other.
func TestNATS_PushPull_QueueIsolation(t *testing.T) {
	s := newNATSStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	qa := uniqueKey("pepper.push.qa")
	qb := uniqueKey("pepper.push.qb")

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

// TestNATS_PushPull_CancelledContext verifies that Pull returns promptly
// when the context is cancelled with an empty queue.
func TestNATS_PushPull_CancelledContext(t *testing.T) {
	s := newNATSStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	queue := uniqueKey("pepper.push.cancel")
	done := make(chan error, 1)
	go func() {
		_, err := s.Pull(ctx, queue)
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Pull should return an error after ctx cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Pull did not return after ctx cancel")
	}
}

// TestNATS_PushPull_BinaryPayload verifies binary round-trip fidelity via
// the work queue (hex encoding must preserve all 256 byte values).
func TestNATS_PushPull_BinaryPayload(t *testing.T) {
	s := newNATSStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	queue := uniqueKey("pepper.push.bin")
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

// TestNATS_PushPull_ConcurrentPullers stress-tests that 10 concurrent pullers
// across a 20-item queue each receive exactly one item (no double delivery).
func TestNATS_PushPull_ConcurrentPullers(t *testing.T) {
	s := newNATSStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const n = 20
	queue := uniqueKey("pepper.push.concurrent")

	for i := 0; i < n; i++ {
		if err := s.Push(ctx, queue, []byte("item")); err != nil {
			t.Fatalf("Push[%d]: %v", i, err)
		}
	}

	var received atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				pullCtx, pullCancel := context.WithTimeout(ctx, 2*time.Second)
				data, err := s.Pull(pullCtx, queue)
				pullCancel()
				if err != nil {
					return
				}
				if string(data) == "item" {
					received.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := received.Load(); got != n {
		t.Fatalf("concurrent: got %d deliveries, want %d", got, n)
	}
}

// Helpers

var keySeq atomic.Int64

func uniqueKey(prefix string) string {
	return prefix + "." + strconv.FormatInt(keySeq.Add(1), 10) + "." + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
