package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/coord"
)

// newTestBus creates a Mula-backed Bus for external (_test) package tests.
func newTestBus(t *testing.T) bus.Bus {
	t.Helper()
	c, err := coord.NewMula("tcp://127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return bus.NewCoordBus(c, coord.MulaAddr(c))
}

// TestMulaBus runs the full publish/subscribe/push behavioural tests against
// a Mula-backed Bus.
func TestMulaBus(t *testing.T) {
	t.Run("PublishSubscribe", func(t *testing.T) { testPublishSubscribe(t) })
	t.Run("SubscribePrefix", func(t *testing.T) { testSubscribePrefix(t) })
	t.Run("PushOne", func(t *testing.T) { testPushOne(t) })
	t.Run("MultiplePublishers", func(t *testing.T) { testMultiplePublishers(t) })
	t.Run("ContextCancellation", func(t *testing.T) { testContextCancellation(t) })
	t.Run("Close", func(t *testing.T) { testClose(t) })
}

func TestMulaCloseIdempotent(t *testing.T) {
	b := newTestBus(t)
	if err := b.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("second Close (idempotent): %v", err)
	}
	if err := b.Publish("test", []byte("data")); err == nil {
		t.Error("Publish after close should fail")
	}
}

func TestTopicHelpers(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		expected string
		fn       func(string) string
	}{
		{"TopicPush", "gpu", "pepper.push.gpu", bus.TopicPush},
		{"TopicPub", "gpu", "pepper.pub.gpu", bus.TopicPub},
		{"TopicRes", "origin123", "pepper.res.origin123", bus.TopicRes},
		{"TopicPipe", "asr", "pepper.pipe.asr", bus.TopicPipe},
		{"TopicStream", "corr456", "pepper.stream.corr456", bus.TopicStream},
		{"TopicCb", "corr789", "pepper.cb.corr789", bus.TopicCb},
		{"TopicHB", "worker1", "pepper.hb.worker1", bus.TopicHB},
		{"TopicControl", "worker1", "pepper.control.worker1", bus.TopicControl},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.fn(tt.topic); got != tt.expected {
				t.Errorf("%s(%q) = %q, want %q", tt.name, tt.topic, got, tt.expected)
			}
		})
	}
}

func TestIsBroadcastTopic(t *testing.T) {
	tests := []struct {
		topic string
		want  bool
	}{
		{"pepper.pub.gpu", true},
		{"pepper.broadcast", true},
		{"pepper.push.gpu", false},
		{"pepper.res.abc", false},
	}
	for _, tt := range tests {
		if got := bus.IsBroadcastTopic(tt.topic); got != tt.want {
			t.Errorf("IsBroadcastTopic(%q) = %v, want %v", tt.topic, got, tt.want)
		}
	}
}

func TestWorkerEnvVars(t *testing.T) {
	vars := bus.WorkerEnvVars("w-1", "tcp://127.0.0.1:7731", "msgpack", []string{"gpu", "asr"}, 5000, 8, "/dev/shm/pepper")
	expected := map[string]string{
		"PEPPER_WORKER_ID":      "w-1",
		"PEPPER_BUS_URL":        "tcp://127.0.0.1:7731",
		"PEPPER_CODEC":          "msgpack",
		"PEPPER_GROUPS":         "gpu,asr",
		"PEPPER_HEARTBEAT_MS":   "5000",
		"PEPPER_MAX_CONCURRENT": "8",
		"PEPPER_BLOB_DIR":       "/dev/shm/pepper",
	}
	for _, v := range vars {
		for k, want := range expected {
			if len(v) > len(k)+1 && v[:len(k)+1] == k+"=" {
				if got := v[len(k)+1:]; got != want {
					t.Errorf("%s = %q, want %q", k, got, want)
				}
			}
		}
	}
}

func TestMockBus(t *testing.T) {
	m := bus.NewMock()
	defer m.Close()

	// Publish / Subscribe round-trip.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := m.Subscribe(ctx, "pepper.pub.test")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	if err := m.Publish("pepper.pub.test", []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case msg := <-ch:
		if string(msg.Data) != "hello" {
			t.Errorf("data: got %q, want hello", msg.Data)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for published message")
	}

	// PushOne / PopPush.
	if err := m.PushOne(ctx, "pepper.push.gpu", []byte("task-payload")); err != nil {
		t.Fatalf("PushOne: %v", err)
	}
	data, ok := m.PopPush("pepper.push.gpu")
	if !ok {
		t.Fatal("PopPush: expected message, got nothing")
	}
	if string(data) != "task-payload" {
		t.Errorf("PopPush: got %q, want task-payload", data)
	}

	// PopPush on empty queue returns false.
	if _, ok := m.PopPush("pepper.push.gpu"); ok {
		t.Error("PopPush: expected empty queue after dequeue")
	}
}

func BenchmarkMulaPublish(b *testing.B) {
	c, err := coord.NewMula("tcp://127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	bbus := bus.NewCoordBus(c, coord.MulaAddr(c))
	defer bbus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "pepper.pub.bench"
	subCh, err := bbus.Subscribe(ctx, topic)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		for range subCh {
		}
	}()

	data := []byte("benchmark data")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bbus.Publish(topic, data); err != nil {
			b.Fatal(err)
		}
	}
}

// shared test helpers

func testPublishSubscribe(t *testing.T) {
	t.Helper()
	b := newTestBus(t)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "pepper.pub.test"
	subCh, err := b.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	want := []byte("hello world")
	if err := b.Publish(topic, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-subCh:
		if msg.Topic != topic {
			t.Errorf("topic: got %q, want %q", msg.Topic, topic)
		}
		if string(msg.Data) != string(want) {
			t.Errorf("data: got %q, want %q", msg.Data, want)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func testSubscribePrefix(t *testing.T) {
	t.Helper()
	b := newTestBus(t)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prefix := "pepper.res."
	subCh, err := b.SubscribePrefix(ctx, prefix)
	if err != nil {
		t.Fatalf("SubscribePrefix: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		received := 0
		for msg := range subCh {
			if len(msg.Topic) < len(prefix) || msg.Topic[:len(prefix)] != prefix {
				t.Errorf("unexpected topic: %s", msg.Topic)
			}
			received++
			if received == 2 {
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	b.Publish("pepper.res.abc", []byte("one"))
	time.Sleep(20 * time.Millisecond)
	b.Publish("pepper.res.def", []byte("two"))
	time.Sleep(20 * time.Millisecond)
	b.Publish("pepper.other.xyz", []byte("noise")) // must not arrive

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for prefix messages")
	}
}

func testPushOne(t *testing.T) {
	t.Helper()
	b := newTestBus(t)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// PushOne with no listeners is acceptable — must not block past ctx.
	_ = b.PushOne(ctx, "pepper.push.gpu", []byte("test"))
}

func testMultiplePublishers(t *testing.T) {
	t.Helper()
	b := newTestBus(t)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "pepper.pub.concurrent"
	subCh, err := b.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	n := 10
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			b.Publish(topic, []byte{byte(id)})
		}(i)
	}
	wg.Wait()

	received := 0
	for received < n {
		select {
		case <-subCh:
			received++
		case <-ctx.Done():
			t.Fatalf("timeout: received %d/%d", received, n)
		}
	}
}

func testContextCancellation(t *testing.T) {
	t.Helper()
	b := newTestBus(t)
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	subCh, err := b.Subscribe(ctx, "pepper.pub.cancel")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	cancel()
	select {
	case _, ok := <-subCh:
		if ok {
			t.Error("channel should be closed after context cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel close after ctx cancel")
	}
}

func testClose(t *testing.T) {
	t.Helper()
	b := newTestBus(t)

	ctx := context.Background()
	subCh, err := b.Subscribe(ctx, "pepper.pub.close")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	b.Close()

	if err := b.Publish("pepper.pub.close", []byte("after")); err == nil {
		t.Error("Publish should fail after close")
	}

	select {
	case _, ok := <-subCh:
		if ok {
			t.Error("channel should be closed after bus close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel close after bus close")
	}

	b.Close() // idempotent
}

// TestPipelinePattern exercises the exact pattern used by the pipeline dispatcher:
// Subscribe to a prefix topic (stageCh)
// Subscribe to a push topic (workerCh)
// Push to the worker push topic (simulating router.Dispatch)
// Worker reads from workerCh, publishes result to the forward topic
// stageCh receives the forwarded result
func TestPipelinePattern(t *testing.T) {
	c, err := coord.NewMula("tcp://127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	b := bus.NewCoordBus(c, coord.MulaAddr(c))
	t.Cleanup(func() { b.Close() })

	bgCtx := context.Background()
	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Step 1: pipeline subscribes to prefix (simulates dispatchPipeline).
	stageCh, err := b.SubscribePrefix(callCtx, "pepper.pipe.foo.")
	if err != nil {
		t.Fatalf("SubscribePrefix: %v", err)
	}

	// Step 2: worker subscribes to its direct push topic (simulates goruntime.Start).
	workerCh, err := b.Subscribe(bgCtx, "pepper.push.w1")
	if err != nil {
		t.Fatalf("Subscribe workerCh: %v", err)
	}

	// Allow subscription goroutines to start.
	time.Sleep(5 * time.Millisecond)

	// Step 3: router pushes to worker (simulates router.Dispatch DispatchAny).
	if err := b.PushOne(callCtx, "pepper.push.w1", []byte(`{"task":"hello"}`)); err != nil {
		t.Fatalf("PushOne: %v", err)
	}

	// Step 4: worker receives from workerCh and publishes result to ForwardTo.
	select {
	case msg := <-workerCh:
		t.Logf("worker received: %s", msg.Data)
		// Worker publishes to the pipe forward topic.
		if err := b.Publish("pepper.pipe.foo.stage.1", []byte(`{"result":"HELLO"}`)); err != nil {
			t.Fatalf("Publish result: %v", err)
		}
	case <-callCtx.Done():
		t.Fatal("timeout: worker never received push message")
	}

	// Step 5: pipeline receives on stageCh.
	select {
	case msg := <-stageCh:
		t.Logf("pipeline stage received: topic=%s data=%s", msg.Topic, msg.Data)
		if string(msg.Data) != `{"result":"HELLO"}` {
			t.Errorf("got %q, want {\"result\":\"HELLO\"}", msg.Data)
		}
	case <-callCtx.Done():
		t.Fatal("timeout: pipeline stage never received forwarded result")
	}
}
