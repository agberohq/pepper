package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/bus"
)

// newTestBus creates a Mula-backed bus for testing.
func newTestBus(t *testing.T) bus.Bus {
	t.Helper()
	m, err := bus.NewMulaFromMulaConfig(bus.MulaConfig{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128})
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	return m.AsBus()
}

func TestMula(t *testing.T) {
	testPublishSubscribe(t)
	testSubscribePrefix(t)
	testPushOne(t)
	testMultiplePublishers(t)
	testContextCancellation(t)
	testClose(t)
}

func TestMulaClose(t *testing.T) {
	b := newTestBus(t)
	if err := b.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("second Close: %v", err)
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

func BenchmarkMulaPublish(b *testing.B) {
	m, err := bus.NewMulaFromMulaConfig(bus.MulaConfig{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128})
	if err != nil {
		b.Fatal(err)
	}
	adapted := m.AsBus()
	defer adapted.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "pepper.pub.bench"
	subCh, err := adapted.Subscribe(ctx, topic)
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
		if err := adapted.Publish(topic, data); err != nil {
			b.Fatal(err)
		}
	}
}

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

	testData := []byte("hello world")
	if err := b.Publish(topic, testData); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-subCh:
		if msg.Topic != topic {
			t.Errorf("expected topic %q, got %q", topic, msg.Topic)
		}
		if string(msg.Data) != string(testData) {
			t.Errorf("expected data %q, got %q", testData, msg.Data)
		}
	case <-time.After(2 * time.Second):
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
	b.Publish("pepper.res.abc", []byte("test"))
	time.Sleep(20 * time.Millisecond)
	b.Publish("pepper.res.def", []byte("test"))
	time.Sleep(20 * time.Millisecond)
	b.Publish("pepper.other.xyz", []byte("test"))

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for prefix messages")
	}
}

func testPushOne(t *testing.T) {
	t.Helper()
	b := newTestBus(t)
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := b.PushOne(ctx, "pepper.push.gpu", []byte("test"))
	if err != nil {
		t.Logf("PushOne returned error (no workers): %v", err)
	}
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
	timeout := time.After(3 * time.Second)
	for received < n {
		select {
		case <-subCh:
			received++
		case <-timeout:
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
	time.Sleep(50 * time.Millisecond)

	select {
	case _, ok := <-subCh:
		if ok {
			t.Error("channel should be closed after context cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel close")
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
		t.Fatal("timeout waiting for channel close")
	}

	b.Close() // idempotent
}
