package bus

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestBusInterface is a reusable compliance suite for any Bus implementation.
// Use it from within the same package (internal test) via NewBus factory.
type TestBusInterface struct {
	Name   string
	NewBus func(cfg Config) (Bus, error)
}

func (s *TestBusInterface) Run(t *testing.T) {
	t.Helper()
	t.Run(s.Name, func(t *testing.T) {
		t.Run("PublishSubscribe", s.testPublishSubscribe)
		t.Run("SubscribePrefix", s.testSubscribePrefix)
		t.Run("PushOne", s.testPushOne)
		t.Run("MultiplePublishers", s.testMultiplePublishers)
		t.Run("ContextCancellation", s.testContextCancellation)
		t.Run("Close", s.testClose)
		t.Run("CloseIdempotent", s.testCloseIdempotent)
	})
}

func (s *TestBusInterface) newBus(t *testing.T) Bus {
	t.Helper()
	b, err := s.NewBus(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func (s *TestBusInterface) testPublishSubscribe(t *testing.T) {
	b := s.newBus(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "pepper.pub.test"
	subCh, err := b.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

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

func (s *TestBusInterface) testSubscribePrefix(t *testing.T) {
	b := s.newBus(t)

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

	time.Sleep(30 * time.Millisecond)
	b.Publish("pepper.res.abc", []byte("one"))
	time.Sleep(10 * time.Millisecond)
	b.Publish("pepper.res.def", []byte("two"))
	time.Sleep(10 * time.Millisecond)
	b.Publish("pepper.other.xyz", []byte("noise")) // must not arrive

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for prefix messages")
	}
}

func (s *TestBusInterface) testPushOne(t *testing.T) {
	b := s.newBus(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// PushOne with no listeners is acceptable — just must not block.
	_ = b.PushOne(ctx, "pepper.push.test", []byte("payload"))
}

func (s *TestBusInterface) testMultiplePublishers(t *testing.T) {
	b := s.newBus(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "pepper.pub.concurrent"
	subCh, err := b.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

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

func (s *TestBusInterface) testContextCancellation(t *testing.T) {
	b := s.newBus(t)

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
		t.Fatal("channel not closed after ctx cancel")
	}
}

func (s *TestBusInterface) testClose(t *testing.T) {
	b, err := s.NewBus(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

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
		t.Fatal("channel not closed after bus close")
	}
}

func (s *TestBusInterface) testCloseIdempotent(t *testing.T) {
	b, err := s.NewBus(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("second Close (idempotent): %v", err)
	}
}
