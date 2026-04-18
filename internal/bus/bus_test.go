package bus

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestBusInterface is a test suite that runs against any Bus implementation.
type TestBusInterface struct {
	NewBus func(Config) (Bus, error)
	Name   string
}

func (suite *TestBusInterface) Run(t *testing.T) {
	t.Run(suite.Name+"/PublishSubscribe", suite.testPublishSubscribe)
	t.Run(suite.Name+"/SubscribePrefix", suite.testSubscribePrefix)
	t.Run(suite.Name+"/PushOne", suite.testPushOne)
	t.Run(suite.Name+"/MultiplePublishers", suite.testMultiplePublishers)
	t.Run(suite.Name+"/ContextCancellation", suite.testContextCancellation)
	t.Run(suite.Name+"/Close", suite.testClose)
}

func (suite *TestBusInterface) testPublishSubscribe(t *testing.T) {
	bus, err := suite.NewBus(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Use a pub topic so it goes through the PUB/SUB system
	topic := "pepper.pub.test"
	subCh, err := bus.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Give time for subscription to be established
	time.Sleep(50 * time.Millisecond)

	testData := []byte("hello world")
	if err := bus.Publish(topic, testData); err != nil {
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

func (suite *TestBusInterface) testSubscribePrefix(t *testing.T) {
	bus, err := suite.NewBus(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prefix := "pepper.res."
	subCh, err := bus.SubscribePrefix(ctx, prefix)
	if err != nil {
		t.Fatalf("SubscribePrefix: %v", err)
	}

	topics := []string{"pepper.res.abc", "pepper.res.def", "pepper.other.xyz"}
	expectedMessages := 2

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		received := 0
		for msg := range subCh {
			if !hasPrefix(msg.Topic, prefix) {
				t.Errorf("received message for non-matching topic: %s", msg.Topic)
			}
			received++
			if received == expectedMessages {
				return
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)

	for _, topic := range topics {
		bus.Publish(topic, []byte("test"))
		time.Sleep(20 * time.Millisecond)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for prefix messages")
	}
}

func (suite *TestBusInterface) testPushOne(t *testing.T) {
	bus, err := suite.NewBus(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	topic := "pepper.push.gpu"

	// PushOne should not block indefinitely
	err = bus.PushOne(ctx, topic, []byte("test"))
	if err != nil {
		t.Logf("PushOne returned error (expected if no workers): %v", err)
	}
}

func (suite *TestBusInterface) testMultiplePublishers(t *testing.T) {
	bus, err := suite.NewBus(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "pepper.pub.concurrent"
	subCh, err := bus.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	numPublishers := 10
	var wg sync.WaitGroup
	wg.Add(numPublishers)

	for i := 0; i < numPublishers; i++ {
		go func(id int) {
			defer wg.Done()
			data := []byte{byte(id)}
			if err := bus.Publish(topic, data); err != nil {
				t.Errorf("Publish %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	received := 0
	timeout := time.After(3 * time.Second)

	for received < numPublishers {
		select {
		case <-subCh:
			received++
		case <-timeout:
			t.Fatalf("timeout: received %d/%d messages", received, numPublishers)
		}
	}
}

func (suite *TestBusInterface) testContextCancellation(t *testing.T) {
	bus, err := suite.NewBus(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())

	topic := "pepper.pub.cancel"
	subCh, err := bus.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Cancel context
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Channel should be closed
	select {
	case _, ok := <-subCh:
		if ok {
			t.Error("channel should be closed after context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel to close")
	}
}

func (suite *TestBusInterface) testClose(t *testing.T) {
	bus, err := suite.NewBus(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	ctx := context.Background()
	topic := "pepper.pub.close"
	subCh, err := bus.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Close the bus
	if err := bus.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	// Publish should fail after close
	err = bus.Publish(topic, []byte("after close"))
	if err == nil {
		t.Error("Publish should fail after close")
	}

	// Channel should be closed
	select {
	case _, ok := <-subCh:
		if ok {
			t.Error("channel should be closed after bus close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel to close")
	}

	// Second close should be safe
	if err := bus.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
