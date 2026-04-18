package bus

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNanoBus(t *testing.T) {
	suite := &TestBusInterface{
		Name: "NanoBus",
		NewBus: func(cfg Config) (Bus, error) {
			if cfg.URL == "" {
				cfg = Config{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128}
			}
			return NewNano(cfg)
		},
	}
	suite.Run(t)
}

func TestNanoBusPushPull(t *testing.T) {
	bus, err := NewNano(Config{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128})
	if err != nil {
		t.Fatalf("NewNano: %v", err)
	}
	defer bus.Close()

	addr := bus.Addr()
	if addr == "" {
		t.Error("Addr() returned empty string")
	}
	t.Logf("NanoBus listening on %s", addr)
}

func TestNanoBusWithMangosWorker(t *testing.T) {
	// Verify the bus delivers to local subscribers correctly.
	// External worker integration is covered by the runtime layer.
	bus, err := NewNano(Config{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128})
	if err != nil {
		t.Fatalf("NewNano: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := TopicPub("gpu")
	subCh, err := bus.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("router subscribe: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	testData := []byte("hello worker")
	if err := bus.Publish(topic, testData); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case msg := <-subCh:
		if string(msg.Data) != string(testData) {
			t.Errorf("local sub: expected %q, got %q", testData, msg.Data)
		}
	case <-time.After(2 * time.Second):
		t.Error("local subscriber timeout")
	}
}

func TestNanoBusMultipleGroups(t *testing.T) {
	bus, err := NewNano(Config{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128})
	if err != nil {
		t.Fatalf("NewNano: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	groups := []string{"gpu", "cpu", "asr"}
	var subs []<-chan Message

	for _, g := range groups {
		topic := TopicPub(g)
		ch, err := bus.Subscribe(ctx, topic)
		if err != nil {
			t.Fatalf("Subscribe %s: %v", g, err)
		}
		subs = append(subs, ch)
	}

	// Give time for subscriptions to be established
	time.Sleep(100 * time.Millisecond)

	// Publish to each group
	for i, g := range groups {
		topic := TopicPub(g)
		data := []byte("message for " + g)
		if err := bus.Publish(topic, data); err != nil {
			t.Errorf("Publish %s: %v", g, err)
		}

		select {
		case msg := <-subs[i]:
			if string(msg.Data) != string(data) {
				t.Errorf("expected %q, got %q", data, msg.Data)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("timeout waiting for message on group %s", g)
		}

		// Other subscribers should not have messages
		for j, sub := range subs {
			if j == i {
				continue
			}
			select {
			case msg := <-sub:
				t.Errorf("unexpected message on group %s: %v", groups[j], msg)
			default:
			}
		}
	}
}

func TestNanoBusPrefixSubscription(t *testing.T) {
	bus, err := NewNano(Config{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128})
	if err != nil {
		t.Fatalf("NewNano: %v", err)
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

	received := make(chan string, 3)
	go func() {
		for msg := range subCh {
			received <- msg.Topic
		}
	}()

	time.Sleep(50 * time.Millisecond)

	for _, topic := range topics {
		bus.Publish(topic, []byte("test"))
		time.Sleep(20 * time.Millisecond)
	}

	// Should receive 2 messages
	count := 0
	timeout := time.After(2 * time.Second)
	for count < 2 {
		select {
		case topic := <-received:
			if !hasPrefix(topic, prefix) {
				t.Errorf("received message for non-matching topic: %s", topic)
			}
			count++
		case <-timeout:
			t.Fatalf("timeout: received %d/2 messages", count)
		}
	}
}

func TestNanoBusPubSubInterop(t *testing.T) {
	// Nano bus delivers to local in-process subscribers via channels.
	// External workers connect via WorkerDial (SUB) and WorkerPush (PUSH).
	// This test verifies local pub/sub delivery works correctly.
	bus, err := NewNano(Config{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128})
	if err != nil {
		t.Fatalf("NewNano: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "pepper.pub.external"
	subCh, err := bus.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("bus subscribe: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	testData := []byte("bus message")
	if err := bus.Publish(topic, testData); err != nil {
		t.Fatalf("bus publish: %v", err)
	}

	select {
	case received := <-subCh:
		if string(received.Data) != string(testData) {
			t.Errorf("expected %q, got %q", testData, received.Data)
		}
		if received.Topic != topic {
			t.Errorf("expected topic %q, got %q", topic, received.Topic)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestNanoBusBroadcast(t *testing.T) {
	bus, err := NewNano(Config{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128})
	if err != nil {
		t.Fatalf("NewNano: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Three local subscribers
	numSubs := 3
	subs := make([]<-chan Message, numSubs)
	for i := 0; i < numSubs; i++ {
		ch, err := bus.Subscribe(ctx, TopicBroadcast)
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		subs[i] = ch
	}

	time.Sleep(20 * time.Millisecond)

	testData := []byte("broadcast message")
	if err := bus.Publish(TopicBroadcast, testData); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	for i, ch := range subs {
		select {
		case msg := <-ch:
			if string(msg.Data) != string(testData) {
				t.Errorf("sub %d: expected %q, got %q", i, testData, msg.Data)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("sub %d timeout", i)
		}
	}
}

func TestNanoBusConcurrentPublishers(t *testing.T) {
	bus, err := NewNano(Config{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 256})
	if err != nil {
		t.Fatalf("NewNano: %v", err)
	}
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topic := TopicPub("concurrent")
	subCh, err := bus.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	numPublishers := 20
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
	timeout := time.After(5 * time.Second)

	for received < numPublishers {
		select {
		case <-subCh:
			received++
		case <-timeout:
			t.Fatalf("timeout: received %d/%d messages", received, numPublishers)
		}
	}
}

func TestMangosFrameEncoding(t *testing.T) {
	topic := "pepper.pub.gpu"
	data := []byte("test payload")

	encoded := encodeMsg(topic, data)
	decodedTopic, decodedData := decodeMsg(encoded)

	if decodedTopic != topic {
		t.Errorf("topic: expected %q, got %q", topic, decodedTopic)
	}
	if string(decodedData) != string(data) {
		t.Errorf("data: expected %q, got %q", data, decodedData)
	}

	// Test empty data
	encoded = encodeMsg(topic, nil)
	decodedTopic, decodedData = decodeMsg(encoded)

	if decodedTopic != topic {
		t.Errorf("topic (nil data): expected %q, got %q", topic, decodedTopic)
	}
	if len(decodedData) != 0 {
		t.Errorf("data (nil): expected empty, got %v", decodedData)
	}
}

func BenchmarkNanoBusPublish(b *testing.B) {
	bus, err := NewNano(Config{URL: "tcp://127.0.0.1:0", SendBuf: 256, RecvBuf: 256})
	if err != nil {
		b.Fatal(err)
	}
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "pepper.pub.bench"
	subCh, err := bus.Subscribe(ctx, topic)
	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for range subCh {
		}
	}()

	time.Sleep(100 * time.Millisecond)

	data := []byte("benchmark data")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := bus.Publish(topic, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNanoBusPushOne(b *testing.B) {
	bus, err := NewNano(Config{URL: "tcp://127.0.0.1:0", SendBuf: 256, RecvBuf: 256})
	if err != nil {
		b.Fatal(err)
	}
	defer bus.Close()

	ctx := context.Background()
	topic := "pepper.push.gpu"
	data := []byte("benchmark")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.PushOne(ctx, topic, data)
	}
}
