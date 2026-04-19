package bus

//
// Tests skip automatically when NATS is not reachable.
// Override the address via env vars:
//
//	PEPPER_TEST_NATS_HOST=my-nats   PEPPER_TEST_NATS_PORT=4222
//
// Run only these tests:
//
//	go test ./internal/bus/ -run TestNATS -v

import (
	"context"
	"net"
	"os"
	"testing"
	"time"
)

func natsTestAddr() string {
	host := os.Getenv("PEPPER_TEST_NATS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_NATS_PORT")
	if port == "" {
		port = "4222"
	}
	return net.JoinHostPort(host, port)
}

// natsAvailableBus dials the NATS port and skips the test if unreachable.
func natsAvailableBus(t *testing.T) string {
	t.Helper()
	addr := natsTestAddr()
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		t.Skipf("NATS not available at %s — skipping integration test", addr)
		return ""
	}
	conn.Close()
	return addr
}

// TestNATSBus runs the full Bus interface compliance suite against the NATS adapter.
func TestNATSBus(t *testing.T) {
	addr := natsAvailableBus(t)
	suite := &TestBusInterface{
		Name: "NATSBus",
		NewBus: func(cfg Config) (Bus, error) {
			cfg.URL = "nats://" + addr
			return NewNATS(cfg)
		},
	}
	suite.Run(t)
}

// TestNATSBusClose verifies idempotent close and post-close error behaviour.
func TestNATSBusClose(t *testing.T) {
	addr := natsAvailableBus(t)
	b, err := NewNATS(Config{URL: "nats://" + addr})
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("second Close should be idempotent: %v", err)
	}
	if err := b.Publish("pepper.pub.test", []byte("after close")); err == nil {
		t.Error("Publish after close should return error")
	}
}

// TestNATSBusAddr verifies Addr() returns the nats:// scheme.
func TestNATSBusAddr(t *testing.T) {
	addr := natsAvailableBus(t)
	b, err := NewNATS(Config{URL: "nats://" + addr})
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer b.Close()
	got := b.Addr()
	if got != "nats://"+addr {
		t.Errorf("Addr() = %q, want %q", got, "nats://"+addr)
	}
}

// TestNATSPubSubRoundTrip exercises publish → subscribe at the Bus interface level.
func TestNATSPubSubRoundTrip(t *testing.T) {
	addr := natsAvailableBus(t)
	b, err := NewNATS(Config{URL: "nats://" + addr})
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "pepper.pub.nats-roundtrip"
	subCh, err := b.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	want := []byte("hello-nats")
	if err := b.Publish(topic, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-subCh:
		if string(msg.Data) != string(want) {
			t.Errorf("got %q, want %q", msg.Data, want)
		}
		t.Logf("NATS pub/sub round trip ok")
	case <-ctx.Done():
		t.Fatal("timeout waiting for NATS message")
	}
}

// TestNATSPrefixSubscribe verifies that SubscribePrefix delivers messages from
// all matching topics using the NATS wildcard (pepper.res.>).
func TestNATSPrefixSubscribe(t *testing.T) {
	addr := natsAvailableBus(t)
	b, err := NewNATS(Config{URL: "nats://" + addr})
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subCh, err := b.SubscribePrefix(ctx, "pepper.res.")
	if err != nil {
		t.Fatalf("SubscribePrefix: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	topics := []string{"pepper.res.abc", "pepper.res.def"}
	for _, topic := range topics {
		if err := b.Publish(topic, []byte("data")); err != nil {
			t.Fatalf("Publish %s: %v", topic, err)
		}
	}

	received := 0
	for received < len(topics) {
		select {
		case msg := <-subCh:
			if !hasPrefix(msg.Topic, "pepper.res.") {
				t.Errorf("unexpected topic: %s", msg.Topic)
			}
			received++
		case <-ctx.Done():
			t.Fatalf("timeout: received %d/%d prefix messages", received, len(topics))
		}
	}
	t.Logf("NATS prefix subscribe ok: %d messages", received)
}

// TestNATSPushOne verifies PushOne does not error when publishing.
func TestNATSPushOne(t *testing.T) {
	addr := natsAvailableBus(t)
	b, err := NewNATS(Config{URL: "nats://" + addr})
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := b.PushOne(ctx, "pepper.push.gpu", []byte("test")); err != nil {
		t.Errorf("PushOne: %v", err)
	}
}
