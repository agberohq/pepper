package bus

//
// Tests skip automatically when Redis is not reachable.
// Override the address via env vars:
//
//	PEPPER_TEST_REDIS_HOST=my-redis   PEPPER_TEST_REDIS_PORT=6379
//
// Run only these tests:
//
//	go test ./internal/bus/ -run TestRedis -v

import (
	"net"
	"os"
	"testing"
	"time"
)

func redisTestAddr() string {
	host := os.Getenv("PEPPER_TEST_REDIS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	return net.JoinHostPort(host, port)
}

// redisAvailableBus dials the Redis port and skips if unreachable.
func redisAvailableBus(t *testing.T) string {
	t.Helper()
	addr := redisTestAddr()
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		t.Skipf("Redis not available at %s — skipping integration test", addr)
		return ""
	}
	conn.Close()
	return addr
}

// TestRedisBus runs the full Bus interface compliance suite against the Redis adapter.
func TestRedisBus(t *testing.T) {
	addr := redisAvailableBus(t)

	suite := &TestBusInterface{
		Name: "RedisBus",
		NewBus: func(cfg Config) (Bus, error) {
			cfg.URL = "redis://" + addr
			return NewRedis(cfg)
		},
	}
	suite.Run(t)
}

// TestRedisBusClose verifies idempotent close and post-close error behaviour.
func TestRedisBusClose(t *testing.T) {
	addr := redisAvailableBus(t)

	b, err := NewRedis(Config{URL: "redis://" + addr})
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
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

// TestRedisBusAddr verifies Addr() returns the redis:// scheme.
func TestRedisBusAddr(t *testing.T) {
	addr := redisAvailableBus(t)

	b, err := NewRedis(Config{URL: "redis://" + addr})
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer b.Close()

	got := b.Addr()
	if got != "redis://"+addr {
		t.Errorf("Addr() = %q, want %q", got, "redis://"+addr)
	}
}

// TestRedisXAddXRead is a lower-level round-trip test of the stream helpers.
func TestRedisXAddXRead(t *testing.T) {
	addr := redisAvailableBus(t)

	b, err := NewRedis(Config{URL: "redis://" + addr})
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer b.Close()

	stream := "pepper.test.xread"
	payload := []byte("hello-redis-stream")

	if err := b.xadd(stream, payload); err != nil {
		t.Fatalf("xadd: %v", err)
	}

	entries, err := b.xread(stream, "0-0", 10, time.Second)
	if err != nil {
		t.Fatalf("xread: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("xread returned no entries")
	}

	found := false
	for _, e := range entries {
		if string(e.data) == string(payload) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("payload %q not found in entries: %+v", payload, entries)
	}
	t.Logf("xread round trip ok: %d entries", len(entries))
}
