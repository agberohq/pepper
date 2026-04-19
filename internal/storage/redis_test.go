package storage

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"
)

// RESP parser unit tests (no Redis needed)

func TestReadRESPSimpleString(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("+OK\r\n"))
	v, err := readRESP(r)
	if err != nil {
		t.Fatalf("readRESP: %v", err)
	}
	if v != "OK" {
		t.Errorf("got %q, want OK", v)
	}
}

func TestReadRESPError(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("-ERR bad command\r\n"))
	_, err := readRESP(r)
	if err == nil {
		t.Fatal("expected error from Redis error reply")
	}
	if !strings.Contains(err.Error(), "bad command") {
		t.Errorf("error = %v, want to contain 'bad command'", err)
	}
}

func TestReadRESPInteger(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(":42\r\n"))
	v, err := readRESP(r)
	if err != nil {
		t.Fatalf("readRESP: %v", err)
	}
	if v.(int64) != 42 {
		t.Errorf("got %v, want 42", v)
	}
}

func TestReadRESPBulkString(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("$5\r\nhello\r\n"))
	v, err := readRESP(r)
	if err != nil {
		t.Fatalf("readRESP: %v", err)
	}
	if v.(string) != "hello" {
		t.Errorf("got %q, want hello", v)
	}
}

func TestReadRESPNilBulk(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("$-1\r\n"))
	v, err := readRESP(r)
	if err != nil {
		t.Fatalf("readRESP: %v", err)
	}
	if v != nil {
		t.Errorf("got %v, want nil", v)
	}
}

func TestReadRESPArray(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	v, err := readRESP(r)
	if err != nil {
		t.Fatalf("readRESP: %v", err)
	}
	arr, ok := v.([]any)
	if !ok {
		t.Fatalf("got %T, want []any", v)
	}
	if len(arr) != 2 || arr[0] != "foo" || arr[1] != "bar" {
		t.Errorf("got %v, want [foo bar]", arr)
	}
}

func TestReadRESPNilArray(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("*-1\r\n"))
	v, err := readRESP(r)
	if err != nil {
		t.Fatalf("readRESP: %v", err)
	}
	if v != nil {
		t.Errorf("got %v, want nil", v)
	}
}

func TestReadRESPUnknownType(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("!unknown\r\n"))
	_, err := readRESP(r)
	if err == nil {
		t.Fatal("expected error for unknown RESP type")
	}
}

// urlSlug helper (via hkey)

func TestHKey(t *testing.T) {
	key := hkey("user-123")
	if key != "pepper:session:user-123" {
		t.Errorf("hkey = %q", key)
	}
}

// NewRedis integration test — skipped when Redis unavailable

func redisAvailable(t *testing.T, addr string) bool {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		t.Skipf("Redis not available at %s — skipping integration test", addr)
		return false
	}
	conn.Close()
	return true
}

const testRedisAddr = "localhost:6379"

func TestRedisStoreSetGet(t *testing.T) {
	if !redisAvailable(t, testRedisAddr) {
		return
	}
	s := NewRedis(testRedisAddr, time.Minute)
	sessID := "redis-test-setget"
	defer s.Clear(sessID)

	if err := s.Set(sessID, "name", "alice"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, ok := s.Get(sessID, "name")
	if !ok {
		t.Fatal("Get returned not-found")
	}
	if v != "alice" {
		t.Errorf("Get = %v, want alice", v)
	}
}

func TestRedisStoreMissingKey(t *testing.T) {
	if !redisAvailable(t, testRedisAddr) {
		return
	}
	s := NewRedis(testRedisAddr, time.Minute)
	sessID := "redis-test-missing"
	defer s.Clear(sessID)

	s.Set(sessID, "key", "val")
	_, ok := s.Get(sessID, "missing")
	if ok {
		t.Error("Get for missing key should return false")
	}
}

func TestRedisStoreGetAll(t *testing.T) {
	if !redisAvailable(t, testRedisAddr) {
		return
	}
	s := NewRedis(testRedisAddr, time.Minute)
	sessID := "redis-test-getall"
	defer s.Clear(sessID)

	s.Set(sessID, "a", "alpha")
	s.Set(sessID, "b", "beta")

	all, ok := s.GetAll(sessID)
	if !ok {
		t.Fatal("GetAll returned not-found")
	}
	if all["a"] != "alpha" || all["b"] != "beta" {
		t.Errorf("GetAll = %v", all)
	}
}

func TestRedisStoreMerge(t *testing.T) {
	if !redisAvailable(t, testRedisAddr) {
		return
	}
	s := NewRedis(testRedisAddr, time.Minute)
	sessID := "redis-test-merge"
	defer s.Clear(sessID)

	s.Set(sessID, "existing", "keep")
	if err := s.Merge(sessID, map[string]any{"new": "val", "existing": "replaced"}); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	v, _ := s.Get(sessID, "new")
	if v != "val" {
		t.Errorf("new = %v, want val", v)
	}
	v, _ = s.Get(sessID, "existing")
	if v != "replaced" {
		t.Errorf("existing = %v, want replaced", v)
	}
}

func TestRedisStoreExists(t *testing.T) {
	if !redisAvailable(t, testRedisAddr) {
		return
	}
	s := NewRedis(testRedisAddr, time.Minute)
	sessID := "redis-test-exists"
	defer s.Clear(sessID)

	if s.Exists(sessID) {
		t.Error("Exists before Set should be false")
	}
	s.Set(sessID, "k", "v")
	if !s.Exists(sessID) {
		t.Error("Exists after Set should be true")
	}
}

func TestRedisStoreClear(t *testing.T) {
	if !redisAvailable(t, testRedisAddr) {
		return
	}
	s := NewRedis(testRedisAddr, time.Minute)
	sessID := "redis-test-clear"

	s.Set(sessID, "k", "v")
	if err := s.Clear(sessID); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if s.Exists(sessID) {
		t.Error("session should not exist after Clear")
	}
}

func TestRedisStoreTouch(t *testing.T) {
	if !redisAvailable(t, testRedisAddr) {
		return
	}
	s := NewRedis(testRedisAddr, time.Minute)
	sessID := "redis-test-touch"
	defer s.Clear(sessID)

	s.Set(sessID, "k", "v")
	if err := s.Touch(sessID); err != nil {
		t.Errorf("Touch: %v", err)
	}
}

func TestRedisStoreGetAllMissing(t *testing.T) {
	if !redisAvailable(t, testRedisAddr) {
		return
	}
	s := NewRedis(testRedisAddr, time.Minute)
	_, ok := s.GetAll("redis-test-no-such-session")
	if ok {
		t.Error("GetAll on missing session should return false")
	}
}

func TestRedisStoreRedisPrefix(t *testing.T) {
	if !redisAvailable(t, testRedisAddr) {
		return
	}
	// NewRedis should accept the redis:// prefix
	s := NewRedis("redis://"+testRedisAddr, time.Minute)
	sessID := "redis-test-prefix"
	defer s.Clear(sessID)

	if err := s.Set(sessID, "k", "v"); err != nil {
		t.Fatalf("Set with redis:// prefix: %v", err)
	}
	if !s.Exists(sessID) {
		t.Error("Exists after Set with redis:// prefix should be true")
	}
}
