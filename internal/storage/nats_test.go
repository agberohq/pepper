package storage

//
// These tests verify that the NATS port is open and speaks the NATS INFO
// protocol, matching the pattern already used for Redis in redis_test.go.
//
// They are skipped automatically when NATS is not running locally.
// Override the address via env vars:
//
//	PEPPER_TEST_NATS_HOST=my-nats-host
//	PEPPER_TEST_NATS_PORT=4222
//
// Run only these tests:
//
//	go test ./internal/storage/ -run TestNATS -v

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func natsAddr() string {
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

// natsAvailable dials the NATS port and skips the test if unreachable.
// Returns the address so callers can use it without calling natsAddr() twice.
func natsAvailable(t *testing.T) string {
	t.Helper()
	addr := natsAddr()
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		t.Skipf("NATS not available at %s — skipping integration test", addr)
		return ""
	}
	conn.Close()
	return addr
}

// TestNATSPortOpen verifies the NATS TCP port is reachable.
func TestNATSPortOpen(t *testing.T) {
	addr := natsAvailable(t)
	t.Logf("NATS reachable at %s", addr)
}

// TestNATSInfoHandshake connects and verifies the NATS INFO banner.
// Every NATS server sends a JSON INFO object on connect before any client
// message — this is the protocol-level equivalent of a Redis PING.
func TestNATSInfoHandshake(t *testing.T) {
	addr := natsAvailable(t)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read INFO banner: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")

	if !strings.HasPrefix(line, "INFO ") {
		t.Fatalf("expected NATS INFO banner, got: %q", line)
	}

	jsonPart := strings.TrimPrefix(line, "INFO ")
	var info map[string]any
	if err := json.Unmarshal([]byte(jsonPart), &info); err != nil {
		t.Fatalf("parse INFO JSON: %v", err)
	}
	if _, ok := info["version"]; !ok {
		t.Errorf("INFO missing 'version': %v", info)
	}
	if _, ok := info["max_payload"]; !ok {
		t.Errorf("INFO missing 'max_payload': %v", info)
	}
	t.Logf("NATS version=%v max_payload=%v", info["version"], info["max_payload"])
}

// TestNATSPingPong sends a NATS PING and expects PONG back.
// This is the same health-check mechanism NATS client libraries use internally.
func TestNATSPingPong(t *testing.T) {
	addr := natsAvailable(t)

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Drain the INFO banner
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	br := bufio.NewReader(conn)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("read INFO: %v", err)
	}

	// Send PING
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprint(conn, "PING\r\n"); err != nil {
		t.Fatalf("write PING: %v", err)
	}

	// Expect PONG
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read PONG: %v", err)
	}
	resp = strings.TrimRight(resp, "\r\n")
	if resp != "PONG" {
		t.Errorf("expected PONG, got %q", resp)
	}
	t.Logf("NATS ping-pong ok")
}

// TestNATSPubSubRoundTrip exercises the core NATS pub/sub wire protocol
// at the raw text-protocol level — no client library dependency.
// This proves Pepper could use NATS as a bus backend without relying on
// nats-server mock behaviour in future transport implementations.
func TestNATSPubSubRoundTrip(t *testing.T) {
	addr := natsAvailable(t)

	// subscriber connection
	subConn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("sub dial: %v", err)
	}
	defer subConn.Close()
	subBR := bufio.NewReader(subConn)

	// Drain INFO
	subConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := subBR.ReadString('\n'); err != nil {
		t.Fatalf("sub read INFO: %v", err)
	}

	// Subscribe to pepper.test.roundtrip
	subject := "pepper.test.roundtrip"
	sid := "1"
	fmt.Fprintf(subConn, "SUB %s %s\r\n", subject, sid)

	// publisher connection
	pubConn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("pub dial: %v", err)
	}
	defer pubConn.Close()
	pubBR := bufio.NewReader(pubConn)

	pubConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := pubBR.ReadString('\n'); err != nil {
		t.Fatalf("pub read INFO: %v", err)
	}

	// Publish a message
	payload := []byte(`{"pepper":"roundtrip-ok"}`)
	fmt.Fprintf(pubConn, "PUB %s %d\r\n", subject, len(payload))
	pubConn.Write(payload)
	pubConn.Write([]byte("\r\n"))

	// receive on subscriber
	subConn.SetReadDeadline(time.Now().Add(3 * time.Second))

	// MSG pepper.test.roundtrip 1 25\r\n{"pepper":"roundtrip-ok"}\r\n
	msgLine, err := subBR.ReadString('\n')
	if err != nil {
		t.Fatalf("read MSG header: %v", err)
	}
	msgLine = strings.TrimRight(msgLine, "\r\n")
	if !strings.HasPrefix(msgLine, "MSG ") {
		t.Fatalf("expected MSG, got: %q", msgLine)
	}

	// Parse byte count from MSG header
	parts := strings.Fields(msgLine)
	if len(parts) < 4 {
		t.Fatalf("malformed MSG header: %q", msgLine)
	}
	var byteCount int
	if _, err := fmt.Sscanf(parts[len(parts)-1], "%d", &byteCount); err != nil {
		t.Fatalf("parse MSG byte count: %v", err)
	}

	buf := make([]byte, byteCount)
	if _, err := subBR.Read(buf); err != nil {
		t.Fatalf("read MSG body: %v", err)
	}

	if string(buf) != string(payload) {
		t.Errorf("roundtrip payload mismatch: got %q, want %q", buf, payload)
	}
	t.Logf("NATS pub/sub round trip ok: %s", buf)
}
