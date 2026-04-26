package pending

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRegisterAndLen(t *testing.T) {
	m := New()
	if m.Len() != 0 {
		t.Fatalf("new map should be empty")
	}
	_, err := m.Register("corr-1")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}
}

func TestRegisterDuplicateReturnsError(t *testing.T) {
	m := New()
	if _, err := m.Register("corr-dup"); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if _, err := m.Register("corr-dup"); err == nil {
		t.Error("expected error on duplicate Register")
	}
}

func TestHasAfterRegister(t *testing.T) {
	m := New()
	if m.Has("missing") {
		t.Error("Has should return false for unregistered id")
	}
	m.Register("present")
	if !m.Has("present") {
		t.Error("Has should return true after Register")
	}
}

func TestResolveDeliversResponse(t *testing.T) {
	m := New()
	ch, _ := m.Register("corr-1")

	resp := Response{
		Payload:  []byte(`{"msg":"hello"}`),
		WorkerID: "w-1",
		Cap:      "echo",
		CapVer:   "1.0.0",
		Hop:      0,
	}
	go m.Resolve("corr-1", resp)

	select {
	case got := <-ch:
		if string(got.Payload) != `{"msg":"hello"}` {
			t.Errorf("Payload = %s, want hello", got.Payload)
		}
		if got.WorkerID != "w-1" {
			t.Errorf("WorkerID = %q, want w-1", got.WorkerID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Resolve")
	}
}

func TestResolveRemovesEntry(t *testing.T) {
	m := New()
	ch, _ := m.Register("corr-1")
	go m.Resolve("corr-1", Response{})
	<-ch
	if m.Len() != 0 {
		t.Errorf("Len = %d after Resolve, want 0", m.Len())
	}
	if m.Has("corr-1") {
		t.Error("Has should return false after Resolve")
	}
}

func TestResolveIdempotent(t *testing.T) {
	m := New()
	ch, _ := m.Register("corr-1")

	m.Resolve("corr-1", Response{Payload: []byte("first")})
	m.Resolve("corr-1", Response{Payload: []byte("second")})

	got := <-ch
	if string(got.Payload) != "first" {
		t.Errorf("expected first payload, got %s", got.Payload)
	}
	select {
	case extra := <-ch:
		t.Errorf("unexpected second delivery: %s", extra.Payload)
	default:
	}
}

func TestResolveUnknownIDIsNoop(t *testing.T) {
	m := New()
	m.Resolve("does-not-exist", Response{})
}

func TestFailDeliversError(t *testing.T) {
	m := New()
	ch, _ := m.Register("corr-fail")
	sentinel := errors.New("deadline exceeded")

	go m.Fail("corr-fail", sentinel)

	select {
	case got := <-ch:
		if got.Err == nil {
			t.Fatal("expected error, got nil")
		}
		if got.Err.Error() != sentinel.Error() {
			t.Errorf("Err = %v, want %v", got.Err, sentinel)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Fail")
	}
}

func TestStreamChunkAndEnd(t *testing.T) {
	m := New()
	ch, err := m.RegisterStream("stream-1", 8)
	if err != nil {
		t.Fatalf("RegisterStream: %v", err)
	}

	chunks := []string{"token1", "token2", "token3"}
	go func() {
		for _, c := range chunks {
			m.Chunk("stream-1", Response{Payload: []byte(c)})
		}
		m.EndStream("stream-1")
	}()

	var received []string
	for resp := range ch {
		received = append(received, string(resp.Payload))
	}

	if len(received) != len(chunks) {
		t.Fatalf("received %d chunks, want %d", len(received), len(chunks))
	}
	for i, got := range received {
		if got != chunks[i] {
			t.Errorf("chunk[%d] = %q, want %q", i, got, chunks[i])
		}
	}
}

func TestEndStreamClosesChannel(t *testing.T) {
	m := New()
	ch, _ := m.RegisterStream("stream-2", 4)
	m.EndStream("stream-2")

	select {
	case _, open := <-ch:
		if open {
			t.Error("channel should be closed after EndStream")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

func TestChunkOnUnknownIDIsNoop(t *testing.T) {
	m := New()
	m.Chunk("no-such-stream", Response{Payload: []byte("x")})
}

func TestEndStreamIdempotent(t *testing.T) {
	m := New()
	ch, _ := m.RegisterStream("stream-3", 4)
	m.EndStream("stream-3")
	m.EndStream("stream-3")
	for range ch {
	}
}

func TestStreamRegisteredTwiceReturnsError(t *testing.T) {
	m := New()
	if _, err := m.RegisterStream("s-dup", 4); err != nil {
		t.Fatalf("first RegisterStream: %v", err)
	}
	if _, err := m.RegisterStream("s-dup", 4); err == nil {
		t.Error("expected error on duplicate RegisterStream")
	}
}

func TestCancelByIDsDeliversErrorToAll(t *testing.T) {
	m := New()
	ch1, _ := m.Register("c1")
	ch2, _ := m.Register("c2")
	ch3, _ := m.Register("c3")

	cancelErr := errors.New("worker died")
	go m.CancelByIDs([]string{"c1", "c2", "c3"}, cancelErr)

	for i, ch := range []<-chan Response{ch1, ch2, ch3} {
		select {
		case got := <-ch:
			if got.Err == nil || got.Err.Error() != cancelErr.Error() {
				t.Errorf("ch%d: Err = %v, want %v", i+1, got.Err, cancelErr)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for cancel on ch%d", i+1)
		}
	}
}

func TestCancelByIDsEmptyListIsNoop(t *testing.T) {
	m := New()
	m.Register("live")
	m.CancelByIDs([]string{}, errors.New("x"))
	if !m.Has("live") {
		t.Error("live entry should not be cancelled by empty ID list")
	}
}

// TestConcurrentRegisterResolve is the regression test for the cluster
// throughput hang. Under high concurrency mappo.Sharded could lose entries
// between SetIfAbsent and Get, causing Resolve to miss and Do() to hang.
func TestConcurrentRegisterResolve(t *testing.T) {
	m := New()
	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			corrID := fmt.Sprintf("corr-%d", id)
			ch, err := m.Register(corrID)
			if err != nil {
				t.Errorf("Register %s: %v", corrID, err)
				return
			}

			// Simulate response arriving from another goroutine.
			go m.Resolve(corrID, Response{Payload: []byte("ok")})

			select {
			case resp := <-ch:
				if string(resp.Payload) != "ok" {
					t.Errorf("corr-%d: got %q, want ok", id, resp.Payload)
				}
			case <-time.After(2 * time.Second):
				t.Errorf("corr-%d: timed out waiting for Resolve — pending entry lost", id)
			}
		}(i)
	}

	wg.Wait()
	if m.Len() != 0 {
		t.Errorf("Len = %d after all resolved, want 0", m.Len())
	}
}

func TestConcurrentChunkAndEndStream(t *testing.T) {
	m := New()
	ch, _ := m.RegisterStream("concurrent-stream", 256)

	const nChunks = 50
	var wg sync.WaitGroup
	wg.Add(nChunks)
	for i := 0; i < nChunks; i++ {
		i := i
		go func() {
			defer wg.Done()
			m.Chunk("concurrent-stream", Response{
				Payload: []byte{byte(i)},
			})
		}()
	}

	go func() {
		wg.Wait()
		m.EndStream("concurrent-stream")
	}()

	count := 0
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, open := <-ch:
			if !open {
				if count > 0 {
					return
				}
				t.Fatal("stream closed with zero chunks")
			}
			count++
		case <-deadline:
			t.Fatalf("timeout: received %d chunks before channel close", count)
		}
	}
}
