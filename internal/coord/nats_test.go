package coord

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func skipIfNoNATS(t *testing.T) {
	host := os.Getenv("PEPPER_TEST_NATS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_NATS_PORT")
	if port == "" {
		port = "4222"
	}
	conn, err := net.DialTimeout("tcp", host+":"+port, 200*time.Millisecond)
	if err != nil {
		t.Skipf("NATS not available at %s:%s: %v", host, port, err)
	}
	conn.Close()
}

func TestNATS_PushPull_ExactTopic(t *testing.T) {
	skipIfNoNATS(t)
	ctx := context.Background()
	url := os.Getenv("PEPPER_TEST_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	s, err := NewNATS(url)
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer s.Close()

	queue := "pepper.push.default"
	payload := []byte("test-payload")

	if err := s.Push(ctx, queue, payload); err != nil {
		t.Fatalf("Push: %v", err)
	}

	got, err := s.Pull(ctx, queue)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("Pull got %q, want %q", got, payload)
	}
}

func TestNATS_Subscribe_ExactAndPrefix(t *testing.T) {
	skipIfNoNATS(t)
	ctx := context.Background()
	url := os.Getenv("PEPPER_TEST_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	s, err := NewNATS(url)
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer s.Close()

	tests := []struct {
		name         string
		subscribeTo  string
		publishTo    []string
		wantReceived []string
	}{
		{
			name:         "exact topic receives exact publish",
			subscribeTo:  "pepper.control.w-1",
			publishTo:    []string{"pepper.control.w-1"},
			wantReceived: []string{"pepper.control.w-1"},
		},
		{
			name:         "prefix subscribe receives child topics",
			subscribeTo:  "pepper.control",
			publishTo:    []string{"pepper.control.w-1", "pepper.control.w-2"},
			wantReceived: []string{"pepper.control.w-1", "pepper.control.w-2"},
		},
		{
			name:         "wildcard * converted to >",
			subscribeTo:  "pepper.*",
			publishTo:    []string{"pepper.foo", "pepper.bar"},
			wantReceived: []string{"pepper.foo", "pepper.bar"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subCtx, subCancel := context.WithCancel(ctx)
			defer subCancel()

			ch, err := s.Subscribe(subCtx, tt.subscribeTo)
			if err != nil {
				t.Fatalf("Subscribe(%q): %v", tt.subscribeTo, err)
			}

			// Small delay to ensure subscription is active
			time.Sleep(50 * time.Millisecond)

			var wg sync.WaitGroup
			received := make([]string, 0, len(tt.publishTo))
			var mu sync.Mutex

			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case ev, ok := <-ch:
						if !ok {
							return
						}
						mu.Lock()
						received = append(received, ev.Channel)
						mu.Unlock()
						if len(received) >= len(tt.wantReceived) {
							return
						}
					case <-time.After(2 * time.Second):
						return
					}
				}
			}()

			for _, topic := range tt.publishTo {
				if err := s.Publish(ctx, topic, []byte("msg")); err != nil {
					t.Errorf("Publish(%q): %v", topic, err)
				}
			}

			wg.Wait()
			// Don't close ch - it's receive-only; context cancellation handles cleanup

			// Verify received topics match expected (order-independent)
			wantSet := make(map[string]bool)
			for _, w := range tt.wantReceived {
				wantSet[w] = true
			}
			gotSet := make(map[string]bool)
			for _, r := range received {
				gotSet[r] = true
			}
			for w := range wantSet {
				if !gotSet[w] {
					t.Errorf("Subscribe(%q) missed expected topic %q; got %v", tt.subscribeTo, w, received)
				}
			}
		})
	}
}

func TestNATS_Subscribe_UniqueSIDs(t *testing.T) {
	skipIfNoNATS(t)
	ctx := context.Background()
	url := os.Getenv("PEPPER_TEST_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	s, err := NewNATS(url)
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer s.Close()

	// Subscribe to exact topic twice – should not overwrite first subscription
	subCtx1, cancel1 := context.WithCancel(ctx)
	subCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel1()
	defer cancel2()

	ch1, err := s.Subscribe(subCtx1, "pepper.control.w-1")
	if err != nil {
		t.Fatalf("Subscribe #1: %v", err)
	}
	ch2, err := s.Subscribe(subCtx2, "pepper.control.w-1")
	if err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}

	// Publish once – both channels should receive (NATS fan-out on same subject)
	if err := s.Publish(ctx, "pepper.control.w-1", []byte("msg")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Both channels should get the message
	var got1, got2 bool
	select {
	case <-ch1:
		got1 = true
	case <-time.After(500 * time.Millisecond):
	}
	select {
	case <-ch2:
		got2 = true
	case <-time.After(500 * time.Millisecond):
	}

	if !got1 || !got2 {
		t.Errorf("Expected both subscriptions to receive message; got1=%v, got2=%v", got1, got2)
	}
	// Don't close ch1/ch2 - receive-only; context cancellation handles cleanup
}

func TestNATS_WildcardConversion(t *testing.T) {
	skipIfNoNATS(t)
	ctx := context.Background()
	url := os.Getenv("PEPPER_TEST_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}
	s, err := NewNATS(url)
	if err != nil {
		t.Fatalf("NewNATS: %v", err)
	}
	defer s.Close()

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	// Subscribe with Redis-style * wildcard – should be converted to NATS >
	ch, err := s.Subscribe(subCtx, "pepper.test.*")
	if err != nil {
		t.Fatalf("Subscribe(pepper.test.*): %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Publish to child topics
	for _, topic := range []string{"pepper.test.a", "pepper.test.b", "pepper.test.c"} {
		if err := s.Publish(ctx, topic, []byte("x")); err != nil {
			t.Errorf("Publish(%q): %v", topic, err)
		}
	}

	// Should receive all three
	count := 0
	for {
		select {
		case <-ch:
			count++
			if count >= 3 {
				return
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Expected 3 messages, got %d", count)
		}
	}
}
