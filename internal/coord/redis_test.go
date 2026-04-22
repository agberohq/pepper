package coord

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func skipIfNoRedis(t *testing.T) {
	host := os.Getenv("PEPPER_TEST_REDIS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	conn, err := net.DialTimeout("tcp", host+":"+port, 200*time.Millisecond)
	if err != nil {
		t.Skipf("Redis not available at %s:%s: %v", host, port, err)
	}
	conn.Close()
}

func TestRedis_PushPull_ExactTopic(t *testing.T) {
	skipIfNoRedis(t)
	ctx := context.Background()
	url := os.Getenv("PEPPER_TEST_REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	s, err := NewRedis(url)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer s.Close()

	queue := "pepper.push.default"
	payload := []byte("redis-test-payload")

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

func TestRedis_Subscribe_PSUBSCRIBE_Pattern(t *testing.T) {
	skipIfNoRedis(t)
	ctx := context.Background()
	url := os.Getenv("PEPPER_TEST_REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	s, err := NewRedis(url)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer s.Close()

	tests := []struct {
		name         string
		subscribeTo  string
		publishTo    []string
		wantReceived []string
	}{
		{
			name:         "prefix subscribe receives child topics via PSUBSCRIBE",
			subscribeTo:  "pepper.control",
			publishTo:    []string{"pepper.control.w-1", "pepper.control.w-2"},
			wantReceived: []string{"pepper.control.w-1", "pepper.control.w-2"},
		},
		{
			name:         "wildcard * appended for PSUBSCRIBE",
			subscribeTo:  "pepper.test",
			publishTo:    []string{"pepper.test.a", "pepper.test.b"},
			wantReceived: []string{"pepper.test.a", "pepper.test.b"},
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
			// Don't close ch - receive-only; context cancellation handles cleanup

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

func TestRedis_Subscribe_ExactTopic(t *testing.T) {
	skipIfNoRedis(t)
	ctx := context.Background()
	url := os.Getenv("PEPPER_TEST_REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	s, err := NewRedis(url)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer s.Close()

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	// Redis PSUBSCRIBE with "pepper.control.w-1*" matches exact + children
	ch, err := s.Subscribe(subCtx, "pepper.control.w-1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Publish to exact topic
	if err := s.Publish(ctx, "pepper.control.w-1", []byte("exact-msg")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.Channel != "pepper.control.w-1" {
			t.Errorf("Got channel %q, want %q", ev.Channel, "pepper.control.w-1")
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for exact topic message")
	}
	// Don't close ch - receive-only; context cancellation handles cleanup
}
