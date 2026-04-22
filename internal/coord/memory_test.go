package coord

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemory_PushPull_ExactTopic(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()
	defer s.Close()

	queue := "pepper.push.default"
	payload := []byte("memory-test-payload")

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

func TestMemory_Subscribe_ExactAndPrefix(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()
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
			name:         "wildcard * converted to prefix match",
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
						received = append(received, ev.Key)
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

func TestMemory_Subscribe_MultipleChannels(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()
	defer s.Close()

	// Subscribe to two different exact topics
	subCtx1, cancel1 := context.WithCancel(ctx)
	subCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel1()
	defer cancel2()

	ch1, err := s.Subscribe(subCtx1, "pepper.control.w-1")
	if err != nil {
		t.Fatalf("Subscribe #1: %v", err)
	}
	ch2, err := s.Subscribe(subCtx2, "pepper.control.w-2")
	if err != nil {
		t.Fatalf("Subscribe #2: %v", err)
	}

	// Publish to both
	if err := s.Publish(ctx, "pepper.control.w-1", []byte("msg1")); err != nil {
		t.Errorf("Publish w-1: %v", err)
	}
	if err := s.Publish(ctx, "pepper.control.w-2", []byte("msg2")); err != nil {
		t.Errorf("Publish w-2: %v", err)
	}

	// Verify each channel gets its message
	var got1, got2 bool
	select {
	case ev := <-ch1:
		if ev.Key == "pepper.control.w-1" {
			got1 = true
		}
	case <-time.After(500 * time.Millisecond):
	}
	select {
	case ev := <-ch2:
		if ev.Key == "pepper.control.w-2" {
			got2 = true
		}
	case <-time.After(500 * time.Millisecond):
	}

	if !got1 || !got2 {
		t.Errorf("Expected both channels to receive messages; got1=%v, got2=%v", got1, got2)
	}
	// Don't close ch1/ch2 - receive-only; context cancellation handles cleanup
}
