package coord

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Single-node tests

func TestMula_Bootstrap(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()
	if MulaAddr(s) == "" {
		t.Fatal("MulaAddr should not be empty")
	}
}

func TestMula_SetGet(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Set(ctx, "mula:test:sg", []byte("hello"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	val, ok, err := s.Get(ctx, "mula:test:sg")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if string(val) != "hello" {
		t.Fatalf("got %q want hello", val)
	}
}

func TestMula_GetMissing(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()
	_, ok, err := s.Get(context.Background(), "mula:test:missing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatal("expected missing")
	}
}

func TestMula_Overwrite(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	s.Set(ctx, "mula:test:ow", []byte("first"), 0)
	s.Set(ctx, "mula:test:ow", []byte("second"), 0)
	val, _, _ := s.Get(ctx, "mula:test:ow")
	if string(val) != "second" {
		t.Fatalf("got %q want second", val)
	}
}

func TestMula_Delete(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	s.Set(ctx, "mula:test:del", []byte("v"), 0)
	if err := s.Delete(ctx, "mula:test:del"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _ := s.Get(ctx, "mula:test:del")
	if ok {
		t.Fatal("key still present after delete")
	}
}

func TestMula_List(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	prefix := fmt.Sprintf("mula:list:%d:", time.Now().UnixNano())
	for i := 0; i < 3; i++ {
		s.Set(ctx, fmt.Sprintf("%s%d", prefix, i), []byte("v"), 0)
	}
	s.Set(ctx, "mula:other:key", []byte("v"), 0)
	keys, err := s.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("List: got %d keys want 3", len(keys))
	}
}

func TestMula_PubSub_ExactMatch(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := s.Subscribe(ctx, "mula:test:exact")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := s.Publish(ctx, "mula:test:exact", []byte("payload")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case ev := <-ch:
		if string(ev.Value) != "payload" {
			t.Fatalf("got %q want payload", ev.Value)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for event")
	}
}

func TestMula_PubSub_PrefixMatch(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Prefix subscription (ends with ".")
	ch, err := s.Subscribe(ctx, "mula:test:prefix.")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Should NOT match: different prefix
	s.Publish(ctx, "mula:test:other.x", []byte("wrong"))
	// Should match
	s.Publish(ctx, "mula:test:prefix.a", []byte("right"))

	select {
	case ev := <-ch:
		if string(ev.Value) != "right" {
			t.Fatalf("got %q want right", ev.Value)
		}
	case <-ctx.Done():
		t.Fatal("timeout")
	}
}

func TestMula_PubSub_MultipleSubscribers(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch1, _ := s.Subscribe(ctx, "mula:multi")
	ch2, _ := s.Subscribe(ctx, "mula:multi")

	s.Publish(ctx, "mula:multi", []byte("broadcast"))

	for _, ch := range []<-chan Event{ch1, ch2} {
		select {
		case ev := <-ch:
			if string(ev.Value) != "broadcast" {
				t.Fatalf("got %q want broadcast", ev.Value)
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for subscriber")
		}
	}
}

func TestMula_PubSub_ContextCancel(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ch, _ := s.Subscribe(ctx, "mula:cancel")
	cancel()

	// Channel should be closed after cancel.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel should be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after context cancel")
	}
}

func TestMula_PushPull(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	queue := fmt.Sprintf("pepper.push.test-%d", time.Now().UnixNano())
	if err := s.Push(ctx, queue, []byte("work")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got, err := s.Pull(ctx, queue)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if string(got) != "work" {
		t.Fatalf("got %q want work", got)
	}
}

func TestMula_PushPull_FIFO(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	queue := fmt.Sprintf("pepper.push.fifo-%d", time.Now().UnixNano())
	items := []string{"first", "second", "third"}
	for _, item := range items {
		s.Push(ctx, queue, []byte(item))
	}
	for i, want := range items {
		got, err := s.Pull(ctx, queue)
		if err != nil {
			t.Fatalf("Pull[%d]: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("Pull[%d]: got %q want %q", i, got, want)
		}
	}
}

func TestMula_PushPull_ExactlyOnce(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := fmt.Sprintf("pepper.push.once-%d", time.Now().UnixNano())

	var mu sync.Mutex
	var received int
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := s.Pull(ctx, queue)
			if err != nil {
				return
			}
			if string(data) == "payload" {
				mu.Lock()
				received++
				mu.Unlock()
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	s.Push(ctx, queue, []byte("payload"))
	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()

	mu.Lock()
	n := received
	mu.Unlock()
	if n != 1 {
		t.Fatalf("ExactlyOnce: %d receivers got item, want 1", n)
	}
}

func TestMula_Pull_ContextCancel(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	queue := fmt.Sprintf("pepper.push.cancel-%d", time.Now().UnixNano())

	done := make(chan error, 1)
	go func() {
		_, err := s.Pull(ctx, queue)
		done <- err
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Pull should return error after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pull did not return after context cancel")
	}
}

// Multi-node cluster tests

func TestMulaCluster_Bootstrap(t *testing.T) {
	s, err := NewMulaCluster("127.0.0.1", 0, 0, nil)
	if err != nil {
		t.Fatalf("NewMulaCluster: %v", err)
	}
	defer s.Close()
	if MulaAddr(s) == "" {
		t.Fatal("MulaAddr should not be empty")
	}
}

func TestMulaCluster_SingleNode_SetGet(t *testing.T) {
	s, err := NewMulaCluster("127.0.0.1", 0, 0, nil)
	if err != nil {
		t.Fatalf("NewMulaCluster: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	s.Set(ctx, "cluster:sg", []byte("hello"), 0)
	val, ok, err := s.Get(ctx, "cluster:sg")
	if err != nil || !ok || string(val) != "hello" {
		t.Fatalf("Get: ok=%v err=%v val=%q", ok, err, val)
	}
}

func TestMulaCluster_TwoNode_Join(t *testing.T) {
	s1, err := NewMulaCluster("127.0.0.1", 0, 0, nil)
	if err != nil {
		t.Fatalf("s1: %v", err)
	}
	defer s1.Close()

	gossipPort := MulaGossipPort(s1)
	s2, err := NewMulaCluster("127.0.0.1", 0, 0, []string{fmt.Sprintf("127.0.0.1:%d", gossipPort)})
	if err != nil {
		t.Fatalf("s2: %v", err)
	}
	defer s2.Close()

	// Allow memberlist to converge.
	time.Sleep(300 * time.Millisecond)

	m1 := s1.(*mulaStore)
	m2 := s2.(*mulaStore)
	if m1.ml.NumMembers() < 2 {
		t.Fatalf("s1 sees %d members, want >=2", m1.ml.NumMembers())
	}
	if m2.ml.NumMembers() < 2 {
		t.Fatalf("s2 sees %d members, want >=2", m2.ml.NumMembers())
	}
}

func TestMulaCluster_TwoNode_SetGet(t *testing.T) {
	s1, err := NewMulaCluster("127.0.0.1", 0, 0, nil)
	if err != nil {
		t.Fatalf("s1: %v", err)
	}
	defer s1.Close()

	s2, err := NewMulaCluster("127.0.0.1", 0, 0, []string{fmt.Sprintf("127.0.0.1:%d", MulaGossipPort(s1))})
	if err != nil {
		t.Fatalf("s2: %v", err)
	}
	defer s2.Close()

	time.Sleep(300 * time.Millisecond)

	ctx := context.Background()
	// Write on s1, read on s2 (owner routing ensures both hit the same node).
	if err := s1.Set(ctx, "cluster:kv:shared", []byte("value"), 0); err != nil {
		t.Fatalf("s1.Set: %v", err)
	}
	val, ok, err := s2.Get(ctx, "cluster:kv:shared")
	if err != nil {
		t.Fatalf("s2.Get: %v", err)
	}
	if !ok {
		t.Fatal("key not found on s2")
	}
	if string(val) != "value" {
		t.Fatalf("got %q want value", val)
	}
}

func TestMulaCluster_TwoNode_PubSub(t *testing.T) {
	s1, err := NewMulaCluster("127.0.0.1", 0, 0, nil)
	if err != nil {
		t.Fatalf("s1: %v", err)
	}
	defer s1.Close()

	s2, err := NewMulaCluster("127.0.0.1", 0, 0, []string{fmt.Sprintf("127.0.0.1:%d", MulaGossipPort(s1))})
	if err != nil {
		t.Fatalf("s2: %v", err)
	}
	defer s2.Close()

	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Subscribe on s2.
	ch, err := s2.Subscribe(ctx, "cluster:pub:test")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Publish on s1 — should fan out to all peers including s2.
	if err := s1.Publish(ctx, "cluster:pub:test", []byte("broadcast")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case ev := <-ch:
		if string(ev.Value) != "broadcast" {
			t.Fatalf("got %q want broadcast", ev.Value)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for cross-node publish")
	}
}

func TestMulaCluster_TwoNode_PushPull(t *testing.T) {
	s1, err := NewMulaCluster("127.0.0.1", 0, 0, nil)
	if err != nil {
		t.Fatalf("s1: %v", err)
	}
	defer s1.Close()

	s2, err := NewMulaCluster("127.0.0.1", 0, 0, []string{fmt.Sprintf("127.0.0.1:%d", MulaGossipPort(s1))})
	if err != nil {
		t.Fatalf("s2: %v", err)
	}
	defer s2.Close()

	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := fmt.Sprintf("pepper.push.cluster-%d", time.Now().UnixNano())

	// Push from s1, pull from s2 (routing sends to queue owner).
	if err := s1.Push(ctx, queue, []byte("work")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got, err := s2.Pull(ctx, queue)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if string(got) != "work" {
		t.Fatalf("got %q want work", got)
	}
}

// Suite integration

func TestMula_Suite(t *testing.T) {
	suite := &Suite{
		Name: "mula",
		NewStore: func(t *testing.T) Store {
			s, err := NewMula("")
			if err != nil {
				t.Fatalf("NewMula: %v", err)
			}
			return s
		},
	}
	suite.Run(t)
}

func TestMulaCluster_Suite(t *testing.T) {
	suite := &Suite{
		Name: "mula-cluster",
		NewStore: func(t *testing.T) Store {
			s, err := NewMulaCluster("127.0.0.1", 0, 0, nil)
			if err != nil {
				t.Fatalf("NewMulaCluster: %v", err)
			}
			return s
		},
	}
	suite.Run(t)
}

// TestMula_LocalSubscribeReceivesPushOneMessages is a regression test for the
// bug where in-process Go workers (goruntime, adapter) subscribed to a push
// topic via coord.Subscribe but never received messages delivered via
// coord.Push because the two paths (pub/sub channel vs work queue) were
// completely separate.  drainPushToLocalSub now bridges the queue to local
// subscribers, matching the behaviour of drainPushToSub for remote TCP subs.
func TestMula_LocalSubscribeReceivesPushOneMessages(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	topic := "pepper.push.gpu"
	ch, err := s.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let drainPushToLocalSub start

	payload := []byte("gpu-task-payload")
	if err := s.Push(ctx, topic, payload); err != nil {
		t.Fatalf("Push: %v", err)
	}

	select {
	case ev := <-ch:
		if string(ev.Value) != string(payload) {
			t.Errorf("got %q, want %q", ev.Value, payload)
		}
		if ev.Channel != topic {
			t.Errorf("channel: got %q, want %q", ev.Channel, topic)
		}
	case <-ctx.Done():
		t.Fatal("timeout: local subscriber did not receive Push'd message — drainPushToLocalSub regression")
	}
}

// TestMula_PushExactlyOnceWithLocalAndRemoteSubs verifies that exactly one
// subscriber receives a Push'd message even when both a local subscriber and
// a remote puller are active.  Only one should win the queue slot.
func TestMula_PushExactlyOnce_LocalSub(t *testing.T) {
	s, err := NewMula("")
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	topic := "pepper.push.exactonce"

	// Two local subscribers competing for the same push queue.
	ch1, _ := s.Subscribe(ctx, topic)
	ch2, _ := s.Subscribe(ctx, topic)
	time.Sleep(20 * time.Millisecond)

	if err := s.Push(ctx, topic, []byte("item")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	received := 0
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-ch1:
			received++
		case <-ch2:
			received++
		case <-timeout:
			if received != 1 {
				t.Errorf("exactly-once: %d receivers got the item, want 1", received)
			}
			return
		}
	}
}
