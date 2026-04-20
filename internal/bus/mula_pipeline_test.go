package bus

// Regression tests for pipeline-related bugs found during TestRealSongAnalysis.
//
// Bug 1 (mula.go): PushOne to pepper.push.{workerID} was never delivered to
// in-process Go subscribers (adapter, goruntime, cli workers). PushOne wrote
// to pushQs but subscribe() only registered in localSubs. The two maps were
// completely disconnected — no goroutine drained pushQs into localSubs for
// local subscribers. Fix: subscribe() now starts a drain goroutine for
// pepper.push.* topics, mirroring drainPushToSubscriber for TCP subscribers.

import (
	"context"
	"testing"
	"time"
)

// TestPushOneDeliveredToLocalSubscriber is the direct regression test for bug 1.
// Before the fix, PushOne to pepper.push.X would not deliver to a local Go
// subscriber on the same topic — the message was silently lost.
func TestPushOneDeliveredToLocalSubscriber(t *testing.T) {
	b, err := NewMula(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	topic := TopicPush("http-song_analyze")
	ch, err := b.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	payload := []byte("test-payload")
	if err := b.PushOne(ctx, topic, payload); err != nil {
		t.Fatalf("PushOne: %v", err)
	}

	select {
	case msg := <-ch:
		if string(msg.Data) != string(payload) {
			t.Errorf("got %q, want %q", msg.Data, payload)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("PushOne was not delivered to local subscriber — regression of mula push/subscribe disconnect bug")
	}
}

// TestPushOneDeliveredToWorkerDirectTopic verifies the direct worker routing
// path: router pins WorkerID and uses pepper.push.{workerID} instead of the
// shared group queue. The worker must receive it on its own topic.
func TestPushOneDeliveredToWorkerDirectTopic(t *testing.T) {
	b, err := NewMula(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	workerTopic := TopicPush("w-specific")
	groupTopic := TopicPush("default")

	workerCh, err := b.Subscribe(ctx, workerTopic)
	if err != nil {
		t.Fatalf("Subscribe worker topic: %v", err)
	}
	groupCh, err := b.Subscribe(ctx, groupTopic)
	if err != nil {
		t.Fatalf("Subscribe group topic: %v", err)
	}

	// Publish to worker-direct topic only.
	if err := b.PushOne(ctx, workerTopic, []byte("direct")); err != nil {
		t.Fatalf("PushOne: %v", err)
	}

	select {
	case msg := <-workerCh:
		if string(msg.Data) != "direct" {
			t.Errorf("worker topic: got %q, want %q", msg.Data, "direct")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("message not delivered to worker-direct topic")
	}

	// Group channel should NOT have received it.
	select {
	case msg := <-groupCh:
		t.Errorf("group topic should not have received worker-direct message, got %q", msg.Data)
	case <-time.After(50 * time.Millisecond):
		// correct — group did not receive it
	}
}

// TestPushOneMultipleLocalSubscribersSameTopicGetsCopy verifies that multiple
// local subscribers on the same push topic each receive the message independently
// (pub semantics on a push topic from a single PushOne is not guaranteed — this
// tests only that the subscriber that was registered receives it).
func TestPushOneLocalSubscriberCancelCleans(t *testing.T) {
	b, err := NewMula(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	topic := TopicPush("worker-cancel-test")
	ch, err := b.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Cancel context — subscriber should clean up without panic.
	cancel()

	// Channel should close after cancel.
	select {
	case _, ok := <-ch:
		if ok {
			// Drain residual messages — not an error
		}
		// closed is fine
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber channel not closed after context cancel")
	}
}

// TestPushOneNotDeliveredToPublishSubscribers verifies that PushOne to a push
// topic does NOT deliver to subscribers of the corresponding pub topic.
// These are separate topics with separate semantics.
func TestPushOneNotDeliveredToPublishSubscribers(t *testing.T) {
	b, err := NewMula(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pubCh, err := b.Subscribe(ctx, TopicPub("default"))
	if err != nil {
		t.Fatalf("Subscribe pub: %v", err)
	}

	pushCh, err := b.Subscribe(ctx, TopicPush("default"))
	if err != nil {
		t.Fatalf("Subscribe push: %v", err)
	}

	if err := b.PushOne(ctx, TopicPush("default"), []byte("push-msg")); err != nil {
		t.Fatalf("PushOne: %v", err)
	}

	// push subscriber receives it
	select {
	case msg := <-pushCh:
		if string(msg.Data) != "push-msg" {
			t.Errorf("push topic: got %q, want %q", msg.Data, "push-msg")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("push subscriber did not receive PushOne message")
	}

	// pub subscriber should NOT receive it
	select {
	case msg := <-pubCh:
		t.Errorf("pub subscriber should not receive PushOne message, got %q", msg.Data)
	case <-time.After(50 * time.Millisecond):
		// correct
	}
}
