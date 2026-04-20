package router

// Regression tests for router direct-worker routing (bug found during pipeline debugging).
//
// affinityWorker() correctly selected the target worker and set env.WorkerID,
// but Dispatch() still called PushOne(TopicPush(env.Group), ...) — the shared group
// queue. Any worker in that group raced to dequeue the message. Adapter workers
// (in-process Go) could not receive from the push queue at all (separate mula bug),
// and Python workers would receive adapter-only caps like "song.analyze" and return
// CAP_NOT_FOUND errors.
//
// when env.WorkerID is set, Dispatch() uses TopicPush(env.WorkerID) — the
// worker's own private queue — so the message goes directly to the intended worker.

import (
	"context"
	"testing"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/envelope"
)

// TestDispatchAnyRoutesToWorkerDirectTopic verifies that DispatchAny with cap
// affinity routes to the worker's own push topic, not the shared group queue.
// This is the direct regression for the "wrong worker receives song.analyze" bug.
func TestDispatchAnyRoutesToWorkerDirectTopic(t *testing.T) {
	r, b, _ := newRouter(t)
	r.RegisterWorker(helloMsg("w-1", []string{"default"}, []string{"song.analyze"}))
	r.MarkCapReady("w-1", "song.analyze")

	env := makeEnv("song.analyze", "default", envelope.DispatchAny)
	if err := r.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// Must land on the worker-direct topic, not the group queue.
	if _, ok := b.PopPush(bus.TopicPush("w-1")); !ok {
		t.Fatal("expected message on worker-direct push topic pepper.push.w-1, got nothing — regression of shared group queue routing bug")
	}

	// Group queue must be empty.
	if data, ok := b.PopPush(bus.TopicPush("default")); ok {
		t.Errorf("message should NOT be on group queue, but got %d bytes — shared queue routing would allow wrong worker to intercept", len(data))
	}
}

// TestDispatchAnyGroupFallbackWhenNoAffinity verifies that when no cap affinity
// exists (no worker has called MarkCapReady for the cap), the router falls back
// to any worker in the group and uses the group push topic.
func TestDispatchAnyGroupFallbackWhenNoAffinity(t *testing.T) {
	r, b, _ := newRouter(t)
	// Register worker in group but do NOT call MarkCapReady — no cap affinity.
	r.RegisterWorker(helloMsg("w-1", []string{"default"}, nil))

	env := makeEnv("any.cap", "default", envelope.DispatchAny)
	if err := r.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// With cap affinity, affinityWorker still returns w-1 (leastBusy of groupWorkers).
	// The message lands on TopicPush("w-1") via direct routing.
	_, onWorker := b.PopPush(bus.TopicPush("w-1"))
	_, onGroup := b.PopPush(bus.TopicPush("default"))
	if !onWorker && !onGroup {
		t.Fatal("expected message on either worker-direct or group push topic")
	}
}

// TestDispatchAnyDoesNotRouteToWrongWorker verifies that when two workers are
// registered but only one has cap affinity, the message goes to the cap-affinity
// worker — not the other one.
func TestDispatchAnyDoesNotRouteToWrongWorker(t *testing.T) {
	r, b, _ := newRouter(t)
	r.RegisterWorker(helloMsg("python-worker", []string{"default"}, []string{"audio.convert"}))
	r.MarkCapReady("python-worker", "audio.convert")
	r.RegisterWorker(helloMsg("adapter-worker", []string{"default"}, []string{"song.analyze"}))
	r.MarkCapReady("adapter-worker", "song.analyze")

	// Dispatch song.analyze — must go to adapter-worker, not python-worker.
	env := makeEnv("song.analyze", "default", envelope.DispatchAny)
	if err := r.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	if _, ok := b.PopPush(bus.TopicPush("adapter-worker")); !ok {
		t.Fatal("expected message on adapter-worker direct topic")
	}
	if data, ok := b.PopPush(bus.TopicPush("python-worker")); ok {
		t.Errorf("message must NOT go to python-worker, got %d bytes — this would cause CAP_NOT_FOUND", len(data))
	}
}
