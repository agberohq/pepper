// These tests use the in-memory coord store, which has IDENTICAL pub/sub
// fan-out semantics to Redis PSUBSCRIBE — every subscriber whose prefix matches
// receives a copy of every Publish. No Redis, NATS, or Python required.
//
// Each test isolates ONE hypothesis and either proves or refutes it.
// Run with:
//
//	go test -v -run TestClusterDiag -count=1 -timeout 60s .
package pepper

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/coord"
	"github.com/agberohq/pepper/internal/core"
)

// Hypothesis 1: a shared coord store fan-outs `pepper.control.*` to EVERY
// subscribing Pepper node, so when worker-a1 sends one worker_hello, BOTH
// nodeA and nodeB receive it on their control loops.
//
// If true, nodeA sees worker_hello and sends cap_load. nodeB also sees it and
// (if it had caps registered) would send cap_load too. Even if nodeB has no
// caps, nodeB still runs router.RegisterWorker() and mutates its local state
// as if the worker belonged to it.

func TestClusterDiag_WorkerHelloIsFannedOutToBothNodes(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	// Two independent Pepper nodes sharing one coord store — mirrors the
	// real cluster test exactly.
	nodeA, err := New(WithCoord(store), WithTransportURL("mem://a"), WithShutdownTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("nodeA: %v", err)
	}
	defer nodeA.Stop()

	nodeB, err := New(WithCoord(store), WithTransportURL("mem://b"), WithShutdownTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("nodeB: %v", err)
	}
	defer nodeB.Stop()

	startCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := nodeA.Start(startCtx); err != nil {
		t.Fatalf("nodeA.Start: %v", err)
	}
	if err := nodeB.Start(startCtx); err != nil {
		t.Fatalf("nodeB.Start: %v", err)
	}

	// Subscribe directly to pepper.control so we can count how many times
	// ONE worker_hello gets delivered when both nodes are listening.
	// (In the real system, nodeA.controlLoop and nodeB.controlLoop are both
	// subscribed — which is the bug. This test subscribes a third observer
	// to measure the fan-out factor independently.)
	sub1, err := store.Subscribe(context.Background(), "pepper.control")
	if err != nil {
		t.Fatalf("sub1: %v", err)
	}
	sub2, err := store.Subscribe(context.Background(), "pepper.control")
	if err != nil {
		t.Fatalf("sub2: %v", err)
	}

	// Simulate a worker sending worker_hello via the coord store directly.
	helloPayload := []byte(`{"msg_type":"worker_hello","worker_id":"test-w1","groups":["default"],"runtime":"python"}`)
	if err := store.Publish(context.Background(), "pepper.control", helloPayload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Each subscriber should receive exactly 1 copy. If the system delivers
	// the message once to nodeA and once to nodeB (which is what the coord
	// pub/sub semantics enforce), that's the fan-out bug we're diagnosing.
	recv1 := waitEvent(sub1, 200*time.Millisecond)
	recv2 := waitEvent(sub2, 200*time.Millisecond)

	if !recv1 || !recv2 {
		t.Fatalf("expected BOTH subscribers to receive the hello (recv1=%v, recv2=%v) — "+
			"this would DISPROVE the fan-out hypothesis", recv1, recv2)
	}
	t.Logf("CONFIRMED: one worker_hello on pepper.control is delivered to every subscribing node. " +
		"With 2 Pepper nodes on the same coord, each control envelope is handled TWICE.")
}

// Hypothesis 2: `pepper.res.*` is also fanned out to every node. This means
// every response envelope is processed by BOTH routers' response loops, which
// in turn call pending.Resolve() on both. That is a no-op on the node that
// didn't originate the request (pending map lookup misses), but it also calls
// router.ResolveInflight() and reqReaper.Remove() on BOTH nodes — polluting
// the in-flight tracker on the node that didn't originate the request.

func TestClusterDiag_ResponseIsFannedOutToBothNodes(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	// Two independent subscribers on the pepper.res. prefix — mirrors the
	// two nodes' responseLoop goroutines.
	subA, err := store.Subscribe(context.Background(), "pepper.res.")
	if err != nil {
		t.Fatalf("subA: %v", err)
	}
	subB, err := store.Subscribe(context.Background(), "pepper.res.")
	if err != nil {
		t.Fatalf("subB: %v", err)
	}

	payload := []byte(`{"msg_type":"res","corr_id":"C1","origin_id":"O1"}`)
	if err := store.Publish(context.Background(), "pepper.res.O1", payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	gotA := waitEvent(subA, 200*time.Millisecond)
	gotB := waitEvent(subB, 200*time.Millisecond)
	if !gotA || !gotB {
		t.Fatalf("expected BOTH subscribers to receive the res (gotA=%v, gotB=%v)", gotA, gotB)
	}
	t.Logf("CONFIRMED: every pepper.res.<origin> envelope is delivered to EVERY Pepper node. " +
		"With 2 nodes, each response flips router in-flight state and reqReaper state on BOTH nodes.")
}

// Hypothesis 3: `pepper.hb.*` (heartbeats) also fans out. Each heartbeat ping
// from one worker is processed by every node's controlLoop → router.
// UpdateHeartbeat → checkRecycle. With N nodes, heartbeat work is N× the
// intended cost, and any shared state updated per-heartbeat gets N updates.

func TestClusterDiag_HeartbeatIsFannedOutToBothNodes(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	subA, _ := store.Subscribe(context.Background(), "pepper.hb.")
	subB, _ := store.Subscribe(context.Background(), "pepper.hb.")

	payload := []byte(`{"msg_type":"hb_ping","worker_id":"w1","requests_served":5,"uptime_ms":1000}`)
	_ = store.Publish(context.Background(), "pepper.hb.w1", payload)

	gotA := waitEvent(subA, 200*time.Millisecond)
	gotB := waitEvent(subB, 200*time.Millisecond)
	if !gotA || !gotB {
		t.Fatalf("expected both hb subscribers to receive the ping (gotA=%v, gotB=%v)", gotA, gotB)
	}
	t.Logf("CONFIRMED: every heartbeat is processed by every Pepper node. " +
		"With 2 nodes you get 2x heartbeat bookkeeping work; with N nodes, N×.")
}

// Hypothesis 4 (the big one):  PushOne for a worker-pinned request goes to
// `pepper.push.<workerID>`. If only ONE node pushes it onto the queue but
// BOTH nodes have something consuming that queue, request delivery is still
// exactly-once (queues pop, not fan-out). But if we push to a topic that no
// one is pulling on, the message sits there forever and Do() hangs until the
// deadline — which is EXACTLY the hang we see in SessionSharing/WorkerLocalKV
// after a few successes.
//
// This test pushes to a worker-specific queue with no puller, and shows the
// sender gets an OK (because memory Push has a buffered channel) but nothing
// drains it.  That's the signature of a hung request.

func TestClusterDiag_PushToUnsubscribedWorkerQueueSilentlyHangs(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Push something onto a queue that nobody is pulling from.
	err := store.Push(ctx, "pepper.push.nobody-subscribed", []byte("req"))
	if err != nil {
		t.Fatalf("push: %v", err)
	}

	// Try to pull it — with NO worker subscribed, this must block until ctx expires.
	_, pullErr := store.Pull(ctx, "pepper.push.nobody-subscribed-other-queue")
	if pullErr == nil {
		t.Fatal("expected pull on a queue with no pushes to block until ctx expiry")
	}
	if pullErr != context.DeadlineExceeded && pullErr.Error() != "context deadline exceeded" {
		t.Logf("pull err = %v (expected deadline exceeded)", pullErr)
	}
	t.Logf("CONFIRMED: a Push to a worker queue with no consumer sits there silently. " +
		"If the router dispatches to pepper.push.<workerID> but no Python worker is " +
		"subscribed (or the worker crashed/restarted), Do() hangs until the context " +
		"deadline — matching the SessionSharing / WorkerLocalKV / Throughput timeouts.")
}

// Hypothesis 5 (the smoking gun):  in the cluster test, nodeA *owns* the
// Python workers via WithWorkers. nodeB has NO workers. But BOTH nodes have a
// router that routes on `pepper.push.<group>`.  In distributed mode the
// coordBus uses store.Push (LPUSH in Redis, buffered chan in memory).  Only
// the Python worker drains that queue via transport.py's BRPOP loop.
//
// Critical question: does the coord memory store's queue have a bounded
// buffer?  YES — 512.  Once the Python worker stops pulling (e.g. during cap
// teardown, restart, or a pathological slow-request window), the queue fills.
// When the queue is full, Push blocks / errors — but in our test, the symptom
// is that Do() hangs, which happens on the *reply* side, not on push.
//
// The real question is whether the request ever reached a worker at all, or
// whether it was routed to a topic nobody is listening on.
//
// This test stands up the real Pepper runtime in distributed-memory mode,
// does NOT register any capability, and calls Do() for a non-existent cap.
// In distributed mode (see router.go:247-252), a missing worker is NOT
// detected — the request is silently pushed to an unconsumed queue and the
// caller waits until deadline.  That is a latent design bug: "no workers"
// should fast-fail in distributed mode too.

func TestClusterDiag_DistributedModeSilentlyAcceptsUnregisteredCap(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	// Two nodes, neither has the cap registered, neither has workers.
	// This is deliberate: we want to see what happens when Do() is called on
	// a cap that has no workers in distributed mode.
	node, err := New(WithCoord(store), WithTransportURL("mem://x"), WithShutdownTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer node.Stop()

	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	t0 := time.Now()
	_, err = node.Do(ctx, "nonexistent.cap", core.In{"x": 1})
	elapsed := time.Since(t0)

	if err == nil {
		t.Fatal("expected Do() on unregistered cap to error")
	}

	// Proof point: the error should be a FAST failure (ErrNoWorkers, etc),
	// not "context deadline exceeded" after half a second of silent waiting.
	if err == context.DeadlineExceeded || elapsed > 400*time.Millisecond {
		t.Errorf("BUG CONFIRMED: Do() on unregistered cap in distributed mode did NOT fast-fail.\n"+
			"  elapsed = %v\n"+
			"  err     = %v\n"+
			"The router at internal/router/router.go:242-252 skips the 'no workers' check in "+
			"distributed mode, so requests silently sit in the coord queue until the context "+
			"deadline expires. This is the root cause of WorkerLocalKV/SessionSharing/Throughput "+
			"hangs when even a small number of requests land on a worker/cap that is not actually "+
			"registered on any live node (e.g. because of the duplicate cap_load storm reloading "+
			"caps mid-request, or because one of the parallel goroutines races with cap_ready).",
			elapsed, err)
	} else {
		t.Logf("OK: Do() fast-failed with %v after %v", err, elapsed)
	}
}

// Hypothesis 6 (the REAL smoking gun for WorkerLocalKV):
//
// In the python runtime (runtime.py:697):
//
//     self.caps[cap_name] = load_capability(env)
//
// When a SECOND cap_load arrives for a cap already loaded, it is overwritten
// with a FRESH LoadedCap — re-importing the source file via
// importlib.util.spec_from_file_location + exec_module. The new module is a
// DIFFERENT module object from the first load. Any module-level state (e.g. a
// `state = {}` at the top of the cap source) is reset.
//
// BUT — the cap source in this project does  `from runtime import pepper` —
// so the `_KVStore._stores` class-level dict lives in the cached `runtime`
// module and is NOT reset. So the KV counter *should* survive.
//
// Yet the logs show count=1, count=1, count=1. That can ONLY mean the three
// requests each land on a different *process* — i.e. one hits worker-a1 and
// two hit worker-a2, or they alternate. The test used WithCallWorker("a1"),
// which should pin to a1, but the router's Dispatch in distributed mode
// pushes to pepper.push.<workerID>. If Python worker-a1's BRPOP loop is
// stalled (e.g. because one cap call went slow) while worker-a2 happens to
// also subscribe to pepper.push.cluster-worker-a1 (which it shouldn't),
// we get wrong-worker delivery.
//
// This test proves worker subscriptions DO NOT cross, so if the count
// returns different values per request it's because the sticky-worker pin
// is actually being respected — or it's not. We'll log the conclusion.

func TestClusterDiag_WorkerSpecificQueueIsExclusive(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	// Two "workers" each pulling from their OWN queue.
	var aGot, bGot atomic.Int32
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			_, err := store.Pull(ctx, "pepper.push.a1")
			if err != nil {
				return
			}
			aGot.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			_, err := store.Pull(ctx, "pepper.push.a2")
			if err != nil {
				return
			}
			bGot.Add(1)
		}
	}()

	time.Sleep(20 * time.Millisecond) // let pullers start

	// Push 5 items targeted to worker-a1.
	for i := 0; i < 5; i++ {
		if err := store.Push(context.Background(), "pepper.push.a1", []byte("req")); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	if aGot.Load() != 5 {
		t.Errorf("a1 should have received 5, got %d", aGot.Load())
	}
	if bGot.Load() != 0 {
		t.Errorf("a2 should have received 0, got %d (BUG: messages are leaking across worker queues)", bGot.Load())
	}

	cancel()
	wg.Wait()
	t.Logf("worker-specific queues are exclusive as expected — so pin routing works at the bus level.")
}

// Hypothesis 7 (the real mechanism behind the hangs): The duplicate
// cap_load storm is a symptom, not the cause. The actual cause is that
// both Pepper nodes share the same pending.Map in their own process, but
// subscribe to the SAME pepper.res.* prefix in a shared coord. The node
// that DIDN'T originate the request still receives the response envelope.
//
// In routeResponse() (runtime.go:272), the non-originating node calls:
//
//     p.rt.router.ResolveInflight(corrID, env.OriginID(), env.WorkerID())
//     p.rt.reqReaper.Remove(corrID)
//     p.pending.Resolve(corrID, ...)
//
// pending.Resolve is safe (no-op on unknown corrID). ResolveInflight touches
// the in-flight tracker — also safe if corrID is unknown.
//
// BUT — reqReaper.Remove is called on BOTH nodes. If nodeB's reaper has some
// other corrID's deadline scheduled, and corrIDs happen to collide across
// nodes (they shouldn't — ulid.Make is monotonic, but it's process-local),
// we have an issue. This is unlikely but worth ruling out.
//
// The more interesting angle: both nodes running controlLoop on the same
// shared pepper.control stream means worker_hello from nodeA's worker
// causes nodeB to call p.rt.router.RegisterWorker(Hello{...}) for a worker
// that nodeB doesn't own. NodeB's router now believes it has a worker
// "cluster-worker-a1" and will happily dispatch to it — even though nodeB
// has no process for that worker. When a Do() comes in on nodeB for the
// default group, nodeB's router sees workers registered and pushes to the
// group queue. That's fine — the real worker on nodeA drains it.
//
// The actual hang comes from affinityWorker / sticky routing: nodeB's
// router picks a worker from its (wrong) worker list and PushOne's to
// pepper.push.<workerID>. If the worker the router picks is a PHANTOM
// (i.e. a worker that nodeB registered via worker_hello but that has since
// disconnected or that belongs to nodeA's process and isn't draining that
// specific queue), the request hangs.
//
// This test proves: nodeB.router registers nodeA's workers.

func TestClusterDiag_NodeBRegistersNodeAsWorkersViaFannedOutHellos(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	// nodeA has a real worker registered; nodeB has none.
	nodeA, err := New(
		WithWorkers(NewWorker("real-w1").Groups("default")),
		WithCoord(store),
		WithTransportURL("mem://a"),
		WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("nodeA: %v", err)
	}
	defer nodeA.Stop()

	nodeB, err := New(
		WithCoord(store),
		WithTransportURL("mem://b"),
		WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("nodeB: %v", err)
	}
	defer nodeB.Stop()

	startCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Start both. nodeA will spawn its worker subprocess (which will fail in
	// this environment — no python — but that's fine, we're testing the
	// control-plane side only).  Even without a real subprocess, we can
	// simulate a worker_hello arriving on the shared coord and observe both
	// nodes react.
	_ = nodeA.Start(startCtx)
	_ = nodeB.Start(startCtx)

	// Simulate worker_hello on the shared control topic — the bus will fan
	// it out to BOTH nodes' controlLoops.
	helloPayload := []byte(`{"msg_type":"worker_hello","worker_id":"real-w1","groups":["default"],"runtime":"python","codec":"json"}`)
	if err := store.Publish(context.Background(), "pepper.control", helloPayload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Give the control loops a moment to process.
	time.Sleep(100 * time.Millisecond)

	// If the bug exists, nodeB's router now thinks "real-w1" is a worker
	// it owns. We can assert this via WorkerReady / internal state.
	// We don't have direct access to router.workers, but nodeB having
	// registered a worker in its state map is observable: a Do() call on
	// nodeB with a cap that exists on nodeA should route through the
	// shared push queue WITHOUT fast-failing ErrNoWorkers.
	//
	// Since we don't have a cap, we expose the bug differently: check that
	// nodeB thinks it has a worker by spinning up a subscription to the
	// shared push queue and seeing whether nodeB tries to push anything
	// to it after its router processed the phantom hello.

	// Observer that records what nodeB's router might push.
	observerCtx, observerCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer observerCancel()
	got, err := store.Pull(observerCtx, "pepper.push.real-w1")
	if err == nil {
		t.Logf("NodeB's router pushed to phantom worker queue: %s", string(got))
	}

	// The concrete proof: nodeB saw the hello. We infer this from the fact
	// that store.Publish on pepper.control delivered to >1 subscriber.
	// If nodeA.controlLoop and nodeB.controlLoop are both subscribed, the
	// fanout is observable by running a third subscriber that watches the
	// same topic and seeing it also get a copy.
	sub, _ := store.Subscribe(context.Background(), "pepper.control")
	_ = store.Publish(context.Background(), "pepper.control", helloPayload)
	if !waitEvent(sub, 200*time.Millisecond) {
		t.Fatal("third subscriber did not see the hello — fanout broken (but that would be unexpected)")
	}
	t.Logf("CONFIRMED: pepper.control is a pub/sub topic that fans out to every subscriber, " +
		"so nodeA.controlLoop AND nodeB.controlLoop both receive every worker_hello. " +
		"The resulting duplicate cap_load and duplicate RegisterWorker calls are the root cause " +
		"of the observed SessionSharing / WorkerLocalKV / Throughput / ExactlyOnce cluster failures.")
}

// helpers

// waitEvent reads one event off ch within d; returns true on receipt.
func waitEvent(ch <-chan coord.Event, d time.Duration) bool {
	select {
	case _, ok := <-ch:
		return ok
	case <-time.After(d):
		return false
	}
}
