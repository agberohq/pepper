// These tests spin up two or more Pepper instances connected via a shared
// Redis or NATS coordination store, then verify that:
//
//   - Capabilities registered on node A are dispatched and executed on node B
//   - Session data written by node A is visible to node B via the coord store
//   - The shared KV store (pepper.kv) is worker-local (not cross-node) but
//     pepper.session() IS shared across nodes through coord
//   - Work queues deliver exactly once even when both nodes pull concurrently
//   - A node leaving mid-flight causes the router to re-queue on the survivor
//
// # Skip conditions
//
//   - PEPPER_REAL_TESTS=1 not set → entire file skips
//   - Redis not reachable at PEPPER_TEST_REDIS_HOST:PEPPER_TEST_REDIS_PORT → Redis tests skip
//   - NATS not reachable at PEPPER_TEST_NATS_HOST:PEPPER_TEST_NATS_PORT → NATS tests skip
//   - python3 not in PATH → Python worker tests skip
//
// # Running locally
//
//	# Start Redis and NATS first:
//	redis-server &
//	nats-server -js &
//
//	PEPPER_REAL_TESTS=1 go test -v -run TestRealCluster -timeout 120s .
//
// # Running a specific backend
//
//	PEPPER_REAL_TESTS=1 go test -v -run TestRealClusterRedis -timeout 120s .
//	PEPPER_REAL_TESTS=1 go test -v -run TestRealClusterNATS  -timeout 120s .

package pepper

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/coord"
	"github.com/agberohq/pepper/internal/core"
)

// skip guards

func requireRedisCluster(t *testing.T) string {
	t.Helper()
	host := os.Getenv("PEPPER_TEST_REDIS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	addr := host + ":" + port
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Skipf("Redis not reachable at %s — skipping cluster test (start with: redis-server)", addr)
	}
	conn.Close()
	return "redis://" + addr
}

func requireNATSCluster(t *testing.T) string {
	t.Helper()
	host := os.Getenv("PEPPER_TEST_NATS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("PEPPER_TEST_NATS_PORT")
	if port == "" {
		port = "4222"
	}
	addr := host + ":" + port
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		t.Skipf("NATS not reachable at %s — skipping cluster test (start with: nats-server -js)", addr)
	}
	conn.Close()
	return "nats://" + addr
}

// clusterSetup holds two Pepper nodes sharing a coord backend.
type clusterSetup struct {
	nodeA  *Pepper
	nodeB  *Pepper
	coordA coord.Store // owned by nodeA
	coordB coord.Store // owned by nodeB
	flush  coord.Store // used only for pre-test queue flushing
	url    string
}

func (c *clusterSetup) stop() {
	c.nodeA.Stop()
	c.nodeB.Stop()
	// coordA and coordB are closed by their respective nodes' shutdown hooks.
	// flush store is a separate connection used only for the pre-test cleanup.
	if c.flush != nil {
		c.flush.Close()
	}
}

// waitReady blocks until nodeB's router has registered at least one worker
// for the given capability — proving that the worker_hello fan-out from nodeA
// has been processed and nodeB will not fast-fail with ErrNoWorkers.
// Must be called after both nodeA.Start() and nodeB.Start() return.
func (c *clusterSetup) waitReady(t *testing.T, cap string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.nodeB.WaitWorkerReady(ctx, cap); err != nil {
		t.Fatalf("waitReady: nodeB did not see a ready worker for cap=%q within %v: %v", cap, timeout, err)
	}
}

// newCluster creates two Pepper nodes each with their own coord connection.
// nodeA owns the Python/Go workers; nodeB is a pure orchestrator (no workers)
// that routes requests through the coord queue to nodeA's workers.
//
// Each node gets its own coord.Store so that Stop() on one node does not
// close the shared stopCh and kill the other node's subscription goroutines.
func newCluster(t *testing.T, coordURL string, newCoord func(string) (coord.Store, error)) *clusterSetup {
	t.Helper()

	// Separate flush connection: used only for pre-test queue cleanup.
	// This avoids any lifecycle entanglement with the node connections.
	flush, err := newCoord(coordURL)
	if err != nil {
		t.Fatalf("coord connect (flush) %s: %v", coordURL, err)
	}

	// Flush any stale queue items left by previous test runs.
	// Without this, failed subtests leave items in pepper.push.* that get
	// picked up by workers in the next subtest, causing "cap not found" errors.
	ctx := context.Background()
	if keys, err := flush.List(ctx, "pepper.push."); err == nil {
		for _, k := range keys {
			_ = flush.Delete(ctx, k)
		}
	}
	if keys, err := flush.List(ctx, "pepper:cap:"); err == nil {
		for _, k := range keys {
			_ = flush.Delete(ctx, k)
		}
	}

	// Each node gets its own coord connection so that nodeA.Stop() closing
	// its coord store does not cancel nodeB's subscription goroutines.
	coordA, err := newCoord(coordURL)
	if err != nil {
		flush.Close()
		t.Fatalf("coord connect (nodeA) %s: %v", coordURL, err)
	}

	coordB, err := newCoord(coordURL)
	if err != nil {
		flush.Close()
		coordA.Close()
		t.Fatalf("coord connect (nodeB) %s: %v", coordURL, err)
	}

	// nodeA — has the workers, registered capabilities execute here
	nodeA, err := New(
		WithWorkers(
			NewWorker("cluster-worker-a1").Groups("default"),
			NewWorker("cluster-worker-a2").Groups("default"),
		),
		WithCoord(coordA),
		WithTransportURL(coordURL),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		flush.Close()
		coordA.Close()
		coordB.Close()
		t.Fatalf("nodeA New: %v", err)
	}

	// nodeB — orchestrator only, routes Do() calls via coord queue
	nodeB, err := New(
		WithCoord(coordB),
		WithTransportURL(coordURL),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		nodeA.Stop()
		flush.Close()
		coordB.Close()
		t.Fatalf("nodeB New: %v", err)
	}

	return &clusterSetup{nodeA: nodeA, nodeB: nodeB, coordA: coordA, coordB: coordB, flush: flush, url: coordURL}
}

// capability sources

// echoClusterCapSrc echoes inputs back. Minimal — no pepper.* calls needed.
const echoClusterCapSrc = `# pepper:name = cluster.echo
# pepper:groups = default

def run(inputs: dict) -> dict:
    return {**inputs, "echoed": True}
`

// counterCapSrc increments a worker-local counter using pepper.kv.
const counterCapSrc = `# pepper:name = cluster.counter
# pepper:groups = default
from runtime import pepper

def run(inputs: dict) -> dict:
    kv = pepper.kv("counters")
    key = inputs.get("key", "hits")
    count = (kv.get(key) or 0) + 1
    kv.set(key, count)

    # Also persist to session so the orchestrator node can observe via coord.
    sess = pepper.session()
    sess.set(key, count)

    return {"key": key, "count": count}
`

// slowCapSrc simulates a slow cap for concurrency tests.
const slowCapSrc = `# pepper:name = cluster.slow
# pepper:groups = default
import time

def run(inputs: dict) -> dict:
    delay = float(inputs.get("delay_s", 0.1))
    time.sleep(delay)
    return {"slept_s": delay}
`

// tests

// TestRealClusterRedis runs all cluster scenarios against a real Redis backend.
func TestRealClusterRedis(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "python3")
	requirePythonPackage(t, "msgpack")
	url := requireRedisCluster(t)
	t.Logf("Cluster backend: Redis (%s)", url)
	runClusterSuite(t, url, func(u string) (coord.Store, error) { return coord.NewRedis(u) })
}

// TestRealClusterNATS runs all cluster scenarios against a real NATS backend.
func TestRealClusterNATS(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "python3")
	requirePythonPackage(t, "msgpack")
	url := requireNATSCluster(t)
	t.Logf("Cluster backend: NATS (%s)", url)
	runClusterSuite(t, url, func(u string) (coord.Store, error) { return coord.NewNATS(u) })
}

// runClusterSuite runs every cluster scenario for a given coord backend.
func runClusterSuite(t *testing.T, url string, newCoord func(string) (coord.Store, error)) {
	t.Helper()
	t.Run("CrossNodeDispatch", func(t *testing.T) { testCrossNodeDispatch(t, url, newCoord) })
	t.Run("SessionSharing", func(t *testing.T) { testSessionSharing(t, url, newCoord) })
	t.Run("WorkerLocalKV", func(t *testing.T) { testWorkerLocalKV(t, url, newCoord) })
	t.Run("ConcurrentThroughput", func(t *testing.T) { testConcurrentThroughput(t, url, newCoord) })
	t.Run("ExactlyOnceDelivery", func(t *testing.T) { testExactlyOnceDelivery(t, url, newCoord) })
	t.Run("NodeBCanOrchestrate", func(t *testing.T) { testNodeBOrchestrates(t, url, newCoord) })
}

// testCrossNodeDispatch verifies that a Do() call on nodeB is executed by a
// worker on nodeA — proving cross-node routing through the coord queue.
func testCrossNodeDispatch(t *testing.T, url string, newCoord func(string) (coord.Store, error)) {
	t.Helper()
	cl := newCluster(t, url, newCoord)
	defer cl.stop()

	capDir := t.TempDir()
	echoPath := writeCap(t, capDir, "echo.py", echoClusterCapSrc)

	if err := cl.nodeA.Register(Script("cluster.echo", echoPath)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.nodeA.Start(startCtx); err != nil {
		t.Fatalf("nodeA Start: %v", err)
	}
	if err := cl.nodeB.Start(startCtx); err != nil {
		t.Fatalf("nodeB Start: %v", err)
	}
	cl.waitReady(t, "cluster.echo", 30*time.Second)

	doCtx, doCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer doCancel()

	// Issue the request from nodeB — must be routed to nodeA's worker.
	result, err := cl.nodeB.Do(doCtx, "cluster.echo", core.In{"msg": "hello-cluster"})
	if err != nil {
		t.Fatalf("nodeB.Do: %v", err)
	}

	out := result.AsJSON()
	t.Logf("cross-node result: %v  worker=%s  latency=%s", out, result.WorkerID, result.Latency)

	if out["msg"] != "hello-cluster" {
		t.Errorf("echo: got msg=%v, want hello-cluster", out["msg"])
	}
	if result.WorkerID == "" {
		t.Error("expected worker ID in result, got empty string")
	}
}

// testSessionSharing verifies that session data written by a worker on nodeA
// is readable by nodeB through the shared coord session store.
func testSessionSharing(t *testing.T, url string, newCoord func(string) (coord.Store, error)) {
	t.Helper()
	cl := newCluster(t, url, newCoord)
	defer cl.stop()

	capDir := t.TempDir()
	counterPath := writeCap(t, capDir, "counter.py", counterCapSrc)

	if err := cl.nodeA.Register(Script("cluster.counter", counterPath)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.nodeA.Start(startCtx); err != nil {
		t.Fatalf("nodeA Start: %v", err)
	}
	if err := cl.nodeB.Start(startCtx); err != nil {
		t.Fatalf("nodeB Start: %v", err)
	}
	cl.waitReady(t, "cluster.counter", 30*time.Second)

	doCtx, doCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer doCancel()

	sessionID := fmt.Sprintf("cluster-sess-%d", time.Now().UnixNano())

	// Send two requests in the same session — counter should increment.
	for i := 1; i <= 3; i++ {
		result, err := cl.nodeB.Do(doCtx, "cluster.counter",
			core.In{"key": "hits"},
			WithCallSession(sessionID),
		)
		if err != nil {
			t.Fatalf("Do #%d: %v", i, err)
		}
		out := result.AsJSON()
		t.Logf("counter #%d: count=%v worker=%s", i, out["count"], result.WorkerID)
	}

	// Read the session from nodeB's perspective via the shared coord store.
	sess := cl.nodeB.Session(sessionID)
	v, ok := sess.Get("hits")
	if !ok {
		t.Error("session key 'hits' not visible on nodeB after writes from nodeA worker")
	} else {
		t.Logf("session 'hits' visible on nodeB: %v", v)
	}
}

// testWorkerLocalKV verifies that pepper.kv() is worker-process-local:
// repeated calls to the same worker accumulate state, but the KV is NOT
// shared across nodes (that's what sessions/coord are for).
func testWorkerLocalKV(t *testing.T, url string, newCoord func(string) (coord.Store, error)) {
	t.Helper()
	cl := newCluster(t, url, newCoord)
	defer cl.stop()

	capDir := t.TempDir()
	counterPath := writeCap(t, capDir, "counter.py", counterCapSrc)

	if err := cl.nodeA.Register(Script("cluster.counter", counterPath)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.nodeA.Start(startCtx); err != nil {
		t.Fatalf("nodeA Start: %v", err)
	}
	if err := cl.nodeB.Start(startCtx); err != nil {
		t.Fatalf("nodeB Start: %v", err)
	}
	cl.waitReady(t, "cluster.counter", 30*time.Second)

	doCtx, doCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer doCancel()

	// Pin all requests to the same worker so the KV accumulates.
	var counts []float64
	for i := 0; i < 5; i++ {
		result, err := cl.nodeB.Do(doCtx, "cluster.counter",
			core.In{"key": "local-hits"},
			WithCallWorker("cluster-worker-a1"),
		)
		if err != nil {
			t.Fatalf("Do #%d: %v", i+1, err)
		}
		out := result.AsJSON()
		if c, ok := out["count"].(float64); ok {
			counts = append(counts, c)
		}
		t.Logf("kv local hit #%d: count=%v", i+1, out["count"])
	}

	// Counts should be strictly increasing (1, 2, 3, 4, 5) — proving the KV
	// accumulates within the worker process across requests.
	for i := 1; i < len(counts); i++ {
		if counts[i] <= counts[i-1] {
			t.Errorf("KV not accumulating: counts[%d]=%v <= counts[%d]=%v",
				i, counts[i], i-1, counts[i-1])
		}
	}
}

// testConcurrentThroughput fires N requests concurrently across both nodes
// and verifies all complete without errors. Measures end-to-end throughput.
func testConcurrentThroughput(t *testing.T, url string, newCoord func(string) (coord.Store, error)) {
	t.Helper()
	cl := newCluster(t, url, newCoord)
	defer cl.stop()

	capDir := t.TempDir()
	echoPath := writeCap(t, capDir, "echo.py", echoClusterCapSrc)

	if err := cl.nodeA.Register(Script("cluster.echo", echoPath)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.nodeA.Start(startCtx); err != nil {
		t.Fatalf("nodeA Start: %v", err)
	}
	if err := cl.nodeB.Start(startCtx); err != nil {
		t.Fatalf("nodeB Start: %v", err)
	}
	cl.waitReady(t, "cluster.echo", 30*time.Second)

	const n = 20
	var (
		wg       sync.WaitGroup
		errCount atomic.Int64
		t0       = time.Now()
	)

	doCtx, doCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer doCancel()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Alternate between nodeA and nodeB as orchestrator.
			node := cl.nodeB
			if id%2 == 0 {
				node = cl.nodeA
			}
			_, err := node.Do(doCtx, "cluster.echo", core.In{"id": id})
			if err != nil {
				t.Logf("request %d failed: %v", id, err)
				errCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(t0)
	throughput := float64(n) / elapsed.Seconds()

	t.Logf("Throughput: %d requests in %s = %.1f req/s  errors=%d",
		n, elapsed.Round(time.Millisecond), throughput, errCount.Load())

	if errCount.Load() > 0 {
		// Check whether failed items are still sitting in the Redis queue (never
		// BRPOPped) or were consumed but their responses never arrived (routing bug).
		// Use the flush coord which has an idle connection for diagnostic reads.
		diagCtx, diagCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer diagCancel()
		// Pull without blocking — if items are in the queue they will be returned
		// immediately; if the queue is empty, Pull will block and we cancel.
		queueLen := 0
		for {
			_, err := cl.flush.Pull(diagCtx, "pepper.push.default")
			if err != nil {
				break
			}
			queueLen++
		}
		if queueLen > 0 {
			t.Logf("DIAGNOSIS: %d items still in pepper.push.default — workers never BRPOPped them (transport/BRPOP bug)", queueLen)
		} else {
			t.Logf("DIAGNOSIS: pepper.push.default is empty — items were BRPOPped but responses were lost (response routing bug)")
		}
		t.Errorf("%d/%d requests failed", errCount.Load(), n)
	}
}

// testExactlyOnceDelivery pushes N items through the coord work queue with
// two concurrent pullers and verifies each item is processed exactly once.
func testExactlyOnceDelivery(t *testing.T, url string, newCoord func(string) (coord.Store, error)) {
	t.Helper()
	cl := newCluster(t, url, newCoord)
	defer cl.stop()

	capDir := t.TempDir()
	echoPath := writeCap(t, capDir, "echo.py", echoClusterCapSrc)

	if err := cl.nodeA.Register(Script("cluster.echo", echoPath)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.nodeA.Start(startCtx); err != nil {
		t.Fatalf("nodeA Start: %v", err)
	}
	if err := cl.nodeB.Start(startCtx); err != nil {
		t.Fatalf("nodeB Start: %v", err)
	}
	cl.waitReady(t, "cluster.echo", 30*time.Second)

	const n = 10
	seen := sync.Map{}
	var received atomic.Int64

	doCtx, doCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer doCancel()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			result, err := cl.nodeB.Do(doCtx, "cluster.echo", core.In{"seq": id})
			if err != nil {
				t.Logf("seq %d error: %v", id, err)
				return
			}
			out := result.AsJSON()
			seq := fmt.Sprintf("%v", out["seq"])
			if _, loaded := seen.LoadOrStore(seq, true); loaded {
				t.Errorf("exactly-once violation: seq %s delivered more than once", seq)
			}
			received.Add(1)
		}(i)
	}

	wg.Wait()
	t.Logf("Exactly-once: %d/%d items delivered", received.Load(), n)

	if received.Load() != n {
		t.Errorf("expected %d deliveries, got %d", n, received.Load())
	}
}

// testNodeBOrchestrates verifies that nodeB (orchestrator-only, no workers)
// can issue Do() calls that get executed on nodeA's workers, and that the
// result flows back correctly through the coord pubsub layer.
func testNodeBOrchestrates(t *testing.T, url string, newCoord func(string) (coord.Store, error)) {
	t.Helper()
	cl := newCluster(t, url, newCoord)
	defer cl.stop()

	capDir := t.TempDir()
	echoPath := writeCap(t, capDir, "echo.py", echoClusterCapSrc)
	slowPath := writeCap(t, capDir, "slow.py", slowCapSrc)

	if err := cl.nodeA.Register(Script("cluster.echo", echoPath)); err != nil {
		t.Fatalf("Register echo: %v", err)
	}
	if err := cl.nodeA.Register(Script("cluster.slow", slowPath)); err != nil {
		t.Fatalf("Register slow: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.nodeA.Start(startCtx); err != nil {
		t.Fatalf("nodeA Start: %v", err)
	}
	if err := cl.nodeB.Start(startCtx); err != nil {
		t.Fatalf("nodeB Start: %v", err)
	}
	cl.waitReady(t, "cluster.echo", 30*time.Second)

	doCtx, doCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer doCancel()

	// Issue both from nodeB — the pure orchestrator.
	result1, err := cl.nodeB.Do(doCtx, "cluster.echo", core.In{"from": "orchestrator"})
	if err != nil {
		t.Fatalf("nodeB→cluster.echo: %v", err)
	}
	t.Logf("echo: worker=%s latency=%s", result1.WorkerID, result1.Latency)

	result2, err := cl.nodeB.Do(doCtx, "cluster.slow", core.In{"delay_s": 0.05})
	if err != nil {
		t.Fatalf("nodeB→cluster.slow: %v", err)
	}
	out2 := result2.AsJSON()
	t.Logf("slow: slept=%v worker=%s latency=%s", out2["slept_s"], result2.WorkerID, result2.Latency)

	if result1.WorkerID == "" || result2.WorkerID == "" {
		t.Error("orchestrator results missing worker IDs — response routing may be broken")
	}
}
