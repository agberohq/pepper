// Isolation test for the cluster bug.  This proves whether the root cause is
// in the Go dispatch path (router → bus → coord queue) or in the Python
// worker's subscribe/BRPOP path.
//
// Strategy:  use the in-memory coord store and a pure-Go "fake Python worker"
// that drains pepper.push.default and replies on pepper.res.<origin>.  No
// Python, no Redis, no NATS.  If the fake worker drains the queue and Do()
// returns successfully, the Go side is fine and the bug lives in the Python
// transport.  If the fake worker never sees the request, the bug is in Go.
//
// Run with:
//   go test -v -run TestClusterIsolation -count=1 -timeout 20s .

package tests

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agberohq/pepper"
	"github.com/agberohq/pepper/internal/coord"
	"github.com/agberohq/pepper/internal/core"
)

// TestClusterIsolation_GoSideDeliversToCoordQueue verifies that when a
// Pepper node in distributed mode calls Do(), the request ACTUALLY lands on
// the coord queue, where a subscriber can drain it.
//
// This test does NOT use Python workers — it plays the role of a worker
// directly via coord.Store.Pull on the push queue.
//
// What it proves:
//   - If Pull() sees the request   → Go dispatch works; bug is in Python transport.
//   - If Pull() times out          → Go dispatch is the bug.
//
// IMPORTANT: this test exposes whether the request even reaches the queue.
// The existing real cluster test can't tell us this because it conflates the
// Go path and the Python path into one end-to-end success criterion.
func TestClusterIsolation_GoSideDeliversToCoordQueue(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	// Register a cap on nodeA so the router believes a worker-capable spec
	// exists.  We register a Go-runtime cap to avoid spawning Python.
	// But register via a plain Script-style handler using an in-memory
	// adapter would require more machinery.  Instead, we bypass Register
	// entirely: we stand up Pepper in distributed mode and manually
	// inject a fake "worker_hello" into the coord bus so the router
	// thinks there's a live worker for the "default" group.
	node, err := pepper.New(
		pepper.WithCodec(pepper.CodecJSON),
		pepper.WithCoord(store),
		pepper.WithTransportURL("mem://iso"),
		pepper.WithShutdownTimeout(2*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer node.Stop()

	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Simulate a live Python worker by sending worker_hello on pepper.control.
	// The router's handleControl will register the worker in its in-flight
	// map and (for python runtime) also iterate caps and send cap_load —
	// but we have no caps registered, so it just registers the worker.
	hello := map[string]any{
		"msg_type":  "worker_hello",
		"worker_id": "fake-py-1",
		"groups":    []string{"default"},
		"runtime":   "python",
		"codec":     "json",
	}
	helloBytes, _ := json.Marshal(hello)
	if err := store.Publish(context.Background(), "pepper.control", helloBytes); err != nil {
		t.Fatalf("publish hello: %v", err)
	}

	// Also send cap_ready so HasCapWorker returns true for "any.cap".
	capReady := map[string]any{
		"msg_type":  "cap_ready",
		"worker_id": "fake-py-1",
		"cap":       "any.cap",
	}
	capReadyBytes, _ := json.Marshal(capReady)
	if err := store.Publish(context.Background(), "pepper.control", capReadyBytes); err != nil {
		t.Fatalf("publish cap_ready: %v", err)
	}

	// Give the control loop a moment to ingest both messages.
	time.Sleep(50 * time.Millisecond)

	// Drain the push queue in a goroutine — this is our fake worker.
	var got atomic.Int32
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer drainCancel()

	type req struct {
		Cap      string `json:"cap"`
		CorrID   string `json:"corr_id"`
		OriginID string `json:"origin_id"`
		MsgType  string `json:"msg_type"`
		ReplyTo  string `json:"reply_to"`
	}

	go func() {
		for {
			data, err := store.Pull(drainCtx, "pepper.push.default")
			if err != nil {
				return
			}
			got.Add(1)
			var r req
			if jerr := json.Unmarshal(data, &r); jerr != nil {
				t.Logf("fake worker: decode: %v  raw=%d bytes", jerr, len(data))
				continue
			}
			t.Logf("fake worker received: cap=%s corr_id=%s origin_id=%s reply_to=%s",
				r.Cap, r.CorrID, r.OriginID, r.ReplyTo)

			// Synthesize a res envelope and publish to reply_to.
			res := map[string]any{
				"msg_type":  "res",
				"corr_id":   r.CorrID,
				"origin_id": r.OriginID,
				"worker_id": "fake-py-1",
				"cap":       r.Cap,
				"payload":   []byte(`{"echoed":true}`),
			}
			resBytes, _ := json.Marshal(res)
			topic := r.ReplyTo
			if topic == "" {
				topic = "pepper.res." + r.OriginID
			}
			if err := store.Publish(context.Background(), topic, resBytes); err != nil {
				t.Logf("fake worker publish res: %v", err)
			}
		}
	}()

	// Now call Do() and see whether (a) the request arrives on the queue,
	// and (b) the caller receives the fake response.
	doCtx, doCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer doCancel()

	t0 := time.Now()
	_, doErr := node.Do(doCtx, "any.cap", core.In{"hello": "world"})
	elapsed := time.Since(t0)

	t.Logf("Do() returned after %v, err=%v, queue_pulls=%d",
		elapsed, doErr, got.Load())

	if got.Load() == 0 {
		t.Error("FAIL: Go dispatch path did NOT deliver the request to the coord queue. " +
			"The bug is in the Go path (router/bus/coord), not in the Python transport. " +
			"See TestClusterDiag_DistributedModeSilentlyAcceptsUnregisteredCap — the router " +
			"pushes to a queue nobody drained, and there is no 'no workers' check in distributed mode.")
		return
	}

	if doErr != nil {
		t.Errorf("FAIL: Request landed on the queue (got=%d) and fake worker replied, "+
			"but Do() still errored: %v. The bug is in the response routing path on the Go side "+
			"(pepper.res.* not being read, or pending.Resolve not firing, or envelope codec mismatch).",
			got.Load(), doErr)
		return
	}

	t.Logf("PASS: Go dispatch and response routing both work end-to-end. " +
		"The cluster hang you see with Python workers must be in the Python transport " +
		"(runtime/python/transport.py) — BRPOP/SUB not actually draining the queue, or " +
		"the worker not publishing the response to pepper.res.<origin> correctly.")
}

// TestClusterIsolation_CodecMismatchFakeWorker shows what happens if the
// fake worker encodes responses with a codec the Go side can't decode — the
// exact failure mode that would cause a hang if Python's msgpack frames
// arrive but Go is configured for JSON (or vice versa).
//
// Symptom: Pull sees the request, publish succeeds, Go's responseLoop
// receives the msg but Unmarshal fails silently (routeResponse line 274:
// "if err := p.codec.Unmarshal(data, &raw); err != nil { return }"), so
// pending.Resolve never fires and the caller hangs until deadline.
func TestClusterIsolation_CodecMismatchCausesSilentHang(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	node, err := pepper.New(
		pepper.WithCodec(pepper.CodecJSON),
		pepper.WithCoord(store),
		pepper.WithTransportURL("mem://codec"),
		pepper.WithShutdownTimeout(2*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer node.Stop()
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Register a phantom worker.
	hello := map[string]any{
		"msg_type": "worker_hello", "worker_id": "phantom",
		"groups": []string{"default"}, "runtime": "python", "codec": "json",
	}
	helloBytes, _ := json.Marshal(hello)
	_ = store.Publish(context.Background(), "pepper.control", helloBytes)
	capReady2 := map[string]any{"msg_type": "cap_ready", "worker_id": "phantom", "cap": "any.cap"}
	capReady2Bytes, _ := json.Marshal(capReady2)
	_ = store.Publish(context.Background(), "pepper.control", capReady2Bytes)
	time.Sleep(50 * time.Millisecond)

	// Fake worker that replies with GARBAGE (simulating a codec mismatch or
	// truncated frame) on the expected reply topic.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer drainCancel()
	go func() {
		for {
			data, err := store.Pull(drainCtx, "pepper.push.default")
			if err != nil {
				return
			}
			var r struct {
				OriginID string `json:"origin_id"`
				ReplyTo  string `json:"reply_to"`
			}
			_ = json.Unmarshal(data, &r)
			topic := r.ReplyTo
			if topic == "" {
				topic = "pepper.res." + r.OriginID
			}
			// Publish deliberately malformed data — Go's codec.Unmarshal
			// will fail and routeResponse returns silently.
			_ = store.Publish(context.Background(), topic, []byte{0xff, 0xfe, 0xfd, 0x00})
		}
	}()

	doCtx, doCancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer doCancel()
	t0 := time.Now()
	_, err = node.Do(doCtx, "any.cap", core.In{"x": 1})
	elapsed := time.Since(t0)

	if err == nil {
		t.Fatal("expected hang due to codec mismatch, but Do() succeeded")
	}
	if elapsed < 700*time.Millisecond {
		t.Fatalf("expected Do() to hang until deadline (~800ms), elapsed=%v", elapsed)
	}
	t.Logf("CONFIRMED: when the worker's response bytes fail codec.Unmarshal, "+
		"routeResponse() returns silently and the caller hangs until the deadline. "+
		"elapsed=%v err=%v", elapsed, err)
	t.Logf("This is one plausible explanation for the SessionSharing / WorkerLocalKV hang. " +
		"Check: is the Python worker's response envelope encoded with the same codec " +
		"(json vs msgpack) that the Go node configured? Search runtime.py for _codec = _make_codec() " +
		"and verify PEPPER_CODEC env var matches Config.Codec on the Go side.")
}

// TestClusterIsolation_EnvelopeDecodeOfRouterDispatch decodes a real Pepper
// dispatched envelope to reveal what's actually on the wire. This tells us
// whether the envelope shape matches what the Python worker's _message_loop
// expects (`msg_type == "req"`).  A mismatch here would explain why Python
// doesn't process the request even though BRPOP sees bytes.
func TestClusterIsolation_EnvelopeShapeOnTheWire(t *testing.T) {
	store := coord.NewMemory()
	defer store.Close()

	node, err := pepper.New(
		pepper.WithCodec(pepper.CodecJSON),
		pepper.WithCoord(store),
		pepper.WithTransportURL("mem://shape"),
		pepper.WithShutdownTimeout(2*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer node.Stop()
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Phantom worker so router accepts the dispatch.
	hello := map[string]any{
		"msg_type": "worker_hello", "worker_id": "shape-w",
		"groups": []string{"default"}, "runtime": "python", "codec": "json",
	}
	helloBytes, _ := json.Marshal(hello)
	_ = store.Publish(context.Background(), "pepper.control", helloBytes)
	capReady3 := map[string]any{"msg_type": "cap_ready", "worker_id": "shape-w", "cap": "shape.cap"}
	capReady3Bytes, _ := json.Marshal(capReady3)
	_ = store.Publish(context.Background(), "pepper.control", capReady3Bytes)
	time.Sleep(50 * time.Millisecond)

	// Dispatch a request and capture the raw envelope.
	captured := make(chan []byte, 1)
	go func() {
		data, err := store.Pull(context.Background(), "pepper.push.default")
		if err == nil {
			captured <- data
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	go func() {
		_, _ = node.Do(ctx, "shape.cap", core.In{"a": 1})
	}()

	select {
	case data := <-captured:
		t.Logf("raw envelope on pepper.push.default: %d bytes", len(data))
		// Best-effort JSON decode (only works if codec=json).
		var env map[string]any
		if err := json.Unmarshal(data, &env); err != nil {
			t.Logf("NOTE: envelope is not JSON — codec is likely msgpack. "+
				"First 32 bytes: % x", data[:min(32, len(data))])
			t.Logf("If Python worker is configured for JSON but Go is sending msgpack " +
				"(or vice-versa), the worker's _message_loop will fail to decode and " +
				"never dispatch to the capability. Check PEPPER_CODEC env var passed to " +
				"the Python subprocess vs the Go Config.Codec.")
			return
		}
		t.Logf("envelope keys: %v", keysOf(env))
		t.Logf("msg_type=%v cap=%v corr_id=%v origin_id=%v reply_to=%v group=%v worker_id=%v",
			env["msg_type"], env["cap"], env["corr_id"], env["origin_id"],
			env["reply_to"], env["group"], env["worker_id"])

		// Verify the critical field — Python expects msg_type == "req".
		if mt, _ := env["msg_type"].(string); mt != "req" {
			t.Errorf("BUG: envelope msg_type=%q (expected 'req'). "+
				"Python worker's _message_loop will skip this message.", mt)
		}
		if rt, _ := env["reply_to"].(string); rt == "" {
			t.Errorf("BUG: envelope reply_to is empty. Python worker can't reply.")
		}
	case <-time.After(1 * time.Second):
		t.Error("FAIL: no envelope arrived on pepper.push.default — Go dispatch is not " +
			"actually pushing to the queue. This is the primary bug.")
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
