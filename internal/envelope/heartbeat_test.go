package envelope

import "testing"

// TestHbPingMap pins the heartbeat field names the router, inspector and every
// bundled runtime read. All in-process runtimes share this builder.
func TestHbPingMap(t *testing.T) {
	hb := HbPingMap("w-1", "http", 42, []string{"gpu"}, 7, 1500)

	if got, want := hb[KeyMsgType], string(MsgHbPing); got != want {
		t.Errorf("msg_type = %v, want %v", got, want)
	}
	if got, want := hb[KeyProtoVer], ProtoVer; got != want {
		t.Errorf("proto_ver = %v, want %v", got, want)
	}
	if got, want := hb[KeyWorkerID], "w-1"; got != want {
		t.Errorf("worker_id = %v, want %v", got, want)
	}
	if got, want := hb[KeyRuntime], "http"; got != want {
		t.Errorf("runtime = %v, want %v", got, want)
	}
	if got, want := hb[KeyLoad], uint8(42); got != want {
		t.Errorf("load = %v, want %v", got, want)
	}
	if got, want := hb[KeyRequestsServed], uint64(7); got != want {
		t.Errorf("requests_served = %v, want %v", got, want)
	}
	if got, want := hb[KeyUptimeMs], int64(1500); got != want {
		t.Errorf("uptime_ms = %v, want %v", got, want)
	}

	// The router reads heartbeats back through Data, so the two must agree.
	d := Data(hb)
	if d.MsgType() != string(MsgHbPing) || d.WorkerID() != "w-1" {
		t.Errorf("Data round-trip mismatch: %+v", hb)
	}
}
