package inspect

import (
	"fmt"
	"time"
)

// Event is a decoded wire message forwarded to inspector subscribers.
type Event struct {
	Time      time.Time      `json:"time"`
	Direction string         `json:"dir"` //  "→" | "←" | "⇢" | "~" | "" | "hb"
	Type      string         `json:"type"`
	CorrID    string         `json:"corr_id,omitempty"`
	OriginID  string         `json:"origin_id,omitempty"`
	WorkerID  string         `json:"worker_id,omitempty"`
	Runtime   string         `json:"runtime,omitempty"`
	Cap       string         `json:"cap,omitempty"`
	Group     string         `json:"group,omitempty"`
	Dispatch  string         `json:"dispatch,omitempty"`
	Hop       uint8          `json:"hop,omitempty"`
	CbDepth   uint8          `json:"cb_depth,omitempty"`
	LatencyMs int64          `json:"latency_ms,omitempty"`
	ErrCode   string         `json:"err_code,omitempty"`
	Load      uint8          `json:"load,omitempty"`
	Groups    []string       `json:"groups,omitempty"`
	Meta      map[string]any `json:"meta,omitempty"`
	// PayloadLen is the byte length of the payload (not the decoded content).
	// DebugPayload level adds DecodedPayload.
	PayloadLen     int `json:"payload_len,omitempty"`
	DecodedPayload any `json:"payload,omitempty"`
}

// Format formats an Event as a human-readable string (default inspector output).
func (e Event) Format() string {
	ts := e.Time.Format("15:04:05.000")
	switch e.Type {
	case "req":
		return fmt.Sprintf("%s → req   %-26s %-20s group=%-8s dispatch=%-6s hop=%d",
			ts, shortID(e.CorrID), e.Cap, e.Group, e.Dispatch, e.Hop)
	case "res":
		return fmt.Sprintf("%s ← res   %-26s %-20s worker=%-8s latency=%dms hop=%d",
			ts, shortID(e.CorrID), e.Cap, e.WorkerID, e.LatencyMs, e.Hop)
	case "err":
		return fmt.Sprintf("%s ← err   %-26s %-20s %s",
			ts, shortID(e.CorrID), e.Cap, e.ErrCode)
	case "pipe":
		return fmt.Sprintf("%s ⇢ pipe  %-26s %-20s worker=%-8s hop=%d",
			ts, shortID(e.CorrID), e.Cap, e.WorkerID, e.Hop)
	case "res_chunk":
		return fmt.Sprintf("%s ~ chunk %-26s %-20s worker=%-8s",
			ts, shortID(e.CorrID), e.Cap, e.WorkerID)
	case "cancel":
		return fmt.Sprintf("%s ✗ cancel %-26s (ctx cancelled)", ts, shortID(e.OriginID))
	case "hb_ping":
		return fmt.Sprintf("%s   hb    %-8s load=%d groups=%v runtime=%s",
			ts, e.WorkerID, e.Load, e.Groups, e.Runtime)
	case "cb_req":
		return fmt.Sprintf("%s ↩ cb    %-26s %-20s worker=%-8s depth=%d",
			ts, shortID(e.CorrID), e.Cap, e.WorkerID, e.CbDepth)
	default:
		return fmt.Sprintf("%s   %-8s %-26s %s", ts, e.Type, shortID(e.CorrID), e.Cap)
	}
}

func shortID(id string) string {
	if len(id) > 10 {
		return id[:10]
	}
	return id
}
