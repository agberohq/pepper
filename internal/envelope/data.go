package envelope

const (
	KeyMsgType    = "msg_type"
	KeyCorrID     = "corr_id"
	KeyOriginID   = "origin_id"
	KeyPayload    = "payload"
	KeyWorkerID   = "worker_id"
	KeyCap        = "cap"
	KeyCapVer     = "cap_ver"
	KeyHop        = "hop"
	KeyCode       = "code"
	KeyMessage    = "message"
	KeyMeta       = "meta"
	KeyDeadlineMs = "deadline_ms"
	KeyGroups     = "groups"
	KeyBlobs      = "_blobs"
)

type Data map[string]any

func (e Data) MsgType() string   { v, _ := e[KeyMsgType].(string); return v }
func (e Data) CorrID() string    { v, _ := e[KeyCorrID].(string); return v }
func (e Data) OriginID() string  { v, _ := e[KeyOriginID].(string); return v }
func (e Data) Payload() []byte   { v, _ := e[KeyPayload].([]byte); return v }
func (e Data) WorkerID() string  { v, _ := e[KeyWorkerID].(string); return v }
func (e Data) Cap() string       { v, _ := e[KeyCap].(string); return v }
func (e Data) CapVer() string    { v, _ := e[KeyCapVer].(string); return v }
func (e Data) Code() string      { v, _ := e[KeyCode].(string); return v }
func (e Data) Message() string   { v, _ := e[KeyMessage].(string); return v }
func (e Data) DeadlineMs() int64 { v, _ := e[KeyDeadlineMs].(float64); return int64(v) }
func (e Data) Hop() uint8        { return toUint8(e[KeyHop]) }

// Meta returns the envelope's meta map.
// msgpack decodes nested dicts as map[any]any, not map[string]any, so we
// normalise both variants here to avoid silent type-assertion failures.
func (e Data) Meta() map[string]any {
	v := e[KeyMeta]
	if m, ok := v.(map[string]any); ok {
		return m
	}
	if m, ok := v.(map[any]any); ok {
		out := make(map[string]any, len(m))
		for k, val := range m {
			if ks, ok := k.(string); ok {
				out[ks] = val
			}
		}
		return out
	}
	return nil
}

func (e Data) Groups() []string { return toStringSlice(e[KeyGroups]) }
func (e Data) Blobs() []string {
	m := e.Meta()
	if m == nil {
		return nil
	}
	return toStringSlice(m[KeyBlobs])
}
func (e Data) ForwardTo() string { v, _ := e["forward_to"].(string); return v }
func (e Data) GatherAt() string  { v, _ := e["gather_at"].(string); return v }
func (e Data) Int64(key string) int64 {
	switch val := e[key].(type) {
	case int64:
		return val
	case float64:
		return int64(val)
	case uint64:
		return int64(val)
	}
	return 0
}

func toUint8(v any) uint8 {
	switch val := v.(type) {
	case uint8:
		return val
	case int64:
		return uint8(val)
	case float64:
		return uint8(val)
	}
	return 0
}

func toStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
