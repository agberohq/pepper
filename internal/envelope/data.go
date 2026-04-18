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

func (e Data) MsgType() string      { v, _ := e[KeyMsgType].(string); return v }
func (e Data) CorrID() string       { v, _ := e[KeyCorrID].(string); return v }
func (e Data) OriginID() string     { v, _ := e[KeyOriginID].(string); return v }
func (e Data) Payload() []byte      { v, _ := e[KeyPayload].([]byte); return v }
func (e Data) WorkerID() string     { v, _ := e[KeyWorkerID].(string); return v }
func (e Data) Cap() string          { v, _ := e[KeyCap].(string); return v }
func (e Data) CapVer() string       { v, _ := e[KeyCapVer].(string); return v }
func (e Data) Code() string         { v, _ := e[KeyCode].(string); return v }
func (e Data) Message() string      { v, _ := e[KeyMessage].(string); return v }
func (e Data) DeadlineMs() int64    { v, _ := e[KeyDeadlineMs].(float64); return int64(v) }
func (e Data) Hop() uint8           { return toUint8(e[KeyHop]) }
func (e Data) Meta() map[string]any { v, _ := e[KeyMeta].(map[string]any); return v }
func (e Data) Groups() []string     { return toStringSlice(e[KeyGroups]) }
func (e Data) Blobs() []string {
	m, _ := e[KeyMeta].(map[string]any)
	list, _ := m[KeyBlobs].([]any)
	out := make([]string, 0, len(list))
	for _, v := range list {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
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
