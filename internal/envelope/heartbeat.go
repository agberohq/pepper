package envelope

// HbPingMap builds the heartbeat envelope published on pepper.hb.{workerID}.
//
// The in-process runtimes (Go functions, HTTP/MCP adapters, CLI tools) all emit
// this message, and the router, the inspector and the script runtimes all read
// it. Keeping one builder means the field names can never drift apart.
func HbPingMap(workerID, runtimeKind string, load uint8, groups []string, requestsServed uint64, uptimeMs int64) map[string]any {
	return map[string]any{
		KeyProtoVer:       ProtoVer,
		KeyMsgType:        string(MsgHbPing),
		KeyWorkerID:       workerID,
		KeyRuntime:        runtimeKind,
		KeyLoad:           load,
		KeyGroups:         groups,
		KeyRequestsServed: requestsServed,
		KeyUptimeMs:       uptimeMs,
	}
}
