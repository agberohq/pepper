package pepper

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/hooks"
	"github.com/agberohq/pepper/internal/pending"
	"github.com/agberohq/pepper/internal/registry"
	"github.com/goccy/go-json"
)

// toWireInput converts any input to the wire map[string]any format.
//   - map[string]any / core.In — passed through unchanged
//   - typed struct — JSON round-trip respecting json tags
//   - nil — empty map
func toWireInput(input any) (core.In, error) {
	if input == nil {
		return core.In{}, nil
	}
	if m, ok := input.(map[string]any); ok {
		return m, nil
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("encode input: %w", err)
	}
	var result core.In
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode input to map: %w", err)
	}
	return result, nil
}

func responseToResult(resp pending.Response, c codec.Codec, latency time.Duration) Result {
	return Result{
		payload: resp.Payload, codec: c,
		WorkerID: resp.WorkerID, Cap: resp.Cap, CapVer: resp.CapVer,
		Hop: resp.Hop, Latency: latency, Meta: resp.Meta, Err: resp.Err,
	}
}

func toResult(hr hooks.Result, c codec.Codec) Result {
	return Result{
		payload: hr.Payload, codec: c,
		WorkerID: hr.WorkerID, Cap: hr.Cap, CapVer: hr.CapVer,
		Hop: hr.Hop, Meta: hr.Meta, Err: hr.Err,
	}
}

func (p *Pepper) buildEnvelope(corrID, originID, cap string, in core.In, o callOpts) envelope.Envelope {
	env := envelope.DefaultEnvelope()
	env.CorrID = corrID
	env.OriginID = originID
	env.Cap = cap
	env.Group = o.group
	if env.Group == "" {
		env.Group = "default"
	}
	env.Dispatch = envelope.Dispatch(o.dispatch)
	env.Quorum = o.quorum
	env.CapVer = o.capVer
	env.WorkerID = o.workerID
	env.SessionID = o.sessionID
	env.MaxHops = o.maxHops
	env.MaxCbDepth = o.maxCbDepth
	env.ReplyTo = "pepper.res." + originID
	deadlineMs := o.deadlineMs
	if deadlineMs == 0 {
		deadlineMs = time.Now().Add(p.cfg.DefaultTimeout).UnixMilli()
	}
	env.DeadlineMs = deadlineMs
	for k, v := range o.meta {
		if env.Meta == nil {
			env.Meta = make(map[string]any, len(o.meta))
		}
		env.Meta[k] = v
	}
	return env
}

func (p *Pepper) defaultCallOpts() callOpts {
	return callOpts{
		dispatch:   string(DispatchAny),
		maxHops:    p.cfg.MaxHops,
		maxCbDepth: p.cfg.MaxCbDepth,
	}
}

func (p *Pepper) freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 7731
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func (p *Pepper) sanitizeName(name string) string {
	out := make([]byte, len(name))
	for i, c := range name {
		if c == '.' {
			out[i] = '_'
		} else {
			out[i] = byte(c)
		}
	}
	return string(out)
}

func (p *Pepper) groupsOverlap(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}

func (p *Pepper) buildCapLoad(spec *registry.Spec) envelope.CapLoad {
	source := spec.Source
	if source != "" && spec.Runtime == registry.RuntimePython {
		if abs, err := filepath.Abs(source); err == nil {
			source = abs
		}
	}
	mc := spec.MaxConcurrent
	if mc <= 0 {
		mc = DefaultMaxConcurrent
	}
	return envelope.CapLoad{
		ProtoVer:      envelope.ProtoVer,
		MsgType:       envelope.MsgCapLoad,
		Cap:           spec.Name,
		CapVer:        spec.Version,
		Source:        source,
		Deps:          spec.Deps,
		TimeoutMs:     spec.Timeout.Milliseconds(),
		MaxConcurrent: mc,
		Groups:        spec.Groups,
		Config:        spec.Config,
	}
}
