package pepper

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/hooks"
	"github.com/agberohq/pepper/internal/pending"
	"github.com/agberohq/pepper/internal/registry"
)

// buildPythonSpec constructs a Spec for a Python capability.
// source may be a file path string or a *CapBuilder.
func buildPythonSpec(name string, source any, opts ...CapOption) (*registry.Spec, error) {
	spec := &registry.Spec{
		Name:    name,
		Runtime: registry.RuntimePython,
		Version: "0.0.0",
	}
	switch v := source.(type) {
	case string:
		spec.Source = v
	case *CapBuilder:
		// CapBuilder carries source + chained options
		spec.Source = v.source
		for _, o := range v.options() {
			o(spec)
		}
	default:
		return nil, fmt.Errorf("source must be a file path string or Cap(...) builder, got %T", source)
	}
	for _, o := range opts {
		o(spec)
	}
	return spec, nil
}

// buildGoSpec constructs a Spec for a Go native worker.
func buildGoSpec(name string, worker core.Worker, opts ...CapOption) (*registry.Spec, error) {
	spec := &registry.Spec{
		Name:     name,
		Runtime:  registry.RuntimeGo,
		GoWorker: worker,
		Version:  "0.0.0",
	}
	for _, o := range opts {
		o(spec)
	}
	return spec, nil
}

// buildAdapterSpec constructs a Spec for an HTTP/MCP adapter.
// The builder's BuildSpec() is called to populate AdapterSpec.
func buildAdapterSpec(name string, adapter AdapterBuilder, opts ...CapOption) (*registry.Spec, error) {
	// Let the builder produce a full spec with its internal state stored in AdapterSpec
	spec := adapter.BuildSpec(name)
	if spec == nil {
		spec = &registry.Spec{Name: name, Runtime: registry.RuntimeHTTP, Version: "0.0.0"}
	}
	for _, o := range opts {
		o(spec)
	}
	return spec, nil
}

// buildCLISpec constructs a Spec for a CLI tool capability.
// The builder's BuildSpec() is called to populate CLISpec.
func buildCLISpec(name string, cmd CMDBuilder, opts ...CapOption) (*registry.Spec, error) {
	spec := cmd.BuildSpec(name)
	if spec == nil {
		spec = &registry.Spec{Name: name, Runtime: registry.RuntimeCLI, Version: "0.0.0"}
	}
	for _, o := range opts {
		o(spec)
	}
	return spec, nil
}

// walkPythonDir walks dir and calls fn for every .py file found.
// Skips __pycache__, hidden directories, and non-.py files.
func walkPythonDir(dir string, fn func(string) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("RegisterDir %q: %w", dir, err)
	}
	for _, e := range entries {
		name := e.Name()
		// Skip hidden and cache directories
		if strings.HasPrefix(name, ".") || name == "__pycache__" {
			continue
		}
		fullPath := filepath.Join(dir, name)
		if e.IsDir() {
			if err := walkPythonDir(fullPath, fn); err != nil {
				return err
			}
			continue
		}
		if strings.HasSuffix(name, ".py") && name != "runtime.py" {
			if err := fn(fullPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func buildCapLoad(spec *registry.Spec) map[string]any {
	source := spec.Source
	// Ensure Python receives an absolute path so it can reliably load the file
	// regardless of the current working directory.
	if source != "" && spec.Runtime == registry.RuntimePython {
		if abs, err := filepath.Abs(source); err == nil {
			source = abs
		}
	}
	return map[string]any{
		"proto_ver":      uint8(1),
		"msg_type":       "cap_load",
		"cap":            spec.Name,
		"cap_ver":        spec.Version,
		"source":         source,
		"deps":           spec.Deps,
		"timeout_ms":     spec.Timeout.Milliseconds(),
		"max_concurrent": spec.MaxConcurrent,
		"groups":         spec.Groups,
		"config":         spec.Config,
	}
}
func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 7731
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}
func sanitizeName(name string) string {
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
func groupsOverlap(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}
func responseToResult(resp pending.Response, c codec.Codec, latency time.Duration) Result {
	return Result{payload: resp.Payload, codec: c, WorkerID: resp.WorkerID, Cap: resp.Cap, CapVer: resp.CapVer, Hop: resp.Hop, Latency: latency, Meta: resp.Meta, Err: resp.Err}
}
func toResult(hr hooks.Result, c codec.Codec) Result {
	return Result{payload: hr.Payload, codec: c, WorkerID: hr.WorkerID, Cap: hr.Cap, CapVer: hr.CapVer, Hop: hr.Hop, Meta: hr.Meta, Err: hr.Err}
}
func buildEnvelope(corrID, originID, cap string, in core.In, o callOpts, defaultTimeout time.Duration) envelope.Envelope {
	env := envelope.DefaultEnvelope()
	env.CorrID = corrID
	env.OriginID = originID
	env.Cap = cap
	env.Group = o.group
	if env.Group == "" {
		env.Group = "default" // empty group routes to the default worker pool
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
		deadlineMs = time.Now().Add(defaultTimeout).UnixMilli()
	}
	env.DeadlineMs = deadlineMs
	return env
}
func buildPipelineSpec(name string, dag any) (*registry.Spec, error) {
	return &registry.Spec{Name: name, Runtime: registry.RuntimePipeline, Pipeline: dag, Version: "0.0.0"}, nil
}
