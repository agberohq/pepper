package pepper

//
// Every runtime type (Go function, Python script, HTTP adapter, CLI tool,
// directory of scripts) implements the Capability interface. Users call
// pp.Register() once regardless of runtime type.
//
// Before (four methods, inconsistent):
//
//	pp.Register("echo", "./caps/echo.py")           // Python
//	pp.Include("text.upper", &UpperWorker{})         // Go
//	pp.Adapt("llm", adapter.HTTP("...").With(...))   // HTTP
//	pp.Prepare("ffmpeg", cli.CMD("ffmpeg"))          // CLI
//
// After (one method):
//
//	pp.Register(pepper.Script("echo", "./caps/echo.py"))
//	pp.Register(pepper.Func("text.upper", upperFn))
//	pp.Register(pepper.HTTP("llm", "http://...").With(pepper.Ollama))
//	pp.Register(pepper.CLI("ffmpeg", "ffmpeg", "-i", "{input}"))
//	pp.Register(pepper.Dir("./caps"))   // registers all .py files in a directory

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/internal/registry"
	"github.com/agberohq/pepper/runtime/adapter"
	"github.com/agberohq/pepper/runtime/cli"
)

// Capability is implemented by every registerable unit regardless of runtime.
// Users obtain a Capability via Func, Script, HTTP, CLI, or Dir.
type Capability interface {
	// name returns the capability's registered name.
	name() string
	// buildSpecs returns one or more registry.Spec values.
	// Dir returns multiple; all others return exactly one.
	buildSpecs() ([]*registry.Spec, error)
}

// Use registers a Capability of any runtime type.
// This is the unified replacement for Register/Include/Adapt/Prepare.
//
//	pp.Register(pepper.Script("echo", "./caps/echo.py"))
//	pp.Register(pepper.Func("text.upper", upperFn))
//	pp.Register(pepper.HTTP("llm", "http://localhost:11434").With(pepper.Ollama))
//	pp.Register(pepper.CLI("convert", "ffmpeg"))
//	pp.Register(pepper.Dir("./caps"))
func (p *Pepper) Register(cap Capability, opts ...CapOption) error {
	specs, err := cap.buildSpecs()
	if err != nil {
		return fmt.Errorf("pepper: use %q: %w", cap.name(), err)
	}
	for _, spec := range specs {
		for _, o := range opts {
			o(spec)
		}
		if err := p.reg.Add(spec); err != nil {
			return fmt.Errorf("pepper: use %q: %w", spec.Name, err)
		}
		p.logRegistered(spec)
	}
	return nil
}

func (p *Pepper) logRegistered(spec *registry.Spec) {
	switch spec.Runtime {
	case registry.RuntimePython:
		p.logger.Fields("name", spec.Name, "source", spec.Source).Debug("registered Python capability")
	case registry.RuntimeGo:
		p.logger.Fields("name", spec.Name).Debug("registered Go capability")
	case registry.RuntimeHTTP:
		p.logger.Fields("name", spec.Name).Debug("registered HTTP/adapter capability")
	case registry.RuntimeCLI:
		p.logger.Fields("name", spec.Name).Debug("registered CLI capability")
	}
}

// Script

// scriptCap is a Python capability loaded from a .py file.
type scriptCap struct {
	capName string
	source  string
	opts    []CapOption
}

// Script creates a Python script capability.
//
//	pp.Register(pepper.Script("echo", "./caps/echo.py"))
//	pp.Register(pepper.Script("transcribe", "./caps/transcribe.py").
//	    Groups("gpu", "asr").
//	    Config(map[string]any{"model_size": "small"}))
func Script(name, path string) *scriptCap {
	return &scriptCap{capName: name, source: path}
}

func (s *scriptCap) name() string { return s.capName }

func (s *scriptCap) Groups(groups ...string) *scriptCap {
	s.opts = append(s.opts, Groups(groups...))
	return s
}

func (s *scriptCap) Version(v string) *scriptCap {
	s.opts = append(s.opts, Version(v))
	return s
}

func (s *scriptCap) Config(cfg map[string]any) *scriptCap {
	s.opts = append(s.opts, WithConfig(cfg))
	return s
}

func (s *scriptCap) Timeout(d time.Duration) *scriptCap {
	s.opts = append(s.opts, func(spec *registry.Spec) { spec.Timeout = d })
	return s
}

func (s *scriptCap) buildSpecs() ([]*registry.Spec, error) {
	spec := &registry.Spec{
		Name:    s.capName,
		Runtime: registry.RuntimePython,
		Source:  s.source,
		Version: defaultCapVersion,
	}
	for _, o := range s.opts {
		o(spec)
	}
	return []*registry.Spec{spec}, nil
}

// Dir

// dirCap registers all .py files in a directory as Python capabilities.
type dirCap struct {
	dir  string
	opts []CapOption
}

// Dir registers every .py file found under dir as a Python capability.
// The capability name is derived from the file path relative to dir
// (e.g. "audio/convert.py" → "audio.convert").
//
//	pp.Register(pepper.Dir("./caps"))
//	pp.Register(pepper.Dir("./caps").Groups("default"))
func Dir(dir string) *dirCap {
	return &dirCap{dir: dir}
}

func (d *dirCap) name() string { return d.dir }

func (d *dirCap) Groups(groups ...string) *dirCap {
	d.opts = append(d.opts, Groups(groups...))
	return d
}

func (d *dirCap) Config(cfg map[string]any) *dirCap {
	d.opts = append(d.opts, WithConfig(cfg))
	return d
}

func (d *dirCap) buildSpecs() ([]*registry.Spec, error) {
	var specs []*registry.Spec
	err := d.walkDir(d.dir, func(path string) error {
		spec := &registry.Spec{
			Name:    registry.DirNameToCap(path),
			Runtime: registry.RuntimePython,
			Source:  path,
			Version: defaultCapVersion,
		}
		for _, o := range d.opts {
			o(spec)
		}
		specs = append(specs, spec)
		return nil
	})
	return specs, err
}

func (d *dirCap) walkDir(dir string, fn func(string) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("RegisterDir %q: %w", dir, err)
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") || name == "__pycache__" {
			continue
		}
		fullPath := filepath.Join(dir, name)
		if e.IsDir() {
			if err := d.walkDir(fullPath, fn); err != nil {
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

// Func

// funcCap wraps a typed Go function as a capability.
// The framework handles map[string]any ↔ typed struct conversion.
type funcCap[In any, Out any] struct {
	capName string
	fn      func(context.Context, In) (Out, error)
	opts    []CapOption
}

// Func creates a typed Go capability from a plain function.
// The framework handles conversion between the wire map[string]any and
// the typed In/Out structs — users never see map[string]any.
//
//	type TextInput  struct { Text string `json:"text"` }
//	type TextOutput struct { Text string `json:"text"` }
//
//	pp.Register(pepper.Func("text.upper", func(ctx context.Context, in TextInput) (TextOutput, error) {
//	    return TextOutput{Text: strings.ToUpper(in.Text)}, nil
//	}))
func Func[In any, Out any](name string, fn func(context.Context, In) (Out, error), opts ...CapOption) *funcCap[In, Out] {
	return &funcCap[In, Out]{capName: name, fn: fn, opts: opts}
}

func (f *funcCap[In, Out]) name() string { return f.capName }

func (f *funcCap[In, Out]) Groups(groups ...string) *funcCap[In, Out] {
	f.opts = append(f.opts, Groups(groups...))
	return f
}

func (f *funcCap[In, Out]) Version(v string) *funcCap[In, Out] {
	f.opts = append(f.opts, Version(v))
	return f
}

func (f *funcCap[In, Out]) buildSpecs() ([]*registry.Spec, error) {
	wrapper := &typedWorkerWrapper[In, Out]{capName: f.capName, fn: f.fn}
	spec := &registry.Spec{
		Name:     f.capName,
		Runtime:  registry.RuntimeGo,
		GoWorker: wrapper,
		Version:  defaultCapVersion,
	}
	for _, o := range f.opts {
		o(spec)
	}
	return []*registry.Spec{spec}, nil
}

// typedWorkerWrapper adapts a typed func(In) (Out, error) to the internal
// core.Worker interface. This is the single conversion boundary.
type typedWorkerWrapper[In any, Out any] struct {
	capName   string
	fn        func(context.Context, In) (Out, error)
	marshal   func(any) ([]byte, error) // set at boot by bootGoWorkers via CodecSetter
	unmarshal func([]byte, any) error   // set at boot
}

func (w *typedWorkerWrapper[In, Out]) Setup(cap string, config map[string]any) error {
	return nil
}

func (w *typedWorkerWrapper[In, Out]) Run(ctx context.Context, _ string, in core.In) (core.In, error) {
	// Decode map[string]any → typed In via JSON round-trip (respects json: tags).
	// The input map has JSON-tagged keys because toWireInput uses json.Marshal.
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("pepper.Func: encode input: %w", err)
	}
	var typed In
	if err := json.Unmarshal(raw, &typed); err != nil {
		return nil, fmt.Errorf("pepper.Func: decode input into %T: %w", typed, err)
	}

	// Call the user function.
	out, err := w.fn(ctx, typed)
	if err != nil {
		return nil, err
	}

	// Encode Out → map[string]any.
	// When the wire codec is msgpack with SetCustomStructTag("msgpack"), encoding
	// Out directly via the codec produces map keys from the struct field names
	// (e.g. "Text"), which the caller's codec.Unmarshal can match back to the
	// same field. JSON tags (e.g. json:"text") are NOT used by msgpack, so the
	// naive json.Marshal→json.Unmarshal path produces lowercase keys ("text")
	// that msgpack cannot decode back into the struct field ("Text") → empty result.
	// Encoding Out via the wire codec then decoding to map preserves round-trip fidelity.
	if w.marshal != nil {
		outBytes, err := w.marshal(out)
		if err != nil {
			return nil, fmt.Errorf("pepper.Func: encode output: %w", err)
		}
		var result core.In
		if err := w.unmarshal(outBytes, &result); err != nil {
			return nil, fmt.Errorf("pepper.Func: decode output to map: %w", err)
		}
		return result, nil
	}
	// No codec set (should not happen in production) — JSON fallback.
	outRaw, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("pepper.Func: encode output: %w", err)
	}
	var result core.In
	if err := json.Unmarshal(outRaw, &result); err != nil {
		return nil, fmt.Errorf("pepper.Func: decode output: %w", err)
	}
	return result, nil
}

// SetCodec injects the wire codec's marshal/unmarshal functions so that
// typed output structs are encoded with the correct tag semantics.
// Called by bootGoWorkers immediately after the wrapper is created.
func (w *typedWorkerWrapper[In, Out]) SetCodec(
	marshal func(any) ([]byte, error),
	unmarshal func([]byte, any) error,
) {
	w.marshal = marshal
	w.unmarshal = unmarshal
}

func (w *typedWorkerWrapper[In, Out]) Capabilities() []core.Capability {
	return []core.Capability{{Name: w.capName, Version: defaultCapVersion}}
}

// HTTP

// httpCap wraps an HTTP adapter builder as a Capability.
type httpCap struct {
	capName string
	builder *adapter.HTTPBuilder
}

// HTTP creates an HTTP adapter capability.
//
//	pp.Register(pepper.HTTP("llm", "http://localhost:11434").With(pepper.Ollama))
//	pp.Register(pepper.HTTP("weather", "https://api.weather.com").
//	    Auth(pepper.BearerToken(key)).
//	    Timeout(10*time.Second))
func HTTP(name, baseURL string) *httpCap {
	return &httpCap{capName: name, builder: adapter.HTTP(baseURL)}
}

func (h *httpCap) name() string { return h.capName }

func (h *httpCap) With(a adapter.Adapter) *httpCap {
	h.builder = h.builder.With(a)
	return h
}

func (h *httpCap) Auth(auth adapter.AuthProvider) *httpCap {
	h.builder = h.builder.Auth(auth)
	return h
}

func (h *httpCap) Timeout(d time.Duration) *httpCap {
	h.builder = h.builder.Timeout(d)
	return h
}

func (h *httpCap) Groups(groups ...string) *httpCap {
	h.builder = h.builder.Groups(groups...)
	return h
}

func (h *httpCap) MapRequest(fn func(map[string]any) (*adapter.Request, error)) *httpCap {
	h.builder = h.builder.MapRequest(fn)
	return h
}

func (h *httpCap) MapResponse(fn func(*adapter.Response) (map[string]any, error)) *httpCap {
	h.builder = h.builder.MapResponse(fn)
	return h
}

func (h *httpCap) buildSpecs() ([]*registry.Spec, error) {
	spec := h.builder.BuildSpec(h.capName)
	if spec == nil {
		spec = &registry.Spec{Name: h.capName, Runtime: registry.RuntimeHTTP, Version: defaultCapVersion}
	}
	return []*registry.Spec{spec}, nil
}

// MCP creates an MCP adapter capability.
//
//	pp.Register(pepper.MCP("tools", "https://mcp.example.com/sse"))
func MCP(name, serverURL string) *mcpCap {
	return &mcpCap{capName: name, builder: adapter.MCP(serverURL)}
}

type mcpCap struct {
	capName string
	builder *adapter.MCPBuilder
}

func (m *mcpCap) name() string { return m.capName }

func (m *mcpCap) Groups(groups ...string) *mcpCap {
	m.builder = m.builder.Groups(groups...)
	return m
}

func (m *mcpCap) buildSpecs() ([]*registry.Spec, error) {
	spec := m.builder.BuildSpec(m.capName)
	if spec == nil {
		spec = &registry.Spec{Name: m.capName, Runtime: registry.RuntimeHTTP, Version: defaultCapVersion}
	}
	return []*registry.Spec{spec}, nil
}

// CLI

// cliCap wraps a CLI builder as a Capability.
type cliCap struct {
	capName string
	builder *cli.Builder
	opts    []CapOption
}

// CLI creates a CLI tool capability.
//
//	pp.Register(pepper.CLI("convert", "ffmpeg", "-i", "{input}", "{output}"))
func CLI(name, command string, args ...string) *cliCap {
	return &cliCap{capName: name, builder: cli.CMD(command, args...)}
}

func (c *cliCap) name() string { return c.capName }

func (c *cliCap) Groups(groups ...string) *cliCap {
	c.builder = c.builder.Groups(groups...)
	return c
}

func (c *cliCap) Timeout(d time.Duration) *cliCap {
	// cli.Builder has no Timeout method; store it as a CapOption applied at buildSpecs time.
	c.opts = append(c.opts, func(spec *registry.Spec) { spec.Timeout = d })
	return c
}

func (c *cliCap) buildSpecs() ([]*registry.Spec, error) {
	spec := c.builder.BuildSpec(c.capName)
	if spec == nil {
		spec = &registry.Spec{Name: c.capName, Runtime: registry.RuntimeCLI, Version: defaultCapVersion}
	}
	for _, o := range c.opts {
		o(spec)
	}
	return []*registry.Spec{spec}, nil
}
