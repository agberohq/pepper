package pepper

import (
	"time"

	"github.com/agberohq/pepper/internal/registry"
)

// AdapterBuilder is implemented by any builder that can produce a registry.Spec
// for an HTTP, MCP, or other external service adapter.
// Concrete implementations: adapter.HTTPBuilder, adapter.MCPBuilder.
type AdapterBuilder interface {
	// BuildSpec returns the completed registry.Spec for this adapter.
	BuildSpec(name string) *registry.Spec
}

// CMDBuilder is implemented by any builder that can produce a registry.Spec
// for a CLI tool capability.
// Concrete implementation: cli.Builder.
type CMDBuilder interface {
	// BuildSpec returns the completed registry.Spec for this CLI capability.
	BuildSpec(name string) *registry.Spec
}

// CapOption modifies a registry.Spec during registration.
// Use the Cap() builder to construct options, or the individual option
// functions (Version, Deps, Groups, Timeout, Config).
type CapOption func(*registry.Spec)

// Cap creates a Python capability option builder for chained configuration.
//
//	pp.Register("speech.transcribe",
//	    pepper.Cap("./caps/speech/transcribe.py").
//	        Version("1.0.0").
//	        Groups("gpu", "asr").
//	        Config(map[string]any{"model_size": "small"}),
//	)
func Cap(source string) *CapBuilder {
	return &CapBuilder{source: source}
}

// CapBuilder builds a set of CapOptions with method chaining.
type CapBuilder struct {
	source string
	opts   []CapOption
}

// Version sets the capability semver. Workers announce this in cap_ready.
func (b *CapBuilder) Version(v string) *CapBuilder {
	b.opts = append(b.opts, func(s *registry.Spec) { s.Version = v })
	return b
}

// Deps sets pip dependencies installed before setup() is called.
func (b *CapBuilder) Deps(deps ...string) *CapBuilder {
	b.opts = append(b.opts, func(s *registry.Spec) { s.Deps = deps })
	return b
}

// Groups sets the worker groups this capability is dispatched to.
func (b *CapBuilder) Groups(groups ...string) *CapBuilder {
	b.opts = append(b.opts, func(s *registry.Spec) { s.Groups = groups })
	return b
}

// Timeout sets the per-request timeout for this capability.
func (b *CapBuilder) Timeout(d time.Duration) *CapBuilder {
	b.opts = append(b.opts, func(s *registry.Spec) { s.Timeout = d })
	return b
}

// Config sets the config dict passed to setup().
func (b *CapBuilder) Config(cfg map[string]any) *CapBuilder {
	b.opts = append(b.opts, func(s *registry.Spec) { s.Config = cfg })
	return b
}

// options returns the accumulated CapOptions plus the source option.
func (b *CapBuilder) options() []CapOption {
	all := make([]CapOption, 0, len(b.opts)+1)
	src := b.source
	all = append(all, func(s *registry.Spec) { s.Source = src })
	all = append(all, b.opts...)
	return all
}

// Standalone CapOption constructors

// Version sets the capability semver as a standalone CapOption.
func Version(v string) CapOption {
	return func(s *registry.Spec) { s.Version = v }
}

// Groups sets the worker groups as a standalone CapOption.
func Groups(groups ...string) CapOption {
	return func(s *registry.Spec) { s.Groups = groups }
}

// WithConfig sets the worker config dict as a standalone CapOption.
func WithConfig(cfg map[string]any) CapOption {
	return func(s *registry.Spec) { s.Config = cfg }
}
