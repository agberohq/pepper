// These methods are defined in a separate file to keep adapter.go focused on
// the HTTP execution logic. They implement the BuildSpec(name) method so that
// *HTTPBuilder and *MCPBuilder satisfy pepper.AdapterBuilder without
// importing the pepper package (which would create a cycle).
package adapter

import (
	"time"

	"github.com/agberohq/pepper/internal/registry"
)

// BuildSpec implements pepper.AdapterBuilder for HTTPBuilder.
// It stores the full builder state in spec.AdapterSpec so the adapter
// runner can retrieve it later without re-parsing.
func (b *HTTPBuilder) BuildSpec(name string) *registry.Spec {
	groups := b.groups
	if len(groups) == 0 {
		groups = []string{"default"}
	}
	timeout := b.timeout
	if timeout == 0 {
		timeout = 120 * time.Second
	}
	return &registry.Spec{
		Name:        name,
		Runtime:     registry.RuntimeHTTP,
		Version:     "0.0.0",
		Source:      b.baseURL,
		Groups:      groups,
		AdapterSpec: b, // store the whole builder for the runner
	}
}

// BuildSpec implements pepper.AdapterBuilder for MCPBuilder.
func (b *MCPBuilder) BuildSpec(name string) *registry.Spec {
	groups := b.groups
	if len(groups) == 0 {
		groups = []string{"default"}
	}
	return &registry.Spec{
		Name:        name,
		Runtime:     registry.RuntimeHTTP,
		Version:     "0.0.0",
		Source:      b.serverURL,
		Groups:      groups,
		AdapterSpec: b,
	}
}
