// Package registry maps capability names to their specifications.
// The registry is built at startup and is read-only at runtime.
// All workers receive cap_load messages derived from the registry.
package registry

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

// Registry holds all registered capabilities.
// Thread-safe for concurrent reads after startup.
type Registry struct {
	mu   sync.RWMutex
	caps map[string]*Spec
}

// New returns an empty Registry.
func New() *Registry {
	return &Registry{caps: make(map[string]*Spec)}
}

// Add registers a capability. Returns error if already registered.
func (r *Registry) Add(spec *Spec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.caps[spec.Name]; exists {
		return fmt.Errorf("pepper/registry: capability %q already registered", spec.Name)
	}
	r.caps[spec.Name] = spec
	return nil
}

// Get returns the Spec for a capability name, or nil if not found.
func (r *Registry) Get(name string) *Spec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.caps[name]
}

// All returns all registered capabilities.
func (r *Registry) All() []*Spec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Spec, 0, len(r.caps))
	for _, v := range r.caps {
		out = append(out, v)
	}
	return out
}

// ForGroup returns all capabilities assigned to a specific group.
func (r *Registry) ForGroup(group string) []*Spec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*Spec
	for _, spec := range r.caps {
		for _, g := range spec.Groups {
			if g == group {
				out = append(out, spec)
				break
			}
		}
	}
	return out
}

// ForRuntime returns all capabilities using a specific runtime.
func (r *Registry) ForRuntime(rt Runtime) []*Spec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*Spec
	for _, spec := range r.caps {
		if spec.Runtime == rt {
			out = append(out, spec)
		}
	}
	return out
}

// Schema returns the Schema for a registered capability.
func (r *Registry) Schema(name string) (*Schema, error) {
	spec := r.Get(name)
	if spec == nil {
		return nil, fmt.Errorf("pepper/registry: capability %q not found", name)
	}
	return &Schema{
		Name:         spec.Name,
		Version:      spec.Version,
		Groups:       spec.Groups,
		Runtime:      string(spec.Runtime),
		InputSchema:  spec.InputSchema,
		OutputSchema: spec.OutputSchema,
	}, nil
}

// Schemas returns CapabilitySchemas for all capabilities matching all filters.
func (r *Registry) Schemas(filters ...Filter) []Schema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Schema
outer:
	for _, spec := range r.caps {
		for _, f := range filters {
			if !f(spec) {
				continue outer
			}
		}
		out = append(out, Schema{
			Name:         spec.Name,
			Version:      spec.Version,
			Groups:       spec.Groups,
			Runtime:      string(spec.Runtime),
			InputSchema:  spec.InputSchema,
			OutputSchema: spec.OutputSchema,
		})
	}
	return out
}

// dirNameToCap converts a file path like "./caps/face/recognize.py"
// to a capability name like "face.recognize".
func DirNameToCap(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	dir := filepath.Dir(path)
	parts := strings.Split(filepath.ToSlash(dir), "/")
	// drop leading dots and "caps" prefix if present
	var keep []string
	for _, p := range parts {
		if p == "" || p == "." || p == ".." || p == "caps" {
			continue
		}
		keep = append(keep, p)
	}
	keep = append(keep, name)
	return strings.Join(keep, ".")
}

// FilterByGroup returns a filter that matches capabilities in any of the given groups.
func FilterByGroup(groups ...string) Filter {
	set := make(map[string]bool, len(groups))
	for _, g := range groups {
		set[g] = true
	}
	return func(spec *Spec) bool {
		for _, g := range spec.Groups {
			if set[g] {
				return true
			}
		}
		return false
	}
}

// FilterByRuntime returns a filter that matches capabilities using a specific runtime.
func FilterByRuntime(rt Runtime) Filter {
	return func(spec *Spec) bool { return spec.Runtime == rt }
}
