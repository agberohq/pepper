package hooks

import (
	"context"

	"github.com/agberohq/pepper/internal/envelope"
)

// Registry holds hook chains per scope.
// Scopes resolve in order: capability-specific → group → global.
// All matching chains are merged and run.
type Registry struct {
	global map[string]*Chain // keyed by "" (always matches)
	groups map[string]*Chain // keyed by group name
	caps   map[string]*Chain // keyed by capability name
}

// NewRegistry creates an empty hook registry.
func NewRegistry() *Registry {
	return &Registry{
		global: map[string]*Chain{"": {}},
		groups: make(map[string]*Chain),
		caps:   make(map[string]*Chain),
	}
}

// Global returns the global hook chain (runs for every capability).
func (r *Registry) Global() *Chain { return r.global[""] }

// ForGroup returns the hook chain for a specific group, creating it if needed.
func (r *Registry) ForGroup(group string) *Chain {
	if _, ok := r.groups[group]; !ok {
		r.groups[group] = &Chain{}
	}
	return r.groups[group]
}

// ForCap returns the hook chain for a specific capability, creating it if needed.
func (r *Registry) ForCap(cap string) *Chain {
	if _, ok := r.caps[cap]; !ok {
		r.caps[cap] = &Chain{}
	}
	return r.caps[cap]
}

// Resolve returns all chains that apply to a given cap+group pair,
// in the correct execution order: global → group → cap.
func (r *Registry) Resolve(cap, group string) []*Chain {
	var chains []*Chain
	if g := r.global[""]; g.Len() > 0 {
		chains = append(chains, g)
	}
	if group != "" {
		if gc, ok := r.groups[group]; ok && gc.Len() > 0 {
			chains = append(chains, gc)
		}
	}
	if cap != "" {
		if cc, ok := r.caps[cap]; ok && cc.Len() > 0 {
			chains = append(chains, cc)
		}
	}
	return chains
}

// RunBefore executes all resolved before hooks for a cap+group pair.
func (r *Registry) RunBefore(ctx context.Context, env *envelope.Envelope, in In) (In, error) {
	chains := r.Resolve(env.Cap, env.Group)
	current := in
	for _, chain := range chains {
		mutated, err := chain.RunBefore(ctx, env, current)
		if err != nil {
			return nil, err
		}
		if mutated != nil {
			current = mutated
		}
	}
	return current, nil
}

// RunAfter executes all resolved after hooks for a cap+group pair (reverse order).
func (r *Registry) RunAfter(ctx context.Context, env *envelope.Envelope, in In, result Result) (Result, error) {
	chains := r.Resolve(env.Cap, env.Group)
	current := result
	// Run after hooks in reverse resolution order (cap → group → global)
	for i := len(chains) - 1; i >= 0; i-- {
		mutated, err := chains[i].RunAfter(ctx, env, in, current)
		if err != nil {
			current.Err = err
		} else {
			current = mutated
		}
	}
	return current, current.Err
}
