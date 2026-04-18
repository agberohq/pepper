package hooks

import (
	"context"

	"github.com/agberohq/pepper/internal/envelope"
)

// Chain holds an ordered list of hooks for a scope.
// Scopes can be global (all capabilities), per-group, or per-capability.
type Chain struct {
	hooks []Hook
}

// Add appends a hook to the chain.
func (c *Chain) Add(h Hook) *Chain {
	c.hooks = append(c.hooks, h)
	return c
}

// Before appends a before-only hook.
func (c *Chain) Before(name string, fn BeforeFunc) *Chain {
	return c.Add(Hook{Name: name, Before: fn})
}

// After appends an after-only hook.
func (c *Chain) After(name string, fn AfterFunc) *Chain {
	return c.Add(Hook{Name: name, After: fn})
}

// RunBefore executes all Before hooks in order.
// Returns the (possibly mutated) inputs, or an error to abort the capability.
func (c *Chain) RunBefore(ctx context.Context, env *envelope.Envelope, in In) (In, error) {
	current := in
	for _, h := range c.hooks {
		if h.Before == nil {
			continue
		}
		mutated, err := h.Before(ctx, env, current)
		if err != nil {
			return nil, err
		}
		if mutated != nil {
			current = mutated
		}
	}
	return current, nil
}

// RunAfter executes all After hooks in reverse order (middleware unwind).
// Returns the (possibly mutated) result.
func (c *Chain) RunAfter(ctx context.Context, env *envelope.Envelope, in In, result Result) (Result, error) {
	current := result
	for i := len(c.hooks) - 1; i >= 0; i-- {
		h := c.hooks[i]
		if h.After == nil {
			continue
		}
		mutated, err := h.After(ctx, env, in, current)
		if err != nil {
			current.Err = err
			continue
		}
		current = mutated
	}
	return current, current.Err
}

// Len returns the number of hooks in the chain.
func (c *Chain) Len() int { return len(c.hooks) }
