// Package hooks implements the Pepper before/after hook system.
//
// Hooks are middleware-style interceptors that run around every capability
// invocation. They can inspect, mutate, short-circuit, or augment any
// request or response without touching capability code.
//
// Common uses:
//   - Auth/authz: reject requests missing valid tokens
//   - Rate limiting: throttle per-capability or per-group
//   - Input mutation: inject fields, normalise encoding, attach session data
//   - Output mutation: redact PII, add watermarks, transform response shape
//   - Observability: log payloads, emit custom metrics, add trace spans
//   - Caching: return cached results before the capability runs
//   - Auditing: record every invocation with full inputs
//
// Hooks execute in registration order. Before hooks run sequentially before
// the capability. After hooks run sequentially after the capability (in reverse
// registration order, like middleware unwind).
package hooks

import (
	"context"

	"github.com/agberohq/pepper/internal/envelope"
)

// In is a capability's input map.
type In = map[string]any

// Result is the result returned by a capability.
type Result struct {
	Payload  []byte
	WorkerID string
	Cap      string
	CapVer   string
	Hop      uint8
	Meta     map[string]any
	Err      error
}

// BeforeFunc is called before the capability executes.
// It receives the full envelope and the decoded input map.
//
// Return values:
//   - modified In: the (possibly mutated) inputs forwarded to the capability.
//     Return nil to pass the original inputs unchanged.
//   - error: if non-nil, the capability is NOT called. The error is returned
//     directly to the Go caller. Use this for auth failures, rate limit, etc.
//
// To short-circuit with a custom result (e.g. cache hit), return a non-nil
// Result from a BeforeFunc by wrapping it in a ShortCircuit error:
//
//	return nil, hooks.ShortCircuit(cachedResult)
type BeforeFunc func(ctx context.Context, env *envelope.Envelope, in In) (In, error)

// AfterFunc is called after the capability executes (whether it succeeded or failed).
// It receives the full envelope, the original inputs, and the result.
//
// Return values:
//   - modified Result: the (possibly mutated) result forwarded to the caller.
//     Return nil to pass the original result unchanged.
//   - error: if non-nil, this error replaces the result's error.
//
// AfterFuncs run even when the capability returned an error. Check result.Err
// to distinguish success from failure.
type AfterFunc func(ctx context.Context, env *envelope.Envelope, in In, result Result) (Result, error)

// Hook is a named before+after pair. Either function may be nil.
type Hook struct {
	Name   string
	Before BeforeFunc
	After  AfterFunc
}

// shortCircuitErr wraps a Result so BeforeFunc can return a cache hit
// without calling the capability.
type shortCircuitErr struct{ result Result }

func (e shortCircuitErr) Error() string { return "pepper: short-circuit" }

// ShortCircuit returns an error that, when returned from a BeforeFunc,
// causes the capability to be skipped and the wrapped Result to be returned
// to the caller instead.
//
// Example — cache hook:
//
//	hooks.Before("cache", func(ctx context.Context, env *envelope.Envelope, in In) (In, error) {
//	    if cached, ok := cache.Get(cacheKey(env, in)); ok {
//	        return nil, hooks.ShortCircuit(cached)
//	    }
//	    return nil, nil  // cache miss — proceed normally
//	})
func ShortCircuit(r Result) error { return shortCircuitErr{result: r} }

// IsShortCircuit reports whether err is a ShortCircuit signal.
func IsShortCircuit(err error) bool {
	_, ok := err.(shortCircuitErr)
	return ok
}

// ShortCircuitResult extracts the Result from a ShortCircuit error.
// Returns zero Result and false if err is not a ShortCircuit.
func ShortCircuitResult(err error) (Result, bool) {
	if sc, ok := err.(shortCircuitErr); ok {
		return sc.result, true
	}
	return Result{}, false
}

// BuiltIn hooks provided by Pepper out of the box.
// These are opt-in — none are registered by default.

// Logger returns a hook pair that logs every invocation at Debug level.
// Use DebugPayload=true to also log the decoded payload (never in production).
func Logger(logger interface{ Debug(msg string, args ...any) }, debugPayload bool) Hook {
	return Hook{
		Name: "pepper.logger",
		Before: func(ctx context.Context, env *envelope.Envelope, in In) (In, error) {
			args := []any{
				"corr_id", env.CorrID,
				"cap", env.Cap,
				"group", env.Group,
				"hop", env.Hop,
				"delivery", env.DeliveryCount,
			}
			if debugPayload {
				args = append(args, "in", in)
			}
			logger.Debug("pepper: before", args...)
			return nil, nil
		},
		After: func(ctx context.Context, env *envelope.Envelope, in In, result Result) (Result, error) {
			args := []any{
				"corr_id", env.CorrID,
				"cap", env.Cap,
				"worker", result.WorkerID,
				"hop", result.Hop,
				"err", result.Err,
			}
			if debugPayload {
				args = append(args, "payload_len", len(result.Payload))
			}
			logger.Debug("pepper: after", args...)
			return result, nil
		},
	}
}

// MetaInject returns a before hook that merges additional metadata into
// the envelope's Meta map before dispatch. Useful for injecting auth tokens,
// request IDs, or tenant context without modifying the capability input.
func MetaInject(kv map[string]any) Hook {
	return Hook{
		Name: "pepper.meta_inject",
		Before: func(ctx context.Context, env *envelope.Envelope, in In) (In, error) {
			if env.Meta == nil {
				env.Meta = make(map[string]any)
			}
			for k, v := range kv {
				env.Meta[k] = v
			}
			return nil, nil
		},
	}
}

// InputTransform returns a before hook that applies fn to the inputs before
// the capability runs. fn receives the current inputs and returns modified inputs.
// Return nil to pass inputs unchanged.
func InputTransform(name string, fn func(In) In) Hook {
	return Hook{
		Name: name,
		Before: func(ctx context.Context, env *envelope.Envelope, in In) (In, error) {
			if mutated := fn(in); mutated != nil {
				return mutated, nil
			}
			return nil, nil
		},
	}
}

// OutputTransform returns an after hook that applies fn to the result payload.
func OutputTransform(name string, fn func(Result) Result) Hook {
	return Hook{
		Name: name,
		After: func(ctx context.Context, env *envelope.Envelope, in In, result Result) (Result, error) {
			return fn(result), nil
		},
	}
}
