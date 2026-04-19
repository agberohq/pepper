// The wire layer always uses map[string]any (In). This file adds two
// compile-time-safe free functions on top:
//
//	// map layer — always works, matches Python side
//	res, err := pp.Do(ctx, "face.recognize", pepper.In{"image": blob})
//	matches := res.AsJSON()["matches"]
//
//	// typed layer — decode at call site, zero extra wire overhead
//	type FaceResult struct{ Matches []Match `msgpack:"matches"` }
//	result, err := pepper.Do[FaceResult](ctx, pp, "face.recognize", pepper.In{"image": blob})
//	// result is FaceResult directly
//
// Go Workers implement the Worker interface below. The goruntime package
// wraps them so the bus always sees map[string]any — generics only exist
// at the user-facing API boundary.
package pepper

import (
	"context"
	"fmt"
	"sync"

	"github.com/agberohq/pepper/internal/core"
)

// Do executes a capability and decodes the result into O.
// Input stays as map[string]any (In) — only the output is typed.
// This is the idiomatic typed call path; pp.Do returns a raw Result.
//
//	type FaceResult struct{ Matches []Match `msgpack:"matches"` }
//	result, err := pepper.Do[FaceResult](ctx, pp, "face.recognize", pepper.In{"image": blob})
func Do[O any](ctx context.Context, pp *Pepper, cap string, in core.In, opts ...CallOption) (O, error) {
	var zero O
	res, err := pp.Do(ctx, cap, in, opts...)
	if err != nil {
		return zero, err
	}
	var out O
	if err := res.Into(&out); err != nil {
		return zero, fmt.Errorf("pepper.Do[%T]: decode: %w", zero, err)
	}
	return out, nil
}

// All executes multiple calls in parallel and decodes each result into O.
// Returns all results and the first non-nil error encountered.
//
//	results, err := pepper.All[FaceResult](ctx, pp,
//	    pepper.MakeCall("face.recognize", pepper.In{"image": img1}),
//	    pepper.MakeCall("face.recognize", pepper.In{"image": img2}),
//	)
func All[O any](ctx context.Context, pp *Pepper, calls ...Call) ([]O, error) {
	out := make([]O, len(calls))
	errs := make([]error, len(calls))

	var wg sync.WaitGroup
	wg.Add(len(calls))

	for i, c := range calls {
		i, c := i, c
		go func() {
			defer wg.Done()
			out[i], errs[i] = Do[O](ctx, pp, c.Cap, c.In, c.Opts...)
		}()
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return out, err
		}
	}
	return out, nil
}
