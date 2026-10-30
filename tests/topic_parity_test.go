package tests

// Cross-runtime protocol parity.
//
// Go owns the topic prefixes in internal/wire. Every other runtime hardcodes the
// same strings (a subprocess cannot import Go constants), so a rename in Go that
// misses one runtime breaks that runtime silently — requests never arrive, or
// responses land nowhere. This test fails the build when that happens.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agberohq/pepper/internal/wire"
)

// TestRuntimeTopicParity asserts that each bundled runtime still contains every
// core topic prefix that Go publishes on.
func TestRuntimeTopicParity(t *testing.T) {
	runtimes := map[string]string{
		"python": "runtime/python/runtime.py",
		"node":   "runtime/node/runtime.js",
		"julia":  "runtime/julia/runtime.jl",
		"rust":   "runtime/rust/src/wire.rs",
	}

	// Prefixes every runtime must know: it has to subscribe to control/broadcast
	// and it has to be able to publish a response or a heartbeat.
	required := map[string]string{
		"control":   wire.TopicControlPrefix,
		"broadcast": wire.TopicBroadcast,
		"push":      wire.TopicPushPrefix,
		"pub":       wire.TopicPubPrefix,
		"res":       wire.TopicResPrefix,
		"hb":        wire.TopicHBPrefix,
	}

	for name, path := range runtimes {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", path))
			if err != nil {
				t.Skipf("%s runtime not present: %v", name, err)
			}
			source := string(data)
			for concept, prefix := range required {
				if !strings.Contains(source, prefix) {
					t.Errorf("%s runtime does not mention the %q topic (%q) — Go and %s have drifted",
						name, concept, prefix, name)
				}
			}
		})
	}
}
