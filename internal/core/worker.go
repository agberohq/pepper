package core

import (
	"context"
)

// Worker is the interface Go native workers must implement.
// All methods must be safe for concurrent use.
// See runtime/goruntime for the runtime wrapper that connects Worker to the bus.
type Worker interface {
	// Setup is called once at boot per capability. Load models, open pools.
	// Blocking — worker does not accept requests until Setup returns.
	Setup(cap string, config map[string]any) error

	// Run executes one capability invocation.
	// ctx is cancelled when deadline passes or Go caller cancels.
	// Must be goroutine-safe — called concurrently up to MaxConcurrent.
	Run(ctx context.Context, cap string, in In) (In, error)

	// Capabilities returns metadata for capabilities this worker provides.
	Capabilities() []Capability
}

// WorkerExtension is implemented by Workers that support extended features.
// The runtime checks for this interface at registration time.
type WorkerExtension interface {
	// Teardown is called on worker_bye before the goroutine exits.
	Teardown(cap string) error
	// Stream returns a channel of output chunks for streaming capabilities.
	Stream(ctx context.Context, cap string, in In) (<-chan In, error)
}
