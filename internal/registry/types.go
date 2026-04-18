package registry

import "time"

// Runtime identifies the execution model for a capability.
type Runtime string

const (
	RuntimePython   Runtime = "python"
	RuntimeGo       Runtime = "go"
	RuntimeHTTP     Runtime = "http"
	RuntimeCLI      Runtime = "cli"
	RuntimePipeline Runtime = "pipeline"
)

// Spec is the complete specification for one registered capability.
// It is the source of truth for cap_load messages sent to workers.
type Spec struct {
	// Identity
	Name    string
	Version string

	// Execution
	Runtime  Runtime
	Source   string // .py path, HTTP URL, CLI command, or "" for Go workers
	GoWorker any    // non-nil for RuntimeGo — the Worker interface implementation

	// AdapterSpec holds the built adapter for RuntimeHTTP.
	// Stored as any to avoid import cycle; cast in the adapter runtime.
	AdapterSpec any

	// CLISpec holds the built CMDSpec for RuntimeCLI.
	// Stored as any to avoid import cycle; cast in the cli runtime.
	CLISpec any

	// Pipeline holds a *compose.DAG for RuntimePipeline.
	// Stored as any to avoid import cycle; cast in the router.
	Pipeline any

	// Dependencies (Python/CLI only)
	Deps []string

	// Timing
	Timeout       time.Duration
	MaxConcurrent int

	// Routing
	Groups []string

	// Pipeline topology (Python/CLI peer communication)
	PipePublishes  []string
	PipeSubscribes []string

	// Worker config passed to setup()
	Config map[string]any

	// Schema (populated from Pydantic model or Form B annotations)
	InputSchema  map[string]any
	OutputSchema map[string]any
}

// Schema is the public representation of a capability for introspection
// and LLM tool use (ToOpenAITools / ToAnthropicTools).
type Schema struct {
	Name         string
	Version      string
	Groups       []string
	Runtime      string
	Description  string
	InputSchema  map[string]any
	OutputSchema map[string]any
}

// Filter is a predicate for filtering capabilities during introspection.
type Filter func(*Spec) bool
