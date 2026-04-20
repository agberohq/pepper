package pepper

//
// Tools[T] is the single conversion function. It takes a slice of capability
// schemas and a formatter function, and returns []T — whatever the target SDK
// expects. Built-in formatters are provided for OpenAI and Anthropic. Custom
// formatters are plain functions, no interface implementation required.
//
// Usage:
//
//	schemas := pp.Capabilities(ctx, pepper.FilterByGroup("tools"))
//
//	// OpenAI
//	tools := pepper.Tools(schemas, pepper.FormatOpenAI)
//	resp, _ := openaiClient.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
//	    Tools: tools,

//	})
//
//	// Anthropic
//	tools := pepper.Tools(schemas, pepper.FormatAnthropic)
//
//	// Custom formatter — inline or as a named var
//	tools := pepper.Tools(schemas, func(s pepper.Schema) MySDKTool {
//	    return MySDKTool{Name: s.Name, Params: s.InputSchema}
//	})

import "github.com/agberohq/pepper/internal/registry"

// Schema is the public representation of a registered capability.
// Re-exported from internal/registry so callers never need to import it directly.
type Schema = registry.Schema

// Tools converts a slice of capability schemas into []T using the provided
// formatter function. T is whatever struct the target SDK expects.
//
//	pepper.Tools(schemas, pepper.FormatOpenAI)
//	pepper.Tools(schemas, pepper.FormatAnthropic)
//	pepper.Tools(schemas, func(s pepper.Schema) MyType { ... })
func Tools[T any](schemas []Schema, format func(Schema) T) []T {
	out := make([]T, len(schemas))
	for i, s := range schemas {
		out[i] = format(s)
	}
	return out
}

// Built-in output types

// OpenAITool is the structure expected by the OpenAI Chat Completions API
// for function calling (tools array).
type OpenAITool struct {
	Type     string         `json:"type"`
	Function OpenAIFunction `json:"function"`
}

// OpenAIFunction is the function definition within an OpenAITool.
type OpenAIFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// AnthropicTool is the structure expected by the Anthropic Messages API
// for tool use.
type AnthropicTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"input_schema"`
}

// GenericTool is a provider-agnostic tool description that includes both
// input and output schemas. Useful for custom integrations or documentation.
type GenericTool struct {
	Name        string         `json:"name"`
	Version     string         `json:"version,omitempty"`
	Groups      []string       `json:"groups,omitempty"`
	Runtime     string         `json:"runtime,omitempty"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	Returns     map[string]any `json:"returns,omitempty"`
}

// Built-in formatters

// FormatOpenAI formats a capability schema as an OpenAI function-calling tool.
//
//	tools := pepper.Tools(schemas, pepper.FormatOpenAI)
var FormatOpenAI = func(s Schema) OpenAITool {
	params := s.InputSchema
	if params == nil {
		params = map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return OpenAITool{
		Type: "function",
		Function: OpenAIFunction{
			Name:        s.Name,
			Description: schemaDescription(s),
			Parameters:  params,
		},
	}
}

// FormatAnthropic formats a capability schema as an Anthropic tool-use tool.
//
//	tools := pepper.Tools(schemas, pepper.FormatAnthropic)
var FormatAnthropic = func(s Schema) AnthropicTool {
	params := s.InputSchema
	if params == nil {
		params = map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return AnthropicTool{
		Name:        s.Name,
		Description: schemaDescription(s),
		InputSchema: params,
	}
}

// FormatGeneric formats a capability schema as a provider-agnostic GenericTool
// that includes both input and output schemas.
//
//	tools := pepper.Tools(schemas, pepper.FormatGeneric)
var FormatGeneric = func(s Schema) GenericTool {
	return GenericTool{
		Name:        s.Name,
		Version:     s.Version,
		Groups:      s.Groups,
		Runtime:     string(s.Runtime),
		Description: schemaDescription(s),
		Parameters:  s.InputSchema,
		Returns:     s.OutputSchema,
	}
}

// Filtering

// FilterByGroup returns a filter matching capabilities in any of the given groups.
var FilterByGroup = registry.FilterByGroup

// FilterByRuntime returns a filter matching capabilities of a specific runtime.
// Use registry.Runtime* constants: registry.RuntimeGo, registry.RuntimePython, etc.
func FilterByRuntime(rt registry.Runtime) registry.Filter {
	return registry.FilterByRuntime(rt)
}

// Internal helpers

func schemaDescription(s Schema) string {
	if s.InputSchema != nil {
		if desc, ok := s.InputSchema["description"].(string); ok && desc != "" {
			return desc
		}
	}
	return s.Name + " (pepper capability, runtime: " + string(s.Runtime) + ")"
}
