// Package pepper — LLM tool format converters.
// Converts CapabilitySchema slices into tool arrays for OpenAI, Anthropic, etc.
// Feed these directly into LLM API calls to enable tool use / function calling.
package pepper

import "github.com/agberohq/pepper/internal/registry"

// ToOpenAITools converts capability schemas to OpenAI function-calling tool format.
//
//	schemas := pp.Capabilities(ctx, pepper.FilterByGroup("tools"))
//	tools := pepper.ToOpenAITools(schemas)
//	// pass tools to openai.ChatCompletion
func ToOpenAITools(schemas []registry.Schema) []map[string]any {
	out := make([]map[string]any, 0, len(schemas))
	for _, s := range schemas {
		schema := s.InputSchema
		if schema == nil {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		out = append(out, map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        s.Name,
				"description": descriptionFromSchema(s),
				"parameters":  schema,
			},
		})
	}
	return out
}

// ToAnthropicTools converts capability schemas to Anthropic tool use format.
//
//	schemas := pp.Capabilities(ctx, pepper.FilterByGroup("tools"))
//	tools := pepper.ToAnthropicTools(schemas)
//	// pass tools to anthropic.Messages
func ToAnthropicTools(schemas []registry.Schema) []map[string]any {
	out := make([]map[string]any, 0, len(schemas))
	for _, s := range schemas {
		schema := s.InputSchema
		if schema == nil {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		out = append(out, map[string]any{
			"name":         s.Name,
			"description":  descriptionFromSchema(s),
			"input_schema": schema,
		})
	}
	return out
}

// ToGenericTools returns a simple name+description+schema slice
// for use with any LLM API that accepts a similar format.
func ToGenericTools(schemas []registry.Schema) []map[string]any {
	out := make([]map[string]any, 0, len(schemas))
	for _, s := range schemas {
		out = append(out, map[string]any{
			"name":        s.Name,
			"version":     s.Version,
			"groups":      s.Groups,
			"runtime":     s.Runtime,
			"description": descriptionFromSchema(s),
			"parameters":  s.InputSchema,
			"returns":     s.OutputSchema,
		})
	}
	return out
}

// FilterByGroup returns a CapFilter matching capabilities in any of the given groups.
var FilterByGroup = registry.FilterByGroup

// FilterByRuntime returns a CapFilter matching a specific runtime.
func FilterByRuntime(rt string) registry.Filter {
	return registry.FilterByRuntime(registry.Runtime(rt))
}

func descriptionFromSchema(s registry.Schema) string {
	if s.InputSchema == nil {
		return s.Name
	}
	if desc, ok := s.InputSchema["description"].(string); ok && desc != "" {
		return desc
	}
	return s.Name + " (pepper capability, runtime: " + s.Runtime + ")"
}
