package pepper

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/agberohq/pepper/internal/registry"
)

// buildPythonSpec constructs a Spec for a Python capability.
// source may be a file path string or a *CapBuilder.
func buildPythonSpec(name string, source any, opts ...CapOption) (*registry.Spec, error) {
	spec := &registry.Spec{
		Name:    name,
		Runtime: registry.RuntimePython,
		Version: "0.0.0",
	}
	switch v := source.(type) {
	case string:
		spec.Source = v
	case *CapBuilder:
		// CapBuilder carries source + chained options
		spec.Source = v.source
		for _, o := range v.options() {
			o(spec)
		}
	default:
		return nil, fmt.Errorf("source must be a file path string or Cap(...) builder, got %T", source)
	}
	for _, o := range opts {
		o(spec)
	}
	return spec, nil
}

// buildGoSpec constructs a Spec for a Go native worker.
func buildGoSpec(name string, worker Worker, opts ...CapOption) (*registry.Spec, error) {
	spec := &registry.Spec{
		Name:     name,
		Runtime:  registry.RuntimeGo,
		GoWorker: worker,
		Version:  "0.0.0",
	}
	for _, o := range opts {
		o(spec)
	}
	return spec, nil
}

// buildAdapterSpec constructs a Spec for an HTTP/MCP adapter.
// The builder's BuildSpec() is called to populate AdapterSpec.
func buildAdapterSpec(name string, adapter AdapterBuilder, opts ...CapOption) (*registry.Spec, error) {
	// Let the builder produce a full spec with its internal state stored in AdapterSpec
	spec := adapter.BuildSpec(name)
	if spec == nil {
		spec = &registry.Spec{Name: name, Runtime: registry.RuntimeHTTP, Version: "0.0.0"}
	}
	for _, o := range opts {
		o(spec)
	}
	return spec, nil
}

// buildCLISpec constructs a Spec for a CLI tool capability.
// The builder's BuildSpec() is called to populate CLISpec.
func buildCLISpec(name string, cmd CMDBuilder, opts ...CapOption) (*registry.Spec, error) {
	spec := cmd.BuildSpec(name)
	if spec == nil {
		spec = &registry.Spec{Name: name, Runtime: registry.RuntimeCLI, Version: "0.0.0"}
	}
	for _, o := range opts {
		o(spec)
	}
	return spec, nil
}

// walkPythonDir walks dir and calls fn for every .py file found.
// Skips __pycache__, hidden directories, and non-.py files.
func walkPythonDir(dir string, fn func(string) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("RegisterDir %q: %w", dir, err)
	}
	for _, e := range entries {
		name := e.Name()
		// Skip hidden and cache directories
		if strings.HasPrefix(name, ".") || name == "__pycache__" {
			continue
		}
		fullPath := filepath.Join(dir, name)
		if e.IsDir() {
			if err := walkPythonDir(fullPath, fn); err != nil {
				return err
			}
			continue
		}
		if strings.HasSuffix(name, ".py") && name != "runtime.py" {
			if err := fn(fullPath); err != nil {
				return err
			}
		}
	}
	return nil
}
