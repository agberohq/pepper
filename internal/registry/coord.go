package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/agberohq/pepper/internal/coord"
)

// coordSpec is the serialisable subset of Spec stored in coord.
// GoWorker, AdapterSpec, CLISpec, and Pipeline cannot be serialised —
// those are always local to the node that registered them.
type coordSpec struct {
	Name         string         `json:"name"`
	Version      string         `json:"version"`
	Runtime      string         `json:"runtime"`
	Source       string         `json:"source"`
	Groups       []string       `json:"groups"`
	InputSchema  map[string]any `json:"input_schema,omitempty"`
	OutputSchema map[string]any `json:"output_schema,omitempty"`
}

func capKey(name string) string { return "pepper:cap:" + name }

// UseCoord attaches a coord.Store to the registry so capability registrations
// are published and remote capabilities are discoverable.
// Call once after New(), before Start().
func (r *Registry) UseCoord(c coord.Store) {
	r.mu.Lock()
	r.coord = c
	r.mu.Unlock()
}

// publishCap writes spec to coord so other nodes can discover it.
func (r *Registry) publishCap(spec *Spec) {
	if r.coord == nil {
		return
	}
	cs := coordSpec{
		Name:         spec.Name,
		Version:      spec.Version,
		Runtime:      string(spec.Runtime),
		Source:       spec.Source,
		Groups:       spec.Groups,
		InputSchema:  spec.InputSchema,
		OutputSchema: spec.OutputSchema,
	}
	data, err := json.Marshal(cs)
	if err != nil {
		return
	}
	_ = r.coord.Set(context.Background(), capKey(spec.Name), data, 0)
	_ = r.coord.Publish(context.Background(), "pepper:cap:"+spec.Name, data)
}

// RemoteCaps returns capabilities registered on other nodes (coord-only, not local).
func (r *Registry) RemoteCaps() ([]Schema, error) {
	r.mu.RLock()
	c := r.coord
	r.mu.RUnlock()
	if c == nil {
		return nil, nil
	}

	keys, err := c.List(context.Background(), "pepper:cap:")
	if err != nil {
		return nil, fmt.Errorf("registry.RemoteCaps: %w", err)
	}

	var out []Schema
	for _, k := range keys {
		name := strings.TrimPrefix(k, "pepper:cap:")
		r.mu.RLock()
		_, local := r.caps[name]
		r.mu.RUnlock()
		if local {
			continue // already in local registry
		}
		data, ok, err := c.Get(context.Background(), k)
		if err != nil || !ok {
			continue
		}
		var cs coordSpec
		if json.Unmarshal(data, &cs) != nil {
			continue
		}
		out = append(out, Schema{
			Name:         cs.Name,
			Version:      cs.Version,
			Groups:       cs.Groups,
			Runtime:      cs.Runtime,
			InputSchema:  cs.InputSchema,
			OutputSchema: cs.OutputSchema,
		})
	}
	return out, nil
}
