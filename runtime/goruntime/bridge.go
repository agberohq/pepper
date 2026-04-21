package goruntime

import (
	"context"

	"github.com/agberohq/pepper/internal/core"
)

// Bridge adapts pepper.Worker to goruntime.Worker.
// Both interfaces have identical method signatures so this is a zero-cost shim.
type Bridge struct{ w core.Worker }

func NewBridge(w core.Worker) *Bridge { return &Bridge{w: w} }

func (b *Bridge) Setup(cap string, config map[string]any) error {
	return b.w.Setup(cap, config)
}
func (b *Bridge) Run(ctx context.Context, cap string, in map[string]any) (map[string]any, error) {
	return b.w.Run(ctx, cap, in)
}
func (b *Bridge) Capabilities() []CapSpec {
	src := b.w.Capabilities()
	out := make([]CapSpec, len(src))
	for i, c := range src {
		out[i] = CapSpec{
			Name:          c.Name,
			Version:       c.Version,
			Groups:        c.Groups,
			MaxConcurrent: c.MaxConcurrent,
		}
	}
	return out
}
