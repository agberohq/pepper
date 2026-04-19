package core

// In is the map-based capability input used at the wire level.
// All internal routing, encoding, and Python/CLI workers use this type.
// For type-safe Go calls use pepper.Do[O] or pepper.All[O].
type In = map[string]any

// Capability describes one capability exported by a Go Worker.
type Capability struct {
	Name          string
	Version       string
	Groups        []string
	MaxConcurrent int
}
