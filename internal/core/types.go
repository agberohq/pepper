package core

// In is the map-based capability input used at the wire level.
// All internal routing, encoding, and Python/CLI workers use this type.
// For type-safe Go calls use pepper.Do[O] or pepper.All[O].
type In = map[string]any

// MetricsSink is the pluggable metrics backend.
// All methods must be safe for concurrent use.
type MetricsSink interface {
	Counter(name string, val int64, tags map[string]string)
	Gauge(name string, val float64, tags map[string]string)
	Histogram(name string, val float64, tags map[string]string)
}

// Capability describes one capability exported by a Go Worker.
type Capability struct {
	Name          string
	Version       string
	Groups        []string
	MaxConcurrent int
}
