package metrics

// NoopSink is the zero-overhead no-op sink.
type NoopSink struct{}

func (NoopSink) Counter(string, int64, map[string]string)     {}
func (NoopSink) Gauge(string, float64, map[string]string)     {}
func (NoopSink) Histogram(string, float64, map[string]string) {}

// Noop returns a MetricsSink that discards all observations.
// This is the default when no sink is configured.
func Noop() Sink { return NoopSink{} }
