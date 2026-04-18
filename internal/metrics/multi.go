package metrics

// Multi fans metrics out to multiple sinks simultaneously.
type Multi struct {
	sinks []Sink
}

// NewMulti returns a Sink that forwards to all provided sinks.
func NewMulti(sinks ...Sink) Sink {
	return &Multi{sinks: sinks}
}

func (m *Multi) Counter(name string, val int64, tags map[string]string) {
	for _, s := range m.sinks {
		s.Counter(name, val, tags)
	}
}

func (m *Multi) Gauge(name string, val float64, tags map[string]string) {
	for _, s := range m.sinks {
		s.Gauge(name, val, tags)
	}
}

func (m *Multi) Histogram(name string, val float64, tags map[string]string) {
	for _, s := range m.sinks {
		s.Histogram(name, val, tags)
	}
}
