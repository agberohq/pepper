package metrics

import "sync"

// InMemorySink stores metrics in memory. Useful for testing and debugging.
type InMemorySink struct {
	mu         sync.RWMutex
	counters   map[string]int64
	gauges     map[string]float64
	histograms map[string][]float64
}

// NewInMemory creates an in-memory metrics sink.
func NewInMemory() *InMemorySink {
	return &InMemorySink{
		counters:   make(map[string]int64),
		gauges:     make(map[string]float64),
		histograms: make(map[string][]float64),
	}
}

func metricKey(name string, tags map[string]string) string {
	if len(tags) == 0 {
		return name
	}
	key := name
	for k, v := range tags {
		key += "," + k + "=" + v
	}
	return key
}

func (s *InMemorySink) Counter(name string, val int64, tags map[string]string) {
	s.mu.Lock()
	s.counters[metricKey(name, tags)] += val
	s.mu.Unlock()
}

func (s *InMemorySink) Gauge(name string, val float64, tags map[string]string) {
	s.mu.Lock()
	s.gauges[metricKey(name, tags)] = val
	s.mu.Unlock()
}

func (s *InMemorySink) Histogram(name string, val float64, tags map[string]string) {
	s.mu.Lock()
	key := metricKey(name, tags)
	s.histograms[key] = append(s.histograms[key], val)
	s.mu.Unlock()
}

func (s *InMemorySink) GetCounter(name string, tags map[string]string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.counters[metricKey(name, tags)]
}

func (s *InMemorySink) GetGauge(name string, tags map[string]string) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gauges[metricKey(name, tags)]
}
