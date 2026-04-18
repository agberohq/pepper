package metrics

import (
	"testing"
)

// Sink interface compliance

func TestNoopSinkImplementsSink(t *testing.T) {
	var _ Sink = NoopSink{}
}

func TestPrometheusSinkImplementsSink(t *testing.T) {
	var _ Sink = (*PrometheusSink)(nil)
}

func TestInMemorySinkImplementsSink(t *testing.T) {
	var _ Sink = (*InMemorySink)(nil)
}

func TestMultiImplementsSink(t *testing.T) {
	var _ Sink = (*Multi)(nil)
}

// NoopSink

func TestNoopSinkDoesNotPanic(t *testing.T) {
	s := Noop()
	s.Counter("x", 1, nil)
	s.Gauge("y", 1.0, nil)
	s.Histogram("z", 1.0, nil)
}

func TestNoop(t *testing.T) {
	s := Noop()
	if s == nil {
		t.Error("Noop() should not return nil")
	}
}

// InMemorySink

func TestInMemoryCounter(t *testing.T) {
	s := NewInMemory()
	s.Counter("requests.total", 1, map[string]string{"cap": "echo"})
	s.Counter("requests.total", 2, map[string]string{"cap": "echo"})
	got := s.GetCounter("requests.total", map[string]string{"cap": "echo"})
	if got != 3 {
		t.Errorf("Counter = %d, want 3", got)
	}
}

func TestInMemoryCounterNoTags(t *testing.T) {
	s := NewInMemory()
	s.Counter("my.counter", 5, nil)
	if got := s.GetCounter("my.counter", nil); got != 5 {
		t.Errorf("Counter = %d, want 5", got)
	}
}

func TestInMemoryCounterDifferentTags(t *testing.T) {
	s := NewInMemory()
	s.Counter("req", 1, map[string]string{"status": "ok"})
	s.Counter("req", 1, map[string]string{"status": "err"})
	ok := s.GetCounter("req", map[string]string{"status": "ok"})
	err := s.GetCounter("req", map[string]string{"status": "err"})
	if ok != 1 || err != 1 {
		t.Errorf("ok=%d err=%d, want both 1", ok, err)
	}
}

func TestInMemoryGauge(t *testing.T) {
	s := NewInMemory()
	s.Gauge("workers.count", 4.0, map[string]string{"group": "gpu"})
	s.Gauge("workers.count", 2.0, map[string]string{"group": "gpu"}) // overwrites
	got := s.GetGauge("workers.count", map[string]string{"group": "gpu"})
	if got != 2.0 {
		t.Errorf("Gauge = %f, want 2.0", got)
	}
}

func TestInMemoryGaugeMissingKey(t *testing.T) {
	s := NewInMemory()
	got := s.GetGauge("nonexistent", nil)
	if got != 0.0 {
		t.Errorf("missing gauge = %f, want 0.0", got)
	}
}

func TestInMemoryHistogram(t *testing.T) {
	s := NewInMemory()
	s.Histogram("latency_ms", 10.0, nil)
	s.Histogram("latency_ms", 20.0, nil)
	s.Histogram("latency_ms", 30.0, nil)
	// No direct accessor for histogram values — just verify no panic and counter increments work
}

func TestInMemoryMissingCounter(t *testing.T) {
	s := NewInMemory()
	if got := s.GetCounter("missing", nil); got != 0 {
		t.Errorf("missing counter = %d, want 0", got)
	}
}

// Multi sink

func TestMultiCounter(t *testing.T) {
	s1 := NewInMemory()
	s2 := NewInMemory()
	m := NewMulti(s1, s2)

	m.Counter("test", 7, nil)
	if s1.GetCounter("test", nil) != 7 {
		t.Errorf("s1 counter = %d, want 7", s1.GetCounter("test", nil))
	}
	if s2.GetCounter("test", nil) != 7 {
		t.Errorf("s2 counter = %d, want 7", s2.GetCounter("test", nil))
	}
}

func TestMultiGauge(t *testing.T) {
	s1 := NewInMemory()
	s2 := NewInMemory()
	m := NewMulti(s1, s2)

	m.Gauge("load", 42.0, nil)
	if s1.GetGauge("load", nil) != 42.0 {
		t.Errorf("s1 gauge = %f", s1.GetGauge("load", nil))
	}
	if s2.GetGauge("load", nil) != 42.0 {
		t.Errorf("s2 gauge = %f", s2.GetGauge("load", nil))
	}
}

func TestMultiHistogram(t *testing.T) {
	s1 := NewInMemory()
	s2 := NewInMemory()
	m := NewMulti(s1, s2)
	m.Histogram("latency", 5.0, nil)
	// No panic = pass
}

func TestMultiEmpty(t *testing.T) {
	m := NewMulti()
	m.Counter("x", 1, nil)
	m.Gauge("y", 1.0, nil)
	m.Histogram("z", 1.0, nil)
}

func TestMultiWithNoop(t *testing.T) {
	s := NewInMemory()
	m := NewMulti(s, Noop())
	m.Counter("req", 3, nil)
	if s.GetCounter("req", nil) != 3 {
		t.Errorf("counter = %d, want 3", s.GetCounter("req", nil))
	}
}

// Prometheus stub

func TestPrometheusReturnsNoop(t *testing.T) {
	s := Prometheus()
	if s == nil {
		t.Error("Prometheus() should not return nil")
	}
	// Should not panic
	s.Counter("x", 1, nil)
	s.Gauge("y", 1.0, nil)
	s.Histogram("z", 1.0, nil)
}

// Metric name constants

func TestMetricNameConstants(t *testing.T) {
	cases := []struct{ name, want string }{
		{MetricRequestsTotal, "pepper.requests.total"},
		{MetricWorkersCount, "pepper.workers.count"},
		{MetricStreamsActive, "pepper.streams.active"},
		{MetricBlobsActive, "pepper.blobs.active"},
		{MetricPipelineLatencyMs, "pepper.pipeline.latency_ms"},
		{MetricCallbacksTotal, "pepper.callbacks.total"},
		{MetricTransportBytesSent, "pepper.transport.bytes_sent"},
	}
	for _, tc := range cases {
		if tc.name != tc.want {
			t.Errorf("metric name = %q, want %q", tc.name, tc.want)
		}
	}
}

// metricKey

func TestMetricKeyNoTags(t *testing.T) {
	k := metricKey("pepper.requests.total", nil)
	if k != "pepper.requests.total" {
		t.Errorf("key = %q", k)
	}
}

func TestMetricKeyWithTags(t *testing.T) {
	k := metricKey("pepper.requests.total", map[string]string{"cap": "echo"})
	if k == "pepper.requests.total" {
		t.Error("key with tags should differ from key without tags")
	}
	if len(k) < len("pepper.requests.total") {
		t.Error("key with tags should be longer")
	}
}

func TestMetricKeySameTagsSameKey(t *testing.T) {
	k1 := metricKey("m", map[string]string{"a": "1"})
	k2 := metricKey("m", map[string]string{"a": "1"})
	if k1 != k2 {
		t.Errorf("same tags should produce same key: %q vs %q", k1, k2)
	}
}
