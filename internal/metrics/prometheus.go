package metrics

// PrometheusSink is a stub until the full prometheus implementation is complete.
type PrometheusSink struct{}

func (p *PrometheusSink) Counter(name string, val int64, tags map[string]string)     {}
func (p *PrometheusSink) Gauge(name string, val float64, tags map[string]string)     {}
func (p *PrometheusSink) Histogram(name string, val float64, tags map[string]string) {}

// Prometheus returns a MetricsSink backed by the default Prometheus registry.
// Exposes /metrics via the default http.DefaultServeMux.
//
// Import side-effect: registers collectors on init.
// Callers must add prometheus/promhttp to their go.mod and start an HTTP server:
//
//	http.Handle("/metrics", promhttp.Handler())
//	go http.ListenAndServe(":9090", nil)
func Prometheus() Sink {
	// TODO: implement using github.com/prometheus/client_golang
	// Return noop until implementation is complete.
	return Noop()
}
