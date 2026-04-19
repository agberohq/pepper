package metrics

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// PrometheusSink implements Sink using atomic counters and exposes a
// Prometheus text-format /metrics handler via ServeHTTP.
//
// No external dependency is required. To expose metrics:
//
//	sink := metrics.Prometheus()
//	http.Handle("/metrics", sink.(*PrometheusSink))
//	go http.ListenAndServe(":9090", nil)
type PrometheusSink struct {
	counters   sync.Map // key → *atomic.Int64
	gauges     sync.Map // key → *gaugeVal
	histograms sync.Map // key → *histVal
}

type gaugeVal struct {
	mu  sync.Mutex
	val float64
}

type histVal struct {
	mu      sync.Mutex
	count   int64
	sum     float64
	buckets []histBucket // sorted by upper bound
}

type histBucket struct {
	le    float64
	count int64
}

var defaultBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

func (s *PrometheusSink) Counter(name string, val int64, tags map[string]string) {
	key := promMetricKey(name, tags)
	v, _ := s.counters.LoadOrStore(key, new(atomic.Int64))
	v.(*atomic.Int64).Add(val)
}

func (s *PrometheusSink) Gauge(name string, val float64, tags map[string]string) {
	key := promMetricKey(name, tags)
	v, _ := s.gauges.LoadOrStore(key, &gaugeVal{})
	g := v.(*gaugeVal)
	g.mu.Lock()
	g.val = val
	g.mu.Unlock()
}

func (s *PrometheusSink) Histogram(name string, val float64, tags map[string]string) {
	key := promMetricKey(name, tags)
	v, _ := s.histograms.LoadOrStore(key, &histVal{
		buckets: makeBuckets(defaultBuckets),
	})
	h := v.(*histVal)
	h.mu.Lock()
	h.count++
	h.sum += val
	for i := range h.buckets {
		if val <= h.buckets[i].le {
			h.buckets[i].count++
		}
	}
	h.mu.Unlock()
}

// ServeHTTP writes Prometheus text format to the response.
func (s *PrometheusSink) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	var sb strings.Builder

	s.counters.Range(func(k, v any) bool {
		name, labels := parseKey(k.(string))
		pname := promName(name)
		fmt.Fprintf(&sb, "# TYPE %s counter\n", pname)
		fmt.Fprintf(&sb, "%s%s %d\n", pname, labels, v.(*atomic.Int64).Load())
		return true
	})
	s.gauges.Range(func(k, v any) bool {
		name, labels := parseKey(k.(string))
		pname := promName(name)
		g := v.(*gaugeVal)
		g.mu.Lock()
		val := g.val
		g.mu.Unlock()
		fmt.Fprintf(&sb, "# TYPE %s gauge\n", pname)
		fmt.Fprintf(&sb, "%s%s %g\n", pname, labels, val)
		return true
	})
	s.histograms.Range(func(k, v any) bool {
		name, labels := parseKey(k.(string))
		pname := promName(name)
		h := v.(*histVal)
		h.mu.Lock()
		count := h.count
		sum := h.sum
		buckets := make([]histBucket, len(h.buckets))
		copy(buckets, h.buckets)
		h.mu.Unlock()
		labelInfix := strings.TrimSuffix(labels, "}")
		if labelInfix == "{" || labelInfix == "" {
			labelInfix = ""
		}
		fmt.Fprintf(&sb, "# TYPE %s histogram\n", pname)
		for _, b := range buckets {
			le := fmt.Sprintf("%g", b.le)
			if math.IsInf(b.le, 1) {
				le = "+Inf"
			}
			if labelInfix == "" {
				fmt.Fprintf(&sb, "%s_bucket{le=%q} %d\n", pname, le, b.count)
			} else {
				fmt.Fprintf(&sb, "%s_bucket%s,le=%q} %d\n", pname, labelInfix, le, b.count)
			}
		}
		fmt.Fprintf(&sb, "%s_sum%s %g\n", pname, labels, sum)
		fmt.Fprintf(&sb, "%s_count%s %d\n", pname, labels, count)
		return true
	})

	fmt.Fprint(w, sb.String())
}

// Prometheus returns a PrometheusSink. Register its ServeHTTP for /metrics:
//
//	sink := metrics.Prometheus()
//	http.Handle("/metrics", sink.(*metrics.PrometheusSink))
func Prometheus() Sink { return &PrometheusSink{} }

// helpers

func promMetricKey(name string, tags map[string]string) string {
	if len(tags) == 0 {
		return name
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString(name)
	sb.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(tags[k])
	}
	sb.WriteByte('}')
	return sb.String()
}

func parseKey(key string) (name, labels string) {
	idx := strings.IndexByte(key, '{')
	if idx < 0 {
		return key, ""
	}
	return key[:idx], key[idx:]
}

func promName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

func makeBuckets(ubs []float64) []histBucket {
	bs := make([]histBucket, len(ubs)+1)
	for i, ub := range ubs {
		bs[i] = histBucket{le: ub}
	}
	bs[len(ubs)] = histBucket{le: math.Inf(1)}
	return bs
}
