// Package metrics implements the Pepper MetricsSink interface and adapters.
//
// Pepper does not depend on any specific metrics backend. The MetricsSink
// interface is the only contract. Adapters for Prometheus, OpenTelemetry,
// and Datadog are provided. The default is NoopSink (zero overhead).
//
// Usage:
//
//	pepper.New(
//	    pepper.Metrics(metrics.Prometheus()),    // exposes /metrics
//	    pepper.Metrics(metrics.Noop()),          // default — zero overhead
//	)
package metrics

// Sink is the pluggable metrics interface.
// All methods must be safe for concurrent use.
type Sink interface {
	Counter(name string, val int64, tags map[string]string)
	Gauge(name string, val float64, tags map[string]string)
	Histogram(name string, val float64, tags map[string]string)
}

const (
	// Request metrics
	MetricRequestsTotal       = "pepper.requests.total"
	MetricRequestsInflight    = "pepper.requests.inflight"
	MetricRequestsLatencyMs   = "pepper.requests.latency_ms"
	MetricRequestsWorkerLatMs = "pepper.requests.worker_latency_ms"
	MetricRequestsQueueDepth  = "pepper.requests.queue_depth"
	MetricRequestsRetries     = "pepper.requests.retries"
	MetricRequestsHops        = "pepper.requests.hops"
	MetricRequestsDelivery    = "pepper.requests.delivery_count"

	// Worker metrics
	MetricWorkersCount    = "pepper.workers.count"
	MetricWorkersCrashes  = "pepper.workers.crashes"
	MetricWorkersRespawns = "pepper.workers.respawns"
	MetricWorkersLoad     = "pepper.workers.load"
	MetricWorkersServed   = "pepper.workers.requests_served"
	MetricWorkersRecycled = "pepper.workers.recycled"

	// Streaming
	MetricStreamsActive = "pepper.streams.active"
	MetricStreamsChunks = "pepper.streams.chunks_total"
	MetricStreamsBytes  = "pepper.streams.bytes_total"

	// Blobs
	MetricBlobsActive      = "pepper.blobs.active"
	MetricBlobsBytesActive = "pepper.blobs.bytes_active"
	MetricBlobsCreated     = "pepper.blobs.created_total"
	MetricBlobsLeaked      = "pepper.blobs.leaked_total"

	// Pipelines
	MetricPipelineLatencyMs  = "pepper.pipeline.latency_ms"
	MetricPipelineStageLatMs = "pepper.pipeline.stage_latency_ms"
	MetricPipelineErrors     = "pepper.pipeline.errors"

	// Groups
	MetricGroupSize       = "pepper.group.size"
	MetricGroupDispatch   = "pepper.group.dispatch_total"
	MetricGroupQuorumFail = "pepper.group.quorum_failures"

	// Transport
	MetricTransportBytesSent = "pepper.transport.bytes_sent"
	MetricTransportBytesRecv = "pepper.transport.bytes_recv"
	MetricTransportDropped   = "pepper.transport.messages_dropped"
	MetricTransportFrameErr  = "pepper.transport.frame_errors"

	// Callbacks
	MetricCallbacksTotal     = "pepper.callbacks.total"
	MetricCallbacksLatencyMs = "pepper.callbacks.latency_ms"
	MetricCallbacksErrors    = "pepper.callbacks.errors"
)

// Ensure all implement Sink.
var _ Sink = NoopSink{}
var _ Sink = (*InMemorySink)(nil)
var _ Sink = (*PrometheusSink)(nil)
var _ Sink = (*Multi)(nil)
