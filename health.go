package pepper

import (
	"context"
	"time"

	"github.com/agberohq/pepper/internal/router"
)

// HealthStatus exposes runtime health data for monitoring and orchestration.
// Call Pepper.Health() to get a snapshot. Users can expose it via HTTP, gRPC,
// or any other mechanism — Pepper is a library and does not run its own server.
type HealthStatus struct {
	// Workers is the total number of registered workers.
	Workers int

	// ReadyWorkers is the number of workers in the ready state.
	ReadyWorkers int

	// DeadWorkers is the number of workers in the dead state.
	DeadWorkers int

	// OpenCircuits is the number of workers with an open circuit breaker
	// (consistently failing and temporarily skipped for routing).
	OpenCircuits int

	// InflightReqs is the number of in-flight requests waiting for a response.
	InflightReqs int

	// CoordHealthy is true when the coordination backend is reachable.
	// Always true in single-node mode (no coord).
	CoordHealthy bool

	// CoordLatency is the round-trip latency to the coordination backend.
	// Zero in single-node mode.
	CoordLatency time.Duration

	// StreamDropped is the total number of streaming chunks dropped due to
	// full buffers across all active streams.
	StreamDropped int64
}

// Health returns a snapshot of the current runtime health.
// Safe to call before Start() — returns zero values if not started.
func (p *Pepper) Health() HealthStatus {
	h := HealthStatus{}
	if p.rt == nil {
		return h
	}

	// Worker counts from the router
	r := p.rt.router
	if r != nil {
		workers := r.Workers()
		h.Workers = len(workers)
		for _, ws := range workers {
			state := ws.State.Load().(router.WorkerHealthState)
			if state == router.WorkerStateReady {
				h.ReadyWorkers++
			} else if state == router.WorkerStateDead {
				h.DeadWorkers++
			}
			if r.IsCircuitOpen(ws) {
				h.OpenCircuits++
			}
		}
	}

	// In-flight requests
	h.InflightReqs = p.pending.Len()

	// Coordination backend health
	if p.cfg.Coord != nil {
		coordHealth := p.checkCoordHealth()
		h.CoordHealthy = coordHealth.healthy
		h.CoordLatency = coordHealth.latency
	} else {
		h.CoordHealthy = true // single-node mode is always healthy
	}

	return h
}

type coordHealthResult struct {
	healthy bool
	latency time.Duration
}

func (p *Pepper) checkCoordHealth() coordHealthResult {
	if p.cfg.Coord == nil {
		return coordHealthResult{healthy: true, latency: 0}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	key := "_pepper_health_check"
	if err := p.cfg.Coord.Set(ctx, key, []byte("1"), 5); err == nil {
		_, ok, err := p.cfg.Coord.Get(ctx, key)
		_ = p.cfg.Coord.Delete(ctx, key)
		return coordHealthResult{
			healthy: ok && err == nil,
			latency: time.Since(start),
		}
	}
	return coordHealthResult{
		healthy: false,
		latency: time.Since(start),
	}
}
