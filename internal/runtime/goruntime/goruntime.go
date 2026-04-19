// Package goruntime implements the Go native worker runtime.
//
// Go workers run as goroutines within the router process — no subprocess,
// no socket overhead. They connect via the bus interface.
//
// Register a Go worker:
//
//	pp.Include("face.detect", &PigoWorker{},
//	    pepper.Worker("w-pigo").Groups("cpu", "fast"),
//	)
//
// Implement the Worker interface:
//
//	type PigoWorker struct{ model *pigo.Pigo }
//	func (w *PigoWorker) Setup(cap string, config map[string]any) error { ... }
//	func (w *PigoWorker) Run(ctx context.Context, cap string, in pepper.In) (pepper.In, error) { ... }
//	func (w *PigoWorker) Capabilities() []pepper.CapSpec { ... }
package goruntime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/runtime/adapter"
	"github.com/olekukonko/ll"
)

// Worker is the interface Go native workers must implement.
// All methods must be safe for concurrent use.
type Worker interface {
	Setup(cap string, config map[string]any) error
	Run(ctx context.Context, cap string, in map[string]any) (map[string]any, error)
	Capabilities() []CapSpec
}

// CapSpec describes one capability for Go workers.
type CapSpec struct {
	Name          string
	Version       string
	Groups        []string
	MaxConcurrent int
}

// OptionalWorker is implemented by workers that support extended features.
type OptionalWorker interface {
	Teardown(cap string) error
	Stream(ctx context.Context, cap string, in map[string]any) (<-chan map[string]any, error)
}

// GoWorkerRuntime wraps a Worker, manages its lifecycle, and connects it to the bus.
type GoWorkerRuntime struct {
	id     string
	worker Worker
	caps   []CapSpec
	groups []string

	// semaphores per capability for MaxConcurrent enforcement
	semaphores map[string]chan struct{}

	requestsServed atomic.Uint64
	startedAt      time.Time
	load           atomic.Uint32 // 0–100

	bus    bus.Bus
	codec  codec.Codec
	logger *ll.Logger

	// per-request cancel functions for context cancellation propagation
	cancelFuncs sync.Map // corrID → context.CancelFunc
	stopped     atomic.Bool
}

// New creates a GoWorkerRuntime.
func New(id string, worker Worker, groups []string, b bus.Bus, c codec.Codec, logger *ll.Logger) *GoWorkerRuntime {
	caps := worker.Capabilities()
	sems := make(map[string]chan struct{}, len(caps))
	for _, spec := range caps {
		mc := spec.MaxConcurrent
		if mc <= 0 {
			mc = 8
		}
		sems[spec.Name] = make(chan struct{}, mc)
	}
	return &GoWorkerRuntime{
		id:         id,
		worker:     worker,
		caps:       caps,
		groups:     groups,
		semaphores: sems,
		bus:        b,
		codec:      c,
		logger:     logger.Namespace("goworker:" + id),
		startedAt:  time.Now(),
	}
}

// Start calls Setup on all capabilities then begins serving requests.
func (r *GoWorkerRuntime) Start(ctx context.Context, config map[string]any) error {
	for _, spec := range r.caps {
		cfg := config
		if cfg == nil {
			cfg = map[string]any{}
		}
		if err := r.worker.Setup(spec.Name, cfg); err != nil {
			return fmt.Errorf("goruntime %s: setup(%q): %w", r.id, spec.Name, err)
		}
		r.logger.Fields("cap", spec.Name).Info("setup complete")
	}

	// Subscribe to work channels for each group
	for _, g := range r.groups {
		// DispatchAny — subscribe to push topic
		pushCh, err := r.bus.Subscribe(ctx, bus.TopicPush(g))
		if err != nil {
			return fmt.Errorf("goruntime %s: subscribe push %q: %w", r.id, bus.TopicPush(g), err)
		}
		go r.serveMessages(ctx, pushCh)

		// Fan-out dispatch modes — PUB/SUB
		subCh, err := r.bus.Subscribe(ctx, bus.TopicPub(g))
		if err != nil {
			return fmt.Errorf("goruntime %s: subscribe %q: %w", r.id, bus.TopicPub(g), err)
		}
		go r.serveMessages(ctx, subCh)
	}

	// Broadcast channel (cancel, worker_bye)
	bcastCh, err := r.bus.Subscribe(ctx, bus.TopicBroadcast)
	if err != nil {
		return fmt.Errorf("goruntime %s: subscribe broadcast: %w", r.id, err)
	}
	go r.serveBroadcast(ctx, bcastCh)

	// Per-worker direct push topic — receives requests pinned to this worker by the router.
	directCh, err := r.bus.Subscribe(ctx, bus.TopicPush(r.id))
	if err != nil {
		return fmt.Errorf("goruntime %s: subscribe direct push: %w", r.id, err)
	}
	go r.serveMessages(ctx, directCh)

	go r.heartbeatLoop(ctx)

	r.logger.Fields("caps", len(r.caps), "groups", r.groups).Info("go worker ready")
	return nil
}

// Message dispatch

func (r *GoWorkerRuntime) serveMessages(ctx context.Context, ch <-chan bus.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			go r.handleMessage(ctx, msg)
		}
	}
}

func (r *GoWorkerRuntime) handleMessage(ctx context.Context, msg bus.Message) {
	if r.stopped.Load() {
		return
	}

	var env envelope.Envelope
	if err := r.codec.Unmarshal(msg.Data, &env); err != nil {
		r.logger.Fields("error", err).Warn("unmarshal envelope failed")
		return
	}

	// Check deadline
	if env.DeadlineMs > 0 && time.Now().UnixMilli() > env.DeadlineMs {
		r.sendError(env, envelope.ErrDeadlineExceeded, "deadline exceeded before execution")
		return
	}

	// Check hop limit
	if env.Hop >= env.MaxHops {
		r.sendError(env, envelope.ErrHopLimit, fmt.Sprintf("hop %d >= max_hops %d", env.Hop, env.MaxHops))
		return
	}

	// Decode payload
	var in map[string]any
	if err := r.codec.Unmarshal(env.Payload, &in); err != nil {
		r.sendError(env, envelope.ErrPayloadInvalid, fmt.Sprintf("decode payload: %v", err))
		return
	}
	if in == nil {
		in = map[string]any{}
	}

	// Resolve any BlobRef values to raw bytes so Go workers never handle
	// local shm paths directly — same guarantee as the adapter layer.
	resolved, err := adapter.ResolveInputs(in)
	if err != nil {
		r.sendError(env, envelope.ErrPayloadInvalid, fmt.Sprintf("resolve blob inputs: %v", err))
		return
	}
	in = resolved

	// Create per-request context with deadline
	reqCtx, cancel := context.WithDeadline(ctx, time.UnixMilli(env.DeadlineMs))
	r.cancelFuncs.Store(env.CorrID, cancel)
	defer func() {
		cancel()
		r.cancelFuncs.Delete(env.CorrID)
	}()

	// Execute
	out, err := r.executeRun(reqCtx, env.Cap, in)
	if err != nil {
		code := envelope.ErrExecError
		if reqCtx.Err() != nil {
			code = envelope.ErrCancelled
		}
		r.sendError(env, code, err.Error())
		return
	}

	r.sendResult(env, out)
}

func (r *GoWorkerRuntime) executeRun(ctx context.Context, capName string, in map[string]any) (map[string]any, error) {
	sem, ok := r.semaphores[capName]
	if !ok {
		return nil, fmt.Errorf("unknown capability %q", capName)
	}

	// Acquire semaphore slot
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Track load
	inFlight := len(sem)
	capacity := chanCap(sem)
	r.load.Store(uint32(inFlight * 100 / capacity))

	defer func() {
		<-sem
		r.load.Store(uint32(len(sem) * 100 / capacity))
	}()

	r.requestsServed.Add(1)
	return r.worker.Run(ctx, capName, in)
}

// Response helpers

func (r *GoWorkerRuntime) sendResult(env envelope.Envelope, out map[string]any) {
	payload, err := r.codec.Marshal(out)
	if err != nil {
		r.sendError(env, envelope.ErrExecError, fmt.Sprintf("encode result: %v", err))
		return
	}

	if env.ForwardTo != "" {
		fwd := map[string]any{
			"proto_ver":  uint8(1),
			"msg_type":   "pipe",
			"corr_id":    env.CorrID,
			"origin_id":  env.OriginID,
			"worker_id":  r.id,
			"cap":        env.Cap,
			"hop":        env.Hop + 1,
			"forward_to": env.ForwardTo,
			"topic":      env.ForwardTo,
			"payload":    payload,
			"meta":       env.Meta,
		}
		data, _ := r.codec.Marshal(fwd)
		_ = r.bus.Publish(env.ForwardTo, data)
		return
	}

	resp := map[string]any{
		"proto_ver": uint8(1),
		"msg_type":  string(envelope.MsgRes),
		"corr_id":   env.CorrID,
		"origin_id": env.OriginID,
		"worker_id": r.id,
		"cap":       env.Cap,
		"cap_ver":   env.CapVer,
		"hop":       env.Hop,
		"meta":      env.Meta,
		"payload":   payload,
	}
	data, _ := r.codec.Marshal(resp)
	_ = r.bus.Publish(bus.TopicRes(env.OriginID), data)
}

func (r *GoWorkerRuntime) sendError(env envelope.Envelope, code envelope.Code, msg string) {
	errEnv := map[string]any{
		"proto_ver": uint8(1),
		"msg_type":  string(envelope.MsgErr),
		"corr_id":   env.CorrID,
		"origin_id": env.OriginID,
		"worker_id": r.id,
		"cap":       env.Cap,
		"code":      string(code),
		"message":   msg,
		"retryable": code.Retryable(),
	}
	data, _ := r.codec.Marshal(errEnv)
	if env.ForwardTo != "" {
		_ = r.bus.Publish(env.ForwardTo, data)
	} else {
		_ = r.bus.Publish(bus.TopicRes(env.OriginID), data)
	}
}

// Broadcast

func (r *GoWorkerRuntime) serveBroadcast(ctx context.Context, ch <-chan bus.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			r.handleBroadcast(msg)
		}
	}
}

func (r *GoWorkerRuntime) handleBroadcast(msg bus.Message) {
	var env map[string]any
	if err := r.codec.Unmarshal(msg.Data, &env); err != nil {
		return
	}
	msgType, _ := env["msg_type"].(string)
	switch msgType {
	case "cancel":
		originID, _ := env["origin_id"].(string)
		r.cancelFuncs.Range(func(corrID, cancel any) bool {
			// Go workers key cancelFuncs by corrID; check if this corrID belongs to originID
			// For simplicity: cancel all — the per-request ctx will propagate to Run()
			// Full implementation tracks corrID→originID mapping
			_ = corrID
			if cancelFn, ok := cancel.(context.CancelFunc); ok {
				cancelFn()
			}
			return true
		})
		r.logger.Fields("origin_id", originID).Debug("cancel received")

	case "worker_bye":
		workerID, _ := env["worker_id"].(string)
		if workerID == "" || workerID == r.id {
			r.stopped.Store(true)
			r.logger.Info("received worker_bye — draining")
		}
	}
}

// Heartbeat

func (r *GoWorkerRuntime) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sendHeartbeat()
		}
	}
}

func (r *GoWorkerRuntime) sendHeartbeat() {
	ping := map[string]any{
		"proto_ver":       uint8(1),
		"msg_type":        string(envelope.MsgHbPing),
		"worker_id":       r.id,
		"runtime":         "go",
		"load":            uint8(r.load.Load()),
		"groups":          r.groups,
		"requests_served": r.requestsServed.Load(),
		"uptime_ms":       time.Since(r.startedAt).Milliseconds(),
	}
	data, err := r.codec.Marshal(ping)
	if err != nil {
		return
	}
	_ = r.bus.Publish(bus.TopicHB(r.id), data)
}

// Stop

// Stop tears down all capabilities and stops the runtime.
func (r *GoWorkerRuntime) Stop() {
	r.stopped.Store(true)
	if opt, ok := r.worker.(OptionalWorker); ok {
		for _, spec := range r.caps {
			if err := opt.Teardown(spec.Name); err != nil {
				r.logger.Fields("cap", spec.Name, "error", err).Warn("teardown error")
			}
		}
	}
	r.logger.Info("go worker stopped")
}

// chanCap returns the capacity of a channel without shadowing the builtin.
func chanCap[T any](ch chan T) int { return cap(ch) }
