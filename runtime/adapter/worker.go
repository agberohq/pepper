package adapter

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/olekukonko/ll"
)

// BusWorker connects an Adapter to the Pepper message bus.
// One BusWorker is created per registered HTTP/MCP capability by bootRuntime.
type BusWorker struct {
	id      string
	capName string
	adapter Adapter
	runner  *Runner
	groups  []string
	bus     bus.Bus
	codec   codec.Codec
	logger  *ll.Logger

	requestsServed atomic.Uint64
	load           atomic.Int64
	startedAt      time.Time
	stopped        atomic.Bool
}

// NewBusWorker creates a BusWorker for an HTTP/MCP adapter capability.
func NewBusWorker(
	workerID, capName string,
	a Adapter,
	auth AuthProvider,
	baseURL string,
	timeout time.Duration,
	groups []string,
	b bus.Bus,
	c codec.Codec,
	log *ll.Logger,
) *BusWorker {

	if a == nil {
		log.Fields("cap", capName).Warn("adapter is nil — using no-op adapter")
		a = &noopAdapter{}
	}

	return &BusWorker{
		id:        workerID,
		capName:   capName,
		adapter:   a,
		runner:    NewRunner(baseURL, a, auth, timeout),
		groups:    groups,
		bus:       b,
		codec:     c,
		logger:    log.Namespace("adapter:" + workerID),
		startedAt: time.Now(),
	}
}

// Start subscribes to group bus topics and begins serving requests.
// Health-check failure is non-fatal — the external service may not be up yet.
func (w *BusWorker) Start(ctx context.Context) error {
	if err := w.adapter.HealthCheck(ctx); err != nil {
		w.logger.Fields("error", err).Warn("health check failed at start — will retry on first request")
	}
	for _, g := range w.groups {
		pushCh, err := w.bus.Subscribe(ctx, bus.TopicPush(g))
		if err != nil {
			return fmt.Errorf("adapter.BusWorker %s: subscribe push %q: %w", w.id, g, err)
		}
		go w.serveLoop(ctx, pushCh)

		pubCh, err := w.bus.Subscribe(ctx, bus.TopicPub(g))
		if err != nil {
			return fmt.Errorf("adapter.BusWorker %s: subscribe pub %q: %w", w.id, g, err)
		}
		go w.serveLoop(ctx, pubCh)
	}

	// Subscribe to a per-worker push topic so the router can route directly
	// to this adapter without competing with Python workers on the group queue.
	// The router uses pepper.push.{workerID} when WorkerID is set in the envelope.
	directCh, err := w.bus.Subscribe(ctx, bus.TopicPush(w.id))
	if err != nil {
		return fmt.Errorf("adapter.BusWorker %s: subscribe direct push: %w", w.id, err)
	}
	go w.serveLoop(ctx, directCh)

	bcastCh, err := w.bus.Subscribe(ctx, bus.TopicBroadcast)
	if err != nil {
		return fmt.Errorf("adapter.BusWorker %s: subscribe broadcast: %w", w.id, err)
	}
	go w.broadcastLoop(ctx, bcastCh)
	go w.heartbeatLoop(ctx)
	w.logger.Fields("cap", w.capName, "groups", w.groups).Info("adapter worker ready")
	return nil
}

// Stop signals the worker to stop accepting new requests.
func (w *BusWorker) Stop() { w.stopped.Store(true) }

func (w *BusWorker) serveLoop(ctx context.Context, ch <-chan bus.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// Peek at cap before spawning goroutine — log non-matching silently.
			var peek struct {
				Cap string `msgpack:"cap"`
			}
			if w.codec.Unmarshal(msg.Data, &peek) == nil && peek.Cap != "" && peek.Cap != w.capName {
				// Not for us — another cap on a shared group topic.
				continue
			}
			w.logger.Fields("cap", peek.Cap).Debug("adapter message dequeued")
			go w.handle(ctx, msg)
		}
	}
}

func (w *BusWorker) handle(ctx context.Context, msg bus.Message) {
	if w.stopped.Load() {
		return
	}
	var env envelope.Envelope
	if err := w.codec.Unmarshal(msg.Data, &env); err != nil {
		return
	}
	if env.Cap != w.capName {
		return
	}
	if env.DeadlineMs > 0 && time.Now().UnixMilli() > env.DeadlineMs {
		w.sendErr(env, envelope.ErrDeadlineExceeded, "deadline exceeded before execution")
		return
	}
	var in map[string]any
	if err := w.codec.Unmarshal(env.Payload, &in); err != nil {
		w.sendErr(env, envelope.ErrPayloadInvalid, fmt.Sprintf("decode payload: %v", err))
		return
	}
	if in == nil {
		in = map[string]any{}
	}
	w.logger.Fields("cap", env.Cap, "corr_id", env.CorrID, "forward_to", env.ForwardTo).Info("adapter exec start")

	reqCtx, cancel := context.WithDeadline(ctx, time.UnixMilli(env.DeadlineMs))
	defer cancel()

	t0 := time.Now()
	w.load.Add(1)
	out, err := w.runner.Run(reqCtx, in)
	w.load.Add(-1)
	w.requestsServed.Add(1)
	durationMs := time.Since(t0).Milliseconds()

	if err != nil {
		w.logger.Fields("cap", env.Cap, "corr_id", env.CorrID, "duration_ms", durationMs, "error", err).Warn("adapter exec error")
		code := envelope.ErrExecError
		if reqCtx.Err() != nil {
			code = envelope.ErrDeadlineExceeded
		}
		w.sendErr(env, code, err.Error())
		return
	}
	w.logger.Fields("cap", env.Cap, "corr_id", env.CorrID, "duration_ms", durationMs, "forward_to", env.ForwardTo).Info("adapter exec done")
	w.sendResult(env, out)
}

func (w *BusWorker) sendResult(env envelope.Envelope, out map[string]any) {
	payload, err := w.codec.Marshal(out)
	if err != nil {
		w.sendErr(env, envelope.ErrExecError, fmt.Sprintf("encode result: %v", err))
		return
	}

	if env.ForwardTo != "" {
		fwd := map[string]any{
			"proto_ver":  envelope.ProtoVer,
			"msg_type":   "pipe",
			"corr_id":    env.CorrID,
			"origin_id":  env.OriginID,
			"worker_id":  w.id,
			"cap":        env.Cap,
			"hop":        env.Hop + 1,
			"forward_to": env.ForwardTo,
			"topic":      env.ForwardTo,
			"payload":    payload,
			"meta":       env.Meta,
		}
		data, _ := w.codec.Marshal(fwd)
		_ = w.bus.Publish(env.ForwardTo, data)
		return
	}

	resp := map[string]any{
		"proto_ver": envelope.ProtoVer, "msg_type": string(envelope.MsgRes),
		"corr_id": env.CorrID, "origin_id": env.OriginID,
		"worker_id": w.id, "cap": env.Cap, "cap_ver": env.CapVer,
		"hop": env.Hop, "meta": env.Meta, "payload": payload,
	}
	data, _ := w.codec.Marshal(resp)
	_ = w.bus.Publish(bus.TopicRes(env.OriginID), data)
}

func (w *BusWorker) sendErr(env envelope.Envelope, code envelope.Code, msg string) {
	errEnv := map[string]any{
		"proto_ver": envelope.ProtoVer, "msg_type": string(envelope.MsgErr),
		"corr_id": env.CorrID, "origin_id": env.OriginID,
		"worker_id": w.id, "cap": env.Cap,
		"code": string(code), "message": msg, "retryable": code.Retryable(),
	}
	data, _ := w.codec.Marshal(errEnv)
	if env.ForwardTo != "" {
		_ = w.bus.Publish(env.ForwardTo, data)
	} else {
		_ = w.bus.Publish(bus.TopicRes(env.OriginID), data)
	}
}

func (w *BusWorker) broadcastLoop(ctx context.Context, ch <-chan bus.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var env map[string]any
			if w.codec.Unmarshal(msg.Data, &env) != nil {
				continue
			}
			if t, _ := env["msg_type"].(string); t == "worker_bye" {
				wid, _ := env["worker_id"].(string)
				if wid == "" || wid == w.id {
					w.stopped.Store(true)
				}
			}
		}
	}
}

func (w *BusWorker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.stopped.Load() {
				return
			}
			load := w.load.Load()
			if load > 100 {
				load = 100
			}
			if load < 0 {
				load = 0
			}
			ping := map[string]any{
				"proto_ver": envelope.ProtoVer, "msg_type": string(envelope.MsgHbPing),
				"worker_id": w.id, "runtime": "http", "load": uint8(load),
				"groups": w.groups, "requests_served": w.requestsServed.Load(),
				"uptime_ms": time.Since(w.startedAt).Milliseconds(),
			}
			data, err := w.codec.Marshal(ping)
			if err != nil {
				continue
			}
			_ = w.bus.Publish(bus.TopicHB(w.id), data)
		}
	}
}
