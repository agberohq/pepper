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
	reqCtx, cancel := context.WithDeadline(ctx, time.UnixMilli(env.DeadlineMs))
	defer cancel()

	w.load.Add(1)
	out, err := w.runner.Run(reqCtx, in)
	w.load.Add(-1)
	w.requestsServed.Add(1)

	if err != nil {
		code := envelope.ErrExecError
		if reqCtx.Err() != nil {
			code = envelope.ErrDeadlineExceeded
		}
		w.sendErr(env, code, err.Error())
		return
	}
	w.sendResult(env, out)
}

func (w *BusWorker) sendResult(env envelope.Envelope, out map[string]any) {
	payload, err := w.codec.Marshal(out)
	if err != nil {
		w.sendErr(env, envelope.ErrExecError, fmt.Sprintf("encode result: %v", err))
		return
	}
	resp := map[string]any{
		"proto_ver": uint8(1), "msg_type": string(envelope.MsgRes),
		"corr_id": env.CorrID, "origin_id": env.OriginID,
		"worker_id": w.id, "cap": env.Cap, "cap_ver": env.CapVer,
		"hop": env.Hop, "meta": env.Meta, "payload": payload,
	}
	data, _ := w.codec.Marshal(resp)
	_ = w.bus.Publish(bus.TopicRes(env.OriginID), data)
}

func (w *BusWorker) sendErr(env envelope.Envelope, code envelope.Code, msg string) {
	errEnv := map[string]any{
		"proto_ver": uint8(1), "msg_type": string(envelope.MsgErr),
		"corr_id": env.CorrID, "origin_id": env.OriginID,
		"worker_id": w.id, "cap": env.Cap,
		"code": string(code), "message": msg, "retryable": code.Retryable(),
	}
	data, _ := w.codec.Marshal(errEnv)
	_ = w.bus.Publish(bus.TopicRes(env.OriginID), data)
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
				"proto_ver": uint8(1), "msg_type": string(envelope.MsgHbPing),
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
