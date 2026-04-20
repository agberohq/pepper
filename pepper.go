package pepper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/compose"
	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/hooks"
	"github.com/agberohq/pepper/internal/metrics"
	"github.com/agberohq/pepper/internal/pending"
	"github.com/agberohq/pepper/internal/registry"
	"github.com/agberohq/pepper/internal/storage"
	"github.com/agberohq/pepper/internal/tracker"
	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
	"github.com/olekukonko/mappo"
)

type Pepper struct {
	cfg      Config
	codec    codec.Codec
	sessions storage.Store
	mu       sync.RWMutex

	reg            *registry.Registry
	pending        *pending.Map
	hooks          *hooks.Registry
	tracker        *tracker.Tracker                  // nil when WithTracking(false)
	processResults *mappo.Concurrent[string, Result] // nil when WithTracking(false)
	rt             *runtimeState
	shutdown       *jack.Shutdown
	logger         *ll.Logger
	started        bool
	stopped        bool
}

func New(opts ...Option) (*Pepper, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	c, err := codec.Get(string(cfg.Codec))
	if err != nil {
		return nil, fmt.Errorf("pepper: codec: %w", err)
	}
	sess := cfg.Storage
	if sess == nil {
		sess = storage.NewMemory(DefaultSessionTTL)
	}
	sd := jack.NewShutdown(
		jack.ShutdownWithTimeout(cfg.ShutdownTimeout),
		jack.ShutdownWithSignals(),
	)

	logger := cfg.logger
	if logger == nil {
		logger = ll.New("pepper").Enable()
	}

	hooksReg := hooks.NewRegistry()
	var trk *tracker.Tracker
	var procResults *mappo.Concurrent[string, Result]
	if cfg.Tracking {
		trk = tracker.New()
		hooksReg.Global().Add(trk.Hook())
		procResults = mappo.NewConcurrent[string, Result]()
	}

	return &Pepper{
		cfg: cfg, codec: c, reg: registry.New(), pending: pending.New(),
		hooks: hooksReg, tracker: trk, processResults: procResults, sessions: sess, shutdown: sd,
		logger: logger,
	}, nil
}

func (p *Pepper) Compose(name string, stages ...Stage) error {
	dag, err := compose.Compile(name, stages)
	if err != nil {
		return fmt.Errorf("compose %q: %w", name, err)
	}
	spec := &registry.Spec{Name: name, Runtime: registry.RuntimePipeline, Pipeline: dag, Version: defaultCapVersion}
	p.logger.Fields("name", name, "stages", len(stages)).Info("composed pipeline")
	return p.reg.Add(spec)
}

func (p *Pepper) RegisterResource(name string, config map[string]any) {
	if p.cfg.Resources == nil {
		p.cfg.Resources = make(map[string]map[string]any)
	}
	p.cfg.Resources[name] = config
}

func (p *Pepper) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		p.logger.Debug("pepper already started")
		return nil
	}
	p.logger.Info("starting pepper runtime")
	if err := p.bootRuntime(ctx); err != nil {
		p.logger.Fields("error", err).Error("failed to start pepper runtime")
		return fmt.Errorf("pepper: start: %w", err)
	}
	p.started = true
	p.logger.Info("pepper runtime started successfully")
	return nil
}

func (p *Pepper) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		p.logger.Debug("pepper already stopped")
		return nil
	}
	p.stopped = true
	p.logger.Info("stopping pepper runtime")
	p.shutdown.TriggerShutdown()
	p.logger.Info("pepper runtime stopped")
	return nil
}

func (p *Pepper) Hooks() *hooks.Registry { return p.hooks }

func (p *Pepper) Do(ctx context.Context, cap string, in core.In, opts ...CallOption) (Result, error) {
	if err := p.ensureStarted(); err != nil {
		return Result{}, err
	}
	return p.dispatch(ctx, cap, in, opts...)
}

func (p *Pepper) Group(ctx context.Context, group string, dispatch string, cap string, in core.In, opts ...CallOption) ([]Result, error) {
	if err := p.ensureStarted(); err != nil {
		return nil, err
	}
	opts = append(opts, WithCallGroup(group), WithCallDispatch(dispatch))
	return p.dispatchMulti(ctx, cap, in, opts...)
}

func (p *Pepper) Broadcast(ctx context.Context, cap string, in core.In, opts ...CallOption) ([]Result, error) {
	if err := p.ensureStarted(); err != nil {
		return nil, err
	}
	opts = append(opts, WithCallGroup(groupBroadcast), WithCallDispatch(string(DispatchAll)))
	return p.dispatchMulti(ctx, cap, in, opts...)
}

// Session binds to an existing session ID.
// Use when you already have the ID — e.g. from a request header, JWT, or cookie.
func (p *Pepper) Session(id string) *Session { return &Session{id: id, pp: p} }

// NewSession creates a new session with a generated ID.
// Call sess.ID() to retrieve the ID and persist it (cookie, JWT, etc.).
//
//	sess := pp.NewSession()
//	http.SetCookie(w, &http.Cookie{Name: "sid", Value: sess.ID()})
func (p *Pepper) NewSession() *Session {
	return &Session{id: ulid.Make().String(), pp: p}
}

func (p *Pepper) Capabilities(ctx context.Context, filters ...registry.Filter) []registry.Schema {
	return p.reg.Schemas(filters...)
}

// WorkerReady reports whether at least one live worker has announced it is ready
// to handle cap. This becomes true only after the worker connects, receives
// cap_load, and replies with cap_ready — it is false for specs that are merely
// registered but whose worker process has not yet started.
//
// Use this to wait for Python subprocess workers after Start() returns, since
// Start() exits as soon as any worker (including in-process adapter workers)
// is ready, which may precede Python worker startup.
func (p *Pepper) WorkerReady(cap string) bool {
	if p.rt == nil || p.rt.router == nil {
		return false
	}
	return p.rt.router.HasCapWorker(cap)
}

func (p *Pepper) ensureStarted() error {
	p.mu.RLock()
	started, stopped := p.started, p.stopped
	p.mu.RUnlock()
	if stopped {
		return fmt.Errorf("pepper: already stopped")
	}
	if started {
		return nil
	}
	p.logger.Debug("auto-starting pepper on demand")
	return p.Start(context.Background())
}

func (p *Pepper) dispatch(ctx context.Context, cap string, in core.In, opts ...CallOption) (Result, error) {
	o := p.defaultCallOpts()
	for _, opt := range opts {
		opt(&o)
	}

	if spec := p.reg.Get(cap); spec != nil && spec.Runtime == registry.RuntimePipeline {
		p.logger.Fields("cap", cap, "group", o.group).Debug("dispatching pipeline request")
		return p.dispatchPipeline(ctx, spec, cap, in, o)
	}

	originID := o.originID
	if originID == "" {
		originID = ulid.Make().String()
	}
	start := time.Now()

	p.logger.Fields("cap", cap, "corr_id", originID, "group", o.group, "timeout_ms", p.cfg.DefaultTimeout.Milliseconds()).Debug("dispatching request")

	capTags := map[string]string{"cap": cap, "group": o.group}

	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Counter(metrics.MetricRequestsTotal, 1, capTags)
		p.cfg.Metrics.Gauge(metrics.MetricRequestsInflight, 1, capTags)
		defer p.cfg.Metrics.Gauge(metrics.MetricRequestsInflight, -1, capTags)
	}

	preEnv := p.buildEnvelope(originID, originID, cap, in, o)
	mutatedIn, err := p.hooks.RunBefore(ctx, &preEnv, in)
	if err != nil {
		if res, ok := hooks.ShortCircuitResult(err); ok {
			p.logger.Fields("cap", cap, "corr_id", originID).Debug("request short-circuited by before hook")
			return toResult(res, p.codec), nil
		}
		p.logger.Fields("cap", cap, "corr_id", originID, "error", err).Warn("before hook failed")
		return Result{}, err
	}

	for attempt := 0; attempt <= p.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			p.logger.Fields("cap", cap, "corr_id", originID, "attempt", attempt, "max_retries", p.cfg.MaxRetries).Debug("retrying request")
			if p.cfg.Metrics != nil {
				p.cfg.Metrics.Counter(metrics.MetricRequestsRetries, 1, capTags)
			}
		}

		corrID := ulid.Make().String()
		env := p.buildEnvelope(corrID, originID, cap, mutatedIn, o)
		env.DeliveryCount = uint8(attempt)
		payload, err := p.codec.Marshal(mutatedIn)
		if err != nil {
			p.logger.Fields("cap", cap, "corr_id", corrID, "error", err).Error("failed to marshal request payload")
			return Result{}, fmt.Errorf("encode: %w", err)
		}
		env.Payload = payload

		ch, err := p.pending.Register(corrID)
		if err != nil {
			p.logger.Fields("cap", cap, "corr_id", corrID, "error", err).Error("failed to register pending request")
			return Result{}, err
		}

		if err := p.rt.router.Dispatch(ctx, env); err != nil {
			p.logger.Fields("cap", cap, "corr_id", corrID, "error", err).Warn("failed to dispatch request")
			p.pending.Fail(corrID, err)
			return Result{}, err
		}

		select {
		case resp := <-ch:
			result := responseToResult(resp, p.codec, time.Since(start))
			latMs := float64(result.Latency.Milliseconds())

			if latMs > 5000 {
				p.logger.Fields("cap", cap, "corr_id", corrID, "latency_ms", latMs).Warn("slow request detected")
			}

			if result.Err == nil {
				p.logger.Fields("cap", cap, "corr_id", corrID, "worker_id", result.WorkerID, "latency_ms", latMs, "hop", result.Hop).Debug("request completed successfully")
				if p.cfg.Metrics != nil {
					p.cfg.Metrics.Histogram(metrics.MetricRequestsLatencyMs, latMs, capTags)
					p.cfg.Metrics.Counter(metrics.MetricRequestsHops, int64(result.Hop), capTags)
				}
				hr := hooks.Result{Payload: result.payload, WorkerID: result.WorkerID, Cap: result.Cap, CapVer: result.CapVer, Hop: result.Hop, Meta: result.Meta, Err: result.Err}
				final, _ := p.hooks.RunAfter(ctx, &env, mutatedIn, hr)
				result.payload = final.Payload
				result.Err = final.Err
				return result, result.Err
			}

			p.logger.Fields("cap", cap, "corr_id", corrID, "error", result.Err.Error(), "attempt", attempt).Warn("request failed")

			if p.cfg.Metrics != nil {
				errTags := map[string]string{"cap": cap, "group": o.group, "status": "err"}
				p.cfg.Metrics.Histogram(metrics.MetricRequestsLatencyMs, latMs, errTags)
			}
			if we, ok := result.Err.(*WorkerError); ok {
				if envelope.Code(we.Code).Retryable() && attempt < p.cfg.MaxRetries {
					p.rt.reqReaper.Remove(corrID)
					p.pending.Fail(corrID, result.Err)
					p.logger.Fields("cap", cap, "corr_id", corrID, "attempt", attempt).Debug("retrying due to retryable error")
					continue
				}
			}
			hr := hooks.Result{Payload: result.payload, WorkerID: result.WorkerID, Cap: result.Cap, CapVer: result.CapVer, Hop: result.Hop, Meta: result.Meta, Err: result.Err}
			final, _ := p.hooks.RunAfter(ctx, &env, mutatedIn, hr)
			result.payload = final.Payload
			result.Err = final.Err
			return result, result.Err
		case <-ctx.Done():
			p.logger.Fields("cap", cap, "corr_id", corrID, "error", ctx.Err()).Warn("request cancelled by context")
			p.rt.router.BroadcastCancel(originID)
			p.pending.Fail(corrID, ctx.Err())
			return Result{}, ctx.Err()
		}
	}

	p.logger.Fields("cap", cap, "corr_id", originID, "max_retries", p.cfg.MaxRetries).Error("max retries exceeded")
	return Result{}, fmt.Errorf("max retries exceeded")
}

func (p *Pepper) dispatchMulti(ctx context.Context, cap string, in core.In, opts ...CallOption) ([]Result, error) {
	o := p.defaultCallOpts()
	for _, opt := range opts {
		opt(&o)
	}
	originID := ulid.Make().String()

	p.logger.Fields("cap", cap, "group", o.group, "dispatch", o.dispatch, "origin_id", originID).Debug("dispatching multi request")

	env := p.buildEnvelope(originID, originID, cap, in, o)
	payload, err := p.codec.Marshal(in)
	if err != nil {
		p.logger.Fields("cap", cap, "error", err).Error("failed to marshal multi request payload")
		return nil, err
	}
	env.Payload = payload
	nWorkers := p.rt.router.WorkerCountInGroup(env.Group)
	if nWorkers == 0 {
		nWorkers = 1
	}

	p.logger.Fields("cap", cap, "group", o.group, "worker_count", nWorkers).Debug("broadcasting to workers")

	var results []Result
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	corrIDs := make([]string, 0, nWorkers)
	for i := 0; i < nWorkers; i++ {
		cID := ulid.Make().String()
		ch, err := p.pending.Register(cID)
		if err != nil {
			p.logger.Fields("cap", cap, "corr_id", cID, "error", err).Warn("failed to register multi request")
			continue
		}
		corrIDs = append(corrIDs, cID)
		wg.Add(1)
		go func(ch <-chan pending.Response) {
			defer wg.Done()
			select {
			case resp, ok := <-ch:
				if !ok || resp.Err != nil {
					if resp.Err != nil {
						p.logger.Fields("cap", cap, "error", resp.Err).Warn("multi request received error response")
					}
					return
				}
				r := responseToResult(resp, p.codec, 0)
				mu.Lock()
				results = append(results, r)
				mu.Unlock()
			case <-ctx.Done():
				p.logger.Fields("cap", cap).Debug("multi request context cancelled")
			}
		}(ch)
	}
	data, _ := p.codec.Marshal(env)
	if err := p.rt.bus.Publish(bus.TopicPub(env.Group), data); err != nil {
		p.logger.Fields("cap", cap, "group", o.group, "error", err).Error("failed to publish multi request")
		for _, cID := range corrIDs {
			p.pending.Fail(cID, err)
		}
		return nil, err
	}
	wg.Wait()
	p.logger.Fields("cap", cap, "results_count", len(results)).Debug("multi request completed")
	return results, nil
}

// Process Tracking (internal)

// track is the internal implementation backing Execute[Out].
// It returns both the tracker processID and the envelope originID so that
// Process[Out].Cancel() can call BroadcastCancel with the correct ID.
func (p *Pepper) track(ctx context.Context, cap string, in core.In, opts ...CallOption) (processID, originID string, err error) {
	if p.tracker == nil {
		return "", "", ErrTrackingDisabled
	}

	processID = ulid.Make().String()
	originID = ulid.Make().String()

	// Inject _process_id into call meta so hooks can find this process.
	// Inject _origin_id to pin the envelope OriginID so Cancel() matches.
	opts = append(opts,
		WithMeta(MetaKeyProcessID, processID),
		withOriginID(originID),
	)

	// Determine total stages: 1 for plain caps, worker-dispatched stage count for pipelines.
	totalStages := 1
	if spec := p.reg.Get(cap); spec != nil {
		if dag, ok := spec.Pipeline.(*compose.DAG); ok {
			totalStages = dag.WorkerStageCount()
		}
	}

	p.tracker.RegisterProcess(processID, cap, totalStages)

	go func() {
		result, err := p.Do(ctx, cap, in, opts...)
		if err != nil {
			p.tracker.FailProcess(processID, err)
			return
		}
		p.processResults.Set(processID, result)
	}()

	return processID, originID, nil
}

// resultOfWatch blocks until a tracked process completes, using the tracker's
// Watch channel instead of polling. Responds immediately when the process ends.
func (p *Pepper) resultOfWatch(ctx context.Context, processID string) (Result, error) {
	if p.tracker == nil {
		return Result{}, ErrTrackingDisabled
	}

	// Fast path: result already stored.
	if r, ok := p.processResults.Get(processID); ok {
		return r, nil
	}

	ch := p.tracker.Watch(processID)
	if ch == nil {
		return Result{}, fmt.Errorf("pepper: unknown process %s", processID)
	}

	// Drain events until the channel closes (done or failed), then read result.
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				// Channel closed — tracker has marked the process terminal.
				// The goroutine in track() stores the result in processResults
				// after dispatchPipeline returns, which happens after the tracker
				// records the final stage done. Poll briefly to let it land.
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					if r, ok := p.processResults.Get(processID); ok {
						return r, nil
					}
					if state, ok := p.tracker.Check(processID); ok && state.Error != "" {
						return Result{}, fmt.Errorf("%s", state.Error)
					}
					select {
					case <-ctx.Done():
						return Result{}, ctx.Err()
					case <-time.After(5 * time.Millisecond):
					}
				}
				return Result{}, fmt.Errorf("pepper: process %s: result not available after timeout", processID)
			}
			// Non-terminal event — continue draining.
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
}

// ErrTrackingDisabled is returned by Execute when WithTracking(true) was not set.
var ErrTrackingDisabled = fmt.Errorf("pepper: process tracking is disabled — use WithTracking(true)")
