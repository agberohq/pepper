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
	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
)

type Pepper struct {
	cfg      Config
	codec    codec.Codec
	sessions storage.Store
	mu       sync.RWMutex

	reg      *registry.Registry
	pending  *pending.Map
	hooks    *hooks.Registry
	rt       *runtimeState
	shutdown *jack.Shutdown
	logger   *ll.Logger
	started  bool
	stopped  bool
}

func New(opts ...Option) (*Pepper, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	c, err := codec.Get(string(cfg.Serializer))
	if err != nil {
		return nil, fmt.Errorf("pepper: codec: %w", err)
	}
	sess := cfg.SessionStore
	if sess == nil {
		sess = storage.NewMemory(24 * time.Hour)
	}
	sd := jack.NewShutdown(
		jack.ShutdownWithTimeout(cfg.ShutdownTimeout),
		jack.ShutdownWithSignals(),
	)

	logger := cfg.logger
	if logger == nil {
		logger = ll.New("pepper").Enable()
	}
	return &Pepper{
		cfg: cfg, codec: c, reg: registry.New(), pending: pending.New(),
		hooks: hooks.NewRegistry(), sessions: sess, shutdown: sd,
		logger: logger,
	}, nil
}

func (p *Pepper) Register(name string, source any, opts ...CapOption) error {
	spec, err := buildPythonSpec(name, source, opts...)
	if err != nil {
		return fmt.Errorf("register %q: %w", name, err)
	}
	return p.reg.Add(spec)
}

func (p *Pepper) RegisterDir(dir string, opts ...CapOption) error {
	return walkPythonDir(dir, func(path string) error {
		name := registry.DirNameToCap(path)
		return p.Register(name, path, opts...)
	})
}

func (p *Pepper) Include(name string, worker core.Worker, opts ...CapOption) error {
	spec, err := buildGoSpec(name, worker, opts...)
	if err != nil {
		return fmt.Errorf("include %q: %w", name, err)
	}
	return p.reg.Add(spec)
}

func (p *Pepper) Adapt(name string, adapter AdapterBuilder, opts ...CapOption) error {
	spec, err := buildAdapterSpec(name, adapter, opts...)
	if err != nil {
		return fmt.Errorf("adapt %q: %w", name, err)
	}
	return p.reg.Add(spec)
}

func (p *Pepper) Prepare(name string, cmd CMDBuilder, opts ...CapOption) error {
	spec, err := buildCLISpec(name, cmd, opts...)
	if err != nil {
		return fmt.Errorf("prepare %q: %w", name, err)
	}
	return p.reg.Add(spec)
}

func (p *Pepper) Compose(name string, stages ...compose.Stage) error {
	dag, err := compose.Compile(name, stages)
	if err != nil {
		return fmt.Errorf("compose %q: %w", name, err)
	}
	spec, err := buildPipelineSpec(name, dag)
	if err != nil {
		return fmt.Errorf("compose %q: build spec: %w", name, err)
	}
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
		return nil
	}
	if err := p.bootRuntime(ctx); err != nil {
		return fmt.Errorf("pepper: start: %w", err)
	}
	p.started = true
	return nil
}

func (p *Pepper) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil
	}
	p.stopped = true
	p.shutdown.TriggerShutdown()
	return nil
}

func (p *Pepper) Hooks() *hooks.Registry { return p.hooks }

func (p *Pepper) Do(ctx context.Context, cap string, in core.In, opts ...CallOption) (Result, error) {
	if err := p.ensureStarted(); err != nil {
		return Result{}, err
	}
	return p.dispatch(ctx, cap, in, opts...)
}

func (p *Pepper) Stream(ctx context.Context, cap string, in core.In, opts ...CallOption) (*Stream, error) {
	if err := p.ensureStarted(); err != nil {
		return nil, err
	}
	return p.dispatchStream(ctx, cap, in, opts...)
}

func (p *Pepper) Group(ctx context.Context, group string, dispatch string, cap string, in core.In, opts ...CallOption) ([]Result, error) {
	if err := p.ensureStarted(); err != nil {
		return nil, err
	}
	opts = append(opts, WithGroup(group), WithDispatch(dispatch))
	return p.dispatchMulti(ctx, cap, in, opts...)
}

func (p *Pepper) Broadcast(ctx context.Context, cap string, in core.In, opts ...CallOption) ([]Result, error) {
	if err := p.ensureStarted(); err != nil {
		return nil, err
	}
	opts = append(opts, WithGroup("*"), WithDispatch("all"))
	return p.dispatchMulti(ctx, cap, in, opts...)
}

func (p *Pepper) Session(id string) *Session { return &Session{id: id, pp: p} }

func (p *Pepper) Capabilities(ctx context.Context, filters ...registry.Filter) []registry.Schema {
	return p.reg.Schemas(filters...)
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
	return p.Start(context.Background())
}

func (p *Pepper) dispatch(ctx context.Context, cap string, in core.In, opts ...CallOption) (Result, error) {
	o := defaultCallOpts()
	for _, opt := range opts {
		opt(&o)
	}

	// Check if capability is a pipeline
	if spec := p.reg.Get(cap); spec != nil && spec.Runtime == registry.RuntimePipeline {
		return p.dispatchPipeline(ctx, spec, cap, in, o)
	}

	capTags := map[string]string{"cap": cap, "group": o.group}

	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Counter(metrics.MetricRequestsTotal, 1, capTags)
		p.cfg.Metrics.Gauge(metrics.MetricRequestsInflight, 1, capTags)
		defer p.cfg.Metrics.Gauge(metrics.MetricRequestsInflight, -1, capTags)
	}

	originID := ulid.Make().String()
	start := time.Now()

	// Build a pre-envelope for before-hooks (cap/group context available)
	preEnv := buildEnvelope(originID, originID, cap, in, o, p.cfg.DefaultTimeout)
	mutatedIn, err := p.hooks.RunBefore(ctx, &preEnv, in)
	if err != nil {
		if res, ok := hooks.ShortCircuitResult(err); ok {
			return toResult(res, p.codec), nil
		}
		return Result{}, err
	}
	for attempt := 0; attempt <= p.cfg.MaxRetries; attempt++ {
		if attempt > 0 && p.cfg.Metrics != nil {
			p.cfg.Metrics.Counter(metrics.MetricRequestsRetries, 1, capTags)
		}
		corrID := ulid.Make().String()
		env := buildEnvelope(corrID, originID, cap, mutatedIn, o, p.cfg.DefaultTimeout)
		env.DeliveryCount = uint8(attempt)
		payload, err := p.codec.Marshal(mutatedIn)
		if err != nil {
			return Result{}, fmt.Errorf("encode: %w", err)
		}
		env.Payload = payload
		ch, err := p.pending.Register(corrID)
		if err != nil {
			return Result{}, err
		}
		if err := p.rt.router.Dispatch(ctx, env); err != nil {
			p.pending.Fail(corrID, err)
			return Result{}, err
		}
		select {
		case resp := <-ch:
			result := responseToResult(resp, p.codec, time.Since(start))
			latMs := float64(result.Latency.Milliseconds())
			if result.Err == nil {
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
			if p.cfg.Metrics != nil {
				errTags := map[string]string{"cap": cap, "group": o.group, "status": "err"}
				p.cfg.Metrics.Histogram(metrics.MetricRequestsLatencyMs, latMs, errTags)
			}
			if we, ok := result.Err.(*WorkerError); ok {
				if envelope.Code(we.Code).Retryable() && attempt < p.cfg.MaxRetries {
					p.rt.reqReaper.Remove(corrID)
					p.pending.Fail(corrID, result.Err)
					continue
				}
			}
			hr := hooks.Result{Payload: result.payload, WorkerID: result.WorkerID, Cap: result.Cap, CapVer: result.CapVer, Hop: result.Hop, Meta: result.Meta, Err: result.Err}
			final, _ := p.hooks.RunAfter(ctx, &env, mutatedIn, hr)
			result.payload = final.Payload
			result.Err = final.Err
			return result, result.Err
		case <-ctx.Done():
			p.rt.router.BroadcastCancel(originID)
			p.pending.Fail(corrID, ctx.Err())
			return Result{}, ctx.Err()
		}
	}
	return Result{}, fmt.Errorf("max retries exceeded")
}

func (p *Pepper) dispatchMulti(ctx context.Context, cap string, in core.In, opts ...CallOption) ([]Result, error) {
	o := defaultCallOpts()
	for _, opt := range opts {
		opt(&o)
	}
	originID := ulid.Make().String()
	env := buildEnvelope(originID, originID, cap, in, o, p.cfg.DefaultTimeout)
	payload, err := p.codec.Marshal(in)
	if err != nil {
		return nil, err
	}
	env.Payload = payload
	nWorkers := p.rt.router.WorkerCountInGroup(env.Group)
	if nWorkers == 0 {
		nWorkers = 1
	}
	var results []Result
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	corrIDs := make([]string, 0, nWorkers)
	for i := 0; i < nWorkers; i++ {
		cID := ulid.Make().String()
		ch, err := p.pending.Register(cID)
		if err != nil {
			continue
		}
		corrIDs = append(corrIDs, cID)
		wg.Add(1)
		go func(ch <-chan pending.Response) {
			defer wg.Done()
			select {
			case resp, ok := <-ch:
				if !ok || resp.Err != nil {
					return
				}
				r := responseToResult(resp, p.codec, 0)
				mu.Lock()
				results = append(results, r)
				mu.Unlock()
			case <-ctx.Done():
			}
		}(ch)
	}
	data, _ := p.codec.Marshal(env)
	if err := p.rt.bus.Publish(bus.TopicPub(env.Group), data); err != nil {
		for _, cID := range corrIDs {
			p.pending.Fail(cID, err)
		}
		return nil, err
	}
	wg.Wait()
	return results, nil
}

func (p *Pepper) dispatchStream(ctx context.Context, cap string, in core.In, opts ...CallOption) (*Stream, error) {
	o := defaultCallOpts()
	for _, opt := range opts {
		opt(&o)
	}
	corrID := ulid.Make().String()
	env := buildEnvelope(corrID, corrID, cap, in, o, p.cfg.DefaultTimeout)
	payload, err := p.codec.Marshal(in)
	if err != nil {
		return nil, err
	}
	env.Payload = payload
	ch, err := p.pending.RegisterStream(corrID, 64)
	if err != nil {
		return nil, err
	}
	p.rt.reqReaper.TouchAt(corrID, time.UnixMilli(env.DeadlineMs))
	if err := p.rt.router.Dispatch(ctx, env); err != nil {
		p.pending.Fail(corrID, err)
		return nil, err
	}
	return &Stream{ch: ch, c: p.codec}, nil
}
