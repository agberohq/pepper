package pepper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/agberohq/pepper/internal/core"
)

// callBase holds routing/timing fields shared by Call[Out] and Exec.
type callBase struct {
	Group    string
	Worker   string
	Timeout  time.Duration
	Deadline time.Time
	Meta     map[string]any
}

func (b callBase) baseOpts() []CallOption {
	var opts []CallOption
	if b.Group != "" {
		opts = append(opts, WithCallGroup(b.Group))
	}
	if b.Worker != "" {
		opts = append(opts, WithCallWorker(b.Worker))
	}
	if b.Timeout > 0 {
		opts = append(opts, WithCallTimeout(b.Timeout))
	} else if !b.Deadline.IsZero() {
		opts = append(opts, WithCallDeadline(b.Deadline))
	}
	for k, v := range b.Meta {
		opts = append(opts, WithMeta(k, v))
	}
	return opts
}

// Call describes a typed capability invocation. It is a pure value — no
// runtime, no context. Bind it to execute.
//
//	result, err := pepper.Call[AnalysisOutput]{
//	    Cap:     "text.analyze",
//	    Input:   TextInput{Text: "hello world"},
//	    Timeout: 30 * time.Second,
//	}.Bind(pp).Do(ctx)
type Call[Out any] struct {
	Cap      string
	Input    any
	Group    string
	Worker   string
	Dispatch Dispatch
	Quorum   uint8
	Timeout  time.Duration
	Deadline time.Time
	Version  string
	Meta     map[string]any
}

func (c Call[Out]) buildOpts() []CallOption {
	b := callBase{Group: c.Group, Worker: c.Worker, Timeout: c.Timeout, Deadline: c.Deadline, Meta: c.Meta}
	opts := b.baseOpts()
	if c.Dispatch != "" {
		opts = append(opts, WithCallDispatch(string(c.Dispatch)))
	}
	if c.Quorum > 0 {
		opts = append(opts, WithCallQuorum(c.Quorum))
	}
	if c.Version != "" {
		opts = append(opts, WithCallVersion(c.Version))
	}
	return opts
}

// Bind attaches a *Pepper runtime and returns a BoundCall[Out] ready to execute.
func (c Call[Out]) Bind(pp *Pepper) BoundCall[Out] {
	return BoundCall[Out]{spec: c, pp: pp}
}

// Session attaches a *Session and returns a BoundCall[Out] ready to execute.
func (c Call[Out]) Session(sess *Session) BoundCall[Out] {
	return BoundCall[Out]{spec: c, pp: sess.pp, sessionID: sess.id}
}

// BoundCall is a Call[Out] bound to a *Pepper runtime.
// Obtained via Call[Out].Bind(pp) or Call[Out].Session(sess).
type BoundCall[Out any] struct {
	spec      Call[Out]
	pp        *Pepper
	sessionID string
}

func (b BoundCall[Out]) opts() []CallOption {
	opts := b.spec.buildOpts()
	if b.sessionID != "" {
		opts = append(opts, WithCallSession(b.sessionID))
	}
	return opts
}

// Do executes the call and decodes the result into Out.
func (b BoundCall[Out]) Do(ctx context.Context) (Out, error) {
	var zero Out
	in, err := toWireInput(b.spec.Input)
	if err != nil {
		return zero, fmt.Errorf("pepper.Call[%T].Do: encode input: %w", zero, err)
	}
	res, err := b.pp.Do(ctx, b.spec.Cap, in, b.opts()...)
	if err != nil {
		return zero, err
	}
	var out Out
	if err := res.Into(&out); err != nil {
		return zero, fmt.Errorf("pepper.Call[%T].Do: decode result: %w", zero, err)
	}
	return out, nil
}

// Execute dispatches the call asynchronously and returns a *Process[Out] handle.
// Requires WithTracking(true) on the *Pepper instance.
func (b BoundCall[Out]) Execute(ctx context.Context) (*Process[Out], error) {
	in, err := toWireInput(b.spec.Input)
	if err != nil {
		return nil, fmt.Errorf("pepper.Call[%T].Execute: encode input: %w", *new(Out), err)
	}
	id, originID, err := b.pp.track(ctx, b.spec.Cap, in, b.opts()...)
	if err != nil {
		return nil, err
	}
	return &Process[Out]{id: id, originID: originID, pp: b.pp}, nil
}

// Stream opens a one-way streaming call and returns a typed chunk channel.
// The channel is closed when the stream ends or ctx is cancelled.
func (b BoundCall[Out]) Stream(ctx context.Context) (<-chan Out, error) {
	in, err := toWireInput(b.spec.Input)
	if err != nil {
		return nil, fmt.Errorf("pepper.Call[%T].Stream: encode input: %w", *new(Out), err)
	}
	raw, err := b.pp.openRawStream(ctx, b.spec.Cap, in, b.opts()...)
	if err != nil {
		return nil, err
	}
	return drainResponses[Out](ctx, raw.outCh, b.pp.codec), nil
}

// Group routes the call to a specific worker group and collects all responses.
//
//	results, err := pepper.Call[VoteResult]{Cap: "tx.validate", Input: txIn}.
//	    Bind(pp).
//	    Group(ctx, "validators", pepper.DispatchVote)
func (b BoundCall[Out]) Group(ctx context.Context, group string, dispatch Dispatch) ([]Out, error) {
	var zero Out
	in, err := toWireInput(b.spec.Input)
	if err != nil {
		return nil, fmt.Errorf("pepper.Call[%T].Group: encode input: %w", zero, err)
	}
	results, err := b.pp.Group(ctx, group, string(dispatch), b.spec.Cap, in, b.opts()...)
	if err != nil {
		return nil, err
	}
	out := make([]Out, 0, len(results))
	for _, r := range results {
		var v Out
		if err := r.Into(&v); err != nil {
			return out, fmt.Errorf("pepper.Call[%T].Group: decode result: %w", zero, err)
		}
		out = append(out, v)
	}
	return out, nil
}

// Broadcast sends the call to every worker and decodes all responses.
//
//	statuses, err := pepper.Call[StatusOut]{Cap: "worker.status", Input: pepper.In{}}.
//	    Bind(pp).
//	    Broadcast(ctx)
func (b BoundCall[Out]) Broadcast(ctx context.Context) ([]Out, error) {
	var zero Out
	in, err := toWireInput(b.spec.Input)
	if err != nil {
		return nil, fmt.Errorf("pepper.Call[%T].Broadcast: encode input: %w", zero, err)
	}
	results, err := b.pp.Broadcast(ctx, b.spec.Cap, in, b.opts()...)
	if err != nil {
		return nil, err
	}
	out := make([]Out, 0, len(results))
	for _, r := range results {
		var v Out
		if err := r.Into(&v); err != nil {
			return out, fmt.Errorf("pepper.Call[%T].Broadcast: decode result: %w", zero, err)
		}
		out = append(out, v)
	}
	return out, nil
}

// Exec describes a fire-and-forget capability invocation that returns no output.
//
//	err := pepper.Exec{Cap: "log.audit", Input: pepper.In{"event": "login"}}.
//	    Bind(pp).Do(ctx)
type Exec struct {
	Cap      string
	Input    any
	Group    string
	Worker   string
	Timeout  time.Duration
	Deadline time.Time
	Meta     map[string]any
}

// Bind attaches a *Pepper runtime and returns a BoundExec ready to execute.
func (e Exec) Bind(pp *Pepper) BoundExec {
	return BoundExec{spec: e, pp: pp}
}

// Session attaches a *Session and returns a BoundExec with the session ID injected.
func (e Exec) Session(sess *Session) BoundExec {
	return BoundExec{spec: e, pp: sess.pp, sessionID: sess.id}
}

// BoundExec is an Exec bound to a *Pepper runtime. Obtained via Exec.Bind or Exec.Session.
type BoundExec struct {
	spec      Exec
	pp        *Pepper
	sessionID string
}

// Do executes the capability and discards the result.
func (b BoundExec) Do(ctx context.Context) error {
	in, err := toWireInput(b.spec.Input)
	if err != nil {
		return fmt.Errorf("pepper.Exec.Do: encode input: %w", err)
	}
	base := callBase{Group: b.spec.Group, Worker: b.spec.Worker, Timeout: b.spec.Timeout, Deadline: b.spec.Deadline, Meta: b.spec.Meta}
	opts := base.baseOpts()
	if b.sessionID != "" {
		opts = append(opts, WithCallSession(b.sessionID))
	}
	_, err = b.pp.Do(ctx, b.spec.Cap, in, opts...)
	return err
}

// Process is the handle for an asynchronously executing capability or pipeline.
// Obtained via BoundCall[Out].Execute(ctx).
//
//	proc, err := pepper.Call[AnalysisOutput]{
//	    Cap:   "audio.pipeline",
//	    Input: pepper.In{"path": audioPath},
//	}.Bind(pp).Execute(ctx)
//
//	go func() {
//	    for event := range proc.Events() {
//	        fmt.Printf("%s: %s (%dms)\n", event.Stage, event.Status, event.DurationMs)
//	    }
//	}()
//	result, err := proc.Wait(ctx)
type Process[Out any] struct {
	id       string
	originID string
	pp       *Pepper
}

// ProcessEvent is emitted for every stage transition.
type ProcessEvent struct {
	Stage      string
	WorkerID   string
	Status     Status
	DurationMs int64
	Error      string
}

// ProcessState is a point-in-time snapshot of a process.
type ProcessState struct {
	ID          string
	Cap         string
	Status      Status
	PercentDone int
	Actions     []ProcessEvent
	Error       string
}

func (p *Process[Out]) ID() string { return p.id }

// Events returns a channel that receives one ProcessEvent per stage transition.
// Closed when the process reaches StatusDone or StatusFailed.
func (p *Process[Out]) Events() <-chan ProcessEvent {
	if p.pp.tracker == nil {
		ch := make(chan ProcessEvent)
		close(ch)
		return ch
	}
	raw := p.pp.tracker.Watch(p.id)
	if raw == nil {
		ch := make(chan ProcessEvent)
		close(ch)
		return ch
	}
	out := make(chan ProcessEvent, 32)
	go func() {
		defer close(out)
		for a := range raw {
			out <- ProcessEvent{
				Stage:      a.Stage,
				WorkerID:   a.WorkerID,
				Status:     Status(a.Status),
				DurationMs: a.DurationMs,
				Error:      a.Error,
			}
		}
	}()
	return out
}

// Wait blocks until the process completes and returns the decoded result.
func (p *Process[Out]) Wait(ctx context.Context) (Out, error) {
	var zero Out
	res, err := p.pp.resultOfWatch(ctx, p.id)
	if err != nil {
		return zero, err
	}
	var out Out
	if err := res.Into(&out); err != nil {
		return zero, fmt.Errorf("pepper.Process[%T].Wait: decode result: %w", zero, err)
	}
	return out, nil
}

// State returns a point-in-time snapshot without blocking.
func (p *Process[Out]) State() ProcessState {
	if p.pp.tracker == nil {
		return ProcessState{ID: p.id, Status: StatusPending}
	}
	raw, ok := p.pp.tracker.Check(p.id)
	if !ok {
		return ProcessState{ID: p.id, Status: StatusPending}
	}
	state := ProcessState{
		ID:          raw.ID,
		Cap:         raw.Cap,
		Status:      Status(raw.Status),
		PercentDone: raw.PercentDone,
		Error:       raw.Error,
	}
	for _, a := range raw.Actions {
		state.Actions = append(state.Actions, ProcessEvent{
			Stage:      a.Stage,
			WorkerID:   a.WorkerID,
			Status:     Status(a.Status),
			DurationMs: a.DurationMs,
			Error:      a.Error,
		})
	}
	return state
}

// Cancel broadcasts a cancellation to all workers and marks the process failed.
// Returns an error if the process handle has no origin ID or tracking is disabled.
func (p *Process[Out]) Cancel() error {
	if p.originID == "" {
		return fmt.Errorf("pepper: Cancel: process has no origin ID")
	}
	if p.pp.rt == nil || p.pp.rt.router == nil {
		return fmt.Errorf("pepper: Cancel: runtime not started")
	}
	p.pp.rt.router.BroadcastCancel(p.originID)
	if p.pp.tracker != nil {
		p.pp.tracker.FailProcess(p.id, fmt.Errorf("cancelled"))
	}
	return nil
}

// CallArgs describes a single capability invocation for use with All[O].
type CallArgs struct {
	Cap  string
	In   core.In
	Opts []CallOption
}

// MakeCall constructs a CallArgs for use with All().
func MakeCall(cap string, in core.In, opts ...CallOption) CallArgs {
	return CallArgs{Cap: cap, In: in, Opts: opts}
}

// CallOption modifies a single invocation.
type CallOption func(*callOpts)

type callOpts struct {
	group      string
	workerID   string
	capVer     string
	dispatch   string
	quorum     uint8
	deadlineMs int64
	sessionID  string
	maxHops    uint8
	maxCbDepth uint8
	meta       map[string]any
	originID   string
}

func WithCallGroup(group string) CallOption   { return func(o *callOpts) { o.group = group } }
func WithCallWorker(id string) CallOption     { return func(o *callOpts) { o.workerID = id } }
func WithCallDispatch(mode string) CallOption { return func(o *callOpts) { o.dispatch = mode } }
func WithCallQuorum(n uint8) CallOption       { return func(o *callOpts) { o.quorum = n } }
func WithCallVersion(c string) CallOption     { return func(o *callOpts) { o.capVer = c } }
func WithCallSession(id string) CallOption    { return func(o *callOpts) { o.sessionID = id } }
func WithCallMaxHops(n uint8) CallOption      { return func(o *callOpts) { o.maxHops = n } }
func WithCallMaxCbDepth(n uint8) CallOption   { return func(o *callOpts) { o.maxCbDepth = n } }

func WithCallDeadline(t time.Time) CallOption {
	return func(o *callOpts) { o.deadlineMs = t.UnixMilli() }
}
func WithCallTimeout(d time.Duration) CallOption {
	return func(o *callOpts) { o.deadlineMs = time.Now().Add(d).UnixMilli() }
}
func WithMeta(key string, value any) CallOption {
	return func(o *callOpts) {
		if o.meta == nil {
			o.meta = make(map[string]any)
		}
		o.meta[key] = value
	}
}

func withOriginID(id string) CallOption { return func(o *callOpts) { o.originID = id } }

// Do executes a capability and decodes the result into O.
// Prefer the fluent API: Call[O]{...}.Bind(pp).Do(ctx)
func Do[O any](ctx context.Context, pp *Pepper, cap string, in core.In, opts ...CallOption) (O, error) {
	var zero O
	res, err := pp.Do(ctx, cap, in, opts...)
	if err != nil {
		return zero, err
	}
	var out O
	if err := res.Into(&out); err != nil {
		return zero, fmt.Errorf("pepper.Do[%T]: decode: %w", zero, err)
	}
	return out, nil
}

// All executes multiple calls in parallel and decodes each result into O.
//
//	results, err := pepper.All[FaceResult](ctx, pp,
//	    pepper.MakeCall("face.recognize", pepper.In{"image": img1}),
//	    pepper.MakeCall("face.recognize", pepper.In{"image": img2}),
//	)
func All[O any](ctx context.Context, pp *Pepper, calls ...CallArgs) ([]O, error) {
	out := make([]O, len(calls))
	errs := make([]error, len(calls))
	var wg sync.WaitGroup
	wg.Add(len(calls))
	for i, c := range calls {
		i, c := i, c
		go func() {
			defer wg.Done()
			out[i], errs[i] = Do[O](ctx, pp, c.Cap, c.In, c.Opts...)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return out, err
		}
	}
	return out, nil
}
