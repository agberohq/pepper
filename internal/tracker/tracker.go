// Package tracker provides process-level observability for Pepper pipelines.
//
// It plugs into the hooks system and records every stage transition —
// dispatched, running, done, failed — with wall-clock timing per worker.
// State is stored in a mappo.Concurrent so reads are lock-free.
//
// Two consumption modes:
//
//	state := t.Check(processID)          // poll anytime — non-blocking
//	ch    := t.Watch(processID)          // streaming — one Action per stage event
//
// Percentage is computed as completed_stages / total_stages * 100.
// total_stages is known at Track() time for pipeline caps (from DAG.StageCount).
// For plain Do() calls it is always 1.
package tracker

import (
	"context"
	"sync"
	"time"

	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/hooks"
	"github.com/olekukonko/mappo"
)

// Status constants.
const (
	StatusPending = "pending"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
)

// Action is one stage invocation within a process.
// A process has exactly TotalStages actions when complete.
type Action struct {
	Stage      string // capability name, e.g. "audio.convert"
	WorkerID   string // which worker handled this stage
	Status     string // StatusRunning | StatusDone | StatusFailed
	StartedAt  time.Time
	FinishedAt time.Time      // zero while still running
	DurationMs int64          // 0 while still running
	Error      string         // non-empty only on StatusFailed
	Meta       map[string]any // envelope Meta at time of dispatch
}

// Process is the full state of one tracked execution.
type Process struct {
	ID          string
	Cap         string // top-level capability / pipeline name
	Status      string
	TotalStages int // known at Track() time; 1 for plain Do() calls
	StartedAt   time.Time
	UpdatedAt   time.Time
	Actions     []Action
	PercentDone int    // 0–100 based on completed (done or failed) stages
	Error       string // set on StatusFailed
}

// process is the internal mutable state stored in the map.
type process struct {
	mu       sync.Mutex
	Process                // embedded; copy on Check()
	watchers []chan Action // streaming subscribers
}

// Tracker records pipeline process state, accessible by processID.
type Tracker struct {
	// processID → *process
	m *mappo.Concurrent[string, *process]

	// corrID → processID  (so Before/After hooks can look up by envelope corrID)
	corrIndex *mappo.Concurrent[string, string]
}

// New creates a new Tracker.
func New() *Tracker {
	return &Tracker{
		m:         mappo.NewConcurrent[string, *process](),
		corrIndex: mappo.NewConcurrent[string, string](),
	}
}

// register creates a new process entry. Called by Pepper before dispatching.
// totalStages: number of stages in the pipeline DAG (1 for plain caps).
func (t *Tracker) register(processID, cap string, totalStages int) {
	p := &process{
		Process: Process{
			ID:          processID,
			Cap:         cap,
			Status:      StatusPending,
			TotalStages: totalStages,
			StartedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		},
	}
	t.m.Set(processID, p)
}

// RegisterProcess is the public form of register, called by pepper.Track().
func (t *Tracker) RegisterProcess(processID, cap string, totalStages int) {
	t.register(processID, cap, totalStages)
}

// RecordStageDispatched records that a pipeline stage was dispatched to a worker.
// Called directly from dispatchPipeline bypassing the hook path, since pipeline
// stages are not individually wrapped by Do().
func (t *Tracker) RecordStageDispatched(processID, cap, corrID string) {
	p, ok := t.m.Get(processID)
	if !ok {
		return
	}
	// Index corrID so RecordStageDone can match without cap name.
	t.corrIndex.Set(corrID, processID+"|"+cap)

	action := Action{
		Stage:     cap,
		Status:    StatusRunning,
		StartedAt: time.Now(),
	}
	p.mu.Lock()
	p.Status = StatusRunning
	p.Actions = append(p.Actions, action)
	p.UpdatedAt = time.Now()
	p.mu.Unlock()
	t.broadcast(p, action)
}

// RecordStageDone records that a pipeline stage completed.
// Called directly from dispatchPipeline when a pipe message arrives.
func (t *Tracker) RecordStageDone(processID, cap, workerID string, failed bool, errMsg string) {
	p, ok := t.m.Get(processID)
	if !ok {
		return
	}

	now := time.Now()
	status := StatusDone
	if failed {
		status = StatusFailed
	}

	p.mu.Lock()
	updated := Action{}
	for i := len(p.Actions) - 1; i >= 0; i-- {
		if p.Actions[i].Stage == cap && p.Actions[i].Status == StatusRunning {
			p.Actions[i].Status = status
			p.Actions[i].WorkerID = workerID
			p.Actions[i].FinishedAt = now
			p.Actions[i].DurationMs = now.Sub(p.Actions[i].StartedAt).Milliseconds()
			p.Actions[i].Error = errMsg
			updated = p.Actions[i]
			break
		}
	}

	completed := 0
	for _, a := range p.Actions {
		if a.Status == StatusDone || a.Status == StatusFailed {
			completed++
		}
	}
	total := p.TotalStages
	if total <= 0 {
		total = 1
	}
	p.PercentDone = (completed * 100) / total
	p.UpdatedAt = now

	processDone := completed >= total
	if processDone {
		if failed {
			p.Status = StatusFailed
			p.Error = errMsg
		} else {
			p.Status = StatusDone
		}
	}

	watchers := p.watchers
	p.mu.Unlock()

	if updated.Stage != "" {
		t.broadcastTo(watchers, updated)
	}
	if processDone {
		p.mu.Lock()
		for _, ch := range p.watchers {
			close(ch)
		}
		p.watchers = nil
		p.mu.Unlock()
	}
}

// FailProcess marks a process as failed from outside the hook path.
// Called by pepper.Track() when the background Do() returns an error before
// any stage hook had a chance to record it (e.g. dispatch failure).
func (t *Tracker) FailProcess(processID string, err error) {
	p, ok := t.m.Get(processID)
	if !ok {
		return
	}
	p.mu.Lock()
	p.Status = StatusFailed
	p.Error = err.Error()
	p.UpdatedAt = time.Now()
	watchers := p.watchers
	p.watchers = nil
	p.mu.Unlock()
	for _, ch := range watchers {
		close(ch)
	}
}

// Check returns a point-in-time snapshot of a process. Safe to call from any goroutine.
// Returns zero Process and false if processID is unknown.
func (t *Tracker) Check(processID string) (Process, bool) {
	p, ok := t.m.Get(processID)
	if !ok {
		return Process{}, false
	}
	p.mu.Lock()
	snap := p.Process // copy
	p.mu.Unlock()
	return snap, true
}

// Watch returns a channel that receives one Action for every stage event
// (StatusRunning on Before, StatusDone/StatusFailed on After).
// The channel is closed when the process reaches StatusDone or StatusFailed.
// Returns nil if processID is unknown.
func (t *Tracker) Watch(processID string) <-chan Action {
	p, ok := t.m.Get(processID)
	if !ok {
		return nil
	}
	ch := make(chan Action, 32) // buffered; slow consumers get dropped on full
	p.mu.Lock()
	p.watchers = append(p.watchers, ch)
	// If already finished, close immediately after draining existing actions.
	alreadyDone := p.Status == StatusDone || p.Status == StatusFailed
	p.mu.Unlock()
	if alreadyDone {
		close(ch)
	}
	return ch
}

// Hook returns a hooks.Hook that the Tracker uses to intercept every cap call.
// Register this globally so it fires for every stage in every pipeline:
//
//	pp.Hooks().Global().Add(tracker.Hook())
//
// When WithTracking(true) is set, Pepper registers this automatically.
func (t *Tracker) Hook() hooks.Hook {
	return hooks.Hook{
		Name:   "pepper.tracker",
		Before: t.before,
		After:  t.after,
	}
}

// before fires when a cap is about to be dispatched to a worker.
// It records a new Action with StatusRunning.
func (t *Tracker) before(ctx context.Context, env *envelope.Envelope, in hooks.In) (hooks.In, error) {
	processID := processIDFromEnv(env)
	if processID == "" {
		return nil, nil
	}

	p, ok := t.m.Get(processID)
	if !ok {
		return nil, nil
	}

	// Index corrID → processID so After can find it even if Meta is stripped.
	t.corrIndex.Set(env.CorrID, processID)

	action := Action{
		Stage:     env.Cap,
		WorkerID:  env.WorkerID,
		Status:    StatusRunning,
		StartedAt: time.Now(),
		Meta:      copyMeta(env.Meta),
	}

	p.mu.Lock()
	p.Status = StatusRunning
	p.Actions = append(p.Actions, action)
	p.UpdatedAt = time.Now()
	p.mu.Unlock()

	t.broadcast(p, action)
	return nil, nil
}

// after fires when a cap returns (success or failure).
// It updates the matching Action with timing and final status.
func (t *Tracker) after(ctx context.Context, env *envelope.Envelope, in hooks.In, result hooks.Result) (hooks.Result, error) {
	processID := processIDFromEnv(env)
	if processID == "" {
		// Fall back to corrID index.
		if pid, ok := t.corrIndex.Get(env.CorrID); ok {
			processID = pid
		}
	}
	if processID == "" {
		return result, nil
	}
	t.corrIndex.Delete(env.CorrID)

	p, ok := t.m.Get(processID)
	if !ok {
		return result, nil
	}

	now := time.Now()
	status := StatusDone
	errStr := ""
	if result.Err != nil {
		status = StatusFailed
		errStr = result.Err.Error()
	}

	p.mu.Lock()

	// Find the last Action for this cap that is still StatusRunning.
	updated := Action{}
	for i := len(p.Actions) - 1; i >= 0; i-- {
		if p.Actions[i].Stage == env.Cap && p.Actions[i].Status == StatusRunning {
			p.Actions[i].Status = status
			p.Actions[i].WorkerID = result.WorkerID
			p.Actions[i].FinishedAt = now
			p.Actions[i].DurationMs = now.Sub(p.Actions[i].StartedAt).Milliseconds()
			p.Actions[i].Error = errStr
			updated = p.Actions[i]
			break
		}
	}

	// Recompute percentage: count completed (done or failed) actions.
	completed := 0
	for _, a := range p.Actions {
		if a.Status == StatusDone || a.Status == StatusFailed {
			completed++
		}
	}
	total := p.TotalStages
	if total <= 0 {
		total = 1
	}
	p.PercentDone = (completed * 100) / total
	p.UpdatedAt = now

	// Check if the process itself is finished.
	// A process is done when we have as many completed actions as total stages.
	processDone := completed >= total
	if processDone {
		if status == StatusFailed {
			p.Status = StatusFailed
			p.Error = errStr
		} else {
			p.Status = StatusDone
		}
	} else if status == StatusFailed {
		// A stage failed but pipeline may continue (error policy).
		// Keep process StatusRunning; caller decides.
		p.Status = StatusRunning
	}

	watchers := p.watchers
	p.mu.Unlock()

	// Broadcast the completed action to all watchers.
	if updated.Stage != "" {
		t.broadcastTo(watchers, updated)
	}

	// Close watcher channels if process is terminal.
	if processDone {
		p.mu.Lock()
		for _, ch := range p.watchers {
			close(ch)
		}
		p.watchers = nil
		p.mu.Unlock()
	}

	return result, nil
}

// broadcast sends an action to all current watchers of a process.
func (t *Tracker) broadcast(p *process, action Action) {
	p.mu.Lock()
	watchers := p.watchers
	p.mu.Unlock()
	t.broadcastTo(watchers, action)
}

func (t *Tracker) broadcastTo(watchers []chan Action, action Action) {
	for _, ch := range watchers {
		select {
		case ch <- action:
		default:
			// Slow consumer; drop. Channel is buffered at 32.
		}
	}
}

// processIDFromEnv extracts the process tracking ID from the envelope Meta.
// Pepper injects "_process_id" into Meta before dispatch when tracking is on.
func processIDFromEnv(env *envelope.Envelope) string {
	if env.Meta == nil {
		return ""
	}
	v, _ := env.Meta["_process_id"].(string)
	return v
}

func copyMeta(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
