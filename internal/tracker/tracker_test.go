package tracker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/hooks"
)

// helpers

// makeEnv builds a minimal envelope with the given corrID, cap, and optional
// _process_id in Meta. This is what Pepper injects before dispatching.
func makeEnv(corrID, cap, processID string) *envelope.Envelope {
	env := envelope.DefaultEnvelope()
	env.CorrID = corrID
	env.Cap = cap
	if processID != "" {
		env.Meta = map[string]any{"_process_id": processID}
	}
	return &env
}

func okResult(workerID string) hooks.Result {
	return hooks.Result{WorkerID: workerID, Cap: ""}
}

func errResult(workerID string, err error) hooks.Result {
	return hooks.Result{WorkerID: workerID, Err: err}
}

// fireStage simulates one full stage: Before → After.
func fireStage(t *Tracker, corrID, cap, processID, workerID string, stageErr error) {
	t.Helper()
	ctx := context.Background()
	env := makeEnv(corrID, cap, processID)
	t.before(ctx, env, hooks.In{})
	res := okResult(workerID)
	if stageErr != nil {
		res = errResult(workerID, stageErr)
	}
	t.after(ctx, env, hooks.In{}, res)
}

// Helper to assert within tests
func (t *Tracker) Helper() {} // satisfies t.Helper() call pattern — no-op on Tracker

// unit tests

// TestRegisterAndCheck verifies that RegisterProcess creates a process with
// the expected initial state, and Check returns a correct snapshot.
func TestRegisterAndCheck(t *testing.T) {
	trk := New()

	trk.RegisterProcess("p1", "my.pipeline", 3)

	state, ok := trk.Check("p1")
	if !ok {
		t.Fatal("Check: expected process to exist")
	}
	if state.ID != "p1" {
		t.Errorf("ID: got %q, want %q", state.ID, "p1")
	}
	if state.Cap != "my.pipeline" {
		t.Errorf("Cap: got %q, want %q", state.Cap, "my.pipeline")
	}
	if state.Status != StatusPending {
		t.Errorf("Status: got %q, want %q", state.Status, StatusPending)
	}
	if state.TotalStages != 3 {
		t.Errorf("TotalStages: got %d, want 3", state.TotalStages)
	}
	if state.PercentDone != 0 {
		t.Errorf("PercentDone: got %d, want 0", state.PercentDone)
	}
	if len(state.Actions) != 0 {
		t.Errorf("Actions: got %d, want 0", len(state.Actions))
	}
}

// TestCheckUnknown verifies Check returns false for an unknown processID.
func TestCheckUnknown(t *testing.T) {
	trk := New()
	_, ok := trk.Check("does-not-exist")
	if ok {
		t.Fatal("Check: expected false for unknown processID")
	}
}

// TestBeforeRecordsRunningAction verifies the Before hook records a running action.
func TestBeforeRecordsRunningAction(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ctx := context.Background()
	env := makeEnv("corr-1", "cap.a", "p1")
	trk.before(ctx, env, hooks.In{})

	state, _ := trk.Check("p1")
	if state.Status != StatusRunning {
		t.Errorf("process Status: got %q, want %q", state.Status, StatusRunning)
	}
	if len(state.Actions) != 1 {
		t.Fatalf("Actions: got %d, want 1", len(state.Actions))
	}
	a := state.Actions[0]
	if a.Stage != "cap.a" {
		t.Errorf("Action.Stage: got %q, want %q", a.Stage, "cap.a")
	}
	if a.Status != StatusRunning {
		t.Errorf("Action.Status: got %q, want %q", a.Status, StatusRunning)
	}
	if a.StartedAt.IsZero() {
		t.Error("Action.StartedAt: should not be zero")
	}
	if a.DurationMs != 0 {
		t.Errorf("Action.DurationMs: got %d, want 0 (still running)", a.DurationMs)
	}
}

// TestAfterCompletesAction verifies the After hook updates the action to done
// with timing and worker ID filled in.
func TestAfterCompletesAction(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ctx := context.Background()
	env := makeEnv("corr-1", "cap.a", "p1")

	trk.before(ctx, env, hooks.In{})
	time.Sleep(2 * time.Millisecond) // ensure non-zero duration
	trk.after(ctx, env, hooks.In{}, okResult("w-1"))

	state, _ := trk.Check("p1")
	if state.Status != StatusDone {
		t.Errorf("process Status: got %q, want %q", state.Status, StatusDone)
	}
	if state.PercentDone != 100 {
		t.Errorf("PercentDone: got %d, want 100", state.PercentDone)
	}

	a := state.Actions[0]
	if a.Status != StatusDone {
		t.Errorf("Action.Status: got %q, want %q", a.Status, StatusDone)
	}
	if a.WorkerID != "w-1" {
		t.Errorf("Action.WorkerID: got %q, want %q", a.WorkerID, "w-1")
	}
	if a.DurationMs <= 0 {
		t.Errorf("Action.DurationMs: got %d, want > 0", a.DurationMs)
	}
	if a.FinishedAt.IsZero() {
		t.Error("Action.FinishedAt: should not be zero")
	}
	if a.Error != "" {
		t.Errorf("Action.Error: got %q, want empty", a.Error)
	}
}

// TestAfterFailedAction verifies the After hook records StatusFailed and the error message.
func TestAfterFailedAction(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ctx := context.Background()
	env := makeEnv("corr-1", "cap.a", "p1")

	trk.before(ctx, env, hooks.In{})
	trk.after(ctx, env, hooks.In{}, errResult("w-1", errors.New("worker exploded")))

	state, _ := trk.Check("p1")
	if state.Status != StatusFailed {
		t.Errorf("process Status: got %q, want %q", state.Status, StatusFailed)
	}
	if state.Error != "worker exploded" {
		t.Errorf("process Error: got %q, want %q", state.Error, "worker exploded")
	}
	if state.PercentDone != 100 {
		t.Errorf("PercentDone: got %d, want 100 (failed stage counts as completed)", state.PercentDone)
	}

	a := state.Actions[0]
	if a.Status != StatusFailed {
		t.Errorf("Action.Status: got %q, want %q", a.Status, StatusFailed)
	}
	if a.Error != "worker exploded" {
		t.Errorf("Action.Error: got %q, want %q", a.Error, "worker exploded")
	}
}

// TestMultiStagePipeline verifies percent done advances correctly across stages.
func TestMultiStagePipeline(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "song.pipeline", 4)

	stages := []string{"audio.convert", "speech.transcribe", "transform", "song.analyze"}

	for i, stage := range stages {
		corrID := stage + "-corr"
		fireStage(trk, corrID, stage, "p1", "w-asr", nil)

		state, _ := trk.Check("p1")
		want := ((i + 1) * 100) / 4
		if state.PercentDone != want {
			t.Errorf("after stage %d (%s): PercentDone got %d, want %d",
				i+1, stage, state.PercentDone, want)
		}
	}

	state, _ := trk.Check("p1")
	if state.Status != StatusDone {
		t.Errorf("final Status: got %q, want %q", state.Status, StatusDone)
	}
	if len(state.Actions) != 4 {
		t.Errorf("Actions: got %d, want 4", len(state.Actions))
	}

	// Verify each action has timing recorded.
	for _, a := range state.Actions {
		if a.DurationMs < 0 {
			t.Errorf("action %s: DurationMs should be >= 0, got %d", a.Stage, a.DurationMs)
		}
		if a.Status != StatusDone {
			t.Errorf("action %s: Status got %q, want %q", a.Stage, a.Status, StatusDone)
		}
	}
}

// TestPercentDoneIntermediateFail verifies percent still advances when one
// stage fails mid-pipeline (pipeline may continue depending on error policy).
func TestPercentDoneIntermediateFail(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "my.pipeline", 3)

	fireStage(trk, "c1", "stage.a", "p1", "w-1", nil)
	fireStage(trk, "c2", "stage.b", "p1", "w-1", errors.New("oops"))

	state, _ := trk.Check("p1")
	// 2 of 3 completed (one done, one failed — both count)
	if state.PercentDone != 66 {
		t.Errorf("PercentDone: got %d, want 66", state.PercentDone)
	}
	// Process is still running — 3rd stage hasn't fired yet.
	if state.Status != StatusRunning {
		t.Errorf("Status: got %q, want %q (pipeline may continue)", state.Status, StatusRunning)
	}
}

// TestCorrIDFallback verifies After can find the process via corrID index
// even when Meta is absent from the envelope (e.g. stripped by a middleware).
func TestCorrIDFallback(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ctx := context.Background()

	// Before: envelope has _process_id in Meta — corrID gets indexed.
	envBefore := makeEnv("corr-1", "cap.a", "p1")
	trk.before(ctx, envBefore, hooks.In{})

	// After: envelope has NO Meta — must fall back to corrID index.
	envAfter := makeEnv("corr-1", "cap.a", "")
	trk.after(ctx, envAfter, hooks.In{}, okResult("w-1"))

	state, _ := trk.Check("p1")
	if state.Status != StatusDone {
		t.Errorf("Status: got %q, want StatusDone (corrID fallback failed)", state.Status)
	}
	if state.Actions[0].WorkerID != "w-1" {
		t.Errorf("WorkerID: got %q, want w-1", state.Actions[0].WorkerID)
	}
}

// TestCorrIDIndexCleanedUp verifies the corrID index entry is removed after After fires.
func TestCorrIDIndexCleanedUp(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ctx := context.Background()
	env := makeEnv("corr-42", "cap.a", "p1")

	trk.before(ctx, env, hooks.In{})
	// corrID should be indexed now
	_, indexedBefore := trk.corrIndex.Get("corr-42")
	if !indexedBefore {
		t.Fatal("corrID should be in index after Before")
	}

	trk.after(ctx, env, hooks.In{}, okResult("w-1"))
	// corrID should be cleaned up after After
	_, indexedAfter := trk.corrIndex.Get("corr-42")
	if indexedAfter {
		t.Fatal("corrID should be removed from index after After")
	}
}

// TestFailProcess verifies FailProcess sets StatusFailed from outside hook path
// (e.g. dispatch failure before Before even fires).
func TestFailProcess(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "my.cap", 1)

	trk.FailProcess("p1", errors.New("bus unreachable"))

	state, ok := trk.Check("p1")
	if !ok {
		t.Fatal("process should still exist after FailProcess")
	}
	if state.Status != StatusFailed {
		t.Errorf("Status: got %q, want %q", state.Status, StatusFailed)
	}
	if state.Error != "bus unreachable" {
		t.Errorf("Error: got %q, want %q", state.Error, "bus unreachable")
	}
}

// TestFailProcessUnknown verifies FailProcess on unknown ID is a no-op (no panic).
func TestFailProcessUnknown(t *testing.T) {
	trk := New()
	trk.FailProcess("ghost", errors.New("whatever")) // must not panic
}

// TestWatchReceivesEvents verifies Watch receives StatusRunning then StatusDone.
func TestWatchReceivesEvents(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ch := trk.Watch("p1")
	if ch == nil {
		t.Fatal("Watch: expected non-nil channel")
	}

	ctx := context.Background()
	env := makeEnv("corr-1", "cap.a", "p1")

	trk.before(ctx, env, hooks.In{})
	trk.after(ctx, env, hooks.In{}, okResult("w-1"))

	// Collect all events — channel must close after final stage.
	var events []Action
	for a := range ch {
		events = append(events, a)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events (running + done), got %d", len(events))
	}
	if events[0].Status != StatusRunning {
		t.Errorf("event[0].Status: got %q, want %q", events[0].Status, StatusRunning)
	}
	if events[1].Status != StatusDone {
		t.Errorf("event[1].Status: got %q, want %q", events[1].Status, StatusDone)
	}
}

// TestWatchMultiStageCloses verifies the channel closes exactly once after all stages complete.
func TestWatchMultiStageCloses(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "pipe", 3)
	ch := trk.Watch("p1")

	go func() {
		for _, stage := range []string{"s1", "s2", "s3"} {
			fireStage(trk, stage+"-c", stage, "p1", "w-1", nil)
		}
	}()

	// Drain and count. Channel must close (range exits) when all 3 stages done.
	timeout := time.After(2 * time.Second)
	var events []Action
loop:
	for {
		select {
		case a, ok := <-ch:
			if !ok {
				break loop
			}
			events = append(events, a)
		case <-timeout:
			t.Fatal("timeout: Watch channel never closed")
		}
	}

	// 3 stages × 2 events (running + done) = 6
	if len(events) != 6 {
		t.Errorf("expected 6 events, got %d", len(events))
	}
}

// TestWatchUnknown verifies Watch returns nil for an unknown processID.
func TestWatchUnknown(t *testing.T) {
	trk := New()
	ch := trk.Watch("nobody")
	if ch != nil {
		t.Error("Watch: expected nil for unknown processID")
	}
}

// TestWatchAlreadyDone verifies Watch returns a closed channel if the process
// is already complete at the time of the call.
func TestWatchAlreadyDone(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	// Complete the process first.
	fireStage(trk, "c1", "cap.a", "p1", "w-1", nil)

	// Now subscribe — should get a closed channel immediately.
	ch := trk.Watch("p1")
	if ch == nil {
		t.Fatal("Watch: expected non-nil channel even for completed process")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed (no buffered items after done)")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Watch channel not closed for completed process")
	}
}

// TestWatchFailProcess verifies Watch channel closes when FailProcess is called.
func TestWatchFailProcess(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ch := trk.Watch("p1")

	trk.FailProcess("p1", errors.New("bus down"))

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel closed after FailProcess")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Watch channel not closed after FailProcess")
	}
}

// TestMultipleWatchers verifies multiple Watch subscribers all receive events.
func TestMultipleWatchers(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ch1 := trk.Watch("p1")
	ch2 := trk.Watch("p1")
	ch3 := trk.Watch("p1")

	fireStage(trk, "c1", "cap.a", "p1", "w-1", nil)

	drain := func(ch <-chan Action) []Action {
		var out []Action
		for a := range ch {
			out = append(out, a)
		}
		return out
	}

	var wg sync.WaitGroup
	results := make([][]Action, 3)
	for i, ch := range []<-chan Action{ch1, ch2, ch3} {
		wg.Add(1)
		go func(idx int, c <-chan Action) {
			defer wg.Done()
			results[idx] = drain(c)
		}(i, ch)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watchers to drain")
	}

	for i, events := range results {
		if len(events) != 2 {
			t.Errorf("watcher %d: expected 2 events, got %d", i, len(events))
		}
	}
}

// TestBeforeNoProcessID verifies Before is a no-op when _process_id is absent.
func TestBeforeNoProcessID(t *testing.T) {
	trk := New()

	ctx := context.Background()
	env := makeEnv("corr-1", "cap.a", "") // no _process_id

	// Must not panic and must not create any state.
	in, err := trk.before(ctx, env, hooks.In{"x": 1})
	if err != nil {
		t.Errorf("before: unexpected error: %v", err)
	}
	if in != nil {
		t.Error("before: expected nil in (no mutation) when no processID")
	}
	if trk.m.Len() != 0 {
		t.Error("before: no process should be registered without _process_id")
	}
}

// TestAfterNoProcessID verifies After is a no-op when neither Meta nor corrIndex
// has a processID.
func TestAfterNoProcessID(t *testing.T) {
	trk := New()

	ctx := context.Background()
	env := makeEnv("corr-orphan", "cap.a", "")
	res := okResult("w-1")

	// Must not panic and must return result unchanged.
	out, err := trk.after(ctx, env, hooks.In{}, res)
	if err != nil {
		t.Errorf("after: unexpected error: %v", err)
	}
	if out.WorkerID != res.WorkerID {
		t.Errorf("after: result modified unexpectedly")
	}
}

// TestHookReturnsCorrectNames verifies the Hook() method returns the right hook name.
func TestHookReturnsCorrectNames(t *testing.T) {
	trk := New()
	h := trk.Hook()
	if h.Name != "pepper.tracker" {
		t.Errorf("Hook.Name: got %q, want %q", h.Name, "pepper.tracker")
	}
	if h.Before == nil {
		t.Error("Hook.Before: should not be nil")
	}
	if h.After == nil {
		t.Error("Hook.After: should not be nil")
	}
}

// TestConcurrentStages verifies concurrent stage execution on different
// capabilities within the same process does not corrupt state.
func TestConcurrentStages(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "parallel.pipeline", 10)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			stage := "stage." + string(rune('a'+idx))
			corrID := stage + "-corr"
			fireStage(trk, corrID, stage, "p1", "w-1", nil)
		}(i)
	}
	wg.Wait()

	state, _ := trk.Check("p1")
	if state.PercentDone != 100 {
		t.Errorf("PercentDone: got %d, want 100", state.PercentDone)
	}
	if state.Status != StatusDone {
		t.Errorf("Status: got %q, want %q", state.Status, StatusDone)
	}
	if len(state.Actions) != 10 {
		t.Errorf("Actions: got %d, want 10", len(state.Actions))
	}
}

// TestConcurrentProcesses verifies multiple independent processes tracked
// simultaneously don't interfere with each other.
func TestConcurrentProcesses(t *testing.T) {
	trk := New()
	n := 20

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pid := "proc-" + string(rune('a'+idx%26)) // unique enough for test
			pid = pid + "-" + string(rune('0'+idx))
			trk.RegisterProcess(pid, "my.cap", 1)
			fireStage(trk, pid+"-corr", "my.cap", pid, "w-1", nil)
		}(i)
	}
	wg.Wait()

	// All processes should be done.
	count := 0
	trk.m.Range(func(_ string, p *process) bool {
		p.mu.Lock()
		s := p.Status
		p.mu.Unlock()
		if s != StatusDone {
			t.Errorf("process %s: got Status %q, want %q", p.ID, s, StatusDone)
		}
		count++
		return true
	})
	if count != n {
		t.Errorf("expected %d processes, found %d", n, count)
	}
}

// TestActionMetaCopied verifies envelope Meta is deep-copied into the Action,
// so later mutations to the envelope don't affect the recorded action.
func TestActionMetaCopied(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ctx := context.Background()
	env := makeEnv("corr-1", "cap.a", "p1")
	env.Meta["extra"] = "original"

	trk.before(ctx, env, hooks.In{})

	// Mutate the envelope Meta after Before fires.
	env.Meta["extra"] = "mutated"

	state, _ := trk.Check("p1")
	if state.Actions[0].Meta["extra"] != "original" {
		t.Errorf("Action.Meta was not deep-copied: got %q, want %q",
			state.Actions[0].Meta["extra"], "original")
	}
}

// TestTimingAccuracy verifies DurationMs is within a reasonable range.
func TestTimingAccuracy(t *testing.T) {
	trk := New()
	trk.RegisterProcess("p1", "cap.a", 1)

	ctx := context.Background()
	env := makeEnv("corr-1", "cap.a", "p1")

	trk.before(ctx, env, hooks.In{})
	sleep := 20 * time.Millisecond
	time.Sleep(sleep)
	trk.after(ctx, env, hooks.In{}, okResult("w-1"))

	state, _ := trk.Check("p1")
	d := state.Actions[0].DurationMs
	// Allow generous range for CI jitter: between 15ms and 500ms.
	if d < 15 || d > 500 {
		t.Errorf("DurationMs: got %dms, expected roughly %dms (15–500ms range)",
			d, sleep.Milliseconds())
	}
}
