package compose

// Regression tests for pipeline stage counting bugs.
//
// Track() used dag.StageCount() for TotalStages, which counts ALL stages
// including router-side ones (Transform, Branch, Return). The tracker never
// reached 100% because it expected N stages but only N-router_side stages
// ever dispatched to workers. The pipeline appeared stuck at 75% even after
// completing, because the polling loop waited for Status=="done" which required
// PercentDone==100.
//
// dag.WorkerStageCount() counts only Dispatch and Scatter stages.

import (
	"testing"

	"github.com/agberohq/pepper/internal/envelope"
)

// TestWorkerStageCountExcludesTransform is the direct regression for the 75%
// tracker bug. A 4-stage pipeline with 1 Transform has WorkerStageCount()=3.
func TestWorkerStageCountExcludesTransform(t *testing.T) {
	dag, err := Compile("test.pipeline", []Stage{
		Pipe("audio.convert").WithGroup("cpu"),
		Pipe("speech.transcribe").WithGroup("cpu"),
		Transform(func(_ envelope.Envelope, in map[string]any) (map[string]any, error) {
			return in, nil
		}),
		Pipe("song.analyze").WithGroup("default"),
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	if got := dag.StageCount(); got != 4 {
		t.Errorf("StageCount: got %d, want 4", got)
	}
	if got := dag.WorkerStageCount(); got != 3 {
		t.Errorf("WorkerStageCount: got %d, want 3 — regression of tracker 75%% bug (Transform counted as worker stage)", got)
	}
}

// TestWorkerStageCountAllPipe verifies all-Pipe pipeline counts all stages.
func TestWorkerStageCountAllPipe(t *testing.T) {
	dag, err := Compile("all.pipe", []Stage{
		Pipe("a"), Pipe("b"), Pipe("c"),
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got := dag.WorkerStageCount(); got != 3 {
		t.Errorf("WorkerStageCount: got %d, want 3", got)
	}
}

// TestWorkerStageCountReturn verifies Return stages are excluded.
func TestWorkerStageCountReturn(t *testing.T) {
	dag, err := Compile("with.return", []Stage{
		Pipe("a"),
		Return(map[string]any{"ok": true}),
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got := dag.WorkerStageCount(); got != 1 {
		t.Errorf("WorkerStageCount: got %d, want 1 (Return is router-side)", got)
	}
}

// TestWorkerStageCountScatterIncluded verifies Scatter stages ARE counted
// since they dispatch to workers.
func TestWorkerStageCountScatterIncluded(t *testing.T) {
	dag, err := Compile("with.scatter", []Stage{
		Pipe("stage.a"),
		Scatter("fan.out").Gather("gather.results", GatherAll),
		Pipe("stage.b"),
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if got := dag.WorkerStageCount(); got != 3 {
		t.Errorf("WorkerStageCount: got %d, want 3 (Scatter dispatches to workers)", got)
	}
}

// TestWorkerStageCountConsistentWithStageKind cross-checks WorkerStageCount
// against StageKind to ensure no stage type is miscategorised.
func TestWorkerStageCountConsistentWithStageKind(t *testing.T) {
	dag, err := Compile("consistency.check", []Stage{
		Pipe("a"),
		Transform(func(_ envelope.Envelope, in map[string]any) (map[string]any, error) {
			return in, nil
		}),
		Pipe("b"),
		Return(map[string]any{}),
		Pipe("c"),
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Manual count using StageKind.
	expected := 0
	for i := 0; i < dag.StageCount(); i++ {
		k, err := dag.StageKind(i)
		if err != nil {
			t.Fatalf("StageKind(%d): %v", i, err)
		}
		if k == StageDispatch || k == StageScatter {
			expected++
		}
	}

	if got := dag.WorkerStageCount(); got != expected {
		t.Errorf("WorkerStageCount()=%d inconsistent with manual StageKind count=%d", got, expected)
	}
}
