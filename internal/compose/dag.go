package compose

import (
	"errors"
	"fmt"
	"time"

	"github.com/agberohq/pepper/internal/envelope"
)

// ErrRouterSideStage is returned by DispatchEnvelope for stages that are
// executed by the router (Transform, Branch, Return, Parallel) and have no
// worker envelope.
var ErrRouterSideStage = errors.New("router-side stage has no dispatch envelope")

// StageKind indicates how a stage is executed at runtime.
type StageKind string

const (
	StageDispatch  StageKind = "dispatch"  // send to worker(s)
	StageScatter   StageKind = "scatter"   // router fans out + gathers
	StageTransform StageKind = "transform" // router applies fn
	StageBranch    StageKind = "branch"    // router evaluates condition
	StageParallel  StageKind = "parallel"  // router spawns multiple
	StageReturn    StageKind = "return"    // router returns value
)

// Compile validates all stages and returns a DAG.
func Compile(name string, stages []Stage) (*DAG, error) {
	if name == "" {
		return nil, fmt.Errorf("compose.Compile: pipeline name is required")
	}
	for i, s := range stages {
		if err := s.validate(); err != nil {
			return nil, fmt.Errorf("compose.Compile: stage %d: %w", i, err)
		}
	}
	return &DAG{Name: name, Stages: stages}, nil
}

// DAG is a validated, compiled pipeline ready for dispatch.
type DAG struct {
	Name   string
	Stages []Stage
}

// StageCount returns the number of stages in the pipeline.
func (d *DAG) StageCount() int { return len(d.Stages) }

// WorkerStageCount returns the number of stages that require worker dispatch
// (Dispatch and Scatter). Router-side stages (Transform, Branch, Return,
// Parallel) are excluded since they never touch the tracker or the bus.
func (d *DAG) WorkerStageCount() int {
	count := 0
	for i := range d.Stages {
		k, _ := d.StageKind(i)
		if k == StageDispatch || k == StageScatter {
			count++
		}
	}
	return count
}

// StageKind returns the execution kind of the stage at the given index.
func (d *DAG) StageKind(stageIdx int) (StageKind, error) {
	if stageIdx < 0 || stageIdx >= len(d.Stages) {
		return "", fmt.Errorf("compose.DAG: stage index %d out of range [0,%d)", stageIdx, len(d.Stages))
	}
	switch d.Stages[stageIdx].(type) {
	case *PipeStage:
		return StageDispatch, nil
	case *ScatterStage:
		return StageScatter, nil
	case *TransformStage:
		return StageTransform, nil
	case *BranchStage:
		return StageBranch, nil
	case *ParallelStage:
		return StageParallel, nil
	case *ReturnStage:
		return StageReturn, nil
	default:
		return "", fmt.Errorf("compose.DAG: unknown stage type %T", d.Stages[stageIdx])
	}
}

// DispatchEnvelope prepares the envelope for a worker-dispatch stage.
// For router-side stages it returns ErrRouterSideStage.
func (d *DAG) DispatchEnvelope(env *envelope.Envelope, stageIdx int) error {
	if stageIdx < 0 || stageIdx >= len(d.Stages) {
		return fmt.Errorf("compose.DAG: stage index %d out of range [0,%d)", stageIdx, len(d.Stages))
	}

	current := d.Stages[stageIdx]
	nextTopic := ""
	if stageIdx+1 < len(d.Stages) {
		nextTopic = pipeTopicForStage(d.Name, stageIdx+1)
	}

	switch s := current.(type) {
	case *PipeStage:
		env.Cap = s.Cap
		env.Group = s.Group
		env.CapVer = s.CapVer
		env.ForwardTo = nextTopic

	case *ScatterStage:
		env.Cap = s.Cap
		env.Group = s.Group
		env.Dispatch = envelope.DispatchAll
		env.GatherAt = pipeTopicForStage(d.Name, stageIdx) + ".gather"
		env.GatherStrategy = string(s.GatherStrategy)
		env.GatherQuorum = s.GatherQuorum
		// After gather completes, the router forwards the gather result to the next stage.
		env.ForwardTo = nextTopic

	case *TransformStage, *BranchStage, *ParallelStage, *ReturnStage:
		return fmt.Errorf("compose.DAG: stage %d (%s): %w", stageIdx, current.stageType(), ErrRouterSideStage)

	default:
		return fmt.Errorf("compose.DAG: unknown stage type %T", current)
	}

	return nil
}

// Stage accessors for router-side execution

func (d *DAG) PipeConfig(stageIdx int) (cap, group, capVer string, policy ErrorPolicy, err error) {
	if stageIdx < 0 || stageIdx >= len(d.Stages) {
		return "", "", "", ErrorPolicy{}, fmt.Errorf("compose.DAG: stage index %d out of range", stageIdx)
	}
	s, ok := d.Stages[stageIdx].(*PipeStage)
	if !ok {
		return "", "", "", ErrorPolicy{}, fmt.Errorf("compose.DAG: stage %d is not a pipe", stageIdx)
	}
	return s.Cap, s.Group, s.CapVer, s.ErrorPolicy, nil
}

func (d *DAG) ScatterGather(stageIdx int) (gatherCap string, strategy GatherMode, quorum uint8, timeout time.Duration, err error) {
	if stageIdx < 0 || stageIdx >= len(d.Stages) {
		return "", "", 0, 0, fmt.Errorf("compose.DAG: stage index %d out of range", stageIdx)
	}
	s, ok := d.Stages[stageIdx].(*ScatterStage)
	if !ok {
		return "", "", 0, 0, fmt.Errorf("compose.DAG: stage %d is not a scatter", stageIdx)
	}
	return s.GatherCap, s.GatherStrategy, s.GatherQuorum, s.GatherTimeout, nil
}

func (d *DAG) BranchTable(stageIdx int) (map[string]string, error) {
	if stageIdx < 0 || stageIdx >= len(d.Stages) {
		return nil, fmt.Errorf("compose.DAG: stage index %d out of range", stageIdx)
	}
	s, ok := d.Stages[stageIdx].(*BranchStage)
	if !ok {
		return nil, fmt.Errorf("compose.DAG: stage %d is not a branch", stageIdx)
	}
	table := make(map[string]string, len(s.Arms))
	for _, arm := range s.Arms {
		table[arm.Condition] = pipeTopicForArm(d.Name, stageIdx, arm.Condition)
	}
	return table, nil
}

func (d *DAG) TransformFn(stageIdx int) (func(envelope.Envelope, map[string]any) (map[string]any, error), error) {
	if stageIdx < 0 || stageIdx >= len(d.Stages) {
		return nil, fmt.Errorf("compose.DAG: stage index %d out of range", stageIdx)
	}
	s, ok := d.Stages[stageIdx].(*TransformStage)
	if !ok {
		return nil, fmt.Errorf("compose.DAG: stage %d is not a transform", stageIdx)
	}
	return s.Fn, nil
}

func (d *DAG) ReturnValue(stageIdx int) (map[string]any, error) {
	if stageIdx < 0 || stageIdx >= len(d.Stages) {
		return nil, fmt.Errorf("compose.DAG: stage index %d out of range", stageIdx)
	}
	s, ok := d.Stages[stageIdx].(*ReturnStage)
	if !ok {
		return nil, fmt.Errorf("compose.DAG: stage %d is not a return", stageIdx)
	}
	return s.Value, nil
}

func (d *DAG) ParallelStages(stageIdx int) ([]Stage, error) {
	if stageIdx < 0 || stageIdx >= len(d.Stages) {
		return nil, fmt.Errorf("compose.DAG: stage index %d out of range", stageIdx)
	}
	s, ok := d.Stages[stageIdx].(*ParallelStage)
	if !ok {
		return nil, fmt.Errorf("compose.DAG: stage %d is not parallel", stageIdx)
	}
	return s.Stages, nil
}
