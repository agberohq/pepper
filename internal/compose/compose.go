package compose

import (
	"fmt"
	"strings"
	"time"

	"github.com/agberohq/pepper/internal/envelope"
)

// Stage is a single node in a pipeline DAG.
type Stage interface {
	stageType() string
	validate() error
}

// Worker-dispatch stages

// PipeStage is a sequential capability invocation.
type PipeStage struct {
	Cap         string
	Group       string
	CapVer      string
	ErrorPolicy ErrorPolicy
}

func (p *PipeStage) stageType() string { return "pipe" }
func (p *PipeStage) validate() error {
	if p.Cap == "" {
		return fmt.Errorf("compose.Pipe: capability name is required")
	}
	return nil
}

func (p *PipeStage) WithGroup(group string) *PipeStage     { p.Group = group; return p }
func (p *PipeStage) WithVersion(v string) *PipeStage       { p.CapVer = v; return p }
func (p *PipeStage) OnError(policy ErrorPolicy) *PipeStage { p.ErrorPolicy = policy; return p }

// ScatterStage fans out to all workers in a group and collects results.
// The router handles the gather internally before continuing to the next stage.
type ScatterStage struct {
	Cap            string
	Group          string
	GatherCap      string
	GatherStrategy GatherMode
	GatherQuorum   uint8
	GatherTimeout  time.Duration
	ErrorPolicy    ErrorPolicy
}

func (s *ScatterStage) stageType() string { return "scatter" }
func (s *ScatterStage) validate() error {
	if s.Cap == "" {
		return fmt.Errorf("compose.Scatter: capability name is required")
	}
	if s.GatherCap == "" {
		return fmt.Errorf("compose.Scatter: Gather() is required")
	}
	return nil
}

func (s *ScatterStage) WithGroup(group string) *ScatterStage { s.Group = group; return s }

func (s *ScatterStage) Gather(gatherCap string, mode GatherMode, opts ...GatherOption) *ScatterStage {
	s.GatherCap = gatherCap
	s.GatherStrategy = mode
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *ScatterStage) OnError(policy ErrorPolicy) *ScatterStage { s.ErrorPolicy = policy; return s }

// Router-side stages

// TransformStage applies a Go-side function between stages.
// Workers never see this — the router applies it before forwarding.
type TransformStage struct {
	Fn func(envelope.Envelope, map[string]any) (map[string]any, error)
}

func (t *TransformStage) stageType() string { return "transform" }
func (t *TransformStage) validate() error {
	if t.Fn == nil {
		return fmt.Errorf("compose.Transform: function is required")
	}
	return nil
}

// BranchStage routes to different capabilities based on output field values.
// Evaluated by the router; never dispatched to a worker.
type BranchStage struct {
	Arms []BranchArm
}

type BranchArm struct {
	Condition string
	Stage     Stage
}

func (b *BranchStage) stageType() string { return "branch" }
func (b *BranchStage) validate() error {
	if len(b.Arms) == 0 {
		return fmt.Errorf("compose.Branch: at least one When or Otherwise is required")
	}
	return nil
}

// ParallelStage runs multiple stages simultaneously; the router waits for all.
type ParallelStage struct {
	Stages []Stage
}

func (p *ParallelStage) stageType() string { return "parallel" }
func (p *ParallelStage) validate() error {
	if len(p.Stages) < 2 {
		return fmt.Errorf("compose.Parallel: at least 2 stages required")
	}
	return nil
}

// ReturnStage short-circuits the pipeline and returns a fixed value.
type ReturnStage struct {
	Value map[string]any
}

func (r *ReturnStage) stageType() string { return "return" }
func (r *ReturnStage) validate() error   { return nil }

// Fluent constructors

func Pipe(cap string) *PipeStage       { return &PipeStage{Cap: cap} }
func Scatter(cap string) *ScatterStage { return &ScatterStage{Cap: cap} }

func Transform(fn func(envelope.Envelope, map[string]any) (map[string]any, error)) *TransformStage {
	return &TransformStage{Fn: fn}
}

func Branch(arms ...BranchArm) *BranchStage { return &BranchStage{Arms: arms} }
func When(condition string, stage Stage) BranchArm {
	return BranchArm{Condition: condition, Stage: stage}
}
func Otherwise(stage Stage) BranchArm {
	return BranchArm{Condition: "_otherwise", Stage: stage}
}

func Parallel(stages ...Stage) *ParallelStage  { return &ParallelStage{Stages: stages} }
func Return(value map[string]any) *ReturnStage { return &ReturnStage{Value: value} }

// Error policy

// ErrorPolicy controls what happens when a stage fails.
// Use the constructor functions — do not compare to integer constants.
type ErrorPolicy struct {
	kind        errorPolicyKind
	maxRetries  int
	fallbackCap string
}

type errorPolicyKind int

const (
	policyAbort errorPolicyKind = iota
	policyRetry
	policyFallback
	policySkip
)

// AbortPipeline is the default: fail the whole pipeline on stage error.
var AbortPipeline = ErrorPolicy{kind: policyAbort}

// SkipStage skips the failed stage and continues with an empty result.
var SkipStage = ErrorPolicy{kind: policySkip}

// RetryStage retries this stage up to n times before applying AbortPipeline.
func RetryStage(n int) ErrorPolicy {
	return ErrorPolicy{kind: policyRetry, maxRetries: n}
}

// FallbackStage tries fallbackCap if the primary stage fails.
func FallbackStage(fallbackCap string) ErrorPolicy {
	return ErrorPolicy{kind: policyFallback, fallbackCap: fallbackCap}
}

// Gather

// GatherMode controls how scatter results are collected.
type GatherMode string

const (
	GatherAll     GatherMode = "all"
	GatherFirst   GatherMode = "first"
	GatherQuorum  GatherMode = "quorum"
	GatherTimeout GatherMode = "timeout"
)

// GatherOption configures scatter-gather behaviour.
type GatherOption func(*ScatterStage)

// WithGatherQuorum sets the minimum responses needed for quorum gather.
func WithGatherQuorum(n uint8) GatherOption {
	return func(s *ScatterStage) { s.GatherQuorum = n }
}

// WithGatherTimeout sets the maximum time to wait for scattered results.
func WithGatherTimeout(d time.Duration) GatherOption {
	return func(s *ScatterStage) { s.GatherTimeout = d }
}

// Topic helpers

func pipeTopicForStage(pipelineName string, stageIdx int) string {
	return fmt.Sprintf("pepper.pipe.%s.stage.%d", sanitizeName(pipelineName), stageIdx)
}

func pipeTopicForArm(pipelineName string, stageIdx int, condition string) string {
	safe := strings.NewReplacer(" ", "_", "'", "", `"`, "", ">", "gt", "<", "lt", "=", "eq").
		Replace(condition)
	return fmt.Sprintf("pepper.pipe.%s.branch.%d.%s", sanitizeName(pipelineName), stageIdx, safe)
}

func sanitizeName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}
