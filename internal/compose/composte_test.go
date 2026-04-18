package compose

import (
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/envelope"
)

// constructors

func TestPipe(t *testing.T) {
	s := Pipe("face.recognize").WithGroup("gpu").WithVersion(">=1.0.0").OnError(RetryStage(2))
	if s.Cap != "face.recognize" {
		t.Errorf("Cap = %q, want face.recognize", s.Cap)
	}
	if s.Group != "gpu" {
		t.Errorf("Group = %q, want gpu", s.Group)
	}
	if s.CapVer != ">=1.0.0" {
		t.Errorf("CapVer = %q, want >=1.0.0", s.CapVer)
	}
	if s.ErrorPolicy != RetryStage(2) {
		t.Error("ErrorPolicy mismatch")
	}
	if s.stageType() != "pipe" {
		t.Errorf("stageType = %q, want pipe", s.stageType())
	}
}

func TestScatter(t *testing.T) {
	s := Scatter("speech.transcribe").
		WithGroup("asr").
		Gather("transcript.merge", GatherAll, WithGatherQuorum(3), WithGatherTimeout(10*time.Second)).
		OnError(FallbackStage("speech.transcribe.lite"))

	if s.Cap != "speech.transcribe" {
		t.Errorf("Cap = %q", s.Cap)
	}
	if s.Group != "asr" {
		t.Errorf("Group = %q", s.Group)
	}
	if s.GatherCap != "transcript.merge" {
		t.Errorf("GatherCap = %q", s.GatherCap)
	}
	if s.GatherStrategy != GatherAll {
		t.Errorf("GatherStrategy = %q", s.GatherStrategy)
	}
	if s.GatherQuorum != 3 {
		t.Errorf("GatherQuorum = %d, want 3", s.GatherQuorum)
	}
	if s.GatherTimeout != 10*time.Second {
		t.Errorf("GatherTimeout = %v, want 10s", s.GatherTimeout)
	}
	if s.ErrorPolicy != FallbackStage("speech.transcribe.lite") {
		t.Error("ErrorPolicy mismatch")
	}
}

func TestTransform(t *testing.T) {
	fn := func(e envelope.Envelope, m map[string]any) (map[string]any, error) { return m, nil }
	s := Transform(fn)
	if s.Fn == nil {
		t.Fatal("Fn is nil")
	}
	if s.stageType() != "transform" {
		t.Errorf("stageType = %q, want transform", s.stageType())
	}
}

func TestBranch(t *testing.T) {
	s := Branch(
		When("lang == 'en'", Pipe("en.diarize")),
		When("lang == 'fr'", Pipe("fr.diarize")),
		Otherwise(Pipe("generic.diarize")),
	)
	if len(s.Arms) != 3 {
		t.Fatalf("len(Arms) = %d, want 3", len(s.Arms))
	}
	if s.Arms[0].Condition != "lang == 'en'" {
		t.Errorf("Arm[0].Condition = %q", s.Arms[0].Condition)
	}
	if s.Arms[2].Condition != "_otherwise" {
		t.Errorf("Otherwise condition = %q, want _otherwise", s.Arms[2].Condition)
	}
}

func TestParallel(t *testing.T) {
	s := Parallel(
		Pipe("sentiment.analyze").WithGroup("cpu"),
		Pipe("topic.classify").WithGroup("cpu"),
	)
	if len(s.Stages) != 2 {
		t.Fatalf("len(Stages) = %d, want 2", len(s.Stages))
	}
}

func TestReturn(t *testing.T) {
	s := Return(map[string]any{"matches": nil})
	if s.Value["matches"] != nil {
		t.Error("Value mismatch")
	}
}

// validation

func TestPipeValidate(t *testing.T) {
	if err := Pipe("ok").validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := (&PipeStage{}).validate(); err == nil {
		t.Error("expected error for empty Cap")
	}
}

func TestScatterValidate(t *testing.T) {
	if err := Scatter("ok").Gather("g", GatherAll).validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := Scatter("").Gather("g", GatherAll).validate(); err == nil {
		t.Error("expected error for empty Cap")
	}
	if err := Scatter("ok").validate(); err == nil {
		t.Error("expected error for missing Gather")
	}
}

func TestTransformValidate(t *testing.T) {
	if err := Transform(nil).validate(); err == nil {
		t.Error("expected error for nil Fn")
	}
}

func TestBranchValidate(t *testing.T) {
	if err := Branch().validate(); err == nil {
		t.Error("expected error for empty branch")
	}
}

func TestParallelValidate(t *testing.T) {
	if err := Parallel(Pipe("a")).validate(); err == nil {
		t.Error("expected error for single stage")
	}
}

// error policies

func TestErrorPolicyEquality(t *testing.T) {
	if AbortPipeline != AbortPipeline {
		t.Error("AbortPipeline self-equality failed")
	}
	if SkipStage != SkipStage {
		t.Error("SkipStage self-equality failed")
	}
	if RetryStage(3) != RetryStage(3) {
		t.Error("RetryStage equality failed")
	}
	if FallbackStage("x") != FallbackStage("x") {
		t.Error("FallbackStage equality failed")
	}
	if RetryStage(3) == RetryStage(4) {
		t.Error("RetryStage inequality failed")
	}
}

func TestErrorPolicyFields(t *testing.T) {
	r := RetryStage(5)
	if r.kind != policyRetry || r.maxRetries != 5 {
		t.Error("RetryStage fields wrong")
	}
	f := FallbackStage("backup")
	if f.kind != policyFallback || f.fallbackCap != "backup" {
		t.Error("FallbackStage fields wrong")
	}
}

// gather options

func TestGatherOptions(t *testing.T) {
	s := &ScatterStage{}
	WithGatherQuorum(7)(s)
	if s.GatherQuorum != 7 {
		t.Errorf("GatherQuorum = %d, want 7", s.GatherQuorum)
	}
	WithGatherTimeout(5 * time.Second)(s)
	if s.GatherTimeout != 5*time.Second {
		t.Errorf("GatherTimeout = %v, want 5s", s.GatherTimeout)
	}
}

// helpers

func TestSanitizeName(t *testing.T) {
	if got := sanitizeName("audio.full"); got != "audio_full" {
		t.Errorf("sanitizeName = %q, want audio_full", got)
	}
}
