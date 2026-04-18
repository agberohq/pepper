package compose

import (
	"errors"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/envelope"
)

// Compile

func TestCompileEmptyName(t *testing.T) {
	_, err := Compile("", []Stage{Pipe("a")})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestCompileValid(t *testing.T) {
	dag, err := Compile("test", []Stage{Pipe("a"), Pipe("b")})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if dag.Name != "test" {
		t.Errorf("Name = %q, want test", dag.Name)
	}
	if len(dag.Stages) != 2 {
		t.Errorf("len(Stages) = %d, want 2", len(dag.Stages))
	}
}

func TestCompileValidationError(t *testing.T) {
	_, err := Compile("test", []Stage{Pipe("")})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

// StageCount / StageKind

func TestStageCount(t *testing.T) {
	dag, _ := Compile("p", []Stage{Pipe("a"), Pipe("b")})
	if dag.StageCount() != 2 {
		t.Errorf("StageCount = %d, want 2", dag.StageCount())
	}
}

func TestStageKind(t *testing.T) {
	dag, _ := Compile("p", []Stage{
		Pipe("a"),
		Scatter("b").Gather("g", GatherAll),
		Transform(func(e envelope.Envelope, m map[string]any) (map[string]any, error) { return m, nil }),
		Branch(Otherwise(Pipe("c"))),
		Parallel(Pipe("d"), Pipe("e")),
		Return(map[string]any{}),
	})

	tests := []struct {
		idx  int
		want StageKind
	}{
		{0, StageDispatch},
		{1, StageScatter},
		{2, StageTransform},
		{3, StageBranch},
		{4, StageParallel},
		{5, StageReturn},
	}

	for _, tc := range tests {
		got, err := dag.StageKind(tc.idx)
		if err != nil {
			t.Fatalf("StageKind(%d): %v", tc.idx, err)
		}
		if got != tc.want {
			t.Errorf("StageKind(%d) = %q, want %q", tc.idx, got, tc.want)
		}
	}
}

func TestStageKindOutOfRange(t *testing.T) {
	dag, _ := Compile("p", []Stage{Pipe("a")})
	if _, err := dag.StageKind(-1); err == nil {
		t.Error("expected error for negative index")
	}
	if _, err := dag.StageKind(99); err == nil {
		t.Error("expected error for out-of-range index")
	}
}

// DispatchEnvelope

func TestDispatchEnvelopePipe(t *testing.T) {
	dag, _ := Compile("pl", []Stage{
		Pipe("audio.denoise").WithGroup("cpu").WithVersion("1.0.0"),
		Pipe("text.fix").WithGroup("cpu"),
	})
	env := envelope.DefaultEnvelope()

	if err := dag.DispatchEnvelope(&env, 0); err != nil {
		t.Fatalf("DispatchEnvelope: %v", err)
	}
	if env.Cap != "audio.denoise" {
		t.Errorf("Cap = %q", env.Cap)
	}
	if env.Group != "cpu" {
		t.Errorf("Group = %q", env.Group)
	}
	if env.CapVer != "1.0.0" {
		t.Errorf("CapVer = %q", env.CapVer)
	}
	if env.ForwardTo != "pepper.pipe.pl.stage.1" {
		t.Errorf("ForwardTo = %q", env.ForwardTo)
	}

	// Last stage has no forward.
	env = envelope.DefaultEnvelope()
	if err := dag.DispatchEnvelope(&env, 1); err != nil {
		t.Fatalf("DispatchEnvelope: %v", err)
	}
	if env.ForwardTo != "" {
		t.Errorf("last stage ForwardTo = %q, want empty", env.ForwardTo)
	}
}

func TestDispatchEnvelopeScatter(t *testing.T) {
	dag, _ := Compile("pl", []Stage{
		Scatter("speech.transcribe").WithGroup("asr").Gather("transcript.merge", GatherQuorum, WithGatherQuorum(3)),
		Pipe("text.fix").WithGroup("cpu"),
	})
	env := envelope.DefaultEnvelope()

	if err := dag.DispatchEnvelope(&env, 0); err != nil {
		t.Fatalf("DispatchEnvelope: %v", err)
	}
	if env.Cap != "speech.transcribe" {
		t.Errorf("Cap = %q", env.Cap)
	}
	if env.Dispatch != envelope.DispatchAll {
		t.Errorf("Dispatch = %q, want all", env.Dispatch)
	}
	if env.GatherStrategy != string(GatherQuorum) {
		t.Errorf("GatherStrategy = %q", env.GatherStrategy)
	}
	if env.GatherQuorum != 3 {
		t.Errorf("GatherQuorum = %d", env.GatherQuorum)
	}
	if env.GatherAt != "pepper.pipe.pl.stage.0.gather" {
		t.Errorf("GatherAt = %q", env.GatherAt)
	}
	if env.ForwardTo != "pepper.pipe.pl.stage.1" {
		t.Errorf("ForwardTo = %q, want next stage topic", env.ForwardTo)
	}
}

func TestDispatchEnvelopeRouterSide(t *testing.T) {
	dag, _ := Compile("pl", []Stage{
		Transform(func(e envelope.Envelope, m map[string]any) (map[string]any, error) { return m, nil }),
		Branch(Otherwise(Pipe("a"))),
		Parallel(Pipe("b"), Pipe("c")),
		Return(map[string]any{"x": 1}),
	})

	for i := 0; i < 4; i++ {
		env := envelope.DefaultEnvelope()
		err := dag.DispatchEnvelope(&env, i)
		if !errors.Is(err, ErrRouterSideStage) {
			t.Errorf("stage %d: expected ErrRouterSideStage, got %v", i, err)
		}
	}
}

func TestDispatchEnvelopeOutOfRange(t *testing.T) {
	dag, _ := Compile("pl", []Stage{Pipe("a")})
	env := envelope.DefaultEnvelope()
	if err := dag.DispatchEnvelope(&env, -1); err == nil {
		t.Error("expected error for negative index")
	}
	if err := dag.DispatchEnvelope(&env, 99); err == nil {
		t.Error("expected error for out-of-range index")
	}
}

// Stage accessors

func TestPipeConfig(t *testing.T) {
	dag, _ := Compile("pl", []Stage{Pipe("cap").WithGroup("g").WithVersion("v").OnError(SkipStage)})
	cap, group, ver, policy, err := dag.PipeConfig(0)
	if err != nil {
		t.Fatalf("PipeConfig: %v", err)
	}
	if cap != "cap" || group != "g" || ver != "v" || policy != SkipStage {
		t.Errorf("unexpected values: %s %s %s %v", cap, group, ver, policy)
	}
}

func TestPipeConfigWrongType(t *testing.T) {
	dag, _ := Compile("pl", []Stage{
		Transform(func(e envelope.Envelope, m map[string]any) (map[string]any, error) { return m, nil }),
	})
	_, _, _, _, err := dag.PipeConfig(0)
	if err == nil {
		t.Error("expected error for non-pipe stage")
	}
}

func TestScatterGather(t *testing.T) {
	dag, _ := Compile("pl", []Stage{
		Scatter("s").WithGroup("g").Gather("gc", GatherFirst, WithGatherTimeout(7*time.Second)),
	})
	gc, mode, quorum, timeout, err := dag.ScatterGather(0)
	if err != nil {
		t.Fatalf("ScatterGather: %v", err)
	}
	if gc != "gc" || mode != GatherFirst || quorum != 0 || timeout != 7*time.Second {
		t.Errorf("unexpected values: %s %s %d %v", gc, mode, quorum, timeout)
	}
}

func TestBranchTable(t *testing.T) {
	dag, _ := Compile("pl", []Stage{
		Branch(
			When("lang == 'en'", Pipe("en")),
			Otherwise(Pipe("generic")),
		),
	})
	table, err := dag.BranchTable(0)
	if err != nil {
		t.Fatalf("BranchTable: %v", err)
	}
	if len(table) != 2 {
		t.Fatalf("len(table) = %d, want 2", len(table))
	}
	if table["lang == 'en'"] != "pepper.pipe.pl.branch.0.lang_eqeq_en" {
		t.Errorf("english topic = %q", table["lang == 'en'"])
	}
	if table["_otherwise"] != "pepper.pipe.pl.branch.0._otherwise" {
		t.Errorf("otherwise topic = %q", table["_otherwise"])
	}
}

func TestTransformFn(t *testing.T) {
	fn := func(e envelope.Envelope, m map[string]any) (map[string]any, error) { return m, nil }
	dag, _ := Compile("pl", []Stage{Transform(fn)})
	got, err := dag.TransformFn(0)
	if err != nil {
		t.Fatalf("TransformFn: %v", err)
	}
	if got == nil {
		t.Error("got nil function")
	}
}

func TestReturnValue(t *testing.T) {
	dag, _ := Compile("pl", []Stage{Return(map[string]any{"x": 42})})
	val, err := dag.ReturnValue(0)
	if err != nil {
		t.Fatalf("ReturnValue: %v", err)
	}
	if val["x"] != 42 {
		t.Errorf("val[x] = %v", val["x"])
	}
}

func TestParallelStages(t *testing.T) {
	dag, _ := Compile("pl", []Stage{Parallel(Pipe("a"), Pipe("b"))})
	stages, err := dag.ParallelStages(0)
	if err != nil {
		t.Fatalf("ParallelStages: %v", err)
	}
	if len(stages) != 2 {
		t.Errorf("len = %d, want 2", len(stages))
	}
}

func TestAccessorsOutOfRange(t *testing.T) {
	dag, _ := Compile("pl", []Stage{Pipe("a")})
	if _, _, _, _, err := dag.PipeConfig(-1); err == nil {
		t.Error("expected error")
	}
	if _, _, _, _, err := dag.ScatterGather(0); err == nil {
		t.Error("expected error for pipe stage")
	}
	if _, err := dag.BranchTable(0); err == nil {
		t.Error("expected error for pipe stage")
	}
	if _, err := dag.TransformFn(0); err == nil {
		t.Error("expected error for pipe stage")
	}
	if _, err := dag.ReturnValue(0); err == nil {
		t.Error("expected error for pipe stage")
	}
	if _, err := dag.ParallelStages(0); err == nil {
		t.Error("expected error for pipe stage")
	}
}

// Spec-compliant complex pipeline

func TestSpecAudioFullPipeline(t *testing.T) {
	// From spec §7.2:
	//   audio.full = audio.denoise → transform → scatter+gather → text.fix → branch
	dag, err := Compile("audio.full", []Stage{
		Pipe("audio.denoise").WithGroup("cpu"),
		Transform(func(res envelope.Envelope, in map[string]any) (map[string]any, error) {
			return map[string]any{"audio": res.Payload, "language": "en"}, nil
		}),
		Scatter("speech.transcribe").
			WithGroup("asr").
			Gather("transcript.merge", GatherAll, WithGatherTimeout(10*time.Second)),
		Pipe("text.fix").WithGroup("cpu"),
		Branch(
			When("lang == 'en'", Pipe("en.diarize")),
			When("lang == 'fr'", Pipe("fr.diarize")),
			Otherwise(Pipe("generic.diarize")),
		),
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if dag.StageCount() != 5 {
		t.Fatalf("StageCount = %d, want 5", dag.StageCount())
	}

	// Stage 0: pipe
	kind, _ := dag.StageKind(0)
	if kind != StageDispatch {
		t.Errorf("stage 0 kind = %q", kind)
	}
	env := envelope.DefaultEnvelope()
	if err := dag.DispatchEnvelope(&env, 0); err != nil {
		t.Fatalf("stage 0 dispatch: %v", err)
	}
	if env.ForwardTo != "pepper.pipe.audio_full.stage.1" {
		t.Errorf("stage 0 ForwardTo = %q", env.ForwardTo)
	}

	// Stage 1: transform — router-side
	kind, _ = dag.StageKind(1)
	if kind != StageTransform {
		t.Errorf("stage 1 kind = %q", kind)
	}
	if err := dag.DispatchEnvelope(&env, 1); !errors.Is(err, ErrRouterSideStage) {
		t.Errorf("stage 1 dispatch = %v, want ErrRouterSideStage", err)
	}
	fn, err := dag.TransformFn(1)
	if err != nil || fn == nil {
		t.Errorf("TransformFn: %v", err)
	}

	// Stage 2: scatter
	kind, _ = dag.StageKind(2)
	if kind != StageScatter {
		t.Errorf("stage 2 kind = %q", kind)
	}
	env = envelope.DefaultEnvelope()
	if err := dag.DispatchEnvelope(&env, 2); err != nil {
		t.Fatalf("stage 2 dispatch: %v", err)
	}
	if env.Dispatch != envelope.DispatchAll {
		t.Errorf("stage 2 Dispatch = %q", env.Dispatch)
	}
	if env.ForwardTo != "pepper.pipe.audio_full.stage.3" {
		t.Errorf("stage 2 ForwardTo = %q", env.ForwardTo)
	}

	// Stage 4: branch — router-side
	kind, _ = dag.StageKind(4)
	if kind != StageBranch {
		t.Errorf("stage 4 kind = %q", kind)
	}
	table, err := dag.BranchTable(4)
	if err != nil {
		t.Fatalf("BranchTable: %v", err)
	}
	if len(table) != 3 {
		t.Errorf("branch table len = %d, want 3", len(table))
	}
}

func TestSpecMeetingProcessPipeline(t *testing.T) {
	// From spec §7.5: pipelines referencing other pipelines
	dag, err := Compile("meeting.process", []Stage{
		Pipe("audio.full"),
		Pipe("summary.generate"),
		Pipe("action.extract"),
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if dag.StageCount() != 3 {
		t.Fatalf("StageCount = %d, want 3", dag.StageCount())
	}
	env := envelope.DefaultEnvelope()
	if err := dag.DispatchEnvelope(&env, 0); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if env.Cap != "audio.full" {
		t.Errorf("Cap = %q", env.Cap)
	}
}

func TestSpecFaceIdentifyPipeline(t *testing.T) {
	// From spec §16.2:
	//   face.identify = detect → branch(return) → scatter+gather → search
	dag, err := Compile("face.identify", []Stage{
		Pipe("face.detect.fast").WithGroup("fast"),
		Branch(
			When("faces_found == 0", Return(map[string]any{"matches": nil})),
		),
		Scatter("face.embed").WithGroup("gpu").Gather("face.merge", GatherAll),
		Pipe("face.search").WithGroup("cpu"),
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Stage 1 branch contains a Return arm.
	arm, ok := dag.Stages[1].(*BranchStage).Arms[0].Stage.(*ReturnStage)
	if !ok {
		t.Fatal("expected ReturnStage in branch arm")
	}
	if arm.Value["matches"] != nil {
		t.Error("Return value mismatch")
	}

	// Dispatch envelope for scatter stage (index 2)
	env := envelope.DefaultEnvelope()
	if err := dag.DispatchEnvelope(&env, 2); err != nil {
		t.Fatalf("scatter dispatch: %v", err)
	}
	if env.GatherAt != "pepper.pipe.face_identify.stage.2.gather" {
		t.Errorf("GatherAt = %q", env.GatherAt)
	}
}
