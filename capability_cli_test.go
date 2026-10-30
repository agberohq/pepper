package pepper

import (
	"reflect"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/registry"
	"github.com/agberohq/pepper/runtime/cli"
)

// TestCoordConstructorsCompile pins the public shape of the cluster coord
// constructors. They are thin wrappers over internal/coord, which external users
// cannot import directly.
//
// The pinning happens through parameter types: passing the function to a helper
// only compiles if the signature still matches.
func pinCoordCtor(f func(string) (Coord, error)) { _ = f }
func pinMulaClusterCtor(f func(string, int, int, []string) (Coord, error)) {
	_ = f
}

func TestCoordConstructorsCompile(t *testing.T) {
	pinCoordCtor(Redis)
	pinCoordCtor(NATS)
	pinCoordCtor(Mula)
	pinMulaClusterCtor(MulaCluster)
}

// so the stdin/stdout contract documented on pepper.CLI actually takes effect.
func TestCLICapStdinStdout(t *testing.T) {
	specs, err := CLI("video.probe", "ffprobe", "-v", "quiet").
		Args("-print_format", "json", "-").
		Stdin(JSONStdin).
		Stdout(JSONStdout).
		Groups("tools").
		Timeout(5 * time.Second).
		buildSpecs()
	if err != nil {
		t.Fatalf("buildSpecs: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("buildSpecs returned %d specs, want 1", len(specs))
	}
	spec := specs[0]
	if spec.Runtime != registry.RuntimeCLI {
		t.Errorf("Runtime = %q, want %q", spec.Runtime, registry.RuntimeCLI)
	}
	if spec.Name != "video.probe" {
		t.Errorf("Name = %q, want video.probe", spec.Name)
	}
	if spec.Source != "ffprobe" {
		t.Errorf("Source = %q, want ffprobe", spec.Source)
	}
	if spec.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", spec.Timeout)
	}
	if !reflect.DeepEqual(spec.Groups, []string{"tools"}) {
		t.Errorf("Groups = %v, want [tools]", spec.Groups)
	}

	cmd, ok := spec.CLISpec.(cli.CMDSpec)
	if !ok {
		t.Fatalf("CLISpec = %T, want cli.CMDSpec", spec.CLISpec)
	}
	if cmd.InputMode != cli.InputJSONStdin {
		t.Errorf("InputMode = %v, want InputJSONStdin", cmd.InputMode)
	}
	if cmd.OutputMode != cli.OutputJSONStdout {
		t.Errorf("OutputMode = %v, want OutputJSONStdout", cmd.OutputMode)
	}
	if want := []string{"-print_format", "json", "-"}; !reflect.DeepEqual(cmd.Args, want) {
		t.Errorf("Args = %v, want %v", cmd.Args, want)
	}
}

// TestCLICapTempFileIO verifies the {input}/{output} temp-file contract used by
// tools such as ffmpeg, including WorkDir, Pool and Env pass-throughs.
func TestCLICapTempFileIO(t *testing.T) {
	specs, err := CLI("video.thumbs", "ffmpeg", "-y", "-i", "{input}", "{output}").
		Input("frame-*.png").
		Output("thumbs-*.png").
		WorkDir("/tmp").
		Pool(2).
		Env("FFREPORT=file=/tmp/ffmpeg.log").
		buildSpecs()
	if err != nil {
		t.Fatalf("buildSpecs: %v", err)
	}

	cmd, ok := specs[0].CLISpec.(cli.CMDSpec)
	if !ok {
		t.Fatalf("CLISpec = %T, want cli.CMDSpec", specs[0].CLISpec)
	}
	if cmd.InputMode != cli.InputTempFile {
		t.Errorf("InputMode = %v, want InputTempFile", cmd.InputMode)
	}
	if cmd.OutputMode != cli.OutputTempFile {
		t.Errorf("OutputMode = %v, want OutputTempFile", cmd.OutputMode)
	}
	if cmd.InputGlob != "frame-*.png" {
		t.Errorf("InputGlob = %q, want frame-*.png", cmd.InputGlob)
	}
	if cmd.OutputGlob != "thumbs-*.png" {
		t.Errorf("OutputGlob = %q, want thumbs-*.png", cmd.OutputGlob)
	}
	if cmd.WorkDir != "/tmp" {
		t.Errorf("WorkDir = %q, want /tmp", cmd.WorkDir)
	}
	if cmd.Pool != 2 {
		t.Errorf("Pool = %d, want 2", cmd.Pool)
	}
	if want := []string{"FFREPORT=file=/tmp/ffmpeg.log"}; !reflect.DeepEqual(cmd.Env, want) {
		t.Errorf("Env = %v, want %v", cmd.Env, want)
	}
	// Groups default when unset.
	if !reflect.DeepEqual(specs[0].Groups, []string{"default"}) {
		t.Errorf("Groups = %v, want [default]", specs[0].Groups)
	}
}
