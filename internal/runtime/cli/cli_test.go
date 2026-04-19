package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CMD builder

func TestCMDBuilderBasic(t *testing.T) {
	b := CMD("ffmpeg", "-i", "input.wav")
	spec := b.Build()
	if spec.Command != "ffmpeg" {
		t.Errorf("Command = %q, want ffmpeg", spec.Command)
	}
	if len(spec.Args) != 2 {
		t.Errorf("Args = %v", spec.Args)
	}
}

func TestCMDBuilderChaining(t *testing.T) {
	spec := CMD("ffmpeg").
		Args("-i", "{input}", "-ar", "16000", "{output}").
		Input("audio-*.wav").
		Output("clean-*.wav").
		WorkDir("/tmp").
		Groups("cpu").
		Pool(4).
		Env("CUDA_VISIBLE_DEVICES=0").
		Build()

	if spec.InputMode != InputTempFile {
		t.Errorf("InputMode = %v, want InputTempFile", spec.InputMode)
	}
	if spec.InputGlob != "audio-*.wav" {
		t.Errorf("InputGlob = %q", spec.InputGlob)
	}
	if spec.OutputMode != OutputTempFile {
		t.Errorf("OutputMode = %v, want OutputTempFile", spec.OutputMode)
	}
	if spec.OutputGlob != "clean-*.wav" {
		t.Errorf("OutputGlob = %q", spec.OutputGlob)
	}
	if spec.WorkDir != "/tmp" {
		t.Errorf("WorkDir = %q", spec.WorkDir)
	}
	if len(spec.Groups) != 1 || spec.Groups[0] != "cpu" {
		t.Errorf("Groups = %v", spec.Groups)
	}
	if spec.Pool != 4 {
		t.Errorf("Pool = %d, want 4", spec.Pool)
	}
	if len(spec.Env) != 1 {
		t.Errorf("Env = %v", spec.Env)
	}
}

func TestCMDBuilderStdinStdout(t *testing.T) {
	spec := CMD("node", "transform.js").
		Stdin(JSONStdin).
		Stdout(JSONStdout).
		Build()

	if spec.InputMode != InputJSONStdin {
		t.Errorf("InputMode = %v, want JSONStdin", spec.InputMode)
	}
	if spec.OutputMode != OutputJSONStdout {
		t.Errorf("OutputMode = %v, want JSONStdout", spec.OutputMode)
	}
}

// BuildSpec

func TestBuilderBuildSpec(t *testing.T) {
	spec := CMD("ffmpeg").Args("-i", "{input}", "{output}").
		Input("*.wav").Output("*.wav").Groups("cpu").
		BuildSpec("audio.convert")

	if spec.Name != "audio.convert" {
		t.Errorf("Name = %q", spec.Name)
	}
	if spec.Source != "ffmpeg" {
		t.Errorf("Source = %q, want ffmpeg", spec.Source)
	}
	if spec.CLISpec == nil {
		t.Error("CLISpec should be set")
	}
}

func TestBuilderBuildSpecDefaultGroups(t *testing.T) {
	spec := CMD("echo").BuildSpec("my.tool")
	if len(spec.Groups) != 1 || spec.Groups[0] != "default" {
		t.Errorf("Groups = %v, want [default]", spec.Groups)
	}
}

// Runner — JSONStdin/JSONStdout

func TestRunnerJSONStdin(t *testing.T) {
	if _, err := os.Stat("/usr/bin/python3"); os.IsNotExist(err) {
		t.Skip("python3 not available")
	}

	spec := CMD("python3", "-c",
		`import sys,json; d=json.load(sys.stdin); print(json.dumps({"echo":d["msg"]}))`).
		Stdin(JSONStdin).
		Stdout(JSONStdout).
		Build()

	r := NewRunner(spec, t.TempDir(), nil, 0)
	out, err := r.Run(context.Background(), map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out["echo"] != "hello" {
		t.Errorf("echo = %v, want hello", out["echo"])
	}
}

func TestRunnerEchoStdout(t *testing.T) {
	spec := CMDSpec{
		Command:    "sh",
		Args:       []string{"-c", "printf 'hello-pepper'"},
		OutputMode: OutputMode(3),
	}
	r := NewRunner(spec, t.TempDir(), nil, 0)
	out, err := r.Run(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	stdout, _ := out["stdout"].([]byte)
	if !strings.Contains(string(stdout), "hello-pepper") {
		t.Errorf("stdout = %q, want hello-pepper", stdout)
	}
}

func TestRunnerCommandNotFound(t *testing.T) {
	spec := CMDSpec{Command: "this-command-does-not-exist-pepper", OutputMode: OutputMode(3)}
	r := NewRunner(spec, t.TempDir(), nil, 0)
	_, err := r.Run(context.Background(), map[string]any{})
	if err == nil {
		t.Error("expected error for missing command")
	}
}

func TestRunnerContextCancellation(t *testing.T) {
	spec := CMDSpec{Command: "sleep", Args: []string{"10"}, OutputMode: OutputMode(3)}
	r := NewRunner(spec, t.TempDir(), nil, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.Run(ctx, map[string]any{})
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestRunnerTempFileInput(t *testing.T) {
	// wc -c {input} reads from temp file, outputs plain text → raw stdout mode
	spec := CMDSpec{
		Command:    "wc",
		Args:       []string{"-c", "{input}"},
		InputMode:  InputTempFile,
		InputGlob:  "input-*.bin",
		OutputMode: OutputMode(3),
	}
	r := NewRunner(spec, t.TempDir(), nil, 0)
	out, err := r.Run(context.Background(), map[string]any{
		"data": []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	stdout, _ := out["stdout"].([]byte)
	if !strings.Contains(string(stdout), "5") {
		t.Errorf("wc output = %q, expected to contain 5", stdout)
	}
}

func TestRunnerTempFileOutput(t *testing.T) {
	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.txt")
	os.WriteFile(outFile, []byte("testdata\n"), 0600)

	// cat reads from a fixed path, outputs to stdout
	spec := CMDSpec{
		Command:    "cat",
		Args:       []string{outFile},
		OutputMode: OutputMode(3),
	}
	r := NewRunner(spec, dir, nil, 0)
	out, err := r.Run(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	stdout, _ := out["stdout"].([]byte)
	if !strings.Contains(string(stdout), "testdata") {
		t.Errorf("stdout = %q", stdout)
	}
}

// substituteArgs

func TestSubstituteArgs(t *testing.T) {
	r := &Runner{spec: CMDSpec{
		Args: []string{"-i", "{input}", "-o", "{output}", "--width", "{width}"},
	}}
	got := r.substituteArgs(map[string]any{"width": 1280}, "/tmp/in.wav", "/tmp/out.wav")
	want := []string{"-i", "/tmp/in.wav", "-o", "/tmp/out.wav", "--width", "1280"}
	if len(got) != len(want) {
		t.Fatalf("args len = %d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("arg[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSubstituteArgsNoPlaceholders(t *testing.T) {
	r := &Runner{spec: CMDSpec{Args: []string{"-f", "wav"}}}
	got := r.substituteArgs(map[string]any{}, "", "")
	if len(got) != 2 || got[0] != "-f" || got[1] != "wav" {
		t.Errorf("args = %v", got)
	}
}

// parseOutput

func TestParseOutputJSONStdout(t *testing.T) {
	r := &Runner{spec: CMDSpec{OutputMode: OutputJSONStdout}}
	data, _ := json.Marshal(map[string]any{"result": "ok"})
	out, err := r.parseOutput(data, "", nil)
	if err != nil {
		t.Fatalf("parseOutput: %v", err)
	}
	if out["result"] != "ok" {
		t.Errorf("result = %v", out["result"])
	}
}

func TestParseOutputJSONStdoutInvalid(t *testing.T) {
	r := &Runner{spec: CMDSpec{OutputMode: OutputJSONStdout}}
	_, err := r.parseOutput([]byte("not json"), "", nil)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseOutputDefault(t *testing.T) {
	r := &Runner{spec: CMDSpec{OutputMode: OutputMode(99)}}
	out, err := r.parseOutput([]byte("raw output"), "", nil)
	if err != nil {
		t.Fatalf("parseOutput: %v", err)
	}
	if _, ok := out["stdout"]; !ok {
		t.Error("stdout key missing from default output")
	}
}

// tmpFilePath

func TestTmpFilePath(t *testing.T) {
	dir := t.TempDir()
	r := &Runner{spec: CMDSpec{}, tmpDir: dir}
	p := r.tmpFilePath("audio-*.wav")
	if !strings.HasPrefix(p, dir) {
		t.Errorf("path %q not under tmpDir %q", p, dir)
	}
	if strings.Contains(p, "*") {
		t.Error("tmpFilePath should not contain '*'")
	}
	if !strings.HasPrefix(filepath.Base(p), "audio-") {
		t.Errorf("filename %q should start with audio-", filepath.Base(p))
	}
	if !strings.HasSuffix(p, ".wav") {
		t.Errorf("path %q should end with .wav", p)
	}
}

// Constants

func TestConstantAliases(t *testing.T) {
	if JSONStdin != InputJSONStdin {
		t.Error("JSONStdin != InputJSONStdin")
	}
	if JSONStdout != OutputJSONStdout {
		t.Error("JSONStdout != OutputJSONStdout")
	}
	if MsgPackStdin != InputMsgPackStdin {
		t.Error("MsgPackStdin != InputMsgPackStdin")
	}
}
