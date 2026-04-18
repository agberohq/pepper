// Package cli implements the Pepper CLI tool runtime.
//
// Any executable — ffmpeg, ImageMagick, pandoc, node, php, terraform —
// becomes a Pepper capability. The CLI runtime spawns the process,
// pipes the payload via stdin or temp file, and reads the result from stdout.
//
// Usage:
//
//	pp.Prepare("audio.convert",
//	    pepper.CMD("ffmpeg").
//	        Args("-i", "{input}", "-ar", "16000", "{output}").
//	        Input(pepper.TempFile("audio-*.wav")).
//	        Output(pepper.TempFile("clean-*.wav")).
//	        Groups("cpu"),
//	)
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/oklog/ulid/v2"
)

// InputMode controls how the payload is passed to the process.
type InputMode int

const (
	InputTempFile     InputMode = iota // write to temp file, pass path as {input}
	InputJSONStdin                     // JSON-encode payload to stdin
	InputMsgPackStdin                  // msgpack-encode payload to stdin
)

// OutputMode controls how the result is read from the process.
type OutputMode int

const (
	OutputTempFile   OutputMode = iota // read from {output} temp file
	OutputJSONStdout                   // JSON-decode stdout
	OutputMsgPackOut                   // msgpack-decode stdout
)

// CMDSpec fully describes a CLI capability.
type CMDSpec struct {
	Command     string
	Args        []string // may contain {input}, {output}, and {field_name} placeholders
	InputMode   InputMode
	OutputMode  OutputMode
	InputGlob   string // temp file pattern e.g. "audio-*.wav"
	OutputGlob  string // temp file pattern e.g. "clean-*.wav"
	WorkDir     string
	Env         []string // additional environment variables
	Groups      []string
	Pool        int // if >0, keep N processes warm with line-framed protocol
	MaxRequests int // recycle pool worker after N requests
}

// Builder builds a CMDSpec with method chaining.
type Builder struct {
	spec CMDSpec
}

// CMD starts building a CLI capability spec.
func CMD(command string, args ...string) *Builder {
	return &Builder{spec: CMDSpec{Command: command, Args: args}}
}

func (b *Builder) Args(args ...string) *Builder {
	b.spec.Args = args
	return b
}

// Input sets the input mode to TempFile with the given glob pattern.
func (b *Builder) Input(glob string) *Builder {
	b.spec.InputMode = InputTempFile
	b.spec.InputGlob = glob
	return b
}

// Output sets the output mode to TempFile with the given glob pattern.
func (b *Builder) Output(glob string) *Builder {
	b.spec.OutputMode = OutputTempFile
	b.spec.OutputGlob = glob
	return b
}

// Stdin sets JSON stdin input mode.
func (b *Builder) Stdin(mode InputMode) *Builder {
	b.spec.InputMode = mode
	return b
}

// Stdout sets JSON stdout output mode.
func (b *Builder) Stdout(mode OutputMode) *Builder {
	b.spec.OutputMode = mode
	return b
}

func (b *Builder) WorkDir(dir string) *Builder {
	b.spec.WorkDir = dir
	return b
}

func (b *Builder) Groups(groups ...string) *Builder {
	b.spec.Groups = groups
	return b
}

func (b *Builder) Pool(n int) *Builder {
	b.spec.Pool = n
	return b
}

func (b *Builder) Env(kv ...string) *Builder {
	b.spec.Env = append(b.spec.Env, kv...)
	return b
}

// Build returns the final CMDSpec.
func (b *Builder) Build() CMDSpec { return b.spec }

// Runner

// Runner executes CLI capabilities.
type Runner struct {
	spec   CMDSpec
	tmpDir string
	mu     sync.Mutex
	pool   []*poolWorker // non-nil when Pool > 0
}

type poolWorker struct {
	cmd    *exec.Cmd
	stdin  *bytes.Buffer
	stdout *bytes.Buffer
	mu     sync.Mutex
	served int
}

// NewRunner creates a Runner for the given spec.
func NewRunner(spec CMDSpec, tmpDir string) *Runner {
	return &Runner{spec: spec, tmpDir: tmpDir}
}

// Run executes the CLI tool with the given inputs and returns the result.
func (r *Runner) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	var inputPath, outputPath string
	var cleanup []func()

	// Prepare input
	switch r.spec.InputMode {
	case InputTempFile:
		path, err := r.writeTempFile(in)
		if err != nil {
			return nil, fmt.Errorf("cli.Run: write input: %w", err)
		}
		inputPath = path
		cleanup = append(cleanup, func() { os.Remove(path) })
	}

	// Prepare output path
	if r.spec.OutputMode == OutputTempFile {
		glob := r.spec.OutputGlob
		if glob == "" {
			glob = "output-*.bin"
		}
		outputPath = r.tmpFilePath(glob)
		cleanup = append(cleanup, func() { os.Remove(outputPath) })
	}

	defer func() {
		for _, fn := range cleanup {
			fn()
		}
	}()

	// Build command with substituted args
	args := r.substituteArgs(in, inputPath, outputPath)
	cmd := exec.CommandContext(ctx, r.spec.Command, args...) //nolint:gosec
	cmd.Dir = r.spec.WorkDir
	cmd.Env = append(os.Environ(), r.spec.Env...)

	// Set up stdin for stdin-mode inputs
	if r.spec.InputMode == InputJSONStdin {
		data, err := json.Marshal(in)
		if err != nil {
			return nil, fmt.Errorf("cli.Run: marshal stdin: %w", err)
		}
		cmd.Stdin = bytes.NewReader(data)
	}

	// Execute
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("cli.Run: %s: %w (stderr: %s)",
			r.spec.Command, err, truncate(stderr.String(), 256))
	}

	// Parse output
	return r.parseOutput(stdout.Bytes(), outputPath, in)
}

// Helpers

func (r *Runner) writeTempFile(in map[string]any) (string, error) {
	glob := r.spec.InputGlob
	if glob == "" {
		glob = "input-*.bin"
	}
	path := r.tmpFilePath(glob)

	// Look for a blob reference or raw bytes in the input
	var data []byte
	for _, v := range in {
		switch val := v.(type) {
		case []byte:
			data = val
		case map[string]any:
			// blob reference — use the path directly
			if blobPath, ok := val["path"].(string); ok {
				return blobPath, nil
			}
		}
		if data != nil {
			break
		}
	}
	if data == nil {
		// Serialise entire input as JSON
		var err error
		data, err = json.Marshal(in)
		if err != nil {
			return "", err
		}
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return "", err
	}
	return path, nil
}

func (r *Runner) parseOutput(stdout []byte, outputPath string, in map[string]any) (map[string]any, error) {
	switch r.spec.OutputMode {
	case OutputTempFile:
		data, err := os.ReadFile(outputPath)
		if err != nil {
			return nil, fmt.Errorf("cli.Run: read output file: %w", err)
		}
		return map[string]any{"data": data, "path": outputPath}, nil

	case OutputJSONStdout:
		var result map[string]any
		if err := json.Unmarshal(stdout, &result); err != nil {
			return nil, fmt.Errorf("cli.Run: parse JSON stdout: %w (output: %s)",
				err, truncate(string(stdout), 256))
		}
		return result, nil

	default:
		return map[string]any{"stdout": stdout}, nil
	}
}

func (r *Runner) substituteArgs(in map[string]any, inputPath, outputPath string) []string {
	args := make([]string, len(r.spec.Args))
	for i, arg := range r.spec.Args {
		arg = strings.ReplaceAll(arg, "{input}", inputPath)
		arg = strings.ReplaceAll(arg, "{output}", outputPath)
		// Substitute any {field_name} with the corresponding input field
		for k, v := range in {
			placeholder := "{" + k + "}"
			if strings.Contains(arg, placeholder) {
				arg = strings.ReplaceAll(arg, placeholder, fmt.Sprintf("%v", v))
			}
		}
		args[i] = arg
	}
	return args
}

func (r *Runner) tmpFilePath(glob string) string {
	dir := r.tmpDir
	if dir == "" {
		dir = os.TempDir()
	}
	// Replace * with a ULID for uniqueness
	name := strings.ReplaceAll(glob, "*", ulid.Make().String())
	return filepath.Join(dir, name)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// TempFile is a convenience constant for Input/Output modes.
const (
	JSONStdin    = InputJSONStdin
	JSONStdout   = OutputJSONStdout
	MsgPackStdin = InputMsgPackStdin
)
