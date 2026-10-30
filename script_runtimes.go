// Julia, Node.js and Rust script runtimes.
//
// All three mirror spawnPythonWorker: locate the runtime, inject the PEPPER_*
// environment, launch the child process, and hand it to startScriptWorker for
// log piping and respawn monitoring. bootJuliaWorkers, bootNodeWorkers and
// bootRustWorkers are called from Start(); sendCapLoads (runtime.go) routes
// cap_load to whichever runtime the connecting worker announces.
//
// Rust differs in one respect: a .rs file cannot be interpreted, so the
// pepper-worker binary compiles it on first use (content-hash cached) and then
// exec()s the result — the PID the router supervises stays the same.

package pepper

import (
	"context"
	"fmt"
	"os"
	stdruntime "runtime"
	"strings"
	"time"

	blobpkg "github.com/agberohq/pepper/internal/blob"
	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/registry"
	"github.com/agberohq/pepper/internal/wire"
	"os/exec"
	"path/filepath"
)

// Julia

// bootJuliaWorkers starts a Julia subprocess for each WorkerConfig that has at
// least one matching Julia capability registered.
func (p *Pepper) bootJuliaWorkers(ctx context.Context, allSpecs []*registry.Spec) error {
	busAddr := p.rt.bus.Addr()
	for i, wc := range p.cfg.Workers {
		wID := wc.ID
		if wID == "" {
			wID = fmt.Sprintf("w-%d", i+1)
		}
		if err := p.spawnJuliaWorker(ctx, wc, wID, busAddr, allSpecs); err != nil {
			return err
		}
	}
	return nil
}

// spawnJuliaWorker launches `julia --startup-file=no --project=@. runtime.jl`
// as a child process. If no Julia capabilities match this worker's groups the
// function is a no-op.
func (p *Pepper) spawnJuliaWorker(ctx context.Context, wc WorkerConfig, workerID, busAddr string, allSpecs []*registry.Spec) error {
	matched := p.matchedSpecs(allSpecs, registry.RuntimeJulia, wc.Groups)
	if len(matched) == 0 {
		return nil
	}

	groups, blobDir, envVars := p.workerEnv(wc, workerID, busAddr)

	rtPath := defaultFinder.JuliaRuntime(matched)
	if rtPath == "" {
		return fmt.Errorf("runtime.jl not found — place it at runtime/julia/runtime.jl")
	}
	if abs, err := filepath.Abs(filepath.Dir(rtPath)); err == nil {
		envVars = append(envVars, wire.EnvJuliaRuntimeDir+"="+abs)
	}
	_ = blobDir // already in envVars via workerEnv

	cmd := exec.CommandContext(ctx,
		defaultFinder.Julia(),
		"--startup-file=no",
		"--project=@.",
		rtPath,
	)
	return p.startScriptWorker(ctx, cmd, envVars, workerID, groups, busAddr, wc, allSpecs, string(registry.RuntimeJulia))
}

// bootNodeWorkers starts a Node.js subprocess for each WorkerConfig that has
// at least one matching Node capability registered.
func (p *Pepper) bootNodeWorkers(ctx context.Context, allSpecs []*registry.Spec) error {
	busAddr := p.rt.bus.Addr()
	for i, wc := range p.cfg.Workers {
		wID := wc.ID
		if wID == "" {
			wID = fmt.Sprintf("w-%d", i+1)
		}
		if err := p.spawnNodeWorker(ctx, wc, wID, busAddr, allSpecs); err != nil {
			return err
		}
	}
	return nil
}

// spawnNodeWorker launches `node runtime.js` as a child process.
// If no Node capabilities match this worker's groups the function is a no-op.
func (p *Pepper) spawnNodeWorker(ctx context.Context, wc WorkerConfig, workerID, busAddr string, allSpecs []*registry.Spec) error {
	matched := p.matchedSpecs(allSpecs, registry.RuntimeNode, wc.Groups)
	if len(matched) == 0 {
		return nil
	}

	groups, blobDir, envVars := p.workerEnv(wc, workerID, busAddr)

	rtPath := defaultFinder.NodeRuntime(matched)
	if rtPath == "" {
		return fmt.Errorf("runtime.js not found — place it at runtime/node/runtime.js")
	}
	if abs, err := filepath.Abs(filepath.Dir(rtPath)); err == nil {
		envVars = append(envVars, wire.EnvNodeRuntimeDir+"="+abs)
	}
	_ = blobDir

	cmd := exec.CommandContext(ctx, defaultFinder.Node(), rtPath)
	return p.startScriptWorker(ctx, cmd, envVars, workerID, groups, busAddr, wc, allSpecs, string(registry.RuntimeNode))
}

// Shared helpers

// matchedSpecs returns specs of the given runtime whose groups overlap wc.Groups.
func (p *Pepper) matchedSpecs(allSpecs []*registry.Spec, rt registry.Runtime, wcGroups []string) []*registry.Spec {
	var out []*registry.Spec
	for _, spec := range allSpecs {
		if spec.Runtime == rt &&
			(len(wcGroups) == 0 || len(spec.Groups) == 0 || p.groupsOverlap(spec.Groups, wcGroups)) {
			out = append(out, spec)
		}
	}
	return out
}

// workerEnv builds the common PEPPER_* environment variables for any scripted worker.
func (p *Pepper) workerEnv(wc WorkerConfig, workerID, busAddr string) (groups []string, blobDir string, envVars []string) {
	groups = wc.Groups
	if len(groups) == 0 {
		groups = []string{wire.GroupDefault}
	}
	blobDir = p.cfg.BlobDir
	if blobDir == "" {
		blobDir = blobpkg.PlatformDefaultDir()
	}
	envVars = bus.WorkerEnvVars(
		workerID, busAddr, string(p.cfg.Codec),
		groups, int(p.cfg.HeartbeatInterval.Milliseconds()),
		p.cfg.MaxConcurrent, blobDir,
	)
	if len(p.cfg.Resources) > 0 {
		if enc, err := p.codec.Marshal(p.cfg.Resources); err == nil {
			envVars = append(envVars, bus.WorkerResourcesVar(enc))
		}
	}
	return
}

// startScriptWorker is the common subprocess launch path used by all scripted
// runtimes (Python, Julia, Node). It configures the process, opens the log
// pipe, starts the process, and sets up respawn monitoring.
func (p *Pepper) startScriptWorker(
	ctx context.Context,
	cmd *exec.Cmd,
	envVars []string,
	workerID string,
	groups []string,
	busAddr string,
	wc WorkerConfig,
	allSpecs []*registry.Spec,
	runtimeKind string,
) error {
	cmd.Env = append(os.Environ(), envVars...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := p.rt.subManager.ConfigureCommand(cmd); err != nil {
		return fmt.Errorf("sub.Configure: %w", err)
	}

	var logR, logW *os.File
	if stdruntime.GOOS != "windows" {
		var pipeErr error
		logR, logW, pipeErr = os.Pipe()
		if pipeErr != nil {
			logR, logW = nil, nil
		} else {
			cmd.ExtraFiles = []*os.File{logW}
		}
	}

	if err := cmd.Start(); err != nil {
		if logW != nil {
			_ = logR.Close()
			_ = logW.Close()
		}
		return err
	}
	if err := p.rt.subManager.AddChildProcess(cmd.Process); err != nil {
		p.logger.Fields("worker", workerID, "error", err).Warn("sub.Track failed")
	}
	if logW != nil {
		_ = logW.Close()
		go p.pipeWorkerLogs(logR, workerID, runtimeKind)
	}

	p.rt.workerStates.Store(workerID, &workerEntry{
		id: workerID, groups: groups, wc: wc, busAddr: busAddr,
	})
	p.startRespawnLooper(workerID, wc, allSpecs)
	go func() {
		if err := cmd.Wait(); err != nil && ctx.Err() == nil {
			p.logger.Fields("worker", workerID, "pid", cmd.Process.Pid, "error", err).
				Warn("worker process exited")
		}
	}()
	return nil
}

// Rust

// bootRustWorkers starts a Rust worker for each WorkerConfig that has at least
// one matching Rust capability.
func (p *Pepper) bootRustWorkers(ctx context.Context, allSpecs []*registry.Spec) error {
	busAddr := p.rt.bus.Addr()
	for i, wc := range p.cfg.Workers {
		wID := wc.ID
		if wID == "" {
			wID = fmt.Sprintf("w-%d", i+1)
		}
		if err := p.spawnRustWorker(ctx, wc, wID, busAddr, allSpecs); err != nil {
			return err
		}
	}
	return nil
}

// spawnRustWorker launches one Rust worker for all .rs capabilities that match
// the worker's groups — the same shape as a Python worker hosting several .py
// files, so N capabilities need one worker, not N.
func (p *Pepper) spawnRustWorker(ctx context.Context, wc WorkerConfig, workerID, busAddr string, allSpecs []*registry.Spec) error {
	matched := p.matchedSpecs(allSpecs, registry.RuntimeRust, wc.Groups)
	if len(matched) == 0 {
		return nil
	}

	runtimeCargo := defaultFinder.RustRuntime(matched)
	if runtimeCargo == "" {
		return fmt.Errorf("pepper-runtime crate not found — expected runtime/rust/Cargo.toml " +
			"(set PEEPER_HOME or PEPPER_RUST_RUNTIME_DIR)")
	}
	runtimeDir := filepath.Dir(runtimeCargo)

	workerBin, err := p.rustWorkerBinary(ctx, runtimeDir)
	if err != nil {
		return fmt.Errorf("rust worker %s: %w", workerID, err)
	}

	caps, err := rustCapabilityArgs(matched)
	if err != nil {
		return fmt.Errorf("rust worker %s: %w", workerID, err)
	}

	groups, blobDir, envVars := p.workerEnv(wc, workerID, busAddr)
	envVars = append(envVars, wire.EnvRustRuntimeDir+"="+runtimeDir)
	_ = blobDir // already in envVars via workerEnv

	cmd := exec.CommandContext(ctx, workerBin, caps...)
	return p.startScriptWorker(ctx, cmd, envVars, workerID, groups, busAddr, wc, allSpecs, string(registry.RuntimeRust))
}

// rustCapabilityArgs builds the repeated --cap arguments for a worker.
func rustCapabilityArgs(specs []*registry.Spec) ([]string, error) {
	args := make([]string, 0, len(specs)*2)
	for _, spec := range specs {
		source, err := filepath.Abs(spec.Source)
		if err != nil {
			return nil, fmt.Errorf("resolve %q: %w", spec.Source, err)
		}
		if _, err := os.Stat(source); err != nil {
			return nil, fmt.Errorf("capability %q source: %w", spec.Name, err)
		}
		args = append(args, "--cap", source)
	}
	return args, nil
}

// rustBuildTimeout bounds how long a capability set may take to compile. It is
// deliberately separate from WithBootTimeout: a build is not a slow start.
const rustBuildTimeout = 15 * time.Minute

// prepareRustCapabilities compiles every Rust capability set ahead of the boot
// window, so Start() never reports "worker never became ready" because cargo was
// still working. Compiler output is surfaced through the returned error, which is
// what a caller needs to fix a broken capability.
func (p *Pepper) prepareRustCapabilities(ctx context.Context, allSpecs []*registry.Spec) error {
	if len(p.matchedSpecs(allSpecs, registry.RuntimeRust, nil)) == 0 {
		return nil
	}
	runtimeCargo := defaultFinder.RustRuntime(allSpecs)
	if runtimeCargo == "" {
		return fmt.Errorf("pepper: rust capabilities registered but the pepper-runtime crate was not found " +
			"— expected runtime/rust/Cargo.toml (set PEEPER_HOME or PEPPER_RUST_RUNTIME_DIR)")
	}
	runtimeDir := filepath.Dir(runtimeCargo)

	for i, wc := range p.cfg.Workers {
		matched := p.matchedSpecs(allSpecs, registry.RuntimeRust, wc.Groups)
		if len(matched) == 0 {
			continue
		}
		wID := wc.ID
		if wID == "" {
			wID = fmt.Sprintf("w-%d", i+1)
		}

		bin, err := p.rustWorkerBinary(ctx, runtimeDir)
		if err != nil {
			return fmt.Errorf("pepper: rust worker %s: %w", wID, err)
		}
		caps, err := rustCapabilityArgs(matched)
		if err != nil {
			return fmt.Errorf("pepper: rust worker %s: %w", wID, err)
		}

		buildCtx, cancel := context.WithTimeout(ctx, rustBuildTimeout)
		args := append([]string{"--prepare"}, caps...)
		cmd := exec.CommandContext(buildCtx, bin, args...)
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			return fmt.Errorf("pepper: rust worker %s: %w\n%s", wID, err, tailLines(string(out), 25))
		}
		p.logger.Fields("worker", wID, "caps", len(matched)).Info("rust capabilities compiled")
	}
	return nil
}

// tailLines returns the last n lines of s, for error reporting.
func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// rustWorkerBinary returns the pepper-worker executable, building the runtime
// crate once if neither an override nor a prebuilt binary is available.
func (p *Pepper) rustWorkerBinary(ctx context.Context, runtimeDir string) (string, error) {
	if bin := os.Getenv(wire.EnvRustWorkerBin); bin != "" {
		return bin, nil
	}
	bin := filepath.Join(runtimeDir, "target", "release", "pepper-worker")
	if _, err := os.Stat(bin); err == nil {
		return bin, nil
	}
	if !defaultFinder.HasCargo() {
		return "", fmt.Errorf("cargo not found in PATH — install Rust (https://rustup.rs) " +
			"or set PEPPER_RUST_WORKER_BIN to a prebuilt pepper-worker")
	}
	p.logger.Fields("dir", runtimeDir).Info("building pepper-runtime (first Rust capability)")
	build := exec.CommandContext(ctx, defaultFinder.Cargo(), "build", "--release", "--manifest-path",
		filepath.Join(runtimeDir, "Cargo.toml"))
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	build.Env = append(os.Environ(), "CARGO_TERM_COLOR=never")
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("cargo build pepper-runtime: %w", err)
	}
	if _, err := os.Stat(bin); err != nil {
		return "", fmt.Errorf("cargo build succeeded but %s is missing", bin)
	}
	return bin, nil
}
