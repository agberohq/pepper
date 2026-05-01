//
// Mirrors spawnPythonWorker for the two new scripted runtimes.
// The three integration points in runtime.go are documented inline.
//
// Integration checklist for runtime.go
//
// In Start(), after bootCLIWorkers, add:
//
//      if err := p.bootJuliaWorkers(bgCtx, allSpecs); err != nil {
//          bgCancel()
//          return fmt.Errorf("pepper: julia workers: %w", err)
//      }
//      if err := p.bootNodeWorkers(bgCtx, allSpecs); err != nil {
//          bgCancel()
//          return fmt.Errorf("pepper: node workers: %w", err)
//      }
//
// In the worker_hello controlLoop handler (the block that sends cap_load),
//    extend the runtime type dispatch from:
//
//      if runtimeType == "python" || runtimeType == "" {
//          // send cap_load for RuntimePython specs
//      }
//
//    to:
//
//      switch runtimeType {
//      case "python", "":
//          p.sendCapLoads(wid, env, registry.RuntimePython)
//      case "julia":
//          p.sendCapLoads(wid, env, registry.RuntimeJulia)
//      case "node":
//          p.sendCapLoads(wid, env, registry.RuntimeNode)
//      }
//
//    where sendCapLoads is the extracted helper below (add to runtime.go):
//
//      func (p *Pepper) sendCapLoads(wid string, env envelope.Envelope, rt registry.Runtime) {
//          for _, spec := range p.reg.All() {
//              if spec.Runtime == rt && (len(env.Groups()) == 0 || len(spec.Groups) == 0 || p.groupsOverlap(spec.Groups, env.Groups())) {
//                  payload, _ := p.codec.Marshal(p.buildCapLoad(spec))
//                  _ = p.rt.bus.Publish(bus.TopicControl(wid), payload)
//              }
//          }
//      }
//

package pepper

import (
	"context"
	"fmt"
	"os"
	stdruntime "runtime"

	blobpkg "github.com/agberohq/pepper/internal/blob"
	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/registry"
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
		envVars = append(envVars, "PEPPER_JULIA_RUNTIME_DIR="+abs)
	}
	_ = blobDir // already in envVars via workerEnv

	cmd := exec.CommandContext(ctx,
		defaultFinder.Julia(),
		"--startup-file=no",
		"--project=@.",
		rtPath,
	)
	return p.startScriptWorker(ctx, cmd, envVars, workerID, groups, busAddr, wc, allSpecs)
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
		envVars = append(envVars, "PEPPER_NODE_RUNTIME_DIR="+abs)
	}
	_ = blobDir

	cmd := exec.CommandContext(ctx, defaultFinder.Node(), rtPath)
	return p.startScriptWorker(ctx, cmd, envVars, workerID, groups, busAddr, wc, allSpecs)
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
		groups = []string{"default"}
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
			envVars = append(envVars, "PEPPER_RESOURCES="+string(enc))
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
			logR.Close()
			logW.Close()
		}
		return err
	}
	if err := p.rt.subManager.AddChildProcess(cmd.Process); err != nil {
		p.logger.Fields("worker", workerID, "error", err).Warn("sub.Track failed")
	}
	if logW != nil {
		logW.Close()
		go p.pipeWorkerLogs(logR, workerID)
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
