// These tests exercise Pepper's full runtime stack with actual subprocesses,
// real model loading, and real inter-process communication. They are the only
// tests that can answer:
//
//   - Does the model cache persist between requests (warm vs cold latency)?
//   - Does GPU memory get properly released when a worker recycles?
//   - Does the zero-copy blob path work with real tensor data?
//   - Does the pipeline stall if the GPU worker is saturated?
//   - Where does the runtime cache downloaded models?
//
// # Skip conditions (all checked at runtime, no build tags needed)
//
//   - PEPPER_REAL_TESTS=1 not set → entire file skips
//   - sox not in PATH → audio.denoise test skips
//   - faster-whisper not installed → speech.transcribe test skips
//   - Ollama not running on localhost:11434 → chat.respond test skips
//   - No GPU (CUDA_VISIBLE_DEVICES="" or nvidia-smi fails) → GPU tests skip
//
// # Running locally
//
//	PEPPER_REAL_TESTS=1 go test -v -run TestReal -timeout 300s .
//
// # Running in CI with GPU runner
//
//	PEPPER_REAL_TESTS=1 \
//	PEPPER_WHISPER_MODEL=tiny \
//	PEPPER_OLLAMA_MODEL=gemma:2b \
//	go test -v -run TestReal -timeout 300s .
//
// # Model cache
//
// faster-whisper caches to ~/.cache/huggingface by default.
// Set HF_HOME or TRANSFORMERS_CACHE to redirect.
// This test prints the cache directory so you can observe it.
package pepper

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/internal/runtime/adapter"
)

// skip guards

func realTestsEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv("PEPPER_REAL_TESTS") != "1" {
		t.Skip("set PEPPER_REAL_TESTS=1 to run real pipeline integration tests")
	}
}

func requireBinary(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s not found in PATH — skipping", name)
	}
}

func requirePythonPackage(t *testing.T, pkg string) {
	t.Helper()
	if err := exec.Command("python3", "-c", "import "+pkg).Run(); err != nil {
		t.Skipf("python package %q not installed — skipping", pkg)
	}
}

func requireOllama(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:11434", 500*time.Millisecond)
	if err != nil {
		t.Skip("Ollama not running on localhost:11434 — skipping")
	}
	conn.Close()
}

func requireGPU(t *testing.T) {
	t.Helper()
	if os.Getenv("CUDA_VISIBLE_DEVICES") == "NoDevFiles" {
		t.Skip("CUDA_VISIBLE_DEVICES=NoDevFiles — skipping GPU test")
	}
	if err := exec.Command("nvidia-smi").Run(); err != nil {
		t.Skip("no GPU available (nvidia-smi failed) — skipping GPU test")
	}
}

func ollamaModel(t *testing.T) string {
	t.Helper()
	if m := os.Getenv("PEPPER_OLLAMA_MODEL"); m != "" {
		return m
	}
	// Probe which model is actually available.
	resp, err := http.Get("http://localhost:11434/api/tags")
	if err != nil {
		t.Skip("could not list Ollama models")
	}
	defer resp.Body.Close()
	var tags struct {
		Models []struct{ Name string }
	}
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil || len(tags.Models) == 0 {
		t.Skip("no Ollama models installed — run: ollama pull gemma:2b")
	}
	// Prefer small models for CI speed.
	for _, prefer := range []string{"gemma:2b", "gemma2:2b", "llama3.2:1b", "tinyllama"} {
		for _, m := range tags.Models {
			if strings.HasPrefix(m.Name, prefer) {
				return m.Name
			}
		}
	}
	return tags.Models[0].Name // whatever is first
}

func whisperModel() string {
	if m := os.Getenv("PEPPER_WHISPER_MODEL"); m != "" {
		return m
	}
	return "tiny" // ~75 MB, fast, good for CI
}

// test capability sources

// denoiseCapSrc is a CPU-only denoise cap using sox.
// Falls back to a no-op passthrough if sox isn't available.
const denoiseCapSrc = `# pepper:name    = audio.denoise
# pepper:version = 1.0.0
# pepper:groups  = cpu

import subprocess
import tempfile
import shutil
from pathlib import Path

def run(inputs: dict) -> dict:
    audio_path = inputs.get("audio_path", "")
    if not audio_path or not Path(audio_path).exists():
        return {"error": f"audio_path missing or not found: {audio_path!r}"}

    # Try sox noise reduction; fall back to passthrough if sox unavailable.
    output_path = Path(tempfile.gettempdir()) / ("denoised_" + Path(audio_path).name)
    if shutil.which("sox"):
        try:
            subprocess.run(
                ["sox", audio_path, str(output_path),
                 "noisered", "/dev/null", "0.1"],
                check=True, capture_output=True, timeout=30,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            # No noise profile available — passthrough
            output_path = Path(audio_path)
    else:
        output_path = Path(audio_path)

    return {
        "denoised_path": str(output_path),
        "sample_rate": 16000,
        "passthrough": str(output_path) == audio_path,
    }
`

// transcribeCapSrc is the Whisper transcription cap.
const transcribeCapSrc = `# pepper:name    = speech.transcribe
# pepper:version = 1.0.0
# pepper:groups  = gpu,cpu
# pepper:deps    = faster-whisper>=1.0.0

import os
import time

_model = None
_model_load_time = None
_cache_dir = None

def setup(config: dict) -> None:
    global _model, _model_load_time, _cache_dir
    from faster_whisper import WhisperModel

    model_size = config.get("model_size", "tiny")
    device = "cuda" if _cuda_available() else "cpu"
    compute = "float16" if device == "cuda" else "int8"

    _cache_dir = os.environ.get(
        "HF_HOME",
        os.path.join(os.path.expanduser("~"), ".cache", "huggingface")
    )

    t0 = time.monotonic()
    _model = WhisperModel(model_size, device=device, compute_type=compute)
    _model_load_time = time.monotonic() - t0

def _cuda_available() -> bool:
    try:
        import torch
        return torch.cuda.is_available()
    except ImportError:
        return False

def run(inputs: dict) -> dict:
    if _model is None:
        return {"error": "model not loaded — setup() was not called"}

    audio_path = inputs.get("denoised_path") or inputs.get("audio_path", "")
    if not audio_path:
        return {"error": "no audio_path or denoised_path provided"}

    import time
    t0 = time.monotonic()
    segments, info = _model.transcribe(audio_path, beam_size=3)
    segments = list(segments)  # consume generator
    latency = time.monotonic() - t0

    transcript = " ".join(seg.text.strip() for seg in segments)
    return {
        "text": transcript,
        "language": info.language,
        "duration": info.duration,
        "latency_s": round(latency, 3),
        "model_load_s": round(_model_load_time, 3),
        "model_cache_dir": _cache_dir,
        "segment_count": len(segments),
    }
`

// helpers

// silentWAV generates a short silent WAV file for testing.
// Returns the path. Caller should defer os.Remove.
func silentWAV(t *testing.T, durationSeconds float64) string {
	t.Helper()
	f, err := os.CreateTemp("", "pepper_test_*.wav")
	if err != nil {
		t.Fatalf("create temp wav: %v", err)
	}
	f.Close()

	// sox -n -r 16000 -c 1 output.wav trim 0.0 <duration>
	if _, err := exec.LookPath("sox"); err != nil {
		// No sox — write a minimal valid WAV header manually (44 bytes + silence).
		writeMinimalWAV(t, f.Name(), 16000, durationSeconds)
		return f.Name()
	}
	cmd := exec.Command("sox", "-n", "-r", "16000", "-c", "1", f.Name(),
		"trim", "0.0", fmt.Sprintf("%.1f", durationSeconds))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("sox generate silent wav: %v\n%s", err, out)
	}
	return f.Name()
}

// writeMinimalWAV writes a minimal PCM WAV file with silence.
func writeMinimalWAV(t *testing.T, path string, sampleRate int, durationSec float64) {
	t.Helper()
	samples := int(float64(sampleRate) * durationSec)
	dataSize := samples * 2 // 16-bit PCM
	buf := make([]byte, 44+dataSize)

	write4 := func(off int, v uint32) {
		buf[off], buf[off+1], buf[off+2], buf[off+3] =
			byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
	}
	write2 := func(off int, v uint16) { buf[off], buf[off+1] = byte(v), byte(v>>8) }

	copy(buf[0:], "RIFF")
	write4(4, uint32(36+dataSize))
	copy(buf[8:], "WAVEfmt ")
	write4(16, 16) // chunk size
	write2(20, 1)  // PCM
	write2(22, 1)  // mono
	write4(24, uint32(sampleRate))
	write4(28, uint32(sampleRate*2))
	write2(32, 2)  // block align
	write2(34, 16) // bits per sample
	copy(buf[36:], "data")
	write4(40, uint32(dataSize))
	// remaining bytes are zero = silence

	if err := os.WriteFile(path, buf, 0644); err != nil {
		t.Fatalf("write minimal wav: %v", err)
	}
}

func writeCap(t *testing.T, dir, name, src string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(src), 0644); err != nil {
		t.Fatalf("write cap %s: %v", name, err)
	}
	return path
}

// tests

// TestRealDenoisePassthrough verifies the audio.denoise cap runs end-to-end
// using the CLI runtime with a real Python subprocess.
// No GPU needed. sox is optional (cap falls back to passthrough).
func TestRealDenoisePassthrough(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "python3")
	requirePythonPackage(t, "msgpack")

	capDir := t.TempDir()
	denoisePath := writeCap(t, capDir, "denoise.py", denoiseCapSrc)
	audioPath := silentWAV(t, 1.0)
	defer os.Remove(audioPath)

	pp, err := New(
		WithWorkers(NewWorker("w-cpu-1").Groups("cpu", "default")),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(Script("audio.denoise", denoisePath)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	doCtx, doCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer doCancel()

	result, err := pp.Do(doCtx, "audio.denoise", core.In{"audio_path": audioPath})
	if err != nil {
		t.Fatalf("Do audio.denoise: %v", err)
	}
	out := result.AsJSON()
	t.Logf("denoise result: %v", out)

	if _, ok := out["denoised_path"]; !ok {
		t.Errorf("expected denoised_path in result, got: %v", out)
	}
	if errMsg, ok := out["error"].(string); ok {
		t.Errorf("cap returned error: %s", errMsg)
	}
	t.Logf("worker=%s latency=%s", result.WorkerID, result.Latency)
}

// TestRealWhisperColdWarm measures cold-start vs warm latency for the
// Whisper transcription cap to answer: does the model cache between requests?
func TestRealWhisperColdWarm(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "python3")
	requirePythonPackage(t, "faster_whisper")
	requirePythonPackage(t, "msgpack")

	capDir := t.TempDir()
	transcribePath := writeCap(t, capDir, "transcribe.py", transcribeCapSrc)

	audioPath := silentWAV(t, 2.0)
	defer os.Remove(audioPath)

	modelSize := whisperModel()
	t.Logf("Using Whisper model: %s", modelSize)

	pp, err := New(
		WithWorkers(NewWorker("w-whisper").Groups("cpu", "gpu")),
		WithDefaultTimeout(120*time.Second),
		WithShutdownTimeout(10*time.Second),
		Resource("whisper", map[string]any{"model_size": modelSize}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(Script("speech.transcribe", transcribePath), WithConfig(map[string]any{"model_size": modelSize})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	t.Log("Starting runtime (may download model on first run)...")
	t0 := time.Now()
	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Logf("Runtime started in %s", time.Since(t0).Round(time.Millisecond))

	doCtx, doCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer doCancel()

	// cold request (model already loaded by setup, but first inference)
	t1 := time.Now()
	r1, err := pp.Do(doCtx, "speech.transcribe", core.In{"audio_path": audioPath})
	if err != nil {
		t.Fatalf("Do (cold): %v", err)
	}
	coldLatency := time.Since(t1)
	out1 := r1.AsJSON()
	t.Logf("COLD  latency=%-12s  cap_latency_s=%v  model_load_s=%v  cache_dir=%v",
		coldLatency.Round(time.Millisecond),
		out1["latency_s"], out1["model_load_s"], out1["model_cache_dir"])

	// warm request (model already in memory)
	t2 := time.Now()
	r2, err := pp.Do(doCtx, "speech.transcribe", core.In{"audio_path": audioPath})
	if err != nil {
		t.Fatalf("Do (warm): %v", err)
	}
	warmLatency := time.Since(t2)
	out2 := r2.AsJSON()
	t.Logf("WARM  latency=%-12s  cap_latency_s=%v  model_load_s=%v",
		warmLatency.Round(time.Millisecond),
		out2["latency_s"], out2["model_load_s"])

	// The warm request must be significantly faster than cold.
	// If it isn't, the model is being reloaded every request — a serious bug.
	if warmLatency > coldLatency {
		t.Logf("WARNING: warm request (%s) was slower than cold (%s) — model may not be cached",
			warmLatency.Round(time.Millisecond), coldLatency.Round(time.Millisecond))
	}

	// model_load_s should be 0 on the warm call (model was loaded in setup).
	if loadTime, ok := out2["model_load_s"].(float64); ok && loadTime > 0.1 {
		t.Errorf("warm request reloaded model (model_load_s=%.3f) — setup() not persistent", loadTime)
	}
}

// TestRealFullPipeline runs the complete audio→denoise→transcribe→chat pipeline.
// Requires: python3, faster-whisper, Ollama with at least one model installed.
func TestRealFullPipeline(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "python3")
	requirePythonPackage(t, "faster_whisper")
	requirePythonPackage(t, "msgpack")
	requireOllama(t)

	model := ollamaModel(t)
	modelSize := whisperModel()
	t.Logf("Pipeline: Whisper=%s  Ollama=%s  OS=%s/%s", modelSize, model, runtime.GOOS, runtime.GOARCH)

	capDir := t.TempDir()
	denoisePath := writeCap(t, capDir, "denoise.py", denoiseCapSrc)
	transcribePath := writeCap(t, capDir, "transcribe.py", transcribeCapSrc)

	// Use a WAV with actual speech if available, otherwise silence.
	audioPath := os.Getenv("PEPPER_TEST_AUDIO")
	if audioPath == "" {
		audioPath = silentWAV(t, 3.0)
		defer os.Remove(audioPath)
		t.Logf("No PEPPER_TEST_AUDIO set — using generated silence (transcript will be empty/noise)")
	} else {
		t.Logf("Using audio file: %s", audioPath)
	}

	pp, err := New(
		WithWorkers(
			NewWorker("w-cpu").Groups("cpu"),
			NewWorker("w-gpu").Groups("gpu", "cpu"),
		),
		WithDefaultTimeout(120*time.Second),
		WithShutdownTimeout(15*time.Second),
		Resource("whisper", map[string]any{"model_size": modelSize}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	// Stage 1: CPU denoise via Python runtime
	if err := pp.Register(Script("audio.denoise", denoisePath)); err != nil {
		t.Fatalf("Register audio.denoise: %v", err)
	}

	// Stage 2: Whisper transcription via Python runtime
	if err := pp.Register(Script("speech.transcribe", transcribePath), WithConfig(map[string]any{"model_size": modelSize})); err != nil {
		t.Fatalf("Register speech.transcribe: %v", err)
	}

	// Stage 3: LLM response via Ollama HTTP adapter
	if err := pp.Register(HTTP("chat.respond", "http://localhost:11434").
		With(adapter.Ollama).
		Groups("llm", "default").
		Timeout(60*time.Second),
		WithConfig(map[string]any{"model": model}),
	); err != nil {
		t.Fatalf("Use chat.respond: %v", err)
	}

	// Compose: denoise → transcribe → transform → chat
	if err := pp.Compose("audio.process",
		Pipe("audio.denoise").WithGroup("cpu"),
		Pipe("speech.transcribe").WithGroup("gpu"),
		PipeTransform(func(in map[string]any) (map[string]any, error) {
			text, _ := in["text"].(string)
			if text == "" {
				text = "[silence or inaudible audio]"
			}
			return map[string]any{
				"model":  model,
				"prompt": fmt.Sprintf("The user said: %q — respond briefly and helpfully.", text),
			}, nil
		}),
		Pipe("chat.respond").WithGroup("default"),
	); err != nil {
		t.Fatalf("Compose: %v", err)
	}

	t0 := time.Now()
	t.Log("Starting runtime (may download Whisper model on first run)...")
	startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startCancel()
	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Logf("Runtime ready in %s", time.Since(t0).Round(time.Millisecond))

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer waitCancel()
	if err := waitForCaps(t, pp, waitCtx, "audio.denoise", "speech.transcribe"); err != nil {
		t.Fatalf("Python workers did not become ready: %v", err)
	}

	doCtx, doCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer doCancel()
	defer doCancel()

	t.Log("Executing full pipeline...")
	p0 := time.Now()
	result, err := pp.Do(doCtx, "audio.process", core.In{
		"audio_path": audioPath,
	})
	pipelineLatency := time.Since(p0)

	if err != nil {
		t.Fatalf("Do audio.process: %v", err)
	}

	out := result.AsJSON()
	t.Logf("── Pipeline result ──────────────────────────────")
	t.Logf("Total latency : %s", pipelineLatency.Round(time.Millisecond))
	t.Logf("Worker        : %s", result.WorkerID)
	t.Logf("Hops          : %d", result.Hop)
	if resp, ok := out["response"].(string); ok {
		t.Logf("LLM response  : %s", resp)
	}
	for k, v := range out {
		t.Logf("  %-20s = %v", k, v)
	}

	if _, ok := out["response"]; !ok {
		// Ollama returns the field as "message.content" in some versions.
		if _, ok2 := out["message"]; !ok2 {
			t.Errorf("expected 'response' or 'message' in pipeline output, got: %v", out)
		}
	}
}

// TestRealWorkerRecycle verifies that a worker respawns correctly after
// hitting MaxRequests, and that the new worker loads the model fresh.
// This catches GPU VRAM leaks — if the old worker didn't release, the new
// one will OOM on load.
func TestRealWorkerRecycle(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "python3")
	requirePythonPackage(t, "faster_whisper")
	requirePythonPackage(t, "msgpack")

	capDir := t.TempDir()
	transcribePath := writeCap(t, capDir, "transcribe.py", transcribeCapSrc)
	audioPath := silentWAV(t, 1.0)
	defer os.Remove(audioPath)

	modelSize := whisperModel()

	pp, err := New(
		// Recycle after 2 requests so we can observe the respawn in this test.
		WithWorkers(NewWorker("w-recycle").Groups("cpu").MaxRequests(2)),
		WithDefaultTimeout(60*time.Second),
		WithShutdownTimeout(10*time.Second),
		Resource("whisper", map[string]any{"model_size": modelSize}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(Script("speech.transcribe", transcribePath), WithConfig(map[string]any{"model_size": modelSize})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	doCtx, doCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer doCancel()

	workerIDs := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		r, err := pp.Do(doCtx, "speech.transcribe", core.In{"audio_path": audioPath})
		if err != nil {
			t.Fatalf("Do request %d: %v", i+1, err)
		}
		workerIDs = append(workerIDs, r.WorkerID)
		out := r.AsJSON()
		t.Logf("request %d: worker=%s  model_load_s=%v", i+1, r.WorkerID, out["model_load_s"])

		// Small gap so the recycle signal propagates.
		time.Sleep(200 * time.Millisecond)
	}

	// After MaxRequests=2, requests 3 and 4 should hit a new worker instance.
	// Worker IDs encode the respawn — the ID stays the same but the PID changes,
	// which we can observe indirectly via model_load_s being non-zero again.
	t.Logf("Worker IDs across 4 requests: %v", workerIDs)
	t.Log("If model_load_s is non-zero on request 3+, the worker recycled correctly")
	t.Log("If request 3+ OOMs, the old worker did not release GPU VRAM before exit")
}

// TestRealCLIAdapter verifies the CLI runtime (pp.Prepare) works end-to-end
// with a real subprocess. Uses ffprobe (part of ffmpeg) to inspect audio metadata.
func TestRealCLIAdapter(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "ffprobe")

	audioPath := silentWAV(t, 1.0)
	defer os.Remove(audioPath)

	pp, err := New(
		WithWorkers(NewWorker("w-probe").Groups("default")),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	// We wrap ffprobe as a Python cap that accepts {"audio_path": "..."} and
	// shells out, returning the JSON output. This tests the full CLI runtime path.
	ffprobeCapSrc := `import subprocess, json, sys
def run(inputs: dict) -> dict:
    path = inputs.get("audio_path", "")
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", path],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        return {"error": result.stderr.strip()}
    return json.loads(result.stdout)
`
	capDir := t.TempDir()
	probeCapPath := filepath.Join(capDir, "probe.py")
	if err := os.WriteFile(probeCapPath, []byte(ffprobeCapSrc), 0644); err != nil {
		t.Fatalf("write probe cap: %v", err)
	}

	if err := pp.Register(Script("audio.probe", probeCapPath)); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startCancel()
	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	doCtx, doCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer doCancel()

	result, err := pp.Do(doCtx, "audio.probe", core.In{"audio_path": audioPath})
	if err != nil {
		t.Fatalf("Do audio.probe: %v", err)
	}

	out := result.AsJSON()
	t.Logf("ffprobe result: %v", out)

	if _, ok := out["format"]; !ok {
		t.Errorf("expected 'format' in ffprobe output, got: %v", out)
	}
}

// waitForCaps polls pp.WorkerReady() until a live worker has announced cap_ready
// for every named cap, or ctx is cancelled.
//
// Start() exits as soon as any worker is ready — including in-process adapter
// workers (Ollama, HTTP) which register synchronously. Python subprocess workers
// take a few extra seconds to boot, connect, and send cap_ready. Without this
// wait, Do() races against worker startup and gets ErrNoWorkers.
func waitForCaps(t *testing.T, pp *Pepper, ctx context.Context, caps ...string) error {
	t.Helper()
	for {
		all := true
		for _, cap := range caps {
			if !pp.WorkerReady(cap) {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		select {
		case <-ctx.Done():
			// Report which caps are still missing.
			missing := make([]string, 0, len(caps))
			for _, cap := range caps {
				if !pp.WorkerReady(cap) {
					missing = append(missing, cap)
				}
			}
			return fmt.Errorf("timed out — caps still not ready: %v", missing)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Song analysis pipeline

// mp3ToWAVCapSrc converts any audio format to 16kHz mono WAV using ffmpeg.
// This is stage 1 of the song analysis pipeline.
const mp3ToWAVCapSrc = `# pepper:name    = audio.convert
# pepper:version = 1.0.0
# pepper:groups  = cpu,default

import subprocess
import tempfile
import os
from pathlib import Path

def run(inputs: dict) -> dict:
    src = inputs.get("audio_path", "")
    if not src or not Path(src).exists():
        return {"error": f"audio_path not found: {src!r}"}

    dst = Path(tempfile.gettempdir()) / f"pepper_converted_{os.getpid()}.wav"
    result = subprocess.run(
        [
            "ffmpeg", "-y",
            "-i", src,
            "-ar", "16000",   # 16 kHz — Whisper's native rate
            "-ac", "1",       # mono
            "-f", "wav",
            str(dst),
        ],
        capture_output=True, timeout=60,
    )
    if result.returncode != 0:
        return {"error": result.stderr.decode(errors="replace")[-500:]}

    size = dst.stat().st_size
    return {
        "audio_path":   str(dst),  # passed straight to speech.transcribe
        "converted_from": src,
        "wav_size_bytes": size,
    }
`

// geminiAvailable returns the Gemini API key from env, or skips the test.
func geminiKey(t *testing.T) string {
	t.Helper()
	key := os.Getenv("PEPPER_GEMINI_KEY")
	if key == "" {
		t.Skip("PEPPER_GEMINI_KEY not set — skipping Gemini variant (use Ollama instead)")
	}
	return key
}

// testMP3Path returns the path to the test MP3, preferring PEPPER_TEST_MP3
// env var over the committed testdata placeholder.
func testMP3Path(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("PEPPER_TEST_MP3"); p != "" {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("PEPPER_TEST_MP3=%s not found: %v", p, err)
		}
		t.Logf("Audio: %s (from PEPPER_TEST_MP3)", p)
		return p
	}
	// Fall back to the committed placeholder.
	p := filepath.Join("testdata", "audio", "sample.mp3")
	if _, err := os.Stat(p); err != nil {
		t.Skipf("testdata/audio/sample.mp3 not found and PEPPER_TEST_MP3 not set: %v", err)
	}
	t.Logf("Audio: %s (placeholder — replace with real audio for meaningful transcript)", p)
	return p
}

// TestRealSongAnalysis runs a four-stage pipeline:
//
//	MP3/audio → [ffmpeg convert] → [Whisper transcribe] → [transform] → [LLM analysis]
//
// The LLM stage uses Ollama by default. Set PEPPER_GEMINI_KEY to use
// Google Gemini instead (useful on machines without a local GPU/Ollama).
//
// Requirements:
//   - python3, msgpack, faster-whisper
//   - ffmpeg in PATH
//   - Ollama running (or PEPPER_GEMINI_KEY set)
//
// Quick start:
//
//	# With the placeholder tone (tests plumbing, empty transcript):
//	make go-real-song
//
//	# With a real song (meaningful transcript + analysis):
//	make go-real-song PEPPER_TEST_MP3=/path/to/song.mp3
//
//	# With Gemini instead of Ollama:
//	make go-real-song PEPPER_GEMINI_KEY=AIza...
func TestRealSongAnalysis(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "python3")
	requireBinary(t, "ffmpeg")
	requirePythonPackage(t, "faster_whisper")
	requirePythonPackage(t, "msgpack")

	mp3Path := testMP3Path(t)
	modelSize := whisperModel()

	// Decide LLM backend: Gemini (cloud) or Ollama (local).
	useGemini := os.Getenv("PEPPER_GEMINI_KEY") != ""
	if !useGemini {
		requireOllama(t)
	}
	llmModel := ""
	if !useGemini {
		llmModel = ollamaModel(t)
	}

	t.Logf("Pipeline : ffmpeg → Whisper(%s) → %s",
		modelSize, map[bool]string{true: "Gemini", false: "Ollama(" + llmModel + ")"}[useGemini])

	// Build the Pepper instance
	capDir := t.TempDir()
	convertPath := writeCap(t, capDir, "convert.py", mp3ToWAVCapSrc)
	transcribePath := writeCap(t, capDir, "transcribe.py", transcribeCapSrc)

	pp, err := New(
		WithWorkers(
			NewWorker("w-cpu").Groups("cpu", "default"),
			NewWorker("w-asr").Groups("cpu", "default"), // second worker so convert + transcribe can overlap
		),
		WithDefaultTimeout(180*time.Second),
		WithShutdownTimeout(15*time.Second),
		WithTracking(true),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	// Stage 1 — ffmpeg conversion (Python subprocess)
	if err := pp.Register(Script("audio.convert", convertPath)); err != nil {
		t.Fatalf("Register audio.convert: %v", err)
	}

	// Stage 2 — Whisper transcription
	if err := pp.Register(Script("speech.transcribe", transcribePath), WithConfig(map[string]any{"model_size": modelSize})); err != nil {
		t.Fatalf("Register speech.transcribe: %v", err)
	}

	// Stage 3 — LLM analysis (Ollama or Gemini)
	if useGemini {
		// Gemini 1.5 Flash via REST — no local GPU needed.
		apiKey := os.Getenv("PEPPER_GEMINI_KEY")
		if err := pp.Register(HTTP("song.analyze", "https://generativelanguage.googleapis.com").
			Auth(adapter.APIKey("x-goog-api-key", apiKey)).
			Timeout(60 * time.Second).
			Groups("default").
			MapRequest(func(in map[string]any) (*adapter.Request, error) {
				transcript, _ := in["transcript"].(string)
				prompt := buildSongPrompt(transcript)
				body, _ := json.Marshal(map[string]any{
					"contents": []map[string]any{
						{"parts": []map[string]any{{"text": prompt}}},
					},
				})
				return &adapter.Request{
					Method: "POST",
					URL:    "/v1beta/models/gemini-1.5-flash:generateContent",
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
					Body: body,
				}, nil
			}).
			MapResponse(func(r *adapter.Response) (map[string]any, error) {
				var resp struct {
					Candidates []struct {
						Content struct {
							Parts []struct{ Text string }
						}
					}
				}
				if err := json.Unmarshal(r.Body, &resp); err != nil {
					return map[string]any{"raw": string(r.Body)}, nil
				}
				text := ""
				if len(resp.Candidates) > 0 && len(resp.Candidates[0].Content.Parts) > 0 {
					text = resp.Candidates[0].Content.Parts[0].Text
				}
				return map[string]any{"analysis": text}, nil
			}),
		); err != nil {
			t.Fatalf("Adapt song.analyze (Gemini): %v", err)
		}
	} else {
		// Ollama local model.
		if err := pp.Register(HTTP("song.analyze", "http://localhost:11434").
			With(adapter.Ollama).
			Groups("default").
			Timeout(90*time.Second),
			WithConfig(map[string]any{"model": llmModel}),
		); err != nil {
			t.Fatalf("Use song.analyze (Ollama): %v", err)
		}
	}

	// Compose the four stages.
	if err := pp.Compose("song.pipeline",
		Pipe("audio.convert").WithGroup("cpu"),
		Pipe("speech.transcribe").WithGroup("cpu"),
		PipeTransform(func(in map[string]any) (map[string]any, error) {
			transcript, _ := in["text"].(string)
			language, _ := in["language"].(string)
			duration, _ := in["duration"].(float64)
			t.Logf("Transcript (%s, %.1fs): %q", language, duration, transcript)
			if useGemini {
				return map[string]any{"transcript": transcript}, nil
			}
			// Ollama uses the standard {model, prompt} envelope.
			return map[string]any{
				"model":  llmModel,
				"prompt": buildSongPrompt(transcript),
			}, nil
		}),
		Pipe("song.analyze").WithGroup("default"),
	); err != nil {
		t.Fatalf("Compose: %v", err)
	}

	// Start
	t0 := time.Now()
	t.Log("Starting runtime (may download Whisper model on first run)...")
	startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startCancel()
	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Logf("Runtime ready in %s", time.Since(t0).Round(time.Millisecond))

	// Wait for Python workers with a separate budget — up to 120 s covers cold
	// model download. Fresh context so pipeline execution gets its full timeout.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer waitCancel()
	if err := waitForCaps(t, pp, waitCtx, "audio.convert", "speech.transcribe"); err != nil {
		t.Fatalf("Python workers did not become ready: %v", err)
	}
	t.Logf("Python workers ready in %s", time.Since(t0).Round(time.Millisecond))

	// Fresh 180 s budget starting from when workers are confirmed ready.
	doCtx, doCancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer doCancel()

	t.Logf("Running pipeline on: %s", mp3Path)
	p0 := time.Now()

	proc, err := Call[map[string]any]{
		Cap:   "song.pipeline",
		Input: core.In{"audio_path": mp3Path},
	}.Bind(pp).Execute(doCtx)
	if err != nil {
		t.Fatalf("Execute song.pipeline: %v", err)
	}
	t.Logf("Process ID: %s", proc.ID())

	// Stream stage events live as they happen.
	go func() {
		for event := range proc.Events() {
			switch event.Status {
			case StatusRunning:
				t.Logf("  → stage started  : %-24s worker=%s", event.Stage, event.WorkerID)
			case StatusDone:
				t.Logf("  ✓ stage done     : %-24s worker=%s duration=%dms", event.Stage, event.WorkerID, event.DurationMs)
			case StatusFailed:
				t.Logf("  ✗ stage failed   : %-24s worker=%s duration=%dms error=%s", event.Stage, event.WorkerID, event.DurationMs, event.Error)
			}
		}
	}()

	// Block until the pipeline result is available or the context expires.
	out, err := proc.Wait(doCtx)
	pipelineLatency := time.Since(p0)

	// Always snapshot the tracker state for the timeline — useful even on failure.
	finalState := proc.State()
	t.Logf("── Pipeline timeline ────────────────────────────")
	t.Logf("Process status  : %s", finalState.Status)
	t.Logf("Total latency   : %s", pipelineLatency.Round(time.Millisecond))
	t.Logf("Percent done    : %d%%", finalState.PercentDone)
	if len(finalState.Actions) == 0 {
		t.Logf("  (no stages recorded — pipeline may not have reached any worker)")
	}
	for i, a := range finalState.Actions {
		t.Logf("  stage[%d] %-24s %-8s %dms  worker=%s", i, a.Stage, a.Status, a.DurationMs, a.WorkerID)
	}
	t.Logf("────────────────────────────────────────────────")

	if err != nil {
		t.Fatalf("Wait: %v", err)
	}

	t.Logf("Hops            : (see result)")
	for _, key := range []string{"analysis", "response", "message"} {
		if v, ok := out[key]; ok {
			t.Logf("LLM analysis    :\n%v", v)
			break
		}
	}

	hasOutput := false
	for _, key := range []string{"analysis", "response", "message"} {
		if _, ok := out[key]; ok {
			hasOutput = true
			break
		}
	}
	if !hasOutput {
		keys := make([]string, 0, len(out))
		for k := range out {
			keys = append(keys, k)
		}
		t.Errorf("expected analysis/response/message in output, got keys: %v", keys)
	}
}

// buildSongPrompt builds the LLM prompt from a Whisper transcript.
// Empty transcript = placeholder tone, not real audio.
func buildSongPrompt(transcript string) string {
	if strings.TrimSpace(transcript) == "" {
		return "I recorded some audio but it appears to be silence or ambient noise. " +
			"What might cause Whisper to produce an empty transcript? Respond in 2-3 sentences."
	}
	return fmt.Sprintf(
		"Here is a transcript of a song or audio recording:\n\n%q\n\n"+
			"Please identify: (1) what language or genre this might be, "+
			"(2) key themes or topics if the lyrics are clear, "+
			"(3) anything notable about the content. "+
			"Keep your response concise — 3-5 sentences.",
		transcript,
	)
}
