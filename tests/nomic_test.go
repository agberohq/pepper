// Performance benchmark comparing different execution paths for Nomic embeddings.
//
// Test scenarios:
// nomic → pepper → python cap → Output           (full Pepper pipeline)
// nomic → pepper → HTTP adapter → Ollama → Output  (Pepper + HTTP adapter)
// nomic → HTTP DIRECT → Ollama → Output            (no Pepper overhead)
// nomic → Python fast HTTP → Output                (direct Python subprocess)
//
// Run tests:
//   make go-real-nomic
//
// Run benchmarks (with alloc tracking):
//   make go-real-nomic-bench

package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agberohq/pepper"
	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/runtime/adapter"
)

// shared test data

var nomicTestTexts = []string{
	"The quick brown fox jumps over the lazy dog",
	"Machine learning is transforming how we process data",
	"Vector embeddings represent text as dense numerical vectors",
}

// skip guards

func requireNomicPython(t testing.TB) {
	t.Helper()
	if err := exec.Command("python3", "-c", "import nomic").Run(); err != nil {
		t.Skipf("nomic python package not installed — skipping: pip install nomic")
	}
}

// direct HTTP client (no Pepper)

type directOllamaClient struct {
	baseURL string
}

func (c *directOllamaClient) Embed(ctx context.Context, model, prompt string) ([]float64, error) {
	body := map[string]any{
		"model":  model,
		"prompt": prompt,
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/api/embeddings", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ollama HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Embedding []float64 `json:"embedding"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}
	return result.Embedding, nil
}

// Test: Scenario 1 — Full Pepper + Python nomic cap

func TestNomic_PepperPythonCap(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "python3")
	requirePythonPackage(t, "msgpack")
	requireNomicPython(t)

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-nomic").Groups("default")),
		pepper.WithShutdownTimeout(10*time.Second),
		// nomic[local] downloads the model (~274 MB) on first call inside setup().
		// Give it up to 5 minutes so the boot wait doesn't time out mid-download.
		// On subsequent runs the model is cached and setup completes in seconds.
		pepper.WithBootTimeout(5*time.Minute),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Script("nomic.embed.direct",
		filepath.Join("testdata", "caps", "nomic.py"))); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	doCtx, doCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer doCancel()

	// Warmup
	t.Log("Nomic warmup (model load)...")
	t0 := time.Now()
	rWarm, err := pp.Do(doCtx, "nomic.embed.direct", core.In{"text": "warmup"})
	if err != nil {
		t.Fatalf("Do warmup: %v", err)
	}
	warmDuration := time.Since(t0)
	t.Logf("Warmup completed in %v", warmDuration.Round(time.Millisecond))

	// Benchmark runs
	var totalLatency time.Duration
	for i, text := range nomicTestTexts {
		t0 := time.Now()
		r, err := pp.Do(doCtx, "nomic.embed.direct", core.In{"text": text})
		latency := time.Since(t0)
		totalLatency += latency

		if err != nil {
			t.Fatalf("Do [%d]: %v", i, err)
		}
		out := r.AsJSON()
		dims := float64(0)
		if d, ok := out["dimensions"].(float64); ok {
			dims = d
		}
		t.Logf("  request[%d] latency=%v worker=%s dimensions=%.0f",
			i, latency.Round(time.Microsecond), r.WorkerID, dims)
	}

	avgLatency := totalLatency / time.Duration(len(nomicTestTexts))
	t.Logf("─── Scenario 1 (Pepper + Python cap) avg latency: %v ───",
		avgLatency.Round(time.Microsecond))

	if warmOut := rWarm.AsJSON(); warmOut != nil {
		t.Logf("Provider: %v", warmOut["provider"])
	}
}

// Test: Scenario 2 — Pepper + HTTP adapter to Ollama

func TestNomic_PepperHTTPAdapter(t *testing.T) {
	realTestsEnabled(t)
	requireOllama(t)

	model := ollamaModel(t)
	t.Logf("Ollama model: %s", model)

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-ollama-adapter").Groups("default")),
		pepper.WithShutdownTimeout(10*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	// Register Ollama as an HTTP adapter with custom request/response mapping
	if err := pp.Register(pepper.HTTP("ollama.embed", "http://localhost:11434").
		With(adapter.Ollama). // Set base adapter (required for HealthCheck)
		Groups("default").
		Timeout(30 * time.Second).
		MapRequest(func(in map[string]any) (*adapter.Request, error) {
			// Override the default Ollama request format to use /api/embeddings
			body, _ := json.Marshal(in)
			return &adapter.Request{
				Method:  "POST",
				URL:     "/api/embeddings",
				Headers: map[string]string{"Content-Type": "application/json"},
				Body:    body,
			}, nil
		}).
		MapResponse(func(r *adapter.Response) (map[string]any, error) {
			var result struct {
				Embedding []float64 `json:"embedding"`
			}
			if err := json.Unmarshal(r.Body, &result); err != nil {
				return map[string]any{
					"raw":        string(r.Body),
					"dimensions": float64(0),
				}, nil
			}
			return map[string]any{
				"embedding":  result.Embedding,
				"dimensions": float64(len(result.Embedding)),
			}, nil
		}),
	); err != nil {
		t.Fatalf("Register: %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pp.Start(startCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	doCtx, doCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer doCancel()

	var totalLatency time.Duration
	for i, text := range nomicTestTexts {
		t0 := time.Now()
		r, err := pp.Do(doCtx, "ollama.embed", core.In{
			"model":  model,
			"prompt": text,
		})
		latency := time.Since(t0)
		totalLatency += latency

		if err != nil {
			t.Fatalf("Do [%d]: %v", i, err)
		}
		out := r.AsJSON()
		dims := float64(0)
		if d, ok := out["dimensions"].(float64); ok {
			dims = d
		}
		t.Logf("  request[%d] latency=%v worker=%s dimensions=%.0f",
			i, latency.Round(time.Microsecond), r.WorkerID, dims)
	}

	avgLatency := totalLatency / time.Duration(len(nomicTestTexts))
	t.Logf("─── Scenario 2 (Pepper + HTTP adapter) avg latency: %v ───",
		avgLatency.Round(time.Microsecond))
}

// Test: Scenario 3 — Direct HTTP to Ollama (no Pepper)

func TestNomic_DirectHTTP(t *testing.T) {
	realTestsEnabled(t)
	requireOllama(t)

	model := ollamaModel(t)
	t.Logf("Ollama model: %s", model)

	client := &directOllamaClient{baseURL: "http://localhost:11434"}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var totalLatency time.Duration
	for i, text := range nomicTestTexts {
		t0 := time.Now()
		embedding, err := client.Embed(ctx, model, text)
		latency := time.Since(t0)
		totalLatency += latency
		if err != nil {
			t.Fatalf("Embed [%d]: %v", i, err)
		}
		t.Logf("  request[%d] latency=%v dimensions=%d",
			i, latency.Round(time.Microsecond), len(embedding))
		if len(embedding) == 0 {
			t.Error("got empty embedding")
		}
	}

	avgLatency := totalLatency / time.Duration(len(nomicTestTexts))
	t.Logf("─── Scenario 3 (Direct HTTP, no Pepper) avg latency: %v ───",
		avgLatency.Round(time.Microsecond))
}

// Test: Scenario 4 — Direct Python subprocess calling Ollama

func TestNomic_DirectPythonHTTP(t *testing.T) {
	realTestsEnabled(t)
	requireBinary(t, "python3")
	requireOllama(t)

	model := ollamaModel(t)

	// Write a minimal Python script that calls Ollama's HTTP API
	script := t.TempDir()
	scriptPath := filepath.Join(script, "nomic_direct.py")
	scriptSrc := fmt.Sprintf(`
import json, sys, time, urllib.request

MODEL = %q
BASE_URL = "http://localhost:11434"

def embed(text):
    body = json.dumps({"model": MODEL, "prompt": text}).encode()
    req = urllib.request.Request(BASE_URL + "/api/embeddings", data=body,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
        return data.get("embedding", [])

if __name__ == "__main__":
    texts = json.loads(sys.stdin.read())
    for text in texts:
        t0 = time.monotonic()
        emb = embed(text)
        lat = time.monotonic() - t0
        print(json.dumps({"latency_s": lat, "dimensions": len(emb)}))
`, model)
	if err := os.WriteFile(scriptPath, []byte(scriptSrc), 0644); err != nil {
		t.Fatalf("write script: %v", err)
	}

	inputJSON, _ := json.Marshal(nomicTestTexts)

	var totalLatency time.Duration
	const batches = 3
	n := len(nomicTestTexts)
	for i := 0; i < batches; i++ {
		t0 := time.Now()
		cmd := exec.CommandContext(context.Background(), "python3", scriptPath)
		cmd.Stdin = bytes.NewReader(inputJSON)
		out, err := cmd.CombinedOutput()
		latency := time.Since(t0)
		totalLatency += latency

		if err != nil {
			t.Fatalf("python subprocess [%d]: %v\n%s", i, err, string(out))
		}

		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		for j, line := range lines {
			var result struct {
				LatencyS   float64 `json:"latency_s"`
				Dimensions int     `json:"dimensions"`
			}
			if err := json.Unmarshal([]byte(line), &result); err != nil {
				t.Logf("  parse line: %v", err)
				continue
			}
			t.Logf("  batch[%d].request[%d] latency=%v dimensions=%d",
				i, j, time.Duration(result.LatencyS*float64(time.Second)).Round(time.Microsecond),
				result.Dimensions)
		}
		_ = n // suppress unused warning
	}

	avgLatency := totalLatency / batches
	t.Logf("─── Scenario 4 (Direct Python HTTP, no Pepper) avg latency: %v (includes subprocess spawn) ───",
		avgLatency.Round(time.Microsecond))
	t.Log("Note: Scenario 4 includes Python subprocess spawn overhead (~50-200ms).")
	t.Log("For fair comparison, compare Scenario 4 latency minus spawn overhead to Scenario 3.")
}

// Benchmark: Pepper dispatch overhead (no-op Go capability)

func BenchmarkNomic_PepperDispatchOnly(b *testing.B) {
	type Void struct{}

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-bench-dispatch").Groups("default")),
		pepper.WithShutdownTimeout(10*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Func("noop",
		func(ctx context.Context, in Void) (Void, error) {
			return Void{}, nil
		},
	)); err != nil {
		b.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pp.Start(ctx); err != nil {
		b.Fatalf("Start: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := pp.Do(ctx, "noop", core.In{})
		if err != nil {
			b.Fatalf("Do [%d]: %v", i, err)
		}
	}
}

// Benchmark: Direct HTTP (no Pepper)

func BenchmarkNomic_DirectHTTP(b *testing.B) {
	realTestsEnabledB(b)
	requireOllamaB(b)

	model := ollamaModelB(b)
	client := &directOllamaClient{baseURL: "http://localhost:11434"}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Warmup
	client.Embed(ctx, model, "warmup")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Embed(ctx, model, fmt.Sprintf("bench %d", i))
		if err != nil {
			b.Fatalf("Embed [%d]: %v", i, err)
		}
	}
}

// Benchmark: Pepper + HTTP adapter

func BenchmarkNomic_PepperHTTPAdapter(b *testing.B) {
	realTestsEnabledB(b)
	requireOllamaB(b)

	model := ollamaModelB(b)

	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("w-bench-ollama").Groups("default")),
		pepper.WithShutdownTimeout(10*time.Second),
		pepper.WithLogger(testLogger),
	)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.HTTP("ollama.embed", "http://localhost:11434").
		With(adapter.Ollama).
		Groups("default").
		Timeout(30 * time.Second).
		MapRequest(func(in map[string]any) (*adapter.Request, error) {
			body, _ := json.Marshal(in)
			return &adapter.Request{
				Method:  "POST",
				URL:     "/api/embeddings",
				Headers: map[string]string{"Content-Type": "application/json"},
				Body:    body,
			}, nil
		}).
		MapResponse(func(r *adapter.Response) (map[string]any, error) {
			var result struct {
				Embedding []float64 `json:"embedding"`
			}
			json.Unmarshal(r.Body, &result)
			return map[string]any{
				"embedding":  result.Embedding,
				"dimensions": float64(len(result.Embedding)),
			}, nil
		}),
	); err != nil {
		b.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := pp.Start(ctx); err != nil {
		b.Fatalf("Start: %v", err)
	}

	// Warmup
	pp.Do(ctx, "ollama.embed", core.In{"model": model, "prompt": "warmup"})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := pp.Do(ctx, "ollama.embed", core.In{
			"model":  model,
			"prompt": fmt.Sprintf("bench %d", i),
		})
		if err != nil {
			b.Fatalf("Do [%d]: %v", i, err)
		}
	}
}

// Benchmark-compatible skip helpers

func realTestsEnabledB(b *testing.B) {
	b.Helper()
	if os.Getenv("PEPPER_REAL_TESTS") != "1" {
		b.Skip("set PEPPER_REAL_TESTS=1 to run benchmarks")
	}
}

func requireOllamaB(b *testing.B) {
	b.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:11434", 500*time.Millisecond)
	if err != nil {
		b.Skip("Ollama not running on localhost:11434")
	}
	conn.Close()
}

func ollamaModelB(b *testing.B) string {
	b.Helper()
	if m := os.Getenv("PEPPER_OLLAMA_MODEL"); m != "" {
		return m
	}
	resp, err := http.Get("http://localhost:11434/api/tags")
	if err != nil {
		b.Skip("cannot list Ollama models")
	}
	defer resp.Body.Close()
	var tags struct {
		Models []struct{ Name string }
	}
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil || len(tags.Models) == 0 {
		b.Skip("no Ollama models installed")
	}
	for _, prefer := range []string{"gemma:2b", "gemma2:2b", "llama3.2:1b", "tinyllama"} {
		for _, m := range tags.Models {
			if strings.HasPrefix(m.Name, prefer) {
				return m.Name
			}
		}
	}
	return tags.Models[0].Name
}
