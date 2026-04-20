> WARNING: This project is under active development.

# 🌶️ Pepper

**The high-performance, zero-copy distributed runtime for the AI era.**

Pepper is a polyglot orchestrator that bridges the reliability and concurrency of Go with the machine learning ecosystem of Python. It lets you treat Python scripts, local CLI binaries, external LLM APIs (OpenAI, Anthropic), and MCP servers as unified, strongly-typed **Capabilities** on a fast, zero-copy message bus.

Stop writing FastAPI wrappers around your PyTorch models. Stop base64-encoding 50MB images over HTTP. Use Pepper.

---

## Key Features

- **Zero-Copy Binary Passing.** Pass tensors, video frames, and audio buffers between Go and Python using `/dev/shm` shared memory, with graceful fallback to HTTP/S3 for multi-node clusters.
- **First-Class Python DX.** Write ML workers with `@capability` and `@resource` decorators. No boilerplate server code.
- **Everything is a Capability.** A Python script, a Go function, a shell command, and an OpenAI API call all share the same MsgPack routing envelope.
- **Declarative DAG Pipelines.** Compose multi-step, conditional, and scatter-gather pipelines in Go. The router evaluates logic dynamically to save network hops.
- **Production Resilience.** Poison Pill detection, Dead Letter Queues, automated worker respawns, backpressure, and end-to-end deadline propagation.
- **Native LLM Tooling.** Export your entire backend as OpenAI or Anthropic function-calling tools with one line.

---

## Quick Start

### 1. The Python Worker

```python
# caps/vision.py
from runtime import Input, Output, capability, read_blob

@capability(name="face.embed", groups=["gpu"], max_concurrent=2)
class FaceEmbedder:
    def setup(self):
        import torch
        self.model = torch.load("model.pt")

    def run(self, image: Input[dict]) -> Output[dict]:
        img_matrix = read_blob(image).as_numpy()
        embedding = self.model(img_matrix).tolist()
        return {"embedding": embedding}
```

### 2. The Go Host

```go
package main

import (
    "context"
    "log"

    "github.com/agberohq/pepper"
)

func main() {
    pp, err := pepper.New(
        pepper.WithWorkers(pepper.NewWorker("gpu-node").Groups("gpu")),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer pp.Stop()

    if err := pp.Register(pepper.Script("face.embed", "./caps/vision.py")); err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    if err := pp.Start(ctx); err != nil {
        log.Fatal(err)
    }

    blob, err := pp.NewBlobFromFile("/path/to/image.jpg")
    if err != nil {
        log.Fatal(err)
    }
    defer blob.Close()

    type EmbedOut struct {
        Embedding []float32 `json:"embedding"`
    }

    result, err := pepper.Call[EmbedOut]{
        Cap:   "face.embed",
        Input: pepper.In{"image": blob.Ref()},
    }.Bind(pp).Do(ctx)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("embedding: %v", result.Embedding)
}
```

---

## Capabilities

Pepper unifies different runtimes behind a single `Register` call.

**Python script** — a single `.py` file with a `@capability` class:

```go
pp.Register(pepper.Script("speech.transcribe", "./caps/transcribe.py"))
```

**Python directory** — every `.py` file under a directory becomes a capability:

```go
pp.Register(pepper.Dir("./caps").Groups("gpu"))
```

**Go function** — inline typed handler, no subprocess:

```go
type SumIn struct{ Values []float64 `json:"values"` }
type SumOut struct{ Total float64 `json:"total"` }

pp.Register(pepper.Func("math.sum", func(ctx context.Context, in SumIn) (SumOut, error) {
    var total float64
    for _, v := range in.Values {
        total += v
    }
    return SumOut{Total: total}, nil
}))
```

**CLI tool** — wrap any binary as a capability:

```go
pp.Register(pepper.CLI("video.probe", "ffprobe", "-v", "quiet", "-print_format", "json", "-show_format"))
```

**HTTP adapter** — wrap any REST API:

```go
pp.Register(pepper.HTTP("sentiment.analyze", "https://api.example.com/sentiment"))
```

**Built-in LLM adapters:**

```go
pp.Register(pepper.HTTP("llm.chat", "").With(pepper.OpenAI))
pp.Register(pepper.HTTP("llm.chat", "").With(pepper.AnthropicAdapter))
pp.Register(pepper.HTTP("llm.chat", "http://localhost:11434").With(pepper.Ollama))
```

**MCP server** — expose all tools from an MCP server as capabilities:

```go
pp.Register(pepper.MCP("tools.browser", "https://mcp.example.com/sse"))
// or auto-name from URL:
pp.AdaptMCP("https://mcp.example.com/sse")
```

---

## Calling Capabilities

**Typed single call:**

```go
type AnalysisOut struct {
    Sentiment string  `json:"sentiment"`
    Score     float64 `json:"score"`
}

result, err := pepper.Call[AnalysisOut]{
    Cap:     "sentiment.analyze",
    Input:   pepper.In{"text": "Pepper is fast"},
    Timeout: 10 * time.Second,
}.Bind(pp).Do(ctx)
```

**Fire-and-forget:**

```go
err := pepper.Exec{
    Cap:   "audit.log",
    Input: pepper.In{"event": "login", "user": "alice"},
}.Bind(pp).Do(ctx)
```

**Parallel fan-out:**

```go
results, err := pepper.All[AnalysisOut](ctx, pp,
    pepper.MakeCall("sentiment.analyze", pepper.In{"text": "first"}),
    pepper.MakeCall("sentiment.analyze", pepper.In{"text": "second"}),
)
```

**Streaming output:**

```go
ch, err := pepper.Call[TranscriptChunk]{
    Cap:   "speech.transcribe",
    Input: pepper.In{"audio_path": "/tmp/audio.wav"},
}.Bind(pp).Stream(ctx)
if err != nil {
    log.Fatal(err)
}
for chunk := range ch {
    fmt.Print(chunk.Text)
}
```

**Bidirectional streaming:**

```go
type AudioChunk struct{ Data []byte `json:"data"` }
type TranscriptChunk struct{ Text string `json:"text"` }

stream, err := pepper.OpenStream[AudioChunk, TranscriptChunk](
    ctx, pp, "speech.transcribe", pepper.In{"language": "en"},
)
if err != nil {
    log.Fatal(err)
}

go func() {
    for _, chunk := range audioChunks {
        stream.Write(AudioChunk{Data: chunk})
    }
    stream.CloseInput()
}()

for chunk := range stream.Chunks(ctx) {
    fmt.Print(chunk.Text)
}
```

---

## DAG Pipelines

Compose multi-step pipelines in Go. The router evaluates routing logic without extra network hops.

```go
pp.Compose("audio.pipeline",
    pepper.Pipe("audio.convert").WithGroup("cpu"),
    pepper.Pipe("speech.transcribe").WithGroup("asr"),
    pepper.PipeTransform(func(in map[string]any) (map[string]any, error) {
        transcript, _ := in["text"].(string)
        return pepper.In{"prompt": "Summarise: " + transcript}, nil
    }),
    pepper.Pipe("llm.chat"),
)

result, err := pp.Do(ctx, "audio.pipeline", pepper.In{"audio_path": "/tmp/speech.wav"})
```

**Async pipeline with stage events:**

```go
proc, err := pepper.Call[SummaryOut]{
    Cap:   "audio.pipeline",
    Input: pepper.In{"audio_path": "/tmp/speech.wav"},
}.Bind(pp).Execute(ctx)
if err != nil {
    log.Fatal(err)
}

go func() {
    for event := range proc.Events() {
        log.Printf("stage %s: %s (%dms)", event.Stage, event.Status, event.DurationMs)
    }
}()

result, err := proc.Wait(ctx)
```

---

## LLM Tool Calling

Because Pepper knows the schema of every registered capability, you can expose your backend to an LLM agent in one call.

```go
schemas := pp.Capabilities(ctx, pepper.FilterByGroup("tools"))

// OpenAI
openAITools := pepper.Tools(schemas, pepper.FormatOpenAI)

// Anthropic
anthropicTools := pepper.Tools(schemas, pepper.FormatAnthropic)
```

---

## Zero-Copy Blobs

Passing large binary data between Go and Python without serialisation:

```go
// From raw bytes
blob, err := pp.NewBlob(imageBytes)
defer blob.Close()

// From a file
blob, err := pp.NewBlobFromFile("/tmp/frame.jpg")
defer blob.Close()

// Pass the ref in any capability input
result, err := pp.Do(ctx, "face.embed", pepper.In{"image": blob.Ref()})
```

In Python, `read_blob(image).as_numpy()` memory-maps the exact same shared memory page. On multi-node deployments, Pepper falls back to treating the path as a URI and caches locally on first access.

---

## Production Resilience

- **Poison Pill detection.** If a payload crashes a Python worker more than `WithPoisonPillThreshold(n)` times, Pepper blacklists the `origin_id`, stops retrying, and writes the payload to the DLQ.
- **Dead Letter Queue.** Persist poison pills to disk for debugging and replay: `pepper.WithDLQ(pepper.FileDLQ("./dlq"))`.
- **Deadline propagation.** A `context.WithTimeout` in Go propagates across the entire message bus. When the deadline passes, the router drops the pending request and broadcasts a cancel signal to interrupt running workers.
- **Load-aware routing.** Requests route to the worker with the lowest active load by default. Override with `WithStrategy(pepper.StrategyRoundRobin)` or `WithStrategy(pepper.StrategyLeastLoaded)`.
- **Worker recycling.** Automatically restart workers after N requests or a maximum uptime: `pepper.NewWorker("w-1").MaxRequests(10000).MaxUptime(24 * time.Hour)`.

---

## Sessions

Maintain state across multiple capability calls:

```go
sess := pp.NewSession()

result, err := pepper.Call[ReplyOut]{
    Cap:   "chat.reply",
    Input: pepper.In{"message": "Hello"},
}.Session(sess).Do(ctx)

// Later, with the same session:
sess2 := pp.Session(sess.ID()) // resume from cookie, JWT, etc.
```

---

## Transports

| Transport | Default | Notes |
|-----------|---------|-------|
| `mula://` | Yes | Zero-dependency custom TCP framing |
| `nats://` | No | Coming soon — NATS JetStream |
| `redis://` | No | Coming soon — Redis Pub/Sub |

Override with `pepper.WithTransport(pepper.TransportTCPLoopback)` or `pepper.WithTransportURL("tcp://0.0.0.0:7731")`.

---

## License

MIT License. See `LICENSE` for more information.
