> WARNING: This project is under active development.

# 🌶️ Pepper

**The high-performance, zero-copy distributed runtime for the AI era.**

Pepper is a polyglot orchestrator that seamlessly bridges the reliability and concurrency of **Go** with the machine learning ecosystem of **Python**. It allows you to treat Python scripts, local CLI binaries (`ffmpeg`), external LLM APIs (OpenAI/Anthropic), and MCP servers as unified, strongly-typed **Capabilities** on a lightning-fast, zero-copy message bus.

Stop writing slow FastAPI wrappers around your PyTorch models. Stop base64-encoding 50MB images over HTTP. Use Pepper.

---

## ✨ Key Features

*   **⚡ Zero-Copy Binary Passing:** Pass massive tensors, video frames, and audio buffers between Go and Python in nanoseconds using `/dev/shm` shared memory (with graceful fallback to HTTP/S3 for multi-node clusters).
*   **🐍 First-Class Python DX:** Write ML workers using elegant `@capability` and `@resource` decorators. No boilerplate server code required.
*   **🧩 Everything is a Capability:** Unify your infrastructure. A Python script, a Go function, a shell command, and an OpenAI API call all share the exact same `MsgPack` routing envelope.
*   **🕸️ Declarative DAG Pipelines:** Compose multi-step, conditional, and scatter-gather pipelines directly in Go. The router evaluates logic dynamically to save network hops.
*   **🛡️ Bulletproof Chaos Engineering:** Built for production realities with Poison Pill detection, Dead Letter Queues (DLQ), automated worker respawns, backpressure, and end-to-end deadline propagation.
*   **🤖 Native LLM Tooling:** Instantly export your entire distributed backend as OpenAI/Anthropic function-calling tools with a single line of code.

---

## 🚀 Quick Start

### 1. The Python Worker
Write your machine learning or data-processing code in pure Python. Use the `@capability` decorator to define inputs, outputs, and concurrency limits.

```python
# caps/vision.py
from runtime import pepper, Input, Output, capability, read_blob

@capability(name="face.embed", groups=["gpu"], max_concurrent=2)
class FaceEmbedder:
    def run(self, image: Input[dict]) -> Output[dict]:
        # Zero-copy memory map directly from the Go router!
        img_matrix = read_blob(image).as_numpy()
        
        # ... run heavy PyTorch model ...
        
        return {"embedding": [0.1, 0.5, -0.2], "faces_found": 1}
```

### 2. The Go Host
Initialize the Pepper router, register your capabilities, and dispatch work.

```go
package main

import (
	"context"
	"log"
	"time"
	"github.com/agberohq/pepper"
)

func main() {
	// 1. Initialize the Pepper router
	pp, _ := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("gpu-node").Groups("gpu")),
	)
	defer pp.Stop()

	// 2. Register Capabilities
	pp.Register("face.embed", "./caps/vision.py")
	
	// You can also wrap CLI tools!
	pp.Prepare("video.extract_audio", pepper.CMD("ffmpeg", "-i", "{input}", "{output}"))

	ctx := context.Background()
	pp.Start(ctx)

	// 3. Create a Zero-Copy Blob
	blob, _ := pp.NewBlobFromFile("/tmp/crowd.jpg")
	defer blob.Close()

	// 4. Dispatch the request
	res, err := pp.Do(ctx, "face.embed", map[string]any{"image": blob.Ref()})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Result: %v", res.AsJSON())
}
```

---

## 🧠 Advanced Architecture

### The Zero-Copy `BlobRef`
Moving rich media between microservices is usually the biggest bottleneck in AI systems. Pepper solves this with the `BlobRef`.

When Go creates a blob, it writes to shared memory (`/dev/shm`) and passes a tiny metadata dictionary over the wire. When Python calls `read_blob().as_numpy()`, it memory-maps the exact same physical RAM.

If your Go router and Python worker are on different physical machines, Pepper gracefully degrades, treating the path as a URI (e.g., `s3://bucket/img.jpg` or `http://...`) and caches it locally on the worker upon the first read.

### Declarative DAG Pipelines
Stop writing coordinator microservices. Define complex routing logic in Go, and the Pepper router will handle the data flow between workers automatically.

```go
// audio.process = Extract Audio -> Transcribe -> Branch (EN vs FR)
pp.Compose("audio.process",
    compose.Pipe("video.extract_audio").WithGroup("cpu"),
    
    // Scatter to all available GPU workers, gather results
    compose.Scatter("speech.transcribe").
        WithGroup("gpu").
        Gather("transcript.merge", compose.GatherAll),
        
    // Route dynamically based on the output of the previous step
    compose.Branch(
        compose.When("language == 'en'", compose.Pipe("nlp.summarize_en")),
        compose.When("language == 'fr'", compose.Pipe("nlp.summarize_fr")),
        compose.Otherwise(compose.Return(map[string]any{"status": "unsupported"})),
    ),
)
```

### Auto-Generating LLM Tools
Because Pepper knows the schema of every registered capability (via Python type hints or Go structs), you can expose your entire infrastructure to an LLM Agent instantly.

```go
// Get all capabilities tagged with the "tools" group
schemas := pp.Capabilities(ctx, pepper.FilterByGroup("tools"))

// Export to OpenAI format
openAITools := pepper.ToOpenAITools(schemas)

// Pass directly to the OpenAI SDK
resp, _ := openaiClient.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
    Model:    openai.GPT4o,
    Messages: messages,
    Tools:    openAITools, // Pepper handles the schema bridging!
})
```

---

## 🛡️ Production Resilience

AI workloads are notorious for crashing (OOMs, CUDA errors, bad data). Pepper isolates your Go backend from Python's instability.

*   **Poison Pill DLQ:** If a specific payload crashes a Python worker multiple times, Pepper blacklists the `origin_id`, stops retrying it, and writes the payload to a Dead Letter Queue for debugging.
*   **Load-Aware Routing:** Requests are routed to the worker with the lowest active pipeline depth.
*   **Global Timeouts:** A `context.WithTimeout` in Go propagates across the entire message bus. If the deadline passes, the router's `Reaper` drops the message, and a `BroadcastCancel` interrupts the Python thread.
*   **Resource Management:** Use `@resource` in Python to manage heavy, long-lived objects (like DB pools or LLM weights). Pepper initializes them once at boot and cleans them up cleanly on teardown.

---

## 🔌 Supported Transports & Adapters

**Transports:**
*   `mula://` (Default) - Ultra-fast, zero-dependency custom TCP framing.
*   `nats://` (Coming Soon) - Distributed queue groups via NATS JetStream.
*   `redis://` (Coming Soon) - Distributed Pub/Sub via Redis.

**Adapters:**
Wrap external APIs as native Pepper capabilities with automatic retries and timeouts:
*   `adapter.OpenAI`
*   `adapter.Anthropic`
*   `adapter.Ollama`
*   `adapter.MCP` (Model Context Protocol servers)

---

## License

MIT License. See `LICENSE` for more information.