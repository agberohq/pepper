# Pepper

[![CI](https://github.com/agberohq/pepper/actions/workflows/go.yml/badge.svg)](https://github.com/agberohq/pepper/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/agberohq/pepper.svg)](https://pkg.go.dev/github.com/agberohq/pepper)
[![Go 1.25+](https://img.shields.io/badge/go-1.25%2B-00ADD8)](go.mod)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-3776AB)](runtime/python/pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**The polyglot capability runtime for ML and LLM workloads.**

Pepper turns Python scripts, Rust and Julia code, Node.js modules, CLI binaries, HTTP/LLM
APIs, MCP servers, and plain Go functions into one thing: a **capability you call
by name**. Every call travels as a MsgPack envelope over a message bus, so
routing, deadlines, retries, streaming, backpressure, and observability are
implemented once in the router — not again in every wrapper.

- **One call surface.** `pepper.Call[Out]{Cap: "face.embed"}.Bind(pp).Do(ctx)`
  works whether the handler is a Go closure, a Python class, an `ffprobe`
  subprocess, or `api.openai.com`.
- **Large payloads without base64.** Blobs are written to shared-memory-backed
  files and passed by reference; Python memory-maps the same bytes instead of
  receiving a copy.
- **Local first, cluster later.** Single node with zero external services by
  default. Point it at Redis or NATS when you need more nodes.

> **Status:** early and actively developed; no tagged release yet. The table
> below is the authoritative list of what is wired up today — please read it
> before designing around a feature.

## Status

| Area | State |
|---|---|
| Python / Julia / Node.js / **Rust** / Go / CLI / HTTP / MCP capabilities | working |
| Typed calls, `Exec`, `All`, async `Execute`/`Process[Out]` | working, covered end to end |
| Server streaming and bidirectional streaming | working with credit-based flow control (Go + Python; Rust emits server-side chunks) |
| Pipelines: `Pipe`, `Transform`, `PipeReturn` | working |
| Pipelines: `PipeScatter` gather, branch stages | `Compose` rejects them with a clear error until the router can gather/branch |
| Blobs on one node, or across nodes sharing a filesystem | working |
| Blobs through S3 / an object store | `blob.S3Backend` exists; `WithBlobStore` is not wired yet |
| Poison pill detection, DLQ, retries, deadlines, worker recycling | working |
| Per-worker circuit breaker | working, opt-in via `WithCircuitBreaker(threshold, openFor)` (disabled by default) |
| Worker authentication (Ed25519 challenge-response) | working, opt-in via `WithAuthKeys` |
| Per-capability rate limiting | working, opt-in via `WithRateLimit` |
| Health data (`Pepper.Health()`) | working, returns `HealthStatus` struct |
| Sessions, in-memory or coord-backed | working |
| `pepper.CodecJSON` with script workers | refused at `Register` with an explicit error (script runtimes speak msgpack) |
| Redis / NATS / Mula coordination | working (NATS uses prefix-based subscriptions) |
| Wire inspector (`pepper inspect` + `inspect` package) | CLI and library exist; the runtime does not emit events to it yet |
| Rust runtime | multi-capability workers, compile-on-boot from `Start()`, `// pepper:deps`, server streaming, real concurrency, auth, cached/prebuilt runs without cargo |
| Rust runtime: `pepper.call()` callbacks, Redis/NATS worker transport | Python-only today |
| `WithInspector`, `WithTLS`, `WithTransport`, `WithStrategy`, `WithMemoryBudget`, `WithStreamCredits` | declared, not wired yet — see [configuration](#configuration) |

---

## Install

| Component | Version | Needed for |
|---|---|---|
| Go | 1.25+ (see `go.mod`) | the orchestrator |
| Python | 3.10+ with `msgpack` | Python capabilities |
| Julia | any, with `MsgPack.jl` | Julia capabilities |
| Node.js | any, with `@msgpack/msgpack` | Node.js capabilities |
| Rust | stable toolchain with `cargo` | Rust capabilities (compiled on first use) |

```bash
go get github.com/agberohq/pepper
pip install msgpack            # only if you use Python capabilities
```

Worker runtimes are discovered, not configured. Pepper looks for
`runtime/python/runtime.py` (then `runtime.jl`, `runtime.js`) in `PEEPER_HOME`
and its `python`, `runtime`, `runtime/python` subdirectories, then the working
directory and its three parents, every `PATH` entry, the directory holding your
binary, and the usual system prefixes (`/opt/peeper`, `/usr/local/share/peeper`,
…). While developing, the simplest option is to point `PEEPER_HOME` at your
pepper checkout:

```bash
export PEEPER_HOME=/path/to/pepper     # contains runtime/python/runtime.py
```

## Quick start

### 1. A capability in Python

`caps/vision.py`:

```python
from cap import Input, Output, capability, read_blob
from runtime import pepper      # pepper.kv(), pepper.config(), pepper.session(), ...

@capability(name="face.embed", groups=["gpu"], max_concurrent=2)
class FaceEmbedder:
    def setup(self, config):
        import torch
        self.model = torch.load(config.get("model", "model.pt"))

    def run(self, image: Input[dict]) -> Output[dict]:
        pixels = read_blob(image).as_numpy()      # mmap when dtype+shape are set
        return {"embedding": self.model(pixels).tolist()}
```

`cap` provides the decorators, schema extraction and blob helpers; `runtime`
provides the worker-side `pepper` handle. `setup()` runs once per worker at
boot, `run()` once per request.

### 2. The Go host

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/agberohq/pepper"
)

type EmbedOut struct {
	Embedding []float32 `msgpack:"embedding"`
}

func main() {
	pp, err := pepper.New(
		pepper.WithWorkers(pepper.NewWorker("gpu-1").Groups("gpu")),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pp.Stop()

	if err := pp.Register(pepper.Script("face.embed", "./caps/vision.py")); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := pp.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Start() returns as soon as the *first* worker is ready. If you depend on
	// a specific capability, wait for its own worker before the first call.
	if err := pp.WaitWorkerReady(ctx, "face.embed"); err != nil {
		log.Fatal(err)
	}

	blob, err := pp.NewBlobFromFile("/path/to/image.jpg")
	if err != nil {
		log.Fatal(err)
	}
	defer blob.Close()

	out, err := pepper.Call[EmbedOut]{
		Cap:   "face.embed",
		Input: pepper.In{"image": blob.Ref()},
	}.Bind(pp).Do(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("embedding: %v", out.Embedding)
}
```

### 3. Run it

```bash
PEEPER_HOME=/path/to/pepper go run .
```

If the capability never becomes ready, verify that `python3 -c "import msgpack"`
succeeds and that `PEEPER_HOME` points at the directory holding `runtime.py`.

---

## Architecture

```
  your Go code                     pepper.New(...)                       workers
 ┌──────────────┐   Call[Out]   ┌──────────────────────────────────┐   ┌─────────────┐
 │  pp.Do(...)  │──────────────▶│ router ──▶ registry ──▶ bus      │──▶│ python (.py)│
 │  Call/Exec   │◀──────────────│   ▲          (caps, groups)      │◀──│ julia (.jl) │
 │  All/Stream  │   Result      │   └── pending, tracker, hooks    │   │ node (.js)  │
 └──────────────┘               └──────────────────────────────────┘   │ go / cli    │
                                 codec: msgpack | json                  │ http / mcp  │
                                 coord:  mula | redis | nats   (opt.)  └─────────────┘
```

Three independent layers, often confused for one:

- **Codec** — how payloads are serialised (`msgpack` by default, `json` opt-in).
- **Transport / bus** — how a node talks to its own worker processes and to
  other nodes. Chosen for you: an in-process Mula TCP bus on a single node, or
  the coord backend when you set `WithCoord`.
- **Coord** — the shared store used for registry replication, distributed
  sessions and cross-node pub/sub: `mula` (built-in, memberlist-based), `redis`,
  or `nats` (JetStream).

A call becomes an `envelope` (correlation id, origin id, cap, group, deadline,
hop count, meta) and round-trips through the router, which owns routing,
retries, deadlines, poison-pill tracking and result correlation.

---

## Capabilities

Every runtime registers through the same call: `pp.Register(capability, opts...)`.

**Script file** — runtime inferred from the extension
(`.py`, `.jl`, `.js`, `.mjs`, `.rs`):

```go
pp.Register(pepper.Script("speech.transcribe", "./caps/transcribe.py"))
pp.Register(pepper.Script("rerank.cross", "./caps/rerank.rs"))
pp.Register(pepper.Script("embed.text", "./caps/embed.jl"))
pp.Register(pepper.Script("search.web", "./caps/search.js"))
```

Script capabilities also accept `Groups`, `Version`, `Timeout`, `Config`:

```go
pp.Register(pepper.Script("speech.transcribe", "./caps/transcribe.py").
    Groups("gpu", "asr").
    Config(map[string]any{"model_size": "small"}))
```

**Directory** — every script under `./caps` becomes a capability, named from its
path (`nlp/embed.py` → `nlp.embed`). Mixed-language directories work; hidden
files, `node_modules`, and the runtime's own `runtime.*`/`cap.*` files are skipped.

```go
pp.Register(pepper.Dir("./caps").Groups("gpu"))
```

**Go function** — in-process, no subprocess, no serialisation of your types:

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

**CLI binary** — the payload is written to a temp file and its path substituted
for `{input}`; the result is read from the file substituted for `{output}`. Other
placeholders are filled from the input map (`{audio_path}`). Use `Stdin`/`Stdout`
for tools that speak JSON over stdio:

```go
// file in, file out
pp.Register(pepper.CLI("video.thumbs", "ffmpeg", "-y", "-i", "{input}", "{output}").
    Input("frame-*.png").
    Output("thumbs-*.png").
    Groups("cpu"))

// JSON on stdin, JSON on stdout
pp.Register(pepper.CLI("video.probe", "ffprobe", "-v", "quiet",
    "-print_format", "json", "-show_format", "-").
    Stdin(pepper.JSONStdin).
    Stdout(pepper.JSONStdout))
```

Also available on CLI capabilities: `Args`, `WorkDir`, `Pool(n)`, `Env("KEY=v")`.

**HTTP / adapter** — POST the input as JSON, receive JSON back:

```go
pp.Register(pepper.HTTP("sentiment.analyze", "https://api.example.com/sentiment"))
```

Built-in LLM adapters set the URL path, request body and response shape; you
supply the base URL and the auth provider:

```go
pp.Register(pepper.HTTP("llm.chat", "https://api.openai.com").
    With(pepper.OpenAI).
    Auth(pepper.BearerToken(os.Getenv("OPENAI_API_KEY"))))

pp.Register(pepper.HTTP("llm.chat", "https://api.anthropic.com").
    With(pepper.AnthropicAdapter).
    Auth(pepper.APIKey("x-api-key", os.Getenv("ANTHROPIC_API_KEY"))))

pp.Register(pepper.HTTP("llm.chat", "http://localhost:11434").With(pepper.Ollama))
```

**MCP server** — every tool the server exposes becomes a capability:

```go
pp.Register(pepper.MCP("tools.browser", "https://mcp.example.com/sse"))

// or derive the name from the URL: mcp.https-mcp-example-com-sse
_ = pp.AdaptMCP("https://mcp.example.com/sse")
```

---

## Calling capabilities

**Typed call** — the result is decoded into your struct through the active codec:

```go
type AnalysisOut struct {
    Sentiment string  `msgpack:"sentiment"`
    Score     float64 `msgpack:"score"`
}

result, err := pepper.Call[AnalysisOut]{
    Cap:     "sentiment.analyze",
    Input:   pepper.In{"text": "pepper is fast"},
    Timeout: 10 * time.Second,
}.Bind(pp).Do(ctx)
```

> **Struct tags matter.** The default codec is msgpack and its decoder matches
> `msgpack:"…"` tags. A result struct tagged only with `json:"…"` decodes
> *silently* into zero values. Tag with `msgpack:"…"` (or both — that is what the
> internal envelope does). `pepper.CodecJSON` exists for tooling, but the bundled
> Python/Node/Julia runtimes expect msgpack, so it is not a drop-in alternative.

`Call[Out]` also accepts `Group`, `Worker`, `Dispatch`, `Quorum`, `Deadline`,
`Version` and `Meta`, and can be bound to a session with `.Session(sess)`.

**Untyped call** — when there is no struct to decode into:

```go
res, err := pp.Do(ctx, "sentiment.analyze", pepper.In{"text": "hello"})
score := res.AsFloat()   // AsString, AsJSON, AsInt, AsBool, AsBytes, AsBlob, Into(&v)
fmt.Println(res.WorkerID, res.Latency, res.Hop, res.Meta)
```

**Fire-and-forget** — returns as soon as the request is published:

```go
err := pepper.Exec{
    Cap:   "audit.log",
    Input: pepper.In{"event": "login", "user": "alice"},
}.Bind(pp).Do(ctx)
```

**Parallel fan-out** — one goroutine per call, results in call order:

```go
results, err := pepper.All[AnalysisOut](ctx, pp,
    pepper.MakeCall("sentiment.analyze", pepper.In{"text": "first"}),
    pepper.MakeCall("sentiment.analyze", pepper.In{"text": "second"}),
)
```

**Server streaming** — the channel closes when the worker stops yielding:

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
    ctx, pp, "speech.transcribe", AudioChunk{},        // seed chunk
    pepper.WithMeta("language", "en"),
)
if err != nil {
    log.Fatal(err)
}

go func() {
    for _, chunk := range audioChunks {
        _ = stream.Write(AudioChunk{Data: chunk})
    }
    _ = stream.CloseInput()
}()

for chunk := range stream.Chunks(ctx) {
    fmt.Print(chunk.Text)
}
```

**Dispatch modes** — how many workers answer a single call:

| Mode | Constant | Behaviour |
|---|---|---|
| `any` (default) | `pepper.DispatchAny` | one worker, preferring a worker that already has the cap loaded |
| `all` | `pepper.DispatchAll` | every worker in the group, all responses collected |
| `first` | `pepper.DispatchFirst` | broadcast, the first response wins |
| `vote` | `pepper.DispatchVote` | broadcast, the majority response wins |
| `quorum` | `pepper.DispatchQuorum` | broadcast, wait for `Quorum` responses |

```go
// every worker that owns the cap, all answers collected
all, err := pp.Broadcast(ctx, "face.embed", in)

// an explicit group + mode, waiting for two responses
two, err := pp.Group(ctx, "gpu", string(pepper.DispatchQuorum), "face.embed", in,
    pepper.WithCallQuorum(2))

// the typed equivalent
typed, err := pepper.Call[EmbedOut]{Cap: "face.embed", Input: in}.Bind(pp).Broadcast(ctx)
```

**Ad-hoc Python** — run a snippet without registering a file:

```go
res, err := pp.Run(ctx, `
    import numpy as np
    return {"mean": float(np.array(data).mean())}
`, pepper.Vars{"data": mySlice})
```

---

## Python capabilities

Two equivalent forms — pick one per file.

**Class form.** `@capability(...)` decorates a class: `setup(config)` runs once per
worker at boot, `run(**inputs)` once per request, `teardown()` on shutdown.

```python
from cap import Input, Output, capability, resource
from runtime import pepper

@resource(name="db")
def db_pool(config):
    import psycopg
    conn = psycopg.connect(config["dsn"])
    yield conn                  # everything after yield runs on worker shutdown
    conn.close()

@capability(
    name="doc.answer",
    version="1.2.0",
    deps=["psycopg>=3"],
    groups=["cpu"],
    timeout="30s",
    max_concurrent=4,
    resources=["db"],           # injected as self.db before run()
    description="Answer a question from the documents table",
)
class DocAnswer:
    def setup(self, config):
        self.table = config.get("table", "docs")

    def run(self, question: Input[dict]) -> Output[dict]:
        return {"answer": "..."}
```

**Header form.** A plain module with `run(inputs)`, configured by contiguous
`# pepper:` comments at the top of the file:

```python
# pepper:name           = speech.transcribe
# pepper:version        = 1.0.0
# pepper:deps           = faster-whisper>=1.0
# pepper:groups         = gpu
# pepper:max_concurrent = 1

def setup(config):
    global model
    model = load_model(config.get("model", "tiny"))

def run(inputs):
    return {"text": model.transcribe(inputs["audio_path"])}
```

Input and output JSON schemas are derived from the `Input[...]` / `Output[...]`
annotations (or pydantic models) and from `run()`'s parameters. Those schemas are
what `pepper.Tools` exports to an LLM.

### The `pepper` handle

| Call | Purpose |
|---|---|
| `pepper.config()` | the map passed to `pepper.Script(...).Config(...)` |
| `pepper.kv(ns)` | process-wide cache shared by every cap on the worker: `get`, `set`, `get_or_set`, `delete`, `keys`, `clear` |
| `pepper.cache_dir()` | `PEPPER_CACHE_DIR`, else `HF_HOME`, else `~/.cache/pepper` |
| `pepper.session()` | `get(key)` / `set(key, value)`; writes are merged back into the session |
| `pepper.call(cap, inputs, timeout=30.0)` | call another capability from inside a worker |
| `pepper.forward(topic, payload)` · `pepper.pipe(...)` · `pepper.gather(...)` | worker-to-worker mesh, without a router hop |
| `pepper.new_blob(data, dtype=..., shape=...)` | return a blob instead of copying bytes |
| `pepper.meta()` · `pepper.corr_id()` · `pepper.origin_id()` · `pepper.delivery_count()` · `pepper.is_cancelled()` | request context |

## Node.js, Julia and Rust capabilities

Both runtimes use the header form plus an optional `setup`, and both are driven by
the same envelope contract as Python.

```js
// caps/search.js
// pepper:name   = search.web
// pepper:groups = cpu
"use strict";

async function setup(config) {
  // runs once per worker
}

async function run(inputs) {
  return { results: [] };
}

module.exports = { setup, run };   // `pepper` is injected as a global
```

```julia
# caps/embed.jl
# pepper:name   = embed.text
# pepper:groups = gpu

const cap = include("cap.jl")      # runtime dir is on LOAD_PATH

function setup!(config::Dict)
end

function run(inputs::Dict)::Dict
    return Dict("embedding" => Float32[])
end
```

Register them like any other script — `pepper.Script("search.web", "./caps/search.js")`,
`pepper.Script("embed.text", "./caps/embed.jl")` — or let `pepper.Dir("./caps")` pick
them up by extension.

## Rust capabilities

Rust capabilities are `.rs` files, registered exactly like the other runtimes — and
one worker serves **all** the files that share its groups, just as a Python worker
serves several `.py` files. You never think about workers, compilation or caching:
you register files.

```go
pp.Register(pepper.Script("rerank.cross", "./caps/rerank.rs").Groups("gpu"))
pp.Register(pepper.Script("embed.text",   "./caps/embed.rs").Groups("gpu"))
// one process, two capabilities, one model cache
```

```rust
// caps/rerank.rs
// pepper:deps = candle-core@0.8, tokenizers@0.19

use pepper_runtime::prelude::*;

#[derive(Default)]
struct Rerank {
    model: Option<Model>,
}

impl Capability for Rerank {
    fn setup(&mut self, config: &Config) -> Result<()> {
        self.model = Some(Model::load(config.str("model").unwrap_or("bge-reranker"))?);
        Ok(())
    }

    fn run(&self, ctx: &Context, input: Input) -> Result<Value> {
        let query = input.str_or("query", "");
        let docs = input.array("documents").cloned().unwrap_or_default();
        let scores = self.model.as_ref().unwrap().score(query, &docs);
        Ok(json!({ "scores": scores, "worker": ctx.worker_id }))
    }
}

capability! {
    name: "rerank.cross",
    version: "1.0.0",
    groups: ["gpu"],
    handler: Rerank,
}
```

`setup` runs once per worker, `run` once per request. Blobs are zero-copy:
`input.blob("tensor")?.as_f32()?` memory-maps the file and derefs to `&[f32]`, and
`new_tensor(bytes, "float32", &[1, 768])` returns a reference for the reply.

**Compilation is the runtime's job, not yours.** `Start()` compiles the capability
set for each matching worker *before* the boot deadline begins, sharing one cargo
target directory so a crate is built once per machine. A capability that does not
compile fails `Start()` with rustc's message and the file that broke:

```
pepper: start: pepper: rust worker w-1: exit status 1
error: expected one of `!`, `.`, `::`, `;`, `?`, `{`, `}`, or an operator, found `does`
 --> src/cap_0_rerank.rs:8:14
```

`// pepper:deps` declares crates with the same convention as the Python, Julia and
Node runtimes; they are merged into the generated manifest (your version wins).

| Variable | Effect |
|---|---|
| `PEPPER_RUST_WORKER_BIN` | use a prebuilt `pepper-worker`, skipping the runtime-crate build |
| `PEPPER_CACHE_DIR` | where compiled capabilities live (default `~/.cache/pepper`) |
| `cargo` in `PATH` | required unless the above override is set |

**Concurrency and streaming are real.** `run` takes `&self`, so one handler instance
serves parallel requests (four 250 ms calls on one worker finish in ~250 ms, not a
second) and `PEPPER_MAX_CONCURRENT` bounds how many execute at once. Override
`run_stream` to emit chunks for `pepper.Call[T]{...}.Stream(ctx)`; bidirectional
input chunks arrive through `ctx.next_chunk()`.

**Deployment needs no toolchain.** Compilation happens once into
`$PEPPER_CACHE_DIR`; after that the cached binary runs directly (multi-stage Docker
or air-gapped hosts just ship the cache and a prebuilt `pepper-worker`).

**Still open** (tracked in the [status table](#status)): `pepper.call()` callbacks
and Redis/NATS worker transport. See
[`runtime/rust/README.md`](runtime/rust/README.md) for the full contract.

---

## Pipelines

A pipeline is a capability composed of stages, dispatched as a single call. The
router runs `Transform` and `PipeReturn` stages inline — no extra network hop — and
hands the payload to the next worker in the chain.

```go
if err := pp.Compose("audio.pipeline",
    pepper.Pipe("audio.convert").WithGroup("cpu"),
    pepper.Pipe("speech.transcribe").WithGroup("asr"),
    pepper.Transform(func(in map[string]any) (map[string]any, error) {
        transcript, _ := in["text"].(string)
        return pepper.In{"prompt": "Summarise: " + transcript}, nil
    }),
    pepper.Pipe("llm.chat"),
); err != nil {
    log.Fatal(err)
}

result, err := pp.Do(ctx, "audio.pipeline", pepper.In{"audio_path": "/tmp/speech.wav"})
```

Stage constructors:

| Stage | Runs where | Notes |
|---|---|---|
| `pepper.Pipe(cap)` | worker | `.WithGroup("asr")`, `.WithVersion("1.2.0")` |
| `pepper.Transform(fn)` | router | reshape the payload between stages |
| `pepper.PipeTransformWithEnv(fn)` | router | same, plus `EnvelopeInfo` (session id, meta, corr id) |
| `pepper.PipeReturn(value)` | router | short-circuit and return a fixed value |
| `pepper.PipeScatter(cap)` | worker | ⚠️ stage type exists, but the router does not gather results yet |

**Async execution with stage events** — requires `pepper.WithTracking(true)`:

```go
pp, err := pepper.New(
    pepper.WithWorkers(pepper.NewWorker("w-1")),
    pepper.WithTracking(true),        // needed for Execute / Process
)

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

`Process[Out]` also exposes `ID()`, `OriginID()`, `State()` (a `Checkpoint`) and
`Cancel()`.

---

## Blobs

Instead of base64-encoding megabytes into the envelope, write the bytes once and
pass a reference: `{_pepper_blob: true, id, path, size, dtype, shape, format}`.

```go
// from raw bytes
blob, err := pp.NewBlob(imageBytes)
if err != nil {
    log.Fatal(err)
}
defer blob.Close()

// from a file already on disk
frame, err := pp.NewBlobFromFile("/tmp/frame.jpg")
if err != nil {
    log.Fatal(err)
}
defer frame.Close()

// the reference is just another input value
res, err := pp.Do(ctx, "face.embed", pepper.In{"image": frame.Ref()})
```

In Python:

```python
from cap import read_blob

def run(inputs):
    pixels = read_blob(inputs["image"]).as_numpy()   # np.memmap — no copy
    frame  = read_blob(inputs["image"]).as_cv2()     # decoded BGR image
    tensor = read_blob(inputs["image"]).as_torch()   # torch tensor
    raw    = read_blob(inputs["image"]).as_bytes()   # one copy
    return {"shape": list(pixels.shape)}
```

Returning a blob from a worker works too — `pepper.new_blob(payload, dtype="float32",
shape=[1, 768])` — and `Result.AsBlob()` picks it back up on the Go side.

**Where the "zero-copy" actually applies** — worth knowing before you design
around it:

- The blob file lives in `/dev/shm/pepper` on Linux when `/dev/shm` exists, and in
  a per-user temp/cache directory elsewhere (`pepper.WithBlobDir(dir)` overrides
  it). On macOS and Windows it is a regular file, so expect page-cache, not
  shared memory.
- `as_numpy()` uses `np.memmap` **only when `dtype` and `shape` are set** on the
  reference; otherwise it reads the file into a buffer (one copy). Set them where
  the blob is created — `pepper.new_blob(payload, dtype="float32", shape=[1, 768])`
  in Python — for true zero-copy.
- `Close()` deletes the file immediately. If you hold the reference across an
  async boundary, use `blob.KeepAlive(deadlineMs)` and let the orphan scanner
  (`DefaultBlobOrphanTTL`, 24h) clean up stragglers.
- Multi-node: the reference is only a path, so it works across nodes that share a
  filesystem. Object-store transfer (`blob.S3Backend`) exists in the tree but
  `pepper.WithBlobStore` is not wired up yet — do not rely on it.

---

## Sessions

A session is a key-value bag carried alongside every call in the envelope,
persisted by a pluggable store.

```go
sess := pp.NewSession()                 // ULID-backed id

result, err := pepper.Call[ReplyOut]{
    Cap:   "chat.reply",
    Input: pepper.In{"message": "Hello"},
}.Session(sess).Do(ctx)

// persist sess.ID() in a cookie / JWT, then resume from it later:
sess2 := pp.Session(idFromCookie)

sess.Set("last_topic", "billing")       // direct access: Get, GetAll, Clear, Exists, Touch
```

From Python, `pepper.session().get("last_topic")` / `.set(...)`; writes come back to
the store automatically after the call. Inside a pipeline transform,
`pepper.SessionFromContext(ctx)` gives you the session id, and
`pepper.PipeTransformWithEnv` exposes it as `EnvelopeInfo.SessionID`.

Storage backends:

| Backend | How |
|---|---|
| in-memory, TTL-expired | default; or `pepper.WithSession(pepper.InMemoryStore(24*time.Hour))` |
| coord-backed (shared by the cluster) | automatic once `pepper.WithCoord(...)` is set |
| custom | implement `pepper.Storage` and pass it to `pepper.WithSession` |

---

## Resilience and observability

- **Poison pill detection.** A payload that kills its worker
  `WithPoisonPillThreshold(n)` times (default 2) gets its `origin_id`
  blacklisted for `WithPoisonPillTTL(d)`, is no longer retried, and is written to
  the DLQ.
- **Dead letter queue.** `pepper.WithDLQ(pepper.FileDLQ("./dlq"))` persists poison
  entries as files for inspection and replay; `pepper.NopDLQ()` discards them.
- **Deadline propagation.** The `context` deadline becomes `DeadlineMs` on the
  envelope. When it passes, the router drops the pending request and broadcasts a
  cancel so workers can stop early (`pepper.is_cancelled()` in Python).
- **Retries.** Failed calls retry up to `WithMaxRetries(n)` (default 2) before the
  error surfaces.
- **Worker recycling.** `pepper.NewWorker("w-1").MaxRequests(10_000).MaxUptime(24*time.Hour)`
  drains and respawns a worker once it has served enough requests or lived long
  enough — the standard fix for slow memory growth in ML workers.
- **Load reporting.** Workers send heartbeats with their in-flight load and
  groups; the router uses them for readiness (`WorkerReady`, `WaitWorkerReady`)
  and liveness.
- **Metrics.** `pepper.WithMetrics(pepper.Prometheus())` emits counters and
  histograms; `pepper.WithMetrics(pepper.Noop())` keeps the plumbing with zero
  cost.
- **Wire inspector.** `cmd/pepper` ships an inspector client (`pepper inspect <socket>`
  with `--filter`/`--format json`) and `inspect.New(path)` is a working read-only
  event tap — but the runtime does not feed it yet and `pepper.WithInspector` is
  not wired, so treat it as a preview of the debugging workflow rather than a
  feature you can turn on today.

- **Hooks.** Middleware around every invocation, globally, per group, or per
  capability:

  ```go
  pp.Hooks().Global().Before("cache", func(ctx context.Context, env *pepper.Envelope, in pepper.In) (pepper.In, error) {
      if hit, ok := cache.Get(in["prompt"]); ok {
          return nil, pepper.ShortCircuit(pepper.CachedResult(hit))   // skips the call
      }
      return in, nil
  }).After("audit", func(ctx context.Context, env *pepper.Envelope, in pepper.In, res pepper.HookResult) (pepper.HookResult, error) {
      audit.Record(env.Cap, res.WorkerID)
      return res, nil
  })
  ```

- **Circuit breaker.** Per-worker failure counting in the router
  (5 consecutive failures, 30s open). Workers that consistently fail are
  temporarily skipped for routing until the circuit closes.

- **Worker authentication.** Ed25519 challenge-response, opt-in via
  `pepper.WithAuthKeys(pubKey)`. Workers present their public key in the Hello
  message, then sign a router-issued challenge. The private key is passed via
  the `PEPPER_AUTH_KEY` environment variable (hex-encoded). Supported in Python,
  Rust, and Go workers.

- **Rate limiting.** Per-capability token bucket, opt-in via
  `pepper.WithRateLimit(true, 100, 50)`. Each capability gets its own limiter;
  requests that exceed the rate receive an `ErrRateLimited` error. Uses
  `jack.RateLimiter` internally.

- **Health data.** `pp.Health()` returns a `HealthStatus` struct with worker
  counts, ready/dead states, open circuit breakers, in-flight requests, coord
  connectivity, and stream drop counts. Pepper is a library — users expose the
  data via HTTP, gRPC, or any other mechanism.

---

## Clustering

Two things are shared across nodes, and they are configured separately:

1. **Coord** — registry replication, session storage, and cross-node pub/sub and
   work distribution. Set with `pepper.WithCoord(...)`; also pass the same URL to
   `pepper.WithTransportURL(...)` so Python/Node/Julia workers know where to
   connect. Pepper refuses to start in distributed mode without it.
2. **Bus / transport** — implied by the coord choice; you do not select it
   directly. With no `WithCoord`, Pepper runs its own Mula TCP bus on
   `tcp://127.0.0.1:<free port>`, which is all a single node needs.

| Coord | Constructor | URL | Notes |
|---|---|---|---|
| Mula (single node) | automatic | `tcp://127.0.0.1:<port>` | built in, no external service, Go↔worker IPC |
| Mula cluster | `pepper.MulaCluster(bind, gossipPort, rpcPort, seeds)` | — | memberlist gossip for peer discovery; `pepper.Mula(url)` shares one bus between local instances |
| Redis | `pepper.Redis(url)` | `redis://host:6379` | needs a running Redis |
| NATS | `pepper.NATS(url)` | `nats://host:4222` | needs JetStream (`nats-server -js`) |

**Single node** — nothing to configure:

```go
pp, err := pepper.New(pepper.WithWorkers(pepper.NewWorker("w-1")))
```

**Multi-node with NATS** — node A owns the workers, node B only routes:

```go
// node A — owns the workers
cA, err := pepper.NATS("nats://127.0.0.1:4222")
if err != nil {
    log.Fatal(err)
}
ppA, err := pepper.New(
    pepper.WithCoord(cA),
    pepper.WithTransportURL("nats://127.0.0.1:4222"),
    pepper.WithWorkers(pepper.NewWorker("w-a").Groups("default")),
)

// node B — no workers, but sees the capabilities registered on node A
cB, err := pepper.NATS("nats://127.0.0.1:4222")
ppB, err := pepper.New(
    pepper.WithCoord(cB),
    pepper.WithTransportURL("nats://127.0.0.1:4222"),
)
```

Redis is the same with `pepper.Redis("redis://127.0.0.1:6379")`. Sessions become
coord-backed automatically once `WithCoord` is set, so a session created on node B
continues to work when the call lands on node A.

Blobs are passed as file paths, so treat them as node-local unless the nodes share
a filesystem — see [Blobs](#blobs).

---

## LLM tool calling

Every registered capability has a JSON schema, so exposing your backend to an
agent is a map plus a formatter:

```go
schemas := pp.Capabilities(ctx, pepper.FilterByGroup("tools"))

openAITools := pepper.Tools(schemas, pepper.FormatOpenAI)      // []pepper.OpenAITool
anthropicTools := pepper.Tools(schemas, pepper.FormatAnthropic) // []pepper.AnthropicTool

// provider-agnostic, includes input and output schemas
generic := pepper.Tools(schemas, pepper.FormatGeneric)

// or anything your SDK wants
mine := pepper.Tools(schemas, func(s pepper.Schema) MyTool {
    return MyTool{Name: s.Name, Params: s.InputSchema}
})
```

Tools returned by `pepper.Tools` are plain structs — hand them to your SDK and
execute the returned tool calls with `pepper.Call`. The model itself can also be a
capability, which keeps the whole agent loop inside the runtime:

```go
if err := pp.Register(pepper.HTTP("llm.chat", "https://api.openai.com").
    With(pepper.OpenAI).
    Auth(pepper.BearerToken(os.Getenv("OPENAI_API_KEY")))); err != nil {
    log.Fatal(err)
}
```

`FilterByGroup` and `FilterByRuntime` compose to scope what the model can see.

---

## Configuration

Every option is a functional option on `pepper.New(...)`.

| Option | Default | Notes |
|---|---|---|
| `WithWorkers(...)` | none | accepts `*WorkerBuilder` or `WorkerConfig` |
| `WithCodec(pepper.CodecMsgPack)` | `msgpack` | `pepper.CodecJSON` also supported |
| `WithTransportURL(url)` | Mula on `tcp://127.0.0.1:<free port>` | coord URL when clustering |
| `WithCoord(c)` | nil (single node) | `pepper.Redis`, `pepper.NATS`, `pepper.Mula`, `pepper.MulaCluster` |
| `WithSession(store)` | in-memory, 24h TTL | coord-backed automatically when clustered |
| `WithDLQ(backend)` | nil (discard) | `pepper.FileDLQ(dir)`, `pepper.NopDLQ()` |
| `WithMetrics(sink)` | nil | `pepper.Prometheus()`, `pepper.Noop()` |
| `WithTracking(bool)` | `false` | required for `Execute`/`Process[Out]` |
| `WithMaxConcurrent(n)` | 8 | per worker |
| `WithDefaultTimeout(d)` | 30s | per request |
| `WithBootTimeout(d)` | 0 → default timeout | max wait for the first ready worker |
| `WithHeartbeatInterval(d)` | 5s | liveness; 3× is the timeout |
| `WithShutdownTimeout(d)` | 30s | drain time in `Stop()` |
| `WithMaxRetries(n)` | 2 | per call |
| `WithMaxHops(n)` | 10 | pipeline / forwarding hops |
| `WithMaxCbDepth(n)` | 5 | nested worker callbacks |
| `WithPoisonPillThreshold(n)` | 2 | worker crashes before blacklisting |
| `WithPoisonPillTTL(d)` | 1h | blacklist duration |
| `WithBlobDir(dir)` | `/dev/shm/pepper` (Linux) else temp | blob storage location |
| `WithLogger(l)` | `ll.New("pepper")` | structured logger |
| `pepper.Resource(name, cfg)` | none | config handed to Python `@resource` |
| `WithAuthKeys(keys ...)` | none | Ed25519 challenge-response auth, opt-in |
| `WithRateLimit(enabled, rate, burst)` | disabled | per-capability token bucket rate limiting |
| `WithTransport`, `WithTLS`, `WithStrategy`, `WithStreamCredits`, `WithMemoryBudget`, `WithInspector`, `WithBlobStore`, `WithDebug` | — | declared, not wired yet |

Call-level options: `WithCallGroup`, `WithCallWorker`, `WithCallDispatch`,
`WithCallQuorum`, `WithCallVersion`, `WithCallSession`, `WithCallMaxHops`,
`WithCallMaxCbDepth`, `WithCallDeadline`, `WithCallTimeout`, `WithMeta`.
Capability-level options: `Groups`, `Version`, `Config`, `Timeout`.

---

## Performance

The repo ships benchmarks so you can measure overhead on your own hardware:

| Benchmark | Measures |
|---|---|
| `BenchmarkNomic_PepperDispatchOnly` | router + bus round-trip, no-op Go capability |
| `BenchmarkNomic_PepperHTTPAdapter` | the same path with an HTTP worker on the far side |
| `BenchmarkNomic_DirectHTTP` | the identical request without Pepper (baseline) |
| `BenchmarkMulaPublish` | raw bus publish throughput |
| `BenchmarkRouter_*` | router dispatch, heartbeat, circuit breaker |
| `BenchmarkCoordMemory_*` | in-memory coord KV, pub/sub, push/pull |
| `BenchmarkCoordRedis_*` | Redis coord KV, pub/sub, push/pull |
| `BenchmarkCoordNATS_*` | NATS coord KV, pub/sub, push/pull |
| `BenchmarkPending_*` | pending map register/resolve, streaming chunks |
| `BenchmarkCodec_*` | msgpack vs JSON marshal/unmarshal |

```bash
make go-real-nomic-bench                                  # needs python3 + msgpack
go test -bench=BenchmarkMulaPublish -run='^$' ./internal/bus
go test -bench=. -run='^$' ./tests/                      # full benchmark suite
```

The `DirectHTTP` baseline in `tests/nomic_test.go` is the one to watch: it tells
you what the envelope costs relative to doing the same HTTP call by hand.

The benchmark suite in `tests/benchmark_bench_test.go` covers all performance
hotspots: router, coordination backends (memory, Redis, NATS), pending map,
codec, bus, and blob manager. Use it to validate changes don't regress.

The wire protocol is specified in [`_spec/pepper-spec.md`](_spec/pepper-spec.md)
and [`_spec/pepper-spec-2.md`](_spec/pepper-spec-2.md) — envelope format, worker
lifecycle, dispatch modes, worker mesh, blob semantics.

## Development

```bash
make go-test              # go test -race ./...
make go-test-unit         # skip anything needing Redis, NATS or Python
make python-test          # pytest over runtime/python (creates .venv)
(cd runtime/rust && cargo test)   # Rust runtime protocol tests
make check-services       # are Redis / NATS reachable?
make go-real-nomic        # end-to-end against a real Python worker
make go-real-cluster-redis
make go-real-cluster-nats
```

CI (`.github/workflows/go.yml`) runs `go build`, `go vet`, `go test -race` and
`golangci-lint` on Linux, macOS and Windows with Go 1.25.

---

## License

MIT License. See [`LICENSE`](LICENSE) for more information.