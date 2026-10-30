# Pepper runtime for Rust

Rust capabilities are `.rs` files. Pepper compiles them on first use (cached by
content hash), then serves them from a worker process that speaks the same envelope
protocol as the Python, Julia and Node runtimes.

One worker hosts **many** Rust capabilities, the same way a Python worker hosts
several `.py` files — register two files in the same group and they share a process,
a model cache and a heartbeat.

```go
pp.Register(pepper.Script("rerank.cross", "./caps/rerank.rs").Groups("gpu"))
pp.Register(pepper.Script("embed.text",   "./caps/embed.rs").Groups("gpu"))
```

```rust
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

    fn run(&mut self, ctx: &Context, input: Input) -> Result<Value> {
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
    max_concurrent: 1,
    handler: Rerank,
}
```

## Requirements

- Rust toolchain with cargo (`rustup.rs`). The crate builds on stable; no nightly
  features are used.
- Crates used by the runtime: `serde`, `serde_bytes`, `serde_json`, `rmp-serde`,
  `memmap2`. The first compile fetches/builds them once into a shared cargo
  target directory, so later capabilities start in well under a second.

## How a `.rs` file becomes a worker

A worker owns a **set** of capabilities, exactly like a Python worker owning
several `.py` files. You register files; pepper figures out the worker:

```go
pp.Register(pepper.Script("rerank.cross", "./caps/rerank.rs").Groups("gpu"))
pp.Register(pepper.Script("embed.text",   "./caps/embed.rs").Groups("gpu"))
// both served by the single worker in the "gpu" group
```

The pipeline:

1. `Start()` **compiles** every Rust capability set whose worker groups match,
   before the boot deadline begins. Compilation is registration's job, not
   startup's: it has its own budget (15 min) and its own error reporting, so a
   slow build can never be mistaken for a worker that never came up.
2. Compilation writes a crate into `$PEPPER_CACHE_DIR/rust/<hash>/`:
   `src/cap_0_rerank.rs` (copy of your file), `src/cap_1_embed.rs`, and a
   `main.rs` that declares one module per capability and calls
   `serve(vec![cap_0_rerank::__pepper_capability(), ...])`. Each capability lives
   in its own module, so two files may both define `struct Handler`.
3. Crates build against a shared `CARGO_TARGET_DIR`: dependencies compile once for
   every capability on the machine.
4. The worker binary is `exec()`ed, so the PID pepper supervises is the one
   serving requests — logging, respawn and heartbeats behave like any other worker.

A build failure fails `Start()` with the compiler output and the offending file:

```
pepper: start: pepper: rust worker w-1: exit status 1
error: expected one of `!`, `.`, `::`, `;`, `?`, `{`, `}`, or an operator, found `does`
 --> src/cap_0_rerank.rs:8:14
```

Cache key = every source + runtime crate version + mtime of `pepper-worker`, so
editing one capability rebuilds that worker's set, and upgrading the runtime
rebuilds everything.

## Declaring dependencies

Add a `// pepper:deps` header to the capability — the same convention as the
Python, Julia and Node runtimes — and the crates are added to the generated
manifest automatically:

```rust
// pepper:deps = candle-core@0.8, tokenizers@0.19, serde_json

use pepper_runtime::prelude::*;
```

Declaring a crate the runtime already depends on (`serde_json` above) is fine: your
version wins instead of producing a duplicate-key manifest. Malformed entries are
reported on stderr and skipped rather than breaking the build.

## Concurrency and streaming

`run` takes `&self`, so **one handler instance serves many requests in parallel** —
you pay for one model load, not one per thread. Mutable state goes behind
`Mutex`/`RwLock`/atomics inside your struct, which is the same trade-off any Rust
service makes. `PEPPER_MAX_CONCURRENT` is a real limit here: requests queue on a
semaphore before entering the handler.

```rust
// go side, 4 x 250ms: measured 251ms, not 1s (tests/rust_parity_test.go)
pepper.All[Out](ctx, pp, call, call, call, call)
```

Server streaming is supported via `run_stream`. Each emitted value becomes a
`res_chunk` envelope, and the runtime closes with `res_end`:

```rust
impl Capability for Tokens {
    fn run(&self, _ctx: &Context, input: Input) -> Result<Value> {
        Ok(json!({ "text": input.str_or("prompt", "") }))
    }

    fn run_stream(
        &self,
        _ctx: &Context,
        input: Input,
        emit: &mut dyn FnMut(Value) -> Result<()>,
    ) -> Result<()> {
        for (i, word) in input.str_or("prompt", "").split_whitespace().enumerate() {
            emit(json!({ "index": i, "token": word }))?;
        }
        Ok(())
    }
}
```

For bidirectional streams the input side is available inside the handler:
`ctx.next_chunk()` blocks until the caller sends a chunk, returning `None` once
they close the stream (`ctx.is_stream()` tells you which mode you are in).

## Deployment without a toolchain

Compilation happens once. After that the cached binary is executed directly, so a
multi-stage Docker build (or an air-gapped host) needs no Rust at runtime:

```dockerfile
FROM rust:1 AS build
WORKDIR /src
COPY runtime/rust ./runtime/rust
RUN cargo build --release --manifest-path runtime/rust/Cargo.toml
RUN runtime/rust/target/release/pepper-worker --prepare       --cap caps/rerank.rs --cap caps/embed.rs      # fills $PEPPER_CACHE_DIR

FROM gcr.io/distroless/cc
COPY --from=build /src/runtime/rust/target/release/pepper-worker /usr/local/bin/
COPY --from=build /src/pepper-cache /var/cache/pepper
COPY caps /caps
ENV PEPPER_RUST_WORKER_BIN=/usr/local/bin/pepper-worker     PEPPER_CACHE_DIR=/var/cache/pepper
```

`TestRustPrebuiltCacheRunsWithoutCargo` asserts the contract: with `CARGO`
pointing at a nonexistent binary, a warm cache still starts and serves.

## Notes and limits

- **One capability per file** — inherent to Rust: a file is a compilation unit, and
  `capability!` declares one entry point. A worker serves as many files as you
  register for its groups.
- **`//!` doc comments** at the top of a cap file are rewritten to `//` because the
  file is `include!`d into a module.
- **`pepper.call()` callbacks** are not implemented yet; `worker_hello` reports
  `cb_supported: false` so the router never waits for messages we do not send.
- **Transport**: `tcp://` (Mula) only. Redis/NATS worker connections live in the
  Python runtime; extend `src/transport.rs` if you need them (the `// pepper:deps`
  header now makes adding an SDK crate a one-line change).
- **Blobs**: `input.blob("tensor")` gives you the file; `.map()` memory-maps it and
  `.as_f32()` derefs to `&[f32]` for zero-copy tensors.
  `new_tensor(bytes, "float32", &shape)` returns a reference for the reply.

## Development

```bash
cd runtime/rust
cargo build --release      # the runtime binary
cargo test                 # protocol + blob unit tests
```

The Go side has an end-to-end test that compiles `caps/echo.rs` and calls it:
`PEPPER_CACHE_DIR=/tmp/pepper-cache PEEPER_HOME=$(pwd) go test -run TestRustCapabilityRoundTrip ./tests`.
