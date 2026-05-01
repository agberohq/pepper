# pepper:name    = nomic.embed.direct
# pepper:version = 1.0.0
# pepper:groups  = default
# pepper:deps    = nomic>=3.0.0

"""Direct Nomic embedding capability.

Model is loaded exactly once in setup() by calling embed.text() with
inference_mode="local". The nomic library caches the underlying
SentenceTransformer/MLX model in a module-level singleton keyed by model
name, so every subsequent embed.text() call in run() hits that cache
instead of re-loading weights from disk.

We store a sentinel in pepper.kv() so setup() is idempotent across any
future worker restarts within the same process.
"""

from runtime import pepper


def setup(config: dict) -> None:
    config = config or {}
    from nomic import embed
    import time
    import sys

    model_name = config.get("model", "nomic-embed-text-v1.5")
    kv_key = f"nomic-ready-{model_name}"

    def _load():
        t0 = time.monotonic()
        # This call forces nomic to build and cache its internal _EmbeddingModel
        # singleton for this model name. All subsequent embed.text() calls in
        # run() will reuse that cached object — no weight reload.
        embed.text(
            texts=["warmup"],
            model=model_name,
            inference_mode="local",
        )
        load_s = round(time.monotonic() - t0, 3)
        print(
            f"[nomic.embed.direct] model loaded: {model_name} in {load_s}s",
            file=sys.stderr,
            flush=True,
        )
        return {"ready": True, "model": model_name, "load_s": load_s}

    entry = pepper.kv("models").get_or_set(kv_key, _load)
    print(f"[nomic.embed.direct] setup complete: {entry}", file=sys.stderr, flush=True)


def run(inputs: dict) -> dict:
    from nomic import embed
    import time

    text = inputs.get("text", "")
    if not text:
        return {"error": "no text provided"}

    model_name = (pepper.config() or {}).get("model", "nomic-embed-text-v1.5")

    # embed.text() reuses the module-level model cache primed in setup().
    # inference_mode="local" is required so it doesn't attempt a remote API call.
    t0 = time.monotonic()
    output = embed.text(
        texts=[text],
        model=model_name,
        inference_mode="local",
    )
    latency = time.monotonic() - t0

    embedding = output["embeddings"][0]

    return {
        "embedding": embedding,
        "dimensions": len(embedding),
        "latency_s": round(latency, 4),
        "provider": "nomic-local",
    }
