# pepper:name    = nomic.embed
# pepper:version = 1.0.0
# pepper:groups  = default
# pepper:deps    = nomic>=3.0.0

"""
Nomic Embed Text capability for Pepper.

Requires:
    pip install nomic

Model is loaded once in setup() via nomic's internal module-level cache.
All run() calls reuse that cached model — no weight reload per request.
Supports both single text ("text") and batch ("texts") inputs.
"""

from runtime import pepper


def setup(config: dict) -> None:
    """Prime nomic's internal model cache once per worker process."""
    from nomic import embed
    import time
    import sys

    model_name = config.get("model", "nomic-embed-text-v1.5")
    kv_key = f"nomic-ready-{model_name}"

    def _load():
        t0 = time.monotonic()
        # Forces nomic to build its _EmbeddingModel singleton for this model.
        # Subsequent embed.text() calls hit that singleton — no re-load.
        embed.text(
            texts=["warmup"],
            model=model_name,
            inference_mode="local",
        )
        load_s = round(time.monotonic() - t0, 3)
        print(
            f"[nomic.embed] model loaded: {model_name} in {load_s}s",
            file=sys.stderr,
            flush=True,
        )
        return {"ready": True, "model": model_name, "load_s": load_s}

    entry = pepper.kv("models").get_or_set(kv_key, _load)
    print(f"[nomic.embed] setup complete: {entry}", file=sys.stderr, flush=True)


def run(inputs: dict) -> dict:
    """Embed one or more texts using the cached Nomic model."""
    from nomic import embed
    import time

    # Accept either "texts" (batch) or "text" (single)
    texts = inputs.get("texts")
    if texts is None:
        text = inputs.get("text", "")
        if not text:
            return {"error": "no 'texts' or 'text' provided"}
        texts = [text]

    if isinstance(texts, str):
        texts = [texts]

    model_name = pepper.config().get("model", "nomic-embed-text-v1.5")

    t0 = time.monotonic()
    output = embed.text(
        texts=texts,
        model=model_name,
        inference_mode="local",
    )
    latency = time.monotonic() - t0

    embeddings = output["embeddings"]

    return {
        "embeddings": embeddings,
        "count": len(embeddings),
        "dimensions": len(embeddings[0]) if embeddings else 0,
        "latency_s": round(latency, 3),
    }
