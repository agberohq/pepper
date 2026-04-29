# pepper:name    = nomic.embed
# pepper:version = 1.0.0
# pepper:groups  = default
# pepper:deps    = nomic>=3.0.0

"""
Nomic Embed Text capability for Pepper.

Requires:
    pip install nomic

The first call loads the model (may take a few seconds).
Subsequent calls reuse the cached model via pepper.kv().
"""

from runtime import pepper


def setup(config: dict) -> None:
    """Load the Nomic embedding model once per worker."""
    from nomic import embed

    model_name = config.get("model", "nomic-embed-text-v1.5")

    def _load():
        import time
        t0 = time.monotonic()
        # We don't actually load here — nomic embeds on first call.
        # But we warm up by doing a tiny embedding.
        _ = embed.text(
            texts=["warmup"],
            model=model_name,
            inference_mode="local",
        )
        return {
            "loaded": True,
            "model": model_name,
        }

    pepper.kv("models").get_or_set(f"nomic-{model_name}", _load)


def run(inputs: dict) -> dict:
    """Embed one or more texts using Nomic."""
    from nomic import embed

    texts = inputs.get("texts")
    if texts is None:
        # Single text fallback
        text = inputs.get("text", "")
        if not text:
            return {"error": "no 'texts' or 'text' provided"}
        texts = [text]

    if isinstance(texts, str):
        texts = [texts]

    model_name = pepper.config().get("model", "nomic-embed-text-v1.5")

    # Load from cache (or load fresh if first call)
    _ = pepper.kv("models").get(f"nomic-{model_name}")

    import time
    t0 = time.monotonic()

    output = embed.text(
        texts=texts,
        model=model_name,
        inference_mode="local",
    )

    latency = time.monotonic() - t0

    return {
        "embeddings": output["embeddings"],
        "count": len(texts),
        "dimensions": len(output["embeddings"][0]) if output["embeddings"] else 0,
        "latency_s": round(latency, 3),
    }