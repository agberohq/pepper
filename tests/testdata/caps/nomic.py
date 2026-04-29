# pepper:name    = nomic.embed.direct
# pepper:version = 1.0.0
# pepper:groups  = default
# pepper:deps    = nomic>=3.0.0

"""Direct Nomic embedding capability."""

from runtime import pepper


def setup(config: dict) -> None:
    from nomic import embed

    model_name = config.get("model", "nomic-embed-text-v1.5")

    def _warmup():
        import time
        t0 = time.monotonic()
        embed.text(texts=["hello"], model=model_name, inference_mode="local")
        return {"model": model_name, "load_s": time.monotonic() - t0}

    entry = pepper.kv("models").get_or_set(f"nomic-{model_name}", _warmup)

    # Log model cache info for the benchmark
    import sys
    print(f"nomic model cached: {entry}", file=sys.stderr, flush=True)


def run(inputs: dict) -> dict:
    from nomic import embed
    import time

    text = inputs.get("text", "")
    if not text:
        return {"error": "no text provided"}

    t0 = time.monotonic()
    output = embed.text(
        texts=[text],
        model=pepper.config().get("model", "nomic-embed-text-v1.5"),
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