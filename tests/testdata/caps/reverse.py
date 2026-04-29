# pepper:name    = reverse
# pepper:version = 1.0.0
# pepper:groups  = default

def run(inputs):
    text = inputs.get("text", "")
    return {"text": text[::-1]}
