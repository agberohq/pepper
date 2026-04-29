# pepper:name    = echo
# pepper:version = 1.0.0
# pepper:groups  = default

def run(inputs):
    return {"msg": inputs.get("msg", "")}
