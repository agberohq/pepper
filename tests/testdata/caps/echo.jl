# pepper:name = echo
# pepper:groups = default
# pepper:version = 1.0.0

function run(inputs::Dict)::Dict
    return merge(inputs, Dict("echoed" => true))
end
