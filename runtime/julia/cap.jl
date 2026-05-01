# cap.jl — Pepper Julia capability contract helpers.
#
# Covers:
#   - Form A header parser  (mirrors parse_form_a in cap.py)
#   - @capability macro     (Form B decorator equivalent)
#   - @resource macro       (shared connection pool lifecycle)
#   - read_blob()           (zero-copy blob access)
#   - Resource lifecycle    (close_resources!)

module Cap

export parse_form_a, @capability, @resource, get_resource, close_resources!,
       read_blob, BlobRef

# ---------------------------------------------------------------------------
# Form A header parser  (spec §10.1)
# ---------------------------------------------------------------------------
# Reads contiguous # pepper:key = value lines from the top of a .jl source.
# Stops at the first non-comment, non-blank line.

const HEADER_RE = r"^#\s*pepper:(\w+)\s*=\s*(.+)$"

"""
    parse_form_a(source::String) -> Dict{String,String}

Parse Form A magic comment header from a Julia source string.
"""
function parse_form_a(source::String)::Dict{String,String}
    meta = Dict{String,String}()
    for line in split(source, '\n')
        stripped = strip(line)
        isempty(stripped) && continue
        startswith(stripped, "#!") && continue          # shebang
        m = match(HEADER_RE, stripped)
        if m !== nothing
            meta[m[1]] = strip(m[2])
        elseif startswith(stripped, "#")
            continue                                    # plain comment — skip
        else
            break                                       # code started — stop
        end
    end
    return meta
end

# ---------------------------------------------------------------------------
# Form B — @capability macro
# ---------------------------------------------------------------------------
# Usage:
#   @capability name="embed.text" groups=["gpu"] max_concurrent=2
#   struct MyEmbedder
#       model
#   end
#   function setup!(cap::MyEmbedder, config::Dict) ... end
#   function run(cap::MyEmbedder, inputs::Dict)::Dict ... end

"""
Attach Pepper capability metadata to a struct type as type-level properties.

    @capability name="cap.name" [groups=["default"]] [max_concurrent=4]
    struct MyCap ... end
"""
macro capability(kwargs...)
    # Collect keyword arguments into a metadata dict that is stored as a
    # const alongside the type. The actual attachment happens at load_capability
    # time by checking for a PEPPER_CAP_META constant in the module.
    meta = Dict{Symbol,Any}()
    for kw in kwargs
        kw isa Expr && kw.head === :(=) || continue
        key = kw.args[1]
        val = kw.args[2]
        meta[key] = val
    end
    # Return an expression that sets a module-level constant __pepper_cap_meta__
    # immediately after the struct is defined. Capabilities authors use this macro
    # before their struct definition; the runtime reads __pepper_cap_meta__.
    quote
        const __pepper_cap_meta__ = $(esc(meta))
    end
end

# ---------------------------------------------------------------------------
# @resource macro
# ---------------------------------------------------------------------------
# Usage:
#   @resource name="db_pool"
#   function make_db_pool(config::Dict)
#       pool = DBPool(config["url"])
#       return pool          # yield equivalent — resource is stored here
#       # teardown code would go after yield in Python; use finalizer in Julia
#   end

const _resources = Dict{String,Any}()
const _resource_factories = Dict{String,Function}()
const _resources_lock = ReentrantLock()

macro resource(kwargs...)
    name_expr = nothing
    for kw in kwargs
        kw isa Expr && kw.head === :(=) && kw.args[1] === :name || continue
        name_expr = kw.args[2]
    end
    name_expr === nothing && error("@resource requires name= keyword")
    quote
        function _register_resource(fn::Function)
            rname = $(esc(name_expr))
            lock(_resources_lock) do
                _resource_factories[rname] = fn
            end
            fn
        end
        _register_resource
    end
end

"""Return the shared resource registered under `name`, or `nothing`."""
function get_resource(name::String)
    lock(_resources_lock) do
        get(_resources, name, nothing)
    end
end

"""
Initialise all registered resources by calling their factory functions.
Called once per worker at startup, before any capability `setup!` calls.
"""
function init_resources!(config::Dict)
    lock(_resources_lock) do
        for (name, factory) in _resource_factories
            try
                _resources[name] = factory(config)
            catch e
                @error "resource init failed" name=name exception=e
            end
        end
    end
end

"""Call cleanup for all resources. Invoked on worker shutdown."""
function close_resources!()
    lock(_resources_lock) do
        # Julia does not have Python's generator-based teardown; instead
        # capabilities should register finalizers on their resource objects.
        # We call a teardown() method if present.
        for (name, res) in _resources
            if res !== nothing && hasmethod(teardown, (typeof(res),))
                try teardown(res) catch e
                    @warn "resource teardown failed" name=name exception=e
                end
            end
        end
        empty!(_resources)
    end
end

# ---------------------------------------------------------------------------
# Blob access  (spec §12.4)
# ---------------------------------------------------------------------------

"""
Zero-copy access to a Pepper blob. Workers receive a blob reference Dict
with `_pepper_blob => true` and call `read_blob(ref)` to get a `BlobRef`.
"""
struct BlobRef
    ref::Dict{String,Any}
    path::String
    size::Int
    dtype::String
    shape::Vector{Int}
    format::String
end

function BlobRef(ref::Dict)
    BlobRef(
        ref,
        get(ref, "path", ""),
        get(ref, "size", 0),
        get(ref, "dtype", ""),
        Int[get(ref, "shape", Int[])...],
        get(ref, "format", ""),
    )
end

"""
    read_blob(ref::Dict) -> BlobRef

Convert a blob reference dict (with `_pepper_blob => true`) to a `BlobRef`.

    img = read_blob(inputs["image"])
    data = as_bytes(img)          # one copy
    arr  = as_array(img)          # mmap — zero copy when dtype/shape are set
"""
function read_blob(ref::Dict)::BlobRef
    get(ref, "_pepper_blob", false) || error("read_blob: argument is not a pepper blob reference")
    BlobRef(ref)
end

"""Read blob as raw bytes."""
function as_bytes(b::BlobRef)::Vector{UInt8}
    read(b.path)
end

"""
Return the blob as a Julia Array, memory-mapped when dtype and shape are known.
Falls back to a plain byte vector when metadata is missing.
"""
function as_array(b::BlobRef)
    if !isempty(b.dtype) && !isempty(b.shape)
        T = _julia_dtype(b.dtype)
        return Mmap.mmap(b.path, Array{T, length(b.shape)}, tuple(b.shape...))
    end
    return as_bytes(b)
end

function _julia_dtype(dtype::String)
    d = Dict(
        "float32" => Float32, "float64" => Float64,
        "int8"    => Int8,    "int16"   => Int16,
        "int32"   => Int32,   "int64"   => Int64,
        "uint8"   => UInt8,   "uint16"  => UInt16,
        "uint32"  => UInt32,  "uint64"  => UInt64,
    )
    get(d, dtype, UInt8)
end

end # module Cap
