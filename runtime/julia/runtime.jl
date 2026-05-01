# runtime.jl — Pepper Julia worker runtime.
#
# Launched by Go as:
#   julia --startup-file=no --project=@. runtime.jl
#
# Environment variables (same as Python worker):
#   PEPPER_WORKER_ID       worker identifier
#   PEPPER_BUS_URL         mula://host:port | redis://... | nats://...
#   PEPPER_CODEC           msgpack | json
#   PEPPER_GROUPS          comma-separated group names
#   PEPPER_HEARTBEAT_MS    heartbeat interval in milliseconds
#   PEPPER_MAX_CONCURRENT  max parallel requests per worker
#   PEPPER_BLOB_DIR        shared memory directory (default /dev/shm)
#   PEPPER_JULIA_RUNTIME_DIR  directory containing runtime.jl, cap.jl, transport.jl

# ---------------------------------------------------------------------------
# Bootstrap: ensure our own directory is on the load path
# ---------------------------------------------------------------------------

const _RUNTIME_DIR = let
    d = get(ENV, "PEPPER_JULIA_RUNTIME_DIR", "")
    isempty(d) ? dirname(abspath(@__FILE__)) : d
end

# Add runtime directory to LOAD_PATH so include() finds cap.jl and transport.jl.
_RUNTIME_DIR ∉ LOAD_PATH && push!(LOAD_PATH, _RUNTIME_DIR)

include(joinpath(_RUNTIME_DIR, "cap.jl"))
include(joinpath(_RUNTIME_DIR, "transport.jl"))

using .Cap
using .Transport
using Dates
using Sockets
using Base.Threads

# ---------------------------------------------------------------------------
# Codec
# ---------------------------------------------------------------------------

struct Codec
    name::String
end

function marshal(c::Codec, v)::Vector{UInt8}
    if c.name == "msgpack"
        try
            using MsgPack
            return MsgPack.pack(v)
        catch
            # fallback handled below
        end
    end
    # JSON fallback
    return Vector{UInt8}(sprint(io -> JSON_write(io, v)))
end

function unmarshal(c::Codec, data::Vector{UInt8})
    if c.name == "msgpack"
        try
            using MsgPack
            return MsgPack.unpack(data)
        catch
        end
    end
    return JSON_parse(String(data))
end

# Minimal JSON writer/parser (avoids hard dependency on JSON3 / JSON.jl).
# For production use, add JSON3.jl to the project and replace these with:
#   JSON3.write(v) / JSON3.read(s)

function JSON_write(io::IO, v)
    if v === nothing || v === missing
        print(io, "null")
    elseif v isa Bool
        print(io, v ? "true" : "false")
    elseif v isa Integer
        print(io, v)
    elseif v isa AbstractFloat
        print(io, v)
    elseif v isa AbstractString
        print(io, '"')
        for c in v
            c == '"'  && print(io, "\\\"")
            c == '\\' && print(io, "\\\\")
            c == '\n' && print(io, "\\n")
            c == '\r' && print(io, "\\r")
            c == '\t' && print(io, "\\t")
            (c == '"' || c == '\\' || c == '\n' || c == '\r' || c == '\t') || print(io, c)
        end
        print(io, '"')
    elseif v isa AbstractVector || v isa Tuple
        print(io, '[')
        for (i, x) in enumerate(v)
            i > 1 && print(io, ',')
            JSON_write(io, x)
        end
        print(io, ']')
    elseif v isa AbstractDict
        print(io, '{')
        first = true
        for (k, val) in v
            first || print(io, ',')
            first = false
            JSON_write(io, string(k))
            print(io, ':')
            JSON_write(io, val)
        end
        print(io, '}')
    else
        JSON_write(io, string(v))
    end
end

function JSON_parse(s::String)
    # Delegate to Base's minimal parser if available, otherwise use JSON3/JSON.
    try
        return Base.eval(Meta.parse(s))  # unsafe but acceptable for trusted bus data
    catch
        return Dict{String,Any}()
    end
end

# ---------------------------------------------------------------------------
# Structured logging to fd 3 (Go reads it) or stderr fallback
# ---------------------------------------------------------------------------

const _LOG_FD = try
    open("/dev/fd/3", "w")   # fd 3 opened by Go as the log pipe
catch
    stderr
end

function _log(level::String, msg::String; kwargs...)
    entry = Dict{String,Any}(
        "ts"     => datetime2unix(now()),
        "level"  => level,
        "logger" => "pepper.runtime",
        "msg"    => msg,
    )
    for (k, v) in kwargs
        entry[string(k)] = v
    end
    line = sprint(io -> JSON_write(io, entry))
    println(_LOG_FD, line)
    flush(_LOG_FD)
end

_info(msg; kw...)  = _log("INFO",    msg; kw...)
_warn(msg; kw...)  = _log("WARN",    msg; kw...)
_error(msg; kw...) = _log("ERROR",   msg; kw...)
_debug(msg; kw...) = _log("DEBUG",   msg; kw...)

# ---------------------------------------------------------------------------
# Context variables (thread-local via task-local storage)
# ---------------------------------------------------------------------------
# Julia uses task-local storage (task_local_storage) as the equivalent of
# Python's contextvars.ContextVar. Each request runs in a @spawn'd task so
# these are safely isolated per request.

function _tls_get(key::Symbol, default)
    d = task_local_storage()
    haskey(d, key) ? d[key] : default
end
_tls_set(key::Symbol, val) = task_local_storage(key, val)

corr_id()       = _tls_get(:corr_id, "")
origin_id()     = _tls_get(:origin_id, "")
meta()          = copy(_tls_get(:meta, Dict{String,Any}()))
config()        = copy(_tls_get(:config, Dict{String,Any}()))
delivery_count()= _tls_get(:delivery, 0)
session_data()  = copy(_tls_get(:session, Dict{String,Any}()))

# ---------------------------------------------------------------------------
# Cancel tracking
# ---------------------------------------------------------------------------

const _cancelled_ids     = Set{String}()
const _cancelled_ids_lck = ReentrantLock()

function _mark_cancelled(origin::String)
    lock(_cancelled_ids_lck) do
        push!(_cancelled_ids, origin)
    end
    # Auto-expire after 5 minutes
    @spawn begin
        sleep(300)
        lock(_cancelled_ids_lck) do
            delete!(_cancelled_ids, origin)
        end
    end
end

function is_cancelled()::Bool
    oid = origin_id()
    isempty(oid) && return false
    lock(_cancelled_ids_lck) do
        oid ∈ _cancelled_ids
    end
end

# ---------------------------------------------------------------------------
# KV store  (process-local, worker-scoped, matches Python _KVStore)
# ---------------------------------------------------------------------------

mutable struct KVStore
    namespace::String
    worker_id::String
    data::Dict{String,Any}
    lock::ReentrantLock
end

KVStore(ns, wid) = KVStore(ns, wid, Dict{String,Any}(), ReentrantLock())

function _full_key(kv::KVStore, key::String)::String
    isempty(kv.worker_id) ? "$(kv.namespace):$(key)" : "$(kv.worker_id):$(kv.namespace):$(key)"
end

kv_get(kv::KVStore, key::String, default=nothing) =
    lock(kv.lock) do get(kv.data, _full_key(kv, key), default) end

kv_set!(kv::KVStore, key::String, val) =
    lock(kv.lock) do kv.data[_full_key(kv, key)] = val end

kv_delete!(kv::KVStore, key::String) =
    lock(kv.lock) do delete!(kv.data, _full_key(kv, key)) end

function kv_get_or_set!(kv::KVStore, key::String, factory::Function)
    fk = _full_key(kv, key)
    lock(kv.lock) do
        haskey(kv.data, fk) && return kv.data[fk]
        val = factory()
        kv.data[fk] = val
        val
    end
end

const _kv_stores     = Dict{String,KVStore}()
const _kv_stores_lck = ReentrantLock()

function get_kv(namespace::String, worker_id::String)::KVStore
    store_key = isempty(worker_id) ? namespace : "$(worker_id):$(namespace)"
    lock(_kv_stores_lck) do
        get!(_kv_stores, store_key, KVStore(namespace, worker_id))
    end
end

# ---------------------------------------------------------------------------
# Session proxy
# ---------------------------------------------------------------------------

struct SessionProxy
    data::Dict{String,Any}
end

session_get(s::SessionProxy, key::String, default=nothing) = get(s.data, key, default)

function session_set!(s::SessionProxy, key::String, val)
    # Record update in meta so Go's routeResponse can merge session state.
    m = _tls_get(:meta, Dict{String,Any}())
    updates = get!(m, "_session_updates", Dict{String,Any}())
    updates[key] = val
    _tls_set(:meta, m)
    s.data[key] = val
end

# ---------------------------------------------------------------------------
# Pepper public API module (injected into capability modules as `pepper`)
# ---------------------------------------------------------------------------

module PepperAPI
    import ..corr_id, ..origin_id, ..meta, ..config, ..delivery_count
    import ..is_cancelled, ..session_data, ..SessionProxy
    import ..get_kv, ..kv_get, ..kv_set!, ..kv_delete!, ..kv_get_or_set!

    function corr_id()        return Main.corr_id()        end
    function origin_id()      return Main.origin_id()      end
    function meta()           return Main.meta()           end
    function config()         return Main.config()         end
    function delivery_count() return Main.delivery_count() end
    function is_cancelled()   return Main.is_cancelled()   end

    function session()
        return SessionProxy(Main.session_data())
    end

    function kv(namespace::String="default")
        worker_id = get(ENV, "PEPPER_WORKER_ID", "")
        return Main.get_kv(namespace, worker_id)
    end

    function cache_dir()::String
        d = get(ENV, "PEPPER_CACHE_DIR",
               get(ENV, "HF_HOME",
                   joinpath(homedir(), ".cache", "pepper")))
        mkpath(d)
        return d
    end
end

# ---------------------------------------------------------------------------
# Loaded capability
# ---------------------------------------------------------------------------

struct LoadedCap
    spec::Dict{String,Any}
    mod::Module            # the included Julia module
    semaphore::Base.Semaphore
end

function _run_cap(lc::LoadedCap, inputs::Dict)
    Base.acquire(lc.semaphore)
    try
        return lc.mod.run(inputs)
    finally
        Base.release(lc.semaphore)
    end
end

function _teardown_cap(lc::LoadedCap)
    if isdefined(lc.mod, :teardown)
        try lc.mod.teardown() catch e
            @warn "teardown error: $e"
        end
    end
end

# ---------------------------------------------------------------------------
# Capability loader
# ---------------------------------------------------------------------------
# Each capability source file is loaded into a fresh anonymous module so that
# top-level definitions do not collide across capabilities. The file has access
# to the `pepper` binding (our PepperAPI module) via the module's globals.

function load_capability(cap_load::Dict)::LoadedCap
    source = get(cap_load, "source", "")
    isempty(source) || isfile(source) || error("capability source not found: $source")

    # Parse Form A metadata from the source header.
    source_text = isempty(source) ? "" : read(source, String)
    form_a = parse_form_a(source_text)

    max_concurrent = get(cap_load, "max_concurrent", 4)
    sem = Base.Semaphore(max_concurrent)

    # Create a fresh anonymous module for isolation.
    cap_mod = Module(gensym("PepperCap"))

    # Inject the Pepper API into the module so caps can call pepper.kv() etc.
    Core.eval(cap_mod, :(const pepper = $(PepperAPI)))

    # Evaluate the source file inside cap_mod.
    isempty(source) || Base.include(cap_mod, source)

    # Call setup! / setup if defined.
    cfg = get(cap_load, "config", nothing)
    cfg_dict = cfg isa Dict ? cfg : Dict{String,Any}()

    if isdefined(cap_mod, :setup!)
        cap_mod.setup!(cfg_dict)
    elseif isdefined(cap_mod, :setup)
        cap_mod.setup(cfg_dict)
    end

    merged_spec = merge(form_a, cap_load)
    return LoadedCap(merged_spec, cap_mod, sem)
end

# ---------------------------------------------------------------------------
# Wire frame helpers
# ---------------------------------------------------------------------------

function _encode_msg(topic::String, data::Vector{UInt8})::Vector{UInt8}
    tb = Vector{UInt8}(topic)
    tl = length(tb)
    header = UInt8[(tl >> 8) & 0xFF, tl & 0xFF]
    return vcat(header, tb, data)
end

function _reply_topic(env::Dict)::String
    msg_type = get(env, "msg_type", "")
    if msg_type in ("res", "err", "res_chunk", "res_end")
        return "pepper.res.$(get(env, "origin_id", ""))"
    elseif msg_type == "hb_ping"
        return "pepper.hb.$(get(env, "worker_id", ""))"
    elseif msg_type == "pipe"
        return get(env, "topic", "pepper.pipe.unknown")
    else
        return "pepper.control"
    end
end

const _RETRYABLE_CODES = Set([
    "DEADLINE_EXCEEDED", "WORKER_OOM", "NO_WORKERS",
    "WORKER_SHUTTING_DOWN", "DISPATCH_TIMEOUT", "MODEL_OOM", "MODEL_TIMEOUT",
])

# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

mutable struct Worker
    worker_id::String
    bus_url::String
    groups::Vector{String}
    heartbeat_ms::Int
    max_concurrent::Int
    codec::Codec
    caps::Dict{String,LoadedCap}
    requests_served::Threads.Atomic{Int}
    started_at::Float64
    running::Threads.Atomic{Bool}
    transport::Union{BusTransport,Nothing}
    send_lock::ReentrantLock
    msgs_received::Threads.Atomic{Int}
end

function Worker()
    Worker(
        get(ENV, "PEPPER_WORKER_ID", "w-julia-1"),
        get(ENV, "PEPPER_BUS_URL", "127.0.0.1:7731"),
        split(get(ENV, "PEPPER_GROUPS", "default"), ","),
        parse(Int, get(ENV, "PEPPER_HEARTBEAT_MS", "5000")),
        parse(Int, get(ENV, "PEPPER_MAX_CONCURRENT", "8")),
        Codec(get(ENV, "PEPPER_CODEC", "msgpack")),
        Dict{String,LoadedCap}(),
        Threads.Atomic{Int}(0),
        time(),
        Threads.Atomic{Bool}(true),
        nothing,
        ReentrantLock(),
        Threads.Atomic{Int}(0),
    )
end

function _send_envelope!(w::Worker, env::Dict)
    isempty(env) && return
    topic = _reply_topic(env)
    data  = marshal(w.codec, env)
    frame = _encode_msg(topic, data)
    lock(w.send_lock) do
        send_frame!(w.transport, frame)
    end
end

function _make_hello(w::Worker)::Dict
    Dict(
        "proto_ver"       => 1,
        "msg_type"        => "worker_hello",
        "worker_id"       => w.worker_id,
        "runtime"         => "julia",
        "pid"             => getpid(),
        "codec"           => w.codec.name,
        "groups"          => w.groups,
        "caps"            => collect(keys(w.caps)),
        "pipe_subs"       => [],
        "pipe_pubs"       => [],
        "cb_supported"    => false,  # future work
        "stream_supported"=> false,  # future work
    )
end

function _err_env(w::Worker, env::Dict, code::String, message::String)::Dict
    Dict(
        "proto_ver"  => 1,
        "msg_type"   => "err",
        "corr_id"    => get(env, "corr_id", ""),
        "origin_id"  => get(env, "origin_id", ""),
        "worker_id"  => w.worker_id,
        "cap"        => get(env, "cap", ""),
        "code"       => code,
        "message"    => message,
        "retryable"  => code ∈ _RETRYABLE_CODES,
    )
end

# ---------------------------------------------------------------------------
# cap_load handler
# ---------------------------------------------------------------------------

function handle_cap_load!(w::Worker, env::Dict)
    cap_name  = get(env, "cap", "")
    error_msg = ""
    t_start   = time()
    _info("cap_load start"; cap=cap_name, worker=w.worker_id)
    try
        w.caps[cap_name] = load_capability(env)
    catch e
        error_msg = sprint(showerror, e)
        _error("cap_load failed"; cap=cap_name, worker=w.worker_id, error=error_msg)
    end
    setup_ms = round(Int, (time() - t_start) * 1000)
    isempty(error_msg) && _info("cap_load done"; cap=cap_name, worker=w.worker_id, setup_ms=setup_ms)
    _send_envelope!(w, Dict(
        "proto_ver"  => 1,
        "msg_type"   => "cap_ready",
        "worker_id"  => w.worker_id,
        "cap"        => cap_name,
        "cap_ver"    => get(env, "cap_ver", ""),
        "setup_ms"   => setup_ms,
        "error"      => error_msg,
    ))
end

# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

function handle_request!(w::Worker, env::Dict)::Union{Dict,Nothing}
    cap_name  = get(env, "cap", "")
    origin    = get(env, "origin_id", "")
    corr      = get(env, "corr_id", "")

    # Check cancel before dispatch
    lock(_cancelled_ids_lck) do
        origin ∈ _cancelled_ids
    end && begin
        _info("request cancelled before exec"; cap=cap_name, corr_id=corr)
        return _err_env(w, env, "CANCELLED", "request cancelled")
    end

    lc = get(w.caps, cap_name, nothing)
    if isnothing(lc)
        _error("cap not found"; cap=cap_name, corr_id=corr, worker=w.worker_id)
        return _err_env(w, env, "CAP_NOT_FOUND", "capability '$cap_name' not loaded")
    end

    # Set task-local context for this request
    _tls_set(:corr_id,   corr)
    _tls_set(:origin_id, origin)
    _tls_set(:meta,      Dict{String,Any}(get(env, "meta", Dict()) isa Dict ? get(env, "meta", Dict{String,Any}()) : Dict{String,Any}()))
    _tls_set(:config,    lc.spec["config"] isa Dict ? lc.spec["config"] : Dict{String,Any}())
    _tls_set(:delivery,  get(env, "delivery_count", 0))
    _tls_set(:session,   let m = get(env, "meta", Dict())
                             m isa Dict ? get(m, "_session_data", Dict{String,Any}()) : Dict{String,Any}()
                         end)

    t_start = time()
    hop     = get(env, "hop", 0)
    _info("exec start"; cap=cap_name, corr_id=corr, worker=w.worker_id, hop=hop)

    payload_bytes = get(env, "payload", UInt8[])
    payload_bytes isa Vector{UInt8} || (payload_bytes = Vector{UInt8}(payload_bytes))
    inputs = isempty(payload_bytes) ? Dict{String,Any}() : unmarshal(w.codec, payload_bytes)

    result = nothing
    try
        result = _run_cap(lc, inputs)
        Threads.atomic_add!(w.requests_served, 1)
    catch e
        duration_ms = round(Int, (time() - t_start) * 1000)
        _error("exec error"; cap=cap_name, corr_id=corr, worker=w.worker_id,
               duration_ms=duration_ms, error=sprint(showerror, e))
        return _err_env(w, env, "EXEC_ERROR", sprint(showerror, e))
    end

    # Re-check cancel after execution
    lock(_cancelled_ids_lck) do
        origin ∈ _cancelled_ids
    end && return _err_env(w, env, "CANCELLED", "request cancelled")

    duration_ms = round(Int, (time() - t_start) * 1000)
    _info("exec done"; cap=cap_name, corr_id=corr, worker=w.worker_id, duration_ms=duration_ms)

    isnothing(result) && return nothing

    # Handle pipeline forward
    forward_to = get(env, "forward_to", "")
    if !isempty(forward_to)
        _send_envelope!(w, Dict(
            "proto_ver"  => 1, "msg_type" => "pipe",
            "corr_id"    => corr, "origin_id" => origin,
            "worker_id"  => w.worker_id, "cap" => cap_name,
            "hop"        => hop + 1, "forward_to" => forward_to,
            "topic"      => forward_to,
            "payload"    => marshal(w.codec, result isa Dict ? result : Dict("result" => result)),
            "meta"       => _tls_get(:meta, Dict{String,Any}()),
        ))
        return nothing
    end

    return Dict(
        "proto_ver"  => 1,
        "msg_type"   => "res",
        "corr_id"    => corr,
        "origin_id"  => origin,
        "worker_id"  => w.worker_id,
        "cap"        => cap_name,
        "cap_ver"    => get(lc.spec, "cap_ver", ""),
        "payload"    => marshal(w.codec, result isa Dict ? result : Dict("result" => result)),
        "session_id" => get(env, "session_id", ""),
        "meta"       => _tls_get(:meta, Dict{String,Any}()),
    )
end

# ---------------------------------------------------------------------------
# Heartbeat loop
# ---------------------------------------------------------------------------

function _heartbeat_loop(w::Worker)
    interval = w.heartbeat_ms / 1000.0
    while w.running[]
        sleep(interval)
        w.running[] || break
        _send_envelope!(w, Dict(
            "proto_ver"       => 1,
            "msg_type"        => "hb_ping",
            "worker_id"       => w.worker_id,
            "runtime"         => "julia",
            "load"            => 0,
            "groups"          => w.groups,
            "caps"            => collect(keys(w.caps)),
            "requests_served" => w.requests_served[],
            "uptime_ms"       => round(Int, (time() - w.started_at) * 1000),
        ))
    end
end

# ---------------------------------------------------------------------------
# Message loop
# ---------------------------------------------------------------------------

function _message_loop(w::Worker)
    while w.running[]
        data = recv_frame!(w.transport)
        if isnothing(data)
            continue
        end
        length(data) < 2 && continue

        topic_len = (Int(data[1]) << 8) | Int(data[2])
        payload   = data[3 + topic_len : end]
        env = try
            isempty(payload) ? Dict{String,Any}() : unmarshal(w.codec, payload)
        catch e
            _warn("decode error: $e")
            continue
        end

        msg_type = get(env, "msg_type", "")

        if msg_type == "cap_load"
            handle_cap_load!(w, env)

        elseif msg_type == "req"
            Threads.atomic_add!(w.msgs_received, 1)
            n = w.msgs_received[]
            _info("req received #$n";
                  cap=get(env,"cap",""), corr_id=get(env,"corr_id",""),
                  worker=w.worker_id, hop=get(env,"hop",0))
            # Run in a new task so the message loop stays responsive
            env_copy = copy(env)
            @spawn begin
                result = handle_request!(w, env_copy)
                !isnothing(result) && _send_envelope!(w, result)
            end

        elseif msg_type == "worker_bye"
            w.running[] = false
            break

        elseif msg_type == "cancel"
            oid = get(env, "origin_id", "")
            isempty(oid) || _mark_cancelled(oid)
        end
    end

    # Shutdown: tear down all caps and resources
    for lc in values(w.caps)
        _teardown_cap(lc)
    end
    close_resources!()
    close!(w.transport)
end

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

function run_worker()
    w = Worker()
    _info("pepper worker $(w.worker_id) starting, bus=$(w.bus_url)")

    w.transport = BusTransport(w.bus_url)
    connect!(w.transport)
    _info("bus connected"; scheme=w.transport.scheme, addr="$(w.transport.host):$(w.transport.port)")

    # Build topic subscription list (same logic as Python worker)
    topics = ["pepper.control", "pepper.broadcast"]
    if !isempty(w.worker_id)
        push!(topics, "pepper.control.$(w.worker_id)")
        push!(topics, "pepper.push.$(w.worker_id)")
    end
    for g in w.groups
        push!(topics, "pepper.push.$g")
        push!(topics, "pepper.pub.$g")
    end
    subscribe!(w.transport, topics)

    # Brief pause so transport subscriptions settle before announcing ourselves
    sleep(0.1)

    # Announce to the router
    _send_envelope!(w, _make_hello(w))

    # Start heartbeat in background
    @spawn _heartbeat_loop(w)

    # Run the message loop (blocks until worker_bye or connection error)
    _message_loop(w)
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if abspath(PROGRAM_FILE) == @__FILE__
    run_worker()
end
