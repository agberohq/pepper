# transport.jl — Pepper bus transport layer for Julia workers.
#
# Three backends, matching transport.py exactly:
#   mula  — raw TCP framing (4-byte big-endian length prefix)
#   redis — redis-cli via Jedis-style RESP (requires Redis.jl)
#   nats  — JetStream push consumers (requires NATS.jl)
#
# Only the Mula backend is in the critical path for single-node deployments.
# Redis and NATS are added for cluster parity with the Python worker.

module Transport

using Sockets
using Logging

export BusTransport, connect!, subscribe!, send_frame!, recv_frame!, close!

# ---------------------------------------------------------------------------
# Wire helpers shared across backends
# ---------------------------------------------------------------------------

"""Write exactly n bytes to `io`, blocking until all are sent."""
function write_exact(io, data::AbstractVector{UInt8})
    n = length(data)
    written = 0
    while written < n
        written += write(io, @view data[written+1:end])
    end
end

"""Read exactly n bytes from `io`, returning nothing on EOF."""
function read_exact(io, n::Int)::Union{Vector{UInt8}, Nothing}
    buf = Vector{UInt8}(undef, n)
    pos = 0
    while pos < n
        chunk = readavailable(io)
        if isempty(chunk)
            # block until data arrives
            try
                chunk = read(io, n - pos)
            catch
                return nothing
            end
        end
        copy!(buf, pos + 1, chunk, 1, min(length(chunk), n - pos))
        pos += min(length(chunk), n - pos)
    end
    return buf
end

"""Decode (topic, payload) from a Worker._send_envelope frame."""
function unwrap_frame(data::Vector{UInt8})::Tuple{String, Vector{UInt8}}
    length(data) < 2 && return ("", data)
    topic_len = (Int(data[1]) << 8) | Int(data[2])
    topic_len == 0 && return ("", data[3:end])
    length(data) < 2 + topic_len && return ("", data)
    topic = String(data[3:2+topic_len])
    payload = data[3+topic_len:end]
    return (topic, payload)
end

"""Wrap payload with zero-length topic prefix for message_loop compatibility."""
function wrap_payload(payload::Vector{UInt8})::Vector{UInt8}
    return vcat(UInt8[0x00, 0x00], payload)
end

# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------

abstract type AbstractTransport end

function connect!(t::AbstractTransport; retries=10) end
function subscribe!(t::AbstractTransport, topics::Vector{String}) end
function send_frame!(t::AbstractTransport, data::Vector{UInt8}) end
function recv_frame!(t::AbstractTransport)::Union{Vector{UInt8}, Nothing} end
function close!(t::AbstractTransport) end

# ---------------------------------------------------------------------------
# Mula (raw TCP) transport
# ---------------------------------------------------------------------------

mutable struct MulaTransport <: AbstractTransport
    host::String
    port::Int
    conn::Union{TCPSocket, Nothing}
    send_lock::ReentrantLock

    MulaTransport(host, port) = new(host, port, nothing, ReentrantLock())
end

function connect!(t::MulaTransport; retries=10)
    for attempt in 1:retries
        try
            t.conn = connect(t.host, t.port)
            return
        catch e
            delay = min(0.1 * 2^(attempt - 1), 4.0)
            @debug "mula connect attempt $attempt failed: $e — retrying in $(delay)s"
            sleep(delay)
        end
    end
    error("mula: could not connect to $(t.host):$(t.port) after $retries attempts")
end

function subscribe!(t::MulaTransport, topics::Vector{String})
    # Send pipe-separated topic list as the hello frame.
    frame = Vector{UInt8}(join(topics, "|"))
    send_frame!(t, frame)
end

function send_frame!(t::MulaTransport, data::Vector{UInt8})
    isnothing(t.conn) && return
    n = length(data)
    header = UInt8[(n >> 24) & 0xFF, (n >> 16) & 0xFF, (n >> 8) & 0xFF, n & 0xFF]
    lock(t.send_lock) do
        try
            write_exact(t.conn, header)
            write_exact(t.conn, data)
        catch e
            @warn "mula: send failed: $e"
        end
    end
end

function recv_frame!(t::MulaTransport)::Union{Vector{UInt8}, Nothing}
    isnothing(t.conn) && return nothing
    header = try
        read(t.conn, 4)
    catch
        return nothing
    end
    length(header) < 4 && return nothing
    n = (Int(header[1]) << 24) | (Int(header[2]) << 16) | (Int(header[3]) << 8) | Int(header[4])
    n == 0 && return UInt8[]
    try
        return read(t.conn, n)
    catch
        return nothing
    end
end

function close!(t::MulaTransport)
    if !isnothing(t.conn)
        try close(t.conn) catch end
        t.conn = nothing
    end
end

# ---------------------------------------------------------------------------
# Redis transport
# ---------------------------------------------------------------------------

mutable struct RedisTransport <: AbstractTransport
    host::String
    port::Int
    # Julia Redis support via Redis.jl. We hold raw sockets to avoid
    # taking a hard dependency — Redis.jl is optional.
    _client::Any
    _brpop_client::Any
    _inbox::Channel{Vector{UInt8}}
    _queues::Vector{String}
    _sub_topics::Vector{String}
    _stop::Threads.Atomic{Bool}

    RedisTransport(host, port) = new(
        host, port, nothing, nothing,
        Channel{Vector{UInt8}}(512),
        String[], String[],
        Threads.Atomic{Bool}(false),
    )
end

function connect!(t::RedisTransport; retries=10)
    try
        redis = Base.require(Base.PkgId(Base.UUID("0a0d4562-1929-4f6e-88f8-f5740e2f92c2"), "Redis"))
        for attempt in 1:retries
            try
                t._client = redis.RedisConnection(host=t.host, port=t.port)
                t._brpop_client = redis.RedisConnection(host=t.host, port=t.port)
                return
            catch e
                sleep(min(0.1 * 2^(attempt - 1), 4.0))
            end
        end
        error("redis: could not connect after $retries attempts")
    catch e
        error("redis transport requires Redis.jl: add it with `using Pkg; Pkg.add(\"Redis\")`")
    end
end

function subscribe!(t::RedisTransport, topics::Vector{String})
    t._queues = filter(s -> startswith(s, "pepper.push."), topics)
    t._sub_topics = filter(s -> !startswith(s, "pepper.push."), topics)
    # Subscribe to pub/sub channels
    if !isempty(t._sub_topics)
        Threads.@spawn _redis_pubsub_loop(t)
    end
    if !isempty(t._queues)
        Threads.@spawn _redis_brpop_loop(t)
    end
end

function _redis_pubsub_loop(t::RedisTransport)
    # Minimal pub/sub listener — implementation deferred to Redis.jl usage
    @warn "redis: pub/sub loop not fully implemented — use Mula for production"
end

function _redis_brpop_loop(t::RedisTransport)
    while !t._stop[]
        try
            result = t._client.blpop(t._queues..., 2)
            if !isnothing(result) && length(result) == 2
                put!(t._inbox, wrap_payload(Vector{UInt8}(result[2])))
            end
        catch e
            t._stop[] || (@warn "redis: brpop error: $e"; sleep(0.5))
        end
    end
end

function send_frame!(t::RedisTransport, data::Vector{UInt8})
    topic, payload = unwrap_frame(data)
    isempty(topic) && return
    try
        t._client.publish(topic, String(payload))
    catch e
        @warn "redis: send_frame failed [$topic]: $e"
    end
end

function recv_frame!(t::RedisTransport)::Union{Vector{UInt8}, Nothing}
    try
        return take!(t._inbox)  # non-blocking via timedwait
    catch
        return nothing
    end
end

function close!(t::RedisTransport)
    t._stop[] = true
    close(t._inbox)
end

# ---------------------------------------------------------------------------
# NATS transport
# ---------------------------------------------------------------------------

mutable struct NATSTransport <: AbstractTransport
    url::String
    _nc::Any
    _inbox::Channel{Vector{UInt8}}
    _stop::Threads.Atomic{Bool}

    NATSTransport(host, port) = new(
        "nats://$(host):$(port)",
        nothing,
        Channel{Vector{UInt8}}(512),
        Threads.Atomic{Bool}(false),
    )
end

function connect!(t::NATSTransport; retries=10)
    try
        nats = Base.require(Base.PkgId(Base.UUID("a6e6e15b-59c9-46c6-a6c9-39d22ce9d4b6"), "NATS"))
        for attempt in 1:retries
            try
                t._nc = nats.connect(t.url)
                return
            catch e
                sleep(min(0.1 * 2^(attempt - 1), 4.0))
            end
        end
        error("nats: could not connect after $retries attempts")
    catch e
        error("nats transport requires NATS.jl: add it with `using Pkg; Pkg.add(\"NATS\")`")
    end
end

function subscribe!(t::NATSTransport, topics::Vector{String})
    push_topics = filter(s -> startswith(s, "pepper.push."), topics)
    core_topics = filter(s -> !startswith(s, "pepper.push."), topics)
    for subject in core_topics
        t._nc.subscribe(subject) do msg
            put!(t._inbox, wrap_payload(Vector{UInt8}(msg.payload)))
        end
    end
    for subject in push_topics
        # JetStream queue-group consumer — mirrors the Python _NATSTransport
        queue_group = "c-" * replace(subject, "." => "_")
        try
            js = t._nc.jetstream()
            js.subscribe(subject, queue=queue_group) do msg
                put!(t._inbox, wrap_payload(Vector{UInt8}(msg.data)))
                msg.ack()
            end
        catch e
            @warn "nats: js subscribe failed [$subject]: $e"
        end
    end
end

function send_frame!(t::NATSTransport, data::Vector{UInt8})
    topic, payload = unwrap_frame(data)
    isempty(topic) && return
    try
        t._nc.publish(topic, payload)
    catch e
        @warn "nats: publish failed: $e"
    end
end

function recv_frame!(t::NATSTransport)::Union{Vector{UInt8}, Nothing}
    try
        return fetch(timedwait(() -> isready(t._inbox), 0.05)) == :ok ? take!(t._inbox) : nothing
    catch
        return nothing
    end
end

function close!(t::NATSTransport)
    t._stop[] = true
    try t._nc.close() catch end
    close(t._inbox)
end

# ---------------------------------------------------------------------------
# BusTransport — public façade, selects backend from PEPPER_BUS_URL
# ---------------------------------------------------------------------------

mutable struct BusTransport
    url::String
    scheme::String
    host::String
    port::Int
    impl::AbstractTransport
end

function _parse_url(url::String)
    if !contains(url, "://")
        # bare host:port — Mula
        host, _, port_str = partition(url, ":")
        port = isempty(port_str) ? 7731 : parse(Int, port_str)
        return "mula", isempty(host) ? "127.0.0.1" : host, port
    end
    scheme, rest = split(url, "://"; limit=2)
    scheme = lowercase(scheme)
    host, _, port_str = partition(rest, ":")
    defaults = Dict("redis" => 6379, "nats" => 4222, "mula" => 7731, "tcp" => 7731)
    port = isempty(port_str) ? get(defaults, scheme, 7731) : parse(Int, port_str)
    return scheme, isempty(host) ? "127.0.0.1" : host, port
end

function BusTransport(url::String)
    scheme, host, port = _parse_url(url)
    impl = if scheme in ("mula", "tcp")
        MulaTransport(host, port)
    elseif scheme == "redis"
        RedisTransport(host, port)
    elseif scheme == "nats"
        NATSTransport(host, port)
    else
        error("unsupported bus scheme: $scheme — supported: mula, tcp, redis, nats")
    end
    BusTransport(url, scheme, host, port, impl)
end

connect!(t::BusTransport; kw...)    = connect!(t.impl; kw...)
subscribe!(t::BusTransport, topics) = subscribe!(t.impl, topics)
send_frame!(t::BusTransport, data)  = send_frame!(t.impl, data)
recv_frame!(t::BusTransport)        = recv_frame!(t.impl)
close!(t::BusTransport)             = close!(t.impl)

end # module Transport
