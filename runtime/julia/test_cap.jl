# test_cap.jl — Unit tests for cap.jl
# Run with: julia --project=@. test_cap.jl

push!(LOAD_PATH, dirname(@__FILE__))
include("cap.jl")
using .Cap

passed = 0
failed = 0

macro test(name, expr)
    quote
        try
            $(esc(expr))
            println("  ✓ ", $(esc(name)))
            global passed += 1
        catch e
            println("  ✗ ", $(esc(name)), ": ", sprint(showerror, e))
            global failed += 1
        end
    end
end

# ---------------------------------------------------------------------------
# parse_form_a
# ---------------------------------------------------------------------------

println("\nparse_form_a")

@test "parses contiguous header" begin
    src = """
# pepper:name = speech.transcribe
# pepper:version = 1.0.0
# pepper:deps = LinearAlgebra, FFTW

function run(inputs)
  # pepper:groups = something-inside
end
"""
    meta = parse_form_a(src)
    @assert meta["name"]    == "speech.transcribe" "name mismatch"
    @assert meta["version"] == "1.0.0"             "version mismatch"
    @assert meta["deps"]    == "LinearAlgebra, FFTW" "deps mismatch"
    @assert !haskey(meta, "groups") "should not parse inside function body"
end

@test "stops at first non-comment line" begin
    src = """
# pepper:name = embed.text
# pepper:version = 2.0.0

using LinearAlgebra
"""
    meta = parse_form_a(src)
    @assert meta["name"]    == "embed.text" "name mismatch"
    @assert meta["version"] == "2.0.0"     "version mismatch"
end

@test "ignores shebang" begin
    src = "#!/usr/bin/env julia\n# pepper:name = x\nconst x = 1\n"
    meta = parse_form_a(src)
    @assert meta["name"] == "x" "name mismatch"
end

@test "empty source returns empty dict" begin
    @assert isempty(parse_form_a("")) "should be empty"
end

@test "blank lines within header are skipped" begin
    src = "# pepper:name = a\n\n# pepper:version = 3.0.0\nconst x = 1\n"
    meta = parse_form_a(src)
    @assert meta["name"]    == "a"     "name mismatch"
    @assert meta["version"] == "3.0.0" "version mismatch"
end

# ---------------------------------------------------------------------------
# read_blob
# ---------------------------------------------------------------------------

println("\nread_blob")

@test "throws on non-blob dict" begin
    thrown = false
    try read_blob(Dict{String,Any}("foo" => "bar"))
    catch; thrown = true; end
    @assert thrown "expected error"
end

@test "constructs BlobRef from valid reference" begin
    ref = Dict{String,Any}(
        "_pepper_blob" => true,
        "id"     => "blob_abc123",
        "path"   => "/dev/shm/pepper_blob_abc123.dat",
        "size"   => 1024,
        "dtype"  => "float32",
        "shape"  => [256, 4],
        "format" => "raw",
    )
    b = read_blob(ref)
    @assert b.path   == "/dev/shm/pepper_blob_abc123.dat" "path mismatch"
    @assert b.size   == 1024                              "size mismatch"
    @assert b.dtype  == "float32"                         "dtype mismatch"
    @assert b.shape  == [256, 4]                          "shape mismatch"
end

@test "as_bytes reads file contents" begin
    tmp = tempname() * ".dat"
    data = Vector{UInt8}("hello pepper")
    write(tmp, data)
    try
        b = read_blob(Dict{String,Any}("_pepper_blob" => true, "path" => tmp, "size" => length(data)))
        @assert as_bytes(b) == data "bytes mismatch"
    finally
        rm(tmp, force=true)
    end
end

@test "as_array returns mmap'd Float32 array when dtype/shape set" begin
    import Mmap
    tmp = tempname() * ".dat"
    arr = Float32[1.0, 2.0, 3.0, 4.0]
    write(tmp, arr)
    try
        b = read_blob(Dict{String,Any}(
            "_pepper_blob" => true, "path" => tmp,
            "size" => sizeof(arr), "dtype" => "float32", "shape" => [4],
        ))
        out = as_array(b)
        @assert length(out) == 4     "length mismatch"
        @assert out[1] ≈ 1.0f0      "value mismatch"
    finally
        rm(tmp, force=true)
    end
end

# ---------------------------------------------------------------------------
# KV store
# ---------------------------------------------------------------------------

println("\nKV helpers")

@test "basic get/set/delete" begin
    kv = Cap.KVStore("test_ns", "w-test")
    Cap.kv_set!(kv, "x", 42)
    @assert Cap.kv_get(kv, "x") == 42 "get after set"
    Cap.kv_delete!(kv, "x")
    @assert Cap.kv_get(kv, "x", 0) == 0 "key should be gone"
end

@test "get_or_set! calls factory once" begin
    kv = Cap.KVStore("lazy", "w-test")
    calls = Ref(0)
    v = Cap.kv_get_or_set!(kv, "model") do
        calls[] += 1
        "my_model"
    end
    @assert v == "my_model"   "wrong value"
    @assert calls[] == 1      "factory called once"
    # Second call should NOT invoke factory
    v2 = Cap.kv_get_or_set!(kv, "model") do
        calls[] += 1
        "other"
    end
    @assert v2 == "my_model"  "should return cached value"
    @assert calls[] == 1      "factory should not be called again"
end

@test "worker-scoped isolation" begin
    kv1 = Cap.KVStore("ns", "w-1")
    kv2 = Cap.KVStore("ns", "w-2")
    Cap.kv_set!(kv1, "shared_key", "worker1_val")
    Cap.kv_set!(kv2, "shared_key", "worker2_val")
    @assert Cap.kv_get(kv1, "shared_key") == "worker1_val" "w-1 isolation"
    @assert Cap.kv_get(kv2, "shared_key") == "worker2_val" "w-2 isolation"
end

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

println("\n$(passed + failed) tests: $passed passed, $failed failed")
exit(failed > 0 ? 1 : 0)
