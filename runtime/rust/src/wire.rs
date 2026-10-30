//! Wire-level constants: bus topics, environment variables and reserved names.
//!
//! Mirrors `internal/wire` on the Go side and the same block in
//! `runtime/python/runtime.py`, `runtime/julia/runtime.jl` and
//! `runtime/node/runtime.js`. If a topic or env name changes, it changes here
//! first and in the Go constants second.

/// Every topic Pepper publishes starts with this prefix.
pub const TOPIC_ROOT: &str = "pepper.";

pub const TOPIC_PUSH: &str = "pepper.push.";
pub const TOPIC_PUB: &str = "pepper.pub.";
pub const TOPIC_RES: &str = "pepper.res.";
pub const TOPIC_PIPE: &str = "pepper.pipe.";
pub const TOPIC_HB: &str = "pepper.hb.";
pub const TOPIC_CONTROL: &str = "pepper.control";
pub const TOPIC_BROADCAST: &str = "pepper.broadcast";

/// Response topic for one request.
pub fn topic_res(origin_id: &str) -> String {
    format!("{TOPIC_RES}{origin_id}")
}

/// Heartbeat topic for one worker.
pub fn topic_hb(worker_id: &str) -> String {
    format!("{TOPIC_HB}{worker_id}")
}

/// Per-worker control topic, used by the router to deliver `cap_load`.
pub fn topic_control_worker(worker_id: &str) -> String {
    format!("{TOPIC_CONTROL}.{worker_id}")
}

/// Environment variables injected by `internal/bus.WorkerEnvVars`.
pub mod env {
    pub const WORKER_ID: &str = "PEPPER_WORKER_ID";
    pub const BUS_URL: &str = "PEPPER_BUS_URL";
    pub const CODEC: &str = "PEPPER_CODEC";
    pub const GROUPS: &str = "PEPPER_GROUPS";
    pub const HEARTBEAT_MS: &str = "PEPPER_HEARTBEAT_MS";
    pub const MAX_CONCURRENT: &str = "PEPPER_MAX_CONCURRENT";
    pub const BLOB_DIR: &str = "PEPPER_BLOB_DIR";
    pub const RESOURCES: &str = "PEPPER_RESOURCES";

    /// Directory holding this crate — used as the path dependency when a
    /// capability source file is compiled on first use.
    pub const RUST_RUNTIME_DIR: &str = "PEPPER_RUST_RUNTIME_DIR";
    /// Optional prebuilt `pepper-worker` binary, bypassing `cargo build`.
    pub const RUST_WORKER_BIN: &str = "PEPPER_RUST_WORKER_BIN";
    /// Cache root for compiled capabilities (also used by the Python runtime).
    pub const CACHE_DIR: &str = "PEPPER_CACHE_DIR";
}

/// Group used when a capability or worker declares none.
pub const GROUP_DEFAULT: &str = "default";
