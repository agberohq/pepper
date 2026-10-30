//! The worker loop: connect, announce, load, dispatch, heartbeat, shut down.
//!
//! This mirrors `runtime/python/runtime.py` message for message. The router does
//! not care which language a worker is written in — it only sees envelopes — so
//! any divergence here shows up as a hung request, not as an error.
//!
//! One worker serves **many** capabilities: the Go runtime passes every `.rs` file
//! that matches a worker's groups, the router sends one `cap_load` per capability,
//! and requests are dispatched by `cap`. That is the same shape as a Python worker
//! hosting several `.py` files.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use crate::cap::{Capability, CapabilitySpec, Config, Context, Input};
use crate::envelope::{self, err_code, msg, Envelope, Error, Result};
use crate::transport::Transport;
use crate::wire;

type Handler = Arc<Mutex<Box<dyn Capability>>>;

/// Configuration read from the environment the Go runtime injects.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub bus_url: String,
    pub groups: Vec<String>,
    pub heartbeat: Duration,
    pub max_concurrent: usize,
    pub codec: String,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self> {
        let worker_id = std::env::var(wire::env::WORKER_ID).map_err(|_| {
            Error::Config(format!(
                "{} is not set — start workers through pepper, not by hand",
                wire::env::WORKER_ID
            ))
        })?;
        let bus_url = std::env::var(wire::env::BUS_URL)
            .map_err(|_| Error::Config(format!("{} is not set", wire::env::BUS_URL)))?;
        let groups = std::env::var(wire::env::GROUPS)
            .unwrap_or_else(|_| wire::GROUP_DEFAULT.to_string())
            .split(',')
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let heartbeat_ms: u64 = std::env::var(wire::env::HEARTBEAT_MS)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000);
        let max_concurrent: usize = std::env::var(wire::env::MAX_CONCURRENT)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8)
            .max(1);
        let codec = std::env::var(wire::env::CODEC).unwrap_or_else(|_| "msgpack".to_string());
        Ok(Self {
            worker_id,
            bus_url,
            groups,
            heartbeat: Duration::from_millis(heartbeat_ms),
            max_concurrent,
            codec,
        })
    }
}

/// Serve every declared capability until the router says goodbye.
pub fn serve(specs: Vec<CapabilitySpec>) -> Result<()> {
    let cfg = WorkerConfig::from_env()?;
    if cfg.codec != "msgpack" {
        return Err(Error::Config(format!(
            "the Rust runtime speaks msgpack only, got codec {:?}",
            cfg.codec
        )));
    }
    if specs.is_empty() {
        return Err(Error::Config("no capabilities were passed to the worker".into()));
    }

    let declared: HashMap<String, CapabilitySpec> =
        specs.into_iter().map(|s| (s.name.clone(), s)).collect();

    let transport = Arc::new(Transport::connect(&cfg.bus_url, 10)?);

    // Topics this worker consumes: control (global + per-worker), broadcast, the
    // per-worker work queue, and every group's work queue and fan-out channel.
    let mut topics = vec![
        wire::TOPIC_CONTROL.to_string(),
        wire::TOPIC_BROADCAST.to_string(),
    ];
    topics.push(wire::topic_control_worker(&cfg.worker_id));
    topics.push(format!("{}{}", wire::TOPIC_PUSH, cfg.worker_id));
    for g in &cfg.groups {
        topics.push(format!("{}{}", wire::TOPIC_PUSH, g));
        topics.push(format!("{}{}", wire::TOPIC_PUB, g));
    }
    transport.subscribe(&topics)?;

    let started_at = Instant::now();
    let cancelled: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let served = Arc::new(AtomicU64::new(0));
    let handlers: Arc<Mutex<HashMap<String, Handler>>> = Arc::new(Mutex::new(HashMap::new()));
    // name -> cap_ver, for heartbeats and response envelopes.
    let loaded: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    send(&transport, hello(&cfg, declared.keys().cloned().collect()))?;
    log(
        "info",
        "worker connected",
        &format!(
            "{{\"worker\":\"{}\",\"groups\":{},\"declared\":{}}}",
            cfg.worker_id,
            json_array(&cfg.groups),
            json_array(&declared.keys().cloned().collect::<Vec<_>>())
        ),
    );

    {
        let hb_transport = Arc::clone(&transport);
        let hb_cfg = cfg.clone();
        let hb_served = Arc::clone(&served);
        let hb_loaded = Arc::clone(&loaded);
        thread::spawn(move || heartbeat_loop(hb_transport, hb_cfg, hb_served, hb_loaded, started_at));
    }

    loop {
        let frame = match transport.recv() {
            Ok(Some(f)) => f,
            Ok(None) => {
                log("warn", "router closed the connection", "{}");
                break;
            }
            Err(e) => {
                log("error", "recv failed", &format!("{{\"error\":\"{e}\"}}"));
                break;
            }
        };

        let (_, payload) = envelope::unframe(&frame);
        let env = match envelope::decode(payload) {
            Ok(e) => e,
            Err(e) => {
                log("warn", "decode failed", &format!("{{\"error\":\"{e}\"}}"));
                continue;
            }
        };

        match env.msg_type.as_str() {
            msg::CAP_LOAD => {
                let started = Instant::now();
                let name = env.cap.clone();
                let spec = match declared.get(&name) {
                    Some(s) => s,
                    None => {
                        // Tell the router straight away instead of leaving it to
                        // time out: nothing in this worker serves that capability.
                        send(
                            &transport,
                            cap_ready(
                                &cfg.worker_id,
                                &name,
                                "",
                                0,
                                "capability is not declared by this worker",
                            ),
                        )?;
                        continue;
                    }
                };

                match load_capability(spec, &env) {
                    Ok(h) => {
                        let setup_ms = started.elapsed().as_millis() as i64;
                        if let Ok(mut map) = handlers.lock() {
                            map.insert(name.clone(), Arc::new(Mutex::new(h)));
                        }
                        if let Ok(mut map) = loaded.lock() {
                            map.insert(name.clone(), env.cap_ver.clone());
                        }
                        send(
                            &transport,
                            cap_ready(&cfg.worker_id, &name, &env.cap_ver, setup_ms, ""),
                        )?;
                        log(
                            "info",
                            "capability loaded",
                            &format!("{{\"cap\":\"{name}\",\"setup_ms\":{setup_ms}}}"),
                        );
                    }
                    Err(e) => {
                        let setup_ms = started.elapsed().as_millis() as i64;
                        send(
                            &transport,
                            cap_ready(&cfg.worker_id, &name, &env.cap_ver, setup_ms, &e.to_string()),
                        )?;
                        log(
                            "error",
                            "capability failed to load",
                            &format!("{{\"cap\":\"{name}\",\"error\":\"{e}\"}}"),
                        );
                    }
                }
            }
            msg::REQ => {
                let cap = env.cap.clone();
                let handler = handlers.lock().ok().and_then(|m| m.get(&cap).cloned());
                let cap_ver = loaded
                    .lock()
                    .ok()
                    .and_then(|m| m.get(&cap).cloned())
                    .unwrap_or_default();

                // One thread per request keeps the read loop free to answer
                // heartbeats and cancels. The handler itself is serialized by
                // its mutex, so a single model instance is never run twice at
                // once; scale with more workers instead.
                let t = Arc::clone(&transport);
                let c = Arc::clone(&cancelled);
                let s = Arc::clone(&served);
                let wid = cfg.worker_id.clone();
                thread::spawn(move || {
                    if let Some(reply) = handle_request(handler, &c, &wid, &cap, &cap_ver, &env) {
                        if let Err(e) = send(&t, reply) {
                            log("error", "send response failed", &format!("{{\"cap\":\"{cap}\",\"error\":\"{e}\"}}"));
                        }
                    }
                    s.fetch_add(1, Ordering::Relaxed);
                });
            }
            msg::CANCEL => {
                let origin = env.origin_id.clone();
                if !origin.is_empty() {
                    if let Ok(mut set) = cancelled.lock() {
                        set.insert(origin);
                    }
                }
            }
            msg::WORKER_BYE => {
                log("info", "worker bye — shutting down", "{}");
                break;
            }
            msg::AUTH_CHALLENGE => {
                if let Err(e) = handle_auth_challenge(&cfg, &transport, &env) {
                    log("error", "auth challenge failed", &format!("{{\"error\":\"{e}\"}}"));
                }
            }
            other => log(
                "debug",
                "ignoring message",
                &format!("{{\"msg_type\":\"{other}\"}}"),
            ),
        }
    }

    if let Ok(map) = handlers.lock() {
        for (_, h) in map.iter() {
            if let Ok(mut h) = h.lock() {
                h.teardown();
            }
        }
    }
    Ok(())
}

fn load_capability(spec: &CapabilitySpec, env: &Envelope) -> Result<Box<dyn Capability>> {
    let mut handler = (spec.factory)();
    handler.setup(&Config(env.config.clone()))?;
    Ok(handler)
}

/// Execute one request and build the reply envelope.
fn handle_request(
    handler: Option<Handler>,
    cancelled: &Arc<Mutex<HashSet<String>>>,
    worker_id: &str,
    cap: &str,
    cap_ver: &str,
    env: &Envelope,
) -> Option<Envelope> {
    let handler = match handler {
        Some(h) => h,
        None => {
            return Some(error_envelope(
                env,
                worker_id,
                err_code::CAP_NOT_FOUND,
                &format!("capability {cap:?} is not loaded on this worker"),
            ))
        }
    };

    let ctx = Context::new(env, Arc::clone(cancelled));
    if ctx.deadline_exceeded() {
        return Some(error_envelope(
            env,
            worker_id,
            err_code::DEADLINE_EXCEEDED,
            "deadline already passed",
        ));
    }
    if ctx.is_cancelled() {
        return Some(error_envelope(env, worker_id, err_code::CANCELLED, "request cancelled"));
    }

    let input = match decode_payload(env.payload_bytes()) {
        Ok(v) => Input::from_value(v),
        Err(e) => {
            return Some(error_envelope(
                env,
                worker_id,
                err_code::PAYLOAD_INVALID,
                &e.to_string(),
            ))
        }
    };

    let outcome = match handler.lock() {
        Ok(mut h) => h.run(&ctx, input),
        Err(_) => Err(Error::Capability("handler panicked while locked".into())),
    };

    match outcome {
        Ok(value) => {
            let payload = match encode_payload(&value) {
                Ok(p) => p,
                Err(e) => {
                    return Some(error_envelope(env, worker_id, err_code::EXEC_ERROR, &e.to_string()))
                }
            };
            let mut meta = Map::new();
            let updates = ctx.session_updates();
            if !updates.is_empty() {
                meta.insert("_session_updates".to_string(), Value::Object(updates));
            }
            Some(Envelope {
                proto_ver: envelope::PROTO_VER,
                msg_type: msg::RES.to_string(),
                corr_id: env.corr_id.clone(),
                origin_id: env.origin_id.clone(),
                worker_id: worker_id.to_string(),
                cap: cap.to_string(),
                cap_ver: cap_ver.to_string(),
                hop: env.hop,
                session_id: env.session_id.clone(),
                meta,
                payload: Some(serde_bytes::ByteBuf::from(payload)),
                ..Default::default()
            })
        }
        Err(e) => Some(error_envelope(env, worker_id, err_code::EXEC_ERROR, &e.to_string())),
    }
}

/// Decode a msgpack payload into a dynamic value.
fn decode_payload(bytes: &[u8]) -> Result<Value> {
    if bytes.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    rmp_serde::from_slice(bytes)
        .map_err(|e| Error::Capability(format!("decode request payload: {e}")))
}

/// Encode a capability result as a msgpack map.
fn encode_payload(value: &Value) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(value)
        .map_err(|e| Error::Capability(format!("encode response payload: {e}")))
}

fn hello(cfg: &WorkerConfig, declared: Vec<String>) -> Envelope {
    let public_key = load_auth_public_key();
    Envelope {
        proto_ver: envelope::PROTO_VER,
        msg_type: msg::WORKER_HELLO.to_string(),
        worker_id: cfg.worker_id.clone(),
        runtime: "rust".to_string(),
        pid: std::process::id(),
        codec: "msgpack".to_string(),
        groups: cfg.groups.clone(),
        caps: declared,
        // Callbacks and streaming are not implemented in this runtime yet;
        // advertising them would make the router wait for messages we never send.
        cb_supported: false,
        stream_supported: false,
        public_key,
        ..Default::default()
    }
}

/// Handle auth_challenge: sign the challenge and send auth_response.
fn handle_auth_challenge(
    cfg: &WorkerConfig,
    transport: &Transport,
    env: &Envelope,
) -> Result<()> {
    let private_key_hex = std::env::var("PEPPER_AUTH_KEY").unwrap_or_default();
    if private_key_hex.is_empty() {
        log("warn", "auth_challenge received but no PEPPER_AUTH_KEY set", "{}");
        return Ok(());
    }
    let priv_bytes = hex::decode(&private_key_hex)
        .map_err(|e| Error::Capability(format!("decode auth key: {e}")))?;
    if priv_bytes.len() != 64 {
        return Err(Error::Capability(
            "PEPPER_AUTH_KEY must be 64 bytes (128 hex chars)".to_string(),
        ));
    }
    let secret = ed25519_dalek::SecretKey::from_slice(&priv_bytes[..32])
        .map_err(|e| Error::Capability(format!("invalid Ed25519 secret key: {e}")))?;
    let signing_key = ed25519_dalek::SigningKey::from(secret);
    let challenge = env.meta.get("challenge").and_then(|v| {
        if let Value::Array(arr) = v {
            Some(arr.iter().filter_map(|b| b.as_u64()).map(|b| b as u8).collect::<Vec<u8>>())
        } else {
            None
        }
    }).unwrap_or_default();
    if challenge.is_empty() {
        // Try reading from the challenge field directly
        return Err(Error::Capability("empty challenge".to_string()));
    }
    let signature = signing_key.sign(&challenge);
    let public_key = *signing_key.verifying_key().as_bytes();
    let mut resp_meta = Map::new();
    resp_meta.insert("public_key".to_string(), Value::Array(
        public_key.iter().map(|b| Value::Number(serde_json::Number::from(*b))).collect()
    ));
    resp_meta.insert("signature".to_string(), Value::Array(
        signature.to_bytes().iter().map(|b| Value::Number(serde_json::Number::from(*b))).collect()
    ));
    let resp = Envelope {
        proto_ver: envelope::PROTO_VER,
        msg_type: msg::AUTH_RESPONSE.to_string(),
        worker_id: cfg.worker_id.clone(),
        meta: resp_meta,
        ..Default::default()
    };
    send(transport, resp)?;
    log("info", "auth_response sent", &format!("{{\"worker_id\":\"{}\"}}", cfg.worker_id));
    Ok(())
}

/// Load the Ed25519 public key from PEPPER_AUTH_KEY env var (hex-encoded).
fn load_auth_public_key() -> Vec<u8> {
    let private_key_hex = match std::env::var("PEPPER_AUTH_KEY") {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let priv_bytes = match hex::decode(&private_key_hex) {
        Ok(b) => b,
        Err(e) => {
            log("warn", "failed to decode PEPPER_AUTH_KEY", &format!("{{\"error\":\"{e}\"}}"));
            return Vec::new();
        }
    };
    if priv_bytes.len() != 64 {
        return Vec::new();
    }
    match ed25519_dalek::SecretKey::from_slice(&priv_bytes[..32]) {
        Ok(secret) => {
            let signing_key = ed25519_dalek::SigningKey::from(secret);
            signing_key.verifying_key().to_bytes().to_vec()
        }
        Err(e) => {
            log("warn", "failed to load auth public key", &format!("{{\"error\":\"{e}\"}}"));
            Vec::new()
        }
    }
}

fn cap_ready(worker_id: &str, cap: &str, cap_ver: &str, setup_ms: i64, error: &str) -> Envelope {
    Envelope {
        proto_ver: envelope::PROTO_VER,
        msg_type: msg::CAP_READY.to_string(),
        worker_id: worker_id.to_string(),
        cap: cap.to_string(),
        cap_ver: cap_ver.to_string(),
        setup_ms,
        error: error.to_string(),
        ..Default::default()
    }
}

fn error_envelope(env: &Envelope, worker_id: &str, code: &str, message: &str) -> Envelope {
    Envelope {
        proto_ver: envelope::PROTO_VER,
        msg_type: msg::ERR.to_string(),
        corr_id: env.corr_id.clone(),
        origin_id: env.origin_id.clone(),
        worker_id: worker_id.to_string(),
        cap: env.cap.clone(),
        code: code.to_string(),
        message: message.to_string(),
        retryable: err_code::retryable(code),
        ..Default::default()
    }
}

fn heartbeat_loop(
    transport: Arc<Transport>,
    cfg: WorkerConfig,
    served: Arc<AtomicU64>,
    loaded: Arc<Mutex<HashMap<String, String>>>,
    started_at: Instant,
) {
    loop {
        thread::sleep(cfg.heartbeat);
        let caps = loaded
            .lock()
            .map(|m| m.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let hb = Envelope {
            proto_ver: envelope::PROTO_VER,
            msg_type: msg::HB_PING.to_string(),
            worker_id: cfg.worker_id.clone(),
            runtime: "rust".to_string(),
            load: 0,
            groups: cfg.groups.clone(),
            caps,
            requests_served: served.load(Ordering::Relaxed),
            uptime_ms: started_at.elapsed().as_millis() as i64,
            ..Default::default()
        };
        if send(&transport, hb).is_err() {
            return;
        }
    }
}

fn send(transport: &Transport, env: Envelope) -> Result<()> {
    let topic = env.reply_topic();
    let data = envelope::encode(&env)?;
    transport.send(&topic, &data)
}

fn json_array(items: &[String]) -> String {
    let body: Vec<String> = items.iter().map(|s| format!("\"{s}\"")).collect();
    format!("[{}]", body.join(","))
}

/// Structured log line on fd 3 when the Go runtime opened it, else stderr.
///
/// The parent pipes fd 3 into its own logger, so capability output stays
/// interleaved with the rest of the process tree.
fn log(level: &str, message: &str, extra: &str) {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let line = format!(
        "{{\"ts\":{ts_ms},\"level\":\"{level}\",\"logger\":\"pepper.rust\",\"msg\":\"{}\",\"extra\":{extra}}}",
        message.replace('"', "'")
    );
    #[cfg(unix)]
    {
        use std::os::fd::FromRawFd;
        // SAFETY: fd 3 is either open (dup'ed by the parent for logs) or closed;
        // writing to a closed fd returns EBADF, which we ignore.
        let mut f = unsafe { std::fs::File::from_raw_fd(3) };
        if writeln!(f, "{line}").is_ok() {
            std::mem::forget(f);
            return;
        }
        std::mem::forget(f);
    }
    eprintln!("[pepper/rust] {level}: {message} {extra}");
}
