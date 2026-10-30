//! The Pepper envelope and framing.
//!
//! Every message on the bus is an [`Envelope`] encoded with msgpack as a map of
//! named fields — never a positional array — because the Python, Julia and Node
//! runtimes all decode into plain dictionaries and read fields by name.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Wire protocol version. Must match `envelope.ProtoVer` on the Go side.
pub const PROTO_VER: u8 = 1;

/// Message types, mirroring `internal/envelope/const.go`.
pub mod msg {
    pub const REQ: &str = "req";
    pub const RES: &str = "res";
    pub const RES_CHUNK: &str = "res_chunk";
    pub const RES_END: &str = "res_end";
    pub const ERR: &str = "err";
    pub const PIPE: &str = "pipe";
    pub const CANCEL: &str = "cancel";
    pub const HB_PING: &str = "hb_ping";
    pub const WORKER_HELLO: &str = "worker_hello";
    pub const WORKER_BYE: &str = "worker_bye";
    pub const CAP_LOAD: &str = "cap_load";
    pub const CAP_READY: &str = "cap_ready";
    pub const STREAM_OPEN: &str = "stream_open";
    pub const STREAM_CHUNK: &str = "stream_chunk";
    pub const STREAM_CLOSE: &str = "stream_close";
    pub const AUTH_CHALLENGE: &str = "auth_challenge";
    pub const AUTH_RESPONSE: &str = "auth_response";
}

/// Error codes the router understands, mirroring `envelope.Code`.
pub mod err_code {
    pub const EXEC_ERROR: &str = "EXEC_ERROR";
    pub const CAP_NOT_FOUND: &str = "CAP_NOT_FOUND";
    pub const DEADLINE_EXCEEDED: &str = "DEADLINE_EXCEEDED";
    pub const CANCELLED: &str = "CANCELLED";
    pub const PAYLOAD_INVALID: &str = "PAYLOAD_INVALID";
    pub const CODEC_MISMATCH: &str = "CODEC_MISMATCH";

    /// Codes the router may safely retry.
    pub fn retryable(code: &str) -> bool {
        matches!(
            code,
            "DEADLINE_EXCEEDED"
                | "WORKER_OOM"
                | "NO_WORKERS"
                | "WORKER_SHUTTING_DOWN"
                | "DISPATCH_TIMEOUT"
                | "MODEL_OOM"
                | "MODEL_TIMEOUT"
        )
    }
}

/// Go encodes empty slices and maps as msgpack nil, not as empty containers.
/// `#[serde(default)]` only covers absent fields, so every container field goes
/// through these helpers to accept nil as "empty".
mod nullable {
    use serde::{Deserialize, Deserializer};
    use serde_json::{Map, Value};

    pub fn vec<'de, D, T>(d: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Ok(Option::<Vec<T>>::deserialize(d)?.unwrap_or_default())
    }

    pub fn map<'de, D>(d: D) -> Result<Map<String, Value>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Option::<Map<String, Value>>::deserialize(d)?.unwrap_or_default())
    }
}

/// One decoded bus message.
///
/// Only the fields this runtime reads or writes are modelled; everything else on
/// the wire is ignored, and unset fields are omitted when replying so the
/// envelope stays small.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(default)]
    pub proto_ver: u8,
    #[serde(default)]
    pub msg_type: String,
    #[serde(default)]
    pub corr_id: String,
    #[serde(default)]
    pub origin_id: String,
    #[serde(default)]
    pub worker_id: String,
    #[serde(default)]
    pub runtime: String,
    #[serde(default)]
    pub cap: String,
    #[serde(default)]
    pub cap_ver: String,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub dispatch: String,
    #[serde(default)]
    pub quorum: u8,
    #[serde(default)]
    pub hop: u8,
    #[serde(default)]
    pub delivery_count: u8,
    #[serde(default)]
    pub deadline_ms: i64,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub forward_to: String,
    #[serde(default)]
    pub stream_id: String,
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub codec: String,
    #[serde(default)]
    pub setup_ms: i64,
    #[serde(default)]
    pub load: u8,
    #[serde(default)]
    pub pid: u32,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub cb_supported: bool,
    #[serde(default)]
    pub stream_supported: bool,
    #[serde(default)]
    pub requests_served: u64,
    #[serde(default)]
    pub uptime_ms: i64,
    #[serde(default, deserialize_with = "nullable::vec")]
    pub groups: Vec<String>,
    #[serde(default, deserialize_with = "nullable::vec")]
    pub caps: Vec<String>,
    #[serde(default, deserialize_with = "nullable::vec")]
    pub pipe_subs: Vec<String>,
    #[serde(default, deserialize_with = "nullable::vec")]
    pub pipe_pubs: Vec<String>,
    #[serde(default, deserialize_with = "nullable::map")]
    pub meta: Map<String, Value>,
    /// cap_load carries the capability config, source and limits at top level.
    #[serde(default, skip_serializing_if = "Map::is_empty", deserialize_with = "nullable::map")]
    pub config: Map<String, Value>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    #[serde(default, deserialize_with = "nullable::vec")]
    pub deps: Vec<String>,
    #[serde(default)]
    pub timeout_ms: i64,
    #[serde(default)]
    pub max_concurrent: u32,
    /// Payload bytes. `serde_bytes` keeps this as msgpack `bin`, not an array of
    /// integers — the Go and Python codecs rely on that distinction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_bytes::ByteBuf>,
    /// Ed25519 public key for worker authentication.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub public_key: Vec<u8>,
}

impl Envelope {
    pub fn new(msg_type: &str) -> Self {
        Self {
            proto_ver: PROTO_VER,
            msg_type: msg_type.to_string(),
            ..Default::default()
        }
    }

    /// Topic this envelope must be published on.
    pub fn reply_topic(&self) -> String {
        match self.msg_type.as_str() {
            msg::RES | msg::ERR | msg::RES_CHUNK | msg::RES_END => {
                crate::wire::topic_res(&self.origin_id)
            }
            msg::HB_PING => crate::wire::topic_hb(&self.worker_id),
            msg::PIPE => self.topic.clone(),
            _ => crate::wire::TOPIC_CONTROL.to_string(),
        }
    }

    pub fn payload_bytes(&self) -> &[u8] {
        self.payload.as_ref().map(|b| &b[..]).unwrap_or(&[]) 
    }
}

/// Encode an envelope to msgpack (named-field map form).
pub fn encode(env: &Envelope) -> Result<Vec<u8>> {
    rmp_serde::to_vec_named(env).map_err(|e| Error::Codec(e.to_string()))
}

/// Decode an envelope from msgpack.
pub fn decode(data: &[u8]) -> Result<Envelope> {
    rmp_serde::from_slice(data).map_err(|e| Error::Codec(e.to_string()))
}

/// Frame a message for the bus: `[2-byte topic len][topic][msgpack envelope]`.
/// The 4-byte length prefix is applied by the transport, which owns the socket.
pub fn frame(topic: &str, payload: &[u8]) -> Vec<u8> {
    let topic_bytes = topic.as_bytes();
    let mut out = Vec::with_capacity(2 + topic_bytes.len() + payload.len());
    let len = topic_bytes.len() as u16;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(topic_bytes);
    out.extend_from_slice(payload);
    out
}

/// Split a received frame into `(topic, payload)`.
pub fn unframe(data: &[u8]) -> (&str, &[u8]) {
    if data.len() < 2 {
        return ("", &[]);
    }
    let topic_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    if data.len() < 2 + topic_len {
        return ("", &[]);
    }
    let topic = std::str::from_utf8(&data[2..2 + topic_len]).unwrap_or("");
    (topic, &data[2 + topic_len..])
}

/// Errors surfaced by the runtime.
#[derive(Debug)]
pub enum Error {
    /// Anything the capability author raised, or the runtime could not complete.
    Capability(String),
    /// Transport failure.
    Io(std::io::Error),
    /// Envelope or payload could not be encoded/decoded.
    Codec(String),
    /// Environment or configuration problem.
    Config(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Capability(m) => write!(f, "{m}"),
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Codec(e) => write!(f, "codec: {e}"),
            Error::Config(m) => write!(f, "config: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Capability(format!("json: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
