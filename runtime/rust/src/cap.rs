//! The capability-facing API: the [`Capability`] trait, [`Context`], [`Input`],
//! blobs, and the `capability!` macro that turns a source file into a worker.
//!
//! ```ignore
//! use pepper_runtime::prelude::*;
//!
//! #[derive(Default)]
//! struct Echo;
//!
//! impl Capability for Echo {
//!     fn run(&mut self, ctx: &Context, input: Input) -> Result<Value> {
//!         Ok(json!({ "msg": input.str_or("msg", ""), "worker": ctx.worker_id }))
//!     }
//! }
//!
//! capability! {
//!     name: "rust.echo",
//!     groups: ["default"],
//!     max_concurrent: 4,
//!     handler: Echo,
//! }
//! ```

use std::cell::RefCell;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::{json, Map, Value};

use crate::envelope::Result;
use crate::wire;


/// Capability configuration, as passed to [`Capability::setup`].
#[derive(Debug, Clone, Default)]
pub struct Config(pub Map<String, Value>);

impl Config {
    pub fn raw(&self) -> &Map<String, Value> {
        &self.0
    }
    pub fn str(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(Value::as_str)
    }
    pub fn i64(&self, key: &str) -> Option<i64> {
        self.0.get(key).and_then(Value::as_i64)
    }
    pub fn f64(&self, key: &str) -> Option<f64> {
        self.0.get(key).and_then(Value::as_f64)
    }
    pub fn bool(&self, key: &str) -> Option<bool> {
        self.0.get(key).and_then(Value::as_bool)
    }
}

/// Per-request context: identifiers, meta, session access and cancellation.
pub struct Context {
    pub worker_id: String,
    pub cap: String,
    pub corr_id: String,
    pub origin_id: String,
    pub hop: u8,
    pub delivery_count: u8,
    pub deadline_ms: i64,
    meta: Map<String, Value>,
    session: RefCell<Map<String, Value>>,
    updates: RefCell<Map<String, Value>>,
    cancelled: Arc<Mutex<HashSet<String>>>,
}

impl Context {
    pub(crate) fn new(env: &crate::envelope::Envelope, cancelled: Arc<Mutex<HashSet<String>>>) -> Self {
        let session = env
            .meta
            .get("_session_data")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        Self {
            worker_id: env.worker_id.clone(),
            cap: env.cap.clone(),
            corr_id: env.corr_id.clone(),
            origin_id: env.origin_id.clone(),
            hop: env.hop,
            delivery_count: env.delivery_count,
            deadline_ms: env.deadline_ms,
            meta: env.meta.clone(),
            session: RefCell::new(session),
            updates: RefCell::new(Map::new()),
            cancelled,
        }
    }

    /// Envelope meta (trace ids, blob ids, cost, …).
    pub fn meta(&self) -> &Map<String, Value> {
        &self.meta
    }

    /// Read a session value written by an earlier call in the same session.
    pub fn session_get(&self, key: &str) -> Option<Value> {
        self.session.borrow().get(key).cloned()
    }

    /// Write a session value. Updates are merged into the store by the router
    /// when the response arrives.
    pub fn session_set(&self, key: &str, value: Value) {
        self.session.borrow_mut().insert(key.to_string(), value.clone());
        self.updates.borrow_mut().insert(key.to_string(), value);
    }

    pub(crate) fn session_updates(&self) -> Map<String, Value> {
        self.updates.borrow().clone()
    }

    /// True when the router broadcast a cancel for this request.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled
            .lock()
            .map(|set| set.contains(&self.origin_id))
            .unwrap_or(false)
    }

    /// True when the request deadline has already passed.
    pub fn deadline_exceeded(&self) -> bool {
        if self.deadline_ms <= 0 {
            return false;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        now_ms > self.deadline_ms
    }
}

/// A capability input map with typed accessors.
#[derive(Debug, Clone, Default)]
pub struct Input(pub Map<String, Value>);

impl Input {
    pub fn from_value(v: Value) -> Self {
        match v {
            Value::Object(m) => Self(m),
            Value::Null => Self(Map::new()),
            other => {
                let mut m = Map::new();
                m.insert("value".to_string(), other);
                Self(m)
            }
        }
    }

    pub fn raw(&self) -> &Map<String, Value> {
        &self.0
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    pub fn str(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(Value::as_str)
    }

    pub fn str_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.str(key).unwrap_or(default)
    }

    pub fn i64(&self, key: &str) -> Option<i64> {
        self.0.get(key).and_then(Value::as_i64)
    }

    pub fn f64(&self, key: &str) -> Option<f64> {
        self.0.get(key).and_then(Value::as_f64)
    }

    pub fn bool(&self, key: &str) -> Option<bool> {
        self.0.get(key).and_then(Value::as_bool)
    }

    pub fn array(&self, key: &str) -> Option<&Vec<Value>> {
        self.0.get(key).and_then(Value::as_array)
    }

    /// Read `key` as a zero-copy-capable blob reference.
    pub fn blob(&self, key: &str) -> Option<BlobRef> {
        self.0.get(key).and_then(BlobRef::from_value)
    }
}

/// A blob reference: a file the router and the worker both can reach.
///
/// `as_bytes` copies; `map` memory-maps the file and `as_f32` wraps that in a
/// `Deref<Target = [f32]>` guard so tensors never leave shared memory.
#[derive(Debug, Clone)]
pub struct BlobRef {
    pub id: String,
    pub path: PathBuf,
    pub size: u64,
    pub dtype: String,
    pub shape: Vec<usize>,
    pub format: String,
}

impl BlobRef {
    pub fn from_value(v: &Value) -> Option<Self> {
        let m = v.as_object()?;
        if !m.get("_pepper_blob").and_then(Value::as_bool).unwrap_or(false) {
            return None;
        }
        Some(Self {
            id: m.get("id").and_then(Value::as_str).unwrap_or("").to_string(),
            path: PathBuf::from(m.get("path").and_then(Value::as_str).unwrap_or("")),
            size: m.get("size").and_then(Value::as_u64).unwrap_or(0),
            dtype: m.get("dtype").and_then(Value::as_str).unwrap_or("").to_string(),
            shape: m
                .get("shape")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_u64).map(|n| n as usize).collect())
                .unwrap_or_default(),
            format: m.get("format").and_then(Value::as_str).unwrap_or("").to_string(),
        })
    }

    /// Read the blob into memory (one copy).
    pub fn as_bytes(&self) -> Result<Vec<u8>> {
        Ok(std::fs::read(&self.path)?)
    }

    /// Memory-map the blob. Zero copy.
    pub fn map(&self) -> Result<memmap2::Mmap> {
        let file = std::fs::File::open(&self.path)?;
        Ok(unsafe { memmap2::Mmap::map(&file)? })
    }

    /// View a float32 tensor without copying, when `dtype` says `float32`.
    pub fn as_f32(&self) -> Result<F32Blob> {
        if !self.dtype.is_empty() && self.dtype != "float32" {
            return Err(crate::envelope::Error::Capability(format!(
                "blob dtype is {:?}, not float32",
                self.dtype
            )));
        }
        let mmap = self.map()?;
        let len = mmap.len() / std::mem::size_of::<f32>();
        Ok(F32Blob { mmap, len })
    }

    /// Element count implied by `shape` (0 when shape is absent).
    pub fn numel(&self) -> usize {
        self.shape.iter().product()
    }
}

/// A memory-mapped `float32` tensor. Deref to `&[f32]`.
pub struct F32Blob {
    mmap: memmap2::Mmap,
    len: usize,
}

impl std::ops::Deref for F32Blob {
    type Target = [f32];
    fn deref(&self) -> &[f32] {
        // mmap is page-aligned, so the f32 cast is aligned; bounds come from
        // the mapping length.
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr() as *const f32, self.len) }
    }
}
/// Write `data` into the worker's blob directory and return the reference map to
/// hand back to Go.
///
/// The router keeps the file alive until the request deadline and deletes it when
/// the response is consumed; `pepper.WithBlobDir` controls the location.
pub fn new_blob(data: &[u8]) -> Result<Value> {
    let dir = blob_dir();
    std::fs::create_dir_all(&dir)?;
    let id = blob_id();
    let path = dir.join(format!("pepper_blob_{id}"));
    std::fs::write(&path, data)?;
    Ok(json!({
        "_pepper_blob": true,
        "id": id,
        "path": path.to_string_lossy(),
        "size": data.len(),
        "dtype": "",
        "shape": Vec::<usize>::new(),
        "format": "",
    }))
}

/// Like [`new_blob`] but records tensor metadata so Python and Go can memory-map
/// the result without guessing dtype/shape.
pub fn new_tensor(data: &[u8], dtype: &str, shape: &[usize]) -> Result<Value> {
    let mut v = new_blob(data)?;
    if let Value::Object(m) = &mut v {
        m.insert("dtype".to_string(), json!(dtype));
        m.insert("shape".to_string(), json!(shape));
    }
    Ok(v)
}

fn blob_dir() -> PathBuf {
    if let Ok(dir) = std::env::var(wire::env::BLOB_DIR) {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    let base = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(base).join("pepper")
}

fn blob_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!(
        "{:012x}{:04x}",
        nanos,
        COUNTER.fetch_add(1, Ordering::Relaxed) & 0xffff
    )
}

/// One capability: `setup` once per worker, `run` once per request.
pub trait Capability: Send {
    fn setup(&mut self, _config: &Config) -> Result<()> {
        Ok(())
    }
    fn run(&mut self, ctx: &Context, input: Input) -> Result<Value>;
    fn teardown(&mut self) {}
}

/// Everything the runtime needs in order to serve one capability.
pub struct CapabilitySpec {
    pub name: String,
    pub version: String,
    pub groups: Vec<String>,
    pub max_concurrent: usize,
    pub description: String,
    pub factory: Box<dyn Fn() -> Box<dyn Capability> + Send + Sync>,
}

impl CapabilitySpec {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            version: "0.0.0".to_string(),
            groups: vec![wire::GROUP_DEFAULT.to_string()],
            max_concurrent: 4,
            description: String::new(),
            factory: Box::new(|| Box::new(NoopCapability)),
        }
    }

    pub fn version(mut self, v: &str) -> Self {
        self.version = v.to_string();
        self
    }

    pub fn groups(mut self, groups: &[&str]) -> Self {
        if !groups.is_empty() {
            self.groups = groups.iter().map(|s| s.to_string()).collect();
        }
        self
    }

    pub fn max_concurrent(mut self, n: usize) -> Self {
        if n > 0 {
            self.max_concurrent = n;
        }
        self
    }

    pub fn description(mut self, d: &str) -> Self {
        self.description = d.to_string();
        self
    }

    /// Install the handler type. It must be `Default` so the runtime can
    /// construct it before `setup` runs.
    pub fn handler<H: Capability + Default + 'static>(mut self) -> Self {
        self.factory = Box::new(|| Box::new(H::default()));
        self
    }
}

struct NoopCapability;

impl Capability for NoopCapability {
    fn run(&mut self, _ctx: &Context, _input: Input) -> Result<Value> {
        Err(crate::envelope::Error::Config(
            "no handler installed for this capability".to_string(),
        ))
    }
}

/// Declare the capability a source file serves.
///
/// Expands to `pub fn __pepper_capability()`, which the generated worker binary
/// calls. Only one `capability!` per file.
#[macro_export]
macro_rules! capability {
    (
        name: $name:expr
        $(, version: $version:expr)?
        $(, groups: $groups:expr)?
        $(, max_concurrent: $max:expr)?
        $(, description: $desc:expr)?
        , handler: $handler:ty $(,)?
    ) => {
        pub fn __pepper_capability() -> $crate::CapabilitySpec {
            $crate::CapabilitySpec::new($name)
                $(.version($version))?
                $(.groups(&$groups))?
                $(.max_concurrent($max))?
                $(.description($desc))?
                .handler::<$handler>()
        }
    };
}
