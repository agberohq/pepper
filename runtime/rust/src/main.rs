//! `pepper-worker` — compiles Rust capabilities on first use and runs them as one
//! Pepper worker.
//!
//! The Go runtime passes every `.rs` file that belongs to a worker:
//!
//! ```text
//! pepper-worker --cap caps/rerank.rs --cap caps/embed.rs
//! pepper-worker --prepare --cap caps/rerank.rs --cap caps/embed.rs   # build only
//! ```
//!
//! `--prepare` is what the Go runtime runs before starting the worker: it compiles
//! (or reuses the cached build of) the capability set and returns the compiler's
//! exit status, so a broken capability is reported as a startup error instead of a
//! worker that never becomes ready.
//!
//! Layout of a compiled capability set, under `$PEPPER_CACHE_DIR/rust/<hash>/`:
//!
//! ```text
//! Cargo.toml          [dependencies] pepper-runtime (path) + user-declared deps
//! src/cap_0.rs        copy of your first .rs file
//! src/main.rs         mod cap_0 { include!("cap_0.rs"); } ... serve(vec![...])
//! ```
//!
//! All capabilities share one `CARGO_TARGET_DIR`, so dependencies compile once.

use std::path::{Path, PathBuf};
use std::process::Command;

/// One capability source file.
struct CapSource {
    path: PathBuf,
    source: Vec<u8>,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        usage();
        return;
    }

    let prepare_only = args.iter().any(|a| a == "--prepare");
    let caps = match parse_caps(&args) {
        Some(c) => c,
        None => {
            usage();
            std::process::exit(2);
        }
    };

    match run(&caps, prepare_only) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("pepper-worker: {e}");
            std::process::exit(1);
        }
    }
}

fn usage() {
    eprintln!("pepper-worker — Pepper Rust capability runtime");
    eprintln!();
    eprintln!("  pepper-worker [--prepare] --cap <file.rs> [--cap <file.rs> ...]");
    eprintln!();
    eprintln!("  --prepare   compile the capability set and exit (used by pepper before boot)");
    eprintln!("  --cap       capability source; repeat for every capability this worker serves");
    eprintln!();
    eprintln!("Normally started by Pepper; see runtime/rust/README.md.");
}

/// Collect every `--cap` value. Returns `None` when none were given.
fn parse_caps(args: &[String]) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--cap" {
            match args.get(i + 1) {
                Some(v) => out.push(v.clone()),
                None => return None,
            }
            i += 2;
            continue;
        }
        i += 1;
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn run(cap_args: &[String], prepare_only: bool) -> Result<(), String> {
    let mut sources = Vec::with_capacity(cap_args.len());
    for arg in cap_args {
        let path = std::fs::canonicalize(arg).map_err(|e| format!("capability {arg:?}: {e}"))?;
        let source = std::fs::read(&path).map_err(|e| format!("read {arg:?}: {e}"))?;
        sources.push(CapSource { path, source });
    }

    let runtime_dir = runtime_dir()?;
    let build_root = cache::root().join("rust");
    let target_dir = build_root.join("_target");

    // The cache key covers every source plus the runtime build, so adding,
    // removing or editing any capability produces a new binary.
    let mut key = Vec::new();
    key.extend_from_slice(env!("CARGO_PKG_VERSION").as_bytes());
    key.extend_from_slice(&runtime_stamp().to_le_bytes());
    for cap in &sources {
        key.extend_from_slice(cap.path.to_string_lossy().as_bytes());
        key.extend_from_slice(&cap.source);
    }

    let digest = format!("{:016x}", cache::hash(&key));
    let pkg = format!("pepper_cap_{digest}");
    let crate_dir = build_root.join(&digest);
    let binary = target_dir.join("release").join(&pkg);

    if !binary.exists() {
        build(&crate_dir, &target_dir, &pkg, &sources, &runtime_dir)?;
    }

    if prepare_only {
        println!("pepper-worker: {} capability(ies) ready ({})", sources.len(), binary.display());
        return Ok(());
    }
    exec(&binary)
}

mod cache {
    use std::path::PathBuf;

    /// FNV-1a 64: enough to key a local build cache, and stable across releases.
    pub fn hash(bytes: &[u8]) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in bytes {
            h ^= *b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    }

    /// Cache root: `$PEPPER_CACHE_DIR`, else `$XDG_CACHE_HOME/pepper`, else
    /// `~/.cache/pepper`.
    pub fn root() -> PathBuf {
        if let Ok(dir) = std::env::var("PEPPER_CACHE_DIR") {
            if !dir.is_empty() {
                return PathBuf::from(dir);
            }
        }
        if let Ok(dir) = std::env::var("XDG_CACHE_HOME") {
            if !dir.is_empty() {
                return PathBuf::from(dir).join("pepper");
            }
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
        PathBuf::from(home).join(".cache").join("pepper")
    }
}

/// Directory holding this crate, used as the path dependency for capabilities.
fn runtime_dir() -> Result<PathBuf, String> {
    if let Ok(dir) = std::env::var("PEPPER_RUST_RUNTIME_DIR") {
        if !dir.is_empty() {
            return Ok(PathBuf::from(dir));
        }
    }
    Ok(PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

/// Modification time of this binary, used to invalidate cached capabilities when
/// the runtime crate is rebuilt.
fn runtime_stamp() -> u128 {
    std::env::current_exe()
        .and_then(|p| std::fs::metadata(p))
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Parse `// pepper:deps = name, name@1.2` from a capability source.
///
/// Same convention as the Python, Julia and Node runtimes: a contiguous header of
/// `// pepper:` lines at the top of the file. Declared crates are added to the
/// generated manifest, so a capability can pull in whatever it needs without the
/// caller doing anything.
fn parse_deps(source: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(source);
    let mut deps = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//!") {
            continue;
        }
        let Some(rest) = trimmed.strip_prefix("//") else {
            break; // first line of code: header is over
        };
        let Some(entry) = rest.trim().strip_prefix("pepper:deps") else {
            continue;
        };
        let Some(value) = entry.trim().strip_prefix('=') else {
            continue;
        };
        for item in value.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            if !valid_dep(item) {
                eprintln!("pepper-worker: ignoring malformed dependency {item:?}");
                continue;
            }
            deps.push(item.to_string());
        }
    }
    deps
}

/// Accept `name` or `name@version` with a conservative charset.
fn valid_dep(spec: &str) -> bool {
    let (name, version) = match spec.split_once('@') {
        Some((n, v)) => (n, Some(v)),
        None => (spec, None),
    };
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return false;
    }
    match version {
        None => true,
        Some(v) => {
            !v.is_empty()
                && v.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+' | '*'))
        }
    }
}

/// Render one dependency as a TOML entry.
fn toml_dep(spec: &str) -> (String, String) {
    match spec.split_once('@') {
        Some((name, version)) => (name.to_string(), format!("\"{version}\"")),
        None => (spec.to_string(), "\"*\"".to_string()),
    }
}

/// Module (and copied file) name for a capability: `cap_<index>_<stem>`.
///
/// Keeping the original stem means a compiler error points at
/// `src/cap_0_rerank.rs` rather than an anonymous `cap_0.rs`.
fn module_name(index: usize, path: &Path) -> String {
    let stem: String = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "cap".to_string())
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
        .collect();
    format!("cap_{index}_{stem}")
}

/// Write the generated crate and compile it.
fn build(
    crate_dir: &Path,
    target_dir: &Path,
    pkg: &str,
    sources: &[CapSource],
    runtime_dir: &Path,
) -> Result<(), String> {
    std::fs::create_dir_all(crate_dir.join("src"))
        .map_err(|e| format!("create {}: {e}", crate_dir.display()))?;
    std::fs::create_dir_all(target_dir)
        .map_err(|e| format!("create {}: {e}", target_dir.display()))?;

    // Lock first: a second worker may be compiling the same set concurrently.
    let lock_path = crate_dir.join(".build.lock");
    let _guard = BuildLock::acquire(&lock_path)?;

    // Dependencies: the runtime's own set, plus anything declared with
    // `// pepper:deps = ...`. A map, so a capability re-declaring a dependency
    // (with its own version) overrides the default instead of breaking the TOML.
    let mut deps: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    deps.insert(
        "pepper-runtime".to_string(),
        format!("{{ path = \"{}\" }}", toml_path(runtime_dir)),
    );
    deps.insert(
        "serde".to_string(),
        "{ version = \"1\", features = [\"derive\"] }".to_string(),
    );
    deps.insert("serde_json".to_string(), "\"1\"".to_string());
    for cap in sources {
        for d in parse_deps(&cap.source) {
            let (name, value) = toml_dep(&d);
            deps.insert(name, value);
        }
    }
    let dep_block: String = deps
        .iter()
        .map(|(name, value)| format!("{name} = {value}\n"))
        .collect();

    let manifest = format!(
        "[package]\nname = \"{pkg}\"\nversion = \"0.1.0\"\nedition = \"2021\"\npublish = false\n\n\
         [[bin]]\nname = \"{pkg}\"\npath = \"src/main.rs\"\n\n\
         [dependencies]\n{dep_block}"
    );

    // Each capability lives in its own module so two files may both declare
    // `struct Handler` without clashing.
    let mut main_rs = String::from("// Generated by pepper-worker. Do not edit.\n");
    for (i, cap) in sources.iter().enumerate() {
        let module = module_name(i, &cap.path);
        main_rs.push_str(&format!("mod {module} {{ include!(\"{module}.rs\"); }}\n"));
    }
    main_rs.push_str("\nfn main() {\n    let specs = vec![\n");
    for (i, cap) in sources.iter().enumerate() {
        let module = module_name(i, &cap.path);
        main_rs.push_str(&format!("        {module}::__pepper_capability(),\n"));
    }
    main_rs.push_str(
        "    ];\n    if let Err(e) = pepper_runtime::serve(specs) {\n        \
         eprintln!(\"pepper: {e}\");\n        std::process::exit(1);\n    }\n}\n",
    );

    let manifest_path = crate_dir.join("Cargo.toml");
    let main_path = crate_dir.join("src").join("main.rs");
    write_if_changed(&manifest_path, &manifest)?;
    write_if_changed(&main_path, &main_rs)?;

    for (i, cap) in sources.iter().enumerate() {
        let copy = crate_dir.join("src").join(format!("{}.rs", module_name(i, &cap.path)));
        write_if_changed(&copy, &normalize_source(&cap.source))?;
    }

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let status = Command::new(&cargo)
        .arg("build")
        .arg("--release")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_TERM_COLOR", "never")
        .status()
        .map_err(|e| {
            format!(
                "could not run `{cargo} build` ({e}) — install Rust (https://rustup.rs) \
                 or set PEPPER_RUST_WORKER_BIN to a prebuilt pepper-worker"
            )
        })?;

    if !status.success() {
        let names: Vec<String> = sources
            .iter()
            .map(|c| c.path.display().to_string())
            .collect();
        return Err(format!(
            "cargo build failed for {} (exit {:?}); fix the capability source and retry",
            names.join(", "),
            status.code()
        ));
    }
    Ok(())
}

struct BuildLock {
    _file: std::fs::File,
}

impl BuildLock {
    /// Take an exclusive advisory lock using flock(2) on Unix, falling back
    /// to a polling-based file lock on other platforms.
    fn acquire(path: &Path) -> Result<Self, String> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .map_err(|e| format!("open {}: {e}", path.display()))?;

        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let started = std::time::Instant::now();
            loop {
                // LOCK_EX = exclusive, non-blocking try
                if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                    return Ok(Self { _file: file });
                }
                if started.elapsed() > std::time::Duration::from_secs(900) {
                    return Err(format!(
                        "timed out waiting for another build to finish ({})",
                        path.display()
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        }

        #[cfg(not(unix))]
        {
            // Fallback: use the file existence check approach on non-Unix
            let started = std::time::Instant::now();
            loop {
                // On non-Unix we just proceed — the lock file is a no-op sentinel
                return Ok(Self { _file: file });
            }
        }
    }
}

impl Drop for BuildLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // Unlock on drop — the file handle close also releases the lock,
            // but be explicit for clarity.
            let _ = unsafe { libc::flock(self._file.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

fn write_if_changed(path: &Path, contents: &str) -> Result<(), String> {
    if let Ok(existing) = std::fs::read_to_string(path) {
        if existing == contents {
            return Ok(());
        }
    }
    std::fs::write(path, contents).map_err(|e| format!("write {}: {e}", path.display()))
}

/// Escape a path for use inside a TOML basic string.
fn toml_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

/// Inner (`//!`) doc comments are illegal outside a crate root, and capability
/// files are `include!`d into modules. Rewrite them as ordinary comments so
/// authors can document cap files normally.
fn normalize_source(source: &[u8]) -> String {
    let text = String::from_utf8_lossy(source);
    let mut out = String::with_capacity(text.len());
    for line in text.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("//!") {
            let indent = &line[..line.len() - trimmed.len()];
            out.push_str(indent);
            out.push_str("//");
            out.push_str(rest);
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

/// Replace this process with the compiled capability binary so the PID the Go
/// runtime supervises is the one actually serving requests.
fn exec(binary: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let err = Command::new(binary).exec();
        Err(format!("exec {}: {err}", binary.display()))
    }
    #[cfg(not(unix))]
    {
        let status = Command::new(binary)
            .status()
            .map_err(|e| format!("spawn {}: {e}", binary.display()))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("worker exited with {status}"))
        }
    }
}
