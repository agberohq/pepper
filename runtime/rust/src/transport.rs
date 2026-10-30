//! Bus transport.
//!
//! Only the Mula backend (raw TCP, `tcp://host:port`) is implemented here: it is
//! the single-node default and the transport the router always offers to local
//! worker processes. Redis and NATS coverage lives in the Python runtime today;
//! `pepper-runtime` reports a clear error instead of silently misbehaving.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::Duration;

use crate::envelope::{Error, Result};

/// A connected Mula socket.
///
/// Reads and writes use separate duplicated handles: `recv` blocks for as long as
/// the router is quiet, and it must never hold the lock the heartbeat and
/// response paths need.
pub struct Transport {
    read: Mutex<TcpStream>,
    write: Mutex<TcpStream>,
}

/// Parse `tcp://host:port` (or a bare `host:port`) into its parts.
pub fn parse_url(url: &str) -> Result<(String, u16)> {
    let rest = match url.split_once("://") {
        Some((scheme, rest)) => match scheme.to_ascii_lowercase().as_str() {
            "tcp" | "mula" => rest,
            other => {
                return Err(Error::Config(format!(
                    "unsupported bus scheme {other:?} — the Rust runtime supports tcp:// only"
                )))
            }
        },
        None => url,
    };
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or_else(|| Error::Config(format!("bus url {url:?} has no port")))?;
    let port: u16 = port
        .parse()
        .map_err(|_| Error::Config(format!("bus url {url:?} has an invalid port")))?;
    let host = if host.is_empty() { "127.0.0.1" } else { host };
    Ok((host.to_string(), port))
}

impl Transport {
    /// Connect with the same backoff the Python runtime uses.
    pub fn connect(url: &str, retries: u32) -> Result<Self> {
        let (host, port) = parse_url(url)?;
        let mut last_err = None;
        for attempt in 0..retries {
            let addr = format!("{host}:{port}");
            match TcpStream::connect(&addr) {
                Ok(stream) => {
                    stream.set_nodelay(true).ok();
                    let writer = stream.try_clone()?;
                    return Ok(Self {
                        read: Mutex::new(stream),
                        write: Mutex::new(writer),
                    });
                }
                Err(e) => {
                    last_err = Some(e);
                    let backoff = std::cmp::min(100 * (1u64 << attempt), 4000);
                    std::thread::sleep(Duration::from_millis(backoff));
                }
            }
        }
        Err(Error::Io(last_err.unwrap_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "connect failed")
        })))
    }

    /// Subscribe to a list of topics.
    ///
    /// Mula expects the topic list as the first frame after connecting, as a
    /// `|`-separated string with no topic header.
    pub fn subscribe(&self, topics: &[String]) -> Result<()> {
        self.send_raw(topics.join("|").as_bytes())
    }

    /// Send raw bytes with the 4-byte big-endian length prefix.
    pub fn send_raw(&self, data: &[u8]) -> Result<()> {
        let mut conn = self.write.lock().expect("transport poisoned");
        let mut out = Vec::with_capacity(4 + data.len());
        out.extend_from_slice(&(data.len() as u32).to_be_bytes());
        out.extend_from_slice(data);
        conn.write_all(&out)?;
        conn.flush()?;
        Ok(())
    }

    /// Send one framed message on `topic`.
    pub fn send(&self, topic: &str, payload: &[u8]) -> Result<()> {
        self.send_raw(&crate::envelope::frame(topic, payload))
    }

    /// Block for the next frame. Returns `None` when the router closed the
    /// connection.
    pub fn recv(&self) -> Result<Option<Vec<u8>>> {
        let mut conn = self.read.lock().expect("transport poisoned");
        let mut len_buf = [0u8; 4];
        if let Err(e) = read_exact(&mut *conn, &mut len_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(Error::Io(e));
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > 64 * 1024 * 1024 {
            return Err(Error::Config(format!("invalid frame length {len}")));
        }
        let mut buf = vec![0u8; len];
        if let Err(e) = read_exact(&mut *conn, &mut buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(Error::Io(e));
        }
        Ok(Some(buf))
    }
}

fn read_exact<R: Read>(r: &mut R, buf: &mut [u8]) -> std::io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                ))
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
