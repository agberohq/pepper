//! Streaming and concurrency plumbing.
//!
//! - [`StreamState`] holds the input chunks of one open stream and lets a
//!   capability pull them (`Context::next_chunk`) while the reader thread keeps
//!   receiving.
//! - [`Semaphore`] bounds how many requests execute at once, which is what
//!   `PEPPER_MAX_CONCURRENT` means for this runtime.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use serde_json::Value;

/// Input side of a bidirectional stream.
pub struct StreamState {
    buf: Mutex<Buf>,
    cv: Condvar,
}

struct Buf {
    chunks: VecDeque<Value>,
    closed: bool,
}

impl StreamState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            buf: Mutex::new(Buf {
                chunks: VecDeque::new(),
                closed: false,
            }),
            cv: Condvar::new(),
        })
    }

    /// Queue one input chunk (called from the reader thread).
    pub fn push(&self, chunk: Value) {
        if let Ok(mut g) = self.buf.lock() {
            g.chunks.push_back(chunk);
            self.cv.notify_all();
        }
    }

    /// No more input will arrive.
    pub fn close(&self) {
        if let Ok(mut g) = self.buf.lock() {
            g.closed = true;
            self.cv.notify_all();
        }
    }

    /// Block until a chunk arrives or the stream closes. `timeout == None` blocks
    /// indefinitely, which is what a capability usually wants.
    pub fn next(&self, timeout: Option<Duration>) -> Option<Value> {
        let mut guard = self.buf.lock().ok()?;
        loop {
            if let Some(chunk) = guard.chunks.pop_front() {
                return Some(chunk);
            }
            if guard.closed {
                return None;
            }
            guard = match timeout {
                Some(d) => {
                    let (next, res) = self.cv.wait_timeout(guard, d).ok()?;
                    if res.timed_out() && next.chunks.is_empty() {
                        return None;
                    }
                    next
                }
                None => self.cv.wait(guard).ok()?,
            };
        }
    }
}

/// Counting semaphore, so `max_concurrent` actually bounds execution.
pub struct Semaphore {
    permits: Mutex<usize>,
    cv: Condvar,
}

impl Semaphore {
    pub fn new(n: usize) -> Arc<Self> {
        Arc::new(Self {
            permits: Mutex::new(n.max(1)),
            cv: Condvar::new(),
        })
    }

    /// Block until a permit is free. The permit is released on drop.
    pub fn acquire(self: &Arc<Self>) -> Permit {
        if let Ok(mut g) = self.permits.lock() {
            while *g == 0 {
                g = match self.cv.wait(g) {
                    Ok(g) => g,
                    Err(_) => return Permit { sem: Arc::clone(self) },
                };
            }
            *g -= 1;
        }
        Permit { sem: Arc::clone(self) }
    }
}

pub struct Permit {
    sem: Arc<Semaphore>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Ok(mut g) = self.sem.permits.lock() {
            *g += 1;
            self.sem.cv.notify_one();
        }
    }
}
