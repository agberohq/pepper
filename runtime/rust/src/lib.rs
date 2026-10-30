//! Pepper worker runtime for Rust.
//!
//! A capability is a struct implementing [`Capability`], declared with the
//! [`capability!`](crate::capability) macro. `pepper.Script("name", "./caps/x.rs")`
//! on the Go side compiles that file once (cached) and runs it as a worker
//! process speaking the same envelope protocol as the Python, Julia and Node
//! runtimes.
//!
//! ```ignore
//! use pepper_runtime::prelude::*;
//!
//! #[derive(Default)]
//! struct Echo;
//!
//! impl Capability for Echo {
//!     fn run(&mut self, _ctx: &Context, input: Input) -> Result<Value> {
//!         Ok(json!({ "upper": input.str_or("msg", "").to_uppercase() }))
//!     }
//! }
//!
//! capability! {
//!     name: "rust.echo",
//!     groups: ["default"],
//!     handler: Echo,
//! }
//! ```

pub mod cap;
pub mod envelope;
pub mod transport;
pub mod wire;
pub mod worker;

pub use cap::{new_blob, new_tensor, BlobRef, Capability, CapabilitySpec, Config, Context, Input};
pub use serde_json::json;
pub use envelope::{Envelope, Error, Result};
pub use worker::{serve, WorkerConfig};

/// Everything a capability file normally needs.
pub mod prelude {
    pub use crate::cap::{
        new_blob, new_tensor, BlobRef, Capability, CapabilitySpec, Config, Context, Input,
    };
    pub use crate::capability;
    pub use crate::envelope::{err_code, msg, Error, Result};
    pub use serde_json::{json, Map, Value};
}

#[cfg(test)]
mod tests;
