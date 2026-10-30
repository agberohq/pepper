// Example Rust capability: uppercase a string and echo the request context.
//
// Run it through Pepper like any other capability:
//
//     pp.Register(pepper.Script("rust.echo", "./caps/echo.rs"))
//
// pepper compiles this file once (cached by content hash) and serves it from the
// worker that owns the group; a worker can host several .rs files at once.

use pepper_runtime::prelude::*;

#[derive(Default)]
struct Echo;

impl Capability for Echo {
    fn run(&mut self, ctx: &Context, input: Input) -> Result<Value> {
        let msg = input.str_or("msg", "");
        Ok(json!({
            "msg": msg,
            "upper": msg.to_uppercase(),
            "length": msg.chars().count(),
            "worker": ctx.worker_id,
            "cap": ctx.cap,
            "hop": ctx.hop,
        }))
    }
}

capability! {
    name: "rust.echo",
    version: "0.1.0",
    groups: ["default"],
    max_concurrent: 4,
    description: "Uppercase a string (Rust runtime smoke test)",
    handler: Echo,
}
