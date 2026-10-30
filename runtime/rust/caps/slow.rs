// A capability that blocks, used to prove requests run in parallel.
//
// `run(&self)` gives the runtime real concurrency against one handler instance;
// `max_concurrent` (PEPPER_MAX_CONCURRENT) bounds how many run at once.

use pepper_runtime::prelude::*;

#[derive(Default)]
struct Slow;

impl Capability for Slow {
    fn run(&self, _ctx: &Context, input: Input) -> Result<Value> {
        let ms = input.i64("sleep_ms").unwrap_or(100).clamp(0, 10_000) as u64;
        std::thread::sleep(std::time::Duration::from_millis(ms));
        Ok(json!({ "slept_ms": ms }))
    }
}

capability! {
    name: "rust.slow",
    version: "0.1.0",
    groups: ["default"],
    max_concurrent: 4,
    description: "Sleeps for sleep_ms (parallelism probe)",
    handler: Slow,
}
