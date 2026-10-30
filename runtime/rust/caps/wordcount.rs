// Second example capability, served by the SAME worker as echo.rs.
//
// It also shows the `// pepper:deps` header: anything listed there is added to
// the generated Cargo.toml, so a capability can pull in its own crates without the
// Go caller doing anything. Names/versions are merged with the runtime's defaults.

// pepper:deps = serde_json@1

use pepper_runtime::prelude::*;

#[derive(Default)]
struct WordCount;

impl Capability for WordCount {
    fn run(&mut self, _ctx: &Context, input: Input) -> Result<Value> {
        let text = input.str_or("text", "");
        let words = text.split_whitespace().count();
        let longest = text.split_whitespace().max_by_key(|w| w.len()).unwrap_or("");
        Ok(json!({
            "words": words,
            "longest": longest,
            "chars": text.chars().count(),
            "worker": _ctx.worker_id,
        }))
    }
}

capability! {
    name: "rust.wordcount",
    version: "0.1.0",
    groups: ["default"],
    description: "Word statistics (Rust runtime multi-capability example)",
    handler: WordCount,
}
