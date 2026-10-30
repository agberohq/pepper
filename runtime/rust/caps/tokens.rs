// Streaming example: emit tokens as they are produced.
//
// A capability overrides `run_stream` and calls `emit` for every chunk. The
// runtime wraps each value in a res_chunk envelope and finishes with res_end, so
// Go callers can use `pepper.Call[Chunk]{...}.Stream(ctx)` or OpenStream.
//
// Input chunks (bidirectional streaming) arrive through `ctx.next_chunk()`; this
// example ignores them and only produces output.

use pepper_runtime::prelude::*;

#[derive(Default)]
struct Tokens;

impl Capability for Tokens {
    fn run(&self, _ctx: &Context, input: Input) -> Result<Value> {
        // Non-streaming callers get the whole answer at once.
        Ok(json!({ "text": input.str_or("prompt", "").to_uppercase() }))
    }

    fn run_stream(
        &self,
        _ctx: &Context,
        input: Input,
        emit: &mut dyn FnMut(Value) -> Result<()>,
    ) -> Result<()> {
        let prompt = input.str_or("prompt", "pepper");
        for (i, word) in prompt.split_whitespace().enumerate() {
            emit(json!({ "index": i, "token": word }))?;
        }
        Ok(())
    }
}

capability! {
    name: "rust.tokens",
    version: "0.1.0",
    groups: ["default"],
    description: "Streams words of the prompt as chunks (Rust streaming example)",
    handler: Tokens,
}
