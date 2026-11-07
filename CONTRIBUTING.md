# Contributing to `rconet`

Thank you for helping improve `rconet`! Every contribution must uphold the workspace's safety, protocol accuracy, and architectural boundaries.

## Architectural & Safety Rules

1. **Zero `unsafe` & Zero Production Panics:**
   - `#![forbid(unsafe_code)]` is enforced across the workspace.
   - `unwrap()`, `expect()`, `panic!()`, `todo!()`, `unimplemented!()`, `unreachable!()`, and unchecked slice indexing (`buf[..4]`) are denied by Clippy. Use checked arithmetic and `.get(...)` slicing on all untrusted wire inputs.
2. **Strict Layer Boundaries (`L0` ➔ `L3`):**
   - `rconet-types` (`L0`) and `rconet-proto` (`L1`) must remain 100% runtime-independent (no `tokio`, OS sockets, or wall-clock dependencies) and compile cleanly on `wasm32-unknown-unknown`.
   - Verify layer rules anytime via `cargo run -p xtask -- layers`.
3. **Conservative Command Delivery:**
   - Never automatically replay a command in `CommandPhase::OutcomeUnknown` unless the caller explicitly opted into both `ReplayMode::ReplayIdempotentOnly` and `CommandOptions::idempotent()`.
4. **Credential Hygiene:**
   - Never log, trace, or embed plaintext passwords in error messages, spans, or unzeroized wire buffers.

## Local Verification Gate

Before opening a pull request, run the full verification gate from the workspace root (requires **Rust 1.88+**):

```sh
# Formatting, isolated --no-default-features tests, Clippy (-D warnings),
# all-features tests, README + rustdoc doctests, strict rustdoc, layer audit & scorecard:
cargo run -p xtask -- ci

# Adversarial panic-hunt & 2,048-case proptest suites:
cargo run -p xtask -- fuzz
```

If you modify `docs/scorecard.toml`, regenerate `docs/scorecard.md` via:

```sh
cargo run -p xtask -- docs
```

## License

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `rconet` by you shall be dual-licensed under `MIT OR Apache-2.0`, without any additional terms or conditions.
