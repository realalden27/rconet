## Summary

-

## Verification Checklist

- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes
- [ ] Deterministic unit and integration tests added for new behavior or bug fixes
- [ ] `cargo test --workspace --all-features` and `--doc` pass
- [ ] `cargo run -p xtask -- layers` and `cargo run -p xtask -- docs --check` pass
- [ ] Credentials remain redacted in `Debug`, `Display`, errors, and `tracing` spans
