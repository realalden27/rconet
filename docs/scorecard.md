<!-- Generated from docs/scorecard.toml via `cargo xtask docs`. Do not edit directly. -->
# `rconet` Quality Scorecard (v0.1.0)

| Capability ID | Layer | Status | Min Verified Tests | Description |
| :--- | :--- | :--- | ---: | :--- |
| `workspace.bootstrap` | `infra` | **complete** | 1 | Workspace layering, lint enforcement, and xtask verification pipeline |
| `types.foundation` | `L0` | **complete** | 10 | Redacted credentials, typed identifiers, bounded limits, and delivery phase taxonomy |
| `proto.wire_codec` | `L1` | **complete** | 26 | Bounds-checked binary framing, zeroized auth buffers, dialect profiles, wire goldens, and state machines |
| `transport.tokio_tcp` | `L2` | **complete** | 11 | Framed async TCP/stream transport with checked deadlines, idle EOF monitor, and test-util mock server |
| `client.session_actor` | `L3` | **complete** | 11 | Cancellation-safe session actor, idle disconnect detection, and conservative non-replay reconnection |
| `client.pool_and_fleet` | `L3` | **complete** | 8 | Bounded session pool with idle TTL eviction, tagged fleet broadcast, and reactive fleet events |
| `sdk.examples_benches_docs` | `L4` | **complete** | 6 | Runnable SDK examples, Criterion benchmarks, and architecture/protocol/security guides |
