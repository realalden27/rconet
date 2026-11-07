<!-- Generated from docs/scorecard.toml via `cargo xtask docs`. Do not edit directly. -->
# `rconet` Quality Scorecard (v0.1.0)

| Capability ID | Layer | Status | Min Verified Tests | Description |
| :--- | :--- | :--- | ---: | :--- |
| `workspace.bootstrap` | `infra` | **complete** | 1 | Workspace layering, lint enforcement, and xtask verification pipeline |
| `types.foundation` | `L0` | **complete** | 10 | Redacted credentials, typed identifiers, bounded limits, and delivery phase taxonomy |
| `proto.wire_codec` | `L1` | **complete** | 22 | Bounds-checked binary framing, dialect profiles, auth & response termination state machines |
| `transport.tokio_tcp` | `L2` | **complete** | 8 | Framed async TCP/stream transport with deadlines and deterministic mock server |
| `client.session_actor` | `L3` | **complete** | 9 | Cancellation-safe session actor and conservative non-replay reconnection engine |
| `client.pool_and_fleet` | `L3` | **complete** | 5 | Bounded session pool, tagged multi-server fleet broadcast, and reactive fleet events |
| `sdk.examples_benches_docs` | `L4` | **complete** | 6 | Runnable SDK examples, Criterion benchmarks, and architecture/protocol/security guides |
