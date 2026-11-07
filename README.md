# rconet

Asynchronous, cancellation-safe Remote Console (RCON) client ecosystem for Rust.

## Crate Layout

| Crate | Layer | Responsibility |
| :--- | :---: | :--- |
| `rconet-types` | `L0` | Redacted credentials, typed identifiers, bounded configuration limits, and command delivery phases |
| `rconet-proto` | `L1` | Runtime-independent binary & JSON wire framing, dialect profiles, and protocol state machines |
| `rconet-transport` | `L2` | Asynchronous Tokio framed transports, deadline enforcement, and deterministic mock test server |
| `rconet` | `L3` | Public client API, cancellation-safe session actor, conservative reconnection, pooling, and fleet management |

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
