# `rconet-proto`

Runtime-independent RCON wire framing, dialect profiles, and deterministic state machines for the [`rconet`](https://crates.io/crates/rconet) ecosystem.

## Highlights

- **Zero Async Runtime Dependencies:** Pure in-memory `BytesMut` codecs and state machines suitable for Tokio, custom event loops, or `wasm32-unknown-unknown`.
- **Verified Server Dialects:** Built-in profiles for Valve `SRCDS` (`Preset::Source`), `MinecraftJava` (`1446`-byte command cap), `SinglePacket`, `Base64Tcp` (`feature = "base64"`), and `TerminationMode::CoalesceWindow`.
- **Zeroizing Auth Wire Encoding:** Outbound `SERVERDATA_AUTH` frames are serialized directly into `Zeroizing<Vec<u8>>` buffers so plaintext passwords are wiped on drop.
- **WebRCON JSON Types (`feature = "webrcon"`):** Forward-compatible JSON request/event framing for WebSocket RCON servers.

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or [MIT license](../../LICENSE-MIT) at your option.
