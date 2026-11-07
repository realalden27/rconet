# Changelog

All notable changes to the `rconet` workspace are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-07

### Added

- **`rconet-types` (`L0`):**
  - `SecretPassword` zeroize-on-drop credential container with `[REDACTED]` `Debug` and `Display` implementations.
  - Typed `PacketId`, monotonic `PacketIdAllocator` (single and paired sentinel allocation without signed overflow), validated `ServerId`, and monotonic `SessionId`.
  - Bounded `FrameLimits` (`source_default` and `minecraft_java_default`) and `TimeoutConfig` (`MAX_TIMEOUT_DURATION` = `24h` ceiling).
  - Explicit `CommandPhase` (`NotSent`, `Sent`, `InFlight`, `Completed`, `OutcomeUnknown`) and `Idempotency` delivery taxonomy.
- **`rconet-proto` (`L1`):**
  - Runtime-independent `FrameCodec` with pre-allocation bounds validation (`wasm32-unknown-unknown` compatible) and `encode_sensitive` (`Zeroizing<Vec<u8>>` outbound auth frame serialization).
  - Evidence-based `DialectProfile` and `Preset` definitions (`Source`, `MinecraftJava`, `SinglePacket`, `Base64Tcp`) plus `DialectProfile::minecraft_java_legacy()`.
  - `TerminationMode::SourceSentinel` (including automatic Valve `SRCDS` `0x00010000` trailer absorption), `TerminationMode::SinglePacket`, and `TerminationMode::CoalesceWindow`.
  - `PayloadEncoding` (`LossyUtf8`, `StrictUtf8`, and optional `Base64Utf8`).
  - Runtime-independent `WebRconRequest`, `WebRconEvent`, and forward-compatible `WebRconMessageKind` JSON wire types (`feature = "webrcon"`).
- **`rconet-transport` (`L2`):**
  - Stream-generic `FramedConnection<S>` over `tokio::net::TcpStream` (`TCP_NODELAY`) or custom `AsyncRead + AsyncWrite` streams.
  - Overflow-safe connect, auth, per-frame I/O, and overall command deadlines, strict stream-taint tracking, and cancel-safe `monitor_idle()` passive `EOF`/`RST` detection.
  - Feature-gated deterministic `mock` server harness (`feature = "test-util"`).
- **`rconet` (`L3`):**
  - `Client`, `ClientBuilder`, and `Client::connect_with` backed by a cancellation-safe `SessionActor` preserving TCP frame alignment across mid-flight caller cancellations and concurrent callers.
  - Conservative `ReconnectPolicy::exponential` (`10 ms` minimum floor + `±20%` SplitMix64 jitter), `ReplayMode` (`NeverReplay` default vs `ReplayIdempotentOnly`), and `AuthRejected` circuit-breaking.
  - Optional `ServerPool` (`feature = "pool"`, `MAX_POOL_SESSIONS = 32`, configurable idle TTL eviction + instant passive `EOF` eviction) and `FleetManager` (`feature = "fleet"`, tag-filtered broadcasts with isolated partial failures and reactive `FleetEvent` streams).
