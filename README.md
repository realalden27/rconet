# rconet

Asynchronous, cancellation-safe Remote Console (RCON) client ecosystem for Rust.

`rconet` provides a layered, production-focused foundation for communicating with, monitoring, and orchestrating game servers over RCON without frame desynchronization, panic-prone wire parsing, or accidental replay of mutating server commands.

---

## Core Guarantees

- **Zero-Panic Wire Parsing (`#![forbid(unsafe_code)]`)** — Every inbound header and payload is treated as untrusted. Frame sizes are validated with checked arithmetic against `FrameLimits` *before* allocating buffer capacity, preventing integer underflows, slice panics, and oversized frame memory exhaustion.
- **Cancellation-Safe Session Actor** — TCP RCON is strictly half-duplex per connection. `Client` multiplexes concurrent callers onto a dedicated background session actor that retains ownership of in-flight wire exchanges if a caller drops its future, draining remaining frames within protocol deadlines so the socket stays aligned.
- **Conservative Reconnection & Non-Replay** — Every failure reports its exact `CommandPhase` (`NotSent`, `Sent`, `InFlight`, `Completed`, or `OutcomeUnknown`). Automatic reconnection heals dropped sessions without silently re-sending commands in `OutcomeUnknown` unless explicitly configured via `ReplayMode::ReplayIdempotentOnly` and `CommandOptions::idempotent()`.
- **Evidence-Based Dialect Profiles** — Built-in presets handle Source multi-packet sentinel framing (including SRCDS `0x00010000` trailer absorption), Minecraft Java's `1446`-byte command ceiling and single-packet behavior, Base64 payload encoding, and runtime-independent WebRCON JSON message correlation.
- **Redacted Credential Hygiene** — Passwords are stored in `SecretPassword` (`zeroize`-backed memory clearing on drop, `[REDACTED]` `Debug`/`Display` formatting, and never emitted in `tracing` spans or error payloads).

---

## Workspace Architecture

| Crate | Layer | Runtime Dependency | Responsibility |
| :--- | :---: | :---: | :--- |
| `rconet-types` | `L0` | None (`wasm32` clean) | `SecretPassword`, `PacketId`, `ServerId`, `SessionId`, `FrameLimits`, `TimeoutConfig`, `CommandPhase`, `Idempotency` |
| `rconet-proto` | `L1` | None (`wasm32` clean) | Binary `FrameCodec`, `DialectProfile` & `Preset`, `ProtocolState` auth/command state machines, `PayloadEncoding`, `WebRcon*` JSON types |
| `rconet-transport` | `L2` | Tokio | `FramedConnection<S>` over TCP or custom `AsyncRead + AsyncWrite` streams, frame/command deadlines, deterministic `MockRconServer` |
| `rconet` | `L3` | Tokio | `Client`, `ClientBuilder`, cancellation-safe session actor, `ReconnectPolicy`, `ServerPool`, `FleetManager` |

---

## Feature Flags (`rconet`)

| Feature | Default | Description |
| :--- | :---: | :--- |
| `tokio-tcp` | Yes | Enables `ClientBuilder::connect()` and `ServerPool::new()` over `tokio::net::TcpStream` |
| `base64` | Yes | Enables `PayloadEncoding::Base64Utf8` and `Preset::Base64Tcp` |
| `pool` | No | Enables bounded multi-session `ServerPool` and RAII `PooledClient` leases |
| `fleet` | No | Enables `FleetManager`, tagged multi-server broadcasts, and reactive `FleetEvent` streams |
| `webrcon` | No | Enables `WebRconRequest`, `WebRconEvent`, and `WebRconMessageKind` JSON wire types |

---

## Quickstart

```rust
use std::time::Duration;
use rconet::{Client, Preset, ReconnectPolicy};

#[tokio::main]
async fn main() -> Result<(), rconet::Error> {
    let client = Client::builder()
        .address("127.0.0.1:25575")
        .password("s3cr3t")
        .preset(Preset::MinecraftJava)
        .command_timeout(Duration::from_secs(5))
        .reconnect_policy(ReconnectPolicy::exponential_default())
        .connect()
        .await?;

    // `Client` is cheaply cloneable (`Clone + Send + Sync`) across tasks.
    let response = client.execute("list").await?;
    println!("{}", response.body());

    client.shutdown().await?;
    Ok(())
}
```

### Selecting a Protocol Preset

| Preset | Max Command Bytes | Termination Strategy | Typical Target |
| :--- | ---: | :--- | :--- |
| `Preset::Source` | `4096` | `SourceSentinel` (multi-packet deterministic echo) | Valve / Source Dedicated Server (`SRCDS`) |
| `Preset::MinecraftJava` | `1446` | `SinglePacket` (avoids `"Unknown request 0"` warnings) | Minecraft Java Edition `1.14.3+` |
| `Preset::SinglePacket` | `4096` | `SinglePacket` | Single-frame TCP RCON servers |
| `Preset::Base64Tcp` | `4096` | `SinglePacket` + `Base64Utf8` | Servers with broken multi-byte UTF-8 parsers |

For legacy Minecraft Java `< 1.14.3` servers requiring inter-command spacing, pass `.dialect(DialectProfile::minecraft_java_legacy())`. If you query commands returning `> 4096` bytes on Minecraft Java and accept the server's sentinel warning packet, customize `.dialect(DialectProfile::from(Preset::MinecraftJava).with_termination(TerminationMode::SourceSentinel))`.

---

## Connection Pooling & Multi-Server Fleets

Enable `features = ["pool", "fleet"]` on `rconet`:

```rust
use std::time::Duration;
use rconet::{
    Client, CommandOptions, FleetFilter, FleetManager, Preset, ServerId, ServerPool,
};

# async fn demo() -> Result<(), rconet::Error> {
// 1. Parallelize slow queries to a single server across up to 4 warm TCP sessions:
let builder = Client::builder()
    .address("127.0.0.1:27015")
    .password("admin-pw")
    .preset(Preset::Source);
let pool = ServerPool::new(builder, 4, Duration::from_secs(3))?;
let status = pool.execute("status").await?;

// 2. Orchestrate a heterogeneous fleet with isolated partial-failure broadcasts:
let fleet = FleetManager::with_max_concurrency(16);
let client = Client::builder()
    .address("127.0.0.1:25575")
    .password("mc-pw")
    .preset(Preset::MinecraftJava)
    .connect()
    .await?;

fleet.register(ServerId::new("eu-survival-1")?, client, ["eu", "survival"]).await;

let results = fleet
    .broadcast(
        &FleetFilter::WithTag("survival".to_string()),
        "list",
        CommandOptions::new().idempotent(),
    )
    .await;
# let _ = (status, results);
# Ok(())
# }
```

---

## Runnable Examples

Every example runs out of the box against an in-process deterministic mock server (or against a live server when `RCON_ADDR` and `RCON_PASSWORD` are set):

```sh
cargo run -p rconet-examples --example quickstart
cargo run -p rconet-examples --example minecraft
cargo run -p rconet-examples --example source_engine
cargo run -p rconet-examples --example server_pool
cargo run -p rconet-examples --example multi_server_fleet
cargo run -p rconet-examples --example webrcon_events
```

---

## Security Notice

Standard TCP RCON transmits passwords and console commands **in cleartext** without cryptographic integrity protection. Never expose raw RCON ports directly to the public internet; bind RCON listeners to `127.0.0.1` or a private management VLAN and tunnel traffic over **SSH port forwarding**, **WireGuard**, or an authenticated private overlay network. See [`docs/security.md`](docs/security.md) for the complete threat model.

---

## Verification & Development

```sh
# Format, minimal-feature check, clippy (-D warnings), tests, doctests, layer audit & scorecard:
cargo run -p xtask -- ci

# Run adversarial panic-hunt and extended property suites:
cargo run -p xtask -- fuzz

# Run Criterion benchmarks:
cargo bench -p rconet-benches
```

Additional engineering documentation:
- [Architecture & Cancellation Model](docs/architecture.md)
- [Protocol Specifications & Dialect Matrix](docs/protocols.md)
- [Security Threat Model & Deployment Hardening](docs/security.md)
- [Quality Scorecard](docs/scorecard.md)

---

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
