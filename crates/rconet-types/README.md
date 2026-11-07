# rconet-types

Foundational, runtime-independent primitives for the [`rconet`](https://github.com/realalden27/rconet) asynchronous RCON ecosystem.

## Overview

- **`SecretPassword`** — `zeroize`-backed credential container with redacted `Debug` and `Display` implementations.
- **`PacketId`, `PacketIdAllocator`, `ServerId`, `SessionId`** — Validated, overflow-safe protocol and fleet identifiers.
- **`FrameLimits` & `TimeoutConfig`** — Bounded wire-frame admission limits and timeout guardrails (`1 ns..=24 h`).
- **`CommandPhase` & `Idempotency`** — Explicit wire lifecycle classification (`NotSent`, `Sent`, `InFlight`, `Completed`, `OutcomeUnknown`) preventing silent replay of mutating commands.

`rconet-types` has zero async runtime or OS networking dependencies and compiles cleanly for `wasm32-unknown-unknown`.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
