# `rconet-transport`

Asynchronous framed RCON transports and deterministic test utilities for the [`rconet`](https://crates.io/crates/rconet) ecosystem.

## Features

- `tokio-tcp` *(default)*: Enables `FramedConnection::connect_tcp` (`TCP_NODELAY`, bounded connect/auth/frame/command deadlines).
- `test-util`: Enables the deterministic `mock` server harness (`MockRconServer`, `serve_duplex_stream`) for integration testing without polluting production builds.

With `--no-default-features`, `FramedConnection<S>` operates over any `tokio::io::AsyncRead + AsyncWrite + Unpin` byte stream (such as TLS wrappers, SSH tunnels, or `tokio::io::DuplexStream`).

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or [MIT license](../../LICENSE-MIT) at your option.
