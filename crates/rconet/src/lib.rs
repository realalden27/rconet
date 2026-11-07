//! Asynchronous, cancellation-safe RCON client ecosystem for Rust.
//!
//! `rconet` provides a thread-safe [`Client`] backed by a dedicated session actor that
//! preserves wire frame alignment across concurrent callers and mid-flight future
//! cancellations, while preventing accidental replay of non-idempotent server commands.
//!
//! # Quickstart
//!
//! ```no_run
//! # #[cfg(feature = "tokio-tcp")]
//! # async fn run() -> Result<(), rconet::Error> {
//! use std::time::Duration;
//! use rconet::{Client, Preset, ReconnectPolicy};
//!
//! let client = Client::builder()
//!     .address("127.0.0.1:25575")
//!     .password("s3cr3t")
//!     .preset(Preset::MinecraftJava)
//!     .command_timeout(Duration::from_secs(5))
//!     .reconnect_policy(ReconnectPolicy::exponential_default())
//!     .connect()
//!     .await?;
//!
//! let response = client.execute("list").await?;
//! println!("{}", response.body());
//!
//! client.shutdown().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

mod actor;
mod client;
mod error;
#[cfg(feature = "fleet")]
mod fleet;
mod policy;
#[cfg(feature = "pool")]
mod pool;
mod state;

pub use client::{Client, ClientBuilder, DEFAULT_QUEUE_CAPACITY, MAX_QUEUE_CAPACITY};
pub use error::Error;
#[cfg(feature = "fleet")]
pub use fleet::{
    DEFAULT_FLEET_CONCURRENCY, DEFAULT_FLEET_EVENT_CAPACITY, FleetEvent, FleetFilter, FleetManager,
    MAX_FLEET_CONCURRENCY,
};
pub use policy::{MIN_RECONNECT_DELAY, ReconnectPolicy, ReplayMode};
#[cfg(feature = "pool")]
pub use pool::{DEFAULT_MAX_IDLE_DURATION, MAX_POOL_SESSIONS, PooledClient, ServerPool};
pub use rconet_proto::{
    CommandOutput, DialectProfile, PayloadEncoding, Preset, ProtoError, TerminationMode,
};
#[cfg(feature = "webrcon")]
pub use rconet_proto::{
    DEFAULT_WEBRCON_CLIENT_NAME, WebRconEvent, WebRconMessageKind, WebRconRequest,
};
pub use rconet_transport::{CommandFailureKind, FramedConnection, TransportError};
pub use rconet_types::{
    CommandPhase, FrameLimits, Idempotency, MAX_TIMEOUT_DURATION, PacketId, SecretPassword,
    ServerId, SessionId, TimeoutConfig, TypeError,
};
pub use state::{CommandOptions, ConnectionState};

/// Low-level protocol primitives re-exported from `rconet-proto`.
pub use rconet_proto as proto;
/// Low-level transport primitives re-exported from `rconet-transport`.
pub use rconet_transport as transport;
/// Foundational types re-exported from `rconet-types`.
pub use rconet_types as types;
