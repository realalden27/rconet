//! Asynchronous framed RCON transports and deterministic test server utilities.
//!
//! `rconet-transport` bridges runtime-independent protocol state machines from
//! `rconet-proto` onto asynchronous Tokio streams with explicit read/write deadlines
//! and strict stream-taint tracking.

#![warn(missing_docs)]

mod connection;
mod error;
#[cfg(feature = "test-util")]
pub mod mock;

pub use connection::FramedConnection;
pub use error::{CommandFailureKind, TransportError};
