//! Foundational types, redacted credentials, bounded limits, and delivery state
//! classifications for the `rconet` ecosystem.
//!
//! This crate is completely independent of asynchronous runtimes and performs no I/O.
//!
//! # Examples
//!
//! ```
//! use rconet_types::{CommandPhase, PacketIdAllocator, SecretPassword};
//!
//! let password = SecretPassword::new("s3cr3t");
//! assert_eq!(format!("{password:?}"), "SecretPassword([REDACTED])");
//!
//! let mut ids = PacketIdAllocator::new();
//! let (cmd_id, sentinel_id) = ids.next_pair();
//! assert!(cmd_id.is_positive() && sentinel_id.is_positive());
//! assert_ne!(cmd_id, sentinel_id);
//!
//! assert!(!CommandPhase::NotSent.may_have_executed());
//! assert!(CommandPhase::OutcomeUnknown.may_have_executed());
//! ```

#![warn(missing_docs)]

mod delivery;
mod error;
mod ids;
mod limits;
mod secret;

pub use delivery::{CommandPhase, Idempotency};
pub use error::TypeError;
pub use ids::{PacketId, PacketIdAllocator, ServerId, SessionId};
pub use limits::{
    DEFAULT_MAX_ACCUMULATED_RESPONSE_BYTES, DEFAULT_MAX_INBOUND_FRAME_BYTES, FrameLimits,
    HARD_MAX_ACCUMULATED_RESPONSE_BYTES, HARD_MAX_INBOUND_FRAME_BYTES, MAX_TIMEOUT_DURATION,
    MINECRAFT_JAVA_MAX_COMMAND_BYTES, SOURCE_MAX_COMMAND_BYTES, TimeoutConfig,
    WIRE_LENGTH_FIELD_BYTES, WIRE_MIN_BODY_HEADER_BYTES,
};
pub use secret::SecretPassword;
