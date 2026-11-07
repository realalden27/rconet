use thiserror::Error;

/// Validation errors returned when constructing foundational `rconet-types` primitives.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TypeError {
    /// Client-allocated [`crate::PacketId`] must be strictly positive (`>= 1`).
    #[error("client packet identifier must be >= 1, got {0}")]
    NonPositivePacketId(i32),

    /// [`crate::ServerId`] failed length or character-set validation.
    #[error("invalid server identifier: {reason}")]
    InvalidServerId {
        /// Human-readable validation failure description.
        reason: &'static str,
    },

    /// A byte limit in [`crate::FrameLimits`] fell outside supported protocol bounds.
    #[error("invalid frame limit '{field}': {value} bytes is outside allowed bounds")]
    InvalidFrameLimit {
        /// Name of the invalid limit field.
        field: &'static str,
        /// Rejected byte count.
        value: usize,
    },

    /// A timeout duration in [`crate::TimeoutConfig`] was zero.
    #[error("timeout '{field}' must be greater than zero")]
    ZeroTimeout {
        /// Name of the zero-valued timeout field.
        field: &'static str,
    },
}
