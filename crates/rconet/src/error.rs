use std::time::Duration;

use rconet_proto::ProtoError;
use rconet_transport::{CommandFailureKind, TransportError};
use rconet_types::{CommandPhase, TypeError};
use thiserror::Error;

/// Primary error type returned by `rconet` client, pool, and fleet operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid configuration limit, identifier, or timeout parameter.
    #[error(transparent)]
    Config(#[from] TypeError),

    /// Missing or invalid server address configuration.
    #[error("invalid server address: {reason}")]
    InvalidAddress {
        /// Reason the address was rejected.
        reason: &'static str,
    },

    /// Server explicitly rejected authentication credentials (`PacketId == -1`).
    #[error("RCON authentication rejected by server")]
    AuthRejected,

    /// Command failed local protocol validation before touching the transport (`NotSent`).
    #[error("invalid command request: {0}")]
    Validation(#[source] ProtoError),

    /// Command timed out at a recorded [`CommandPhase`].
    #[error("command timed out after {timeout:?} (phase: {phase:?})")]
    CommandTimeout {
        /// Lifecycle phase reached when the deadline elapsed.
        phase: CommandPhase,
        /// Configured deadline duration.
        timeout: Duration,
    },

    /// Underlying connection failed during handshake, reconnection, or command execution.
    #[error("transport failure (phase: {phase:?}): {source}")]
    Transport {
        /// Command lifecycle phase, if the failure occurred during command execution.
        phase: Option<CommandPhase>,
        /// Underlying transport error.
        #[source]
        source: Box<TransportError>,
    },

    /// Bounded session request queue is full.
    #[error("client command queue is full (capacity: {capacity})")]
    QueueFull {
        /// Configured bounded channel capacity.
        capacity: usize,
    },

    /// Session actor has shut down or permanently closed after an unrecoverable error.
    #[error("RCON client session is closed")]
    ClientClosed,

    /// Reconnection exhausted its configured maximum attempts without restoring connectivity.
    #[error("reconnection failed after {attempts} attempts: {last_error}")]
    ReconnectExhausted {
        /// Number of reconnection attempts made.
        attempts: u32,
        /// Error returned by the final reconnection attempt.
        #[source]
        last_error: Box<TransportError>,
    },

    /// Acquiring a healthy session from a [`crate::ServerPool`] timed out.
    #[cfg(feature = "pool")]
    #[error("timed out after {timeout:?} waiting for an available pooled RCON session")]
    PoolAcquireTimeout {
        /// Configured pool checkout timeout.
        timeout: Duration,
    },

    /// Referenced [`rconet_types::ServerId`] is not registered in the fleet manager.
    #[cfg(feature = "fleet")]
    #[error("unknown server id '{server_id}'")]
    UnknownServer {
        /// Unregistered server identifier.
        server_id: rconet_types::ServerId,
    },
}

impl Error {
    /// Converts a [`TransportError`] into a structured client [`Self`] preserving [`CommandPhase`].
    #[must_use]
    pub fn from_transport(err: TransportError) -> Self {
        if err.is_auth_rejected() {
            return Self::AuthRejected;
        }

        match err {
            TransportError::Command {
                phase: CommandPhase::NotSent,
                kind: CommandFailureKind::Validation(proto_err),
            } => Self::Validation(proto_err),
            TransportError::Command {
                phase,
                kind:
                    CommandFailureKind::DeadlineExceeded { timeout }
                    | CommandFailureKind::FrameTimeout { timeout }
                    | CommandFailureKind::WriteTimeout { timeout },
            } => Self::CommandTimeout { phase, timeout },
            other => {
                let phase = other.command_phase();
                Self::Transport {
                    phase,
                    source: Box::new(other),
                }
            },
        }
    }

    /// Returns the [`CommandPhase`] reached when this error occurred, if applicable.
    #[must_use]
    pub const fn command_phase(&self) -> Option<CommandPhase> {
        match self {
            Self::Validation(_)
            | Self::QueueFull { .. }
            | Self::ClientClosed
            | Self::ReconnectExhausted { .. } => Some(CommandPhase::NotSent),
            Self::CommandTimeout { phase, .. } => Some(*phase),
            Self::Transport { phase, .. } => *phase,
            _ => None,
        }
    }

    /// Returns `true` if command bytes may have reached the remote server prior to failure.
    #[must_use]
    pub const fn may_have_executed(&self) -> bool {
        match self.command_phase() {
            Some(phase) => phase.may_have_executed(),
            None => false,
        }
    }

    /// Returns `true` when authentication credentials were explicitly rejected (`PacketId == -1`).
    #[must_use]
    pub const fn is_auth_rejected(&self) -> bool {
        matches!(self, Self::AuthRejected)
    }
}
