use std::io;
use std::time::Duration;

use rconet_proto::ProtoError;
use rconet_types::CommandPhase;
use thiserror::Error;

/// Specific underlying cause of a failed command exchange on a framed transport.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CommandFailureKind {
    /// Request failed local protocol validation before any bytes were written.
    #[error(transparent)]
    Validation(ProtoError),

    /// Transport had already been marked unusable by an earlier framing or I/O failure.
    #[error("transport session is no longer usable after an earlier stream error")]
    SessionTainted,

    /// Socket write failed after initiation; partial bytes may have been transmitted.
    #[error("socket write failed: {0}")]
    WriteIo(#[source] io::Error),

    /// Socket write stalled past the configured frame I/O timeout.
    #[error("socket write timed out after {timeout:?}")]
    WriteTimeout {
        /// Configured write deadline that elapsed.
        timeout: Duration,
    },

    /// Remote server closed the stream before completing the command response.
    #[error("connection closed by remote server while awaiting response")]
    UnexpectedEof,

    /// Socket read failed while awaiting response frames.
    #[error("socket read failed: {0}")]
    ReadIo(#[source] io::Error),

    /// Individual frame read stalled past the configured frame I/O timeout.
    #[error("frame read timed out after {timeout:?}")]
    FrameTimeout {
        /// Configured per-frame read deadline that elapsed.
        timeout: Duration,
    },

    /// Total command exchange exceeded its end-to-end deadline.
    #[error("command execution timed out after {timeout:?}")]
    DeadlineExceeded {
        /// Configured end-to-end command deadline that elapsed.
        timeout: Duration,
    },

    /// Server sent a malformed frame or violated protocol state invariants.
    #[error(transparent)]
    Protocol(ProtoError),
}

/// Asynchronous transport and handshake errors produced by `rconet-transport`.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum TransportError {
    /// Establishing the underlying network connection timed out.
    #[error("connection attempt timed out after {timeout:?}")]
    ConnectTimeout {
        /// Configured connection timeout.
        timeout: Duration,
    },

    /// Operating system socket connection or configuration error.
    #[error("connection I/O error: {0}")]
    ConnectIo(#[source] io::Error),

    /// Authentication handshake exceeded `auth_timeout`.
    #[error("authentication handshake timed out after {timeout:?}")]
    AuthTimeout {
        /// Configured authentication timeout.
        timeout: Duration,
    },

    /// Remote server closed the socket during authentication (common on wrong password or IP ban).
    #[error("remote server closed connection during authentication handshake")]
    AuthClosedByPeer,

    /// Authentication failed due to explicit server rejection or protocol error.
    #[error("authentication failed: {0}")]
    AuthProtocol(#[source] ProtoError),

    /// Command execution failed at a recorded [`CommandPhase`].
    #[error("command failed during {phase:?}: {kind}")]
    Command {
        /// Exact wire lifecycle phase reached when failure occurred.
        phase: CommandPhase,
        /// Underlying failure cause.
        #[source]
        kind: CommandFailureKind,
    },
}

impl TransportError {
    /// Returns the [`CommandPhase`] associated with this error if it occurred during command execution.
    #[must_use]
    pub const fn command_phase(&self) -> Option<CommandPhase> {
        match self {
            Self::Command { phase, .. } => Some(*phase),
            _ => None,
        }
    }

    /// Returns `true` when the error explicitly indicates rejected credentials (`PacketId == -1`).
    #[must_use]
    pub const fn is_auth_rejected(&self) -> bool {
        matches!(
            self,
            Self::AuthProtocol(ProtoError::AuthRejected)
                | Self::Command {
                    kind: CommandFailureKind::Protocol(ProtoError::AuthRejected),
                    ..
                }
        )
    }
}
