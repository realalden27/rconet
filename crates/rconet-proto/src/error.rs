use rconet_types::PacketId;
use thiserror::Error;

/// Protocol framing, validation, and state-machine errors produced by `rconet-proto`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtoError {
    /// Wire `length` header was smaller than the minimum 10-byte RCON frame body (`4B id + 4B type + 2B NUL`).
    #[error("malformed RCON frame length {length}: minimum valid length is 10 bytes")]
    FrameTooSmall {
        /// Signed 32-bit length value read from the wire header.
        length: i32,
    },

    /// Wire `length` header exceeded the configured inbound frame byte ceiling.
    #[error("inbound RCON frame length {length} bytes exceeds limit of {max} bytes")]
    FrameExceedsLimit {
        /// Frame length advertised by the remote peer.
        length: usize,
        /// Configured maximum inbound frame size.
        max: usize,
    },

    /// Inbound frame did not end with the mandatory two NUL (`0x00, 0x00`) pad bytes.
    #[error("malformed RCON frame: missing trailing two-byte NUL terminator")]
    MissingTerminator,

    /// Outbound command contained an interior NUL (`0x00`) byte incompatible with C-string framing.
    #[error("outbound command must not contain interior NUL (0x00) bytes")]
    CommandContainsNullByte,

    /// Outbound command exceeded the dialect's maximum command payload byte length.
    #[error("command payload of {length} bytes exceeds dialect limit of {max} bytes")]
    CommandTooLong {
        /// Encoded command byte length.
        length: usize,
        /// Maximum outbound command byte length allowed by the active dialect.
        max: usize,
    },

    /// Password contained an interior NUL (`0x00`) byte incompatible with C-string framing.
    #[error("authentication credential contains an invalid interior NUL (0x00) byte")]
    PasswordContainsNullByte,

    /// Password exceeded the maximum outbound frame limit.
    #[error("authentication credential length ({length} bytes) exceeds limit of {max} bytes")]
    PasswordTooLong {
        /// Encoded credential byte length (never the credential value itself).
        length: usize,
        /// Maximum allowed byte length.
        max: usize,
    },

    /// Inbound payload contained invalid UTF-8 while `PayloadEncoding::StrictUtf8` was configured.
    #[error("response payload contained invalid UTF-8 byte sequence")]
    InvalidUtf8,

    /// Inbound payload was not valid Base64 while `PayloadEncoding::Base64Utf8` was configured.
    #[error("response payload failed Base64 decoding")]
    InvalidBase64,

    /// Server explicitly rejected authentication (`PacketId::AUTH_FAILURE` / `-1`).
    #[error("RCON authentication rejected by server")]
    AuthRejected,

    /// Server sent too many preliminary packets during authentication without completing handshake.
    #[error("authentication handshake exceeded maximum of {max} preliminary frames")]
    TooManyPreAuthFrames {
        /// Configured preliminary frame cap.
        max: usize,
    },

    /// Server returned a frame whose [`PacketId`] did not match the active exchange, indicating stream desynchronization.
    #[error("unexpected packet id {actual} while awaiting {expected}")]
    UnexpectedPacketId {
        /// Expected request identifier.
        expected: PacketId,
        /// Actual identifier received on the wire.
        actual: PacketId,
    },

    /// Server returned an unexpected packet type integer for the current protocol state.
    #[error("unexpected RCON packet type {raw_type} in current exchange state")]
    UnexpectedPacketType {
        /// Raw 32-bit packet type integer read from the frame.
        raw_type: i32,
    },

    /// Multi-packet response exceeded the configured cumulative byte limit.
    #[error("accumulated multi-packet response ({accumulated} bytes) exceeds limit of {max} bytes")]
    ResponseTooLarge {
        /// Byte count that would have been reached.
        accumulated: usize,
        /// Configured cumulative response byte ceiling.
        max: usize,
    },

    /// JSON payload in WebRCON mode failed serialization or deserialization.
    #[error("malformed WebRCON JSON message: {reason}")]
    InvalidWebRconJson {
        /// Sanitized parse error summary.
        reason: String,
    },
}
