use rconet_types::PacketId;
use serde::{Deserialize, Serialize};

use crate::error::ProtoError;

/// Default client tag sent in outbound JSON WebRCON commands.
pub const DEFAULT_WEBRCON_CLIENT_NAME: &str = "rconet";

/// Outbound JSON command frame used by WebSocket RCON servers.
///
/// Kept inside `rconet-proto` so JSON wire semantics remain completely independent
/// of any asynchronous WebSocket transport library.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebRconRequest {
    /// Positive client correlation identifier echoed in the server's reply.
    #[serde(rename = "Identifier")]
    pub identifier: i32,
    /// Console command text to execute.
    #[serde(rename = "Message")]
    pub message: String,
    /// Logical sender tag expected by WebRCON servers.
    #[serde(rename = "Name")]
    pub name: String,
}

impl WebRconRequest {
    /// Constructs an outbound [`WebRconRequest`] from a validated [`PacketId`].
    #[must_use]
    pub fn new(id: PacketId, command: impl Into<String>) -> Self {
        Self {
            identifier: id.as_i32(),
            message: command.into(),
            name: DEFAULT_WEBRCON_CLIENT_NAME.to_string(),
        }
    }

    /// Serializes this request into a JSON string suitable for a WebSocket text frame.
    pub fn to_json(&self) -> Result<String, ProtoError> {
        serde_json::to_string(self).map_err(|err| ProtoError::InvalidWebRconJson {
            reason: err.to_string(),
        })
    }
}

/// Forward-compatible classification of inbound WebRCON message categories.
#[non_exhaustive]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WebRconMessageKind {
    /// Generic server output or command reply.
    #[default]
    Generic,
    /// Informational console log message.
    Log,
    /// Server warning message.
    Warning,
    /// Server error or exception message.
    Error,
    /// Chat broadcast message.
    Chat,
    /// Report or telemetry message.
    Report,
    /// Unrecognized category emitted by a newer or modded server.
    #[serde(other)]
    Unknown,
}

/// Inbound JSON message decoded from a WebSocket RCON server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebRconEvent {
    /// Correlation identifier (`> 0` for command replies, `<= 0` for unsolicited broadcasts).
    #[serde(rename = "Identifier", default)]
    pub identifier: i32,
    /// Console text body.
    #[serde(rename = "Message", default)]
    pub message: String,
    /// Message category tag.
    #[serde(rename = "Type", default)]
    pub kind: WebRconMessageKind,
    /// Optional server stacktrace string on errors.
    #[serde(rename = "Stacktrace", default)]
    pub stacktrace: String,
}

impl WebRconEvent {
    /// Parses an inbound JSON text payload with bounded size validation.
    pub fn from_json(raw: &str, max_bytes: usize) -> Result<Self, ProtoError> {
        if raw.len() > max_bytes {
            return Err(ProtoError::FrameExceedsLimit {
                length: raw.len(),
                max: max_bytes,
            });
        }
        serde_json::from_str(raw).map_err(|err| ProtoError::InvalidWebRconJson {
            reason: err.to_string(),
        })
    }

    /// Returns the correlation [`PacketId`].
    #[must_use]
    pub const fn packet_id(&self) -> PacketId {
        PacketId::from_raw(self.identifier)
    }

    /// Returns `true` when `identifier <= 0`, indicating an unsolicited server broadcast
    /// (such as live chat or console log output) rather than a command reply.
    #[must_use]
    pub const fn is_unsolicited_broadcast(&self) -> bool {
        self.identifier <= 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_command_reply_and_unsolicited_unknown_category() {
        let reply_json =
            r#"{"Identifier":42,"Message":"hostname: test","Type":"Generic","Stacktrace":""}"#;
        let reply = WebRconEvent::from_json(reply_json, 4096);
        assert!(reply.is_ok());
        let ev = reply.unwrap_or_else(|_| WebRconEvent {
            identifier: 0,
            message: String::new(),
            kind: WebRconMessageKind::Unknown,
            stacktrace: String::new(),
        });
        assert_eq!(ev.packet_id(), PacketId::from_raw(42));
        assert!(!ev.is_unsolicited_broadcast());
        assert_eq!(ev.kind, WebRconMessageKind::Generic);

        // Forward-compatible decoding when server introduces a brand new Type variant and omits Stacktrace:
        let broadcast_json =
            r#"{"Identifier":0,"Message":"[Chat] hello","Type":"FutureModdedCategory"}"#;
        let bcast = WebRconEvent::from_json(broadcast_json, 4096);
        assert!(bcast.is_ok());
        let bev = bcast.unwrap_or(ev);
        assert!(bev.is_unsolicited_broadcast());
        assert_eq!(bev.kind, WebRconMessageKind::Unknown);
    }

    #[test]
    fn rejects_oversized_json_before_parsing() {
        let huge = "a".repeat(2048);
        assert!(matches!(
            WebRconEvent::from_json(&huge, 1024),
            Err(ProtoError::FrameExceedsLimit { .. })
        ));
    }
}
