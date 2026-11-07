use std::time::Duration;

use bytes::BytesMut;
use rconet_types::{PacketId, PacketIdAllocator, SecretPassword};
use zeroize::Zeroizing;

use crate::dialect::{DialectProfile, TerminationMode};
use crate::error::ProtoError;
use crate::frame::{FrameCodec, PacketKind, RawFrame};

/// Trailing 4-byte payload emitted by classic Valve SRCDS in a second `RESPONSE_VALUE`
/// packet immediately behind an empty sentinel acknowledgment.
const SRCDS_SENTINEL_TRAILER: &[u8] = &[0x00, 0x01, 0x00, 0x00];

/// Session-scoped protocol state tracking [`PacketId`] allocation and optional trailing
/// sentinel acknowledgments across consecutive commands on a single stream.
#[derive(Debug, Clone)]
pub struct ProtocolState {
    dialect: DialectProfile,
    codec: FrameCodec,
    id_allocator: PacketIdAllocator,
    pending_sentinel_trailer: Option<PacketId>,
}

impl ProtocolState {
    /// Creates a fresh protocol state for a newly established transport stream.
    #[must_use]
    pub fn new(dialect: DialectProfile) -> Self {
        Self {
            dialect,
            codec: FrameCodec::new(dialect.limits()),
            id_allocator: PacketIdAllocator::new(),
            pending_sentinel_trailer: None,
        }
    }

    /// Returns the active [`DialectProfile`].
    #[must_use]
    pub const fn dialect(&self) -> DialectProfile {
        self.dialect
    }

    /// Returns the [`FrameCodec`] configured with the dialect's frame limits.
    #[must_use]
    pub const fn codec(&self) -> FrameCodec {
        self.codec
    }

    /// Begins an authentication handshake, returning the serialized outbound wire bytes
    /// in a [`Zeroizing`] buffer alongside the [`AuthExchange`] state machine so plaintext
    /// password bytes are wiped on drop immediately after socket transmission.
    pub fn begin_auth(
        &mut self,
        password: &SecretPassword,
    ) -> Result<(AuthExchange, Zeroizing<Vec<u8>>), ProtoError> {
        self.pending_sentinel_trailer = None;
        let auth_id = self.id_allocator.next_id();
        let max_pw_bytes = self
            .dialect
            .limits()
            .max_inbound_frame_bytes()
            .saturating_sub(rconet_types::WIRE_MIN_BODY_HEADER_BYTES);

        let encoded_pw = self
            .dialect
            .auth_encoding()
            .encode_password(password.expose_secret(), max_pw_bytes)?;

        let wire = self
            .codec
            .encode_sensitive(auth_id, PacketKind::Auth, &encoded_pw)?;

        let exchange = AuthExchange {
            expected_id: auth_id,
            pre_auth_frames_seen: 0,
            max_pre_auth_frames: self.dialect.max_pre_auth_frames(),
        };
        Ok((exchange, wire))
    }

    /// Begins a command execution exchange, encoding the command frame (plus the paired
    /// sentinel frame when [`TerminationMode::SourceSentinel`] is active) into a single buffer.
    pub fn begin_command(
        &mut self,
        command: &str,
    ) -> Result<(CommandExchange, BytesMut), ProtoError> {
        let encoded_cmd = self
            .dialect
            .encoding()
            .encode_command(command, self.dialect.limits().max_command_bytes())?;

        let mut wire = BytesMut::new();
        let (command_id, sentinel_id) = match self.dialect.termination() {
            TerminationMode::SourceSentinel => {
                let (cmd_id, sen_id) = self.id_allocator.next_pair();
                self.codec
                    .encode(&RawFrame::exec_command(cmd_id, encoded_cmd), &mut wire)?;
                self.codec
                    .encode(&RawFrame::response_value(sen_id, &[][..]), &mut wire)?;
                (cmd_id, Some(sen_id))
            },
            TerminationMode::SinglePacket | TerminationMode::CoalesceWindow { .. } => {
                let cmd_id = self.id_allocator.next_id();
                self.codec
                    .encode(&RawFrame::exec_command(cmd_id, encoded_cmd), &mut wire)?;
                (cmd_id, None)
            },
        };

        let exchange = CommandExchange {
            dialect: self.dialect,
            command_id,
            sentinel_id,
            fragments: 0,
            accumulated: Vec::new(),
        };
        Ok((exchange, wire))
    }

    /// Advances an active [`CommandExchange`] with a newly decoded inbound [`RawFrame`].
    pub fn advance_command(
        &mut self,
        exchange: &mut CommandExchange,
        frame: RawFrame,
    ) -> Result<CommandStep, ProtoError> {
        // If classic SRCDS emitted its optional second sentinel trailer (`00 01 00 00`)
        // from the immediately preceding command, absorb that exact single frame cleanly.
        if let Some(prev_sentinel) = self.pending_sentinel_trailer
            && frame.id == prev_sentinel
            && frame.kind == PacketKind::ResponseValue
            && frame.payload.as_ref() == SRCDS_SENTINEL_TRAILER
        {
            self.pending_sentinel_trailer = None;
            return Ok(CommandStep::NeedMoreFrames);
        }
        // Any non-trailer arrival clears stale trailer expectation so old IDs cannot match later.
        self.pending_sentinel_trailer = None;

        exchange.on_frame(frame, &mut self.pending_sentinel_trailer)
    }

    /// Checks whether `frame` is the optional trailing SRCDS sentinel packet left in a read buffer
    /// immediately after command completion, consuming it if matched.
    pub fn try_absorb_immediate_trailer(&mut self, frame: &RawFrame) -> bool {
        if let Some(prev_sentinel) = self.pending_sentinel_trailer
            && frame.id == prev_sentinel
            && frame.kind == PacketKind::ResponseValue
            && frame.payload.as_ref() == SRCDS_SENTINEL_TRAILER
        {
            self.pending_sentinel_trailer = None;
            return true;
        }
        false
    }

    /// Finalizes a [`CommandExchange`] operating in [`TerminationMode::CoalesceWindow`]
    /// once the transport's post-fragment idle window has elapsed without further frames.
    pub fn finish_coalesced(
        &self,
        exchange: &mut CommandExchange,
    ) -> Result<CommandOutput, ProtoError> {
        exchange.finish_coalesced()
    }
}

/// Step outcome returned while driving an [`AuthExchange`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthStep {
    /// Preliminary packet processed (such as SRCDS's empty `RESPONSE_VALUE`); read another frame.
    NeedMoreFrames,
    /// Server confirmed authentication with the expected positive [`PacketId`].
    Authenticated,
}

/// Pure state machine validating server responses during RCON authentication.
#[derive(Debug, Clone)]
pub struct AuthExchange {
    expected_id: PacketId,
    pre_auth_frames_seen: usize,
    max_pre_auth_frames: usize,
}

impl AuthExchange {
    /// Expected positive [`PacketId`] sent in the `SERVERDATA_AUTH` frame.
    #[must_use]
    pub const fn expected_id(&self) -> PacketId {
        self.expected_id
    }

    /// Processes a single inbound [`RawFrame`] during authentication.
    pub fn on_frame(&mut self, frame: RawFrame) -> Result<AuthStep, ProtoError> {
        if frame.id.is_auth_failure() {
            return Err(ProtoError::AuthRejected);
        }

        match frame.kind {
            PacketKind::ResponseValue => {
                // SRCDS emits an empty RESPONSE_VALUE with `expected_id` (or banner lines) prior
                // to AUTH_RESPONSE. Reject mismatched IDs and cap count to prevent infinite loops.
                if frame.id != self.expected_id {
                    return Err(ProtoError::UnexpectedPacketId {
                        expected: self.expected_id,
                        actual: frame.id,
                    });
                }
                self.pre_auth_frames_seen = self.pre_auth_frames_seen.saturating_add(1);
                if self.pre_auth_frames_seen > self.max_pre_auth_frames {
                    return Err(ProtoError::TooManyPreAuthFrames {
                        max: self.max_pre_auth_frames,
                    });
                }
                Ok(AuthStep::NeedMoreFrames)
            },
            PacketKind::ExecOrAuthResponse => {
                if frame.id == self.expected_id {
                    Ok(AuthStep::Authenticated)
                } else {
                    Err(ProtoError::UnexpectedPacketId {
                        expected: self.expected_id,
                        actual: frame.id,
                    })
                }
            },
            other => Err(ProtoError::UnexpectedPacketType {
                raw_type: other.as_i32(),
            }),
        }
    }
}

/// Final assembled output of a completed RCON command execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    command_id: PacketId,
    body: String,
    fragments: usize,
}

impl CommandOutput {
    /// Constructs a [`CommandOutput`].
    #[must_use]
    pub const fn new(command_id: PacketId, body: String, fragments: usize) -> Self {
        Self {
            command_id,
            body,
            fragments,
        }
    }

    /// Request [`PacketId`] that produced this response.
    #[must_use]
    pub const fn command_id(&self) -> PacketId {
        self.command_id
    }

    /// Assembled UTF-8 response text.
    #[must_use]
    pub fn body(&self) -> &str {
        self.body.as_str()
    }

    /// Consumes `self` and returns the owned response string.
    #[must_use]
    pub fn into_body(self) -> String {
        self.body
    }

    /// Number of `RESPONSE_VALUE` data frames assembled into this output.
    #[must_use]
    pub const fn fragments(&self) -> usize {
        self.fragments
    }
}

/// Step outcome returned while driving a [`CommandExchange`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandStep {
    /// Fragment accepted; more frames are required to reach deterministic termination.
    NeedMoreFrames,
    /// Fragment accepted in [`TerminationMode::CoalesceWindow`]; wait up to `idle_window`
    /// for any follow-up fragments before calling [`ProtocolState::finish_coalesced`].
    Coalescing {
        /// Quiet window to wait for any subsequent response fragment.
        idle_window: Duration,
    },
    /// Command response deterministically terminated and decoded.
    Completed(CommandOutput),
}

/// Pure state machine reassembling single- or multi-packet responses for one RCON command.
#[derive(Debug, Clone)]
pub struct CommandExchange {
    dialect: DialectProfile,
    command_id: PacketId,
    sentinel_id: Option<PacketId>,
    fragments: usize,
    accumulated: Vec<u8>,
}

impl CommandExchange {
    /// Primary [`PacketId`] assigned to the outbound command frame.
    #[must_use]
    pub const fn command_id(&self) -> PacketId {
        self.command_id
    }

    /// Paired sentinel [`PacketId`], if [`TerminationMode::SourceSentinel`] is active.
    #[must_use]
    pub const fn sentinel_id(&self) -> Option<PacketId> {
        self.sentinel_id
    }

    fn on_frame(
        &mut self,
        frame: RawFrame,
        pending_sentinel_trailer: &mut Option<PacketId>,
    ) -> Result<CommandStep, ProtoError> {
        if frame.id.is_auth_failure() {
            return Err(ProtoError::AuthRejected);
        }

        if let Some(sentinel_id) = self.sentinel_id
            && frame.id == sentinel_id
        {
            return self.finish_sentinel(frame, sentinel_id, pending_sentinel_trailer);
        }

        if frame.id != self.command_id
            && (!self.dialect.allow_zero_packet_id() || frame.id != PacketId::from_raw(0))
        {
            return Err(ProtoError::UnexpectedPacketId {
                expected: self.command_id,
                actual: frame.id,
            });
        }

        if frame.kind != PacketKind::ResponseValue {
            return Err(ProtoError::UnexpectedPacketType {
                raw_type: frame.kind.as_i32(),
            });
        }

        let max_total = self.dialect.limits().max_accumulated_response_bytes();
        let next_len = self
            .accumulated
            .len()
            .checked_add(frame.payload.len())
            .ok_or(ProtoError::ResponseTooLarge {
                accumulated: usize::MAX,
                max: max_total,
            })?;

        if next_len > max_total {
            return Err(ProtoError::ResponseTooLarge {
                accumulated: next_len,
                max: max_total,
            });
        }

        self.accumulated.extend_from_slice(&frame.payload);
        self.fragments = self.fragments.saturating_add(1);

        match self.dialect.termination() {
            TerminationMode::SinglePacket => {
                let decoded = self.dialect.encoding().decode_response(&self.accumulated)?;
                Ok(CommandStep::Completed(CommandOutput::new(
                    self.command_id,
                    decoded,
                    self.fragments,
                )))
            },
            TerminationMode::SourceSentinel => Ok(CommandStep::NeedMoreFrames),
            TerminationMode::CoalesceWindow { idle_window } => {
                Ok(CommandStep::Coalescing { idle_window })
            },
        }
    }

    fn finish_coalesced(&mut self) -> Result<CommandOutput, ProtoError> {
        let decoded = self.dialect.encoding().decode_response(&self.accumulated)?;
        Ok(CommandOutput::new(self.command_id, decoded, self.fragments))
    }

    fn finish_sentinel(
        &mut self,
        frame: RawFrame,
        sentinel_id: PacketId,
        pending_sentinel_trailer: &mut Option<PacketId>,
    ) -> Result<CommandStep, ProtoError> {
        if frame.kind != PacketKind::ResponseValue {
            return Err(ProtoError::UnexpectedPacketType {
                raw_type: frame.kind.as_i32(),
            });
        }

        // Classic Valve SRCDS sends an empty RESPONSE_VALUE for `sentinel_id` followed immediately
        // by a second RESPONSE_VALUE for `sentinel_id` with payload `[0x00, 0x01, 0x00, 0x00]`.
        // Non-SRCDS servers (e.g. vanilla Minecraft echoing `"Unknown request 0"`) emit a single
        // non-empty frame and no trailer. Recording `pending_sentinel_trailer` only when the
        // sentinel body is empty handles both implementations with zero polling delay.
        *pending_sentinel_trailer = if frame.payload.is_empty() {
            Some(sentinel_id)
        } else {
            None
        };

        let decoded = self.dialect.encoding().decode_response(&self.accumulated)?;
        Ok(CommandStep::Completed(CommandOutput::new(
            self.command_id,
            decoded,
            self.fragments,
        )))
    }
}

#[cfg(test)]
mod tests {
    use rconet_types::FrameLimits;

    use super::*;
    use crate::dialect::Preset;

    #[test]
    fn source_auth_handles_preliminary_response_value_then_auth_response() -> Result<(), ProtoError>
    {
        let mut proto = ProtocolState::new(DialectProfile::from(Preset::Source));
        let pw = SecretPassword::new("hunter2");
        let (mut auth, _wire) = proto.begin_auth(&pw)?;
        let id = auth.expected_id();

        // SRCDS sends empty RESPONSE_VALUE (0) before AUTH_RESPONSE (2)
        assert_eq!(
            auth.on_frame(RawFrame::response_value(id, &[][..])),
            Ok(AuthStep::NeedMoreFrames)
        );
        assert_eq!(
            auth.on_frame(RawFrame::auth_response(id)),
            Ok(AuthStep::Authenticated)
        );
        Ok(())
    }

    #[test]
    fn auth_rejects_minus_one_and_mismatched_positive_ids() -> Result<(), ProtoError> {
        let mut proto = ProtocolState::new(DialectProfile::from(Preset::MinecraftJava));
        let pw = SecretPassword::new("wrong");
        let (mut auth, _) = proto.begin_auth(&pw)?;

        assert_eq!(
            auth.on_frame(RawFrame::auth_response(PacketId::AUTH_FAILURE)),
            Err(ProtoError::AuthRejected)
        );

        let (mut auth2, _) = proto.begin_auth(&pw)?;
        let wrong_positive = PacketId::from_raw(auth2.expected_id().as_i32() + 99);
        assert_eq!(
            auth2.on_frame(RawFrame::auth_response(wrong_positive)),
            Err(ProtoError::UnexpectedPacketId {
                expected: auth2.expected_id(),
                actual: wrong_positive,
            })
        );
        Ok(())
    }

    #[test]
    fn auth_caps_preliminary_frames_against_rogue_server() -> Result<(), ProtoError> {
        let mut proto = ProtocolState::new(DialectProfile::from(Preset::MinecraftJava));
        let pw = SecretPassword::new("pw");
        let (mut auth, _) = proto.begin_auth(&pw)?;
        let id = auth.expected_id();

        for _ in 0..proto.dialect().max_pre_auth_frames() {
            assert_eq!(
                auth.on_frame(RawFrame::response_value(id, "banner")),
                Ok(AuthStep::NeedMoreFrames)
            );
        }
        assert!(matches!(
            auth.on_frame(RawFrame::response_value(id, "overflow")),
            Err(ProtoError::TooManyPreAuthFrames { .. })
        ));
        Ok(())
    }

    #[test]
    fn source_multi_packet_reassembly_and_srcds_trailer_absorption() -> Result<(), ProtoError> {
        let mut proto = ProtocolState::new(DialectProfile::from(Preset::Source));
        let (mut ex1, _wire1) = proto.begin_command("cvarlist")?;

        let cmd1_id = ex1.command_id();
        let sen1_id = ex1.sentinel_id().unwrap_or(PacketId::MIN_POSITIVE);

        assert_eq!(
            proto.advance_command(&mut ex1, RawFrame::response_value(cmd1_id, "part-1 ")),
            Ok(CommandStep::NeedMoreFrames)
        );
        assert_eq!(
            proto.advance_command(&mut ex1, RawFrame::response_value(cmd1_id, "part-2")),
            Ok(CommandStep::NeedMoreFrames)
        );

        // First sentinel ack (empty body) completes ex1 immediately
        let done = proto.advance_command(&mut ex1, RawFrame::response_value(sen1_id, &[][..]));
        assert_eq!(
            done,
            Ok(CommandStep::Completed(CommandOutput::new(
                cmd1_id,
                "part-1 part-2".to_string(),
                2,
            )))
        );

        // Now start command 2 while SRCDS's second trailer packet (`00 01 00 00`) for sen1_id is still in flight!
        let (mut ex2, _wire2) = proto.begin_command("status")?;
        let cmd2_id = ex2.command_id();
        let sen2_id = ex2.sentinel_id().unwrap_or(PacketId::MIN_POSITIVE);

        // Trailer from command 1 arrives first: cleanly absorbed without desynchronizing command 2!
        let trailer = RawFrame::response_value(sen1_id, SRCDS_SENTINEL_TRAILER);
        assert_eq!(
            proto.advance_command(&mut ex2, trailer),
            Ok(CommandStep::NeedMoreFrames)
        );

        // Duplicate trailer MUST NOT be absorbed a second time:
        let dup_trailer = RawFrame::response_value(sen1_id, SRCDS_SENTINEL_TRAILER);
        assert_eq!(
            proto.advance_command(&mut ex2, dup_trailer),
            Err(ProtoError::UnexpectedPacketId {
                expected: cmd2_id,
                actual: sen1_id,
            })
        );

        // Command 2 normal completion:
        assert_eq!(
            proto.advance_command(&mut ex2, RawFrame::response_value(cmd2_id, "map: dust2")),
            Ok(CommandStep::NeedMoreFrames)
        );
        let done2 = proto.advance_command(&mut ex2, RawFrame::response_value(sen2_id, &[][..]));
        assert_eq!(
            done2,
            Ok(CommandStep::Completed(CommandOutput::new(
                cmd2_id,
                "map: dust2".to_string(),
                1,
            )))
        );
        Ok(())
    }

    #[test]
    fn minecraft_java_enforces_1446_byte_command_limit_and_single_packet_completion()
    -> Result<(), ProtoError> {
        let mut proto = ProtocolState::new(DialectProfile::from(Preset::MinecraftJava));

        let too_long = "a".repeat(1447);
        assert_eq!(
            proto.begin_command(&too_long).err(),
            Some(ProtoError::CommandTooLong {
                length: 1447,
                max: 1446,
            })
        );

        let exact_limit = "a".repeat(1446);
        let (mut ex, _wire) = proto.begin_command(&exact_limit)?;
        assert!(ex.sentinel_id().is_none());

        let cmd_id = ex.command_id();
        let step = proto.advance_command(
            &mut ex,
            RawFrame::response_value(cmd_id, "There are 2 of a max of 20 players online"),
        );
        assert!(matches!(step, Ok(CommandStep::Completed(_))));
        Ok(())
    }

    #[test]
    fn enforces_max_accumulated_response_bytes() -> Result<(), ProtoError> {
        let small_limits = FrameLimits::new(1024, 512, 1500).ok().unwrap_or_default();
        let dialect = DialectProfile::from(Preset::Source).with_limits(small_limits);
        let mut proto = ProtocolState::new(dialect);
        let (mut ex, _) = proto.begin_command("dump")?;
        let id = ex.command_id();

        let chunk = vec![b'x'; 900];
        assert_eq!(
            proto.advance_command(&mut ex, RawFrame::response_value(id, chunk.clone())),
            Ok(CommandStep::NeedMoreFrames)
        );
        assert_eq!(
            proto.advance_command(&mut ex, RawFrame::response_value(id, chunk)),
            Err(ProtoError::ResponseTooLarge {
                accumulated: 1800,
                max: 1500,
            })
        );
        Ok(())
    }

    #[test]
    #[ignore = "adversarial panic-hunt matrix; run with cargo xtask fuzz"]
    fn panic_hunt_state_machines_and_encodings() {
        let presets = [
            Preset::Source,
            Preset::MinecraftJava,
            Preset::SinglePacket,
            #[cfg(feature = "base64")]
            Preset::Base64Tcp,
        ];
        for preset in presets {
            exercise_preset_adversarial(preset);
        }
    }

    fn exercise_preset_adversarial(preset: Preset) {
        let frames = build_hostile_frames();
        let mut proto = ProtocolState::new(DialectProfile::from(preset));

        for cmd in ["", "status", "bad\0cmd", &"x".repeat(5000)] {
            let _ = proto.begin_command(cmd);
        }

        if let Ok((mut auth, _)) = proto.begin_auth(&SecretPassword::new("pw")) {
            for frame in &frames {
                let _ = auth.on_frame(frame.clone());
            }
        }

        if let Ok((mut ex, _)) = proto.begin_command("status") {
            for frame in &frames {
                let _ = proto.advance_command(&mut ex, frame.clone());
            }
        }
    }

    fn build_hostile_frames() -> Vec<RawFrame> {
        let hostile_ids = [i32::MIN, -2, -1, 0, 1, 2, 42, i32::MAX];
        let hostile_types = [i32::MIN, -1, 0, 1, 2, 3, 99, i32::MAX];
        let hostile_payloads: &[&[u8]] = &[
            b"",
            b"\x00\x01\x00\x00",
            b"\xff\xfe\xfd\x80",
            b"===not-base64!!!===",
            b"normal-ascii-body",
        ];

        let mut out = Vec::new();
        for &id in &hostile_ids {
            for &ty in &hostile_types {
                for &body in hostile_payloads {
                    out.push(RawFrame {
                        id: PacketId::from_raw(id),
                        kind: PacketKind::from_raw(ty),
                        payload: bytes::Bytes::copy_from_slice(body),
                    });
                }
            }
        }
        out
    }
}
