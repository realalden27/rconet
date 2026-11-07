use bytes::{Buf, BufMut, Bytes, BytesMut};
use rconet_types::{FrameLimits, PacketId, WIRE_LENGTH_FIELD_BYTES, WIRE_MIN_BODY_HEADER_BYTES};
use zeroize::Zeroizing;

use crate::error::ProtoError;

/// Wire packet type code in the 32-bit little-endian RCON header.
///
/// Note that wire code `2` serves a dual role in the binary RCON specification:
/// client-to-server `SERVERDATA_EXECCOMMAND` (`2`) and server-to-client
/// `SERVERDATA_AUTH_RESPONSE` (`2`). Representing `2` honestly as a single wire
/// code avoids misclassifying frames without direction context.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PacketKind {
    /// `0` — `SERVERDATA_RESPONSE_VALUE` (command output or sentinel echo).
    ResponseValue,
    /// `2` — `SERVERDATA_EXECCOMMAND` (outbound) or `SERVERDATA_AUTH_RESPONSE` (inbound).
    ExecOrAuthResponse,
    /// `3` — `SERVERDATA_AUTH` (outbound authentication request).
    Auth,
    /// Unrecognized vendor-specific packet type preserved verbatim for forward compatibility.
    Unknown(i32),
}

impl PacketKind {
    /// Maps a raw 32-bit wire integer to a [`PacketKind`].
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        match raw {
            0 => Self::ResponseValue,
            2 => Self::ExecOrAuthResponse,
            3 => Self::Auth,
            other => Self::Unknown(other),
        }
    }

    /// Returns the 32-bit signed integer written to the wire header.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        match self {
            Self::ResponseValue => 0,
            Self::ExecOrAuthResponse => 2,
            Self::Auth => 3,
            Self::Unknown(raw) => raw,
        }
    }
}

/// Decoded or outbound binary RCON frame.
///
/// Wire layout (little-endian):
/// - `length: i32` (`4 + 4 + payload.len() + 2`)
/// - `id: i32`
/// - `kind: i32`
/// - `payload: [u8]`
/// - `terminator: [0x00, 0x00]`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    /// Correlating packet identifier.
    pub id: PacketId,
    /// Wire packet type code.
    pub kind: PacketKind,
    /// Raw frame body excluding the two trailing NUL terminator bytes.
    pub payload: Bytes,
}

impl RawFrame {
    /// Constructs a [`RawFrame`] with explicit identifier, wire packet kind, and payload.
    #[must_use]
    pub fn new(id: PacketId, kind: PacketKind, payload: impl Into<Bytes>) -> Self {
        Self {
            id,
            kind,
            payload: payload.into(),
        }
    }

    /// Constructs an outbound `SERVERDATA_AUTH` (`3`) frame.
    #[must_use]
    pub fn auth(id: PacketId, password_bytes: impl Into<Bytes>) -> Self {
        Self {
            id,
            kind: PacketKind::Auth,
            payload: password_bytes.into(),
        }
    }

    /// Constructs an outbound `SERVERDATA_EXECCOMMAND` (`2`) frame.
    #[must_use]
    pub fn exec_command(id: PacketId, command_bytes: impl Into<Bytes>) -> Self {
        Self {
            id,
            kind: PacketKind::ExecOrAuthResponse,
            payload: command_bytes.into(),
        }
    }

    /// Constructs an inbound or sentinel `SERVERDATA_RESPONSE_VALUE` (`0`) frame.
    #[must_use]
    pub fn response_value(id: PacketId, body: impl Into<Bytes>) -> Self {
        Self {
            id,
            kind: PacketKind::ResponseValue,
            payload: body.into(),
        }
    }

    /// Constructs a server `SERVERDATA_AUTH_RESPONSE` (`2`) frame.
    #[must_use]
    pub fn auth_response(id: PacketId) -> Self {
        Self {
            id,
            kind: PacketKind::ExecOrAuthResponse,
            payload: Bytes::new(),
        }
    }

    /// Computes the wire `length` field (`10 + payload.len()`) using checked arithmetic.
    pub fn wire_body_length(&self) -> Result<i32, ProtoError> {
        let body_len = WIRE_MIN_BODY_HEADER_BYTES
            .checked_add(self.payload.len())
            .ok_or(ProtoError::FrameExceedsLimit {
                length: usize::MAX,
                max: i32::MAX as usize,
            })?;

        i32::try_from(body_len).map_err(|_| ProtoError::FrameExceedsLimit {
            length: body_len,
            max: i32::MAX as usize,
        })
    }
}

/// Bounds-checked, runtime-independent binary RCON frame encoder and decoder.
#[derive(Debug, Clone, Copy)]
pub struct FrameCodec {
    limits: FrameLimits,
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self::new(FrameLimits::default())
    }
}

impl FrameCodec {
    /// Creates a codec enforcing the supplied [`FrameLimits`].
    #[must_use]
    pub const fn new(limits: FrameLimits) -> Self {
        Self { limits }
    }

    /// Returns the active [`FrameLimits`].
    #[must_use]
    pub const fn limits(&self) -> FrameLimits {
        self.limits
    }

    /// Serializes `frame` into `dst`.
    pub fn encode(&self, frame: &RawFrame, dst: &mut BytesMut) -> Result<(), ProtoError> {
        let body_len_i32 = frame.wire_body_length()?;
        let body_len = usize::try_from(body_len_i32).unwrap_or(usize::MAX);
        let total_wire_len =
            WIRE_LENGTH_FIELD_BYTES
                .checked_add(body_len)
                .ok_or(ProtoError::FrameExceedsLimit {
                    length: usize::MAX,
                    max: self.limits.max_inbound_frame_bytes(),
                })?;

        dst.reserve(total_wire_len);
        dst.put_i32_le(body_len_i32);
        dst.put_i32_le(frame.id.as_i32());
        dst.put_i32_le(frame.kind.as_i32());
        dst.extend_from_slice(&frame.payload);
        dst.put_u8(0x00);
        dst.put_u8(0x00);
        Ok(())
    }

    /// Serializes a sensitive frame (such as `SERVERDATA_AUTH`) directly into a pre-sized
    /// [`Zeroizing`] buffer so secret payload bytes are never copied into unzeroized heap
    /// allocations or abandoned during buffer growth.
    pub fn encode_sensitive(
        &self,
        id: PacketId,
        kind: PacketKind,
        secret_payload: &[u8],
    ) -> Result<Zeroizing<Vec<u8>>, ProtoError> {
        let body_len = WIRE_MIN_BODY_HEADER_BYTES
            .checked_add(secret_payload.len())
            .ok_or(ProtoError::FrameExceedsLimit {
                length: usize::MAX,
                max: self.limits.max_inbound_frame_bytes(),
            })?;
        let body_len_i32 = i32::try_from(body_len).map_err(|_| ProtoError::FrameExceedsLimit {
            length: body_len,
            max: i32::MAX as usize,
        })?;
        let total_wire_len =
            WIRE_LENGTH_FIELD_BYTES
                .checked_add(body_len)
                .ok_or(ProtoError::FrameExceedsLimit {
                    length: usize::MAX,
                    max: self.limits.max_inbound_frame_bytes(),
                })?;

        let mut dst = Zeroizing::new(Vec::with_capacity(total_wire_len));
        dst.extend_from_slice(&body_len_i32.to_le_bytes());
        dst.extend_from_slice(&id.as_i32().to_le_bytes());
        dst.extend_from_slice(&kind.as_i32().to_le_bytes());
        dst.extend_from_slice(secret_payload);
        dst.extend_from_slice(&[0x00, 0x00]);
        Ok(dst)
    }

    /// Attempts to decode a single complete [`RawFrame`] from `src`.
    ///
    /// - Returns `Ok(None)` when `src` holds a partial frame; `src` is left intact so
    ///   subsequent socket reads can append more bytes.
    /// - Validates minimum (`>= 10`) and maximum (`<= max_inbound_frame_bytes`) header
    ///   lengths **before** reserving any memory in `src`.
    pub fn decode(&self, src: &mut BytesMut) -> Result<Option<RawFrame>, ProtoError> {
        let Some(len_bytes) = src.get(0..WIRE_LENGTH_FIELD_BYTES) else {
            return Ok(None);
        };
        let Ok(len_arr) = <[u8; 4]>::try_from(len_bytes) else {
            return Ok(None);
        };
        let raw_len = i32::from_le_bytes(len_arr);

        let Ok(body_len) = usize::try_from(raw_len) else {
            return Err(ProtoError::FrameTooSmall { length: raw_len });
        };
        if body_len < WIRE_MIN_BODY_HEADER_BYTES {
            return Err(ProtoError::FrameTooSmall { length: raw_len });
        }

        let max_frame = self.limits.max_inbound_frame_bytes();
        if body_len > max_frame {
            return Err(ProtoError::FrameExceedsLimit {
                length: body_len,
                max: max_frame,
            });
        }

        let total_wire_len =
            WIRE_LENGTH_FIELD_BYTES
                .checked_add(body_len)
                .ok_or(ProtoError::FrameExceedsLimit {
                    length: usize::MAX,
                    max: max_frame,
                })?;

        if src.len() < total_wire_len {
            src.reserve(total_wire_len.saturating_sub(src.len()));
            return Ok(None);
        }

        src.advance(WIRE_LENGTH_FIELD_BYTES);
        let mut frame_bytes = src.split_to(body_len);

        let id = PacketId::from_raw(frame_bytes.get_i32_le());
        let kind = PacketKind::from_raw(frame_bytes.get_i32_le());

        let payload_len = frame_bytes
            .len()
            .checked_sub(2)
            .ok_or(ProtoError::FrameTooSmall { length: raw_len })?;
        let payload = frame_bytes.split_to(payload_len).freeze();

        if frame_bytes.as_ref() != [0x00, 0x00] {
            return Err(ProtoError::MissingTerminator);
        }

        Ok(Some(RawFrame { id, kind, payload }))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn encodes_and_decodes_known_wire_vector() {
        let codec = FrameCodec::default();
        let frame = RawFrame::exec_command(PacketId::from_raw(7), "status");
        let mut buf = BytesMut::new();

        assert!(codec.encode(&frame, &mut buf).is_ok());
        // length = 10 + 6 = 16 (0x10), id = 7, kind = 2, "status", 0x00, 0x00
        let expected: &[u8] = &[
            16, 0, 0, 0, // length
            7, 0, 0, 0, // id
            2, 0, 0, 0, // type
            b's', b't', b'a', b't', b'u', b's', // payload
            0, 0, // terminator
        ];
        assert_eq!(buf.as_ref(), expected);

        let decoded = codec.decode(&mut buf);
        assert_eq!(decoded, Ok(Some(frame)));
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_byte_by_byte_feed_decodes_cleanly() {
        let codec = FrameCodec::default();
        let original = RawFrame::response_value(PacketId::from_raw(42), "players online: 3");
        let mut wire = BytesMut::new();
        assert!(codec.encode(&original, &mut wire).is_ok());

        let mut recv = BytesMut::new();
        let last_idx = wire.len().saturating_sub(1);
        for (idx, &byte) in wire.iter().enumerate() {
            recv.put_u8(byte);
            if idx < last_idx {
                assert_eq!(codec.decode(&mut recv), Ok(None));
            }
        }
        assert_eq!(codec.decode(&mut recv), Ok(Some(original)));
    }

    #[test]
    fn rejects_underflow_and_negative_lengths_without_panicking() {
        let codec = FrameCodec::default();
        for bad_len in [i32::MIN, -100, -1, 0, 1, 9] {
            let mut buf = BytesMut::from(&bad_len.to_le_bytes()[..]);
            let err = codec.decode(&mut buf);
            assert_eq!(err, Err(ProtoError::FrameTooSmall { length: bad_len }));
        }
    }

    #[test]
    fn rejects_oversized_length_header_before_allocating() {
        let codec = FrameCodec::default();
        // Advertise a 2 GiB frame with only 4 bytes on the wire: must fail immediately, not OOM.
        let mut buf = BytesMut::from(&i32::MAX.to_le_bytes()[..]);
        let initial_cap = buf.capacity();
        let res = codec.decode(&mut buf);
        assert!(matches!(res, Err(ProtoError::FrameExceedsLimit { .. })));
        assert_eq!(buf.capacity(), initial_cap);
    }

    #[test]
    fn rejects_non_null_frame_terminator() {
        let codec = FrameCodec::default();
        let mut bad_frame = BytesMut::new();
        bad_frame.put_i32_le(12); // 4 id + 4 type + 2 body + 2 pad
        bad_frame.put_i32_le(1);
        bad_frame.put_i32_le(0);
        bad_frame.extend_from_slice(b"ok");
        bad_frame.extend_from_slice(&[0x00, 0xFF]); // corrupted pad byte

        assert_eq!(
            codec.decode(&mut bad_frame),
            Err(ProtoError::MissingTerminator)
        );
    }

    #[test]
    fn preserves_unknown_packet_type_codes() {
        let codec = FrameCodec::default();
        let unknown = RawFrame {
            id: PacketId::from_raw(9),
            kind: PacketKind::Unknown(77),
            payload: Bytes::from_static(b"vendor"),
        };
        let mut buf = BytesMut::new();
        assert!(codec.encode(&unknown, &mut buf).is_ok());
        assert_eq!(codec.decode(&mut buf), Ok(Some(unknown)));
    }

    proptest! {
        #[test]
        fn prop_roundtrip_arbitrary_frames(
            id in any::<i32>(),
            raw_kind in any::<i32>(),
            payload in prop::collection::vec(any::<u8>(), 0..2048),
        ) {
            let codec = FrameCodec::default();
            let frame = RawFrame {
                id: PacketId::from_raw(id),
                kind: PacketKind::from_raw(raw_kind),
                payload: Bytes::from(payload),
            };

            let mut buf = BytesMut::new();
            prop_assert!(codec.encode(&frame, &mut buf).is_ok());
            let decoded = codec.decode(&mut buf);
            prop_assert_eq!(decoded, Ok(Some(frame)));
            prop_assert!(buf.is_empty());
        }

        #[test]
        fn prop_decoder_never_panics_on_arbitrary_bytes(
            random_bytes in prop::collection::vec(any::<u8>(), 0..512),
        ) {
            let codec = FrameCodec::default();
            let mut buf = BytesMut::from(random_bytes.as_slice());
            let _ = codec.decode(&mut buf);
        }
    }

    #[test]
    fn encode_sensitive_matches_standard_wire_layout() {
        let codec = FrameCodec::default();
        let id = PacketId::from_raw(11);
        let secret = b"super-secret-rcon-pw";

        let sensitive = codec
            .encode_sensitive(id, PacketKind::Auth, secret)
            .unwrap_or_default();
        let mut regular = BytesMut::new();
        assert!(
            codec
                .encode(&RawFrame::auth(id, &secret[..]), &mut regular)
                .is_ok()
        );
        assert_eq!(sensitive.as_slice(), regular.as_ref());
    }

    #[test]
    #[ignore = "adversarial panic-hunt matrix; run with cargo xtask fuzz"]
    fn panic_hunt_hostile_frame_streams() {
        let codec = FrameCodec::default();
        let corpus = build_hostile_wire_corpus();
        assert!(corpus.len() >= 1_000);

        for raw in corpus {
            let mut buf = BytesMut::from(raw.as_slice());
            while let Ok(Some(_frame)) = codec.decode(&mut buf) {}
        }
    }

    fn build_hostile_wire_corpus() -> Vec<Vec<u8>> {
        let mut cases = Vec::with_capacity(1_500);
        cases.push(Vec::new());
        for prefix_len in 1..=14usize {
            cases.push(vec![0xFF; prefix_len]);
        }

        let boundary_lengths = (i32::MIN..=i32::MIN + 64)
            .chain(-64..=32)
            .chain(1440..=1470)
            .chain(4090..=4120)
            .chain(16380..=16395)
            .chain((i32::MAX - 32)..=i32::MAX);

        for raw_len in boundary_lengths {
            let mut header_only = Vec::with_capacity(4);
            header_only.extend_from_slice(&raw_len.to_le_bytes());
            cases.push(header_only);

            let mut with_partial_body = Vec::with_capacity(20);
            with_partial_body.extend_from_slice(&raw_len.to_le_bytes());
            with_partial_body.extend_from_slice(&[1, 0, 0, 0, 2, 0, 0, 0, b'x', b'y', 0, 0]);
            cases.push(with_partial_body);
        }

        append_terminator_and_concat_mutations(&mut cases);
        cases
    }

    fn append_terminator_and_concat_mutations(cases: &mut Vec<Vec<u8>>) {
        for bad_byte in 1u8..=255 {
            cases.push(vec![11, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, b'a', 0, bad_byte]);
            cases.push(vec![11, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, b'a', bad_byte, 0]);
        }

        // Valid frame followed immediately by malformed / truncated / oversized frame:
        let valid = [12, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, b'o', b'k', 0, 0];
        for tail in [
            &[][..],
            &[0xFF, 0xFF, 0xFF, 0x7F][..],
            &[0x05, 0x00, 0x00, 0x00][..],
            &[10, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 1][..],
            &valid[..],
        ] {
            let mut combined = Vec::from(valid);
            combined.extend_from_slice(tail);
            cases.push(combined);
        }
    }
}
