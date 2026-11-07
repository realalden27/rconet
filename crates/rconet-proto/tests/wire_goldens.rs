//! Byte-exact wire compatibility golden tests for Valve SRCDS and Minecraft Java RCON.
//!
//! Validates raw little-endian wire headers, sentinel packet layouts, `0x00010000` trailers,
//! multi-packet UTF-8 codepoint splits across 4096-byte chunk boundaries, and 1460-byte
//! Minecraft Java frame ceilings.

use std::time::Duration;

use bytes::{BufMut, BytesMut};
use rconet_proto::{
    AuthStep, CommandStep, DialectProfile, PayloadEncoding, Preset, ProtoError, ProtocolState,
    TerminationMode,
};
use rconet_types::SecretPassword;

#[test]
fn srcds_wire_golden_auth_accept_and_reject_after_preliminary_response_value()
-> Result<(), ProtoError> {
    let mut proto = ProtocolState::new(DialectProfile::from(Preset::Source));
    let pw = SecretPassword::new("rcon_pass");
    let (mut auth, wire) = proto.begin_auth(&pw)?;

    // Exact 23-byte wire layout: length=19, id=1, type=3 (SERVERDATA_AUTH), "rcon_pass", 0x00, 0x00
    let expected_outbound: &[u8] = &[
        19, 0, 0, 0, // length = 4 + 4 + 9 + 2 = 19
        1, 0, 0, 0, // id = 1
        3, 0, 0, 0, // SERVERDATA_AUTH = 3
        b'r', b'c', b'o', b'n', b'_', b'p', b'a', b's', b's', // payload
        0, 0, // NUL payload + NUL pad
    ];
    assert_eq!(wire.as_slice(), expected_outbound);

    // Raw SRCDS wire reply: empty RESPONSE_VALUE (id=1, type=0) + AUTH_RESPONSE (id=1, type=2)
    let mut inbound_ok = BytesMut::from(
        &[
            10, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, // preliminary RESPONSE_VALUE
            10, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, // AUTH_RESPONSE (id=1)
        ][..],
    );

    let codec = proto.codec();
    let f1 = codec
        .decode(&mut inbound_ok)?
        .ok_or(ProtoError::MissingTerminator)?;
    assert_eq!(auth.on_frame(f1)?, AuthStep::NeedMoreFrames);

    let f2 = codec
        .decode(&mut inbound_ok)?
        .ok_or(ProtoError::MissingTerminator)?;
    assert_eq!(auth.on_frame(f2)?, AuthStep::Authenticated);
    assert!(inbound_ok.is_empty());

    // Rejected password capture: SRCDS sends preliminary RESPONSE_VALUE (id=2, type=0)
    // followed by AUTH_RESPONSE with id = -1 (0xFF_FF_FF_FF).
    let (mut bad_auth, _bad_wire) = proto.begin_auth(&SecretPassword::new("wrong"))?;
    let mut inbound_rejected = BytesMut::from(
        &[
            10, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, // preliminary RESPONSE_VALUE (id=2)
            10, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 2, 0, 0, 0, 0, 0, // AUTH_RESPONSE (id=-1)
        ][..],
    );
    let r1 = codec
        .decode(&mut inbound_rejected)?
        .ok_or(ProtoError::MissingTerminator)?;
    assert_eq!(bad_auth.on_frame(r1)?, AuthStep::NeedMoreFrames);

    let r2 = codec
        .decode(&mut inbound_rejected)?
        .ok_or(ProtoError::MissingTerminator)?;
    assert_eq!(bad_auth.on_frame(r2), Err(ProtoError::AuthRejected));

    Ok(())
}

#[test]
fn srcds_wire_golden_multipacket_utf8_split_and_18_byte_sentinel_trailer() -> Result<(), ProtoError>
{
    let dialect = DialectProfile::from(Preset::Source).with_encoding(PayloadEncoding::StrictUtf8);
    let mut proto = ProtocolState::new(dialect);

    let (mut ex, outbound) = proto.begin_command("cvarlist")?;
    assert_eq!(ex.command_id().as_i32(), 1);
    assert_eq!(ex.sentinel_id().map(|id| id.as_i32()), Some(2));

    // Outbound must be exact command frame (22B) + empty sentinel frame (14B) = 36 bytes:
    let expected_outbound: &[u8] = &[
        18, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, b'c', b'v', b'a', b'r', b'l', b'i', b's', b't', 0, 0,
        10, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    assert_eq!(outbound.as_ref(), expected_outbound);

    // Construct raw SRCDS stream splitting 3-byte UTF-8 '€' ([0xE2, 0x82, 0xAC]) across a
    // 4096-byte payload boundary:
    // - Packet 1 (id=1, type=0): 4095 'A' bytes + [0xE2] -> payload = 4096 bytes, length = 4106
    // - Packet 2 (id=1, type=0): [0x82, 0xAC] + " end\n" -> payload = 7 bytes, length = 17
    // - Packet 3 (id=2, type=0): empty sentinel echo -> 14 wire bytes
    // - Packet 4 (id=2, type=0): SRCDS trailer [0x00, 0x01, 0x00, 0x00] -> 18 wire bytes
    let mut stream = BytesMut::with_capacity(4200);
    stream.put_i32_le(4106);
    stream.put_i32_le(1);
    stream.put_i32_le(0);
    stream.extend_from_slice(&vec![b'A'; 4095]);
    stream.put_u8(0xE2);
    stream.extend_from_slice(&[0x00, 0x00]);

    stream.put_i32_le(17);
    stream.put_i32_le(1);
    stream.put_i32_le(0);
    stream.extend_from_slice(&[0x82, 0xAC]);
    stream.extend_from_slice(b" end\n");
    stream.extend_from_slice(&[0x00, 0x00]);

    // Primary empty sentinel + classic 18-byte SRCDS 0x00010000 trailer:
    stream.extend_from_slice(&[10, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    stream.extend_from_slice(&[14, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]);

    let codec = proto.codec();
    let p1 = codec
        .decode(&mut stream)?
        .ok_or(ProtoError::MissingTerminator)?;
    assert_eq!(
        proto.advance_command(&mut ex, p1)?,
        CommandStep::NeedMoreFrames
    );

    let p2 = codec
        .decode(&mut stream)?
        .ok_or(ProtoError::MissingTerminator)?;
    assert_eq!(
        proto.advance_command(&mut ex, p2)?,
        CommandStep::NeedMoreFrames
    );

    let p3 = codec
        .decode(&mut stream)?
        .ok_or(ProtoError::MissingTerminator)?;
    let step = proto.advance_command(&mut ex, p3)?;
    let CommandStep::Completed(out) = step else {
        return Err(ProtoError::MissingTerminator);
    };

    assert_eq!(out.fragments(), 2);
    assert!(out.body().ends_with("A€ end\n"));
    assert_eq!(out.body().len(), 4095 + "€".len() + " end\n".len());

    // Remaining 18 bytes in `stream` are the SRCDS 0x00010000 trailer:
    assert_eq!(stream.len(), 18);
    let trailer = codec
        .decode(&mut stream)?
        .ok_or(ProtoError::MissingTerminator)?;
    assert!(proto.try_absorb_immediate_trailer(&trailer));
    assert!(stream.is_empty());

    Ok(())
}

#[test]
fn minecraft_java_wire_golden_1460_ceiling_section_formatting_and_coalesce_window()
-> Result<(), ProtoError> {
    let mut single_proto = ProtocolState::new(DialectProfile::from(Preset::MinecraftJava));

    // Exact 1446-byte command produces a 1460-byte wire packet (Minecraft RconClient read buffer size):
    let max_cmd = "say ".to_string() + &"m".repeat(1442);
    assert_eq!(max_cmd.len(), 1446);
    let (mut ex, wire) = single_proto.begin_command(&max_cmd)?;
    assert_eq!(wire.len(), 1460);
    assert_eq!(
        wire.get(..4),
        Some(&1456i32.to_le_bytes()[..]),
        "body length must equal 1456 (1460 - 4)"
    );

    // Vanilla Minecraft formatting response with UTF-8 '§' (0xC2 0xA7):
    let mc_body = "§6There are §c2§6 of a max of §c20§6 players online: §fAlex, Steve";
    let mut reply_wire = BytesMut::new();
    let body_len = i32::try_from(10 + mc_body.len()).unwrap_or(10);
    reply_wire.put_i32_le(body_len);
    reply_wire.put_i32_le(ex.command_id().as_i32());
    reply_wire.put_i32_le(0);
    reply_wire.extend_from_slice(mc_body.as_bytes());
    reply_wire.extend_from_slice(&[0, 0]);

    let frame = single_proto
        .codec()
        .decode(&mut reply_wire)?
        .ok_or(ProtoError::MissingTerminator)?;
    let CommandStep::Completed(out) = single_proto.advance_command(&mut ex, frame)? else {
        return Err(ProtoError::MissingTerminator);
    };
    assert_eq!(out.body(), mc_body);

    // Multi-packet (>4096B) Minecraft response via CoalesceWindow (zero "Unknown request 0" logs):
    let window = Duration::from_millis(25);
    let coalesce_dialect = DialectProfile::from(Preset::MinecraftJava).with_termination(
        TerminationMode::CoalesceWindow {
            idle_window: window,
        },
    );
    let mut coalesce_proto = ProtocolState::new(coalesce_dialect);
    let (mut cex, cwire) = coalesce_proto.begin_command("data get entity @p")?;
    assert!(cex.sentinel_id().is_none());
    assert_eq!(cwire.len(), 14 + "data get entity @p".len());

    let mut multi_wire = BytesMut::new();
    for chunk in [&vec![b'N'; 4096][..], b"BT_TAIL}" as &[u8]] {
        let len = i32::try_from(10 + chunk.len()).unwrap_or(10);
        multi_wire.put_i32_le(len);
        multi_wire.put_i32_le(cex.command_id().as_i32());
        multi_wire.put_i32_le(0);
        multi_wire.extend_from_slice(chunk);
        multi_wire.extend_from_slice(&[0, 0]);
    }

    while let Some(chunk_frame) = coalesce_proto.codec().decode(&mut multi_wire)? {
        assert_eq!(
            coalesce_proto.advance_command(&mut cex, chunk_frame)?,
            CommandStep::Coalescing {
                idle_window: window
            }
        );
    }
    let final_out = coalesce_proto.finish_coalesced(&mut cex)?;
    assert_eq!(final_out.fragments(), 2);
    assert_eq!(final_out.body().len(), 4096 + 8);
    assert!(final_out.body().ends_with("NBT_TAIL}"));

    Ok(())
}
