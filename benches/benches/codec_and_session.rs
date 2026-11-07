use std::hint::black_box;

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rconet::{
    Client, DialectProfile, FrameLimits, FramedConnection, PacketId, Preset, SecretPassword,
    TimeoutConfig,
};
use rconet_proto::{FrameCodec, ProtocolState, RawFrame};
use rconet_transport::mock::{MockReply, MockServerConfig, serve_duplex_stream};

fn bench_frame_codec(c: &mut Criterion) {
    let codec = FrameCodec::new(FrameLimits::source_default());
    let payload =
        Bytes::from("hostname: Benchmark Server | map: de_dust2 | players: 24/32".repeat(4));
    let payload_len = payload.len() as u64;
    let frame = RawFrame::response_value(PacketId::from_raw(42), payload);
    let mut buf = BytesMut::with_capacity(512);

    let mut group = c.benchmark_group("wire_codec");
    group.throughput(Throughput::Bytes(payload_len + 14));
    group.bench_function("encode_decode_256b_frame", |b| {
        b.iter(|| {
            buf.clear();
            let _ = codec.encode(black_box(&frame), &mut buf);
            let decoded = codec.decode(&mut buf);
            let _ = black_box(decoded);
        });
    });
    group.finish();
}

fn bench_multi_packet_reassembly(c: &mut Criterion) {
    let chunk = Bytes::from("x".repeat(4000));
    let mut proto = ProtocolState::new(DialectProfile::from(Preset::Source));

    let mut group = c.benchmark_group("protocol_state");
    group.throughput(Throughput::Bytes(16_000));
    group.bench_function("source_reassemble_4x4kb_sentinel", |b| {
        b.iter(|| {
            if let Ok((mut ex, _wire)) = proto.begin_command(black_box("cvarlist")) {
                let cmd_id = ex.command_id();
                if let Some(sentinel_id) = ex.sentinel_id() {
                    for _ in 0..4 {
                        let _ = proto.advance_command(
                            &mut ex,
                            RawFrame::response_value(cmd_id, chunk.clone()),
                        );
                    }
                    let done =
                        proto.advance_command(&mut ex, RawFrame::response_value(sentinel_id, ""));
                    let _ = black_box(done);
                }
            }
        });
    });
    group.finish();
}

fn bench_session_actor_duplex(c: &mut Criterion) {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };

    let client_opt = rt.block_on(async {
        let (client_io, server_io) = tokio::io::duplex(16_384);
        let cfg = MockServerConfig::with_password("bench-pw")
            .route("ping", MockReply::Single("pong".to_string()));
        tokio::spawn(serve_duplex_stream(server_io, cfg));

        let pw = SecretPassword::new("bench-pw");
        let conn = FramedConnection::handshake(
            client_io,
            &pw,
            DialectProfile::from(Preset::SinglePacket),
            TimeoutConfig::default(),
        )
        .await
        .ok()?;
        Some(Client::from_connection(conn))
    });

    let Some(client) = client_opt else {
        return;
    };

    let mut group = c.benchmark_group("session_actor");
    group.bench_function("duplex_single_packet_roundtrip", |b| {
        b.iter(|| {
            let out = rt.block_on(client.execute(black_box("ping")));
            let _ = black_box(out);
        });
    });
    group.finish();

    let _ = rt.block_on(client.shutdown());
}

criterion_group!(
    benches,
    bench_frame_codec,
    bench_multi_packet_reassembly,
    bench_session_actor_duplex
);
criterion_main!(benches);
