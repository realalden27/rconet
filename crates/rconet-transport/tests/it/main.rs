use std::time::Duration;

use bytes::Bytes;
use rconet_proto::{DialectProfile, Preset, TerminationMode};
use rconet_transport::mock::{
    MockAuthMode, MockRconServer, MockReply, MockSentinelMode, MockServerConfig,
};
use rconet_transport::{CommandFailureKind, FramedConnection, TransportError};
use rconet_types::{CommandPhase, SecretPassword, TimeoutConfig};
use tokio::io::duplex;

#[tokio::test]
async fn source_multi_packet_over_one_byte_trickle_tcp_stream() -> Result<(), TransportError> {
    let config = MockServerConfig::with_password("s3cr3t")
        .auth_mode(MockAuthMode::SourceWithPreliminaryEmpty)
        .sentinel_mode(MockSentinelMode::SourceDualPacket)
        .write_chunk_size(1) // Every server write arrives 1 byte at a time!
        .route(
            "cvarlist",
            MockReply::MultiPacket(vec![
                "sv_cheats 0\n".to_string(),
                "sv_gravity 800\n".to_string(),
                "mp_timelimit 30\n".to_string(),
            ]),
        )
        .route("status", MockReply::Single("hostname: arena-1".to_string()));

    let server = MockRconServer::start(config)
        .await
        .map_err(TransportError::ConnectIo)?;

    let mut conn = FramedConnection::connect_tcp(
        server.addr(),
        &SecretPassword::new("s3cr3t"),
        DialectProfile::from(Preset::Source),
        TimeoutConfig::default(),
    )
    .await?;

    let out1 = conn.execute_command("cvarlist").await?;
    assert_eq!(out1.fragments(), 3);
    assert_eq!(
        out1.body(),
        "sv_cheats 0\nsv_gravity 800\nmp_timelimit 30\n"
    );

    // Consecutive command proves the 1-byte trickled SRCDS trailer did not desync framing:
    let out2 = conn.execute_command("status").await?;
    assert_eq!(out2.body(), "hostname: arena-1");
    assert!(conn.is_healthy());

    let _ = conn.shutdown().await;
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn sentinel_mode_works_with_single_packet_warning_servers() -> Result<(), TransportError> {
    let config = MockServerConfig::with_password("pw")
        .auth_mode(MockAuthMode::DirectAuthResponse)
        .sentinel_mode(MockSentinelMode::SinglePacketWarning)
        .route(
            "help",
            MockReply::MultiPacket(vec!["page1-".to_string(), "page2".to_string()]),
        );

    let server = MockRconServer::start(config)
        .await
        .map_err(TransportError::ConnectIo)?;

    let dialect = DialectProfile::from(Preset::MinecraftJava)
        .with_termination(TerminationMode::SourceSentinel);
    let mut conn = FramedConnection::connect_tcp(
        server.addr(),
        &SecretPassword::new("pw"),
        dialect,
        TimeoutConfig::default(),
    )
    .await?;

    let out = conn.execute_command("help").await?;
    assert_eq!(out.body(), "page1-page2");
    assert_eq!(out.fragments(), 2);

    let follow_up = conn.execute_command("list").await?;
    assert_eq!(follow_up.body(), "echo: list");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn validation_error_reports_not_sent_and_preserves_stream_health()
-> Result<(), TransportError> {
    let server = MockRconServer::start(
        MockServerConfig::with_password("mc-pass").auth_mode(MockAuthMode::DirectAuthResponse),
    )
    .await
    .map_err(TransportError::ConnectIo)?;

    let mut conn = FramedConnection::connect_tcp(
        server.addr(),
        &SecretPassword::new("mc-pass"),
        DialectProfile::from(Preset::MinecraftJava),
        TimeoutConfig::default(),
    )
    .await?;

    let oversized = "x".repeat(1447);
    let err = conn.execute_command(&oversized).await;
    assert!(matches!(
        err,
        Err(TransportError::Command {
            phase: CommandPhase::NotSent,
            kind: CommandFailureKind::Validation(_),
        })
    ));
    // Stream was never touched; connection remains healthy for subsequent commands!
    assert!(conn.is_healthy());

    let valid = conn.execute_command("list").await?;
    assert_eq!(valid.body(), "echo: list");
    assert_eq!(server.executed_commands(), vec!["list".to_string()]);

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn auth_rejection_and_immediate_close_never_leak_password() -> Result<(), TransportError> {
    let server_reject = MockRconServer::start(MockServerConfig::with_password("correct-secret"))
        .await
        .map_err(TransportError::ConnectIo)?;

    let wrong = SecretPassword::new("wrong-super-secret-value");
    let res = FramedConnection::connect_tcp(
        server_reject.addr(),
        &wrong,
        DialectProfile::from(Preset::Source),
        TimeoutConfig::default(),
    )
    .await;

    assert!(matches!(res, Err(ref e) if e.is_auth_rejected()));
    let formatted = format!("{res:?}");
    assert!(!formatted.contains("wrong-super-secret-value"));
    server_reject.shutdown().await;

    let server_drop = MockRconServer::start(
        MockServerConfig::with_password("correct-secret")
            .auth_mode(MockAuthMode::CloseImmediatelyOnFailure),
    )
    .await
    .map_err(TransportError::ConnectIo)?;

    let drop_res = FramedConnection::connect_tcp(
        server_drop.addr(),
        &wrong,
        DialectProfile::from(Preset::Source),
        TimeoutConfig::default(),
    )
    .await;
    assert!(matches!(drop_res, Err(TransportError::AuthClosedByPeer)));
    server_drop.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn stalled_partial_write_reports_outcome_unknown_and_taints_session() {
    // Duplex buffer of only 8 bytes with an unread peer forces `write_all` to stall mid-frame!
    let (client_io, _unread_server_io) = duplex(8);
    let fast_timeouts = TimeoutConfig::new(
        Duration::from_millis(50),
        Duration::from_millis(50),
        Duration::from_millis(40),
        Duration::from_millis(25),
    )
    .unwrap_or_default();

    let mut conn = FramedConnection::new_unauthenticated(
        client_io,
        DialectProfile::from(Preset::SinglePacket),
        fast_timeouts,
    );

    let err = conn.execute_command("say partial write test payload").await;
    assert!(matches!(
        err,
        Err(TransportError::Command {
            phase: CommandPhase::OutcomeUnknown,
            ..
        })
    ));
    assert!(!conn.is_healthy());

    // Subsequent call on tainted stream immediately fails with NotSent + SessionTainted:
    let second = conn.execute_command("status").await;
    assert!(matches!(
        second,
        Err(TransportError::Command {
            phase: CommandPhase::NotSent,
            kind: CommandFailureKind::SessionTainted,
        })
    ));
}

#[tokio::test]
async fn mid_flight_disconnect_reports_outcome_unknown_and_taints_stream()
-> Result<(), TransportError> {
    let config =
        MockServerConfig::with_password("pw").route("crash", MockReply::DropConnectionMidFlight);
    let server = MockRconServer::start(config)
        .await
        .map_err(TransportError::ConnectIo)?;

    let mut conn = FramedConnection::connect_tcp(
        server.addr(),
        &SecretPassword::new("pw"),
        DialectProfile::from(Preset::SinglePacket),
        TimeoutConfig::default(),
    )
    .await?;

    let res = conn.execute_command("crash").await;
    assert!(matches!(
        res,
        Err(TransportError::Command {
            phase: CommandPhase::OutcomeUnknown,
            kind: CommandFailureKind::UnexpectedEof | CommandFailureKind::ReadIo(_),
        })
    ));
    assert!(!conn.is_healthy());

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn hostile_oversized_server_frame_terminates_session_safely() -> Result<(), TransportError> {
    // Inject a 4-byte header claiming `i32::MAX` bytes:
    let hostile = Bytes::copy_from_slice(&i32::MAX.to_le_bytes());
    let config =
        MockServerConfig::with_password("pw").route("boom", MockReply::InjectRawWire(hostile));
    let server = MockRconServer::start(config)
        .await
        .map_err(TransportError::ConnectIo)?;

    let mut conn = FramedConnection::connect_tcp(
        server.addr(),
        &SecretPassword::new("pw"),
        DialectProfile::from(Preset::SinglePacket),
        TimeoutConfig::default(),
    )
    .await?;

    let res = conn.execute_command("boom").await;
    assert!(matches!(
        res,
        Err(TransportError::Command {
            phase: CommandPhase::OutcomeUnknown,
            kind: CommandFailureKind::Protocol(_),
        })
    ));
    assert!(!conn.is_healthy());

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn command_deadline_expiration_taints_connection() -> Result<(), TransportError> {
    let config = MockServerConfig::with_password("pw").route(
        "slow",
        MockReply::Delayed {
            delay: Duration::from_millis(250),
            body: "too late".to_string(),
        },
    );
    let server = MockRconServer::start(config)
        .await
        .map_err(TransportError::ConnectIo)?;

    let mut conn = FramedConnection::connect_tcp(
        server.addr(),
        &SecretPassword::new("pw"),
        DialectProfile::from(Preset::SinglePacket),
        TimeoutConfig::default(),
    )
    .await?;

    let res = conn
        .execute_command_with_timeout("slow", Duration::from_millis(40))
        .await;
    assert!(matches!(
        res,
        Err(TransportError::Command {
            phase: CommandPhase::OutcomeUnknown,
            kind: CommandFailureKind::DeadlineExceeded { .. },
        })
    ));
    assert!(!conn.is_healthy());

    server.shutdown().await;
    Ok(())
}
