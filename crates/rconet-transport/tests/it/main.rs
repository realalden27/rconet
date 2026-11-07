use std::time::Duration;

use rconet_proto::{DialectProfile, Preset, TerminationMode};
use rconet_transport::mock::{
    MockAuthMode, MockReply, MockSentinelMode, MockServerConfig, serve_duplex_stream,
};
use rconet_transport::{CommandFailureKind, FramedConnection, TransportError};
use rconet_types::{CommandPhase, SecretPassword, TimeoutConfig};
use tokio::io::{AsyncWriteExt, duplex};

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
async fn duration_max_command_timeout_clamps_safely_without_panicking() -> Result<(), TransportError>
{
    let (client_io, server_io) = duplex(4096);
    let cfg =
        MockServerConfig::with_password("pw").route("ping", MockReply::Single("pong".to_string()));
    tokio::spawn(serve_duplex_stream(server_io, cfg));

    let mut conn = FramedConnection::handshake(
        client_io,
        &SecretPassword::new("pw"),
        DialectProfile::from(Preset::SinglePacket),
        TimeoutConfig::default(),
    )
    .await?;

    // Previously `Instant::now() + Duration::MAX` panicked; now clamped cleanly to MAX_TIMEOUT_DURATION.
    let out = conn
        .execute_command_with_timeout("ping", Duration::MAX)
        .await?;
    assert_eq!(out.body(), "pong");
    assert!(conn.is_healthy());
    Ok(())
}

#[tokio::test]
async fn coalesce_window_reassembles_multi_packet_response_without_sentinel()
-> Result<(), TransportError> {
    let (client_io, server_io) = duplex(8192);
    let cfg = MockServerConfig::with_password("pw")
        .auth_mode(MockAuthMode::DirectAuthResponse)
        .sentinel_mode(MockSentinelMode::IgnoreSilent)
        .write_chunk_size(1)
        .route(
            "dump",
            MockReply::MultiPacket(vec![
                "chunk-A-".to_string(),
                "chunk-B-".to_string(),
                "chunk-C".to_string(),
            ]),
        )
        .route("status", MockReply::Single("ok-next".to_string()));
    tokio::spawn(serve_duplex_stream(server_io, cfg));

    let dialect = DialectProfile::from(Preset::MinecraftJava).with_termination(
        TerminationMode::CoalesceWindow {
            idle_window: Duration::from_millis(25),
        },
    );

    let mut conn = FramedConnection::handshake(
        client_io,
        &SecretPassword::new("pw"),
        dialect,
        TimeoutConfig::default(),
    )
    .await?;

    let out1 = conn.execute_command("dump").await?;
    assert_eq!(out1.fragments(), 3);
    assert_eq!(out1.body(), "chunk-A-chunk-B-chunk-C");

    let out2 = conn.execute_command("status").await?;
    assert_eq!(out2.body(), "ok-next");
    assert!(conn.is_healthy());
    Ok(())
}

#[tokio::test]
async fn monitor_idle_absorbs_delayed_srcds_trailer_and_detects_remote_eof()
-> Result<(), TransportError> {
    let (client_io, mut server_io) = duplex(4096);

    let server_task = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 256];
        // Read auth packet (id = 1) and reply with AUTH_RESPONSE (id = 1):
        let _ = server_io.read(&mut buf).await;
        let _ = server_io
            .write_all(&[10, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0])
            .await;

        // Read command (id = 2) + sentinel (id = 3):
        let _ = server_io.read(&mut buf).await;
        // Send ONLY data (id = 2) + primary empty sentinel (id = 3) first:
        let _ = server_io
            .write_all(&[
                12, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, b'o', b'k', 0, 0, // data "ok"
                10, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, // primary empty sentinel
            ])
            .await;
        let _ = server_io.flush().await;

        // Wait 15ms so client is already inside `monitor_idle()`, then send delayed 18B SRCDS trailer:
        tokio::time::sleep(Duration::from_millis(15)).await;
        let _ = server_io
            .write_all(&[14, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0])
            .await;
        let _ = server_io.flush().await;

        // Wait another 20ms, then close the socket (EOF) while idle:
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(server_io);
    });

    let mut conn = FramedConnection::handshake(
        client_io,
        &SecretPassword::new("pw"),
        DialectProfile::from(Preset::Source),
        TimeoutConfig::default(),
    )
    .await?;

    let out = conn.execute_command("ping").await?;
    assert_eq!(out.body(), "ok");
    assert!(conn.is_healthy());

    // `monitor_idle` must absorb the delayed 18B trailer cleanly, then return once `server_io` drops!
    tokio::time::timeout(Duration::from_secs(1), conn.monitor_idle())
        .await
        .map_err(|_| TransportError::AuthClosedByPeer)?;
    assert!(!conn.is_healthy());

    let _ = server_task.await;
    Ok(())
}

#[cfg(feature = "tokio-tcp")]
mod tcp_tests {
    use std::time::Duration;

    use bytes::Bytes;
    use rconet_proto::{DialectProfile, Preset, TerminationMode};
    use rconet_transport::mock::{
        MockAuthMode, MockRconServer, MockReply, MockSentinelMode, MockServerConfig,
    };
    use rconet_transport::{CommandFailureKind, FramedConnection, TransportError};
    use rconet_types::{CommandPhase, SecretPassword, TimeoutConfig};

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
    async fn sentinel_mode_works_with_single_packet_warning_servers() -> Result<(), TransportError>
    {
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
    async fn auth_rejection_and_immediate_close_never_leak_password() -> Result<(), TransportError>
    {
        let server_reject =
            MockRconServer::start(MockServerConfig::with_password("correct-secret"))
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
    async fn mid_flight_disconnect_reports_outcome_unknown_and_taints_stream()
    -> Result<(), TransportError> {
        let config = MockServerConfig::with_password("pw")
            .route("crash", MockReply::DropConnectionMidFlight);
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
    async fn hostile_oversized_server_frame_terminates_session_safely() -> Result<(), TransportError>
    {
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
}
