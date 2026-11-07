#[cfg(feature = "tokio-tcp")]
mod live_server;
mod pool_fleet;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rconet::transport::mock::{MockReply, MockServerConfig, serve_duplex_stream};
use rconet::{
    Client, CommandOptions, CommandPhase, ConnectionState, DEFAULT_QUEUE_CAPACITY, DialectProfile,
    Error, FramedConnection, MAX_TIMEOUT_DURATION, Preset, ReconnectPolicy, ReplayMode,
    SecretPassword, TimeoutConfig, TypeError,
};
use tokio::io::duplex;

#[tokio::test]
async fn connect_with_custom_duplex_stream_supports_safe_reconnection_and_bounds()
-> Result<(), Error> {
    let dials = Arc::new(AtomicUsize::new(0));
    let dials_clone = Arc::clone(&dials);

    let client = Client::connect_with(
        move || {
            let attempt_num = dials_clone.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                let (client_io, server_io) = duplex(8192);
                let cfg = if attempt_num == 1 {
                    MockServerConfig::with_password("pw")
                        .route("drop_me", MockReply::DropConnectionMidFlight)
                } else {
                    MockServerConfig::with_password("pw")
                        .route("status", MockReply::Single("recovered-duplex".to_string()))
                };
                tokio::spawn(serve_duplex_stream(server_io, cfg));
                FramedConnection::handshake(
                    client_io,
                    &SecretPassword::new("pw"),
                    DialectProfile::from(Preset::SinglePacket),
                    TimeoutConfig::default(),
                )
                .await
            }
        },
        ReconnectPolicy::exponential(
            Duration::from_millis(10),
            Duration::from_millis(40),
            1.5,
            Some(3),
        ),
        ReplayMode::NeverReplay,
        DEFAULT_QUEUE_CAPACITY,
    )
    .await?;

    // Excessive per-command timeout is rejected pre-write (`NotSent`) without tainting the stream:
    let excessive = client
        .execute_with("status", CommandOptions::new().with_timeout(Duration::MAX))
        .await;
    assert!(matches!(
        excessive,
        Err(Error::Config(TypeError::ExcessiveTimeout { max, .. })) if max == MAX_TIMEOUT_DURATION
    ));
    assert!(matches!(excessive, Err(ref e) if !e.may_have_executed()));

    // Mid-flight drop returns OutcomeUnknown without replay, then heals via `connect_with` factory:
    let dropped = client.execute("drop_me").await;
    assert!(
        matches!(dropped, Err(ref e) if e.command_phase() == Some(CommandPhase::OutcomeUnknown))
    );

    let healed = client.execute("status").await?;
    assert_eq!(healed.body(), "recovered-duplex");
    assert_eq!(dials.load(Ordering::SeqCst), 2);

    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn idle_disconnect_on_custom_stream_immediately_transitions_state_to_closed()
-> Result<(), Error> {
    let (client_io, mut server_io) = duplex(4096);
    let server_task = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = [0u8; 128];
        let _ = server_io.read(&mut buf).await;
        let _ = server_io
            .write_all(&[10, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0])
            .await;
        let _ = server_io.flush().await;
        // Hold stream idle briefly, then drop server side while ZERO commands are queued:
        tokio::time::sleep(Duration::from_millis(25)).await;
        drop(server_io);
    });

    let conn = FramedConnection::handshake(
        client_io,
        &SecretPassword::new("pw"),
        DialectProfile::from(Preset::SinglePacket),
        TimeoutConfig::default(),
    )
    .await
    .map_err(Error::from_transport)?;

    let client = Client::from_connection(conn);
    let mut state_rx = client.subscribe_state();
    assert!(client.state().is_ready());

    // Wait for `SessionActor` idle monitor to observe EOF and publish `ConnectionState::Closed`:
    tokio::time::timeout(Duration::from_secs(1), async {
        while *state_rx.borrow_and_update() != ConnectionState::Closed {
            if state_rx.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .map_err(|_| Error::ClientClosed)?;

    assert_eq!(client.state(), ConnectionState::Closed);
    let _ = server_task.await;
    Ok(())
}

#[cfg(feature = "tokio-tcp")]
mod tcp_session_tests {
    use std::time::Duration;

    use bytes::BytesMut;
    use rconet::proto::{FrameCodec, RawFrame};
    use rconet::transport::mock::{MockRconServer, MockReply, MockServerConfig};
    use rconet::{
        Client, CommandOptions, CommandPhase, ConnectionState, Error, FrameLimits, PacketId,
        Preset, ReconnectPolicy, ReplayMode,
    };

    #[tokio::test]
    async fn caller_cancellation_mid_flight_preserves_tcp_frame_alignment() -> Result<(), Error> {
        let config = MockServerConfig::with_password("pw")
            .route(
                "slow_cmd",
                MockReply::Delayed {
                    delay: Duration::from_millis(80),
                    body: "STALE_SLOW_RESPONSE".to_string(),
                },
            )
            .route(
                "fast_cmd",
                MockReply::Single("FRESH_FAST_RESPONSE".to_string()),
            );

        let server = MockRconServer::start(config)
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let client = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::Source)
            .reconnect_policy(ReconnectPolicy::Disabled)
            .connect()
            .await?;

        let timed_out =
            tokio::time::timeout(Duration::from_millis(15), client.execute("slow_cmd")).await;
        assert!(timed_out.is_err());

        let fresh = client.execute("fast_cmd").await?;
        assert_eq!(fresh.body(), "FRESH_FAST_RESPONSE");
        assert_eq!(server.connections_accepted(), 1);
        assert!(client.state().is_ready());

        client.shutdown().await?;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn queued_cancellation_before_dispatch_never_reaches_server() -> Result<(), Error> {
        let config = MockServerConfig::with_password("pw").route(
            "blocker",
            MockReply::Delayed {
                delay: Duration::from_millis(80),
                body: "blocker-done".to_string(),
            },
        );
        let server = MockRconServer::start(config)
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let client = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::SinglePacket)
            .connect()
            .await?;

        let c1 = client.clone();
        let blocker_handle = tokio::spawn(async move { c1.execute("blocker").await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let _ = tokio::time::timeout(
            Duration::from_millis(10),
            client.execute("should_never_send"),
        )
        .await;

        let blocker_out = blocker_handle.await.ok();
        assert!(matches!(blocker_out, Some(Ok(_))));

        let after = client.execute("after_cancel").await?;
        assert_eq!(after.body(), "echo: after_cancel");
        assert_eq!(
            server.executed_commands(),
            vec!["blocker".to_string(), "after_cancel".to_string()]
        );

        client.shutdown().await?;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn non_replay_on_mid_flight_disconnect_and_automatic_session_recovery()
    -> Result<(), Error> {
        let config = MockServerConfig::with_password("pw")
            .route(
                "ban griefer",
                MockReply::DropFirstThenSingle("banned".to_string()),
            )
            .route("status", MockReply::Single("online".to_string()));

        let server = MockRconServer::start(config)
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let fast_reconnect = ReconnectPolicy::exponential(
            Duration::from_millis(15),
            Duration::from_millis(60),
            2.0,
            Some(4),
        );

        let client = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::SinglePacket)
            .reconnect_policy(fast_reconnect)
            .replay_mode(ReplayMode::NeverReplay)
            .connect()
            .await?;

        let err = client.execute("ban griefer").await;
        assert!(
            matches!(err, Err(ref e) if e.command_phase() == Some(CommandPhase::OutcomeUnknown))
        );
        assert!(matches!(err, Err(ref e) if e.may_have_executed()));

        let status = client.execute("status").await?;
        assert_eq!(status.body(), "online");
        assert_eq!(server.connections_accepted(), 2);

        assert_eq!(
            server.executed_commands(),
            vec!["ban griefer".to_string(), "status".to_string()]
        );

        client.shutdown().await?;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn opt_in_replay_only_replays_explicitly_marked_idempotent_commands() -> Result<(), Error>
    {
        let config = MockServerConfig::with_password("pw")
            .route(
                "pay alice 100",
                MockReply::DropFirstThenSingle("paid".to_string()),
            )
            .route(
                "list",
                MockReply::DropFirstThenSingle("players: 3".to_string()),
            );

        let server = MockRconServer::start(config)
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let fast_reconnect = ReconnectPolicy::exponential(
            Duration::from_millis(10),
            Duration::from_millis(40),
            1.5,
            Some(3),
        );

        let client = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::SinglePacket)
            .reconnect_policy(fast_reconnect)
            .replay_mode(ReplayMode::ReplayIdempotentOnly { max_replays: 1 })
            .connect()
            .await?;

        let mutating_res = client.execute("pay alice 100").await;
        assert!(
            matches!(mutating_res, Err(ref e) if e.command_phase() == Some(CommandPhase::OutcomeUnknown))
        );

        let list_out = client
            .execute_with("list", CommandOptions::new().idempotent())
            .await?;
        assert_eq!(list_out.body(), "players: 3");

        assert_eq!(
            server.executed_commands(),
            vec![
                "pay alice 100".to_string(),
                "list".to_string(),
                "list".to_string(),
            ]
        );

        client.shutdown().await?;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_callers_serialize_without_frame_interleaving() -> Result<(), Error> {
        let config = MockServerConfig::with_password("pw").write_chunk_size(3);
        let server = MockRconServer::start(config)
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let client = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::Source)
            .connect()
            .await?;

        let mut handles = Vec::new();
        for idx in 0..16_usize {
            let handle_client = client.clone();
            handles.push(tokio::spawn(async move {
                let cmd = format!("cmd_{idx}");
                let out = handle_client.execute(&cmd).await?;
                Ok::<(String, String), Error>((cmd, out.into_body()))
            }));
        }

        for join in handles {
            let pair = join.await.map_err(|_| Error::ClientClosed)??;
            assert_eq!(pair.1, format!("echo: {}", pair.0));
        }

        assert_eq!(server.connections_accepted(), 1);
        client.shutdown().await?;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn mismatched_packet_id_terminates_session_rather_than_continuing() -> Result<(), Error> {
        let codec = FrameCodec::new(FrameLimits::source_default());
        let mut bogus_wire = BytesMut::new();
        let _ = codec.encode(
            &RawFrame::response_value(PacketId::from_raw(9999), "wrong-id-payload"),
            &mut bogus_wire,
        );

        let config = MockServerConfig::with_password("pw")
            .route("desync", MockReply::InjectRawWire(bogus_wire.freeze()));
        let server = MockRconServer::start(config)
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let client = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::SinglePacket)
            .reconnect_policy(ReconnectPolicy::Disabled)
            .connect()
            .await?;

        let err = client.execute("desync").await;
        assert!(
            matches!(err, Err(ref e) if e.command_phase() == Some(CommandPhase::OutcomeUnknown))
        );
        assert_eq!(client.state(), ConnectionState::Closed);

        let next = client.execute("status").await;
        assert!(matches!(next, Err(Error::ClientClosed)));
        assert!(matches!(next, Err(ref e) if e.command_phase() == Some(CommandPhase::NotSent)));

        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn bounded_queue_try_execute_enforces_backpressure() -> Result<(), Error> {
        let config = MockServerConfig::with_password("pw").route(
            "hold",
            MockReply::Delayed {
                delay: Duration::from_millis(120),
                body: "done".to_string(),
            },
        );
        let server = MockRconServer::start(config)
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let client = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::SinglePacket)
            .queue_capacity(1)
            .connect()
            .await?;

        let c1 = client.clone();
        let h1 = tokio::spawn(async move { c1.execute("hold").await });
        tokio::time::sleep(Duration::from_millis(15)).await;

        let c2 = client.clone();
        let h2 = tokio::spawn(async move { c2.execute("queued_slot").await });
        tokio::time::sleep(Duration::from_millis(15)).await;

        let overflow = client.try_execute("overflow", CommandOptions::new()).await;
        assert!(matches!(overflow, Err(Error::QueueFull { capacity: 1 })));
        assert!(matches!(overflow, Err(ref e) if !e.may_have_executed()));

        let _ = h1.await;
        let _ = h2.await;
        client.shutdown().await?;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn builder_and_client_debug_never_expose_credentials() -> Result<(), Error> {
        let secret = "top-secret-admin-pass-9876";
        let builder = Client::builder()
            .address("127.0.0.1:25575")
            .password(secret);

        let builder_dbg = format!("{builder:?}");
        assert!(!builder_dbg.contains(secret));
        assert!(builder_dbg.contains("[REDACTED]"));

        let server = MockRconServer::start(MockServerConfig::with_password(secret))
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let client = Client::builder()
            .address(server.addr().to_string())
            .password(secret)
            .connect()
            .await?;

        let client_dbg = format!("{client:?}");
        assert!(!client_dbg.contains(secret));

        client.shutdown().await?;
        server.shutdown().await;
        Ok(())
    }
}
