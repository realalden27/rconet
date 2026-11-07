use std::time::Duration;

use bytes::BytesMut;
use rconet::proto::{FrameCodec, RawFrame};
use rconet::transport::mock::{MockRconServer, MockReply, MockServerConfig};
use rconet::{
    Client, CommandOptions, CommandPhase, ConnectionState, Error, FrameLimits, PacketId, Preset,
    ReconnectPolicy, ReplayMode,
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

    // Drop the caller future after 15ms while `slow_cmd` is already executing on the wire!
    let timed_out =
        tokio::time::timeout(Duration::from_millis(15), client.execute("slow_cmd")).await;
    assert!(timed_out.is_err());

    // Immediately issue `fast_cmd` on the exact same TCP connection:
    // the session actor must finish draining `slow_cmd` + its sentinel before running `fast_cmd`!
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

    // Give `blocker` a moment to start in the actor so the next request sits in the queue:
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Enqueue and cancel `should_never_send` while `blocker` still owns the actor:
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
async fn non_replay_on_mid_flight_disconnect_and_automatic_session_recovery() -> Result<(), Error> {
    let config = MockServerConfig::with_password("pw")
        .route(
            "ban griefer",
            MockReply::DropFirstThenSingle("banned".to_string()),
        )
        .route("status", MockReply::Single("online".to_string()));

    let server = MockRconServer::start(config)
        .await
        .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

    let fast_reconnect = ReconnectPolicy::ExponentialBackoff {
        initial_delay: Duration::from_millis(15),
        max_delay: Duration::from_millis(60),
        multiplier: 2.0,
        max_attempts: Some(4),
    };

    let client = Client::builder()
        .address(server.addr().to_string())
        .password("pw")
        .preset(Preset::SinglePacket)
        .reconnect_policy(fast_reconnect)
        .replay_mode(ReplayMode::NeverReplay)
        .connect()
        .await?;

    // `ban griefer` drops mid-flight: MUST report OutcomeUnknown and MUST NOT replay!
    let err = client.execute("ban griefer").await;
    assert!(matches!(err, Err(ref e) if e.command_phase() == Some(CommandPhase::OutcomeUnknown)));
    assert!(matches!(err, Err(ref e) if e.may_have_executed()));

    // Subsequent `status` succeeds on the healed second connection:
    let status = client.execute("status").await?;
    assert_eq!(status.body(), "online");
    assert_eq!(server.connections_accepted(), 2);

    // Crucial verification: `ban griefer` was executed ONCE, never replayed!
    assert_eq!(
        server.executed_commands(),
        vec!["ban griefer".to_string(), "status".to_string()]
    );

    client.shutdown().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn opt_in_replay_only_replays_explicitly_marked_idempotent_commands() -> Result<(), Error> {
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

    let fast_reconnect = ReconnectPolicy::ExponentialBackoff {
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(40),
        multiplier: 1.5,
        max_attempts: Some(3),
    };

    let client = Client::builder()
        .address(server.addr().to_string())
        .password("pw")
        .preset(Preset::SinglePacket)
        .reconnect_policy(fast_reconnect)
        .replay_mode(ReplayMode::ReplayIdempotentOnly { max_replays: 1 })
        .connect()
        .await?;

    // Default `execute` is NonIdempotent -> STILL must never replay even under ReplayIdempotentOnly!
    let mutating_res = client.execute("pay alice 100").await;
    assert!(
        matches!(mutating_res, Err(ref e) if e.command_phase() == Some(CommandPhase::OutcomeUnknown))
    );

    // Explicitly marked Idempotent command IS replayed once after reconnecting:
    let list_out = client
        .execute_with("list", CommandOptions::new().idempotent())
        .await?;
    assert_eq!(list_out.body(), "players: 3");

    assert_eq!(
        server.executed_commands(),
        vec![
            "pay alice 100".to_string(), // Only once!
            "list".to_string(),          // Attempt 1 (dropped)
            "list".to_string(),          // Replay 1 (succeeded)
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
    assert!(matches!(err, Err(ref e) if e.command_phase() == Some(CommandPhase::OutcomeUnknown)));
    assert_eq!(client.state(), ConnectionState::Closed);

    // Session is terminated cleanly; subsequent calls fail fast with NotSent instead of reading garbage:
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

    // 1st request enters actor execution:
    let c1 = client.clone();
    let h1 = tokio::spawn(async move { c1.execute("hold").await });
    tokio::time::sleep(Duration::from_millis(15)).await;

    // 2nd request occupies the single queue slot (capacity = 1):
    let c2 = client.clone();
    let h2 = tokio::spawn(async move { c2.execute("queued_slot").await });
    tokio::time::sleep(Duration::from_millis(15)).await;

    // 3rd request via `try_execute` immediately receives QueueFull (`NotSent`):
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
