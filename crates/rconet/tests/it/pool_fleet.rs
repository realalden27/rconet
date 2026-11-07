#[cfg(feature = "pool")]
mod pool_tests {
    use std::time::Duration;

    use rconet::transport::mock::{MockRconServer, MockReply, MockServerConfig};
    use rconet::{
        Client, CommandPhase, Error, MAX_POOL_SESSIONS, Preset, ReconnectPolicy, ServerPool,
    };

    #[tokio::test]
    async fn pool_opens_parallel_sessions_to_avoid_head_of_line_blocking() -> Result<(), Error> {
        let config = MockServerConfig::with_password("pw")
            .route(
                "slow",
                MockReply::Delayed {
                    delay: Duration::from_millis(120),
                    body: "slow-ok".to_string(),
                },
            )
            .route("fast", MockReply::Single("fast-ok".to_string()))
            .route("reuse", MockReply::Single("reused-ok".to_string()));

        let server = MockRconServer::start(config)
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let builder = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::SinglePacket);

        let pool = ServerPool::new(builder, 2, Duration::from_secs(2))?;
        assert_eq!(pool.max_sessions(), 2);
        assert_eq!(pool.idle_count(), 0);

        let pool_slow = pool.clone();
        let slow_task = tokio::spawn(async move { pool_slow.execute("slow").await });

        // Ensure `slow` acquires the first socket before `fast` runs:
        tokio::time::sleep(Duration::from_millis(20)).await;

        // `fast` opens a second parallel session instead of waiting behind `slow`:
        let started = tokio::time::Instant::now();
        let fast_out = pool.execute("fast").await?;
        assert_eq!(fast_out.body(), "fast-ok");
        assert!(started.elapsed() < Duration::from_millis(90));

        let slow_out = slow_task.await.map_err(|_| Error::ClientClosed)??;
        assert_eq!(slow_out.body(), "slow-ok");

        assert_eq!(server.connections_accepted(), 2);
        assert_eq!(pool.idle_count(), 2);

        // Subsequent command reuses one of the warm idle sessions without dialing a 3rd socket:
        let reused = pool.execute("reuse").await?;
        assert_eq!(reused.body(), "reused-ok");
        assert_eq!(server.connections_accepted(), 2);

        pool.shutdown().await;
        assert_eq!(pool.idle_count(), 0);
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn pool_enforces_acquire_timeout_and_discards_tainted_sessions() -> Result<(), Error> {
        // Validate configuration bounds first:
        let dummy = Client::builder().address("127.0.0.1:25575").password("pw");
        assert!(ServerPool::new(dummy.clone(), 0, Duration::from_secs(1)).is_err());
        assert!(
            ServerPool::new(dummy.clone(), MAX_POOL_SESSIONS + 1, Duration::from_secs(1)).is_err()
        );
        assert!(ServerPool::new(dummy, 2, Duration::ZERO).is_err());

        let config = MockServerConfig::with_password("pw")
            .route(
                "drop_once",
                MockReply::DropFirstThenSingle("recovered".to_string()),
            )
            .route("ping", MockReply::Single("pong".to_string()));

        let server = MockRconServer::start(config)
            .await
            .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let builder = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::SinglePacket)
            .reconnect_policy(ReconnectPolicy::Disabled);

        let pool = ServerPool::new(builder, 1, Duration::from_millis(35))?;

        let lease = pool.acquire().await?;
        assert_eq!(lease.execute("ping").await?.body(), "pong");

        // Pool capacity is 1 and `lease` is still held -> second checkout times out cleanly:
        let saturated = pool.acquire().await;
        assert!(matches!(saturated, Err(Error::PoolAcquireTimeout { .. })));

        // Trigger a mid-flight disconnect on `lease`:
        let dropped = lease.execute("drop_once").await;
        assert!(
            matches!(dropped, Err(ref e) if e.command_phase() == Some(CommandPhase::OutcomeUnknown))
        );

        // Dropping the degraded lease must discard the closed Client rather than returning it to `idle`:
        drop(lease);
        assert_eq!(pool.idle_count(), 0);

        // Next execute lazily establishes a fresh healthy session:
        let healed = pool.execute("drop_once").await?;
        assert_eq!(healed.body(), "recovered");
        assert_eq!(server.connections_accepted(), 2);

        pool.shutdown().await;
        server.shutdown().await;
        Ok(())
    }
}

#[cfg(feature = "fleet")]
mod fleet_tests {
    use std::time::Duration;

    use rconet::transport::mock::{MockRconServer, MockReply, MockServerConfig};
    use rconet::{
        Client, CommandOptions, CommandPhase, ConnectionState, Error, FleetEvent, FleetFilter,
        FleetManager, Preset, ReconnectPolicy, ServerId,
    };

    #[tokio::test]
    async fn fleet_targeted_execution_and_isolated_partial_failure_broadcast() -> Result<(), Error>
    {
        let srv_alpha = MockRconServer::start(
            MockServerConfig::with_password("pw-a")
                .route("save-all", MockReply::Single("alpha-saved".to_string()))
                .route("list", MockReply::Single("alpha: 12/20".to_string())),
        )
        .await
        .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let srv_beta = MockRconServer::start(
            MockServerConfig::with_password("pw-b")
                .route("save-all", MockReply::Single("beta-saved".to_string())),
        )
        .await
        .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let srv_gamma = MockRconServer::start(MockServerConfig::with_password("pw-c").route(
            "save-all",
            MockReply::DropFirstThenSingle("gamma-saved".to_string()),
        ))
        .await
        .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let id_alpha = ServerId::new("eu-survival-1")?;
        let id_beta = ServerId::new("eu-creative-1")?;
        let id_gamma = ServerId::new("us-survival-1")?;

        let c_alpha = Client::builder()
            .address(srv_alpha.addr().to_string())
            .password("pw-a")
            .preset(Preset::MinecraftJava)
            .connect()
            .await?;

        let c_beta = Client::builder()
            .address(srv_beta.addr().to_string())
            .password("pw-b")
            .preset(Preset::Source)
            .connect()
            .await?;

        let c_gamma = Client::builder()
            .address(srv_gamma.addr().to_string())
            .password("pw-c")
            .preset(Preset::SinglePacket)
            .reconnect_policy(ReconnectPolicy::Disabled)
            .connect()
            .await?;

        let fleet = FleetManager::with_max_concurrency(2);
        assert!(fleet.is_empty().await);

        fleet
            .register(id_alpha.clone(), c_alpha, ["eu", "survival"])
            .await;
        fleet
            .register(id_beta.clone(), c_beta, ["eu", "creative"])
            .await;
        fleet
            .register(id_gamma.clone(), c_gamma, ["us", "survival"])
            .await;
        assert_eq!(fleet.len().await, 3);

        // Targeted execution + unknown ServerId check:
        assert_eq!(
            fleet.execute(&id_alpha, "list").await?.body(),
            "alpha: 12/20"
        );
        let missing = ServerId::new("nonexistent")?;
        assert!(matches!(
            fleet.execute(&missing, "list").await,
            Err(Error::UnknownServer { .. })
        ));

        // Tag-filtered broadcast: `gamma` drops mid-flight, but `alpha` still succeeds cleanly
        // and `beta` (tagged `creative`) is untouched!
        let results = fleet
            .broadcast(
                &FleetFilter::WithTag("survival".to_string()),
                "save-all",
                CommandOptions::new(),
            )
            .await;

        assert_eq!(results.len(), 2);
        assert!(!results.contains_key(&id_beta));
        assert!(matches!(
            results.get(&id_alpha),
            Some(Ok(out)) if out.body() == "alpha-saved"
        ));
        assert!(matches!(
            results.get(&id_gamma),
            Some(Err(err)) if err.command_phase() == Some(CommandPhase::OutcomeUnknown)
        ));
        assert!(srv_beta.executed_commands().is_empty());

        let snapshot = fleet.health_snapshot().await;
        assert!(matches!(
            snapshot.get(&id_alpha),
            Some(ConnectionState::Ready { .. })
        ));
        assert!(matches!(
            snapshot.get(&id_beta),
            Some(ConnectionState::Ready { .. })
        ));
        assert_eq!(snapshot.get(&id_gamma), Some(&ConnectionState::Closed));

        fleet.shutdown_all().await;
        assert!(fleet.is_empty().await);

        srv_alpha.shutdown().await;
        srv_beta.shutdown().await;
        srv_gamma.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn fleet_event_stream_emits_registration_reconnection_and_removal() -> Result<(), Error> {
        let server = MockRconServer::start(
            MockServerConfig::with_password("pw")
                .route("flap", MockReply::DropFirstThenSingle("healed".to_string()))
                .route("ok", MockReply::Single("fine".to_string())),
        )
        .await
        .map_err(|e| Error::from_transport(rconet::TransportError::ConnectIo(e)))?;

        let client = Client::builder()
            .address(server.addr().to_string())
            .password("pw")
            .preset(Preset::SinglePacket)
            .reconnect_policy(ReconnectPolicy::ExponentialBackoff {
                initial_delay: Duration::from_millis(15),
                max_delay: Duration::from_millis(45),
                multiplier: 1.5,
                max_attempts: Some(3),
            })
            .connect()
            .await?;

        let fleet = FleetManager::new();
        let mut events = fleet.subscribe_events();
        let id = ServerId::new("node-1")?;

        fleet.register(id.clone(), client, ["prod"]).await;

        let ev_reg = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .map_err(|_| Error::ClientClosed)?
            .map_err(|_| Error::ClientClosed)?;
        assert!(matches!(
            ev_reg,
            FleetEvent::ServerRegistered { ref server_id, state: ConnectionState::Ready { .. } }
            if server_id == &id
        ));

        // Trigger transient disconnect -> background heal:
        let _ = fleet.execute(&id, "flap").await;
        assert_eq!(fleet.execute(&id, "ok").await?.body(), "fine");

        // Drain events until we observe both `Reconnecting` and `Ready` transitions:
        let mut saw_reconnecting = false;
        let mut saw_ready_again = false;
        for _ in 0..4 {
            if let Ok(Ok(ev)) =
                tokio::time::timeout(Duration::from_millis(200), events.recv()).await
            {
                match ev {
                    FleetEvent::StateChanged {
                        state: ConnectionState::Reconnecting { .. },
                        ..
                    } => saw_reconnecting = true,
                    FleetEvent::StateChanged {
                        state: ConnectionState::Ready { .. },
                        ..
                    } => saw_ready_again = true,
                    _ => {},
                }
            }
            if saw_reconnecting && saw_ready_again {
                break;
            }
        }
        assert!(saw_reconnecting);
        assert!(saw_ready_again);

        fleet.shutdown_all().await;
        let mut saw_removed = false;
        for _ in 0..4 {
            if let Ok(Ok(FleetEvent::ServerRemoved { server_id })) =
                tokio::time::timeout(Duration::from_millis(200), events.recv()).await
                && server_id == id
            {
                saw_removed = true;
                break;
            }
        }
        assert!(saw_removed);

        server.shutdown().await;
        Ok(())
    }
}

#[cfg(feature = "webrcon")]
mod webrcon_tests {
    use rconet::{Error, PacketId, WebRconEvent, WebRconMessageKind, WebRconRequest};

    #[test]
    fn webrcon_request_and_live_event_classification_work_through_facade() -> Result<(), Error> {
        let req_id = PacketId::from_raw(77);
        let req = WebRconRequest::new(req_id, "serverinfo");
        let encoded = req.to_json().map_err(Error::Validation)?;
        assert!(encoded.contains("\"Identifier\":77"));
        assert!(encoded.contains("\"Message\":\"serverinfo\""));

        let reply = WebRconEvent::from_json(
            r#"{"Identifier":77,"Message":"{\"Players\":14}","Type":"Generic","Stacktrace":""}"#,
            4096,
        )
        .map_err(Error::Validation)?;
        assert_eq!(reply.packet_id(), req_id);
        assert!(!reply.is_unsolicited_broadcast());
        assert_eq!(reply.kind, WebRconMessageKind::Generic);

        let chat_push = WebRconEvent::from_json(
            r#"{"Identifier":-1,"Message":"[Chat] Builder: hi","Type":"Chat","Stacktrace":""}"#,
            4096,
        )
        .map_err(Error::Validation)?;
        assert!(chat_push.is_unsolicited_broadcast());
        assert_eq!(chat_push.kind, WebRconMessageKind::Chat);
        Ok(())
    }
}
