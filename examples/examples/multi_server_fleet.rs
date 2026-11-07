//! Multi-server `FleetManager` example demonstrating heterogeneous game server registration,
//! tag-filtered broadcasts with isolated error handling, and fleet health snapshots.

use rconet::transport::mock::{MockRconServer, MockReply, MockServerConfig};
use rconet::{Client, CommandOptions, FleetFilter, FleetManager, Preset, ServerId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let eu_lobby = MockRconServer::start(
        MockServerConfig::with_password("pw-1")
            .route("list", MockReply::Single("Lobby: 14/50".to_string())),
    )
    .await?;

    let us_arena = MockRconServer::start(
        MockServerConfig::with_password("pw-2")
            .route("list", MockReply::Single("Arena: 8/16".to_string())),
    )
    .await?;

    let fleet = FleetManager::with_max_concurrency(8);
    let mut events = fleet.subscribe_events();

    let c1 = Client::builder()
        .address(eu_lobby.addr().to_string())
        .password("pw-1")
        .preset(Preset::MinecraftJava)
        .connect()
        .await?;

    let c2 = Client::builder()
        .address(us_arena.addr().to_string())
        .password("pw-2")
        .preset(Preset::SinglePacket)
        .connect()
        .await?;

    fleet
        .register(ServerId::new("eu-lobby-1")?, c1, ["eu", "production"])
        .await;
    fleet
        .register(ServerId::new("us-arena-1")?, c2, ["us", "production"])
        .await;

    let replies = fleet
        .broadcast(
            &FleetFilter::WithTag("production".to_string()),
            "list",
            CommandOptions::new().idempotent(),
        )
        .await;

    for (server_id, outcome) in &replies {
        match outcome {
            Ok(out) => println!("[{server_id}] OK -> {}", out.body()),
            Err(err) => println!("[{server_id}] ERR -> {err}"),
        }
    }

    let snapshot = fleet.health_snapshot().await;
    println!(
        "Health snapshot across {} servers: {snapshot:?}",
        snapshot.len()
    );

    fleet.shutdown_all().await;
    while let Ok(ev) = events.try_recv() {
        println!("Fleet event: {ev:?}");
    }

    eu_lobby.shutdown().await;
    us_arena.shutdown().await;
    Ok(())
}
