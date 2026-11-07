//! Minecraft Java Edition RCON example demonstrating preset limits, conservative
//! non-replay vs explicit idempotent read queries, and legacy `< 1.14.3` pacing.

use std::env;
use std::time::Duration;

use rconet::transport::mock::{MockRconServer, MockReply, MockServerConfig};
use rconet::{Client, CommandOptions, DialectProfile, Preset, ReconnectPolicy, ReplayMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut demo_server: Option<MockRconServer> = None;

    let (addr, password) = match (env::var("RCON_ADDR"), env::var("RCON_PASSWORD")) {
        (Ok(addr), Ok(pw)) => (addr, pw),
        _ => {
            let cfg = MockServerConfig::with_password("mc-secret")
                .route(
                    "list",
                    MockReply::Single(
                        "There are 2 of a max of 20 players online: Alex, Steve".to_string(),
                    ),
                )
                .route(
                    "say Server maintenance in 5 minutes",
                    MockReply::Single(String::new()),
                );
            let srv = MockRconServer::start(cfg).await?;
            let bound = srv.addr().to_string();
            demo_server = Some(srv);
            (bound, "mc-secret".to_string())
        },
    };

    // For Minecraft Java < 1.14.3 servers, `DialectProfile::minecraft_java_legacy()`
    // can be passed to `.dialect(...)` to enable 3ms inter-command spacing.
    let _legacy_profile = DialectProfile::minecraft_java_legacy();

    let client = Client::builder()
        .address(addr)
        .password(password)
        .preset(Preset::MinecraftJava)
        .command_timeout(Duration::from_secs(5))
        .reconnect_policy(ReconnectPolicy::exponential_default())
        .replay_mode(ReplayMode::ReplayIdempotentOnly { max_replays: 1 })
        .connect()
        .await?;

    // Read-only queries can opt into idempotency:
    let online = client
        .execute_with("list", CommandOptions::new().idempotent())
        .await?;
    println!("Online check: {}", online.body());

    // Mutating or broadcast commands remain NonIdempotent by default so they never double-fire:
    client
        .execute("say Server maintenance in 5 minutes")
        .await?;
    println!("Announcement delivered.");

    client.shutdown().await?;
    if let Some(srv) = demo_server {
        srv.shutdown().await;
    }
    Ok(())
}
