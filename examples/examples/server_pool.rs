//! Connection pooling example demonstrating parallel command execution across
//! bounded `ServerPool` sessions.

use std::time::Duration;

use rconet::transport::mock::{MockRconServer, MockReply, MockServerConfig};
use rconet::{Client, Preset, ServerPool};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = MockServerConfig::with_password("pool-pw")
        .route(
            "stats",
            MockReply::Delayed {
                delay: Duration::from_millis(40),
                body:
                    "CPU   In    Out   Uptime  Users   FPS\n12.4  8400  9200  142     18      66.7"
                        .to_string(),
            },
        )
        .route(
            "status",
            MockReply::Single("players: 18 active".to_string()),
        );

    let server = MockRconServer::start(cfg).await?;

    let builder = Client::builder()
        .address(server.addr().to_string())
        .password("pool-pw")
        .preset(Preset::Source);

    let pool = ServerPool::new(builder, 4, Duration::from_secs(2))?;

    let p1 = pool.clone();
    let p2 = pool.clone();
    let (stats_res, status_res) = tokio::join!(p1.execute("stats"), p2.execute("status"));

    println!("Stats:\n{}", stats_res?.body());
    println!("Status: {}", status_res?.body());
    println!(
        "Sockets opened: {} (idle in pool: {})",
        server.connections_accepted(),
        pool.idle_count()
    );

    pool.shutdown().await;
    server.shutdown().await;
    Ok(())
}
