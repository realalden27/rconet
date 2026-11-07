//! Minimal `rconet` quickstart example.
//!
//! Connects to `RCON_ADDR` with `RCON_PASSWORD` if set in the environment; otherwise
//! spins up a local deterministic mock server so the example runs out of the box.

use std::env;
use std::time::Duration;

use rconet::transport::mock::{MockRconServer, MockReply, MockServerConfig};
use rconet::{Client, Preset, ReconnectPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut demo_server: Option<MockRconServer> = None;

    let (addr, password) = match (env::var("RCON_ADDR"), env::var("RCON_PASSWORD")) {
        (Ok(addr), Ok(pw)) => (addr, pw),
        _ => {
            let cfg = MockServerConfig::with_password("demo-secret").route(
                "status",
                MockReply::Single("hostname: Demo Server\nplayers : 8 / 24".to_string()),
            );
            let srv = MockRconServer::start(cfg).await?;
            let bound = srv.addr().to_string();
            demo_server = Some(srv);
            (bound, "demo-secret".to_string())
        },
    };

    let client = Client::builder()
        .address(addr)
        .password(password)
        .preset(Preset::Source)
        .command_timeout(Duration::from_secs(5))
        .reconnect_policy(ReconnectPolicy::exponential_default())
        .connect()
        .await?;

    let output = client.execute("status").await?;
    println!("--- status ---\n{}", output.body());

    client.shutdown().await?;
    if let Some(srv) = demo_server {
        srv.shutdown().await;
    }
    Ok(())
}
