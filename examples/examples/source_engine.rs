//! Source Engine multi-packet RCON example demonstrating deterministic sentinel framing
//! without artificial idle-sleep polling.

use std::env;

use rconet::transport::mock::{MockRconServer, MockReply, MockSentinelMode, MockServerConfig};
use rconet::{Client, Preset};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut demo_server: Option<MockRconServer> = None;

    let (addr, password) = match (env::var("RCON_ADDR"), env::var("RCON_PASSWORD")) {
        (Ok(addr), Ok(pw)) => (addr, pw),
        _ => {
            let chunks = vec![
                "cvar list\n--------------\n".to_string(),
                "sv_cheats        : 0\n".to_string(),
                "mp_timelimit     : 30\n".to_string(),
                "--------------\n2 total convars/concommands".to_string(),
            ];
            let cfg = MockServerConfig::with_password("srcds-pw")
                .sentinel_mode(MockSentinelMode::SourceDualPacket)
                .route("cvarlist", MockReply::MultiPacket(chunks));
            let srv = MockRconServer::start(cfg).await?;
            let bound = srv.addr().to_string();
            demo_server = Some(srv);
            (bound, "srcds-pw".to_string())
        },
    };

    let client = Client::builder()
        .address(addr)
        .password(password)
        .preset(Preset::Source)
        .connect()
        .await?;

    let output = client.execute("cvarlist").await?;
    println!(
        "Reassembled {} response frames ({} bytes):\n{}",
        output.fragments(),
        output.body().len(),
        output.body()
    );

    client.shutdown().await?;
    if let Some(srv) = demo_server {
        srv.shutdown().await;
    }
    Ok(())
}
