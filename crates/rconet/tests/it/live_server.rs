//! Live server integration suite executed against a real containerized RCON server in CI
//! (or locally via `RCON_LIVE_ADDR` and `RCON_LIVE_PASSWORD`).
//!
//! Note: Marked `#[ignore = "live_server: ..."]` (without `"panic_hunt"` in its name) so
//! `cargo xtask fuzz` does not run it without a live server, while CI executes:
//! `cargo test -p rconet --all-features -- --ignored live_server`.

use std::env;
use std::time::Duration;

#[cfg(feature = "pool")]
use rconet::ServerPool;
use rconet::{Client, CommandOptions, Error, Preset, ReconnectPolicy};

#[tokio::test]
#[ignore = "requires RCON_LIVE_ADDR and RCON_LIVE_PASSWORD pointing to a live RCON server"]
async fn live_server_auth_rejection_concurrent_commands_and_pool() -> Result<(), Error> {
    let addr = env::var("RCON_LIVE_ADDR").map_err(|_| Error::InvalidAddress {
        reason: "RCON_LIVE_ADDR environment variable must be set",
    })?;
    let password = env::var("RCON_LIVE_PASSWORD").map_err(|_| Error::InvalidAddress {
        reason: "RCON_LIVE_PASSWORD environment variable must be set",
    })?;
    let preset = match env::var("RCON_LIVE_PRESET")
        .unwrap_or_else(|_| "minecraft".to_string())
        .as_str()
    {
        "source" => Preset::Source,
        "single" => Preset::SinglePacket,
        _ => Preset::MinecraftJava,
    };
    let query_cmd = env::var("RCON_LIVE_CMD").unwrap_or_else(|_| match preset {
        Preset::Source => "status".to_string(),
        _ => "list".to_string(),
    });

    // 1. Verify wrong password fails cleanly without leaking secret:
    let bad_secret = format!("{password}-definitely-wrong");
    let bad_res = Client::builder()
        .address(&addr)
        .password(&bad_secret)
        .preset(preset)
        .reconnect_policy(ReconnectPolicy::exponential_default())
        .connect()
        .await;
    assert!(bad_res.is_err());
    assert!(!format!("{bad_res:?}").contains(&bad_secret));

    // 2. Authenticate and run sequential + 8-way concurrent commands on one session:
    let client = Client::builder()
        .address(&addr)
        .password(&password)
        .preset(preset)
        .command_timeout(Duration::from_secs(10))
        .reconnect_policy(ReconnectPolicy::exponential_default())
        .connect()
        .await?;

    let initial = client
        .execute_with(&query_cmd, CommandOptions::new().idempotent())
        .await?;
    assert!(!initial.body().is_empty());

    let mut tasks = Vec::with_capacity(8);
    for _ in 0..8 {
        let c = client.clone();
        let q = query_cmd.clone();
        tasks.push(tokio::spawn(async move { c.execute(&q).await }));
    }
    for task in tasks {
        let out = task.await.map_err(|_| Error::ClientClosed)??;
        assert!(!out.body().is_empty());
    }
    client.shutdown().await?;

    // 3. Exercise `ServerPool` against the live server:
    #[cfg(feature = "pool")]
    {
        let pool_builder = Client::builder()
            .address(&addr)
            .password(&password)
            .preset(preset);
        let pool = ServerPool::new(pool_builder, 2, Duration::from_secs(10))?;
        let (r1, r2) = tokio::join!(pool.execute(&query_cmd), pool.execute(&query_cmd));
        assert!(!r1?.body().is_empty());
        assert!(!r2?.body().is_empty());
        pool.shutdown().await;
    }

    Ok(())
}
