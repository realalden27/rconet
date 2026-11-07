//! Palworld RCON example demonstrating `Preset::Palworld`.

use rconet::{Client, Preset};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .address("127.0.0.1:25575")
        .password("1234567890")
        .preset(Preset::Palworld)
        .command_timeout(Duration::from_secs(5))
        .connect()
        .await?;

    let info = client.execute("Info").await?;
    println!("--- RCON Info ---\n{}", info.body());

    let players = client.execute("ShowPlayers").await?;
    println!("--- RCON ShowPlayers ---\n{}", players.body());

    client.shutdown().await?;
    Ok(())
}
