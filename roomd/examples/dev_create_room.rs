//! Development helper: creates a room through the public gRPC API and prints
//! its id. Pair it with `dev_token` for a local WebSocket session.
//!
//! ```sh
//! cargo run --example dev_create_room -- --guild guild-1 --channel channel-1
//! ```

use anyhow::{Context, Result};
use roomd::pb::roomd::v1 as pb;

#[tokio::main]
async fn main() -> Result<()> {
    let mut guild_id = "guild-1".to_owned();
    let mut channel_id = "channel-1".to_owned();
    let mut host_user_id = "42".to_owned();
    let mut endpoint = "http://127.0.0.1:50051".to_owned();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!(
                    "Usage: cargo run --example dev_create_room -- \
                     [--guild <id>] [--channel <id>] [--host <user_id>] [--endpoint <url>]"
                );
                return Ok(());
            }
            "--guild" => guild_id = args.next().context("--guild requires a value")?,
            "--channel" => channel_id = args.next().context("--channel requires a value")?,
            "--host" => host_user_id = args.next().context("--host requires a value")?,
            "--endpoint" => endpoint = args.next().context("--endpoint requires a value")?,
            other => anyhow::bail!("unknown argument {other:?}; use --help"),
        }
    }

    let mut client = pb::room_service_client::RoomServiceClient::connect(endpoint)
        .await
        .context("failed to connect to roomd gRPC")?;
    let response = client
        .create_room(pb::CreateRoomRequest {
            guild_id,
            channel_id,
            host_user_id,
        })
        .await
        .context("CreateRoom failed")?
        .into_inner();

    println!("{}", response.room_id);
    Ok(())
}
