//! Development-only room-token minting utility (PLAN S0, protocol §12).
//!
//! The key below is a public test key: never use it outside local development.
//!
//! ```sh
//! # Print the matching `ROOMD_JWT_PUBLIC_KEY` value:
//! cargo run --example dev_token -- --print-public-key
//!
//! # Mint a host token for an existing room:
//! cargo run --example dev_token -- --room <room_id> --role host
//! ```

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use roomd::auth::{Claims, Role};

/// Deterministic development seed. Public by design; do not reuse in production.
const DEV_SEED: [u8; 32] = [7u8; 32];
const DEV_KEY_ID: &str = "dev-2026-11";
const DEV_ISSUER: &str = "cinema-bot";

const USAGE: &str = "\
Usage: cargo run --example dev_token -- [options]

Options:
  --print-public-key   Print ROOMD_JWT_PUBLIC_KEY for this dev key
  --room <room_id>     Room to mint the token for (required unless printing)
  --user <user_id>     Opaque user id (default: 42)
  --guild <guild_id>   Opaque guild id (default: guild-1)
  --role <host|viewer> Role claim (default: host)
  --ttl <seconds>      Token lifetime (default: 300)
  --kid <key_id>       JWT key id header (default: dev-2026-11)
  -h, --help           Print this help

The matching dev public key is also wired into docker-compose.yml. Replace the
key pair before deploying anything that is reachable from the internet.";

fn main() -> anyhow::Result<()> {
    let signing_key = SigningKey::from_bytes(&DEV_SEED);

    if std::env::args().any(|arg| arg == "--print-public-key") {
        println!(
            "{}",
            URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes())
        );
        return Ok(());
    }

    let mut room_id = None;
    let mut user_id = "42".to_owned();
    let mut guild_id = "guild-1".to_owned();
    let mut role = Role::Host;
    let mut ttl = 300u64;
    let mut key_id = DEV_KEY_ID.to_owned();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            "--room" => room_id = Some(args.next().context("--room requires a value")?),
            "--user" => user_id = args.next().context("--user requires a value")?,
            "--guild" => guild_id = args.next().context("--guild requires a value")?,
            "--role" => {
                role = match args.next().context("--role requires a value")?.as_str() {
                    "host" => Role::Host,
                    "viewer" => Role::Viewer,
                    other => bail!("--role must be host or viewer, got {other:?}"),
                };
            }
            "--ttl" => {
                ttl = args
                    .next()
                    .context("--ttl requires a value")?
                    .parse()
                    .context("--ttl must be a number of seconds")?;
            }
            "--kid" => key_id = args.next().context("--kid requires a value")?,
            other => bail!("unknown argument {other:?}; use --help"),
        }
    }

    let room_id = room_id.context("--room is required; use --help")?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs();
    let claims = Claims {
        iss: Some(DEV_ISSUER.to_owned()),
        sub: Some(user_id.clone()),
        guild_id: Some(guild_id),
        room_id,
        user_id,
        role,
        iat: Some(now),
        exp: now + ttl,
    };

    let mut pkcs8 = vec![
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ];
    pkcs8.extend_from_slice(&DEV_SEED);

    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(key_id);
    let token = encode(&header, &claims, &EncodingKey::from_ed_der(&pkcs8))?;

    println!("{token}");
    Ok(())
}
