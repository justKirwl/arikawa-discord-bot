//! Development WebSocket client simulator for stage S3.9 broadcast debugging.
//! Joins a room, echoes every incoming frame verbatim, and can drive `play`
//! once after the first `state` or spam `chat_send` on a timer.
//!
//! ```sh
//! cargo run --example ws_sim -- --url ws://127.0.0.1:8080/ws \
//!   --token <jwt> [--play] [--chat "text"] [--interval-ms 500]
//! ```

use std::io::Write;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::time::{Duration, interval};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const USAGE: &str = "\
Usage: cargo run --example ws_sim -- --url <ws-url> --token <jwt> [options]

Options:
  --url <url>          WebSocket endpoint (default: ws://127.0.0.1:8080/ws)
  --token <jwt>        Room token sent in `join` (required)
  --play               Send one `play` after the first `state` frame
  --chat <text>        Send `chat_send` with this text every interval until Ctrl+C
  --interval-ms <ms>   Chat interval (default: 500)
  -h, --help           Print this help";

/// Print one line and flush so piped output stays in sync with the wire.
fn out(line: impl std::fmt::Display) -> Result<()> {
    println!("{line}");
    std::io::stdout().flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut url = "ws://127.0.0.1:8080/ws".to_owned();
    let mut token = None;
    let mut play = false;
    let mut chat = None::<String>;
    let mut interval_ms = 500u64;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            "--url" => url = args.next().context("--url requires a value")?,
            "--token" => token = Some(args.next().context("--token requires a value")?),
            "--play" => play = true,
            "--chat" => chat = Some(args.next().context("--chat requires a value")?),
            "--interval-ms" => {
                interval_ms = args
                    .next()
                    .context("--interval-ms requires a value")?
                    .parse()
                    .context("--interval-ms must be a number of milliseconds")?;
            }
            other => bail!("unknown argument {other:?}; use --help"),
        }
    }
    let token = token.context("--token is required; use --help")?;

    let (socket, response) = connect_async(url.as_str())
        .await
        .with_context(|| format!("failed to connect to {url}"))?;
    out(format!("# connected: {}", response.status()))?;

    let (mut sink, mut stream) = socket.split();
    sink.send(Message::text(
        json!({ "type": "join", "token": token }).to_string(),
    ))
    .await
    .context("failed to send join")?;

    let mut ticker = interval(Duration::from_millis(interval_ms));
    ticker.tick().await; // consume the immediate first tick
    let mut sent_play = false;

    loop {
        tokio::select! {
            frame = stream.next() => match frame {
                Some(Ok(Message::Text(text))) => {
                    // Only inspect `type` to arm --play; never interpret the state itself.
                    let is_state = serde_json::from_str::<Value>(text.as_str())
                        .ok()
                        .and_then(|v| v["type"].as_str().map(|t| t == "state"))
                        .unwrap_or(false);
                    out(text.as_str())?;
                    if is_state && play && !sent_play {
                        sink.send(Message::text(r#"{"type":"play"}"#)).await?;
                        out("# -> play")?;
                        sent_play = true;
                    }
                }
                Some(Ok(Message::Close(frame))) => {
                    if let Some(frame) = frame {
                        let code = u16::from(frame.code);
                        out(format!("# close code={code} reason={}", frame.reason))?;
                        if matches!(code, 1008 | 1009 | 1013) {
                            bail!("server closed with fatal code {code}");
                        }
                    } else {
                        out("# close")?;
                    }
                    break;
                }
                Some(Ok(other)) => out(format!("# {other:?}"))?,
                Some(Err(err)) => bail!("websocket error: {err}"),
                None => { out("# connection closed by peer")?; break; }
            },
            _ = ticker.tick(), if chat.is_some() => {
                let text = chat.as_deref().unwrap_or_default();
                sink.send(Message::text(json!({ "type": "chat_send", "text": text }).to_string()))
                    .await?;
                out("# -> chat_send")?;
            }
            _ = tokio::signal::ctrl_c() => {
                out("# ctrl+c, closing")?;
                let _ = sink.close().await;
                break;
            }
        }
    }
    Ok(())
}
