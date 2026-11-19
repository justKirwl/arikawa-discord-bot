//! WebSocket integration test for multiple simulated clients in one room (S3.9).
//!
//! Drives the full lifecycle through the contract in `docs/protocol.md`:
//! room creation / media over gRPC, two WebSocket clients joining, playback
//! fan-out, chat/reaction seq ordering, rate limiting, disconnect and the
//! bot-initiated room close. Assertions target the contract, not implementation
//! quirks: frames may interleave, so helpers match on `type` and skip the rest.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use roomd::auth::JwtAuth;
use roomd::grpc;
use roomd::http::{self, AppState};
use roomd::hub::RoomHub;
use roomd::pb::media::v1::{MediaKind, MediaRef};
use roomd::pb::roomd::v1 as pb;
use roomd::state::RoomStore;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const SEED: [u8; 32] = [7u8; 32];
const KEY_ID: &str = "test-key";

const RECV_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Serialize)]
struct TestClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    guild_id: &'a str,
    room_id: &'a str,
    user_id: &'a str,
    role: &'a str,
    iat: u64,
    exp: u64,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn encoding_key(seed: [u8; 32]) -> EncodingKey {
    let mut pkcs8 = vec![
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ];
    pkcs8.extend_from_slice(&seed);
    EncodingKey::from_ed_der(&pkcs8)
}

/// Dev Ed25519 key minted by the fake bot (same seed/kid as the other tests).
fn jwt_auth() -> JwtAuth {
    let public = URL_SAFE_NO_PAD.encode(SigningKey::from_bytes(&SEED).verifying_key().to_bytes());
    JwtAuth::new(
        &public,
        Some("cinema-bot".to_owned()),
        Some(KEY_ID.to_owned()),
    )
    .expect("test key is valid")
}

fn token(room_id: &str, role: &str) -> String {
    // Distinct user_id per role: two sockets sharing a user_id are the same
    // participant (reconnect upsert), not two people in the room (protocol §12).
    let user_id = format!("{role}-user");
    let claims = TestClaims {
        iss: "cinema-bot",
        sub: &user_id,
        guild_id: "guild-1",
        room_id,
        user_id: &user_id,
        role,
        iat: now(),
        exp: now() + 300,
    };
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(KEY_ID.to_owned());
    encode(&header, &claims, &encoding_key(SEED)).expect("token encoding")
}

/// Both listeners share one `RoomStore`, just like `roomd::run`.
async fn start_servers() -> (String, std::net::SocketAddr) {
    let store = Arc::new(RoomStore::default());
    let hub = Arc::new(RoomHub::new());

    let grpc_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("grpc listener");
    let grpc_addr = grpc_listener.local_addr().expect("grpc address");
    tokio::spawn(grpc::serve(
        grpc_listener,
        store.clone(),
        hub.clone(),
        std::future::pending(),
    ));

    let http_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("http listener");
    let http_addr = http_listener.local_addr().expect("http address");
    let state = AppState::with_services(store, Some(Arc::new(jwt_auth())), hub);
    tokio::spawn(http::serve(http_listener, state, std::future::pending()));

    (format!("http://{grpc_addr}"), http_addr)
}

async fn grpc_client(
    endpoint: &str,
) -> pb::room_service_client::RoomServiceClient<tonic::transport::Channel> {
    tokio::time::timeout(
        Duration::from_secs(5),
        pb::room_service_client::RoomServiceClient::connect(endpoint.to_owned()),
    )
    .await
    .expect("grpc connect timeout")
    .expect("grpc connect")
}

async fn connect(addr: std::net::SocketAddr) -> Client {
    let (socket, response) = connect_async(format!("ws://{addr}/ws"))
        .await
        .expect("websocket connect");
    assert_eq!(response.status(), 101);
    socket
}

async fn send(socket: &mut Client, value: Value) {
    socket
        .send(Message::text(value.to_string()))
        .await
        .expect("send");
}

/// Next frame of the wanted type within a total deadline, skipping unrelated
/// message types (the protocol allows interleaving of broadcast frames).
async fn recv_type(socket: &mut Client, wanted: &str, timeout: Duration) -> Value {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {wanted}");
        let message = tokio::time::timeout(remaining, socket.next())
            .await
            .expect("message timeout")
            .expect("stream ended")
            .expect("websocket error");
        match message {
            Message::Text(text) => {
                let value: Value = serde_json::from_str(text.as_str()).expect("json");
                if value["type"].as_str() == Some(wanted) {
                    return value;
                }
            }
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// `None` on timeout / end of stream, used for negative assertions and drains.
async fn try_recv_json(socket: &mut Client, timeout: Duration) -> Option<Value> {
    let message = tokio::time::timeout(timeout, socket.next())
        .await
        .ok()??
        .ok()?;
    match message {
        Message::Text(text) => serde_json::from_str(text.as_str()).ok(),
        _ => None,
    }
}

async fn assert_silent(socket: &mut Client, timeout: Duration) {
    if let Some(value) = try_recv_json(socket, timeout).await {
        panic!("unexpected frame on socket: {value}");
    }
}

/// Read until the socket is idle, collecting every JSON frame.
async fn drain(socket: &mut Client, idle: Duration) -> Vec<Value> {
    let mut frames = Vec::new();
    while let Some(value) = try_recv_json(socket, idle).await {
        frames.push(value);
    }
    frames
}

/// The contract allows either a Close frame with the given code or a plain
/// stream end when the room is closed by the bot (protocol §7, §10).
async fn expect_close(socket: &mut Client, code: u16) {
    let deadline = tokio::time::Instant::now() + CLOSE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for close");
        let Some(message) = tokio::time::timeout(remaining, socket.next())
            .await
            .expect("close timeout")
        else {
            return; // stream ended
        };
        match message {
            Ok(Message::Close(Some(frame))) => {
                assert_eq!(u16::from(frame.code), code);
                return;
            }
            Ok(Message::Close(None)) => return,
            Ok(_) => continue, // skip any late broadcast frame
            Err(_) => return,  // transport error ends the stream
        }
    }
}

#[tokio::test]
async fn multiple_clients_share_room_state_and_events() {
    let (endpoint, http_addr) = start_servers().await;
    let mut grpc = grpc_client(&endpoint).await;

    // 1. Room + media exist before any client joins (protocol §3.1, §5).
    let room_id = grpc
        .create_room(pb::CreateRoomRequest {
            guild_id: "guild-1".to_owned(),
            channel_id: "channel-1".to_owned(),
            host_user_id: "42".to_owned(),
        })
        .await
        .expect("create room")
        .into_inner()
        .room_id;

    grpc.set_media(pb::SetMediaRequest {
        room_id: room_id.clone(),
        media_ref: Some(MediaRef {
            media_id: "media-1".to_owned(),
            kind: MediaKind::Hls as i32,
            url: "https://cdn.example/master.m3u8".to_owned(),
            duration_ms: 7_200_000,
            ..MediaRef::default()
        }),
    })
    .await
    .expect("set media");

    let mut host = connect(http_addr).await;
    let mut viewer = connect(http_addr).await;

    // 2. Host join: state (media present) then participants with one host.
    send(
        &mut host,
        json!({ "type": "join", "token": token(&room_id, "host") }),
    )
    .await;
    let host_state = recv_type(&mut host, "state", RECV_TIMEOUT).await;
    assert_eq!(host_state["type"], "state");
    assert!(
        !host_state["media_ref"].is_null(),
        "media_ref must be present"
    );
    assert_eq!(host_state["media_ref"]["media_id"], "media-1");
    assert!(
        host_state["version"].as_u64().expect("version") >= 2,
        "create + set_media must bump version to at least 2"
    );
    assert_eq!(host_state["playing"], false);

    let host_participants = recv_type(&mut host, "participants", RECV_TIMEOUT).await;
    assert_eq!(
        host_participants["participants"].as_array().unwrap().len(),
        1
    );
    assert_eq!(host_participants["participants"][0]["role"], "host");
    let participants_seq = host_participants["seq"]
        .as_u64()
        .expect("participants seq is required");

    // 3. Viewer join: viewer gets state + 2 participants; host is updated too.
    send(
        &mut viewer,
        json!({ "type": "join", "token": token(&room_id, "viewer") }),
    )
    .await;
    let viewer_state = recv_type(&mut viewer, "state", RECV_TIMEOUT).await;
    assert_eq!(viewer_state["type"], "state");
    let viewer_participants = recv_type(&mut viewer, "participants", RECV_TIMEOUT).await;
    assert_eq!(
        viewer_participants["participants"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let viewer_participants_seq = viewer_participants["seq"]
        .as_u64()
        .expect("participants seq is required");
    assert!(viewer_participants_seq >= participants_seq);

    let host_update = recv_type(&mut host, "participants", RECV_TIMEOUT).await;
    assert_eq!(host_update["participants"].as_array().unwrap().len(), 2);

    // 4. Host play: both sockets see state with playing=true and a higher version.
    let version_before = host_state["version"].as_u64().expect("version");
    send(&mut host, json!({ "type": "play", "request_id": 11 })).await;
    let host_played = recv_type(&mut host, "state", RECV_TIMEOUT).await;
    let viewer_played = recv_type(&mut viewer, "state", RECV_TIMEOUT).await;
    for played in [&host_played, &viewer_played] {
        assert_eq!(played["type"], "state");
        assert_eq!(played["playing"], true);
        assert!(
            played["version"].as_u64().expect("version") > version_before,
            "play must increment version"
        );
    }
    assert_eq!(host_played["version"], viewer_played["version"]);

    // 5. Viewer play is rejected; no state broadcast follows on the host socket.
    send(&mut viewer, json!({ "type": "play", "request_id": 12 })).await;
    let error = recv_type(&mut viewer, "error", RECV_TIMEOUT).await;
    assert_eq!(error["code"], "not_host");
    assert_silent(&mut host, Duration::from_millis(400)).await;

    // 6. Chat from the viewer fans out to both with the same text/seq.
    let chat_text = "починаємо!";
    send(
        &mut viewer,
        json!({ "type": "chat_send", "text": chat_text, "client_msg_id": "c-17", "request_id": 13 }),
    )
    .await;
    let host_chat = recv_type(&mut host, "chat", RECV_TIMEOUT).await;
    let viewer_chat = recv_type(&mut viewer, "chat", RECV_TIMEOUT).await;
    let chat_seq = host_chat["seq"].as_u64().expect("chat seq is required");
    for chat in [&host_chat, &viewer_chat] {
        assert_eq!(chat["type"], "chat");
        assert_eq!(chat["user_id"], "viewer-user");
        assert_eq!(chat["text"], chat_text);
        assert_eq!(chat["client_msg_id"], "c-17");
        assert_eq!(chat["seq"].as_u64().expect("chat seq"), chat_seq);
    }
    assert!(
        chat_seq > viewer_participants_seq,
        "room event seq must be monotonic: chat after participants"
    );

    // 7. Reaction uses the next event seq.
    send(
        &mut viewer,
        json!({ "type": "reaction", "emoji": "🍿", "request_id": 14 }),
    )
    .await;
    let host_reaction = recv_type(&mut host, "reaction", RECV_TIMEOUT).await;
    let viewer_reaction = recv_type(&mut viewer, "reaction", RECV_TIMEOUT).await;
    for reaction in [&host_reaction, &viewer_reaction] {
        assert_eq!(reaction["type"], "reaction");
        assert_eq!(reaction["user_id"], "viewer-user");
        assert_eq!(reaction["emoji"], "🍿");
        assert_eq!(
            reaction["seq"].as_u64().expect("reaction seq"),
            chat_seq + 1
        );
    }

    // 8. ping/pong echoes client_ts on the requesting connection.
    let client_ts = 1_763_300_000_123i64;
    send(&mut host, json!({ "type": "ping", "client_ts": client_ts })).await;
    let pong = recv_type(&mut host, "pong", RECV_TIMEOUT).await;
    assert_eq!(pong["client_ts"], client_ts);
    assert!(pong["server_ts"].as_i64().expect("server_ts") > 0);

    // 9. Spamming chat must trip the per-connection token bucket (protocol §9).
    for i in 0..6 {
        send(
            &mut viewer,
            json!({ "type": "chat_send", "text": format!("spam-{i}"), "request_id": 20 + i }),
        )
        .await;
    }
    let viewer_spam = drain(&mut viewer, Duration::from_millis(700)).await;
    let rate_limited: Vec<&Value> = viewer_spam
        .iter()
        .filter(|frame| frame["type"] == "error" && frame["code"] == "rate_limited")
        .collect();
    assert!(
        !rate_limited.is_empty(),
        "expected at least one rate_limited error, got: {viewer_spam:?}"
    );
    for error in rate_limited {
        if let Some(retry) = error["retry_after_ms"].as_u64() {
            assert!(retry > 0, "retry_after_ms must be positive");
        }
    }
    // Keep the host socket clean before the disconnect assertion.
    let _ = drain(&mut host, Duration::from_millis(500)).await;

    // 10. Viewer leaves: host receives a participants frame with one entry.
    drop(viewer);
    let host_after_leave = recv_type(&mut host, "participants", RECV_TIMEOUT).await;
    assert_eq!(
        host_after_leave["participants"].as_array().unwrap().len(),
        1
    );

    // 11. Bot closes the room: the host socket closes with code 1000 (§7, §10).
    grpc.close_room(pb::CloseRoomRequest {
        room_id: room_id.clone(),
    })
    .await
    .expect("close room");
    expect_close(&mut host, 1000).await;
}
