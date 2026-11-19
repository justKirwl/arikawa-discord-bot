//! WebSocket integration tests: `join` (S3.3) and playback state (S3.4).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use roomd::auth::{JWT_LEEWAY_SECS, JwtAuth};
use roomd::http::{self, AppState};
use roomd::hub::RoomHub;
use roomd::pb::media::v1::{AudioTrack, MediaKind, MediaRef, SubtitleTrack};
use roomd::state::RoomStore;
use roomd::ws::MAX_MESSAGE_BYTES;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const SEED: [u8; 32] = [7u8; 32];
const OTHER_SEED: [u8; 32] = [9u8; 32];
const KEY_ID: &str = "test-key";

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

fn jwt_auth() -> JwtAuth {
    let public = URL_SAFE_NO_PAD.encode(SigningKey::from_bytes(&SEED).verifying_key().to_bytes());
    JwtAuth::new(
        &public,
        Some("cinema-bot".to_owned()),
        Some(KEY_ID.to_owned()),
    )
    .expect("test key is valid")
}

fn token(seed: [u8; 32], room_id: &str, role: &str, exp_offset: i64) -> String {
    let claims = TestClaims {
        iss: "cinema-bot",
        sub: "42",
        guild_id: "guild-1",
        room_id,
        user_id: "42",
        role,
        iat: now(),
        exp: (now() as i64 + exp_offset) as u64,
    };
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(KEY_ID.to_owned());
    encode(&header, &claims, &encoding_key(seed)).expect("token encoding")
}

async fn start_server(store: Arc<RoomStore>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let addr = listener.local_addr().expect("bound address");
    let state =
        AppState::with_services(store, Some(Arc::new(jwt_auth())), Arc::new(RoomHub::new()));

    tokio::spawn(http::serve(listener, state, std::future::pending()));

    addr
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

async fn recv(socket: &mut Client) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("message timeout")
        .expect("stream ended")
        .expect("websocket error");
    match message {
        Message::Text(text) => serde_json::from_str(text.as_str()).expect("json"),
        other => panic!("expected text frame, got {other:?}"),
    }
}

async fn recv_close(socket: &mut Client) -> u16 {
    let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("close timeout")
        .expect("stream ended")
        .expect("websocket error");
    match message {
        Message::Close(Some(frame)) => frame.code.into(),
        other => panic!("expected close frame, got {other:?}"),
    }
}

fn create_room(store: &RoomStore) -> String {
    store
        .create("guild-1", "channel-1", "42")
        .expect("room created")
        .room_id
}

fn set_media(store: &RoomStore, room_id: &str) {
    store
        .set_media(
            room_id,
            MediaRef {
                media_id: "media-1".to_owned(),
                kind: MediaKind::Hls as i32,
                url: "https://cdn.example/master.m3u8".to_owned(),
                duration_ms: 7_200_000,
                ..MediaRef::default()
            },
        )
        .expect("media set");
}

async fn join(socket: &mut Client, room_id: &str, role: &str) -> Value {
    send(
        socket,
        json!({ "type": "join", "token": token(SEED, room_id, role, 120) }),
    )
    .await;
    // Protocol §3.1: the joiner receives `state` first, then `participants`.
    let state = recv(socket).await;
    assert_eq!(state["type"], "state");
    assert_eq!(recv(socket).await["type"], "participants");
    state
}

#[tokio::test]
async fn join_sends_state_and_ping_gets_pong() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    store
        .set_media(
            &room_id,
            MediaRef {
                media_id: "media-1".to_owned(),
                kind: MediaKind::Hls as i32,
                url: "https://cdn.example/master.m3u8?sig=1".to_owned(),
                duration_ms: 7_200_000,
                audio_tracks: vec![AudioTrack {
                    id: "a1".to_owned(),
                    language: "uk".to_owned(),
                    label: "Українська".to_owned(),
                    default: true,
                }],
                subtitle_tracks: vec![SubtitleTrack {
                    id: "s1".to_owned(),
                    language: "uk".to_owned(),
                    label: "Українські".to_owned(),
                    url: "https://cdn.example/sub.vtt".to_owned(),
                    default: true,
                }],
                poster_url: "https://cdn.example/poster.jpg".to_owned(),
            },
        )
        .expect("media set");
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    send(
        &mut socket,
        json!({ "type": "join", "token": token(SEED, &room_id, "host", 120), "request_id": 1 }),
    )
    .await;

    let state = recv(&mut socket).await;
    assert_eq!(state["type"], "state");
    assert_eq!(state["version"], 2);
    assert_eq!(state["position_ms"], 0);
    assert_eq!(state["playing"], false);
    assert!(state["server_ts"].as_i64().unwrap() > 0);

    let media = &state["media_ref"];
    assert_eq!(media["media_id"], "media-1");
    assert_eq!(media["kind"], "hls");
    assert_eq!(media["duration_ms"], 7_200_000);
    assert_eq!(media["audio_tracks"][0]["language"], "uk");
    assert_eq!(media["audio_tracks"][0]["default"], true);
    assert_eq!(
        media["subtitle_tracks"][0]["url"],
        "https://cdn.example/sub.vtt"
    );
    assert_eq!(media["poster_url"], "https://cdn.example/poster.jpg");

    // Protocol §3.1: `state` is followed by `participants` for the joiner.
    assert_eq!(recv(&mut socket).await["type"], "participants");

    send(&mut socket, json!({ "type": "ping", "client_ts": 1234 })).await;
    let pong = recv(&mut socket).await;
    assert_eq!(pong["type"], "pong");
    assert_eq!(pong["client_ts"], 1234);
    assert!(pong["server_ts"].as_i64().unwrap() > 0);

    send(
        &mut socket,
        json!({ "type": "join", "token": token(SEED, &room_id, "host", 120) }),
    )
    .await;
    let error = recv(&mut socket).await;
    assert_eq!(error["type"], "error");
    assert_eq!(error["code"], "already_joined");
}

#[tokio::test]
async fn viewer_role_is_accepted() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    send(
        &mut socket,
        json!({ "type": "join", "token": token(SEED, &room_id, "viewer", 120) }),
    )
    .await;

    let message = recv(&mut socket).await;
    assert_eq!(message["type"], "state");
}

#[tokio::test]
async fn invalid_signature_closes_with_1008() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    send(
        &mut socket,
        json!({ "type": "join", "token": token(OTHER_SEED, &room_id, "host", 120) }),
    )
    .await;

    let error = recv(&mut socket).await;
    assert_eq!(error["code"], "invalid_token");
    assert_eq!(recv_close(&mut socket).await, 1008);
}

#[tokio::test]
async fn expired_token_closes_with_1008() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    send(
        &mut socket,
        json!({
            "type": "join",
            "token": token(SEED, &room_id, "host", -(JWT_LEEWAY_SECS as i64) - 1),
        }),
    )
    .await;

    let error = recv(&mut socket).await;
    assert_eq!(error["code"], "token_expired");
    assert_eq!(recv_close(&mut socket).await, 1008);
}

#[tokio::test]
async fn unknown_room_closes_with_1008() {
    let addr = start_server(Arc::new(RoomStore::default())).await;
    let mut socket = connect(addr).await;

    send(
        &mut socket,
        json!({ "type": "join", "token": token(SEED, "missing-room", "host", 120) }),
    )
    .await;

    let error = recv(&mut socket).await;
    assert_eq!(error["code"], "room_not_found");
    assert_eq!(recv_close(&mut socket).await, 1008);
}

#[tokio::test]
async fn invalid_role_is_rejected() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    send(
        &mut socket,
        json!({ "type": "join", "token": token(SEED, &room_id, "admin", 120) }),
    )
    .await;

    let error = recv(&mut socket).await;
    assert_eq!(error["code"], "invalid_token");
    assert_eq!(recv_close(&mut socket).await, 1008);
}

#[tokio::test]
async fn malformed_messages_keep_the_connection_open() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    send(
        &mut socket,
        json!({ "type": "join", "token": token(SEED, &room_id, "host", 120) }),
    )
    .await;
    assert_eq!(recv(&mut socket).await["type"], "state");
    assert_eq!(recv(&mut socket).await["type"], "participants");

    socket.send(Message::text("{")).await.expect("send");
    assert_eq!(recv(&mut socket).await["code"], "bad_message");

    send(&mut socket, json!({ "type": "no_such_type" })).await;
    assert_eq!(recv(&mut socket).await["code"], "unknown_type");

    send(&mut socket, json!({ "type": "ping", "client_ts": 7 })).await;
    let pong = recv(&mut socket).await;
    assert_eq!(pong["type"], "pong");
    assert_eq!(pong["client_ts"], 7);
}

#[tokio::test]
async fn binary_frames_close_with_1003() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    send(
        &mut socket,
        json!({ "type": "join", "token": token(SEED, &room_id, "host", 120) }),
    )
    .await;
    assert_eq!(recv(&mut socket).await["type"], "state");
    assert_eq!(recv(&mut socket).await["type"], "participants");

    socket
        .send(Message::binary(vec![1, 2, 3]))
        .await
        .expect("send");
    assert_eq!(recv_close(&mut socket).await, 1003);
}

#[tokio::test]
async fn oversized_messages_close_with_1009() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    send(
        &mut socket,
        json!({ "type": "join", "token": token(SEED, &room_id, "host", 120) }),
    )
    .await;
    assert_eq!(recv(&mut socket).await["type"], "state");
    assert_eq!(recv(&mut socket).await["type"], "participants");

    socket
        .send(Message::text("x".repeat(MAX_MESSAGE_BYTES + 1)))
        .await
        .expect("send");
    assert_eq!(recv(&mut socket).await["code"], "message_too_large");
    assert_eq!(recv_close(&mut socket).await, 1009);
}

#[tokio::test]
async fn commands_before_join_are_rejected() {
    let addr = start_server(Arc::new(RoomStore::default())).await;
    let mut socket = connect(addr).await;

    send(&mut socket, json!({ "type": "play" })).await;

    let error = recv(&mut socket).await;
    assert_eq!(error["code"], "invalid_token");
    assert_eq!(recv_close(&mut socket).await, 1008);
}

#[tokio::test]
async fn host_play_pause_and_seek_bump_version() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    set_media(&store, &room_id);
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    let joined = join(&mut socket, &room_id, "host").await;
    assert_eq!(joined["type"], "state");
    assert_eq!(joined["version"], 2);
    assert_eq!(joined["playing"], false);

    send(&mut socket, json!({ "type": "play", "request_id": 11 })).await;
    let played = recv(&mut socket).await;
    assert_eq!(played["type"], "state");
    assert_eq!(played["version"], 3);
    assert_eq!(played["playing"], true);
    let played_position = played["position_ms"].as_i64().expect("position");

    send(&mut socket, json!({ "type": "pause", "request_id": 12 })).await;
    let paused = recv(&mut socket).await;
    assert_eq!(paused["type"], "state");
    assert_eq!(paused["version"], 4);
    assert_eq!(paused["playing"], false);
    assert!(paused["position_ms"].as_i64().expect("position") >= played_position);

    send(
        &mut socket,
        json!({ "type": "seek", "position_ms": 60_000, "request_id": 13 }),
    )
    .await;
    let seeked = recv(&mut socket).await;
    assert_eq!(seeked["type"], "state");
    assert_eq!(seeked["version"], 5);
    assert_eq!(seeked["position_ms"], 60_000);
    assert_eq!(seeked["playing"], false);

    send(&mut socket, json!({ "type": "seek", "position_ms": -500 })).await;
    let clamped_low = recv(&mut socket).await;
    assert_eq!(clamped_low["version"], 6);
    assert_eq!(clamped_low["position_ms"], 0);

    send(
        &mut socket,
        json!({ "type": "seek", "position_ms": 7_200_001 }),
    )
    .await;
    let clamped_high = recv(&mut socket).await;
    assert_eq!(clamped_high["version"], 7);
    assert_eq!(clamped_high["position_ms"], 7_200_000);

    // All seven rapid controls fit the §9 control burst of 10 (no sleep needed).
    send(&mut socket, json!({ "type": "play" })).await;
    let playing_again = recv(&mut socket).await;
    assert_eq!(playing_again["version"], 8);
    assert_eq!(playing_again["playing"], true);
    assert!(playing_again["server_ts"].as_i64().expect("server_ts") > 0);

    send(&mut socket, json!({ "type": "seek", "position_ms": 1_000 })).await;
    let seeked_while_playing = recv(&mut socket).await;
    assert_eq!(seeked_while_playing["version"], 9);
    assert_eq!(seeked_while_playing["playing"], true);
    assert_eq!(seeked_while_playing["position_ms"], 1_000);
}

#[tokio::test]
async fn viewer_playback_is_rejected_with_not_host() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    set_media(&store, &room_id);
    let addr = start_server(store).await;
    let mut socket = connect(addr).await;

    assert_eq!(join(&mut socket, &room_id, "viewer").await["version"], 2);

    for (request_id, message) in [
        (21, json!({ "type": "play", "request_id": 21 })),
        (22, json!({ "type": "pause", "request_id": 22 })),
        (
            23,
            json!({ "type": "seek", "position_ms": 100, "request_id": 23 }),
        ),
    ] {
        send(&mut socket, message).await;
        let error = recv(&mut socket).await;
        assert_eq!(error["type"], "error");
        assert_eq!(error["code"], "not_host");
        assert_eq!(error["request_id"], request_id);
    }

    send(&mut socket, json!({ "type": "ping", "client_ts": 1 })).await;
    assert_eq!(recv(&mut socket).await["type"], "pong");
}

#[tokio::test]
async fn playback_without_media_is_rejected() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    let addr = start_server(store.clone()).await;
    let mut socket = connect(addr).await;

    assert_eq!(join(&mut socket, &room_id, "host").await["version"], 1);

    for message in [
        json!({ "type": "play" }),
        json!({ "type": "pause" }),
        json!({ "type": "seek", "position_ms": 5 }),
    ] {
        send(&mut socket, message).await;
        let error = recv(&mut socket).await;
        assert_eq!(error["type"], "error");
        assert_eq!(error["code"], "no_media");
    }

    assert_eq!(store.get(&room_id).expect("room").version, 1);
}

#[tokio::test]
async fn seek_requires_an_integer_position() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    set_media(&store, &room_id);
    let addr = start_server(store.clone()).await;
    let mut socket = connect(addr).await;

    assert_eq!(join(&mut socket, &room_id, "host").await["version"], 2);

    for message in [
        json!({ "type": "seek" }),
        json!({ "type": "seek", "position_ms": "120000" }),
        json!({ "type": "seek", "position_ms": 1.5 }),
    ] {
        send(&mut socket, message).await;
        let error = recv(&mut socket).await;
        assert_eq!(error["type"], "error");
        assert_eq!(error["code"], "bad_message");
    }

    assert_eq!(store.get(&room_id).expect("room").version, 2);
}

#[tokio::test]
async fn playback_after_room_close_reports_room_closed() {
    let store = Arc::new(RoomStore::default());
    let room_id = create_room(&store);
    set_media(&store, &room_id);
    let addr = start_server(store.clone()).await;
    let mut socket = connect(addr).await;

    assert_eq!(join(&mut socket, &room_id, "host").await["type"], "state");

    store.close(&room_id).expect("room closed");

    send(&mut socket, json!({ "type": "play" })).await;
    let error = recv(&mut socket).await;
    assert_eq!(error["type"], "error");
    assert_eq!(error["code"], "room_closed");
    assert_eq!(recv_close(&mut socket).await, 1000);
}
