//! WebSocket endpoint (protocol v0.1).
//!
//! S3.3 implements the connection envelope and `join` with Ed25519 token
//! verification. Playback mutations, broadcasting and participants follow in
//! S3.4–S3.6.

use std::sync::Arc;

use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::Value;

use crate::auth::{AuthError, JwtAuth, Role};
use crate::http::AppState;
use crate::pb::media::v1::{MediaKind, MediaRef};
use crate::state::{RoomSnapshot, RoomStore, StoreError, now_ms};

/// Protocol §1: messages larger than 32 KiB are rejected.
pub const MAX_MESSAGE_BYTES: usize = 32 * 1024;

const CLOSE_NORMAL: u16 = 1000;
const CLOSE_UNSUPPORTED_DATA: u16 = 1003;
const CLOSE_POLICY_VIOLATION: u16 = 1008;
const CLOSE_TOO_LARGE: u16 = 1009;

/// Upgrades `GET /ws` and hands the socket to the message loop.
pub async fn handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    let Some(auth) = state.jwt().cloned() else {
        tracing::error!("websocket rejected: ROOMD_JWT_PUBLIC_KEY is not configured");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "jwt verification is not configured",
        )
            .into_response();
    };
    let store = state.store().clone();

    ws.max_message_size(MAX_MESSAGE_BYTES + 1)
        .max_frame_size(MAX_MESSAGE_BYTES + 1)
        .on_upgrade(move |socket| serve(socket, store, auth))
}

#[derive(Debug, Clone)]
struct Session {
    room_id: String,
    user_id: String,
    role: Role,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    State {
        version: u64,
        media_ref: Option<MediaRefJson>,
        position_ms: i64,
        playing: bool,
        server_ts: i64,
    },
    Pong {
        client_ts: i64,
        server_ts: i64,
    },
    Error {
        code: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<i64>,
    },
}

#[derive(Debug, Serialize)]
struct MediaRefJson {
    media_id: String,
    kind: &'static str,
    url: String,
    duration_ms: i64,
    audio_tracks: Vec<AudioTrackJson>,
    subtitle_tracks: Vec<SubtitleTrackJson>,
    poster_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct AudioTrackJson {
    id: String,
    language: String,
    label: String,
    default: bool,
}

#[derive(Debug, Serialize)]
struct SubtitleTrackJson {
    id: String,
    language: String,
    label: String,
    url: String,
    default: bool,
}

impl From<MediaRef> for MediaRefJson {
    fn from(media: MediaRef) -> Self {
        let kind = match MediaKind::try_from(media.kind) {
            Ok(MediaKind::Hls) => "hls",
            _ => "file",
        };

        Self {
            media_id: media.media_id,
            kind,
            url: media.url,
            duration_ms: media.duration_ms,
            audio_tracks: media
                .audio_tracks
                .into_iter()
                .map(|track| AudioTrackJson {
                    id: track.id,
                    language: track.language,
                    label: track.label,
                    default: track.default,
                })
                .collect(),
            subtitle_tracks: media
                .subtitle_tracks
                .into_iter()
                .map(|track| SubtitleTrackJson {
                    id: track.id,
                    language: track.language,
                    label: track.label,
                    url: track.url,
                    default: track.default,
                })
                .collect(),
            poster_url: (!media.poster_url.is_empty()).then_some(media.poster_url),
        }
    }
}

fn state_message(snapshot: RoomSnapshot) -> ServerMessage {
    ServerMessage::State {
        version: snapshot.version,
        media_ref: snapshot.media_ref.map(MediaRefJson::from),
        position_ms: snapshot.position_ms,
        playing: snapshot.playing,
        server_ts: snapshot.updated_at_server_ms,
    }
}

async fn serve(mut socket: WebSocket, store: Arc<RoomStore>, auth: Arc<JwtAuth>) {
    let mut session: Option<Session> = None;

    while let Some(message) = socket.recv().await {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                tracing::debug!(%error, "websocket receive error");
                break;
            }
        };

        match message {
            Message::Text(text) => {
                if text.len() > MAX_MESSAGE_BYTES {
                    send_error(
                        &mut socket,
                        "message_too_large",
                        Some("message exceeds 32 KiB".to_owned()),
                        None,
                    )
                    .await;
                    close(&mut socket, CLOSE_TOO_LARGE, "message too large").await;
                    break;
                }
                if !handle_text(&mut socket, &store, &auth, &mut session, text.as_str()).await {
                    break;
                }
            }
            Message::Binary(_) => {
                close(
                    &mut socket,
                    CLOSE_UNSUPPORTED_DATA,
                    "binary frames are not supported",
                )
                .await;
                break;
            }
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) => {}
        }
    }

    if let Some(session) = session {
        tracing::info!(
            room_id = %session.room_id,
            user_id = %session.user_id,
            role = session.role.as_str(),
            "websocket disconnected"
        );
    }
}

async fn handle_text(
    socket: &mut WebSocket,
    store: &RoomStore,
    auth: &JwtAuth,
    session: &mut Option<Session>,
    text: &str,
) -> bool {
    let value: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(error) => {
            return send_error(
                socket,
                "bad_message",
                Some(format!("invalid JSON: {error}")),
                None,
            )
            .await;
        }
    };

    let Some(object) = value.as_object() else {
        return send_error(
            socket,
            "bad_message",
            Some("message must be a JSON object".to_owned()),
            None,
        )
        .await;
    };
    let Some(kind) = object.get("type").and_then(Value::as_str) else {
        return send_error(
            socket,
            "bad_message",
            Some("type is required".to_owned()),
            None,
        )
        .await;
    };
    let request_id = object.get("request_id").and_then(Value::as_i64);

    match kind {
        "join" => handle_join(socket, store, auth, session, object, request_id).await,
        "ping" => {
            if session.is_none() {
                return reject_unauthenticated(socket, request_id).await;
            }
            let Some(client_ts) = object.get("client_ts").and_then(Value::as_i64) else {
                return send_error(
                    socket,
                    "bad_message",
                    Some("client_ts is required".to_owned()),
                    request_id,
                )
                .await;
            };
            send_json(
                socket,
                &ServerMessage::Pong {
                    client_ts,
                    server_ts: now_ms(),
                },
            )
            .await
        }
        "play" | "pause" | "seek" | "chat_send" | "reaction" => {
            if session.is_none() {
                return reject_unauthenticated(socket, request_id).await;
            }
            send_error(
                socket,
                "internal",
                Some(format!("{kind} is not implemented yet")),
                request_id,
            )
            .await
        }
        _ => {
            send_error(
                socket,
                "unknown_type",
                Some(format!("unknown message type {kind:?}")),
                request_id,
            )
            .await
        }
    }
}

async fn handle_join(
    socket: &mut WebSocket,
    store: &RoomStore,
    auth: &JwtAuth,
    session: &mut Option<Session>,
    object: &serde_json::Map<String, Value>,
    request_id: Option<i64>,
) -> bool {
    if session.is_some() {
        return send_error(
            socket,
            "already_joined",
            Some("this connection already joined a room".to_owned()),
            request_id,
        )
        .await;
    }

    let Some(token) = object.get("token").and_then(Value::as_str) else {
        return send_error(
            socket,
            "bad_message",
            Some("token is required".to_owned()),
            request_id,
        )
        .await;
    };

    let claims = match auth.verify(token) {
        Ok(claims) => claims,
        Err(AuthError::Expired) => {
            send_error(
                socket,
                "token_expired",
                Some("token has expired".to_owned()),
                request_id,
            )
            .await;
            close(socket, CLOSE_POLICY_VIOLATION, "token expired").await;
            return false;
        }
        Err(AuthError::Invalid) => {
            send_error(
                socket,
                "invalid_token",
                Some("token verification failed".to_owned()),
                request_id,
            )
            .await;
            close(socket, CLOSE_POLICY_VIOLATION, "invalid token").await;
            return false;
        }
        Err(AuthError::Config(message)) => {
            tracing::error!(%message, "jwt verifier misconfigured");
            send_error(
                socket,
                "internal",
                Some("token verification is unavailable".to_owned()),
                request_id,
            )
            .await;
            close(socket, CLOSE_NORMAL, "internal error").await;
            return false;
        }
    };

    let snapshot = match store.get(&claims.room_id) {
        Ok(snapshot) => snapshot,
        Err(StoreError::NotFound(room_id)) => {
            send_error(
                socket,
                "room_not_found",
                Some(format!("room {room_id} not found")),
                request_id,
            )
            .await;
            close(socket, CLOSE_POLICY_VIOLATION, "room not found").await;
            return false;
        }
        Err(StoreError::InvalidArgument(message)) => {
            send_error(socket, "bad_message", Some(message), request_id).await;
            close(socket, CLOSE_POLICY_VIOLATION, "bad join request").await;
            return false;
        }
    };

    if !send_json(socket, &state_message(snapshot)).await {
        return false;
    }

    tracing::info!(
        room_id = %claims.room_id,
        user_id = %claims.user_id,
        role = claims.role.as_str(),
        "websocket joined"
    );
    *session = Some(Session {
        room_id: claims.room_id,
        user_id: claims.user_id,
        role: claims.role,
    });

    true
}

async fn reject_unauthenticated(socket: &mut WebSocket, request_id: Option<i64>) -> bool {
    send_error(
        socket,
        "invalid_token",
        Some("join a room before sending commands".to_owned()),
        request_id,
    )
    .await;
    close(socket, CLOSE_POLICY_VIOLATION, "not joined").await;
    false
}

async fn send_json(socket: &mut WebSocket, message: &ServerMessage) -> bool {
    match serde_json::to_string(message) {
        Ok(payload) => socket.send(Message::text(payload)).await.is_ok(),
        Err(error) => {
            tracing::error!(%error, "failed to serialize websocket message");
            false
        }
    }
}

async fn send_error(
    socket: &mut WebSocket,
    code: &'static str,
    message: Option<String>,
    request_id: Option<i64>,
) -> bool {
    send_json(
        socket,
        &ServerMessage::Error {
            code,
            message,
            request_id,
        },
    )
    .await
}

async fn close(socket: &mut WebSocket, code: u16, reason: &'static str) {
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await;
}
