//! WebSocket endpoint (protocol v0.1).
//!
//! S3.3 implements the connection envelope and `join` with Ed25519 token
//! verification. S3.4 adds host-only `play`/`pause`/`seek` state mutations.
//! S3.6–S3.10 add participant registry, chat/reactions, rate limiting,
//! backpressure and the split-socket writer: broadcasts fan out through each
//! connection's outbound queue so every client observes the same frame order.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinHandle;

use crate::auth::{AuthError, JwtAuth, Role};
use crate::backpressure::{Outbound, OutboundQueue, Priority, PushOutcome};
use crate::http::AppState;
use crate::hub::{Connection, RoomHub};
use crate::participants::{JoinError, Participant};
use crate::pb::media::v1::{MediaKind, MediaRef};
use crate::ratelimit::{Category, RateDecision, RateLimiter};
use crate::state::{RoomSnapshot, RoomStore, StoreError, now_ms};

/// Protocol §1: messages larger than 32 KiB are rejected.
pub const MAX_MESSAGE_BYTES: usize = 32 * 1024;

const CLOSE_NORMAL: u16 = 1000;
const CLOSE_UNSUPPORTED_DATA: u16 = 1003;
const CLOSE_POLICY_VIOLATION: u16 = 1008;
const CLOSE_TOO_LARGE: u16 = 1009;
const CLOSE_INTERNAL: u16 = 1011;
const CLOSE_SLOW_CONSUMER: u16 = 1013;

/// How long the writer gets to flush queued frames during disconnect cleanup.
const WRITER_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

type SocketSink = SplitSink<WebSocket, Message>;

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
    let hub = state.hub().clone();

    ws.max_message_size(MAX_MESSAGE_BYTES + 1)
        .max_frame_size(MAX_MESSAGE_BYTES + 1)
        .on_upgrade(move |socket| serve(socket, store, auth, hub))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaybackCommand {
    Play,
    Pause,
    Seek,
}

#[derive(Debug, Clone)]
struct Session {
    room_id: String,
    user_id: String,
    role: Role,
}

/// State owned by a joined connection: the hub's outbound queue plus the
/// writer task that drains it into the socket sink.
struct Joined {
    connection: Connection,
    writer: JoinHandle<()>,
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
    Participants {
        participants: Vec<ParticipantJson>,
        seq: i64,
        server_ts: i64,
    },
    Pong {
        client_ts: i64,
        server_ts: i64,
    },
    Chat {
        user_id: String,
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_msg_id: Option<String>,
        server_ts: i64,
        seq: i64,
    },
    Reaction {
        user_id: String,
        emoji: String,
        server_ts: i64,
        seq: i64,
    },
    Error {
        code: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<i64>,
    },
}

#[derive(Debug, Serialize)]
struct ParticipantJson {
    user_id: String,
    role: &'static str,
    joined_at_server_ms: i64,
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

fn encode(message: &ServerMessage) -> Option<String> {
    match serde_json::to_string(message) {
        Ok(payload) => Some(payload),
        Err(error) => {
            tracing::error!(%error, "failed to serialize websocket message");
            None
        }
    }
}

/// Server JSON serializers shared with `hub` for room-wide broadcasts.
pub(crate) fn state_json(s: &RoomSnapshot) -> String {
    encode(&ServerMessage::State {
        version: s.version,
        media_ref: s.media_ref.clone().map(MediaRefJson::from),
        position_ms: s.position_ms,
        playing: s.playing,
        server_ts: s.updated_at_server_ms,
    })
    .unwrap_or_default()
}

pub(crate) fn participants_json(p: &[Participant], seq: i64, server_ts: i64) -> String {
    encode(&ServerMessage::Participants {
        participants: p
            .iter()
            .map(|participant| ParticipantJson {
                user_id: participant.user_id.clone(),
                role: participant.role,
                joined_at_server_ms: participant.joined_at_server_ms,
            })
            .collect(),
        seq,
        server_ts,
    })
    .unwrap_or_default()
}

pub(crate) fn chat_json(m: &crate::chat::ChatMessage, server_ts: i64, seq: i64) -> String {
    encode(&ServerMessage::Chat {
        user_id: m.user_id.clone(),
        text: m.text.clone(),
        client_msg_id: m.client_msg_id.clone(),
        server_ts,
        seq,
    })
    .unwrap_or_default()
}

pub(crate) fn reaction_json(r: &crate::chat::Reaction, server_ts: i64, seq: i64) -> String {
    encode(&ServerMessage::Reaction {
        user_id: r.user_id.clone(),
        emoji: r.emoji.clone(),
        server_ts,
        seq,
    })
    .unwrap_or_default()
}

/// Drains the connection's queue into the socket sink. Every frame is written
/// in enqueue order; no per-frame logging.
async fn write_loop(mut sink: SocketSink, queue: OutboundQueue) {
    while let Some(outbound) = queue.recv().await {
        match outbound {
            Outbound::Message(text) => {
                if sink.send(Message::text(text)).await.is_err() {
                    break;
                }
            }
            Outbound::Close { code, reason } => {
                let _ = sink
                    .send(Message::Close(Some(CloseFrame {
                        code,
                        reason: reason.into(),
                    })))
                    .await;
                break;
            }
        }
    }
}

/// An essential push that found the queue at hard capacity ends the
/// connection. Returns whether the caller may continue.
fn handle_overwhelmed(outcome: PushOutcome, queue: &OutboundQueue) -> bool {
    if outcome == PushOutcome::Overwhelmed {
        // shortcut: error{slow_consumer} is dropped because the queue is already
        // at hard capacity; request_close enqueues the 1013 frame directly.
        queue.request_close(CLOSE_SLOW_CONSUMER, "slow consumer");
        tracing::warn!("websocket outbound queue overwhelmed; closing with 1013");
        false
    } else {
        true
    }
}

/// Pushes an already-serialized essential frame to a joined connection.
fn reply_raw(joined: &Joined, payload: String) -> bool {
    let queue = &joined.connection.queue;
    handle_overwhelmed(queue.push(Priority::Essential, payload), queue)
}

/// Pushes a typed essential frame to a joined connection.
fn reply_json(joined: &Joined, message: &ServerMessage) -> bool {
    match encode(message) {
        Some(payload) => reply_raw(joined, payload),
        None => false,
    }
}

/// Sends an error either through the queue (joined) or directly through the
/// sink (pre-join, where no broadcasts exist yet).
async fn reply_error(
    sink: &mut Option<SocketSink>,
    joined: &Option<Joined>,
    code: &'static str,
    message: Option<String>,
    request_id: Option<i64>,
    retry_after_ms: Option<i64>,
) -> bool {
    let Some(payload) = encode(&ServerMessage::Error {
        code,
        message,
        request_id,
        retry_after_ms,
    }) else {
        return false;
    };

    if let Some(joined) = joined {
        reply_raw(joined, payload)
    } else if let Some(sink) = sink.as_mut() {
        sink.send(Message::text(payload)).await.is_ok()
    } else {
        false
    }
}

/// Schedules a close on the queue (joined) or writes it directly (pre-join).
async fn reply_close(
    sink: &mut Option<SocketSink>,
    joined: &Option<Joined>,
    code: u16,
    reason: &'static str,
) -> bool {
    if let Some(joined) = joined {
        joined.connection.queue.request_close(code, reason);
        true
    } else if let Some(sink) = sink.as_mut() {
        sink.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await
        .is_ok()
    } else {
        false
    }
}

/// Outcome of a rate-limit check. The connection-level `bool` each handler
/// returns means "keep the socket open": a rejected command must not be
/// processed, but only repeated violations close the connection.
enum RateCheck {
    /// Consume the command normally.
    Allow,
    /// `rate_limited` replied; skip this command but keep the connection.
    Skip,
    /// `rate_limited` replied and 1008 close scheduled; end the connection.
    Disconnect,
}

async fn check_rate(
    sink: &mut Option<SocketSink>,
    joined: &Option<Joined>,
    limiter: &mut RateLimiter,
    category: Category,
    now: i64,
    request_id: Option<i64>,
) -> RateCheck {
    let RateDecision::Deny { retry_after_ms } = limiter.check(category, now) else {
        return RateCheck::Allow;
    };
    reply_error(
        sink,
        joined,
        "rate_limited",
        None,
        request_id,
        Some(retry_after_ms),
    )
    .await;
    if limiter.should_disconnect(now) {
        reply_close(
            sink,
            joined,
            CLOSE_POLICY_VIOLATION,
            "rate limit violations",
        )
        .await;
        RateCheck::Disconnect
    } else {
        RateCheck::Skip
    }
}

async fn serve(socket: WebSocket, store: Arc<RoomStore>, auth: Arc<JwtAuth>, hub: Arc<RoomHub>) {
    let (sink, mut stream) = socket.split();
    // `sink` is moved into the writer task on successful join; until then the
    // reader uses it directly for the few pre-join error frames.
    let mut sink = Some(sink);
    let mut joined: Option<Joined> = None;
    let mut session: Option<Session> = None;
    let mut limiter = RateLimiter::new();

    while let Some(message) = stream.next().await {
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
                    reply_error(
                        &mut sink,
                        &joined,
                        "message_too_large",
                        Some("message exceeds 32 KiB".to_owned()),
                        None,
                        None,
                    )
                    .await;
                    reply_close(&mut sink, &joined, CLOSE_TOO_LARGE, "message too large").await;
                    break;
                }
                if !handle_text(
                    &mut sink,
                    &mut joined,
                    &mut limiter,
                    &store,
                    &auth,
                    &hub,
                    &mut session,
                    text.as_str(),
                )
                .await
                {
                    break;
                }
            }
            Message::Binary(_) => {
                reply_close(
                    &mut sink,
                    &joined,
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

    if let (Some(session), Some(joined)) = (session, joined) {
        tracing::info!(
            room_id = %session.room_id,
            user_id = %session.user_id,
            role = session.role.as_str(),
            "websocket disconnected"
        );

        let now = now_ms();
        hub.detach(&session.room_id, joined.connection.id, now);
        hub.broadcast_participants(&session.room_id, now);
        joined
            .connection
            .queue
            .request_close(CLOSE_NORMAL, "connection closed");

        let mut writer = joined.writer;
        if tokio::time::timeout(WRITER_DRAIN_TIMEOUT, &mut writer)
            .await
            .is_err()
        {
            writer.abort();
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_text(
    sink: &mut Option<SocketSink>,
    joined: &mut Option<Joined>,
    limiter: &mut RateLimiter,
    store: &RoomStore,
    auth: &JwtAuth,
    hub: &RoomHub,
    session: &mut Option<Session>,
    text: &str,
) -> bool {
    let value: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(error) => {
            return reply_error(
                sink,
                joined,
                "bad_message",
                Some(format!("invalid JSON: {error}")),
                None,
                None,
            )
            .await;
        }
    };

    let Some(object) = value.as_object() else {
        return reply_error(
            sink,
            joined,
            "bad_message",
            Some("message must be a JSON object".to_owned()),
            None,
            None,
        )
        .await;
    };
    let Some(kind) = object.get("type").and_then(Value::as_str) else {
        return reply_error(
            sink,
            joined,
            "bad_message",
            Some("type is required".to_owned()),
            None,
            None,
        )
        .await;
    };
    let request_id = object.get("request_id").and_then(Value::as_i64);

    match kind {
        "join" => handle_join(sink, joined, store, auth, hub, session, object, request_id).await,
        "ping" => {
            if session.is_none() {
                return reject_unauthenticated(sink, joined, request_id).await;
            }
            match check_rate(sink, joined, limiter, Category::Ping, now_ms(), request_id).await {
                RateCheck::Allow => {}
                RateCheck::Skip => return true,
                RateCheck::Disconnect => return false,
            }
            let Some(client_ts) = object.get("client_ts").and_then(Value::as_i64) else {
                return reply_error(
                    sink,
                    joined,
                    "bad_message",
                    Some("client_ts is required".to_owned()),
                    request_id,
                    None,
                )
                .await;
            };
            let Some(joined) = joined.as_ref() else {
                return false;
            };
            reply_json(
                joined,
                &ServerMessage::Pong {
                    client_ts,
                    server_ts: now_ms(),
                },
            )
        }
        "play" | "pause" | "seek" => {
            let command = match kind {
                "play" => PlaybackCommand::Play,
                "pause" => PlaybackCommand::Pause,
                _ => PlaybackCommand::Seek,
            };
            handle_playback(
                sink, joined, limiter, store, hub, session, command, object, request_id,
            )
            .await
        }
        "chat_send" | "reaction" => {
            if session.is_none() {
                return reject_unauthenticated(sink, joined, request_id).await;
            }
            handle_social(
                sink, joined, limiter, hub, session, kind, object, request_id,
            )
            .await
        }
        _ => {
            reply_error(
                sink,
                joined,
                "unknown_type",
                Some(format!("unknown message type {kind:?}")),
                request_id,
                None,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_join(
    sink: &mut Option<SocketSink>,
    joined: &mut Option<Joined>,
    store: &RoomStore,
    auth: &JwtAuth,
    hub: &RoomHub,
    session: &mut Option<Session>,
    object: &serde_json::Map<String, Value>,
    request_id: Option<i64>,
) -> bool {
    if session.is_some() {
        return reply_error(
            sink,
            joined,
            "already_joined",
            Some("this connection already joined a room".to_owned()),
            request_id,
            None,
        )
        .await;
    }

    let Some(token) = object.get("token").and_then(Value::as_str) else {
        return reply_error(
            sink,
            joined,
            "bad_message",
            Some("token is required".to_owned()),
            request_id,
            None,
        )
        .await;
    };

    let claims = match auth.verify(token) {
        Ok(claims) => claims,
        Err(AuthError::Expired) => {
            reply_error(
                sink,
                joined,
                "token_expired",
                Some("token has expired".to_owned()),
                request_id,
                None,
            )
            .await;
            reply_close(sink, joined, CLOSE_POLICY_VIOLATION, "token expired").await;
            return false;
        }
        Err(AuthError::Invalid) => {
            reply_error(
                sink,
                joined,
                "invalid_token",
                Some("token verification failed".to_owned()),
                request_id,
                None,
            )
            .await;
            reply_close(sink, joined, CLOSE_POLICY_VIOLATION, "invalid token").await;
            return false;
        }
        Err(AuthError::Config(message)) => {
            tracing::error!(%message, "jwt verifier misconfigured");
            reply_error(
                sink,
                joined,
                "internal",
                Some("token verification is unavailable".to_owned()),
                request_id,
                None,
            )
            .await;
            reply_close(sink, joined, CLOSE_INTERNAL, "internal error").await;
            return false;
        }
    };

    let snapshot = match store.get(&claims.room_id) {
        Ok(snapshot) => snapshot,
        Err(StoreError::NotFound(room_id)) => {
            reply_error(
                sink,
                joined,
                "room_not_found",
                Some(format!("room {room_id} not found")),
                request_id,
                None,
            )
            .await;
            reply_close(sink, joined, CLOSE_POLICY_VIOLATION, "room not found").await;
            return false;
        }
        Err(StoreError::InvalidArgument(message)) => {
            reply_error(sink, joined, "bad_message", Some(message), request_id, None).await;
            reply_close(sink, joined, CLOSE_POLICY_VIOLATION, "bad join request").await;
            return false;
        }
        Err(error) => {
            tracing::error!(%error, "failed to load room state");
            reply_error(
                sink,
                joined,
                "internal",
                Some("room state is unavailable".to_owned()),
                request_id,
                None,
            )
            .await;
            reply_close(sink, joined, CLOSE_INTERNAL, "internal error").await;
            return false;
        }
    };

    let now = now_ms();
    let connection = match hub.attach(&claims.room_id, &claims.user_id, claims.role.as_str(), now) {
        Ok(connection) => connection,
        Err(JoinError::RoomFull) => {
            // shortcut: `room_full` is not in protocol §11 (frozen v0.1); used to
            // enforce the 50-participant cap (§14). Upgrade: contract-change commit
            // reviewed by the Go developer before the code string is relied upon.
            return reply_error(
                sink,
                joined,
                "room_full",
                Some("room is full".to_owned()),
                request_id,
                None,
            )
            .await;
        }
    };

    let queue = connection.queue.clone();
    let Some(socket_sink) = sink.take() else {
        return false;
    };
    let writer = tokio::spawn(write_loop(socket_sink, queue));
    *session = Some(Session {
        room_id: claims.room_id.clone(),
        user_id: claims.user_id.clone(),
        role: claims.role,
    });
    *joined = Some(Joined { connection, writer });

    // Protocol §3.1: the joiner receives `state` first, then `participants`
    // via the same room-wide broadcast every other member receives.
    let Some(joined) = joined.as_ref() else {
        return false;
    };
    if !reply_raw(joined, state_json(&snapshot)) {
        return false;
    }
    hub.broadcast_participants(&claims.room_id, now);

    tracing::info!(
        room_id = %claims.room_id,
        user_id = %claims.user_id,
        role = claims.role.as_str(),
        "websocket joined"
    );

    true
}

#[allow(clippy::too_many_arguments)]
async fn handle_playback(
    sink: &mut Option<SocketSink>,
    joined: &mut Option<Joined>,
    limiter: &mut RateLimiter,
    store: &RoomStore,
    hub: &RoomHub,
    session: &Option<Session>,
    command: PlaybackCommand,
    object: &serde_json::Map<String, Value>,
    request_id: Option<i64>,
) -> bool {
    let Some(session) = session.as_ref() else {
        return reject_unauthenticated(sink, joined, request_id).await;
    };

    match check_rate(
        sink,
        joined,
        limiter,
        Category::Control,
        now_ms(),
        request_id,
    )
    .await
    {
        RateCheck::Allow => {}
        RateCheck::Skip => return true,
        RateCheck::Disconnect => return false,
    }

    if session.role != Role::Host {
        return reply_error(
            sink,
            joined,
            "not_host",
            Some("only the host can control playback".to_owned()),
            request_id,
            None,
        )
        .await;
    }

    let result = match command {
        PlaybackCommand::Play => store.play(&session.room_id),
        PlaybackCommand::Pause => store.pause(&session.room_id),
        PlaybackCommand::Seek => {
            let Some(position_ms) = object.get("position_ms").and_then(Value::as_i64) else {
                return reply_error(
                    sink,
                    joined,
                    "bad_message",
                    Some("position_ms must be an integer".to_owned()),
                    request_id,
                    None,
                )
                .await;
            };
            store.seek(&session.room_id, position_ms)
        }
    };

    match result {
        Ok(snapshot) => {
            // The broadcast includes the caller, so no separate reply is sent.
            hub.broadcast_state(&session.room_id, &snapshot);
            true
        }
        Err(StoreError::NoMedia) => {
            reply_error(
                sink,
                joined,
                "no_media",
                Some("media is not set in this room".to_owned()),
                request_id,
                None,
            )
            .await
        }
        Err(StoreError::NotFound(room_id)) => {
            reply_error(
                sink,
                joined,
                "room_closed",
                Some(format!("room {room_id} is closed")),
                request_id,
                None,
            )
            .await;
            reply_close(sink, joined, CLOSE_NORMAL, "room closed").await;
            false
        }
        Err(StoreError::InvalidArgument(message)) => {
            reply_error(sink, joined, "bad_message", Some(message), request_id, None).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_social(
    sink: &mut Option<SocketSink>,
    joined: &mut Option<Joined>,
    limiter: &mut RateLimiter,
    hub: &RoomHub,
    session: &Option<Session>,
    kind: &str,
    object: &serde_json::Map<String, Value>,
    request_id: Option<i64>,
) -> bool {
    let Some(session) = session.as_ref() else {
        return reject_unauthenticated(sink, joined, request_id).await;
    };

    let now = now_ms();
    let is_chat = kind == "chat_send";
    let category = if is_chat {
        Category::Chat
    } else {
        Category::Reaction
    };
    match check_rate(sink, joined, limiter, category, now, request_id).await {
        RateCheck::Allow => {}
        RateCheck::Skip => return true,
        RateCheck::Disconnect => return false,
    }

    if is_chat {
        let message = match crate::chat::parse_chat(object, &session.user_id) {
            Ok(message) => message,
            Err(code) => {
                return reply_error(
                    sink,
                    joined,
                    code,
                    Some(format!("invalid {kind} message")),
                    request_id,
                    None,
                )
                .await;
            }
        };
        hub.broadcast_chat(&session.room_id, &message, now);
    } else {
        let reaction = match crate::chat::parse_reaction(object, &session.user_id) {
            Ok(reaction) => reaction,
            Err(code) => {
                return reply_error(
                    sink,
                    joined,
                    code,
                    Some(format!("invalid {kind} message")),
                    request_id,
                    None,
                )
                .await;
            }
        };
        hub.broadcast_reaction(&session.room_id, &reaction, now);
    }

    true
}

async fn reject_unauthenticated(
    sink: &mut Option<SocketSink>,
    joined: &Option<Joined>,
    request_id: Option<i64>,
) -> bool {
    reply_error(
        sink,
        joined,
        "invalid_token",
        Some("join a room before sending commands".to_owned()),
        request_id,
        None,
    )
    .await;
    reply_close(sink, joined, CLOSE_POLICY_VIOLATION, "not joined").await;
    false
}
