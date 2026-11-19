//! gRPC surface consumed by the Go bot.
//!
//! Room lifecycle (`CreateRoom`, `CloseRoom`, `GetRoomState`, `SetMedia`) and
//! `StreamRoomEvents` are implemented (S3.2, S3.8).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::events::{RoomCloseReason, RoomEvent};
use crate::pb::roomd::v1 as pb;
use crate::state::{RoomSnapshot, RoomStore, StoreError};

type EventStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::RoomEvent, Status>> + Send>>;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// Maps the registry's `"host"` / `"viewer"` roles to the proto `Role` enum
/// (pb.rs exposes the enum field as `i32`).
fn role_to_pb(role: &str) -> i32 {
    match role {
        "host" => pb::Role::Host as i32,
        "viewer" => pb::Role::Viewer as i32,
        _ => pb::Role::Unspecified as i32,
    }
}

#[derive(Clone)]
pub struct RoomService {
    store: Arc<RoomStore>,
    hub: Arc<crate::hub::RoomHub>,
}

// Keep `Debug` without depending on `RoomHub` implementing it.
impl std::fmt::Debug for RoomService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoomService")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl RoomService {
    pub fn new(store: Arc<RoomStore>, hub: Arc<crate::hub::RoomHub>) -> Self {
        Self { store, hub }
    }

    fn state_to_pb(&self, snapshot: RoomSnapshot) -> pb::RoomState {
        let participants = self
            .hub
            .participants_list(&snapshot.room_id)
            .into_iter()
            .map(|participant| pb::Participant {
                user_id: participant.user_id,
                role: role_to_pb(participant.role),
                joined_at_server_ms: participant.joined_at_server_ms,
            })
            .collect();

        pb::RoomState {
            room_id: snapshot.room_id,
            media_ref: snapshot.media_ref,
            position_ms: snapshot.position_ms,
            playing: snapshot.playing,
            updated_at_server_ms: snapshot.updated_at_server_ms,
            version: snapshot.version,
            participants,
        }
    }

    /// Converts a bus event to the bot-facing proto event, returning `None`
    /// when the event belongs to another room.
    fn room_event_to_pb(event: RoomEvent, room_id: &str) -> Option<pb::RoomEvent> {
        let (event_room_id, kind) = match event {
            RoomEvent::ParticipantJoined {
                room_id,
                user_id,
                role,
                joined_at_server_ms,
            } => (
                room_id,
                pb::room_event::Event::ParticipantJoined(pb::ParticipantJoined {
                    participant: Some(pb::Participant {
                        user_id,
                        role: role_to_pb(role),
                        joined_at_server_ms,
                    }),
                }),
            ),
            RoomEvent::ParticipantLeft { room_id, user_id } => (
                room_id,
                pb::room_event::Event::ParticipantLeft(pb::ParticipantLeft { user_id }),
            ),
            RoomEvent::MediaEnded { room_id, media_id } => (
                room_id.clone(),
                pb::room_event::Event::MediaEnded(pb::MediaEnded { media_id, room_id }),
            ),
            RoomEvent::RoomClosed { room_id, reason } => (
                room_id,
                pb::room_event::Event::RoomClosed(pb::RoomClosed {
                    reason: reason.as_str().to_owned(),
                }),
            ),
        };

        if event_room_id != room_id {
            return None;
        }

        Some(pb::RoomEvent {
            room_id: event_room_id,
            // shortcut: bot does not consume the stream yet; thread a seq counter
            // through the bus when Go starts consuming (M3).
            sequence: 0,
            server_ts_ms: now_ms(),
            event: Some(kind),
        })
    }
}

impl From<StoreError> for Status {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(room_id) => Status::not_found(format!("room {room_id} not found")),
            StoreError::InvalidArgument(message) => Status::invalid_argument(message),
            StoreError::NoMedia => Status::failed_precondition("media is not set in this room"),
        }
    }
}

#[tonic::async_trait]
impl pb::room_service_server::RoomService for RoomService {
    async fn create_room(
        &self,
        request: Request<pb::CreateRoomRequest>,
    ) -> Result<Response<pb::CreateRoomResponse>, Status> {
        let request = request.into_inner();

        let snapshot = self.store.create(
            &request.guild_id,
            &request.channel_id,
            &request.host_user_id,
        )?;

        tracing::info!(
            room_id = %snapshot.room_id,
            guild_id = %request.guild_id,
            channel_id = %request.channel_id,
            "room created"
        );

        Ok(Response::new(pb::CreateRoomResponse {
            room_id: snapshot.room_id,
        }))
    }

    async fn close_room(
        &self,
        request: Request<pb::CloseRoomRequest>,
    ) -> Result<Response<pb::CloseRoomResponse>, Status> {
        let request = request.into_inner();
        let snapshot = self.store.close(&request.room_id)?;

        // Dropping the queues closes client sockets with code 1000 (protocol §7).
        let _ = self
            .hub
            .close_room(&request.room_id, RoomCloseReason::ClosedByBot);

        tracing::info!(
            room_id = %request.room_id,
            version = snapshot.version,
            "room closed"
        );

        Ok(Response::new(pb::CloseRoomResponse {}))
    }

    async fn get_room_state(
        &self,
        request: Request<pb::GetRoomStateRequest>,
    ) -> Result<Response<pb::GetRoomStateResponse>, Status> {
        let request = request.into_inner();
        let snapshot = self.store.get(&request.room_id)?;

        Ok(Response::new(pb::GetRoomStateResponse {
            state: Some(self.state_to_pb(snapshot)),
        }))
    }

    async fn set_media(
        &self,
        request: Request<pb::SetMediaRequest>,
    ) -> Result<Response<pb::SetMediaResponse>, Status> {
        let request = request.into_inner();
        let media_ref = request
            .media_ref
            .ok_or_else(|| Status::invalid_argument("media_ref is required"))?;

        let snapshot = self.store.set_media(&request.room_id, media_ref)?;

        // Protocol §4.1: state is broadcast after every accepted state change.
        self.hub.broadcast_state(&request.room_id, &snapshot);

        tracing::info!(
            room_id = %request.room_id,
            version = snapshot.version,
            "media set"
        );

        Ok(Response::new(pb::SetMediaResponse {
            state: Some(self.state_to_pb(snapshot)),
        }))
    }

    type StreamRoomEventsStream = EventStream;

    async fn stream_room_events(
        &self,
        request: Request<pb::StreamRoomEventsRequest>,
    ) -> Result<Response<Self::StreamRoomEventsStream>, Status> {
        let request = request.into_inner();
        if request.room_id.is_empty() {
            return Err(Status::invalid_argument("room_id is required"));
        }

        let room_id = request.room_id;
        let rx = self.hub.events().subscribe();

        let stream = futures_util::stream::unfold(rx, move |mut rx| {
            let room_id = room_id.clone();
            async move {
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            if let Some(event) = Self::room_event_to_pb(event, &room_id) {
                                return Some((Ok::<_, Status>(event), rx));
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped, "room event receiver lagged, dropping events");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }
}

/// Serves the gRPC API on an already-bound listener.
pub async fn serve(
    listener: TcpListener,
    store: Arc<RoomStore>,
    hub: Arc<crate::hub::RoomHub>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    Server::builder()
        .add_service(pb::room_service_server::RoomServiceServer::new(
            RoomService::new(store, hub),
        ))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_event_mapping_filters_and_maps_variants() {
        let joined = RoomEvent::ParticipantJoined {
            room_id: "room-1".to_owned(),
            user_id: "user-1".to_owned(),
            role: "host",
            joined_at_server_ms: 1_000,
        };
        assert!(RoomService::room_event_to_pb(joined.clone(), "room-2").is_none());

        let mapped = RoomService::room_event_to_pb(joined, "room-1").expect("mapped event");
        assert_eq!(mapped.room_id, "room-1");
        assert!(matches!(
            &mapped.event,
            Some(pb::room_event::Event::ParticipantJoined(joined))
                if joined.participant.as_ref().map(|p| p.user_id.as_str()) == Some("user-1")
                    && joined.participant.as_ref().map(|p| p.role) == Some(pb::Role::Host as i32)
                    && joined.participant.as_ref().map(|p| p.joined_at_server_ms) == Some(1_000)
        ));

        let closed = RoomService::room_event_to_pb(
            RoomEvent::RoomClosed {
                room_id: "room-1".to_owned(),
                reason: RoomCloseReason::ClosedByBot,
            },
            "room-1",
        )
        .expect("mapped event");
        assert!(matches!(
            &closed.event,
            Some(pb::room_event::Event::RoomClosed(reason)) if reason.reason == "closed_by_host"
        ));
    }
}
