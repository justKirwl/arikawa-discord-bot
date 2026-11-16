//! gRPC surface consumed by the Go bot.
//!
//! Room lifecycle (`CreateRoom`, `CloseRoom`, `GetRoomState`, `SetMedia`) is
//! implemented in S3.2; `StreamRoomEvents` follows in S3.8.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::pb::roomd::v1 as pb;
use crate::state::{RoomSnapshot, RoomStore, StoreError};

type EventStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::RoomEvent, Status>> + Send>>;

#[derive(Debug, Clone)]
pub struct RoomService {
    store: Arc<RoomStore>,
}

impl RoomService {
    pub fn new(store: Arc<RoomStore>) -> Self {
        Self { store }
    }

    fn state_to_pb(snapshot: RoomSnapshot) -> pb::RoomState {
        pb::RoomState {
            room_id: snapshot.room_id,
            media_ref: snapshot.media_ref,
            position_ms: snapshot.position_ms,
            playing: snapshot.playing,
            updated_at_server_ms: snapshot.updated_at_server_ms,
            version: snapshot.version,
            // Participants are tracked from S3.6 onwards.
            participants: Vec::new(),
        }
    }
}

impl From<StoreError> for Status {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(room_id) => Status::not_found(format!("room {room_id} not found")),
            StoreError::InvalidArgument(message) => Status::invalid_argument(message),
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
            state: Some(Self::state_to_pb(snapshot)),
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

        tracing::info!(
            room_id = %request.room_id,
            version = snapshot.version,
            "media set"
        );

        Ok(Response::new(pb::SetMediaResponse {
            state: Some(Self::state_to_pb(snapshot)),
        }))
    }

    type StreamRoomEventsStream = EventStream;

    async fn stream_room_events(
        &self,
        _request: Request<pb::StreamRoomEventsRequest>,
    ) -> Result<Response<Self::StreamRoomEventsStream>, Status> {
        Err(Status::unimplemented(
            "StreamRoomEvents is implemented in S3.8",
        ))
    }
}

/// Serves the gRPC API on an already-bound listener.
pub async fn serve(
    listener: TcpListener,
    store: Arc<RoomStore>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    Server::builder()
        .add_service(pb::room_service_server::RoomServiceServer::new(
            RoomService::new(store),
        ))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await?;

    Ok(())
}
