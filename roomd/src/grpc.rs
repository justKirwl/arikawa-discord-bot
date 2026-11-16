//! gRPC surface consumed by the Go bot.
//!
//! The handlers are wired in S3.1 and implemented task by task:
//! `CreateRoom`/`CloseRoom`/`GetRoomState`/`SetMedia` in S3.2,
//! `StreamRoomEvents` in S3.8.

use std::pin::Pin;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::pb::roomd::v1 as pb;

type EventStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<pb::RoomEvent, Status>> + Send>>;

#[derive(Debug, Default)]
pub struct RoomService;

#[tonic::async_trait]
impl pb::room_service_server::RoomService for RoomService {
    async fn create_room(
        &self,
        _request: Request<pb::CreateRoomRequest>,
    ) -> Result<Response<pb::CreateRoomResponse>, Status> {
        Err(Status::unimplemented("CreateRoom is implemented in S3.2"))
    }

    async fn close_room(
        &self,
        _request: Request<pb::CloseRoomRequest>,
    ) -> Result<Response<pb::CloseRoomResponse>, Status> {
        Err(Status::unimplemented("CloseRoom is implemented in S3.2"))
    }

    async fn get_room_state(
        &self,
        _request: Request<pb::GetRoomStateRequest>,
    ) -> Result<Response<pb::GetRoomStateResponse>, Status> {
        Err(Status::unimplemented("GetRoomState is implemented in S3.2"))
    }

    async fn set_media(
        &self,
        _request: Request<pb::SetMediaRequest>,
    ) -> Result<Response<pb::SetMediaResponse>, Status> {
        Err(Status::unimplemented("SetMedia is implemented in S3.2"))
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
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    Server::builder()
        .add_service(pb::room_service_server::RoomServiceServer::new(RoomService))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        .await?;

    Ok(())
}
