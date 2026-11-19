use std::sync::Arc;
use std::time::Duration;

use roomd::grpc;
use roomd::hub::RoomHub;
use roomd::pb::media::v1::{MediaKind, MediaRef};
use roomd::pb::roomd::v1 as pb;
use roomd::state::RoomStore;
use tokio::net::TcpListener;

async fn start_server() -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral port");
    let addr = listener.local_addr().expect("bound address");

    let server = tokio::spawn(grpc::serve(
        listener,
        Arc::new(RoomStore::default()),
        Arc::new(RoomHub::new()),
        std::future::pending(),
    ));

    (format!("http://{addr}"), server)
}

async fn client(
    endpoint: &str,
) -> pb::room_service_client::RoomServiceClient<tonic::transport::Channel> {
    tokio::time::timeout(
        Duration::from_secs(5),
        pb::room_service_client::RoomServiceClient::connect(endpoint.to_owned()),
    )
    .await
    .expect("connect timeout")
    .expect("connect")
}

#[tokio::test]
async fn room_lifecycle_over_grpc() {
    let (endpoint, server) = start_server().await;
    let mut client = client(&endpoint).await;

    let created = client
        .create_room(pb::CreateRoomRequest {
            guild_id: "guild-1".to_owned(),
            channel_id: "channel-1".to_owned(),
            host_user_id: "user-1".to_owned(),
        })
        .await
        .expect("create room")
        .into_inner();
    assert!(!created.room_id.is_empty());

    let state = client
        .get_room_state(pb::GetRoomStateRequest {
            room_id: created.room_id.clone(),
        })
        .await
        .expect("get state")
        .into_inner()
        .state
        .expect("state present");
    assert_eq!(state.room_id, created.room_id);
    assert_eq!(state.version, 1);
    assert_eq!(state.position_ms, 0);
    assert!(!state.playing);
    assert!(state.media_ref.is_none());
    assert!(state.participants.is_empty());

    let with_media = client
        .set_media(pb::SetMediaRequest {
            room_id: created.room_id.clone(),
            media_ref: Some(MediaRef {
                media_id: "media-1".to_owned(),
                kind: MediaKind::Hls as i32,
                url: "https://cdn.example/master.m3u8".to_owned(),
                duration_ms: 7_200_000,
                ..MediaRef::default()
            }),
        })
        .await
        .expect("set media")
        .into_inner()
        .state
        .expect("state present");
    assert_eq!(with_media.version, state.version + 1);
    assert_eq!(with_media.position_ms, 0);
    assert!(!with_media.playing);
    assert_eq!(
        with_media.media_ref.expect("media present").media_id,
        "media-1"
    );

    client
        .close_room(pb::CloseRoomRequest {
            room_id: created.room_id.clone(),
        })
        .await
        .expect("close room");

    let error = client
        .get_room_state(pb::GetRoomStateRequest {
            room_id: created.room_id,
        })
        .await
        .expect_err("closed room must be gone");
    assert_eq!(error.code(), tonic::Code::NotFound);

    server.abort();
}

#[tokio::test]
async fn invalid_create_request_is_rejected() {
    let (endpoint, server) = start_server().await;
    let mut client = client(&endpoint).await;

    let error = client
        .create_room(pb::CreateRoomRequest::default())
        .await
        .expect_err("empty guild_id must fail");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    server.abort();
}
