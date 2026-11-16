//! In-memory room registry.
//!
//! S3.2 keeps rooms in process memory only: the Go bot owns durable state
//! (Postgres). Restarting `roomd` drops rooms by design.

use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use thiserror::Error;

use crate::pb::media::v1::{MediaKind, MediaRef};

/// Immutable view of a room at a single point in time.
#[derive(Debug, Clone, PartialEq)]
pub struct RoomSnapshot {
    pub room_id: String,
    pub media_ref: Option<MediaRef>,
    pub position_ms: i64,
    pub playing: bool,
    pub updated_at_server_ms: i64,
    pub version: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StoreError {
    #[error("room {0} not found")]
    NotFound(String),
    #[error("{0}")]
    InvalidArgument(String),
}

#[derive(Debug, Clone)]
struct Room {
    media_ref: Option<MediaRef>,
    position_ms: i64,
    playing: bool,
    updated_at_server_ms: i64,
    version: u64,
}

impl Room {
    fn snapshot(&self, room_id: &str) -> RoomSnapshot {
        RoomSnapshot {
            room_id: room_id.to_owned(),
            media_ref: self.media_ref.clone(),
            position_ms: self.position_ms,
            playing: self.playing,
            updated_at_server_ms: self.updated_at_server_ms,
            version: self.version,
        }
    }
}

#[derive(Debug, Default)]
pub struct RoomStore {
    rooms: DashMap<String, Room>,
}

impl RoomStore {
    /// Creates an empty room with version 1 and no media.
    pub fn create(
        &self,
        guild_id: &str,
        channel_id: &str,
        host_user_id: &str,
    ) -> Result<RoomSnapshot, StoreError> {
        validate_id("guild_id", guild_id)?;
        validate_id("channel_id", channel_id)?;
        validate_id("host_user_id", host_user_id)?;

        let room_id = uuid::Uuid::now_v7().to_string();
        let room = Room {
            media_ref: None,
            position_ms: 0,
            playing: false,
            updated_at_server_ms: now_ms(),
            version: 1,
        };
        let snapshot = room.snapshot(&room_id);
        self.rooms.insert(room_id, room);

        Ok(snapshot)
    }

    pub fn get(&self, room_id: &str) -> Result<RoomSnapshot, StoreError> {
        self.rooms
            .get(room_id)
            .map(|room| room.snapshot(room_id))
            .ok_or_else(|| StoreError::NotFound(room_id.to_owned()))
    }

    /// Replaces the media item: playback resets to position 0 and pauses.
    pub fn set_media(
        &self,
        room_id: &str,
        media_ref: MediaRef,
    ) -> Result<RoomSnapshot, StoreError> {
        validate_media(&media_ref)?;

        let mut room = self
            .rooms
            .get_mut(room_id)
            .ok_or_else(|| StoreError::NotFound(room_id.to_owned()))?;

        room.media_ref = Some(media_ref);
        room.position_ms = 0;
        room.playing = false;
        room.updated_at_server_ms = now_ms();
        room.version += 1;

        Ok(room.snapshot(room_id))
    }

    pub fn close(&self, room_id: &str) -> Result<RoomSnapshot, StoreError> {
        let (_, room) = self
            .rooms
            .remove(room_id)
            .ok_or_else(|| StoreError::NotFound(room_id.to_owned()))?;

        Ok(room.snapshot(room_id))
    }

    pub fn len(&self) -> usize {
        self.rooms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rooms.is_empty()
    }
}

fn validate_id(field: &str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        return Err(StoreError::InvalidArgument(format!(
            "{field} must not be empty"
        )));
    }
    if value.len() > 128 {
        return Err(StoreError::InvalidArgument(format!(
            "{field} must be at most 128 bytes"
        )));
    }

    Ok(())
}

fn validate_media(media_ref: &MediaRef) -> Result<(), StoreError> {
    validate_id("media_ref.media_id", &media_ref.media_id)?;

    if media_ref.url.trim().is_empty() {
        return Err(StoreError::InvalidArgument(
            "media_ref.url must not be empty".to_owned(),
        ));
    }
    if !(media_ref.url.starts_with("https://") || media_ref.url.starts_with("http://")) {
        return Err(StoreError::InvalidArgument(
            "media_ref.url must be an http(s) URL".to_owned(),
        ));
    }

    match media_ref.kind() {
        MediaKind::File | MediaKind::Hls => {}
        MediaKind::Unspecified => {
            return Err(StoreError::InvalidArgument(
                "media_ref.kind must be file or hls".to_owned(),
            ));
        }
    }

    if media_ref.duration_ms < 0 {
        return Err(StoreError::InvalidArgument(
            "media_ref.duration_ms must not be negative".to_owned(),
        ));
    }

    Ok(())
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hls_media() -> MediaRef {
        MediaRef {
            media_id: "m-1".to_owned(),
            kind: MediaKind::Hls as i32,
            url: "https://cdn.example/media/master.m3u8".to_owned(),
            duration_ms: 7_200_000,
            ..MediaRef::default()
        }
    }

    #[test]
    fn create_returns_initial_state() {
        let store = RoomStore::default();

        let room = store.create("guild", "channel", "host").expect("room");

        assert!(!room.room_id.is_empty());
        assert_eq!(room.version, 1);
        assert_eq!(room.position_ms, 0);
        assert!(!room.playing);
        assert!(room.media_ref.is_none());
        assert!(room.updated_at_server_ms > 0);
        assert_eq!(store.len(), 1);
        assert_eq!(store.get(&room.room_id).unwrap(), room);
    }

    #[test]
    fn create_rejects_empty_ids() {
        let store = RoomStore::default();

        let error = store.create("", "channel", "host").unwrap_err();

        assert!(matches!(error, StoreError::InvalidArgument(_)));
        assert!(store.is_empty());
    }

    #[test]
    fn get_missing_room_is_not_found() {
        let store = RoomStore::default();

        assert_eq!(
            store.get("missing").unwrap_err(),
            StoreError::NotFound("missing".to_owned())
        );
    }

    #[test]
    fn set_media_resets_playback_and_bumps_version() {
        let store = RoomStore::default();
        let created = store.create("guild", "channel", "host").unwrap();

        let updated = store
            .set_media(&created.room_id, hls_media())
            .expect("media is valid");

        assert_eq!(updated.version, created.version + 1);
        assert_eq!(updated.position_ms, 0);
        assert!(!updated.playing);
        assert_eq!(updated.media_ref.expect("media").media_id, "m-1");
        assert!(updated.updated_at_server_ms >= created.updated_at_server_ms);
    }

    #[test]
    fn set_media_rejects_unspecified_kind() {
        let store = RoomStore::default();
        let created = store.create("guild", "channel", "host").unwrap();
        let mut media = hls_media();
        media.kind = MediaKind::Unspecified as i32;

        let error = store.set_media(&created.room_id, media).unwrap_err();

        assert!(matches!(error, StoreError::InvalidArgument(_)));
    }

    #[test]
    fn set_media_rejects_non_http_url() {
        let store = RoomStore::default();
        let created = store.create("guild", "channel", "host").unwrap();
        let mut media = hls_media();
        media.url = "file:///etc/passwd".to_owned();

        let error = store.set_media(&created.room_id, media).unwrap_err();

        assert!(matches!(error, StoreError::InvalidArgument(_)));
    }

    #[test]
    fn set_media_on_missing_room_is_not_found() {
        let store = RoomStore::default();

        let error = store.set_media("missing", hls_media()).unwrap_err();

        assert_eq!(error, StoreError::NotFound("missing".to_owned()));
    }

    #[test]
    fn close_removes_the_room() {
        let store = RoomStore::default();
        let created = store.create("guild", "channel", "host").unwrap();

        store.close(&created.room_id).expect("room exists");

        assert!(store.is_empty());
        assert_eq!(
            store.close(&created.room_id).unwrap_err(),
            StoreError::NotFound(created.room_id)
        );
    }
}
