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
    #[error("media is not set in this room")]
    NoMedia,
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

    fn position_at(&self, now: i64) -> i64 {
        let position_ms = if self.playing {
            self.position_ms
                .saturating_add(now.saturating_sub(self.updated_at_server_ms).max(0))
        } else {
            self.position_ms
        };

        match self.media_ref.as_ref() {
            Some(media) => clamp_position(position_ms, media.duration_ms),
            None => position_ms,
        }
    }

    /// Records an accepted mutation: every mutation bumps the version.
    fn commit(&mut self, now: i64) {
        self.updated_at_server_ms = now;
        self.version = self.version.saturating_add(1);
    }

    fn require_media(&self) -> Result<(), StoreError> {
        if self.media_ref.is_none() {
            return Err(StoreError::NoMedia);
        }

        Ok(())
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

    /// Starts or resumes playback. The position is materialized at the moment
    /// of the call, so a paused room resumes from where it stopped.
    pub fn play(&self, room_id: &str) -> Result<RoomSnapshot, StoreError> {
        self.play_at(room_id, now_ms())
    }

    /// Pauses playback, keeping the materialized position.
    pub fn pause(&self, room_id: &str) -> Result<RoomSnapshot, StoreError> {
        self.pause_at(room_id, now_ms())
    }

    /// Seeks to `position_ms`, clamped to `[0, duration_ms]`.
    ///
    /// The playing flag is preserved: seeking while playing keeps playing.
    pub fn seek(&self, room_id: &str, position_ms: i64) -> Result<RoomSnapshot, StoreError> {
        self.seek_at(room_id, position_ms, now_ms())
    }

    /// Deterministic variant of [`Self::play`] for tests.
    pub(crate) fn play_at(&self, room_id: &str, now: i64) -> Result<RoomSnapshot, StoreError> {
        let mut room = self
            .rooms
            .get_mut(room_id)
            .ok_or_else(|| StoreError::NotFound(room_id.to_owned()))?;
        room.require_media()?;

        room.position_ms = room.position_at(now);
        room.playing = true;
        room.commit(now);

        Ok(room.snapshot(room_id))
    }

    /// Deterministic variant of [`Self::pause`] for tests.
    pub(crate) fn pause_at(&self, room_id: &str, now: i64) -> Result<RoomSnapshot, StoreError> {
        let mut room = self
            .rooms
            .get_mut(room_id)
            .ok_or_else(|| StoreError::NotFound(room_id.to_owned()))?;
        room.require_media()?;

        room.position_ms = room.position_at(now);
        room.playing = false;
        room.commit(now);

        Ok(room.snapshot(room_id))
    }

    /// Deterministic variant of [`Self::seek`] for tests.
    pub(crate) fn seek_at(
        &self,
        room_id: &str,
        position_ms: i64,
        now: i64,
    ) -> Result<RoomSnapshot, StoreError> {
        let mut room = self
            .rooms
            .get_mut(room_id)
            .ok_or_else(|| StoreError::NotFound(room_id.to_owned()))?;
        room.require_media()?;

        let duration_ms = room.media_ref.as_ref().map_or(0, |media| media.duration_ms);
        room.position_ms = clamp_position(position_ms, duration_ms);
        room.commit(now);

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

fn clamp_position(position_ms: i64, duration_ms: i64) -> i64 {
    let position_ms = position_ms.max(0);

    // `duration_ms == 0` means the duration is unknown (media/v1/media.proto),
    // so only the lower bound applies.
    if duration_ms > 0 {
        position_ms.min(duration_ms)
    } else {
        position_ms
    }
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

    fn store_with_media() -> (RoomStore, RoomSnapshot) {
        let store = RoomStore::default();
        let created = store.create("guild", "channel", "host").unwrap();
        let with_media = store.set_media(&created.room_id, hls_media()).unwrap();

        (store, with_media)
    }

    #[test]
    fn play_starts_from_the_stored_position_and_bumps_version() {
        let (store, media_set) = store_with_media();
        let now = media_set.updated_at_server_ms + 1_500;

        let played = store.play_at(&media_set.room_id, now).expect("media set");

        assert!(played.playing);
        // The room was paused at 0, so a paused position does not advance.
        assert_eq!(played.position_ms, 0);
        assert_eq!(played.updated_at_server_ms, now);
        assert_eq!(played.version, media_set.version + 1);
        assert_eq!(store.get(&media_set.room_id).unwrap(), played);
    }

    #[test]
    fn repeated_play_keeps_elapsed_position() {
        let (store, media_set) = store_with_media();
        let start = media_set.updated_at_server_ms;

        let first = store
            .play_at(&media_set.room_id, start)
            .expect("first play");
        let second = store
            .play_at(&media_set.room_id, start + 2_500)
            .expect("second play");

        assert!(second.playing);
        assert_eq!(second.position_ms, 2_500);
        assert_eq!(second.updated_at_server_ms, start + 2_500);
        assert_eq!(second.version, first.version + 1);
    }

    #[test]
    fn pause_materializes_elapsed_time_and_clamps_to_duration() {
        let (store, media_set) = store_with_media();
        let start = media_set.updated_at_server_ms;
        let duration_ms = media_set.media_ref.as_ref().unwrap().duration_ms;

        store.play_at(&media_set.room_id, start).expect("play");
        let paused = store
            .pause_at(&media_set.room_id, start + duration_ms + 5)
            .expect("pause");

        assert!(!paused.playing);
        assert_eq!(paused.position_ms, duration_ms);
        assert_eq!(paused.updated_at_server_ms, start + duration_ms + 5);
        assert_eq!(paused.version, media_set.version + 2);
    }

    #[test]
    fn pause_while_paused_keeps_position() {
        let (store, media_set) = store_with_media();

        let paused = store
            .pause_at(&media_set.room_id, media_set.updated_at_server_ms + 5_000)
            .expect("pause");

        assert!(!paused.playing);
        assert_eq!(paused.position_ms, 0);
        assert_eq!(paused.version, media_set.version + 1);
    }

    #[test]
    fn seek_clamps_to_media_bounds() {
        let (store, media_set) = store_with_media();
        let now = media_set.updated_at_server_ms;
        let duration_ms = media_set.media_ref.as_ref().unwrap().duration_ms;

        let below = store
            .seek_at(&media_set.room_id, -500, now)
            .expect("seek below zero");
        assert_eq!(below.position_ms, 0);
        assert_eq!(below.version, media_set.version + 1);

        let above = store
            .seek_at(&media_set.room_id, duration_ms + 1, now)
            .expect("seek past end");
        assert_eq!(above.position_ms, duration_ms);
        assert_eq!(above.version, media_set.version + 2);
    }

    #[test]
    fn seek_with_unknown_duration_only_clamps_lower_bound() {
        let store = RoomStore::default();
        let created = store.create("guild", "channel", "host").unwrap();
        let mut media = hls_media();
        media.duration_ms = 0;
        let media_set = store.set_media(&created.room_id, media).unwrap();

        let above = store
            .seek_at(
                &media_set.room_id,
                9_999_999,
                media_set.updated_at_server_ms,
            )
            .expect("seek with unknown duration");
        assert_eq!(above.position_ms, 9_999_999);
        assert_eq!(above.version, media_set.version + 1);
    }

    #[test]
    fn seek_preserves_playing_state() {
        let (store, media_set) = store_with_media();
        let start = media_set.updated_at_server_ms;

        let played = store.play_at(&media_set.room_id, start).expect("play");
        let seeked = store
            .seek_at(&media_set.room_id, 60_000, start + 2_000)
            .expect("seek while playing");

        assert!(seeked.playing);
        assert_eq!(seeked.position_ms, 60_000);
        assert_eq!(seeked.updated_at_server_ms, start + 2_000);
        assert_eq!(seeked.version, played.version + 1);

        let paused = store
            .pause_at(&media_set.room_id, start + 3_000)
            .expect("pause");
        assert_eq!(paused.position_ms, 61_000);

        let seeked_paused = store
            .seek_at(&media_set.room_id, 10_000, start + 4_000)
            .expect("seek while paused");
        assert!(!seeked_paused.playing);
        assert_eq!(seeked_paused.position_ms, 10_000);
        assert_eq!(seeked_paused.version, paused.version + 1);
    }

    #[test]
    fn playback_without_media_is_rejected() {
        let store = RoomStore::default();
        let created = store.create("guild", "channel", "host").unwrap();

        for result in [
            store.play(&created.room_id),
            store.pause(&created.room_id),
            store.seek(&created.room_id, 1_000),
        ] {
            assert_eq!(result.unwrap_err(), StoreError::NoMedia);
        }
        assert_eq!(store.get(&created.room_id).unwrap(), created);
    }

    #[test]
    fn playback_on_missing_room_is_not_found() {
        let store = RoomStore::default();

        for result in [
            store.play("missing"),
            store.pause("missing"),
            store.seek("missing", 0),
        ] {
            assert_eq!(
                result.unwrap_err(),
                StoreError::NotFound("missing".to_owned())
            );
        }
    }
}
