//! Detection of playback reaching the end of its media.
//!
//! A periodic sweeper calls [`has_ended`] for every playing room. When it
//! returns `true` the caller fires the gRPC `MediaEnded` event. This module is
//! a pure predicate: it never mutates the room state or bumps `version`.
//! Per protocol §5, `MediaEnded` does not change `version` until the bot
//! calls `SetMedia` (S6.3) — detection only.

use crate::state::RoomSnapshot;

/// True when the room is playing, media duration is known (> 0) and the
/// materialized position at now_ms has reached duration_ms.
pub fn has_ended(snapshot: &RoomSnapshot, now_ms: i64) -> bool {
    if !snapshot.playing {
        return false;
    }

    // `duration_ms == 0` means the duration is unknown (media/v1/media.proto),
    // so an unknown-duration item can never be reported as ended.
    let duration_ms = match snapshot.media_ref.as_ref() {
        Some(media) if media.duration_ms > 0 => media.duration_ms,
        _ => return false,
    };

    // `Room::position_at` is private, so duplicate its tiny playback formula
    // here. We skip its clamp-to-duration step: for this comparison the raw
    // position is equivalent, and reaching exactly duration_ms already ends.
    let position_ms = snapshot
        .position_ms
        .saturating_add(now_ms.saturating_sub(snapshot.updated_at_server_ms).max(0));

    position_ms >= duration_ms
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::media::v1::MediaRef;

    fn snapshot(playing: bool, position_ms: i64, duration_ms: i64) -> RoomSnapshot {
        RoomSnapshot {
            room_id: "room-1".to_owned(),
            media_ref: Some(MediaRef {
                duration_ms,
                ..MediaRef::default()
            }),
            position_ms,
            playing,
            updated_at_server_ms: 1_000,
            version: 1,
        }
    }

    #[test]
    fn paused_never_ends() {
        // Even far past the duration, a paused room must not fire.
        let snapshot = snapshot(false, 0, 10_000);

        assert!(!has_ended(&snapshot, 1_000 + 99_999));
    }

    #[test]
    fn unknown_duration_never_ends() {
        // duration_ms == 0 means unknown, not "zero-length".
        let snapshot = snapshot(true, 0, 0);

        assert!(!has_ended(&snapshot, 1_000 + 99_999));
    }

    #[test]
    fn playing_ends_past_duration() {
        let snapshot = snapshot(true, 9_000, 10_000);

        assert!(has_ended(&snapshot, 1_000 + 2_000));
    }

    #[test]
    fn playing_ends_exactly_at_boundary() {
        let snapshot = snapshot(true, 9_000, 10_000);

        assert!(has_ended(&snapshot, 1_000 + 1_000));
    }

    #[test]
    fn playing_before_boundary_does_not_end() {
        let snapshot = snapshot(true, 9_000, 10_000);

        assert!(!has_ended(&snapshot, 1_000 + 999));
    }
}
