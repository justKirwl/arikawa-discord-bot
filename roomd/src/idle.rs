//! Periodic idle sweeper (S3.7, protocol §7).
//!
//! Every five seconds the sweeper walks the live rooms and asks two questions:
//! did a playing room reach the end of its media, and has an empty room stayed
//! empty for longer than `room_idle_timeout`. The decision itself is pure in
//! [`sweep_room`] so the boundary conditions can be unit-tested; this module
//! only wires the decision to the store, the hub and the shutdown watch.

use std::collections::HashSet;

use tracing::info;

use crate::events::{RoomCloseReason, RoomEvent};
use crate::hub::RoomHub;
use crate::state::RoomStore;

/// One sweep decision, pure and testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepAction {
    pub room_id: String,
    pub close_for_idle: bool,
    pub fire_media_ended: bool,
}

/// Decides actions for one room from a snapshot + participant-empty timestamp.
///
/// Pure — unit-test this. `empty_since_ms`: Some(t) when the room has had no
/// participants since t, None when participants are present or unknown.
pub fn sweep_room(
    room_id: &str,
    snapshot: &crate::state::RoomSnapshot,
    empty_since_ms: Option<i64>,
    now_ms: i64,
    idle_timeout_ms: i64,
) -> SweepAction {
    let fire_media_ended = crate::media_end::has_ended(snapshot, now_ms);
    let close_for_idle = match empty_since_ms {
        // saturating_sub keeps a backwards clock from closing the room early.
        Some(since_ms) => now_ms.saturating_sub(since_ms) >= idle_timeout_ms,
        None => false,
    };

    SweepAction {
        room_id: room_id.to_owned(),
        close_for_idle,
        fire_media_ended,
    }
}

/// Spawns the periodic task (tick every 5 s, also wake on shutdown change).
pub fn spawn_idle_sweeper(
    store: std::sync::Arc<crate::state::RoomStore>,
    hub: std::sync::Arc<crate::hub::RoomHub>,
    idle_timeout: std::time::Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let idle_timeout_ms = i64::try_from(idle_timeout.as_millis()).unwrap_or(i64::MAX);

    tokio::spawn(async move {
        if *shutdown.borrow() {
            return;
        }

        // Rooms that already got a MediaEnded event; cleared once playback
        // stops so a later playback can fire its own event.
        let mut fired: HashSet<String> = HashSet::new();
        // A second receiver watches the same value; keeping it separate from
        // `shutdown` avoids a borrow conflict inside `select!`.
        let mut changed_rx = shutdown.clone();

        loop {
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    sweep_once(&store, &hub, idle_timeout_ms, &mut fired);
                }
                result = changed_rx.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    })
}

fn sweep_once(store: &RoomStore, hub: &RoomHub, idle_timeout_ms: i64, fired: &mut HashSet<String>) {
    let now_ms = crate::state::now_ms();

    for room_id in store.room_ids() {
        let snapshot = match store.get(&room_id) {
            Ok(snapshot) => snapshot,
            // Closed between listing and lookup; nothing left to do.
            Err(_) => continue,
        };

        if !snapshot.playing {
            fired.remove(&room_id);
        }

        let action = sweep_room(
            &room_id,
            &snapshot,
            hub.participants_empty_since(&room_id),
            now_ms,
            idle_timeout_ms,
        );

        if action.fire_media_ended && fired.insert(room_id.clone()) {
            let media_id = snapshot
                .media_ref
                .as_ref()
                .map(|media| media.media_id.clone())
                .unwrap_or_default();
            hub.events().publish(RoomEvent::MediaEnded {
                room_id: room_id.clone(),
                media_id,
            });
        }

        if action.close_for_idle {
            let _ = store.close(&room_id);
            // Drops the drained outbound queues; close_room publishes RoomClosed.
            let _ = hub.close_room(&room_id, RoomCloseReason::IdleTimeout);
            fired.remove(&room_id);
            info!(room_id = %room_id, reason = "idle_timeout", "closed idle room");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::media::v1::{MediaKind, MediaRef};
    use crate::state::RoomSnapshot;

    const TIMEOUT: i64 = 300_000;

    fn snapshot(
        playing: bool,
        duration_ms: i64,
        position_ms: i64,
        updated_at: i64,
    ) -> RoomSnapshot {
        RoomSnapshot {
            room_id: "r".to_owned(),
            media_ref: Some(MediaRef {
                media_id: "m".to_owned(),
                kind: MediaKind::Hls as i32,
                url: "https://example.test/master.m3u8".to_owned(),
                duration_ms,
                ..MediaRef::default()
            }),
            position_ms,
            playing,
            updated_at_server_ms: updated_at,
            version: 1,
        }
    }

    #[test]
    fn room_closes_exactly_at_the_timeout() {
        let snapshot = snapshot(false, 0, 0, 0);

        let action = sweep_room("r", &snapshot, Some(1_000), 1_000 + TIMEOUT, TIMEOUT);

        assert_eq!(action.room_id, "r");
        assert!(action.close_for_idle);
        assert!(!action.fire_media_ended);
    }

    #[test]
    fn room_is_kept_one_millisecond_before_the_timeout() {
        let snapshot = snapshot(false, 0, 0, 0);

        let action = sweep_room("r", &snapshot, Some(1_000), 1_000 + TIMEOUT - 1, TIMEOUT);

        assert!(!action.close_for_idle);
    }

    #[test]
    fn room_with_participants_is_never_idle() {
        let snapshot = snapshot(false, 0, 0, 0);

        let action = sweep_room("r", &snapshot, None, 999 * TIMEOUT, TIMEOUT);

        assert!(!action.close_for_idle);
    }

    #[test]
    fn backwards_clock_does_not_close_the_room() {
        let snapshot = snapshot(false, 0, 0, 0);

        let action = sweep_room("r", &snapshot, Some(2_000), 1_000, TIMEOUT);

        assert!(!action.close_for_idle);
    }

    #[test]
    fn playing_room_at_the_end_fires_media_ended_only() {
        let snapshot = snapshot(true, 1_000, 1_000, 1_000);

        let action = sweep_room("r", &snapshot, None, 2_000, TIMEOUT);

        assert!(action.fire_media_ended);
        assert!(!action.close_for_idle);
    }

    #[test]
    fn playing_room_before_the_end_does_not_fire() {
        let snapshot = snapshot(true, 10_000, 0, 1_000);

        let action = sweep_room("r", &snapshot, None, 1_000, TIMEOUT);

        assert!(!action.fire_media_ended);
    }

    #[test]
    fn paused_room_at_the_end_does_not_fire() {
        let snapshot = snapshot(false, 1_000, 1_000, 1_000);

        let action = sweep_room("r", &snapshot, None, 5_000, TIMEOUT);

        assert!(!action.fire_media_ended);
    }

    #[test]
    fn both_actions_can_be_decided_at_once() {
        let snapshot = snapshot(true, 1_000, 1_000, 1_000);

        let action = sweep_room("r", &snapshot, Some(1_000), 1_000 + TIMEOUT, TIMEOUT);

        assert!(action.fire_media_ended);
        assert!(action.close_for_idle);
    }
}
