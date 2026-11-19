//! Per-room participant registry.
//!
//! S3.6 keeps participants in process memory only (DashMap, no I/O). The
//! registry also owns the monotonic per-room event `seq` shared by the
//! `participants`, `chat` and `reaction` messages (protocol §2).

use std::collections::HashMap;

use dashmap::DashMap;
use thiserror::Error;

/// Maximum number of distinct participants in one room (protocol §14).
pub const MAX_PARTICIPANTS: usize = 50;

/// One participant as exposed in the `participants` message (protocol §4.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Participant {
    pub user_id: String,
    pub role: &'static str, // "host" / "viewer"
    pub joined_at_server_ms: i64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum JoinError {
    #[error("room is full")]
    RoomFull,
}

#[derive(Debug, Default)]
struct RoomEntry {
    participants: HashMap<String, Participant>,
    seq: i64,
    empty_since: Option<i64>,
}

/// Thread-safe, in-memory registry of participants, keyed by room id.
#[derive(Debug, Default)]
pub struct ParticipantRegistry {
    rooms: DashMap<String, RoomEntry>,
}

impl ParticipantRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Upserts by `user_id`: a reconnect refreshes `joined_at` (and `role`).
    /// A new user is rejected when the room is already at [`MAX_PARTICIPANTS`].
    pub fn add(
        &self,
        room_id: &str,
        user_id: &str,
        role: &'static str,
        now_ms: i64,
    ) -> Result<Participant, JoinError> {
        let mut entry = self.rooms.entry(room_id.to_owned()).or_default();
        if !entry.participants.contains_key(user_id) && entry.participants.len() >= MAX_PARTICIPANTS
        {
            return Err(JoinError::RoomFull);
        }

        let participant = Participant {
            user_id: user_id.to_owned(),
            role,
            joined_at_server_ms: now_ms,
        };
        entry
            .participants
            .insert(user_id.to_owned(), participant.clone());
        // A room with participants is no longer idle.
        entry.empty_since = None;

        Ok(participant)
    }

    /// Removes one participant. Returns `true` only when the room existed,
    /// contained `user_id` and became empty with this removal.
    pub fn remove(&self, room_id: &str, user_id: &str, now_ms: i64) -> bool {
        let mut entry = match self.rooms.get_mut(room_id) {
            Some(entry) => entry,
            None => return false,
        };
        if entry.participants.remove(user_id).is_none() {
            return false;
        }
        if entry.participants.is_empty() {
            entry.empty_since = Some(now_ms);
            true
        } else {
            false
        }
    }

    /// Participants sorted by join time, then user id, for stable output.
    pub fn list(&self, room_id: &str) -> Vec<Participant> {
        let entry = match self.rooms.get(room_id) {
            Some(entry) => entry,
            None => return Vec::new(),
        };
        let mut participants: Vec<Participant> = entry.participants.values().cloned().collect();
        participants.sort_by(|a, b| {
            a.joined_at_server_ms
                .cmp(&b.joined_at_server_ms)
                .then_with(|| a.user_id.cmp(&b.user_id))
        });
        participants
    }

    /// Monotonic per-room event sequence; the first call returns 1.
    pub fn next_seq(&self, room_id: &str) -> i64 {
        let mut entry = self.rooms.entry(room_id.to_owned()).or_default();
        entry.seq = entry.seq.saturating_add(1);
        entry.seq
    }

    /// `Some(empty_since_ms)` while the room exists with zero participants.
    pub fn empty_since(&self, room_id: &str) -> Option<i64> {
        self.rooms.get(room_id).and_then(|entry| entry.empty_since)
    }

    /// Drops all bookkeeping for a room (called when the room closes).
    pub fn remove_room(&self, room_id: &str) {
        self.rooms.remove(room_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_is_sorted_by_joined_at_then_user_id() {
        let registry = ParticipantRegistry::new();
        registry.add("r", "late", "viewer", 300).unwrap();
        registry.add("r", "b", "viewer", 100).unwrap();
        registry.add("r", "a", "host", 100).unwrap();

        let ids: Vec<_> = registry
            .list("r")
            .into_iter()
            .map(|participant| participant.user_id)
            .collect();
        assert_eq!(ids, ["a", "b", "late"]);
    }

    #[test]
    fn list_of_unknown_room_is_empty() {
        assert!(ParticipantRegistry::new().list("missing").is_empty());
    }

    #[test]
    fn add_upserts_existing_user() {
        let registry = ParticipantRegistry::new();
        registry.add("r", "u", "viewer", 100).unwrap();
        registry.add("r", "u", "host", 200).unwrap();

        let participants = registry.list("r");
        assert_eq!(participants.len(), 1);
        assert_eq!(participants[0].role, "host");
        assert_eq!(participants[0].joined_at_server_ms, 200);
    }

    #[test]
    fn add_rejects_new_user_above_limit_but_allows_reconnect() {
        let registry = ParticipantRegistry::new();
        for index in 0..MAX_PARTICIPANTS {
            registry
                .add("r", &format!("u{index}"), "viewer", index as i64)
                .unwrap();
        }

        assert_eq!(
            registry
                .add("r", "one-too-many", "viewer", 999)
                .unwrap_err(),
            JoinError::RoomFull
        );
        assert_eq!(registry.list("r").len(), MAX_PARTICIPANTS);

        // A reconnect at capacity does not grow the room.
        registry.add("r", "u0", "viewer", 1_000).unwrap();
        assert_eq!(registry.list("r").len(), MAX_PARTICIPANTS);
    }

    #[test]
    fn empty_since_is_recorded_only_when_room_becomes_empty() {
        let registry = ParticipantRegistry::new();
        registry.add("r", "a", "host", 100).unwrap();
        registry.add("r", "b", "viewer", 200).unwrap();

        assert!(!registry.remove("r", "a", 300));
        assert_eq!(registry.empty_since("r"), None);

        assert!(registry.remove("r", "b", 400));
        assert_eq!(registry.empty_since("r"), Some(400));
        assert!(registry.list("r").is_empty());

        // Unknown room or participant never marks it empty.
        assert!(!registry.remove("missing", "a", 500));
        assert!(!registry.remove("r", "a", 500));
        assert_eq!(registry.empty_since("r"), Some(400));
    }

    #[test]
    fn rejoin_clears_empty_since() {
        let registry = ParticipantRegistry::new();
        registry.add("r", "a", "host", 100).unwrap();
        assert!(registry.remove("r", "a", 200));
        assert_eq!(registry.empty_since("r"), Some(200));

        registry.add("r", "a", "host", 300).unwrap();
        assert_eq!(registry.empty_since("r"), None);
    }

    #[test]
    fn next_seq_is_monotonic_and_independent_per_room() {
        let registry = ParticipantRegistry::new();
        assert_eq!(registry.next_seq("r1"), 1);
        assert_eq!(registry.next_seq("r1"), 2);
        assert_eq!(registry.next_seq("r1"), 3);
        assert_eq!(registry.next_seq("r2"), 1);
        assert_eq!(registry.next_seq("r1"), 4);
    }

    #[test]
    fn remove_room_clears_all_bookkeeping() {
        let registry = ParticipantRegistry::new();
        registry.add("r", "a", "host", 100).unwrap();
        registry.next_seq("r");

        registry.remove_room("r");

        assert!(registry.list("r").is_empty());
        assert_eq!(registry.empty_since("r"), None);
        assert_eq!(registry.next_seq("r"), 1);
    }
}
