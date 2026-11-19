//! Glue between the participant registry, per-connection outbound queues and
//! the room event bus.
//!
//! WebSocket handlers (S3.6/S3.10) attach a connection queue on `join` and
//! broadcast messages into it; gRPC (S3.8) and the idle sweeper (S3.7) close a
//! room and consume the resulting lifecycle events. All fan-out goes through
//! [`OutboundQueue`], whose FIFO order preserves the `state`-then-`participants`
//! ordering required by protocol §3.1.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use tracing::{debug, info};

use crate::backpressure::{OutboundQueue, Priority};
use crate::events::{RoomCloseReason, RoomEvent, RoomEventBus};
use crate::participants::{JoinError, Participant, ParticipantRegistry};
use crate::state::RoomSnapshot;

pub type ConnId = u64;

/// One attached WebSocket connection and the queue its writer task drains.
#[derive(Debug)]
pub struct Connection {
    pub id: ConnId,
    pub queue: OutboundQueue,
    pub user_id: String,
}

/// Shared hub of live rooms. Cheap to clone behind an `Arc` (all state is
/// interior-mutable); the queues themselves are `Arc`-shared with their writers.
pub struct RoomHub {
    participants: ParticipantRegistry,
    events: RoomEventBus,
    // The owner's user_id is kept per connection so `detach(conn_id)` can find
    // which registry entry to remove without an extra id→user lookup table.
    conns: DashMap<String, Vec<Connection>>,
    next_conn: AtomicU64,
}

// ws.rs owns the real protocol serializers (§4.1–§4.4); reuse them for every
// broadcast so the JSON envelope has a single source of truth in all builds.
fn state_json(snapshot: &RoomSnapshot) -> String {
    crate::ws::state_json(snapshot)
}

fn participants_json(participants: &[Participant], seq: i64, server_ts: i64) -> String {
    crate::ws::participants_json(participants, seq, server_ts)
}

fn chat_json(message: &crate::chat::ChatMessage, server_ts: i64, seq: i64) -> String {
    crate::ws::chat_json(message, server_ts, seq)
}

fn reaction_json(reaction: &crate::chat::Reaction, server_ts: i64, seq: i64) -> String {
    crate::ws::reaction_json(reaction, server_ts, seq)
}

impl RoomHub {
    pub fn new() -> Self {
        Self {
            participants: ParticipantRegistry::new(),
            events: RoomEventBus::new(),
            conns: DashMap::new(),
            next_conn: AtomicU64::new(1),
        }
    }

    /// Registers the participant and its connection queue, then publishes
    /// `ParticipantJoined`. A full room propagates `JoinError::RoomFull` and
    /// leaves no connection behind.
    pub fn attach(
        &self,
        room_id: &str,
        user_id: &str,
        role: &'static str,
        now_ms: i64,
    ) -> Result<Connection, JoinError> {
        self.participants.add(room_id, user_id, role, now_ms)?;

        let id = self.next_conn.fetch_add(1, Ordering::Relaxed);
        let queue = OutboundQueue::new();
        self.conns
            .entry(room_id.to_owned())
            .or_default()
            .push(Connection {
                id,
                queue: queue.clone(),
                user_id: user_id.to_owned(),
            });
        self.events.publish(RoomEvent::ParticipantJoined {
            room_id: room_id.to_owned(),
            user_id: user_id.to_owned(),
            role,
            joined_at_server_ms: now_ms,
        });

        Ok(Connection {
            id,
            queue,
            user_id: user_id.to_owned(),
        })
    }

    /// Removes the connection, its registry entry and publishes
    /// `ParticipantLeft`. Returns the owner's `user_id`, or `None` when the
    /// connection id is unknown in that room.
    pub fn detach(&self, room_id: &str, conn_id: ConnId, now_ms: i64) -> Option<String> {
        let user_id = {
            let mut room = self.conns.get_mut(room_id)?;
            let index = room.iter().position(|conn| conn.id == conn_id)?;
            room.remove(index).user_id
        };
        // Drop the shard guard above: remove_if takes the same write lock.
        self.conns.remove_if(room_id, |_, room| room.is_empty());

        self.participants.remove(room_id, &user_id, now_ms);
        self.events.publish(RoomEvent::ParticipantLeft {
            room_id: room_id.to_owned(),
            user_id: user_id.clone(),
        });

        Some(user_id)
    }

    /// Broadcasts room state (protocol §4.1) at essential priority.
    pub fn broadcast_state(&self, room_id: &str, snapshot: &RoomSnapshot) {
        let queues = self.connection_queues(room_id, "state");
        if queues.is_empty() {
            return;
        }
        let json = state_json(snapshot);
        for queue in queues {
            let _ = queue.push(Priority::Essential, json.clone());
        }
        debug!(room_id, "state broadcast");
    }

    /// Broadcasts the participant list with a fresh room `seq` (protocol §4.2).
    pub fn broadcast_participants(&self, room_id: &str, now_ms: i64) {
        let queues = self.connection_queues(room_id, "participants");
        if queues.is_empty() {
            return;
        }
        let participants = self.participants.list(room_id);
        let seq = self.participants.next_seq(room_id);
        let json = participants_json(&participants, seq, now_ms);
        for queue in queues {
            let _ = queue.push(Priority::Essential, json.clone());
        }
        debug!(room_id, "participants broadcast");
    }

    /// Broadcasts a chat message (protocol §4.4); droppable under backpressure.
    pub fn broadcast_chat(&self, room_id: &str, message: &crate::chat::ChatMessage, now_ms: i64) {
        let queues = self.connection_queues(room_id, "chat");
        if queues.is_empty() {
            return;
        }
        let json = chat_json(message, now_ms, self.participants.next_seq(room_id));
        for queue in queues {
            let _ = queue.push(Priority::Droppable, json.clone());
        }
        debug!(room_id, "chat broadcast");
    }

    /// Broadcasts a reaction (protocol §4.4); droppable under backpressure.
    pub fn broadcast_reaction(&self, room_id: &str, reaction: &crate::chat::Reaction, now_ms: i64) {
        let queues = self.connection_queues(room_id, "reaction");
        if queues.is_empty() {
            return;
        }
        let json = reaction_json(reaction, now_ms, self.participants.next_seq(room_id));
        for queue in queues {
            let _ = queue.push(Priority::Droppable, json.clone());
        }
        debug!(room_id, "reaction broadcast");
    }

    /// Closes every connection with code 1000, clears participants and
    /// publishes `RoomClosed`. The drained queues are returned so the caller
    /// can keep them alive until their writer tasks finish.
    pub fn close_room(&self, room_id: &str, reason: RoomCloseReason) -> Vec<OutboundQueue> {
        let queues: Vec<OutboundQueue> = self
            .conns
            .remove(room_id)
            .map(|(_, connections)| {
                connections
                    .into_iter()
                    .map(|connection| connection.queue)
                    .collect()
            })
            .unwrap_or_default();

        for queue in &queues {
            queue.request_close(1000, reason.as_str());
        }
        self.participants.remove_room(room_id);
        self.events.publish(RoomEvent::RoomClosed {
            room_id: room_id.to_owned(),
            reason: reason.clone(),
        });

        info!(
            room_id,
            reason = reason.as_str(),
            connections = queues.len(),
            "room closed"
        );
        queues
    }

    pub fn participants_empty_since(&self, room_id: &str) -> Option<i64> {
        self.participants.empty_since(room_id)
    }

    /// Snapshot of a room's participants for the gRPC `GetRoomState` response.
    pub fn participants_list(&self, room_id: &str) -> Vec<Participant> {
        self.participants.list(room_id)
    }

    pub fn events(&self) -> &RoomEventBus {
        &self.events
    }

    /// Snapshot the queues before pushing so no DashMap shard lock is held
    /// while a producer enqueues; an empty room is a cheap, logged no-op.
    fn connection_queues(&self, room_id: &str, kind: &str) -> Vec<OutboundQueue> {
        match self.conns.get(room_id) {
            Some(room) if !room.is_empty() => room.iter().map(|conn| conn.queue.clone()).collect(),
            _ => {
                debug!(room_id, kind, "broadcast skipped: no connections");
                Vec::new()
            }
        }
    }
}

impl Default for RoomHub {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure::Outbound;
    use crate::chat::{ChatMessage, Reaction};
    use crate::participants::MAX_PARTICIPANTS;

    fn snapshot(room_id: &str, version: u64) -> RoomSnapshot {
        RoomSnapshot {
            room_id: room_id.to_owned(),
            media_ref: None,
            position_ms: 1_000,
            playing: true,
            updated_at_server_ms: 500,
            version,
        }
    }

    async fn drain(queue: &OutboundQueue) -> Vec<Outbound> {
        let mut items = Vec::new();
        for _ in 0..queue.len() {
            if let Some(item) = queue.recv().await {
                items.push(item);
            }
        }
        items
    }

    fn message_text(outbound: &Outbound) -> &str {
        match outbound {
            Outbound::Message(text) => text,
            Outbound::Close { .. } => panic!("expected a message, got a close frame"),
        }
    }

    #[test]
    fn attach_and_detach_bookkeeping() {
        let hub = RoomHub::new();
        let mut rx = hub.events().subscribe();

        let first = hub.attach("r", "u1", "host", 100).expect("first attach");
        let second = hub.attach("r", "u2", "viewer", 200).expect("second attach");
        assert_eq!(first.user_id, "u1");
        assert!(first.id < second.id);
        assert_eq!(hub.participants.list("r").len(), 2);
        assert_eq!(hub.conns.get("r").expect("room").len(), 2);
        assert_eq!(hub.participants_empty_since("r"), None);
        assert_eq!(
            rx.try_recv().expect("joined u1"),
            RoomEvent::ParticipantJoined {
                room_id: "r".into(),
                user_id: "u1".into(),
                role: "host",
                joined_at_server_ms: 100,
            }
        );
        assert_eq!(
            rx.try_recv().expect("joined u2"),
            RoomEvent::ParticipantJoined {
                room_id: "r".into(),
                user_id: "u2".into(),
                role: "viewer",
                joined_at_server_ms: 200,
            }
        );

        assert_eq!(hub.detach("r", first.id, 300), Some("u1".to_owned()));
        assert_eq!(hub.participants.list("r").len(), 1);
        assert_eq!(hub.conns.get("r").expect("room").len(), 1);
        assert_eq!(
            rx.try_recv().expect("left u1"),
            RoomEvent::ParticipantLeft {
                room_id: "r".into(),
                user_id: "u1".into(),
            }
        );

        // Unknown connection and unknown room are no-ops.
        assert_eq!(hub.detach("r", 9_999, 400), None);
        assert_eq!(hub.detach("missing", first.id, 400), None);

        assert_eq!(hub.detach("r", second.id, 500), Some("u2".to_owned()));
        assert!(hub.participants.list("r").is_empty());
        assert_eq!(hub.participants_empty_since("r"), Some(500));
        assert!(hub.conns.get("r").is_none());
    }

    #[tokio::test]
    async fn broadcast_reaches_both_queues_in_protocol_order() {
        let hub = RoomHub::new();
        let first = hub.attach("r", "u1", "host", 100).expect("first attach");
        let second = hub.attach("r", "u2", "viewer", 100).expect("second attach");

        // Protocol §3.1: state is enqueued before participants on every socket.
        hub.broadcast_state("r", &snapshot("r", 7));
        hub.broadcast_participants("r", 200);

        let first_items = drain(&first.queue).await;
        let second_items = drain(&second.queue).await;
        for items in [&first_items, &second_items] {
            assert_eq!(items.len(), 2);
            assert!(message_text(&items[0]).contains("\"state\""));
            assert!(message_text(&items[1]).contains("\"participants\""));
        }

        let state: serde_json::Value =
            serde_json::from_str(message_text(&first_items[0])).expect("state json");
        assert_eq!(state["version"], 7);
        assert_eq!(state["server_ts"], 500);

        // The participants broadcast consumed exactly one room sequence number.
        let participants: serde_json::Value =
            serde_json::from_str(message_text(&second_items[1])).expect("participants json");
        assert_eq!(participants["seq"], 1);
        assert_eq!(participants["server_ts"], 200);
        assert_eq!(
            participants["participants"].as_array().expect("list").len(),
            2
        );
    }

    #[tokio::test]
    async fn chat_and_reaction_broadcasts_are_droppable_with_distinct_seqs() {
        let hub = RoomHub::new();
        let connection = hub.attach("r", "u1", "host", 100).expect("attach");

        hub.broadcast_chat(
            "r",
            &ChatMessage {
                user_id: "u1".into(),
                text: "hello".into(),
                client_msg_id: Some("c-1".into()),
            },
            300,
        );
        hub.broadcast_reaction(
            "r",
            &Reaction {
                user_id: "u1".into(),
                emoji: "🍿".into(),
            },
            400,
        );

        let items = drain(&connection.queue).await;
        assert_eq!(items.len(), 2);
        let chat: serde_json::Value =
            serde_json::from_str(message_text(&items[0])).expect("chat json");
        assert_eq!(chat["type"], "chat");
        assert_eq!(chat["seq"], 1);
        assert_eq!(chat["client_msg_id"], "c-1");
        let reaction: serde_json::Value =
            serde_json::from_str(message_text(&items[1])).expect("reaction json");
        assert_eq!(reaction["type"], "reaction");
        assert_eq!(reaction["seq"], 2);
        assert_eq!(reaction["server_ts"], 400);
    }

    #[tokio::test]
    async fn broadcast_to_a_room_without_connections_is_a_noop() {
        let hub = RoomHub::new();
        hub.broadcast_state("missing", &snapshot("missing", 1));
        hub.broadcast_participants("missing", 10);
        hub.broadcast_chat(
            "missing",
            &ChatMessage {
                user_id: "u".into(),
                text: "hi".into(),
                client_msg_id: None,
            },
            10,
        );
        hub.broadcast_reaction(
            "missing",
            &Reaction {
                user_id: "u".into(),
                emoji: "🍿".into(),
            },
            10,
        );

        assert!(hub.conns.get("missing").is_none());
    }

    #[tokio::test]
    async fn close_room_closes_all_queues_and_publishes_room_closed() {
        let hub = RoomHub::new();
        let first = hub.attach("r", "u1", "host", 100).expect("first attach");
        let second = hub.attach("r", "u2", "viewer", 100).expect("second attach");
        hub.broadcast_state("r", &snapshot("r", 2));

        let mut rx = hub.events().subscribe();
        let queues = hub.close_room("r", RoomCloseReason::ClosedByBot);
        assert_eq!(queues.len(), 2);

        // Previously queued frames are drained, then the close frame arrives.
        for queue in &queues {
            let items = drain(queue).await;
            assert_eq!(items.len(), 2);
            assert!(message_text(&items[0]).contains("\"state\""));
            assert_eq!(
                items[1],
                Outbound::Close {
                    code: 1000,
                    reason: "closed_by_host".to_owned(),
                }
            );
        }

        assert_eq!(
            rx.try_recv().expect("closed"),
            RoomEvent::RoomClosed {
                room_id: "r".into(),
                reason: RoomCloseReason::ClosedByBot,
            }
        );
        assert!(hub.conns.get("r").is_none());
        assert!(hub.participants.list("r").is_empty());
        assert_eq!(hub.participants_empty_since("r"), None);
        assert_eq!(hub.detach("r", first.id, 200), None);
        assert_eq!(hub.detach("r", second.id, 200), None);
    }

    #[tokio::test]
    async fn idle_timeout_reason_uses_the_protocol_string() {
        let hub = RoomHub::new();
        let connection = hub.attach("r", "u", "host", 100).expect("attach");

        let mut rx = hub.events().subscribe();
        let queues = hub.close_room("r", RoomCloseReason::IdleTimeout);
        assert_eq!(queues.len(), 1);
        assert_eq!(
            drain(&queues[0]).await,
            vec![Outbound::Close {
                code: 1000,
                reason: "idle_timeout".to_owned(),
            }]
        );
        assert_eq!(
            rx.try_recv().expect("closed"),
            RoomEvent::RoomClosed {
                room_id: "r".into(),
                reason: RoomCloseReason::IdleTimeout,
            }
        );
        let _ = connection;
    }

    #[test]
    fn room_full_propagates_from_the_51st_unique_attach() {
        let hub = RoomHub::new();
        for index in 0..MAX_PARTICIPANTS {
            hub.attach("r", &format!("u{index}"), "viewer", index as i64)
                .expect("within capacity");
        }

        assert_eq!(
            hub.attach("r", "one-too-many", "viewer", 999).unwrap_err(),
            JoinError::RoomFull
        );
        assert_eq!(hub.conns.get("r").expect("room").len(), MAX_PARTICIPANTS);

        // A reconnect at capacity still succeeds via the registry's upsert.
        hub.attach("r", "u0", "host", 1_000).expect("reconnect");
        assert_eq!(
            hub.conns.get("r").expect("room").len(),
            MAX_PARTICIPANTS + 1
        );
    }
}
