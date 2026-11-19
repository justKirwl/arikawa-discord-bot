//! In-process room event bus for the `StreamRoomEvents` gRPC stream.
//!
//! Producers (WebSocket handlers, the idle reaper, the `CloseRoom` RPC) publish
//! lifecycle events here; one gRPC subscriber per room forwards them to the bot.
//! Events are ephemeral: a subscriber only sees events published after it
//! subscribed — queue and chat history are not replayed (protocol.md §7).
//!
//! A slow subscriber cannot block producers: every receiver owns a bounded
//! queue and observes [`broadcast::error::RecvError::Lagged`] when it falls
//! behind, so the bot can detect the gap and re-subscribe.

use tokio::sync::broadcast;
use tracing::debug;

/// Why a room was closed, carried by [`RoomEvent::RoomClosed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomCloseReason {
    /// The room stayed empty for `room_idle_timeout`.
    IdleTimeout,
    /// The bot called `CloseRoom`.
    ClosedByBot,
}

impl RoomCloseReason {
    /// Machine-readable reason: `"idle_timeout"` / `"closed_by_host"` per
    /// protocol.md §7.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IdleTimeout => "idle_timeout",
            Self::ClosedByBot => "closed_by_host",
        }
    }
}

/// A lifecycle event delivered to the bot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomEvent {
    ParticipantJoined {
        room_id: String,
        user_id: String,
        role: &'static str,
        joined_at_server_ms: i64,
    },
    ParticipantLeft {
        room_id: String,
        user_id: String,
    },
    MediaEnded {
        room_id: String,
        media_id: String,
    },
    RoomClosed {
        room_id: String,
        reason: RoomCloseReason,
    },
}

/// Fan-out hub for room events. Clone the bus (or keep it in an `Arc`) to share
/// it between producers; each [`subscribe`](Self::subscribe) call gets its own
/// independent view of the stream.
pub struct RoomEventBus {
    tx: broadcast::Sender<RoomEvent>,
}

impl RoomEventBus {
    /// Bounded queue per subscriber; lagging receivers get `Lagged` instead of
    /// stalling the publishers.
    pub const CAPACITY: usize = 1024;

    pub fn new() -> Self {
        // CAPACITY is non-zero, so `channel` cannot fail here.
        let (tx, _) = broadcast::channel(Self::CAPACITY);
        Self { tx }
    }

    /// Non-blocking; log at debug when there are no subscribers.
    pub fn publish(&self, event: RoomEvent) {
        if self.tx.send(event).is_err() {
            debug!("publishing room event with no subscribers, dropping");
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RoomEvent> {
        self.tx.subscribe()
    }
}

impl Default for RoomEventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::RecvError;

    fn joined(
        room_id: &str,
        user_id: &str,
        role: &'static str,
        joined_at_server_ms: i64,
    ) -> RoomEvent {
        RoomEvent::ParticipantJoined {
            room_id: room_id.to_owned(),
            user_id: user_id.to_owned(),
            role,
            joined_at_server_ms,
        }
    }

    #[test]
    fn close_reason_strings_follow_the_protocol() {
        assert_eq!(RoomCloseReason::IdleTimeout.as_str(), "idle_timeout");
        assert_eq!(RoomCloseReason::ClosedByBot.as_str(), "closed_by_host");
    }

    #[tokio::test]
    async fn published_event_reaches_a_subscriber() {
        let bus = RoomEventBus::new();
        let mut rx = bus.subscribe();

        bus.publish(joined("room-1", "user-1", "host", 1_000));

        assert_eq!(
            rx.recv().await.unwrap(),
            joined("room-1", "user-1", "host", 1_000)
        );
    }

    #[tokio::test]
    async fn every_subscriber_gets_a_copy() {
        let bus = RoomEventBus::new();
        let mut first = bus.subscribe();
        let mut second = bus.subscribe();

        let event = RoomEvent::RoomClosed {
            room_id: "room-1".to_owned(),
            reason: RoomCloseReason::ClosedByBot,
        };
        bus.publish(event.clone());

        assert_eq!(first.recv().await.unwrap(), event);
        assert_eq!(second.recv().await.unwrap(), event);
    }

    #[tokio::test]
    async fn slow_subscriber_observes_lagged_instead_of_panicking() {
        let bus = RoomEventBus::new();
        let mut rx = bus.subscribe();

        for _ in 0..=RoomEventBus::CAPACITY {
            bus.publish(RoomEvent::MediaEnded {
                room_id: "room-1".to_owned(),
                media_id: "m-1".to_owned(),
            });
        }

        assert!(matches!(rx.recv().await, Err(RecvError::Lagged(_))));
    }
}
