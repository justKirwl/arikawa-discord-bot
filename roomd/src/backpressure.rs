//! Per-connection outbound queue with the protocol §9 drop policy.
//!
//! The WebSocket writer task owns the socket sink and drains this queue via
//! [`OutboundQueue::recv`]. Producers call [`OutboundQueue::push`], which never
//! blocks: at the soft capacity the oldest droppable frame (chat/reaction) is
//! evicted to make room, essential frames (state/participants/pong/error) are
//! never evicted, and an essential-only backlog beyond the hard capacity means
//! the consumer cannot keep up ([`PushOutcome::Overwhelmed`]).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;

/// Whether a frame may be evicted under backpressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// State, participants, pong, error: never dropped.
    Essential,
    /// Chat and reaction: first to be dropped when the queue is full.
    Droppable,
}

/// Result of an attempt to enqueue a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    /// The frame was appended (or the queue is closing; see [`OutboundQueue::push`]).
    Queued,
    /// An older droppable frame was evicted to make room.
    DroppedDroppable,
    /// The queue is full of essential frames; the caller must send
    /// `error{code:"slow_consumer"}` and close with 1013.
    Overwhelmed,
}

/// A frame waiting to be written to the socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outbound {
    Message(String),
    Close { code: u16, reason: String },
}

#[derive(Debug)]
struct Entry {
    outbound: Outbound,
    priority: Priority,
}

#[derive(Debug, Default)]
struct Inner {
    queue: VecDeque<Entry>,
    closed: bool,
}

/// An `Arc`-shared queue so the writer and producers can hold handles.
#[derive(Clone, Debug)]
pub struct OutboundQueue {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
}

impl OutboundQueue {
    /// Soft capacity: beyond this, droppable frames are evicted first.
    pub const SOFT_CAPACITY: usize = 256;
    /// Hard capacity: an essential-only backlog this large means the peer is
    /// too slow and the connection must be closed.
    pub const HARD_CAPACITY: usize = 1024;

    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Recover from a poisoned lock rather than panicking: a partially written
    /// queue is still safe to drain, and a panic here would kill the writer.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Never blocks. Essential frames are never dropped.
    ///
    /// After [`Self::request_close`] this is a no-op that returns
    /// [`PushOutcome::Queued`]: the connection is already winding down and the
    /// caller should not treat a late frame as an overload condition.
    pub fn push(&self, priority: Priority, message: String) -> PushOutcome {
        let mut inner = self.lock();

        if inner.closed {
            return PushOutcome::Queued;
        }

        let mut outcome = PushOutcome::Queued;

        // At the soft capacity, shed the oldest droppable before accepting
        // more traffic, regardless of the incoming frame's priority.
        if inner.queue.len() >= Self::SOFT_CAPACITY {
            if let Some(index) = inner
                .queue
                .iter()
                .position(|entry| entry.priority == Priority::Droppable)
            {
                inner.queue.remove(index);
                outcome = PushOutcome::DroppedDroppable;
            }
        }

        // Essential frames are never dropped, so a full queue of them means
        // the consumer cannot keep up.
        if inner.queue.len() >= Self::HARD_CAPACITY {
            return PushOutcome::Overwhelmed;
        }

        inner.queue.push_back(Entry {
            outbound: Outbound::Message(message),
            priority,
        });
        drop(inner);
        self.notify.notify_one();

        outcome
    }

    /// Writer loop drains this. After the queue is closed and drained, yields
    /// [`Outbound::Close`] once, then `None` forever.
    pub async fn recv(&self) -> Option<Outbound> {
        loop {
            // Register before checking the queue so a concurrent push cannot
            // be missed between the check and the wait.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut inner = self.lock();
                if let Some(entry) = inner.queue.pop_front() {
                    return Some(entry.outbound);
                }

                // A closed, empty queue has nothing left to yield. The Close
                // frame itself is queued by request_close and drained above.
                if inner.closed {
                    return None;
                }
            }

            notified.await;
        }
    }

    /// Schedules a terminal Close frame after draining; also ends the loop.
    pub fn request_close(&self, code: u16, reason: &str) {
        let mut inner = self.lock();
        if inner.closed {
            return;
        }
        inner.closed = true;
        inner.queue.push_back(Entry {
            outbound: Outbound::Close {
                code,
                reason: reason.to_owned(),
            },
            priority: Priority::Essential,
        });
        drop(inner);
        self.notify.notify_one();
    }

    pub fn len(&self) -> usize {
        self.lock().queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().queue.is_empty()
    }
}

impl Default for OutboundQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn messages(queue: &OutboundQueue) -> Vec<String> {
        queue
            .lock()
            .queue
            .iter()
            .map(|entry| match &entry.outbound {
                Outbound::Message(text) => text.clone(),
                Outbound::Close { code, reason } => format!("close:{code}:{reason}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn fifo_order() {
        let queue = OutboundQueue::new();
        for text in ["a", "b", "c"] {
            assert_eq!(
                queue.push(Priority::Essential, text.to_owned()),
                PushOutcome::Queued
            );
        }

        assert_eq!(queue.recv().await, Some(Outbound::Message("a".into())));
        assert_eq!(queue.recv().await, Some(Outbound::Message("b".into())));
        assert_eq!(queue.recv().await, Some(Outbound::Message("c".into())));
        assert_eq!(queue.len(), 0);
    }

    #[tokio::test]
    async fn essential_is_never_dropped_at_soft_capacity() {
        let queue = OutboundQueue::new();
        for index in 0..OutboundQueue::SOFT_CAPACITY {
            assert_eq!(
                queue.push(Priority::Droppable, format!("d{index}")),
                PushOutcome::Queued
            );
        }
        assert_eq!(queue.len(), OutboundQueue::SOFT_CAPACITY);

        assert_eq!(
            queue.push(Priority::Essential, "keep-me".to_owned()),
            PushOutcome::DroppedDroppable
        );
        assert_eq!(queue.len(), OutboundQueue::SOFT_CAPACITY);

        let contents = messages(&queue);
        assert!(!contents.contains(&"d0".to_owned()));
        assert_eq!(contents[0], "d1");
        assert_eq!(contents.last().map(String::as_str), Some("keep-me"));
    }

    #[test]
    fn overwhelmed_with_essential_only_backlog() {
        let queue = OutboundQueue::new();
        for index in 0..OutboundQueue::HARD_CAPACITY {
            assert_eq!(
                queue.push(Priority::Essential, format!("e{index}")),
                PushOutcome::Queued
            );
        }
        assert_eq!(queue.len(), OutboundQueue::HARD_CAPACITY);

        assert_eq!(
            queue.push(Priority::Essential, "too-much".to_owned()),
            PushOutcome::Overwhelmed
        );
        assert_eq!(
            queue.push(Priority::Droppable, "also-too-much".to_owned()),
            PushOutcome::Overwhelmed
        );
        assert_eq!(queue.len(), OutboundQueue::HARD_CAPACITY);
    }

    #[test]
    fn droppable_is_evicted_before_hard_capacity_overflow() {
        let queue = OutboundQueue::new();
        for index in 0..OutboundQueue::HARD_CAPACITY - 1 {
            assert_eq!(
                queue.push(Priority::Essential, format!("e{index}")),
                PushOutcome::Queued
            );
        }
        assert_eq!(
            queue.push(Priority::Droppable, "old".to_owned()),
            PushOutcome::Queued
        );
        assert_eq!(queue.len(), OutboundQueue::HARD_CAPACITY);

        assert_eq!(
            queue.push(Priority::Essential, "replacement".to_owned()),
            PushOutcome::DroppedDroppable
        );
        assert_eq!(queue.len(), OutboundQueue::HARD_CAPACITY);
        assert!(!messages(&queue).contains(&"old".to_owned()));
    }

    #[tokio::test]
    async fn request_close_drains_then_yields_close_then_none() {
        let queue = OutboundQueue::new();
        for text in ["first", "second"] {
            queue.push(Priority::Droppable, text.to_owned());
        }
        queue.request_close(1013, "slow_consumer");

        // A late push after close is a no-op that reports Queued.
        assert_eq!(
            queue.push(Priority::Essential, "late".to_owned()),
            PushOutcome::Queued
        );
        queue.request_close(1000, "ignored");

        assert_eq!(queue.recv().await, Some(Outbound::Message("first".into())));
        assert_eq!(queue.recv().await, Some(Outbound::Message("second".into())));
        assert_eq!(
            queue.recv().await,
            Some(Outbound::Close {
                code: 1013,
                reason: "slow_consumer".to_owned(),
            })
        );
        assert_eq!(queue.recv().await, None);
        assert_eq!(queue.recv().await, None);
    }

    #[tokio::test]
    async fn recv_wakes_on_concurrent_push() {
        let queue = OutboundQueue::new();
        let producer = queue.clone();
        let handle = tokio::spawn(async move {
            producer.push(Priority::Essential, "wake".to_owned());
        });

        assert_eq!(queue.recv().await, Some(Outbound::Message("wake".into())));
        handle.await.expect("producer task");
    }
}
