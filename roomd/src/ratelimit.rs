//! Per-connection anti-flood limiter (protocol §9).
//!
//! One [`RateLimiter`] is owned by each WebSocket connection. Every matched
//! category has a token bucket: tokens accrue continuously at the sustained
//! rate and the bucket caps at the burst size, so a client may spend its burst
//! instantly but cannot exceed the sustained rate over time. Only calls that
//! return [`RateDecision::Allow`] consume a token. Denials are recorded
//! separately so a connection that keeps violating the limits can be dropped
//! (more than 20 in 60 s → close 1008).

use std::collections::VecDeque;

/// Message groups subject to the §9 limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    /// `chat_send`.
    Chat,
    /// `reaction`.
    Reaction,
    /// `play` / `pause` / `seek`.
    Control,
    /// `ping`.
    Ping,
}

/// Outcome of a single [`RateLimiter::check`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateDecision {
    Allow,
    /// `retry_after_ms` is the wait until one token is available.
    Deny {
        retry_after_ms: i64,
    },
}

/// Sustained refill rate plus the burst capacity (protocol §9 table).
#[derive(Debug, Clone, Copy)]
struct Limits {
    /// Tokens replenished per `window_ms`.
    refill: u32,
    window_ms: i64,
    /// Maximum tokens the bucket can hold.
    capacity: u32,
}

impl Category {
    fn limits(self) -> Limits {
        match self {
            Self::Chat => Limits {
                refill: 3,
                window_ms: 5_000,
                capacity: 3,
            },
            Self::Reaction => Limits {
                refill: 5,
                window_ms: 5_000,
                capacity: 5,
            },
            Self::Control => Limits {
                refill: 5,
                window_ms: 1_000,
                capacity: 10,
            },
            Self::Ping => Limits {
                refill: 1,
                window_ms: 1_000,
                capacity: 3,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    last_refill_ms: i64,
}

impl Bucket {
    fn full(capacity: u32) -> Self {
        Self {
            tokens: capacity as f64,
            last_refill_ms: 0,
        }
    }
}

#[derive(Debug)]
pub struct RateLimiter {
    chat: Bucket,
    reaction: Bucket,
    control: Bucket,
    ping: Bucket,
    // Timestamps of denials across all categories, for the 60 s disconnect rule.
    denials: VecDeque<i64>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            chat: Bucket::full(Category::Chat.limits().capacity),
            reaction: Bucket::full(Category::Reaction.limits().capacity),
            control: Bucket::full(Category::Control.limits().capacity),
            ping: Bucket::full(Category::Ping.limits().capacity),
            denials: VecDeque::new(),
        }
    }

    /// Decides whether an event of `category` at `now_ms` is allowed.
    ///
    /// `now_ms` is injected so tests stay deterministic; callers pass wall
    /// clock time (see `crate::state::now_ms`). Timestamps are expected to be
    /// non-decreasing. A denial does not consume a token.
    pub fn check(&mut self, category: Category, now_ms: i64) -> RateDecision {
        let limits = category.limits();
        let refill_per_ms = f64::from(limits.refill) / limits.window_ms as f64;

        let decision = {
            let bucket = self.bucket_mut(category);
            let elapsed = now_ms.saturating_sub(bucket.last_refill_ms).max(0);
            bucket.tokens =
                (bucket.tokens + elapsed as f64 * refill_per_ms).min(f64::from(limits.capacity));
            bucket.last_refill_ms = now_ms;

            if bucket.tokens >= 1.0 {
                bucket.tokens -= 1.0;
                RateDecision::Allow
            } else {
                // Ceil keeps retry_after_ms >= 1; 0 would invite a tight retry loop.
                let deficit = 1.0 - bucket.tokens;
                RateDecision::Deny {
                    retry_after_ms: (deficit / refill_per_ms).ceil() as i64,
                }
            }
        };

        if matches!(decision, RateDecision::Deny { .. }) {
            self.denials.push_back(now_ms);
            prune(&mut self.denials, now_ms, DENIAL_WINDOW_MS);
        }
        decision
    }

    /// Number of denials recorded in the last 60 s (protocol §9).
    pub fn denial_count_last_minute(&self, now_ms: i64) -> u32 {
        let cutoff = now_ms.saturating_sub(DENIAL_WINDOW_MS);
        self.denials
            .iter()
            .filter(|&&ts| ts > cutoff && ts <= now_ms)
            .count() as u32
    }

    /// True when more than 20 denials happened in the last 60 s; the
    /// connection should then be closed with code 1008.
    pub fn should_disconnect(&self, now_ms: i64) -> bool {
        self.denial_count_last_minute(now_ms) > 20
    }

    fn bucket_mut(&mut self, category: Category) -> &mut Bucket {
        match category {
            Category::Chat => &mut self.chat,
            Category::Reaction => &mut self.reaction,
            Category::Control => &mut self.control,
            Category::Ping => &mut self.ping,
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

const DENIAL_WINDOW_MS: i64 = 60_000;

/// Removes events at or before the window cutoff; `events` is time-ordered.
fn prune(events: &mut VecDeque<i64>, now_ms: i64, window_ms: i64) {
    let cutoff = now_ms.saturating_sub(window_ms);
    while events.front().is_some_and(|&ts| ts <= cutoff) {
        events.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry_of(decision: RateDecision) -> Option<i64> {
        match decision {
            RateDecision::Deny { retry_after_ms } => Some(retry_after_ms),
            RateDecision::Allow => None,
        }
    }

    #[test]
    fn chat_allows_three_instantly_then_denies() {
        let mut limiter = RateLimiter::new();

        // Burst capacity is 3; all three fit at the same instant.
        for _ in 0..3 {
            assert_eq!(limiter.check(Category::Chat, 0), RateDecision::Allow);
        }
        let retry = retry_of(limiter.check(Category::Chat, 0)).expect("denied");
        assert!(retry > 0 && retry <= 5_000);
        // Sustained refill is 3 per 5 s, so one token is back after 2 s.
        assert_eq!(limiter.check(Category::Chat, 2_000), RateDecision::Allow);
    }

    #[test]
    fn reaction_allows_five_instantly_then_denies() {
        let mut limiter = RateLimiter::new();

        for _ in 0..5 {
            assert_eq!(limiter.check(Category::Reaction, 0), RateDecision::Allow);
        }
        assert!(retry_of(limiter.check(Category::Reaction, 0)).is_some());
        // Sustained refill is 5 per 5 s, so one token is back after 1 s.
        assert_eq!(
            limiter.check(Category::Reaction, 1_000),
            RateDecision::Allow
        );
    }

    #[test]
    fn control_allows_ten_instantly_then_denies() {
        let mut limiter = RateLimiter::new();

        // Burst capacity is 10, so a rapid sequence of controls fits.
        for _ in 0..10 {
            assert_eq!(limiter.check(Category::Control, 0), RateDecision::Allow);
        }
        assert!(retry_of(limiter.check(Category::Control, 0)).is_some());
        // Sustained refill is 5 per 1 s, so one token is back after 200 ms.
        assert_eq!(limiter.check(Category::Control, 400), RateDecision::Allow);
    }

    #[test]
    fn ping_allows_three_instantly_then_denies() {
        let mut limiter = RateLimiter::new();

        for _ in 0..3 {
            assert_eq!(limiter.check(Category::Ping, 0), RateDecision::Allow);
        }
        assert!(retry_of(limiter.check(Category::Ping, 0)).is_some());
        // Sustained refill is 1 per 1 s.
        assert_eq!(limiter.check(Category::Ping, 1_000), RateDecision::Allow);
    }

    #[test]
    fn categories_have_independent_buckets() {
        let mut limiter = RateLimiter::new();

        for _ in 0..3 {
            assert_eq!(limiter.check(Category::Chat, 0), RateDecision::Allow);
        }
        assert!(retry_of(limiter.check(Category::Chat, 0)).is_some());
        // A drained chat bucket must not block a fresh ping bucket.
        assert_eq!(limiter.check(Category::Ping, 0), RateDecision::Allow);
    }

    #[test]
    fn disconnect_requires_more_than_twenty_denials_in_a_minute() {
        let mut limiter = RateLimiter::new();

        // Drain the ping bucket, then keep denying at the same instant.
        for _ in 0..3 {
            assert_eq!(limiter.check(Category::Ping, 0), RateDecision::Allow);
        }
        for _ in 0..20 {
            assert!(retry_of(limiter.check(Category::Ping, 0)).is_some());
        }
        assert_eq!(limiter.denial_count_last_minute(0), 20);
        assert!(!limiter.should_disconnect(0));

        assert!(retry_of(limiter.check(Category::Ping, 0)).is_some());
        assert_eq!(limiter.denial_count_last_minute(0), 21);
        assert!(limiter.should_disconnect(0));

        // Denials expire after the 60 s window.
        assert_eq!(limiter.denial_count_last_minute(60_000), 0);
        assert!(!limiter.should_disconnect(60_000));
    }

    #[test]
    fn allows_are_not_counted_as_denials() {
        let mut limiter = RateLimiter::new();

        for t in [0, 1_000, 2_000] {
            assert_eq!(limiter.check(Category::Ping, t), RateDecision::Allow);
        }
        assert_eq!(limiter.denial_count_last_minute(2_000), 0);
        assert!(!limiter.should_disconnect(2_000));
    }
}
