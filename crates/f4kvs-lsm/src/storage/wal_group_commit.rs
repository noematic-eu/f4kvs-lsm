//! Shared group-commit timing helpers.

use std::time::Duration;
use tokio::time::Instant;

/// Timestamps used to schedule max-wait and idle flushes.
#[derive(Debug, Default)]
pub struct GroupCommitTiming {
    pub first_pending_at: Option<Instant>,
    pub last_enqueue_at: Option<Instant>,
}

impl GroupCommitTiming {
    pub fn record_enqueue(&mut self, pending_was_empty: bool) {
        let now = Instant::now();
        if pending_was_empty {
            self.first_pending_at = Some(now);
        }
        self.last_enqueue_at = Some(now);
    }

    pub fn clear(&mut self) {
        self.first_pending_at = None;
        self.last_enqueue_at = None;
    }
}

/// Next flush instant for a non-empty queue, or `None` when idle.
pub fn next_flush_deadline(
    timing: &GroupCommitTiming,
    pending_count: usize,
    max_wait: Duration,
    idle_flush: Option<Duration>,
) -> Option<Instant> {
    if pending_count == 0 {
        return None;
    }
    let first = timing.first_pending_at?;
    let mut deadline = first + max_wait;
    if let (Some(idle), Some(last)) = (idle_flush, timing.last_enqueue_at) {
        deadline = deadline.min(last + idle);
    }
    Some(deadline.max(Instant::now()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_flush_deadline_sooner_than_max_wait() {
        let mut timing = GroupCommitTiming::default();
        timing.record_enqueue(true);
        let first = timing.first_pending_at.unwrap();
        timing.last_enqueue_at = Some(first);
        let dl = next_flush_deadline(
            &timing,
            1,
            Duration::from_secs(60),
            Some(Duration::from_millis(100)),
        )
        .unwrap();
        assert!(dl <= first + Duration::from_millis(100));
    }
}