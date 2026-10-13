//! Bound repeated Kubernetes Warning Events during persistent reconciliation failures.
//!
//! Reconciliation and error logging continue on every attempt. Only identical
//! Event publications are throttled, so a changed error remains visible.

use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

const REPEAT_INTERVAL: Duration = Duration::from_secs(300);

/// Identifies the exact error shown on a particular Service.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EventKey {
    pub service_uid: String,
    pub reason: String,
    pub note: String,
}

/// Records recent Event publication attempts for one controller instance.
#[derive(Debug, Default)]
pub(crate) struct EventThrottle {
    recent: Mutex<HashMap<EventKey, Instant>>,
}

impl EventThrottle {
    /// Reserve an Event. Concurrent reconciliations cannot reserve the same key.
    /// Expired entries are removed to avoid retaining deleted Services indefinitely.
    pub fn reserve(&self, key: &EventKey, now: Instant) -> bool {
        let mut recent = self.recent.lock().unwrap_or_else(PoisonError::into_inner);
        recent.retain(|_, last| now.saturating_duration_since(*last) < REPEAT_INTERVAL);
        if recent.contains_key(key) {
            return false;
        }
        recent.insert(key.clone(), now);
        true
    }

    /// Permit another attempt if publishing failed. Do not clear a newer reservation.
    pub fn release(&self, key: &EventKey, reserved_at: Instant) {
        let mut recent = self.recent.lock().unwrap_or_else(PoisonError::into_inner);
        if recent.get(key) == Some(&reserved_at) {
            recent.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(uid: &str, reason: &str, note: &str) -> EventKey {
        EventKey {
            service_uid: uid.to_string(),
            reason: reason.to_string(),
            note: note.to_string(),
        }
    }

    #[test]
    fn identical_event_is_suppressed_for_five_minutes_then_resent() {
        let throttle = EventThrottle::default();
        let first = Instant::now();
        let event = key("service-uid", "SyncLoadBalancerFailed", "Hetzner 503");
        assert!(throttle.reserve(&event, first));
        assert!(!throttle.reserve(&event, first + Duration::from_secs(299)));
        assert!(throttle.reserve(&event, first + REPEAT_INTERVAL));
        assert!(!throttle.reserve(&event, first + REPEAT_INTERVAL));
    }

    #[test]
    fn different_service_reason_or_message_is_not_suppressed() {
        let throttle = EventThrottle::default();
        let now = Instant::now();
        assert!(throttle.reserve(&key("uid-a", "SyncLoadBalancerFailed", "503"), now));
        assert!(throttle.reserve(&key("uid-b", "SyncLoadBalancerFailed", "503"), now));
        assert!(throttle.reserve(&key("uid-a", "InvalidLoadBalancerConfiguration", "503"), now));
        assert!(throttle.reserve(&key("uid-a", "SyncLoadBalancerFailed", "502"), now));
    }

    #[test]
    fn failed_event_publication_does_not_suppress_retry() {
        let throttle = EventThrottle::default();
        let now = Instant::now();
        let event = key("uid", "SyncLoadBalancerFailed", "503");
        assert!(throttle.reserve(&event, now));
        throttle.release(&event, now);
        assert!(throttle.reserve(&event, now + Duration::from_secs(30)));
    }

    #[test]
    fn stale_failure_cannot_cancel_a_new_reservation() {
        let throttle = EventThrottle::default();
        let now = Instant::now();
        let event = key("uid", "SyncLoadBalancerFailed", "503");
        assert!(throttle.reserve(&event, now));
        assert!(throttle.reserve(&event, now + REPEAT_INTERVAL));
        throttle.release(&event, now);
        assert!(!throttle.reserve(&event, now + REPEAT_INTERVAL));
    }

    #[test]
    fn only_one_concurrent_reconcile_may_publish_an_event() {
        let throttle = std::sync::Arc::new(EventThrottle::default());
        let now = Instant::now();
        let event = key("uid", "SyncLoadBalancerFailed", "503");
        let reservations: usize = std::thread::scope(|scope| {
            let tasks: Vec<_> = (0..8)
                .map(|_| {
                    let throttle = throttle.clone();
                    let event = event.clone();
                    scope.spawn(move || usize::from(throttle.reserve(&event, now)))
                })
                .collect();
            tasks.into_iter().map(|task| task.join().unwrap()).sum()
        });
        assert_eq!(reservations, 1);
    }
}
