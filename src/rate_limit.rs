//! Shared Hetzner API rate-limit backoff.
//!
//! Hetzner API quotas are project-wide; one Service reaching 429 must pause
//! every Service managed by the operator rather than retrying independently.

use std::{
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

const FIRST_PAUSE_SECS: u64 = 60;
const MAX_PAUSE_SECS: u64 = 960;
const RESET_AFTER: Duration = Duration::from_secs(3600);

#[derive(Debug, Default)]
struct State {
    until: Option<Instant>,
    last_hit: Option<Instant>,
    consecutive: u32,
}

/// Gate shared across all Service reconciliation tasks in one leader process.
#[derive(Debug, Default)]
pub struct RateLimitGate {
    state: Mutex<State>,
}

impl RateLimitGate {
    /// The time remaining before calls may resume, if the gate is closed.
    pub fn remaining(&self, now: Instant) -> Option<Duration> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .until
            .and_then(|until| until.checked_duration_since(now))
            .filter(|duration| !duration.is_zero())
    }

    /// Record a 429 without extending an existing pause for in-flight requests.
    pub fn on_rate_limit(&self, now: Instant) -> Duration {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(remaining) = state
            .until
            .and_then(|until| until.checked_duration_since(now))
            && !remaining.is_zero()
        {
            return remaining;
        }
        if state
            .last_hit
            .is_none_or(|last| now.saturating_duration_since(last) > RESET_AFTER)
        {
            state.consecutive = 0;
        }
        let seconds = (FIRST_PAUSE_SECS << state.consecutive.min(4)).min(MAX_PAUSE_SECS);
        let delay = Duration::from_secs(seconds);
        state.until = Some(now + delay);
        state.last_hit = Some(now);
        state.consecutive = state.consecutive.saturating_add(1);
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_shared_and_does_not_extend_for_simultaneous_failures() {
        let gate = RateLimitGate::default();
        let now = Instant::now();
        assert_eq!(gate.on_rate_limit(now), Duration::from_secs(60));
        assert_eq!(gate.on_rate_limit(now), Duration::from_secs(60));
        assert_eq!(
            gate.remaining(now + Duration::from_secs(30)),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            gate.on_rate_limit(now + Duration::from_secs(60)),
            Duration::from_secs(120)
        );
    }

    #[test]
    fn resets_after_a_quiet_hour() {
        let gate = RateLimitGate::default();
        let now = Instant::now();
        gate.on_rate_limit(now);
        assert_eq!(
            gate.on_rate_limit(now + Duration::from_secs(3601)),
            Duration::from_secs(60)
        );
    }
}
