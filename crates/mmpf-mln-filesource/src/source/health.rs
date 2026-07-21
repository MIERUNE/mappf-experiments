use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::metrics::fs_metrics;

/// Process-local evidence that at least one resource request is actively
/// riding out a transient upstream failure. The guard follows the whole retry
/// sequence (including backoff), rather than a single HTTP attempt, so health
/// classification does not flap between attempts.
#[derive(Clone, Debug, Default)]
pub struct ProviderHealthTracker {
    active_retries: Arc<AtomicUsize>,
    slow_attempts: Arc<AtomicUsize>,
}

impl ProviderHealthTracker {
    pub fn new() -> Self {
        Self::default()
    }

    #[doc(hidden)]
    pub fn begin_retry(&self) -> ProviderRetryGuard {
        self.active_retries.fetch_add(1, Ordering::AcqRel);
        fs_metrics().retry_sequences_inflight.inc();
        ProviderRetryGuard {
            tracker: self.clone(),
            active: true,
        }
    }

    /// Provisional evidence for an upstream attempt that has remained in
    /// actual network I/O past the slow-attempt threshold. Fast healthy
    /// requests and requests merely waiting for an admission permit must never
    /// classify an unrelated renderer loss as provider-caused.
    pub(crate) fn begin_slow_attempt(&self) -> ProviderAttemptGuard {
        self.slow_attempts.fetch_add(1, Ordering::AcqRel);
        fs_metrics().slow_attempts_inflight.inc();
        ProviderAttemptGuard {
            tracker: self.clone(),
            active: true,
        }
    }

    fn active_retries(&self) -> usize {
        self.active_retries.load(Ordering::Acquire)
    }

    fn has_active_retry(&self) -> bool {
        self.active_retries() != 0
    }

    fn has_slow_attempt(&self) -> bool {
        self.slow_attempts.load(Ordering::Acquire) != 0
    }

    /// Any current evidence that an unavailable slot is externally caused: a
    /// retry sequence in progress, or a render-blocking attempt still in flight.
    pub fn has_external_evidence(&self) -> bool {
        self.has_active_retry() || self.has_slow_attempt()
    }
}

#[derive(Debug)]
pub struct ProviderRetryGuard {
    tracker: ProviderHealthTracker,
    active: bool,
}

impl Drop for ProviderRetryGuard {
    fn drop(&mut self) {
        if self.active {
            self.tracker.active_retries.fetch_sub(1, Ordering::AcqRel);
            fs_metrics().retry_sequences_inflight.dec();
            self.active = false;
        }
    }
}

#[derive(Debug)]
pub(crate) struct ProviderAttemptGuard {
    tracker: ProviderHealthTracker,
    active: bool,
}

impl Drop for ProviderAttemptGuard {
    fn drop(&mut self) {
        if self.active {
            self.tracker.slow_attempts.fetch_sub(1, Ordering::AcqRel);
            fs_metrics().slow_attempts_inflight.dec();
            self.active = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_evidence_is_scoped_to_the_active_sequence() {
        let health = ProviderHealthTracker::new();
        assert!(!health.has_active_retry());

        let first = health.begin_retry();
        let second = health.begin_retry();
        assert_eq!(health.active_retries(), 2);

        drop(first);
        assert!(health.has_active_retry());
        drop(second);
        assert!(!health.has_active_retry());
    }

    #[test]
    fn only_a_promoted_slow_attempt_is_provisional_external_evidence() {
        let health = ProviderHealthTracker::new();
        assert!(!health.has_external_evidence());

        let attempt = health.begin_slow_attempt();
        assert!(health.has_slow_attempt());
        assert!(
            health.has_external_evidence(),
            "an in-flight render-blocking attempt is external evidence before a retry starts"
        );
        assert!(
            !health.has_active_retry(),
            "an attempt is not a retry sequence"
        );

        drop(attempt);
        assert!(!health.has_external_evidence());
    }
}
