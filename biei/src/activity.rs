//! `ProfileActivityTracker` — records the last time each worker profile was observed.
//! Used as a tiebreak signal for eviction (oldest-seen-first) when allocation
//! counts are equal. Old profiles are evicted because this is only a routing
//! heuristic and attacker-controlled lazy style ids must not grow state forever.

use std::time::Duration;

use moka::sync::Cache;
use tokio::time::Instant;

use crate::types::WorkerProfile;

pub struct ProfileActivityTracker {
    inner: Cache<WorkerProfile, Instant>,
}

const ACTIVITY_MAX_PROFILES: u64 = 16_384;
const ACTIVITY_IDLE_TTL: Duration = Duration::from_secs(60 * 60);

impl ProfileActivityTracker {
    pub fn new() -> Self {
        Self {
            inner: Cache::builder()
                .max_capacity(ACTIVITY_MAX_PROFILES)
                .time_to_idle(ACTIVITY_IDLE_TTL)
                .build(),
        }
    }

    pub fn record(&self, profile: WorkerProfile, now: Instant) {
        self.inner.insert(profile, now);
    }

    pub fn last_seen(&self, profile: &WorkerProfile) -> Option<Instant> {
        self.inner.get(profile)
    }
}

impl Default for ProfileActivityTracker {
    fn default() -> Self {
        Self::new()
    }
}
