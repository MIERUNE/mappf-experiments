//! Admission control for CPU-heavy request work.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use axum::http::StatusCode;
use ishikari_core::metrics::NodeMetrics;

use super::HttpError;

/// RAII reservation in the CPU-work admission counter. Reserving fails (a shed)
/// when the counter is already at its ceiling; the reservation is released on
/// drop — including when the awaiting future is cancelled before it acquires a
/// permit — so the count can never leak.
struct CpuWorkSlot {
    inflight: Arc<AtomicUsize>,
}

impl CpuWorkSlot {
    fn try_reserve(inflight: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        let previous = inflight.fetch_add(1, Ordering::Relaxed);
        if previous >= max {
            inflight.fetch_sub(1, Ordering::Relaxed);
            None
        } else {
            Some(Self {
                inflight: Arc::clone(inflight),
            })
        }
    }
}

impl Drop for CpuWorkSlot {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Admission ticket for one unit of CPU-bound request work. Holds both a
/// concurrency permit and an in-flight slot; dropping it releases both.
pub(super) struct CpuWorkPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _slot: CpuWorkSlot,
}

#[derive(Clone)]
pub(super) struct CpuWorkGate {
    semaphore: Arc<tokio::sync::Semaphore>,
    inflight: Arc<AtomicUsize>,
    concurrency: usize,
    max_inflight: usize,
}

pub(super) struct CpuWorkSnapshot {
    pub(super) running: usize,
    pub(super) inflight: usize,
    pub(super) concurrency: usize,
    pub(super) max_inflight: usize,
}

impl CpuWorkGate {
    pub(super) fn new(concurrency: usize, max_inflight: usize) -> Self {
        let concurrency = concurrency.max(1);
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(concurrency)),
            inflight: Arc::new(AtomicUsize::new(0)),
            concurrency,
            max_inflight: max_inflight.max(concurrency),
        }
    }

    /// Reserves an in-flight slot, shedding with `503` at the configured ceiling,
    /// and then waits for a concurrency permit. Hold the returned permit for the
    /// entire blocking job.
    pub(super) async fn admit(
        &self,
        metrics: &NodeMetrics,
        work: &'static str,
    ) -> Result<CpuWorkPermit, HttpError> {
        let queue_started = Instant::now();
        let slot =
            CpuWorkSlot::try_reserve(&self.inflight, self.max_inflight).ok_or_else(|| {
                metrics.record_cpu_work_admission(work, "shed");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "server overloaded".to_string(),
                )
            })?;
        let permit = self.semaphore.clone().acquire_owned().await.map_err(|_| {
            metrics.record_cpu_work_admission(work, "shutdown");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "cpu work is shutting down".to_string(),
            )
        })?;
        metrics.record_cpu_work_admission(work, "accepted");
        metrics.record_cpu_work_queue_duration(work, queue_started.elapsed());
        Ok(CpuWorkPermit {
            _permit: permit,
            _slot: slot,
        })
    }

    pub(super) fn snapshot(&self) -> CpuWorkSnapshot {
        let inflight = self.inflight.load(Ordering::Relaxed);
        let running = self
            .concurrency
            .saturating_sub(self.semaphore.available_permits());
        CpuWorkSnapshot {
            running,
            inflight,
            concurrency: self.concurrency,
            max_inflight: self.max_inflight,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::CpuWorkSlot;

    #[test]
    fn cpu_work_admission_sheds_at_ceiling_and_releases_on_drop() {
        let inflight = Arc::new(AtomicUsize::new(0));
        // Fill the two slots.
        let first = CpuWorkSlot::try_reserve(&inflight, 2).expect("first slot");
        let second = CpuWorkSlot::try_reserve(&inflight, 2).expect("second slot");
        // The third is shed while the counter is at its ceiling, and the failed
        // reservation must not leave the counter inflated.
        assert!(CpuWorkSlot::try_reserve(&inflight, 2).is_none());
        assert_eq!(inflight.load(Ordering::Relaxed), 2);
        // Freeing one slot re-opens admission.
        drop(first);
        let third = CpuWorkSlot::try_reserve(&inflight, 2).expect("slot after release");
        drop(second);
        drop(third);
        assert_eq!(inflight.load(Ordering::Relaxed), 0);
    }
}
