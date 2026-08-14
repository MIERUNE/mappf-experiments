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

use super::{HttpError, inflight::InflightSlot};

/// Admission ticket for one unit of CPU-bound request work. Holds both a
/// concurrency permit and an in-flight slot; dropping it releases both.
pub(super) struct CpuWorkPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _slot: InflightSlot,
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
            InflightSlot::try_reserve(&self.inflight, self.max_inflight).ok_or_else(|| {
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
