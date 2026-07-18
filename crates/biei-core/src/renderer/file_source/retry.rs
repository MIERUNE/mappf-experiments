//! Retry timing and per-attempt network-I/O budgeting.

use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use maplibre_native::file_source::{ErrorReason, Response};

/// Network-I/O timeout per fetch attempt (connect + headers + body). Admission
/// waits deliberately do not consume this budget.
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Backoff ladder for transient failures; later retries stay on the last
/// entry (plus deterministic per-URL jitter). Transient failures retry for as
/// long as MapLibre keeps the request alive, bounded by `RETRY_WINDOW`: mbgl's
/// Still mode never completes a render whose resources ended in a hard error,
/// so an early final error wedges the renderer thread on an unfinishable wait.
/// Definitive answers still return immediately, and mbgl cancellation aborts
/// the request task.
pub(super) const RETRY_BACKOFF: [Duration; 5] = [
    Duration::from_millis(100),
    Duration::from_millis(300),
    Duration::from_secs(1),
    Duration::from_secs(3),
    Duration::from_secs(10),
];

/// Cap on a single retry delay, including server-requested `Retry-After`.
pub(super) const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Upper bound on total retry time per request. Renders abandoned by their
/// callers stay pending inside mbgl, so this keeps their background fetch
/// churn finite while still riding out realistic upstream incidents.
pub(super) const RETRY_WINDOW: Duration = Duration::from_secs(600);

/// Counts only time spent performing network I/O. Admission waits are kept
/// outside `run`, so a cold burst cannot consume an attempt's timeout before
/// the request or response body reaches the network.
pub(super) struct NetworkAttemptBudget {
    pub(super) remaining: Duration,
}

impl NetworkAttemptBudget {
    pub(super) fn new() -> Self {
        Self {
            remaining: REQUEST_TIMEOUT,
        }
    }

    pub(super) async fn run<F>(
        &mut self,
        future: F,
    ) -> Result<F::Output, tokio::time::error::Elapsed>
    where
        F: Future,
    {
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(self.remaining, future).await;
        self.remaining = self.remaining.saturating_sub(started.elapsed());
        result
    }
}

pub(super) fn request_timeout_response() -> Response {
    Response::error(ErrorReason::Connection, "resource request timed out")
}

pub(super) fn retry_delay(url: &str, retry_index: usize) -> Duration {
    let base = RETRY_BACKOFF[retry_index.min(RETRY_BACKOFF.len() - 1)];
    let mut hasher = DefaultHasher::new();
    url.hash(&mut hasher);
    retry_index.hash(&mut hasher);
    base + Duration::from_millis(hasher.finish() % 50)
}
