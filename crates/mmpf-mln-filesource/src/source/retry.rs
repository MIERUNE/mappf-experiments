//! Retry timing and per-attempt network-I/O budgeting.

use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use maplibre_native::file_source::{ErrorReason, Response};

/// Network-I/O timeout per fetch attempt (connect + headers + body). Admission
/// waits deliberately do not consume this budget. This must stay well below
/// the renderer SLA so one stalled resource cannot consume the whole request.
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// One short retry absorbs an ordinary connection race without holding a
/// native still render through a provider incident. MapLibre completes still
/// renders with an error when a required resource ends in error, so returning
/// the final response is both safe and preferable to a long retry loop.
pub(super) const RETRY_BACKOFF: [Duration; 1] = [Duration::from_millis(100)];

/// Initial request plus one retry. A render fans out to many resources, so a
/// larger per-resource attempt count would also multiply provider load.
pub(super) const MAX_ATTEMPTS: usize = 2;

/// Absolute cap on a server-requested delay. The tighter effective limit is
/// computed from the time left in `RETRY_WINDOW`, including one complete next
/// attempt; a fast 429 can therefore honor a few seconds without letting an
/// already-slow request overrun the render budget.
pub(super) const MAX_RETRY_DELAY: Duration = Duration::from_secs(3);

/// Deadline for admitting another retry. The attempt cap normally ends the
/// sequence first; this rejects long `Retry-After` values rather than sleeping
/// inside a render. Local admission waits remain outside network-I/O budgets.
pub(super) const RETRY_WINDOW: Duration = Duration::from_secs(5);

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

pub(super) fn retry_fits_budget(
    attempts_completed: usize,
    elapsed: Duration,
    delay: Duration,
) -> bool {
    attempts_completed < MAX_ATTEMPTS
        && delay <= MAX_RETRY_DELAY
        && elapsed
            .saturating_add(delay)
            .saturating_add(REQUEST_TIMEOUT)
            <= RETRY_WINDOW
}
