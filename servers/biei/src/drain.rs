//! Shared production drain state.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use mmpf_common::sync::wait_for_change;
use tokio::sync::Notify;

#[derive(Clone, Debug)]
pub(crate) struct DrainController {
    inner: Arc<DrainState>,
}

#[derive(Debug)]
struct DrainState {
    accepting: AtomicBool,
    in_flight: AtomicUsize,
    idle: Notify,
}

#[derive(Debug)]
pub(crate) struct DrainPermit {
    inner: Arc<DrainState>,
}

impl DrainController {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(DrainState {
                accepting: AtomicBool::new(true),
                in_flight: AtomicUsize::new(0),
                idle: Notify::new(),
            }),
        }
    }

    pub(crate) fn begin_draining(&self) {
        self.inner.accepting.store(false, Ordering::Release);
        if self.in_flight() == 0 {
            self.inner.idle.notify_waiters();
        }
    }

    pub(crate) fn is_draining(&self) -> bool {
        !self.inner.accepting.load(Ordering::Acquire)
    }

    pub(crate) fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }

    pub(crate) fn try_acquire(&self) -> Option<DrainPermit> {
        if self.is_draining() {
            return None;
        }
        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
        if self.is_draining() {
            if self.inner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.inner.idle.notify_waiters();
            }
            return None;
        }
        Some(DrainPermit {
            inner: self.inner.clone(),
        })
    }

    pub(crate) async fn wait_idle(&self, timeout: Duration) -> bool {
        if self.in_flight() == 0 {
            return true;
        }
        // The last permit dropping fires `notify_waiters` (no stored permit),
        // so `wait_for_change` registers before re-checking `in_flight` to keep
        // a fully-drained node from blocking for the whole timeout.
        tokio::time::timeout(
            timeout,
            wait_for_change(&self.inner.idle, || {
                if self.in_flight() == 0 {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }),
        )
        .await
        .is_ok()
    }
}

impl Default for DrainController {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for DrainPermit {
    fn drop(&mut self) {
        if self.inner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_rejects_new_permits_and_waits_for_existing_work() {
        let drain = DrainController::new();
        let permit = drain.try_acquire().expect("accepting before drain");

        drain.begin_draining();

        assert!(drain.try_acquire().is_none());
        assert!(!drain.wait_idle(Duration::from_millis(1)).await);

        drop(permit);

        assert!(drain.wait_idle(Duration::from_secs(1)).await);
    }
}
