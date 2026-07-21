//! Synchronization helpers shared by service crates.

use std::ops::ControlFlow;
use std::sync::{Mutex, MutexGuard};

use tokio::sync::Notify;

/// Locks a mutex and recovers its data after a previous holder panicked.
///
/// Use this only for mutexes protecting independently valid data without a
/// cross-lock invariant. Recovery prevents an unrelated request from turning
/// one task panic into a process-wide failure.
pub fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Waits for a [`Notify`] change signal, re-evaluating `check` before parking.
///
/// This encapsulates the register-before-check discipline that using `Notify`
/// safely requires. [`Notify::notify_waiters`] stores no permit, and a
/// `Notified` future only joins the waiter list once polled — so a signal
/// fired between reading shared state and awaiting would otherwise be lost,
/// parking the caller until the next (possibly never) notification. Here the
/// future is pinned and `enable()`d *before* every `check`, so a
/// `notify_waiters` landing between the check and the park is retained.
///
/// `check` runs immediately and again after each wakeup. Returning
/// [`ControlFlow::Break`] yields its value; [`ControlFlow::Continue`] parks
/// until the next notification, then re-checks.
///
/// Pair this with `notify_waiters()` on the producer side. It is only correct
/// when the producer signals *after* making the state `check` observes visible.
pub async fn wait_for_change<T>(notify: &Notify, mut check: impl FnMut() -> ControlFlow<T>) -> T {
    // Fast path: resolve without registering as a waiter. `enable()` takes the
    // `Notify` lock and inserts into its waiter list, so callers that are
    // already satisfied (the hot case for a warm single-flight or a valid
    // cache snapshot) must not pay for it. `check` must be idempotent for the
    // `Continue` case, which this relies on — its `Break` arms may mutate.
    if let ControlFlow::Break(value) = check() {
        return value;
    }
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        // Register before re-checking: a `notify_waiters` landing between the
        // check above (or below) and the park cannot then be lost.
        notified.as_mut().enable();
        match check() {
            ControlFlow::Break(value) => return value,
            ControlFlow::Continue(()) => notified.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::ControlFlow;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::Notify;

    use super::{lock_unpoisoned, wait_for_change};

    #[tokio::test]
    async fn returns_immediately_when_condition_already_holds() {
        let notify = Notify::new();
        let value = wait_for_change(&notify, || ControlFlow::Break(7)).await;
        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn does_not_lose_a_wakeup_fired_before_the_park() {
        // A `notify_waiters` landing after the ready flag is set but before the
        // waiter parks must still release it — the exact lost-wakeup this helper
        // guards against. The waiter sees `Continue` on its first check, then the
        // flag flips and the notify fires while it is between check and await.
        let notify = Arc::new(Notify::new());
        let ready = Arc::new(AtomicBool::new(false));
        let waiter = {
            let (notify, ready) = (Arc::clone(&notify), Arc::clone(&ready));
            tokio::spawn(async move {
                wait_for_change(&notify, || {
                    if ready.load(Ordering::Acquire) {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .await
            })
        };
        tokio::task::yield_now().await;
        ready.store(true, Ordering::Release);
        notify.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter must not hang")
            .expect("waiter task panicked");
    }

    #[test]
    fn recovers_data_from_a_poisoned_mutex() {
        let data = Arc::new(Mutex::new(1));
        let worker_data = Arc::clone(&data);
        let _ = std::thread::spawn(move || {
            let mut guard = worker_data.lock().expect("initial lock");
            *guard = 2;
            panic!("poison mutex after updating recoverable data");
        })
        .join();

        assert!(data.is_poisoned());
        assert_eq!(*lock_unpoisoned(&data), 2);
    }
}
