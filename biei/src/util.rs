//! Small crate-wide helpers.

use std::sync::{Mutex, MutexGuard};

use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts};

// Metric constructors: descriptors are static, so construction failure is a
// programming error and `expect` is appropriate at every call site.

pub(crate) fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), labels).expect("valid counter vec")
}

pub(crate) fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), labels).expect("valid gauge vec")
}

pub(crate) fn histogram_vec(name: &str, help: &str, labels: &[&str]) -> HistogramVec {
    HistogramVec::new(HistogramOpts::new(name, help), labels).expect("valid histogram vec")
}

pub(crate) fn histogram_vec_buckets(
    name: &str,
    help: &str,
    buckets: &[f64],
    labels: &[&str],
) -> HistogramVec {
    HistogramVec::new(
        HistogramOpts::new(name, help).buckets(buckets.to_vec()),
        labels,
    )
    .expect("valid histogram vec")
}

/// Lock a `Mutex`, recovering the guard if a previous holder panicked. biei's
/// mutexes guard plain data with no cross-lock invariant, so continuing from a
/// poisoned lock is safe and preferable to cascading the panic.
pub(crate) fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
