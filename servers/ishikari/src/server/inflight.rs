//! Cancellation-safe in-flight request accounting.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// RAII reservation in a bounded in-flight counter.
pub(super) struct InflightSlot {
    counter: Arc<AtomicUsize>,
}

impl InflightSlot {
    pub(super) fn try_reserve(counter: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        let previous = counter.fetch_add(1, Ordering::Relaxed);
        if previous >= max {
            counter.fetch_sub(1, Ordering::Relaxed);
            None
        } else {
            Some(Self {
                counter: Arc::clone(counter),
            })
        }
    }
}

impl Drop for InflightSlot {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::InflightSlot;

    #[test]
    fn reservation_sheds_at_ceiling_and_releases_on_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        let first = InflightSlot::try_reserve(&counter, 2).expect("first slot");
        let second = InflightSlot::try_reserve(&counter, 2).expect("second slot");

        assert!(InflightSlot::try_reserve(&counter, 2).is_none());
        assert_eq!(counter.load(Ordering::Relaxed), 2);

        drop(first);
        let third = InflightSlot::try_reserve(&counter, 2).expect("slot after release");
        drop(second);
        drop(third);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
