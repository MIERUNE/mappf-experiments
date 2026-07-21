//! Renderer capacity, orphan budget, and health classification.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use biei_core::types::WorkerId;
use mmpf_common::sync::lock_unpoisoned;

#[derive(Clone, Debug)]
pub(crate) struct RendererActorSupervisor {
    inner: Arc<RendererActorSupervisorInner>,
}

#[derive(Debug)]
struct RendererActorSupervisorInner {
    total_slots: usize,
    available_slots: AtomicUsize,
    max_orphaned_threads: usize,
    orphaned_threads: AtomicUsize,
    orphaned_by_worker: Mutex<HashMap<WorkerId, usize>>,
    replacements_succeeded: AtomicU64,
    replacements_exhausted: AtomicU64,
    replacements_failed: AtomicU64,
    provider_health: mmpf_mln_filesource::ProviderHealthTracker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RendererHealth {
    Full,
    ExternalDegraded,
    InternalUnrecoverable,
}

impl RendererHealth {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::ExternalDegraded => "external_degraded",
            Self::InternalUnrecoverable => "internal_unrecoverable",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RendererActorHealthSnapshot {
    pub total_slots: usize,
    pub available_slots: usize,
    pub orphaned_threads: usize,
    pub replacements_succeeded: u64,
    pub replacements_exhausted: u64,
    pub replacements_failed: u64,
    pub health: RendererHealth,
}

impl RendererActorSupervisor {
    #[cfg(test)]
    pub(crate) fn new(total_slots: usize) -> Self {
        Self::with_provider_health(
            total_slots,
            mmpf_mln_filesource::ProviderHealthTracker::new(),
        )
    }

    pub(crate) fn with_provider_health(
        total_slots: usize,
        provider_health: mmpf_mln_filesource::ProviderHealthTracker,
    ) -> Self {
        let total_slots = total_slots.max(1);
        Self {
            inner: Arc::new(RendererActorSupervisorInner {
                total_slots,
                available_slots: AtomicUsize::new(total_slots),
                // One abandoned native render per configured slot is enough
                // to recover a complete first-wave wedge without allowing an
                // attacker to leak threads indefinitely.
                max_orphaned_threads: total_slots,
                orphaned_threads: AtomicUsize::new(0),
                orphaned_by_worker: Mutex::new(HashMap::new()),
                replacements_succeeded: AtomicU64::new(0),
                replacements_exhausted: AtomicU64::new(0),
                replacements_failed: AtomicU64::new(0),
                provider_health,
            }),
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        !matches!(self.health(), RendererHealth::InternalUnrecoverable)
    }

    /// Per-slot render capacity: true while any slot is available, even when
    /// `ExternalDegraded`. Gating on `Full` would let one lost slot stop every
    /// healthy slot; a systemic outage self-limits via the orphan budget.
    pub(crate) fn can_start_render(&self) -> bool {
        self.inner.available_slots.load(Ordering::Acquire) > 0
    }

    /// A `can_start_render` closure installed as an immutable `NodeSpawn`
    /// dependency, so gossip and request handling share one policy from boot.
    pub(crate) fn render_admission_probe(&self) -> Arc<dyn Fn() -> bool + Send + Sync> {
        let supervisor = self.clone();
        Arc::new(move || supervisor.can_start_render())
    }

    /// External provider degradation is not repaired by restarting this
    /// process, so it remains live while an actual FileSource retry sequence is
    /// active. An unavailable slot without that evidence is an internal
    /// failure; autonomous repair gets the probe grace before process restart.
    pub(crate) fn is_livable(&self) -> bool {
        !matches!(self.health(), RendererHealth::InternalUnrecoverable)
    }

    pub(crate) fn health(&self) -> RendererHealth {
        if self.inner.available_slots.load(Ordering::Acquire) == self.inner.total_slots {
            return RendererHealth::Full;
        }

        // Elapsed time cannot turn a provider outage into an internal fault:
        // restarting still cannot repair the provider and destroys warm cache.
        // Slow-attempt evidence is promoted only after admission and a network
        // threshold, so normal fast traffic does not mask a renderer loss.
        if self.inner.provider_health.has_external_evidence() {
            RendererHealth::ExternalDegraded
        } else {
            RendererHealth::InternalUnrecoverable
        }
    }

    pub(crate) fn snapshot(&self) -> RendererActorHealthSnapshot {
        RendererActorHealthSnapshot {
            total_slots: self.inner.total_slots,
            available_slots: self.inner.available_slots.load(Ordering::Acquire),
            orphaned_threads: self.inner.orphaned_threads.load(Ordering::Acquire),
            replacements_succeeded: self.inner.replacements_succeeded.load(Ordering::Relaxed),
            replacements_exhausted: self.inner.replacements_exhausted.load(Ordering::Relaxed),
            replacements_failed: self.inner.replacements_failed.load(Ordering::Relaxed),
            health: self.health(),
        }
    }

    pub(super) fn try_reserve_orphan(&self, worker_id: WorkerId) -> bool {
        let mut by_worker = lock_unpoisoned(&self.inner.orphaned_by_worker);
        if by_worker.contains_key(&worker_id)
            || self.inner.orphaned_threads.load(Ordering::Acquire)
                >= self.inner.max_orphaned_threads
        {
            return false;
        }
        by_worker.insert(worker_id, 1);
        self.inner.orphaned_threads.fetch_add(1, Ordering::AcqRel);
        true
    }

    pub(super) fn reserve_orphan_unchecked(&self, worker_id: WorkerId) {
        *lock_unpoisoned(&self.inner.orphaned_by_worker)
            .entry(worker_id)
            .or_default() += 1;
        self.inner.orphaned_threads.fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn release_orphan(&self, worker_id: WorkerId) {
        let mut by_worker = lock_unpoisoned(&self.inner.orphaned_by_worker);
        let Some(count) = by_worker.get_mut(&worker_id) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            by_worker.remove(&worker_id);
        }
        self.inner.orphaned_threads.fetch_sub(1, Ordering::AcqRel);
    }

    pub(crate) fn record_replacement_succeeded(&self) {
        self.inner
            .replacements_succeeded
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_replacement_exhausted(&self) {
        self.inner
            .replacements_exhausted
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_replacement_failed(&self) {
        self.inner
            .replacements_failed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_slot_available(&self, available: &mut bool, next: bool) {
        if *available == next {
            return;
        }
        if next {
            self.inner.available_slots.fetch_add(1, Ordering::AcqRel);
        } else {
            self.inner.available_slots.fetch_sub(1, Ordering::AcqRel);
        }
        *available = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orphan_budget_is_fair_across_workers() {
        let supervisor = RendererActorSupervisor::new(2);

        assert!(supervisor.try_reserve_orphan(7));
        assert!(
            !supervisor.try_reserve_orphan(7),
            "one hot worker must not consume another slot's orphan budget"
        );
        assert!(supervisor.try_reserve_orphan(8));
        assert_eq!(supervisor.snapshot().orphaned_threads, 2);

        supervisor.release_orphan(7);
        assert!(supervisor.try_reserve_orphan(7));
        supervisor.release_orphan(7);
        supervisor.release_orphan(8);
        assert_eq!(supervisor.snapshot().orphaned_threads, 0);
    }

    #[test]
    fn unavailable_slot_sheds_readiness_before_process_recovery() {
        let supervisor = RendererActorSupervisor::new(2);
        let mut first_slot_available = true;

        supervisor.set_slot_available(&mut first_slot_available, false);

        assert!(
            !supervisor.is_ready(),
            "a degraded pod must stop accepting new work before restart"
        );
        assert!(
            !supervisor.is_livable(),
            "a permanently lost slot at exhausted budget needs process recovery"
        );
    }

    #[test]
    fn one_lost_slot_with_budget_remaining_requires_process_recovery() {
        // A hot worker may consume only its own orphan allowance while global
        // budget remains. If it wedges again, replacement is still impossible
        // for that slot. Readiness sheds the pod even though other slots remain;
        // liveness may eventually restore capacity after its recovery grace.
        let supervisor = RendererActorSupervisor::new(16);
        let mut lost_slot_available = true;

        // First wedge orphaned one thread; the second wedge on the same worker
        // is refused a replacement and marks the slot unavailable.
        assert!(supervisor.try_reserve_orphan(3));
        supervisor.set_slot_available(&mut lost_slot_available, false);

        let health = supervisor.snapshot();
        assert_eq!(health.available_slots, 15);
        assert_eq!(health.orphaned_threads, 1);
        assert!(
            !supervisor.is_ready(),
            "one unavailable slot must shed traffic before liveness restarts the pod"
        );
        assert!(
            !supervisor.is_livable(),
            "an unavailable slot must not remain hidden behind unused global orphan budget"
        );
    }

    #[test]
    fn health_distinguishes_active_provider_failure_from_internal_loss() {
        let provider = mmpf_mln_filesource::ProviderHealthTracker::new();
        let supervisor = RendererActorSupervisor::with_provider_health(2, provider.clone());
        assert_eq!(supervisor.health(), RendererHealth::Full);
        assert!(supervisor.can_start_render());

        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);
        assert_eq!(supervisor.health(), RendererHealth::InternalUnrecoverable);
        assert!(!supervisor.is_ready());
        assert!(!supervisor.is_livable());

        let retry = provider.begin_retry();
        assert_eq!(supervisor.health(), RendererHealth::ExternalDegraded);
        assert!(
            supervisor.is_ready(),
            "external degradation must keep cached responses reachable"
        );
        assert!(supervisor.is_livable());
        assert!(
            supervisor.can_start_render(),
            "the remaining healthy slot still renders while externally degraded"
        );

        drop(retry);
        assert_eq!(supervisor.health(), RendererHealth::InternalUnrecoverable);
    }

    #[tokio::test(start_paused = true)]
    async fn continuing_provider_evidence_never_becomes_restart_pressure() {
        let provider = mmpf_mln_filesource::ProviderHealthTracker::new();
        let supervisor = RendererActorSupervisor::with_provider_health(2, provider.clone());
        // Hold a process-global provider retry for the whole test.
        let _retry = provider.begin_retry();
        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);

        assert_eq!(supervisor.health(), RendererHealth::ExternalDegraded);
        assert!(supervisor.is_livable());

        tokio::time::advance(std::time::Duration::from_secs(24 * 60 * 60)).await;
        assert_eq!(supervisor.health(), RendererHealth::ExternalDegraded);
        assert!(
            supervisor.is_livable(),
            "elapsed time alone must not cause a cache-destroying restart"
        );

        supervisor.set_slot_available(&mut slot_available, true);
        assert_eq!(supervisor.health(), RendererHealth::Full);
    }

    #[test]
    fn one_lost_slot_does_not_stop_the_remaining_healthy_slots() {
        let supervisor = RendererActorSupervisor::new(3);
        assert!(supervisor.can_start_render());

        // One slot is lost: the pod is no longer `Full`, but the two healthy
        // slots must keep accepting renders. Gating on `Full` would amplify one
        // slot's fault into a whole-pod render outage, blocking even renders
        // that only touch already-cached resources.
        let mut a = true;
        supervisor.set_slot_available(&mut a, false);
        assert_ne!(supervisor.health(), RendererHealth::Full);
        assert!(
            supervisor.can_start_render(),
            "healthy slots keep rendering while one slot is down"
        );

        // All slots lost: only now, with no capacity at all, does admission
        // close.
        let mut b = true;
        supervisor.set_slot_available(&mut b, false);
        let mut c = true;
        supervisor.set_slot_available(&mut c, false);
        assert!(
            !supervisor.can_start_render(),
            "with no slot available the pod finally stops starting native work"
        );
    }
}
