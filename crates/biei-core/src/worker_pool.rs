//! `WorkerPool` — elastic worker pick + atomic BL reservation + dispatch to
//! `worker_loop` via mpsc.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::renderer::{BoxRenderer, PreparedProfile};
use crate::types::{
    InternalTask, NodeId, NodeKvs, ProcessError, RouteTier, TaskOutcome, TaskResult, WorkerId,
    WorkerProfile, WorkerView, encode_worker_kvs,
};
use crate::worker::{WorkerCmd, worker_loop};
use mmpf_common::sync::lock_unpoisoned;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PickTier {
    WarmComfort,
    Fresh,
    WarmFull,
    AllocSwapIdle,
    WarmOverflow,
    WarmSaturated,
    ShortestQueue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PickScore {
    tier: PickTier,
    source_miss: bool,
    queue_depth: usize,
    protected_singleton: bool,
    shape_mismatch: bool,
    profile_count: Reverse<usize>,
    last_seen: Option<Instant>,
}

struct PickContext<'a> {
    loaded: &'a [Option<WorkerProfile>],
    addlayer_source_ids: &'a [HashSet<String>],
    incoming_addlayer_source_id: Option<&'a str>,
    incoming_profile: &'a WorkerProfile,
    bl_capacity: usize,
    queue_capacity: usize,
    profile_counts: &'a HashMap<WorkerProfile, usize>,
    last_used: &'a [Option<Instant>],
}

pub(crate) struct WorkerHandle {
    pub tx: mpsc::Sender<WorkerCmd>,
    pub queue_depth: Arc<AtomicUsize>,
    /// Retained so a graceful shutdown can await the worker task. `None` after it
    /// has been joined (or its join timed out and the handle was detached).
    /// Interior-mutable so `WorkerPool::shutdown(&self, ..)` works through the
    /// shared `Arc<NodeInner>` without owning the pool.
    join: Mutex<Option<JoinHandle<()>>>,
}

/// Owns the accounting side effects of an accepted worker command.
///
/// This guard travels with `WorkerCmd`, rather than staying in the caller
/// future. An HTTP disconnect may drop the caller after the command has been
/// enqueued, but the native render is not cancellable and still consumes the
/// worker. Keeping the reservation with the command makes queue depth, drain,
/// and hard admission limits describe the work that actually remains.
pub(crate) struct WorkerCompletion {
    counter: Arc<AtomicUsize>,
    state: Arc<Mutex<PoolState>>,
    worker_idx: usize,
    dispatch_generation: u64,
    clear_loaded_on_drop: bool,
}

impl std::fmt::Debug for WorkerCompletion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerCompletion")
            .field("worker_idx", &self.worker_idx)
            .finish_non_exhaustive()
    }
}

impl WorkerCompletion {
    fn new(
        counter: Arc<AtomicUsize>,
        state: Arc<Mutex<PoolState>>,
        worker_idx: usize,
        dispatch_generation: u64,
    ) -> Self {
        Self {
            counter,
            state,
            worker_idx,
            dispatch_generation,
            // If a queued command is dropped before it produces an outcome,
            // the eager dispatch-time warm-state prediction is not valid.
            clear_loaded_on_drop: true,
        }
    }

    pub(crate) fn finish(mut self, outcome: &TaskOutcome) {
        if matches!(
            outcome.result,
            TaskResult::Failed { .. } | TaskResult::Rejected { .. }
        ) {
            lock_unpoisoned(&self.state)
                .clear_loaded_if_latest(self.worker_idx, self.dispatch_generation);
        }
        self.clear_loaded_on_drop = false;
    }
}

impl Drop for WorkerCompletion {
    fn drop(&mut self) {
        if self.clear_loaded_on_drop {
            lock_unpoisoned(&self.state)
                .clear_loaded_if_latest(self.worker_idx, self.dispatch_generation);
        }
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Pool-side view of "which profile each worker is now (logically) committed
/// to". Updated eagerly at dispatch time, so it reflects the worker's state
/// after its queue drains — not necessarily right now.
pub(crate) struct PoolState {
    pub loaded: Vec<Option<WorkerProfile>>,
    dispatch_generations: Vec<u64>,
    profile_counts: HashMap<WorkerProfile, usize>,
    last_used: Vec<Option<Instant>>,
    addlayer_source_ids: Vec<HashSet<String>>,
    addlayer_source_lru: Vec<VecDeque<String>>,
}

impl PoolState {
    fn new(n: usize) -> Self {
        Self {
            loaded: vec![None; n],
            dispatch_generations: vec![0; n],
            profile_counts: HashMap::new(),
            last_used: vec![None; n],
            addlayer_source_ids: vec![HashSet::new(); n],
            addlayer_source_lru: vec![VecDeque::new(); n],
        }
    }

    fn mark_loaded(&mut self, idx: usize, profile: WorkerProfile) -> u64 {
        self.dispatch_generations[idx] = self.dispatch_generations[idx].wrapping_add(1);
        let generation = self.dispatch_generations[idx];
        self.last_used[idx] = Some(Instant::now());
        if self.loaded[idx].as_ref() == Some(&profile) {
            return generation;
        }

        if let Some(previous) = self.loaded[idx].take() {
            self.decrement_profile_count(&previous);
        }
        self.clear_addlayer_sources(idx);
        *self.profile_counts.entry(profile.clone()).or_insert(0) += 1;
        self.loaded[idx] = Some(profile);
        generation
    }

    fn mark_addlayer_source(&mut self, idx: usize, source_id: String) {
        const ADDLAYER_SOURCE_AFFINITY_CAPACITY: usize = 64;
        let ids = &mut self.addlayer_source_ids[idx];
        let lru = &mut self.addlayer_source_lru[idx];
        if ids.contains(&source_id) {
            lru.retain(|cached| cached != &source_id);
            lru.push_back(source_id);
            return;
        }
        while ids.len() >= ADDLAYER_SOURCE_AFFINITY_CAPACITY
            && let Some(evicted) = lru.pop_front()
        {
            ids.remove(&evicted);
        }
        ids.insert(source_id.clone());
        lru.push_back(source_id);
    }

    fn clear_loaded(&mut self, idx: usize) {
        if let Some(previous) = self.loaded[idx].take() {
            self.decrement_profile_count(&previous);
        }
        self.last_used[idx] = None;
        self.clear_addlayer_sources(idx);
    }

    fn clear_loaded_if_latest(&mut self, idx: usize, dispatch_generation: u64) {
        if self.dispatch_generations[idx] == dispatch_generation {
            self.clear_loaded(idx);
        }
    }

    fn decrement_profile_count(&mut self, profile: &WorkerProfile) {
        let remove = if let Some(count) = self.profile_counts.get_mut(profile) {
            *count -= 1;
            *count == 0
        } else {
            false
        };
        if remove {
            self.profile_counts.remove(profile);
        }
    }

    fn clear_addlayer_sources(&mut self, idx: usize) {
        self.addlayer_source_ids[idx].clear();
        self.addlayer_source_lru[idx].clear();
    }
}

/// Lightweight, cheap-to-clone handle for producing the pool's KV snapshot
/// (the publisher diffs against the last sent snapshot and gossips changed
/// keys).
#[derive(Clone)]
pub(crate) struct PoolSnapshotter {
    pub queue_depths: Vec<Arc<AtomicUsize>>,
    pub state: Arc<Mutex<PoolState>>,
}

impl PoolSnapshotter {
    /// Encode current per-slot (loaded_profile, queue) into a flat KV map
    /// suitable for a batched `GossipBus::set_many` call.
    pub(crate) fn snapshot_kvs(&self) -> NodeKvs {
        let s = lock_unpoisoned(&self.state);
        let mut out = NodeKvs::new();
        for (i, qd) in self.queue_depths.iter().enumerate() {
            let depth = qd.load(Ordering::Relaxed);
            let profile = s.loaded[i].as_ref();
            encode_worker_kvs(&mut out, i as WorkerId, profile, depth);
        }
        out
    }

    pub(crate) fn snapshot_workers(&self) -> Vec<WorkerView> {
        let s = lock_unpoisoned(&self.state);
        self.queue_depths
            .iter()
            .enumerate()
            .map(|(i, qd)| WorkerView {
                id: i as WorkerId,
                loaded_profile: s.loaded[i].clone(),
                queue_depth: qd.load(Ordering::Relaxed),
            })
            .collect()
    }
}

pub(crate) struct WorkerPool {
    pub workers: Vec<WorkerHandle>,
    pub state: Arc<Mutex<PoolState>>,
    /// SLA-oriented soft queue limit per renderer slot (BL).
    pub bl_capacity: usize,
    /// Hard admission/backpressure limit per renderer slot.
    pub queue_capacity: usize,
    /// Node-wide render execution permits (held across a task's whole
    /// worker-side execution: setup → source → render).
    pub render_permits: usize,
    render_permit_sem: Arc<Semaphore>,
    /// Node-wide CPU/GPU-heavy render-stage permits. Defaults to
    /// `render_permits` at config resolution time, but may be lower to model
    /// I/O overlap with a fixed render bottleneck.
    pub native_render_permits: usize,
    native_render_permit_sem: Arc<Semaphore>,
}

pub(crate) struct WorkerPoolSpawn {
    pub node_id: NodeId,
    pub renderers: Vec<BoxRenderer>,
    pub bl_capacity: usize,
    pub queue_capacity: usize,
    pub render_permits: usize,
    pub native_render_permits: usize,
    pub source_cache_capacity: usize,
}

impl WorkerPool {
    pub(crate) fn spawn(spec: WorkerPoolSpawn) -> Self {
        let WorkerPoolSpawn {
            node_id,
            renderers,
            bl_capacity,
            queue_capacity,
            render_permits,
            native_render_permits,
            source_cache_capacity,
        } = spec;
        let n = renderers.len();
        let render_permits = render_permits.max(1).min(n.max(1));
        let native_render_permits = native_render_permits.max(1).min(render_permits);
        let permit_sem = Arc::new(Semaphore::new(render_permits));
        let native_render_permit_sem = Arc::new(Semaphore::new(native_render_permits));
        let mut workers = Vec::with_capacity(n);
        for (i, renderer) in renderers.into_iter().enumerate() {
            let (tx, rx) = mpsc::channel(1024);
            let queue_depth = Arc::new(AtomicUsize::new(0));
            let id = i as WorkerId;
            let worker_node_id = node_id.clone();
            let worker_permits = permit_sem.clone();
            let worker_native_render_permits = native_render_permit_sem.clone();
            let join = tokio::spawn(async move {
                worker_loop(
                    id,
                    worker_node_id,
                    rx,
                    renderer,
                    worker_permits,
                    worker_native_render_permits,
                    source_cache_capacity,
                )
                .await;
            });
            workers.push(WorkerHandle {
                tx,
                queue_depth,
                join: Mutex::new(Some(join)),
            });
        }
        Self {
            workers,
            state: Arc::new(Mutex::new(PoolState::new(n))),
            bl_capacity,
            queue_capacity: queue_capacity.max(bl_capacity),
            render_permits,
            render_permit_sem: permit_sem,
            native_render_permits,
            native_render_permit_sem,
        }
    }

    /// Tasks currently executing on a worker (holding the render permit across
    /// any stage: setup, source load, or render). Used by the simulator to
    /// tell when a draining node has finished all of its *local* work,
    /// independent of tasks it forwarded to peers.
    pub(crate) fn render_permits_inuse(&self) -> usize {
        self.render_permits
            .saturating_sub(self.render_permit_sem.available_permits())
    }

    pub(crate) fn native_render_permits_inuse(&self) -> usize {
        self.native_render_permits
            .saturating_sub(self.native_render_permit_sem.available_permits())
    }

    pub(crate) fn snapshotter(&self) -> PoolSnapshotter {
        PoolSnapshotter {
            queue_depths: self.workers.iter().map(|w| w.queue_depth.clone()).collect(),
            state: self.state.clone(),
        }
    }

    fn queue_at(&self, idx: usize) -> usize {
        self.workers[idx].queue_depth.load(Ordering::Relaxed)
    }

    /// Pick local worker. Priority (uses two warm bands so we expand
    /// proactively before warm workers saturate). Warm judgment is on
    /// `loaded_profile == Some(task.worker_profile())` (style revision + mode
    /// + scale), so style updates, Static/Tile changes, and @1x/@2x changes
    ///   use separate warm workers.
    ///   1. Warm-for-profile with queue < 2/3·BL — comfortable, use it.
    ///   2. None worker — cold-start. Fires both when no warm exists *and*
    ///      when warm workers are getting busy (queue ≥ 2/3·BL).
    ///   3. Warm-for-profile with 2/3·BL ≤ queue < BL — None ran out.
    ///   4. Allocation-aware swap (idle worker only).
    ///   5. Transient overload: warm-for-profile up to admission cap.
    ///   6. Fallback: saturated warm-for-profile → reject path.
    fn best_available_worker(
        &self,
        task: &InternalTask,
        addlayer_source_id: Option<&str>,
    ) -> Option<usize> {
        let s = lock_unpoisoned(&self.state);
        let task_profile = task.worker_profile();
        let ctx = PickContext {
            loaded: &s.loaded,
            addlayer_source_ids: &s.addlayer_source_ids,
            incoming_addlayer_source_id: addlayer_source_id,
            incoming_profile: &task_profile,
            bl_capacity: self.bl_capacity,
            queue_capacity: self.queue_capacity,
            profile_counts: &s.profile_counts,
            last_used: &s.last_used,
        };

        (0..self.workers.len())
            .filter_map(|idx| {
                let queue_depth = self.queue_at(idx);
                (queue_depth < self.queue_capacity)
                    .then(|| (idx, pick_score(&ctx, idx, queue_depth)))
            })
            .min_by_key(|(_, score)| *score)
            .map(|(idx, _)| idx)
    }

    /// Atomically reserve one queue slot at `idx` without breaching the hard
    /// queue limit. Returns the pre-admission depth, or `None` if the worker is
    /// already at the hard limit.
    fn try_reserve(&self, idx: usize) -> Option<usize> {
        let w = &self.workers[idx];
        let mut current = w.queue_depth.load(Ordering::Acquire);
        loop {
            if current >= self.queue_capacity {
                return None;
            }
            match w.queue_depth.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(current),
                Err(actual) => current = actual,
            }
        }
    }

    /// Reserve the best worker that currently has capacity. The common path is
    /// one allocation-free linear scan. If another task fills that worker before
    /// the CAS, rescan current state rather than sorting every worker up front.
    fn reserve_best_available(
        &self,
        task: &InternalTask,
        addlayer_source_id: Option<&str>,
    ) -> Option<(usize, usize)> {
        loop {
            let idx = self.best_available_worker(task, addlayer_source_id)?;
            if let Some(depth) = self.try_reserve(idx) {
                return Some((idx, depth));
            }
        }
    }

    /// Process a task at one of the local workers. Returns `QueueFull(task)`
    /// if the pool cannot accept without breaching the hard queue limit.
    /// Tier 3 bypasses the BL soft limit but still respects the hard limit.
    pub(crate) async fn process(
        &self,
        task: InternalTask,
        prepared_profile: Option<PreparedProfile>,
        route_tier: RouteTier,
        worker_hint: Option<WorkerId>,
    ) -> Result<TaskOutcome, ProcessError> {
        let addlayer_source_id = prepared_profile
            .as_ref()
            .and_then(|prepared| prepared.addlayer_source.as_ref())
            .map(|source| source.stable_source_id());
        // Reserve a worker before committing any predicted profile/source
        // state. A hint intentionally targets one worker (e.g. an eviction
        // target), so it is honored exactly with no fallback. Otherwise select
        // the best worker with current capacity and rescan only after a lost
        // check-and-reserve race.
        let (idx, pre_admit_depth) = match worker_hint {
            Some(wid) if (wid as usize) < self.workers.len() => {
                let idx = wid as usize;
                match self.try_reserve(idx) {
                    Some(depth) => (idx, depth),
                    None => return Err(ProcessError::QueueFull(Box::new(task))),
                }
            }
            _ => match self.reserve_best_available(&task, addlayer_source_id.as_deref()) {
                Some(reserved) => reserved,
                None => return Err(ProcessError::QueueFull(Box::new(task))),
            },
        };
        let w = &self.workers[idx];
        let admitted_at_overflow = pre_admit_depth >= self.bl_capacity;
        let task_profile = task.worker_profile();
        let dispatch_generation = {
            let mut s = lock_unpoisoned(&self.state);
            let generation = s.mark_loaded(idx, task_profile);
            if let Some(source_id) = addlayer_source_id {
                s.mark_addlayer_source(idx, source_id);
            }
            generation
        };
        // Move completion accounting into the command. The caller future may
        // be cancelled after `send`, while the worker must still execute the
        // non-cancellable native render and remain visible to admission/drain.
        let completion = WorkerCompletion::new(
            w.queue_depth.clone(),
            self.state.clone(),
            idx,
            dispatch_generation,
        );
        let (tx, rx) = oneshot::channel();
        if let Err(err) =
            w.tx.send(WorkerCmd::Process {
                task,
                prepared_profile,
                route_tier,
                admitted_at_overflow,
                respond_to: tx,
                completion,
            })
            .await
        {
            // We only ever send `Process` here; recover its task on send failure.
            if let WorkerCmd::Process { task, .. } = err.0 {
                return Err(ProcessError::QueueFull(Box::new(task)));
            }
            unreachable!("worker send failure returns the Process command we sent");
        }
        rx.await.map_err(|_| ProcessError::QueueDisconnected)
    }

    /// Gracefully stop every worker within `deadline` and report the outcome.
    ///
    /// Each worker is asked to `Retire` — it drains the `Process` commands
    /// already queued ahead of that message (native renders are non-preemptible
    /// and must finish) and then exits. A worker whose task does not finish
    /// within `deadline` is detached (its native render keeps running) and
    /// counted as `timed_out`, so the caller can distinguish a clean shutdown
    /// from a forced one rather than silently treating both as graceful.
    pub(crate) async fn shutdown(&self, deadline: Instant) -> WorkerShutdown {
        for worker in &self.workers {
            if tokio::time::timeout_at(deadline, worker.tx.send(WorkerCmd::Retire))
                .await
                .is_err()
            {
                break;
            }
        }
        let mut outcome = WorkerShutdown::default();
        for worker in &self.workers {
            let Some(handle) = lock_unpoisoned(&worker.join).take() else {
                continue;
            };
            match tokio::time::timeout_at(deadline, handle).await {
                Ok(_) => outcome.joined += 1,
                Err(_) => outcome.timed_out += 1,
            }
        }
        outcome
    }
}

/// Result of [`WorkerPool::shutdown`]: how many worker tasks joined cleanly
/// versus were still running at the deadline and had to be detached.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WorkerShutdown {
    pub joined: usize,
    pub timed_out: usize,
}

impl WorkerShutdown {
    pub fn is_complete(self) -> bool {
        self.timed_out == 0
    }
}

#[cfg(test)]
fn profile_counts(loaded: &[Option<WorkerProfile>]) -> HashMap<WorkerProfile, usize> {
    let mut counts = HashMap::new();
    for profile in loaded.iter().flatten() {
        *counts.entry(profile.clone()).or_insert(0) += 1;
    }
    counts
}

fn pick_score(ctx: &PickContext<'_>, idx: usize, queue_depth: usize) -> PickScore {
    let loaded_profile = ctx.loaded[idx].as_ref();
    let warm = loaded_profile == Some(ctx.incoming_profile);
    let expand_threshold = (ctx.bl_capacity * 2 / 3).max(1);

    let tier = match loaded_profile {
        Some(_) if warm && queue_depth < expand_threshold => PickTier::WarmComfort,
        None => PickTier::Fresh,
        Some(_) if warm && queue_depth < ctx.bl_capacity => PickTier::WarmFull,
        Some(profile) if profile != ctx.incoming_profile && queue_depth == 0 => {
            PickTier::AllocSwapIdle
        }
        Some(_) if warm && queue_depth < ctx.queue_capacity => PickTier::WarmOverflow,
        Some(_) if warm => PickTier::WarmSaturated,
        Some(_) => PickTier::ShortestQueue,
    };

    let (protected_singleton, shape_mismatch, profile_count, last_seen) =
        if tier == PickTier::AllocSwapIdle {
            let profile = loaded_profile.expect("alloc swap candidate is assigned");
            let count = ctx.profile_counts.get(profile).copied().unwrap_or(0);
            (
                count <= 1,
                profile.render_mode != ctx.incoming_profile.render_mode
                    || profile.scale != ctx.incoming_profile.scale,
                Reverse(count),
                ctx.last_used[idx],
            )
        } else {
            (false, false, Reverse(usize::MAX), None)
        };
    let source_miss = ctx
        .incoming_addlayer_source_id
        .is_some_and(|id| !ctx.addlayer_source_ids[idx].contains(id));

    PickScore {
        tier,
        source_miss,
        queue_depth,
        protected_singleton,
        shape_mismatch,
        profile_count,
        last_seen,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::renderer::{PreparedProfile, Renderer, RendererOutput};
    use crate::types::{
        AddLayer, AddLayerSource, CredentialCachePartition, ImageFormat, InternalTask,
        NamespaceSet, Padding, PixelRatio, Positioning, RenderAuthorization, RenderMode,
        RenderOutput, RenderRequest, RendererError, Scale, SourceHash, StyleId, StyleRevision,
        TaskId, TaskResult,
    };

    use super::*;

    struct NoopRenderer;

    struct FailingRenderer;

    struct SlowRenderer {
        delay: Duration,
        retire_count: Arc<AtomicUsize>,
    }

    struct CountingRenderer {
        inflight: Arc<AtomicUsize>,
        max_seen: Arc<AtomicUsize>,
        delay: Duration,
    }

    struct RenderFailingRenderer {
        setup_count: Arc<AtomicUsize>,
    }

    struct SourceSetupRenderer;

    struct GatedFailingRenderer {
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }

    struct RepairProbeRenderer {
        repair_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Renderer for NoopRenderer {
        async fn setup_profile(
            &mut self,
            _task: &InternalTask,
            _prepared: Option<PreparedProfile>,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            }
            .into())
        }
    }

    #[async_trait::async_trait]
    impl Renderer for FailingRenderer {
        async fn setup_profile(
            &mut self,
            task: &InternalTask,
            _prepared: Option<PreparedProfile>,
        ) -> Result<(), RendererError> {
            Err(RendererError::StyleLoadFailed {
                style_id: task.style.id.clone(),
                source: "test failure".to_string(),
            })
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            }
            .into())
        }
    }

    #[async_trait::async_trait]
    impl Renderer for SlowRenderer {
        async fn setup_profile(
            &mut self,
            _task: &InternalTask,
            _prepared: Option<PreparedProfile>,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
            tokio::time::sleep(self.delay).await;
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            }
            .into())
        }

        fn retire_after_current(&mut self) {
            self.retire_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[async_trait::async_trait]
    impl Renderer for CountingRenderer {
        async fn setup_profile(
            &mut self,
            _task: &InternalTask,
            _prepared: Option<PreparedProfile>,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
            let current = self.inflight.fetch_add(1, Ordering::AcqRel) + 1;
            self.max_seen.fetch_max(current, Ordering::AcqRel);
            tokio::time::sleep(self.delay).await;
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            }
            .into())
        }
    }

    #[async_trait::async_trait]
    impl Renderer for RenderFailingRenderer {
        async fn setup_profile(
            &mut self,
            _task: &InternalTask,
            _prepared: Option<PreparedProfile>,
        ) -> Result<(), RendererError> {
            self.setup_count.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, _task: &InternalTask) -> Result<RendererOutput, RendererError> {
            Err(RendererError::RenderFailed(
                "test render failure".to_string(),
            ))
        }
    }

    #[async_trait::async_trait]
    impl Renderer for SourceSetupRenderer {
        async fn setup_profile(
            &mut self,
            _task: &InternalTask,
            _prepared: Option<PreparedProfile>,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
            let has_resolved_source = matches!(
                &task.request,
                RenderRequest::StaticImage {
                    addlayer: Some(AddLayer {
                        source: Some(AddLayerSource { json, .. }),
                        ..
                    }),
                    ..
                } if json.contains("https://tiles.test/")
            );
            if !has_resolved_source {
                return Err(RendererError::RenderFailed(
                    "prepared addlayer source was not applied".to_string(),
                ));
            }
            Ok(RendererOutput {
                output: RenderOutput {
                    bytes: bytes::Bytes::new(),
                    format: task.output_format,
                },
                source_setup_duration: Some(Duration::from_millis(3)),
            })
        }
    }

    #[async_trait::async_trait]
    impl Renderer for GatedFailingRenderer {
        async fn setup_profile(
            &mut self,
            task: &InternalTask,
            _prepared: Option<PreparedProfile>,
        ) -> Result<(), RendererError> {
            self.started.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("test release semaphore remains open")
                .forget();
            Err(RendererError::StyleLoadFailed {
                style_id: task.style.id.clone(),
                source: "gated test failure".to_string(),
            })
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            }
            .into())
        }
    }

    #[async_trait::async_trait]
    impl Renderer for RepairProbeRenderer {
        async fn setup_profile(
            &mut self,
            _task: &InternalTask,
            _prepared: Option<PreparedProfile>,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            }
            .into())
        }

        fn repair_if_needed(&mut self) -> Result<bool, RendererError> {
            self.repair_count.fetch_add(1, Ordering::AcqRel);
            Ok(true)
        }
    }

    #[tokio::test]
    async fn idle_worker_runs_autonomous_renderer_repair() {
        let repair_count = Arc::new(AtomicUsize::new(0));
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(RepairProbeRenderer {
                repair_count: repair_count.clone(),
            })],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while repair_count.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("idle worker must repair without an admitted task");
        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn caller_cancellation_keeps_command_accounted_until_worker_finishes() {
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let pool = Arc::new(WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(GatedFailingRenderer {
                started: started.clone(),
                release: release.clone(),
            })],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        }));

        let caller_pool = pool.clone();
        let caller = tokio::spawn(async move {
            caller_pool
                .process(make_task(1, 1), None, RouteTier::Tier2HrwBl, Some(0))
                .await
        });
        started
            .acquire()
            .await
            .expect("worker start semaphore remains open")
            .forget();
        assert_eq!(pool.queue_at(0), 1);
        assert!(lock_unpoisoned(&pool.state).loaded[0].is_some());

        caller.abort();
        let _ = caller.await;
        assert_eq!(
            pool.queue_at(0),
            1,
            "dropping the caller must not hide an executing native command"
        );

        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while pool.queue_at(0) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker finishes after release");
        assert!(
            lock_unpoisoned(&pool.state).loaded[0].is_none(),
            "worker-side failure must clear eager warm state without a caller"
        );

        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn completed_task_carries_calibration_observation() {
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(NoopRenderer)],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        let outcome = pool
            .process(make_task(1, 1), None, RouteTier::Tier2HrwBl, Some(0))
            .await
            .expect("task processes");
        let TaskResult::Completed { info, .. } = outcome.result else {
            panic!("expected completion");
        };
        let observation = info.render_observation.expect("render observation");
        assert_eq!(observation.render_mode, RenderMode::Tile);
        assert_eq!(observation.scale, Scale::X1);
        assert_eq!(observation.output_format, ImageFormat::Png);
        assert_eq!((observation.width, observation.height), (256, 256));
        assert!(observation.style_setup_duration.is_some());
        assert_eq!(observation.source_setup_duration, None);

        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn reservation_falls_through_a_full_worker_to_an_idle_one() {
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(NoopRenderer), Box::new(NoopRenderer)],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        // Fill worker 0 to the hard queue limit.
        assert_eq!(pool.try_reserve(0), Some(0));
        assert_eq!(pool.try_reserve(0), None, "worker 0 is at the hard limit");

        // With the best candidate full, reservation must fall through to the
        // idle worker 1 instead of rejecting (the pre-fix behavior).
        let task = make_task(1, 1);
        let (idx, depth) = pool
            .reserve_best_available(&task, None)
            .expect("an idle worker must still admit the task");
        assert_eq!(idx, 1);
        assert_eq!(depth, 0);
        assert_eq!(pool.queue_at(1), 1);

        // Only when every candidate is full does reservation give up.
        assert_eq!(pool.reserve_best_available(&task, None), None);

        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn addlayer_source_setup_is_reported_from_the_renderer() {
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(SourceSetupRenderer)],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });
        let mut task = make_task(1, 1);
        task.request = RenderRequest::StaticImage {
            positioning: Positioning::Center {
                lon: 0.0,
                lat: 0.0,
                zoom: 1.0,
                bearing: 0.0,
                pitch: 0.0,
            },
            width: 256,
            height: 256,
            overlays: Vec::new(),
            before_layer: None,
            padding: Padding::default(),
            addlayer: Some(AddLayer {
                json: r#"{"id":"test","type":"fill","source":{"type":"vector","url":"tiles"}}"#
                    .to_string(),
                hash: 1,
                source: Some(AddLayerSource {
                    tileset_id: "tiles".to_string(),
                    json: r#"{"type":"vector","url":"tiles"}"#.to_string(),
                }),
            }),
        };
        let prepared = PreparedProfile {
            revision: task.style.clone(),
            authorization_partition: None,
            style_json: Arc::from("{}"),
            addlayer_source: Some(AddLayerSource {
                tileset_id: "tiles".to_string(),
                json: r#"{"type":"vector","tiles":["https://tiles.test/{z}/{x}/{y}.pbf"]}"#
                    .to_string(),
            }),
        };

        let outcome = pool
            .process(task, Some(prepared), RouteTier::Tier2HrwBl, Some(0))
            .await
            .expect("task processes");
        assert!(outcome.had_source);
        let TaskResult::Completed { info, .. } = outcome.result else {
            panic!("expected completion");
        };
        assert!(info.source_loaded);
        assert_eq!(
            info.render_observation
                .expect("render observation")
                .source_setup_duration,
            Some(Duration::from_millis(3))
        );

        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    fn rev(id: u32) -> StyleRevision {
        StyleRevision {
            id: StyleId(format!("style-{}", id)),
            version: 0,
        }
    }

    fn lp(id: u32) -> WorkerProfile {
        lp_with(id, RenderMode::Tile, Scale::X1)
    }

    fn lp_with(id: u32, render_mode: RenderMode, scale: Scale) -> WorkerProfile {
        WorkerProfile {
            style: rev(id),
            render_mode,
            scale,
        }
    }

    fn make_task(id: TaskId, style_index: u32) -> InternalTask {
        let now = Instant::now();
        InternalTask {
            id,
            request_id: crate::types::RequestId::from_string("worker-pool-test"),
            authorization: None,
            style: rev(style_index),
            source: None,
            request: RenderRequest::Tile {
                z: 14,
                x: 0,
                y: 0,
                tile_size: 256,
            },
            pixel_ratio: PixelRatio::X1,
            output_format: ImageFormat::Png,
            arrived_at: now,
            deadline: now + Duration::from_secs(10),
            forwarding_hops: 0,
        }
    }

    fn alloc_swap_candidate(
        loaded: &[Option<WorkerProfile>],
        queue_depths: &[usize],
        incoming_profile: &WorkerProfile,
        last_used: &[Option<Instant>],
    ) -> Option<usize> {
        let counts = profile_counts(loaded);
        let addlayer_source_ids = vec![HashSet::new(); loaded.len()];
        let ctx = PickContext {
            loaded,
            addlayer_source_ids: &addlayer_source_ids,
            incoming_addlayer_source_id: None,
            incoming_profile,
            bl_capacity: 1,
            queue_capacity: 1,
            profile_counts: &counts,
            last_used,
        };
        (0..loaded.len())
            .filter_map(|idx| {
                let score = pick_score(&ctx, idx, queue_depths[idx]);
                (score.tier == PickTier::AllocSwapIdle).then_some((idx, score))
            })
            .min_by_key(|(_, score)| *score)
            .map(|(idx, _)| idx)
    }

    #[test]
    fn profile_counts_ignores_unassigned_workers() {
        let mut state = PoolState::new(4);
        state.mark_loaded(0, lp(1));
        state.mark_loaded(2, lp(1));
        state.mark_loaded(3, lp(2));

        assert_eq!(state.profile_counts.get(&lp(1)), Some(&2));
        assert_eq!(state.profile_counts.get(&lp(2)), Some(&1));
        assert_eq!(state.profile_counts.get(&lp(3)), None);

        state.mark_loaded(2, lp(2));
        state.clear_loaded(0);
        assert_eq!(state.profile_counts.get(&lp(1)), None);
        assert_eq!(state.profile_counts.get(&lp(2)), Some(&2));
    }

    #[test]
    fn older_failure_cannot_clear_a_later_dispatch_prediction() {
        let mut state = PoolState::new(1);
        let first = state.mark_loaded(0, lp(1));
        let second = state.mark_loaded(0, lp(2));

        state.clear_loaded_if_latest(0, first);
        assert_eq!(state.loaded[0], Some(lp(2)));

        state.clear_loaded_if_latest(0, second);
        assert_eq!(state.loaded[0], None);
    }

    #[test]
    fn local_swap_prefers_over_allocated_style_then_oldest_seen() {
        let now = Instant::now();

        let picked = alloc_swap_candidate(
            &[Some(lp(1)), Some(lp(1)), Some(lp(2)), Some(lp(2))],
            &[0, 0, 0, 0],
            &lp(9),
            &[
                Some(now),
                Some(now - Duration::from_secs(40)),
                Some(now - Duration::from_secs(20)),
                Some(now - Duration::from_secs(10)),
            ],
        )
        .unwrap();

        assert_eq!(picked, 1);
    }

    #[test]
    fn local_swap_prefers_same_renderer_shape_within_over_allocated_profiles() {
        let now = Instant::now();
        let different_shape = lp_with(1, RenderMode::Static, Scale::X1);
        let same_shape = lp(2);

        let picked = alloc_swap_candidate(
            &[
                Some(different_shape.clone()),
                Some(same_shape.clone()),
                Some(different_shape),
                Some(same_shape),
            ],
            &[0, 0, 0, 0],
            &lp(9),
            &[
                Some(now - Duration::from_secs(20)),
                Some(now),
                Some(now - Duration::from_secs(30)),
                Some(now - Duration::from_secs(10)),
            ],
        );

        assert_eq!(picked, Some(3));
    }

    #[test]
    fn local_swap_skips_full_workers_and_incoming_profile() {
        let picked = alloc_swap_candidate(
            &[Some(lp(9)), Some(lp(1)), Some(lp(1)), Some(lp(2))],
            &[0, 1, 0, 0],
            &lp(9),
            &[None; 4],
        );

        assert_eq!(picked, Some(2));
    }

    #[test]
    fn local_swap_can_steal_idle_singleton_when_no_over_allocated_style_exists() {
        let picked = alloc_swap_candidate(
            &[Some(lp(9)), Some(lp(1)), Some(lp(2))],
            &[1, 0, 1],
            &lp(9),
            &[None; 3],
        );

        assert_eq!(picked, Some(1));
    }

    #[test]
    fn local_swap_does_not_steal_busy_singleton_for_another_style() {
        let picked = alloc_swap_candidate(
            &[Some(lp(9)), Some(lp(1)), Some(lp(2))],
            &[1, 1, 1],
            &lp(9),
            &[None; 3],
        );

        assert_eq!(picked, None);
    }

    #[test]
    fn local_swap_does_not_steal_busy_over_allocated_style() {
        let picked = alloc_swap_candidate(
            &[Some(lp(9)), Some(lp(1)), Some(lp(1))],
            &[0, 1, 1],
            &lp(9),
            &[None; 3],
        );

        assert_eq!(picked, None);
    }

    #[test]
    fn local_pick_prefers_cached_addlayer_source_within_same_tier() {
        let loaded = vec![Some(lp(1)), Some(lp(1))];
        let queue_depths = [0, 0];
        let mut addlayer_source_ids = vec![HashSet::new(), HashSet::new()];
        addlayer_source_ids[1].insert("__biei_addlayer_source_cached".to_string());
        let profile_counts = profile_counts(&loaded);
        let ctx = PickContext {
            loaded: &loaded,
            addlayer_source_ids: &addlayer_source_ids,
            incoming_addlayer_source_id: Some("__biei_addlayer_source_cached"),
            incoming_profile: &lp(1),
            bl_capacity: 2,
            queue_capacity: 2,
            profile_counts: &profile_counts,
            last_used: &[None; 2],
        };

        let picked = (0..loaded.len())
            .min_by_key(|idx| pick_score(&ctx, *idx, queue_depths[*idx]))
            .unwrap();

        assert_eq!(picked, 1);
    }

    #[test]
    fn pool_source_affinity_cache_evicts_oldest_entry() {
        let mut state = PoolState::new(1);
        for i in 0..64 {
            state.mark_addlayer_source(0, format!("source-{i}"));
        }
        state.mark_addlayer_source(0, "source-0".to_string());
        state.mark_addlayer_source(0, "source-64".to_string());

        assert!(state.addlayer_source_ids[0].contains("source-0"));
        assert!(!state.addlayer_source_ids[0].contains("source-1"));
        assert!(state.addlayer_source_ids[0].contains("source-64"));
        assert_eq!(state.addlayer_source_ids[0].len(), 64);
    }

    #[tokio::test]
    async fn tier3_still_respects_hard_queue_capacity() {
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(NoopRenderer)],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });
        pool.workers[0].queue_depth.store(1, Ordering::Release);

        let task = make_task(1, 1);

        let result = pool
            .process(task, None, RouteTier::Tier3DrainSwap, Some(0))
            .await;

        assert!(result.is_err());
        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn renderer_failure_clears_eager_loaded_state() {
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(FailingRenderer)],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        let task = make_task(1, 1);
        let outcome = pool
            .process(task, None, RouteTier::Tier2HrwBl, Some(0))
            .await
            .expect("failure is reported as TaskOutcome::Failed");

        assert!(matches!(
            outcome.result,
            crate::types::TaskResult::Failed { .. }
        ));
        {
            let state = lock_unpoisoned(&pool.state);
            assert_eq!(state.loaded[0], None);
        }
        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn render_failure_preserves_worker_local_warm_state() {
        let setup_count = Arc::new(AtomicUsize::new(0));
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(RenderFailingRenderer {
                setup_count: setup_count.clone(),
            })],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        for _ in 0..2 {
            let outcome = pool
                .process(make_task(1, 1), None, RouteTier::Tier2HrwBl, Some(0))
                .await
                .expect("render failure is reported as TaskOutcome::Failed");
            assert!(matches!(
                outcome.result,
                crate::types::TaskResult::Failed { .. }
            ));
        }

        assert_eq!(setup_count.load(Ordering::Acquire), 1);
        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn credential_change_forces_worker_local_style_setup() {
        let setup_count = Arc::new(AtomicUsize::new(0));
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(RenderFailingRenderer {
                setup_count: setup_count.clone(),
            })],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        for partition in [[1; 32], [1; 32], [2; 32]] {
            let mut task = make_task(1, 1);
            task.authorization = Some(RenderAuthorization {
                readable_namespaces: NamespaceSet::try_new(vec!["carto".to_string()]).unwrap(),
                cache_partition: CredentialCachePartition::from_digest(partition),
                provider_bearer_token: crate::types::ProviderBearerToken::try_new(
                    "public.worker-test".to_string(),
                )
                .unwrap(),
            });
            let outcome = pool
                .process(task, None, RouteTier::Tier2HrwBl, Some(0))
                .await
                .expect("render failure is reported as TaskOutcome::Failed");
            assert!(matches!(outcome.result, TaskResult::Failed { .. }));
        }

        assert_eq!(
            setup_count.load(Ordering::Acquire),
            2,
            "the same credential stays warm; a different credential reloads the style"
        );
        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn expired_task_is_rejected_before_worker_work() {
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(NoopRenderer)],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        let mut task = make_task(1, 1);
        task.deadline = Instant::now();
        let outcome = pool
            .process(task, None, RouteTier::Tier2HrwBl, Some(0))
            .await
            .expect("deadline rejection is an outcome");

        assert!(matches!(
            outcome.result,
            crate::types::TaskResult::Rejected {
                reason: crate::types::RejectionReason::DeadlineExceeded
            }
        ));
        {
            let state = lock_unpoisoned(&pool.state);
            assert_eq!(state.loaded[0], None);
        }
        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn render_finishing_after_deadline_is_failed_as_timeout() {
        let retire_count = Arc::new(AtomicUsize::new(0));
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(SlowRenderer {
                delay: Duration::from_millis(5),
                retire_count: retire_count.clone(),
            })],
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        let mut task = make_task(1, 1);
        task.deadline = Instant::now() + Duration::from_millis(1);
        let outcome = pool
            .process(task, None, RouteTier::Tier2HrwBl, Some(0))
            .await
            .expect("render timeout is an outcome");

        let crate::types::TaskResult::Failed { error, .. } = outcome.result else {
            panic!("expected failed timeout");
        };
        assert_eq!(error, RendererError::Timeout.to_string());
        assert_eq!(retire_count.load(Ordering::Acquire), 1);
        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn render_permits_limit_concurrent_execution_across_warm_slots() {
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let renderers: Vec<BoxRenderer> = (0..2)
            .map(|_| {
                Box::new(CountingRenderer {
                    inflight: inflight.clone(),
                    max_seen: max_seen.clone(),
                    delay: Duration::from_millis(10),
                }) as BoxRenderer
            })
            .collect();
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers,
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        let task_a = make_task(1, 1);
        let task_b = make_task(2, 2);
        let (a, b) = tokio::join!(
            pool.process(task_a, None, RouteTier::Tier2HrwBl, Some(0)),
            pool.process(task_b, None, RouteTier::Tier2HrwBl, Some(1)),
        );

        assert!(a.is_ok());
        assert!(b.is_ok());
        assert_eq!(max_seen.load(Ordering::Acquire), 1);
        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn native_render_permits_limit_render_stage_when_execution_can_overlap() {
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let renderers: Vec<BoxRenderer> = (0..2)
            .map(|_| {
                Box::new(CountingRenderer {
                    inflight: inflight.clone(),
                    max_seen: max_seen.clone(),
                    delay: Duration::from_millis(10),
                }) as BoxRenderer
            })
            .collect();
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers,
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 2,
            native_render_permits: 1,
            source_cache_capacity: 1,
        });

        let task_a = make_task(1, 1);
        let task_b = make_task(2, 2);
        let (a, b) = tokio::join!(
            pool.process(task_a, None, RouteTier::Tier2HrwBl, Some(0)),
            pool.process(task_b, None, RouteTier::Tier2HrwBl, Some(1)),
        );

        assert!(a.is_ok());
        assert!(b.is_ok());
        assert_eq!(max_seen.load(Ordering::Acquire), 1);
        assert_eq!(pool.native_render_permits, 1);
        pool.shutdown(Instant::now() + Duration::from_secs(5)).await;
    }
}
