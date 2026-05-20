//! `WorkerPool` — elastic worker pick + atomic BL reservation + dispatch to
//! `worker_loop` via mpsc.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Semaphore, mpsc, oneshot};
#[cfg(test)]
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::activity::ProfileActivityTracker;
use crate::renderer::{BoxRenderer, PreparedProfile};
use crate::types::{
    InternalTask, NodeId, NodeKvs, ProcessError, RouteTier, TaskOutcome, WorkerId, WorkerProfile,
    WorkerView, encode_worker_kvs,
};
use crate::worker::{WorkerCmd, worker_loop};

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
    queue_depths: &'a [usize],
    addlayer_source_ids: &'a [HashSet<String>],
    incoming_addlayer_source_id: Option<&'a str>,
    incoming_profile: &'a WorkerProfile,
    bl_capacity: usize,
    queue_capacity: usize,
    profile_counts: &'a HashMap<WorkerProfile, usize>,
    activity: &'a ProfileActivityTracker,
}

pub struct WorkerHandle {
    pub tx: mpsc::Sender<WorkerCmd>,
    pub queue_depth: Arc<AtomicUsize>,
    #[cfg(test)]
    join: JoinHandle<()>,
}

/// RAII handle for a hard-limit reservation on a worker. Decrements
/// `queue_depth` on drop so the reservation is always released — including
/// error paths where `send` fails or the worker drops its response channel.
struct QueueSlot<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> QueueSlot<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        Self { counter }
    }
}

impl Drop for QueueSlot<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Pool-side view of "which profile each worker is now (logically) committed
/// to". Updated eagerly at dispatch time, so it reflects the worker's state
/// after its queue drains — not necessarily right now.
pub struct PoolState {
    pub loaded: Vec<Option<WorkerProfile>>,
    addlayer_source_ids: Vec<HashSet<String>>,
    addlayer_source_lru: Vec<VecDeque<String>>,
}

impl PoolState {
    fn new(n: usize) -> Self {
        Self {
            loaded: vec![None; n],
            addlayer_source_ids: vec![HashSet::new(); n],
            addlayer_source_lru: vec![VecDeque::new(); n],
        }
    }

    fn mark_loaded(&mut self, idx: usize, profile: WorkerProfile) {
        if self.loaded[idx].as_ref() != Some(&profile) {
            self.clear_addlayer_sources(idx);
        }
        self.loaded[idx] = Some(profile);
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
        self.loaded[idx] = None;
        self.clear_addlayer_sources(idx);
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
pub struct PoolSnapshotter {
    pub queue_depths: Vec<Arc<AtomicUsize>>,
    pub state: Arc<Mutex<PoolState>>,
}

impl PoolSnapshotter {
    /// Encode current per-slot (loaded_profile, queue) into a flat KV map
    /// suitable for `GossipBus::set` calls.
    pub fn snapshot_kvs(&self) -> NodeKvs {
        let s = self.state.lock().expect("pool state poisoned");
        let mut out = NodeKvs::new();
        for (i, qd) in self.queue_depths.iter().enumerate() {
            let depth = qd.load(Ordering::Relaxed);
            let profile = s.loaded[i].as_ref();
            encode_worker_kvs(&mut out, i as WorkerId, profile, depth);
        }
        out
    }

    pub fn snapshot_workers(&self) -> Vec<WorkerView> {
        let s = self.state.lock().expect("pool state poisoned");
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

pub struct WorkerPool {
    pub workers: Vec<WorkerHandle>,
    pub state: Arc<Mutex<PoolState>>,
    pub activity: Arc<ProfileActivityTracker>,
    /// SLA-oriented soft queue limit per renderer slot (BL).
    pub bl_capacity: usize,
    /// Hard admission/backpressure limit per renderer slot.
    pub queue_capacity: usize,
    /// Node-wide CPU/GPU-heavy render-stage permits. Defaults to
    /// `render_permits` at config resolution time, but may be lower to model
    /// I/O overlap with a fixed render bottleneck.
    pub cpu_render_permits: usize,
    cpu_permit_sem: Arc<Semaphore>,
}

pub struct WorkerPoolSpawn {
    pub node_id: NodeId,
    pub renderers: Vec<BoxRenderer>,
    pub activity: Arc<ProfileActivityTracker>,
    pub bl_capacity: usize,
    pub queue_capacity: usize,
    pub render_permits: usize,
    pub cpu_render_permits: usize,
    pub source_cache_capacity: usize,
}

impl WorkerPool {
    pub fn spawn(spec: WorkerPoolSpawn) -> Self {
        let WorkerPoolSpawn {
            node_id,
            renderers,
            activity,
            bl_capacity,
            queue_capacity,
            render_permits,
            cpu_render_permits,
            source_cache_capacity,
        } = spec;
        let n = renderers.len();
        let render_permits = render_permits.max(1).min(n.max(1));
        let cpu_render_permits = cpu_render_permits.max(1).min(render_permits);
        let permit_sem = Arc::new(Semaphore::new(render_permits));
        let cpu_permit_sem = Arc::new(Semaphore::new(cpu_render_permits));
        let mut workers = Vec::with_capacity(n);
        for (i, renderer) in renderers.into_iter().enumerate() {
            let (tx, rx) = mpsc::channel(1024);
            let queue_depth = Arc::new(AtomicUsize::new(0));
            let id = i as WorkerId;
            let worker_node_id = node_id.clone();
            let worker_permits = permit_sem.clone();
            let worker_cpu_permits = cpu_permit_sem.clone();
            let join = tokio::spawn(async move {
                worker_loop(
                    id,
                    worker_node_id,
                    rx,
                    renderer,
                    worker_permits,
                    worker_cpu_permits,
                    source_cache_capacity,
                )
                .await;
            });
            #[cfg(test)]
            let worker = WorkerHandle {
                tx,
                queue_depth,
                join,
            };
            #[cfg(not(test))]
            let worker = {
                drop(join);
                WorkerHandle { tx, queue_depth }
            };
            workers.push(worker);
        }
        Self {
            workers,
            state: Arc::new(Mutex::new(PoolState::new(n))),
            activity,
            bl_capacity,
            queue_capacity: queue_capacity.max(bl_capacity),
            cpu_render_permits,
            cpu_permit_sem,
        }
    }

    pub fn cpu_permits_inuse(&self) -> usize {
        self.cpu_render_permits
            .saturating_sub(self.cpu_permit_sem.available_permits())
    }

    pub fn snapshotter(&self) -> PoolSnapshotter {
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
    fn pick_local(&self, task: &InternalTask, prepared_profile: Option<&PreparedProfile>) -> usize {
        let s = self.state.lock().expect("pool state poisoned");
        let queue_depths: Vec<usize> = (0..self.workers.len()).map(|i| self.queue_at(i)).collect();
        let task_profile = task.worker_profile();
        let profile_counts = profile_counts(&s.loaded);
        let addlayer_source_id = prepared_profile
            .and_then(|prepared| prepared.addlayer_source.as_ref())
            .map(|source| source.stable_source_id());
        let ctx = PickContext {
            loaded: &s.loaded,
            queue_depths: &queue_depths,
            addlayer_source_ids: &s.addlayer_source_ids,
            incoming_addlayer_source_id: addlayer_source_id.as_deref(),
            incoming_profile: &task_profile,
            bl_capacity: self.bl_capacity,
            queue_capacity: self.queue_capacity,
            profile_counts: &profile_counts,
            activity: &self.activity,
        };

        (0..self.workers.len())
            .min_by_key(|i| pick_score(&ctx, *i))
            .expect("worker pool is empty")
    }

    /// Process a task at one of the local workers. Returns `QueueFull(task)`
    /// if the pool cannot accept without breaching the hard queue limit.
    /// Tier 3 bypasses the BL soft limit but still respects the hard limit.
    pub async fn process(
        &self,
        task: InternalTask,
        prepared_profile: Option<PreparedProfile>,
        route_tier: RouteTier,
        worker_hint: Option<WorkerId>,
    ) -> Result<TaskOutcome, ProcessError> {
        let idx = match worker_hint {
            Some(wid) if (wid as usize) < self.workers.len() => wid as usize,
            _ => self.pick_local(&task, prepared_profile.as_ref()),
        };
        let w = &self.workers[idx];
        // Atomic check-and-reserve so concurrent `process` calls cannot
        // both see the same spare slot and overshoot the hard queue limit.
        let pre_admit_depth = {
            let mut current = w.queue_depth.load(Ordering::Acquire);
            loop {
                if current >= self.queue_capacity {
                    return Err(ProcessError::QueueFull(Box::new(task)));
                }
                match w.queue_depth.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break current,
                    Err(actual) => current = actual,
                }
            }
        };
        let admitted_at_overflow = pre_admit_depth >= self.bl_capacity;
        let task_profile = task.worker_profile();
        let addlayer_source_id = prepared_profile
            .as_ref()
            .and_then(|prepared| prepared.addlayer_source.as_ref())
            .map(|source| source.stable_source_id());
        self.activity.record(task_profile.clone(), Instant::now());
        {
            let mut s = self.state.lock().expect("pool state poisoned");
            s.mark_loaded(idx, task_profile);
            if let Some(source_id) = addlayer_source_id {
                s.mark_addlayer_source(idx, source_id);
            }
        }
        // RAII guard so any exit path releases the hard-limit reservation.
        let _slot = QueueSlot::new(&w.queue_depth);
        let (tx, rx) = oneshot::channel();
        if let Err(err) =
            w.tx.send(WorkerCmd::Process {
                task,
                prepared_profile,
                route_tier,
                admitted_at_overflow,
                respond_to: tx,
            })
            .await
        {
            let WorkerCmd::Process { task, .. } = err.0;
            return Err(ProcessError::QueueFull(Box::new(task)));
        }
        match rx.await {
            Ok(outcome) => {
                if matches!(
                    outcome.result,
                    crate::types::TaskResult::Failed { .. }
                        | crate::types::TaskResult::Rejected { .. }
                ) {
                    let mut s = self.state.lock().expect("pool state poisoned");
                    s.clear_loaded(idx);
                }
                Ok(outcome)
            }
            Err(_) => {
                let mut s = self.state.lock().expect("pool state poisoned");
                s.clear_loaded(idx);
                Err(ProcessError::QueueDisconnected)
            }
        }
    }

    #[cfg(test)]
    async fn shutdown(self) {
        let joins: Vec<_> = self
            .workers
            .into_iter()
            .map(|w| {
                drop(w.tx);
                w.join
            })
            .collect();
        for j in joins {
            let _ = j.await;
        }
    }
}

fn profile_counts(loaded: &[Option<WorkerProfile>]) -> HashMap<WorkerProfile, usize> {
    let mut counts = HashMap::new();
    for profile in loaded.iter().flatten() {
        *counts.entry(profile.clone()).or_insert(0) += 1;
    }
    counts
}

fn pick_score(ctx: &PickContext<'_>, idx: usize) -> PickScore {
    let queue_depth = ctx.queue_depths[idx];
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
                ctx.activity.last_seen(profile),
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

    use crate::renderer::{PreparedProfile, Renderer};
    use crate::types::{
        ImageFormat, InternalTask, PixelRatio, RenderMode, RenderOutput, RenderRequest,
        RendererError, Scale, SourceHash, StyleId, StyleRevision, TaskId,
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

        async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError> {
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            })
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

        async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError> {
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            })
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

        async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError> {
            tokio::time::sleep(self.delay).await;
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            })
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

        async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError> {
            let current = self.inflight.fetch_add(1, Ordering::AcqRel) + 1;
            self.max_seen.fetch_max(current, Ordering::AcqRel);
            tokio::time::sleep(self.delay).await;
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            Ok(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: task.output_format,
            })
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

        async fn render(&mut self, _task: &InternalTask) -> Result<RenderOutput, RendererError> {
            Err(RendererError::RenderFailed(
                "test render failure".to_string(),
            ))
        }
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
        activity: &ProfileActivityTracker,
    ) -> Option<usize> {
        let counts = profile_counts(loaded);
        let addlayer_source_ids = vec![HashSet::new(); loaded.len()];
        let ctx = PickContext {
            loaded,
            queue_depths,
            addlayer_source_ids: &addlayer_source_ids,
            incoming_addlayer_source_id: None,
            incoming_profile,
            bl_capacity: 1,
            queue_capacity: 1,
            profile_counts: &counts,
            activity,
        };
        (0..loaded.len())
            .filter_map(|idx| {
                let score = pick_score(&ctx, idx);
                (score.tier == PickTier::AllocSwapIdle).then_some((idx, score))
            })
            .min_by_key(|(_, score)| *score)
            .map(|(idx, _)| idx)
    }

    #[test]
    fn profile_counts_ignores_unassigned_workers() {
        let counts = profile_counts(&[Some(lp(1)), None, Some(lp(1)), Some(lp(2))]);
        assert_eq!(counts.get(&lp(1)), Some(&2));
        assert_eq!(counts.get(&lp(2)), Some(&1));
        assert_eq!(counts.get(&lp(3)), None);
    }

    #[test]
    fn local_swap_prefers_over_allocated_style_then_oldest_seen() {
        let activity = Arc::new(ProfileActivityTracker::new());
        let now = Instant::now();
        activity.record(lp(1), now);
        activity.record(lp(2), now - Duration::from_secs(20));
        activity.record(lp(3), now - Duration::from_secs(40));

        let picked = alloc_swap_candidate(
            &[Some(lp(1)), Some(lp(2)), Some(lp(1)), Some(lp(3))],
            &[0, 0, 0, 0],
            &lp(9),
            &activity,
        )
        .unwrap();

        assert!(matches!(picked, 0 | 2));
    }

    #[test]
    fn local_swap_prefers_same_renderer_shape_within_over_allocated_profiles() {
        let activity = Arc::new(ProfileActivityTracker::new());
        let now = Instant::now();
        let different_shape = lp_with(1, RenderMode::Static, Scale::X1);
        let same_shape = lp(2);
        activity.record(different_shape.clone(), now - Duration::from_secs(20));
        activity.record(same_shape.clone(), now);

        let picked = alloc_swap_candidate(
            &[
                Some(different_shape.clone()),
                Some(same_shape.clone()),
                Some(different_shape),
                Some(same_shape),
            ],
            &[0, 0, 0, 0],
            &lp(9),
            &activity,
        );

        assert_eq!(picked, Some(1));
    }

    #[test]
    fn local_swap_skips_full_workers_and_incoming_profile() {
        let activity = ProfileActivityTracker::new();

        let picked = alloc_swap_candidate(
            &[Some(lp(9)), Some(lp(1)), Some(lp(1)), Some(lp(2))],
            &[0, 1, 0, 0],
            &lp(9),
            &activity,
        );

        assert_eq!(picked, Some(2));
    }

    #[test]
    fn local_swap_can_steal_idle_singleton_when_no_over_allocated_style_exists() {
        let activity = ProfileActivityTracker::new();

        let picked = alloc_swap_candidate(
            &[Some(lp(9)), Some(lp(1)), Some(lp(2))],
            &[1, 0, 1],
            &lp(9),
            &activity,
        );

        assert_eq!(picked, Some(1));
    }

    #[test]
    fn local_swap_does_not_steal_busy_singleton_for_another_style() {
        let activity = ProfileActivityTracker::new();

        let picked = alloc_swap_candidate(
            &[Some(lp(9)), Some(lp(1)), Some(lp(2))],
            &[1, 1, 1],
            &lp(9),
            &activity,
        );

        assert_eq!(picked, None);
    }

    #[test]
    fn local_swap_does_not_steal_busy_over_allocated_style() {
        let activity = ProfileActivityTracker::new();

        let picked = alloc_swap_candidate(
            &[Some(lp(9)), Some(lp(1)), Some(lp(1))],
            &[0, 1, 1],
            &lp(9),
            &activity,
        );

        assert_eq!(picked, None);
    }

    #[test]
    fn local_pick_prefers_cached_addlayer_source_within_same_tier() {
        let activity = ProfileActivityTracker::new();
        let loaded = vec![Some(lp(1)), Some(lp(1))];
        let queue_depths = vec![0, 0];
        let mut addlayer_source_ids = vec![HashSet::new(), HashSet::new()];
        addlayer_source_ids[1].insert("__biei_addlayer_source_cached".to_string());
        let profile_counts = profile_counts(&loaded);
        let ctx = PickContext {
            loaded: &loaded,
            queue_depths: &queue_depths,
            addlayer_source_ids: &addlayer_source_ids,
            incoming_addlayer_source_id: Some("__biei_addlayer_source_cached"),
            incoming_profile: &lp(1),
            bl_capacity: 2,
            queue_capacity: 2,
            profile_counts: &profile_counts,
            activity: &activity,
        };

        let picked = (0..loaded.len())
            .min_by_key(|idx| pick_score(&ctx, *idx))
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
            activity: Arc::new(ProfileActivityTracker::new()),
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            cpu_render_permits: 1,
            source_cache_capacity: 1,
        });
        pool.workers[0].queue_depth.store(1, Ordering::Release);

        let task = make_task(1, 1);

        let result = pool
            .process(task, None, RouteTier::Tier3DrainSwap, Some(0))
            .await;

        assert!(result.is_err());
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn renderer_failure_clears_eager_loaded_state() {
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(FailingRenderer)],
            activity: Arc::new(ProfileActivityTracker::new()),
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            cpu_render_permits: 1,
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
            let state = pool.state.lock().expect("pool state poisoned");
            assert_eq!(state.loaded[0], None);
        }
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn render_failure_preserves_worker_local_warm_state() {
        let setup_count = Arc::new(AtomicUsize::new(0));
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(RenderFailingRenderer {
                setup_count: setup_count.clone(),
            })],
            activity: Arc::new(ProfileActivityTracker::new()),
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            cpu_render_permits: 1,
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
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn expired_task_is_rejected_before_worker_work() {
        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: NodeId::from_index(0),
            renderers: vec![Box::new(NoopRenderer)],
            activity: Arc::new(ProfileActivityTracker::new()),
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            cpu_render_permits: 1,
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
            let state = pool.state.lock().expect("pool state poisoned");
            assert_eq!(state.loaded[0], None);
        }
        pool.shutdown().await;
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
            activity: Arc::new(ProfileActivityTracker::new()),
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            cpu_render_permits: 1,
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
        pool.shutdown().await;
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
            activity: Arc::new(ProfileActivityTracker::new()),
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            cpu_render_permits: 1,
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
        pool.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn cpu_render_permits_limit_render_stage_when_execution_can_overlap() {
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
            activity: Arc::new(ProfileActivityTracker::new()),
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 2,
            cpu_render_permits: 1,
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
        assert_eq!(pool.cpu_render_permits, 1);
        pool.shutdown().await;
    }
}
