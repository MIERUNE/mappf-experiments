//! `Node` — request/response entry point composing dispatcher + worker pool +
//! gossip publisher.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::Instrument;

use crate::activity::ProfileActivityTracker;
use crate::config::{CostConfig, GossipConfig, RoutingConfig};
use crate::dispatcher::{Dispatcher, DispatcherSpawn};
use crate::gossip::GossipBus;
use crate::metrics::NodeMetrics;
use crate::render_cache::{
    RenderCacheLookup, RenderFlightLeader, RenderOutputCache, cache_hit_outcome,
};
use crate::renderer::{BoxRenderer, PreparedProfile, ProfilePreparer};
use crate::style_catalog::StyleCatalog;
use crate::transport::{ForwardError, Transport};
use crate::types::{
    ClusterView, Decision, InternalTask, NodeId, ProcessError, RENDER_ADMISSION_GOSSIP_KEY,
    RejectionReason, RequestId, RouteTier, TaskOutcome, TaskResult, WorkerId, WorkerView,
};
use crate::util::lock_unpoisoned;
use crate::wire::ForwardRequest;
use crate::worker_pool::{PoolSnapshotter, WorkerPool, WorkerPoolSpawn};

const MIN_FORWARD_BUDGET_MS: u64 = 200;
const MAX_FORWARDING_HOPS: u8 = 1;
const MAX_CLUSTER_VIEW_CACHE_TTL: Duration = Duration::from_millis(100);
const MIN_CLUSTER_VIEW_CACHE_TTL: Duration = Duration::from_millis(1);

#[derive(Clone, Copy)]
enum CacheMissAdmission {
    /// Public ingress may dispatch the miss to a healthy peer even when this
    /// process cannot render it locally.
    MayForward,
    /// A forwarded request has already reached its selected destination; a
    /// miss here would necessarily start local native work.
    RequiresLocalRenderer,
}

impl CacheMissAdmission {
    fn requires_local_renderer(self) -> bool {
        matches!(self, Self::RequiresLocalRenderer)
    }
}

/// Cheap-to-clone handle for a node. Internals hidden behind `Arc<NodeInner>`
/// so transports and entry points can call methods without owning the node.
#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
}

struct NodeInner {
    id: NodeId,
    pool: WorkerPool,
    dispatcher: Dispatcher,
    style_catalog: Arc<StyleCatalog>,
    gossip: Arc<dyn GossipBus>,
    view_cache: ClusterViewCache,
    transport: Arc<dyn Transport>,
    hop_latency: Duration,
    metrics: Arc<NodeMetrics>,
    render_output_cache: RenderOutputCache,
    profile_preparer: Arc<dyn ProfilePreparer>,
    snapshotter: PoolSnapshotter,
    publisher: JoinHandle<()>,
    /// Render-admission probe (production wires it to the supervisor's
    /// `can_start_render()`); unset means always-admit for the sim/tests.
    render_admission: Arc<OnceLock<Arc<dyn Fn() -> bool + Send + Sync>>>,
}

struct ClusterViewCache {
    ttl: Duration,
    state: Mutex<ClusterViewCacheState>,
    changed: watch::Sender<u64>,
}

#[derive(Default)]
struct ClusterViewCacheState {
    cached: Option<CachedClusterView>,
    loading: bool,
}

struct CachedClusterView {
    expires_at: Instant,
    view: Arc<ClusterView>,
}

impl ClusterViewCache {
    fn new(ttl: Duration) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            ttl,
            state: Mutex::new(ClusterViewCacheState::default()),
            changed,
        }
    }

    async fn get_or_load(
        &self,
        gossip: &dyn GossipBus,
        deadline: Instant,
    ) -> Option<Arc<ClusterView>> {
        loop {
            if Instant::now() >= deadline {
                return None;
            }
            // Avoid constructing a watch receiver on the normal fresh-cache
            // path. The second check below closes the completion race before
            // a caller can wait.
            if let Some(view) = {
                let state = lock_unpoisoned(&self.state);
                usable_cached_view(&state, self.ttl)
            } {
                return Some(view);
            }

            let mut changed = self.changed.subscribe();
            let should_load = {
                let mut state = lock_unpoisoned(&self.state);
                if let Some(view) = usable_cached_view(&state, self.ttl) {
                    return Some(view);
                }
                if state.loading {
                    false
                } else {
                    state.loading = true;
                    true
                }
            };

            if should_load {
                let load = ClusterViewLoad::new(self);
                let view = match tokio::time::timeout_at(deadline, gossip.view()).await {
                    Ok(view) => Arc::new(view),
                    Err(_) => {
                        drop(load);
                        return None;
                    }
                };
                load.complete(Arc::clone(&view));
                return Some(view);
            }

            // `watch` remembers changes that happen after subscribe but
            // before this await, avoiding a lost wakeup on the initial load.
            if tokio::time::timeout_at(deadline, changed.changed())
                .await
                .is_err()
            {
                return None;
            }
        }
    }
}

fn usable_cached_view(
    state: &ClusterViewCacheState,
    stale_grace: Duration,
) -> Option<Arc<ClusterView>> {
    let cached = state.cached.as_ref()?;
    // A bounded stale snapshot is preferable to making a request wait behind
    // the single refresh already in progress.
    let now = Instant::now();
    let bounded_stale = state.loading
        && cached
            .expires_at
            .checked_add(stale_grace)
            .is_some_and(|stale_until| stale_until > now);
    (cached.expires_at > now || bounded_stale).then(|| Arc::clone(&cached.view))
}

struct ClusterViewLoad<'a> {
    cache: &'a ClusterViewCache,
    complete: bool,
}

impl<'a> ClusterViewLoad<'a> {
    fn new(cache: &'a ClusterViewCache) -> Self {
        Self {
            cache,
            complete: false,
        }
    }

    fn complete(mut self, view: Arc<ClusterView>) {
        let mut state = lock_unpoisoned(&self.cache.state);
        state.cached = Some(CachedClusterView {
            expires_at: Instant::now() + self.cache.ttl,
            view,
        });
        state.loading = false;
        self.complete = true;
        drop(state);
        self.cache.changed.send_modify(|version| {
            *version = version.wrapping_add(1);
        });
    }
}

impl Drop for ClusterViewLoad<'_> {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        lock_unpoisoned(&self.cache.state).loading = false;
        self.cache.changed.send_modify(|version| {
            *version = version.wrapping_add(1);
        });
    }
}

fn cluster_view_cache_ttl(publish_interval: Duration) -> Duration {
    publish_interval
        .min(MAX_CLUSTER_VIEW_CACHE_TTL)
        .max(MIN_CLUSTER_VIEW_CACHE_TTL)
}

impl Drop for NodeInner {
    fn drop(&mut self) {
        self.publisher.abort();
    }
}

pub struct NodeSpawn {
    pub id: NodeId,
    pub renderers: Vec<BoxRenderer>,
    pub profile_preparer: Arc<dyn ProfilePreparer>,
    pub gossip: Arc<dyn GossipBus>,
    pub transport: Arc<dyn Transport>,
    pub style_catalog: Arc<StyleCatalog>,
    pub activity: Arc<ProfileActivityTracker>,
    pub routing: RoutingConfig,
    pub costs: CostConfig,
    pub gossip_cfg: GossipConfig,
    pub bl_capacity: usize,
    pub queue_capacity: usize,
    pub render_permits: usize,
    pub cpu_render_permits: usize,
    pub source_cache_capacity: usize,
    pub render_output_cache_capacity_bytes: u64,
    pub dispatcher_seed: u64,
}

impl Node {
    pub fn spawn(spec: NodeSpawn) -> Self {
        let NodeSpawn {
            id,
            renderers,
            profile_preparer,
            gossip,
            transport,
            style_catalog,
            activity,
            routing,
            costs,
            gossip_cfg,
            bl_capacity,
            queue_capacity,
            render_permits,
            cpu_render_permits,
            source_cache_capacity,
            render_output_cache_capacity_bytes,
            dispatcher_seed,
        } = spec;
        let hop_latency = costs.hop_latency;

        let pool = WorkerPool::spawn(WorkerPoolSpawn {
            node_id: id.clone(),
            renderers,
            activity: activity.clone(),
            bl_capacity,
            queue_capacity,
            render_permits,
            cpu_render_permits,
            source_cache_capacity,
        });
        let metrics = Arc::new(NodeMetrics::default());
        let render_output_cache = RenderOutputCache::new(render_output_cache_capacity_bytes);
        let snapshotter = pool.snapshotter();
        let render_admission: Arc<OnceLock<Arc<dyn Fn() -> bool + Send + Sync>>> =
            Arc::new(OnceLock::new());
        let dispatcher = Dispatcher::new(DispatcherSpawn {
            node_id: id.clone(),
            config: routing,
            costs,
            bl_capacity,
            queue_capacity,
            activity,
            seed: dispatcher_seed,
        });

        let publisher = {
            let snap = snapshotter.clone();
            let gossip = gossip.clone();
            let interval = gossip_cfg.publish_interval;
            let publisher_node_id = id.clone();
            let render_admission = Arc::clone(&render_admission);
            tokio::spawn(async move {
                let mut last_sent = crate::types::NodeKvs::new();
                loop {
                    let mut kvs = snap.snapshot_kvs();
                    let accepts_new_renders = render_admission.get().is_none_or(|probe| probe());
                    kvs.insert(
                        RENDER_ADMISSION_GOSSIP_KEY.to_string(),
                        accepts_new_renders.to_string(),
                    );
                    let changed: crate::types::NodeKvs = kvs
                        .iter()
                        .filter(|(key, value)| last_sent.get(*key) != Some(*value))
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();
                    if !changed.is_empty() {
                        gossip.set_many(publisher_node_id.clone(), changed).await;
                    }
                    last_sent = kvs;
                    tokio::time::sleep(interval).await;
                }
            })
        };

        Self {
            inner: Arc::new(NodeInner {
                id,
                pool,
                dispatcher,
                style_catalog,
                gossip,
                view_cache: ClusterViewCache::new(cluster_view_cache_ttl(
                    gossip_cfg.publish_interval,
                )),
                transport,
                hop_latency,
                metrics,
                render_output_cache,
                profile_preparer,
                snapshotter,
                publisher,
                render_admission,
            }),
        }
    }

    /// Install the render-admission probe (wired once at startup). Gates
    /// renders, not cache hits: a degraded node still serves hits and joins
    /// in-flight single-flights, shedding only new native work.
    pub fn set_render_admission_probe(&self, probe: Arc<dyn Fn() -> bool + Send + Sync>) {
        let _ = self.inner.render_admission.set(probe);
    }

    fn can_start_render(&self) -> bool {
        self.inner
            .render_admission
            .get()
            .is_none_or(|probe| probe())
    }

    /// Shed a would-be render on a degraded renderer. Uses `NoCapacity` (a
    /// wire-safe, forward-retryable reason); the public ingress relabels it to
    /// `renderer_degraded`. Counts the distinct cause here at the shed moment.
    fn renderer_degraded_reject(&self, task: &InternalTask) -> TaskOutcome {
        self.inner.metrics.record_render_admission_shed();
        TaskMeta::of(task).reject(RejectionReason::NoCapacity)
    }

    pub fn id(&self) -> NodeId {
        self.inner.id.clone()
    }

    pub fn worker_snapshot(&self) -> Vec<WorkerView> {
        self.inner.snapshotter.snapshot_workers()
    }

    pub fn metrics(&self) -> Arc<NodeMetrics> {
        self.inner.metrics.clone()
    }

    pub fn cpu_permits_inuse(&self) -> usize {
        self.inner.pool.cpu_permits_inuse()
    }

    /// Tasks currently executing locally on this node's workers (render permit
    /// held, any stage). Zero means the node has no in-flight *local* work —
    /// the signal the simulator uses to reap a fully-drained node.
    pub fn render_permits_inuse(&self) -> usize {
        self.inner.pool.render_permits_inuse()
    }

    /// Entry point: workload / external client lands here. Dispatcher
    /// decides; we either dispatch locally, forward to another node and
    /// await its outcome, or reject.
    pub async fn handle_incoming(&self, task: InternalTask) -> TaskOutcome {
        let span = tracing::info_span!(
            "handle_incoming",
            request_id = %task.request_id.as_str(),
            task_id = task.id,
            style_id = %task.style.id.as_str()
        );
        self.handle_incoming_inner(task).instrument(span).await
    }

    async fn handle_incoming_inner(&self, task: InternalTask) -> TaskOutcome {
        let meta = TaskMeta::of(&task);

        if tokio::time::Instant::now() >= task.deadline {
            tracing::debug!(
                task_id = meta.task_id,
                style_id = %task.style.id.as_str(),
                "rejecting incoming task after deadline"
            );
            return self.record_ingress_outcome(meta.reject(RejectionReason::DeadlineExceeded));
        }

        let cache_flight = match self
            .acquire_render_output_cache(&task, CacheMissAdmission::MayForward)
            .await
        {
            Ok(flight) => flight,
            Err(outcome) => return self.record_ingress_outcome(outcome),
        };
        let Some(view) = self
            .inner
            .view_cache
            .get_or_load(self.inner.gossip.as_ref(), task.deadline)
            .await
        else {
            return self.record_ingress_outcome(meta.reject(RejectionReason::DeadlineExceeded));
        };
        if Instant::now() >= task.deadline {
            return self.record_ingress_outcome(meta.reject(RejectionReason::DeadlineExceeded));
        }
        let outcome = match self.inner.dispatcher.decide(&task, &view) {
            Decision::Local {
                route_tier,
                worker_hint,
                fallback_candidates,
            } => {
                tracing::debug!(
                    task_id = meta.task_id,
                    style_id = %task.style.id.as_str(),
                    ?route_tier,
                    ?worker_hint,
                    fallback_candidates = fallback_candidates.len(),
                    "routing task locally"
                );
                self.process_local_route(task, route_tier, worker_hint, fallback_candidates)
                    .await
            }
            Decision::Forward {
                route_tier,
                candidates,
            } => {
                tracing::debug!(
                    task_id = meta.task_id,
                    style_id = %task.style.id.as_str(),
                    ?route_tier,
                    candidates = candidates.len(),
                    "forwarding task"
                );
                self.forward_with_failover(task, route_tier, candidates)
                    .await
            }
            Decision::Reject { reason } => {
                tracing::debug!(
                    task_id = meta.task_id,
                    style_id = %task.style.id.as_str(),
                    ?reason,
                    "dispatcher rejected task"
                );
                meta.reject(reason)
            }
        };
        self.maybe_insert_render_output_cache(cache_flight.as_ref(), &outcome);
        self.record_ingress_outcome(outcome)
    }

    /// Transport delivers forwarded tasks here. Bypasses dispatcher's tier
    /// decision; uses the entry dispatcher's carried tier and drain hint.
    pub async fn handle_forwarded(&self, fwd: ForwardRequest) -> TaskOutcome {
        let span = tracing::info_span!(
            "handle_forwarded",
            request_id = %fwd.task.request_id.as_str(),
            task_id = fwd.task.id,
            style_id = %fwd.task.style.id.as_str()
        );
        self.handle_forwarded_inner(fwd).instrument(span).await
    }

    async fn handle_forwarded_inner(&self, fwd: ForwardRequest) -> TaskOutcome {
        let ForwardRequest {
            task: wire_task,
            route_tier,
            drain_worker,
            origin_response_budget_ms: _,
        } = fwd;
        let now = tokio::time::Instant::now();
        // `into_internal(now)` sets `arrived_at = now`, so this meta is valid
        // both before and after the wire conversion.
        let meta = TaskMeta {
            task_id: wire_task.id,
            request_id: wire_task.request_id.clone(),
            arrived_at: now,
            had_source: wire_task.source.is_some() || wire_task.request.has_addlayer_source(),
        };
        if !self.inner.style_catalog.accepts_revision(&wire_task.style) {
            tracing::debug!(
                task_id = meta.task_id,
                style_id = %wire_task.style.id.as_str(),
                version = wire_task.style.version,
                "rejecting forwarded task with unknown style revision"
            );
            return self.record_forwarded_outcome(meta.reject(RejectionReason::UnknownStyle));
        }
        let task = wire_task.into_internal(now);
        if now >= task.deadline {
            tracing::debug!(
                task_id = meta.task_id,
                style_id = %task.style.id.as_str(),
                "rejecting forwarded task after deadline"
            );
            return self.record_forwarded_outcome(meta.reject(RejectionReason::DeadlineExceeded));
        }
        let cache_flight = match self
            .acquire_render_output_cache(&task, CacheMissAdmission::RequiresLocalRenderer)
            .await
        {
            Ok(flight) => flight,
            Err(outcome) => return self.record_forwarded_outcome(outcome),
        };
        let prepared_profile = match self.prepare_local_profile(&task).await {
            Ok(prepared) => prepared,
            Err(err) => {
                return self.record_forwarded_outcome(meta.fail(
                    err.to_string(),
                    crate::types::FailureKind::from_renderer_error(&err),
                ));
            }
        };
        let outcome = match self
            .process_local_task(task, prepared_profile, route_tier, drain_worker)
            .await
        {
            Ok(o) => o,
            Err(err) => meta.process_error_outcome(err),
        };
        self.maybe_insert_render_output_cache(cache_flight.as_ref(), &outcome);
        self.record_forwarded_outcome(outcome)
    }

    async fn acquire_render_output_cache(
        &self,
        task: &InternalTask,
        miss_admission: CacheMissAdmission,
    ) -> Result<Option<RenderFlightLeader>, TaskOutcome> {
        let mut joined_existing_render = false;
        loop {
            match self.inner.render_output_cache.lookup_or_join(task) {
                RenderCacheLookup::Disabled => {
                    // No cache: a forwarded miss must render locally, so health
                    // gates it here (a public miss can still forward to a peer).
                    if miss_admission.requires_local_renderer() && !self.can_start_render() {
                        return Err(self.renderer_degraded_reject(task));
                    }
                    return Ok(None);
                }
                RenderCacheLookup::Hit(output) => {
                    tracing::debug!(
                        task_id = task.id,
                        style_id = %task.style.id.as_str(),
                        "serving task from render output cache"
                    );
                    self.inner.metrics.record_render_output_cache_hit();
                    // Exact hit: served even while degraded — no native work.
                    return Err(cache_hit_outcome(self.inner.id.clone(), task, output));
                }
                RenderCacheLookup::Leader(leader) => {
                    // A forwarded miss must render locally; shed when degraded.
                    // Dropping `leader` frees the flight entry and wakes waiters.
                    // (A public miss keeps leadership to forward to a peer.)
                    if miss_admission.requires_local_renderer() && !self.can_start_render() {
                        drop(leader);
                        return Err(self.renderer_degraded_reject(task));
                    }
                    self.inner.metrics.record_render_output_cache_miss();
                    return Ok(Some(leader));
                }
                RenderCacheLookup::Wait(mut changed) => {
                    if !joined_existing_render {
                        self.inner.metrics.record_render_output_cache_coalesced();
                        joined_existing_render = true;
                    }
                    tokio::select! {
                        result = changed.changed() => {
                            // A leader may complete without a cacheable result.
                            // Re-check both the cache and flight election state.
                            let _ = result;
                        }
                        _ = tokio::time::sleep_until(task.deadline) => {
                            return Err(TaskMeta::of(task).reject(RejectionReason::DeadlineExceeded));
                        }
                    }
                }
            }
        }
    }

    async fn prepare_local_profile(
        &self,
        task: &InternalTask,
    ) -> Result<Option<PreparedProfile>, crate::types::RendererError> {
        let started_at = Instant::now();
        let result = self.inner.profile_preparer.prepare_profile(task).await;
        self.inner
            .metrics
            .record_profile_prepare(started_at.elapsed(), result.is_ok());
        result
    }

    async fn process_local_route(
        &self,
        task: InternalTask,
        route_tier: RouteTier,
        worker_hint: Option<WorkerId>,
        fallback_candidates: Vec<crate::types::ForwardCandidate>,
    ) -> TaskOutcome {
        let meta = TaskMeta::of(&task);

        // The local gossip snapshot can lag health by one publish/view-cache
        // interval. Avoid even profile I/O when the selected local renderer is
        // already closed, and use the dispatcher's remaining peers instead.
        if !self.can_start_render() {
            if fallback_candidates.is_empty() {
                return self.renderer_degraded_reject(&task);
            }
            tracing::debug!(
                task_id = meta.task_id,
                "local renderer degraded before profile preparation; trying remaining HRW candidates"
            );
            return self
                .forward_with_failover(task, route_tier, fallback_candidates)
                .await;
        }

        let prepared_profile = match self.prepare_local_profile(&task).await {
            Ok(prepared) => prepared,
            Err(err) => {
                return meta.fail(
                    err.to_string(),
                    crate::types::FailureKind::from_renderer_error(&err),
                );
            }
        };
        let fallback_task = (!fallback_candidates.is_empty()).then(|| task.clone());
        match self
            .process_local_task(task, prepared_profile, route_tier, worker_hint)
            .await
        {
            Ok(outcome) => outcome,
            Err(ProcessError::RenderAdmissionClosed(task)) if !fallback_candidates.is_empty() => {
                tracing::debug!(
                    task_id = meta.task_id,
                    "local renderer degraded after profile preparation; trying remaining HRW candidates"
                );
                self.forward_with_failover(*task, route_tier, fallback_candidates)
                    .await
            }
            Err(err) if fallback_task.is_some() => {
                tracing::debug!(
                    task_id = meta.task_id,
                    error = ?err,
                    "local admission failed; trying remaining HRW candidates"
                );
                self.forward_with_failover(
                    fallback_task.expect("checked above"),
                    route_tier,
                    fallback_candidates,
                )
                .await
            }
            Err(err) => meta.process_error_outcome(err),
        }
    }

    async fn process_local_task(
        &self,
        task: InternalTask,
        prepared_profile: Option<PreparedProfile>,
        route_tier: RouteTier,
        worker_hint: Option<WorkerId>,
    ) -> Result<TaskOutcome, ProcessError> {
        // Re-check at the last shared boundary before worker/native admission.
        // Health can change while the request is decoded, waits on the cluster
        // view, or performs profile I/O after the output-cache check. Returning
        // the task lets an ingress-selected local route use its existing peer
        // fallback instead of feeding another native render into an outage.
        if !self.can_start_render() {
            return Err(ProcessError::RenderAdmissionClosed(Box::new(task)));
        }
        let revision = task.style.clone();
        let outcome = self
            .inner
            .pool
            .process(task, prepared_profile, route_tier, worker_hint)
            .await?;
        if matches!(
            &outcome.result,
            TaskResult::Failed {
                kind: crate::types::FailureKind::StyleUnavailable,
                ..
            }
        ) {
            self.inner
                .profile_preparer
                .mark_style_load_failed(&revision);
        }
        Ok(outcome)
    }

    /// Confirm a style is actually fetchable at its provider, reusing the
    /// profile preparer's fetch / cache / single-flight / negative-cache path.
    /// The preview endpoint uses this to 404 styles that resolve in the catalog
    /// (e.g. via a URL template, which accepts any id) but don't exist upstream.
    pub async fn ensure_style_available(
        &self,
        revision: &crate::types::StyleRevision,
        deadline: Instant,
    ) -> Result<(), crate::renderer::StyleAvailabilityError> {
        self.inner
            .profile_preparer
            .ensure_style_available(revision, deadline)
            .await
    }

    fn maybe_insert_render_output_cache(
        &self,
        cache_flight: Option<&RenderFlightLeader>,
        outcome: &TaskOutcome,
    ) {
        if cache_flight.is_some_and(|flight| flight.insert_from_outcome(outcome)) {
            self.inner.metrics.record_render_output_cache_insert();
        }
    }

    async fn forward_with_failover(
        &self,
        task: InternalTask,
        route_tier: RouteTier,
        candidates: Vec<crate::types::ForwardCandidate>,
    ) -> TaskOutcome {
        let meta = TaskMeta::of(&task);
        let forwarded_task = task;

        if forwarded_task.forwarding_hops >= MAX_FORWARDING_HOPS {
            tracing::debug!(
                task_id = meta.task_id,
                hops = forwarded_task.forwarding_hops,
                "rejecting task at forward hop limit"
            );
            return meta.reject(RejectionReason::HopLimitExceeded);
        }

        if forward_budget_too_small(&forwarded_task) {
            tracing::debug!(
                task_id = meta.task_id,
                "rejecting task with too little forward budget"
            );
            return meta.reject(RejectionReason::DeadlineTooClose);
        }

        let mut last_retryable_rejection: Option<RejectionReason> = None;
        let mut saw_transport_failure = false;

        for candidate in candidates {
            if forward_budget_too_small(&forwarded_task) {
                tracing::debug!(
                    task_id = meta.task_id,
                    "rejecting task with too little forward budget"
                );
                return meta.reject(RejectionReason::DeadlineTooClose);
            }

            let target = candidate.node_id;
            let drain_worker = candidate.drain_worker;
            let send_started_at = tokio::time::Instant::now();
            let origin_response_budget_ms = forwarded_task
                .deadline
                .saturating_duration_since(send_started_at)
                .as_millis()
                .min(u32::MAX as u128) as u32;
            let fwd = ForwardRequest {
                task: forwarded_task.to_forward_wire(send_started_at, self.inner.hop_latency),
                route_tier,
                drain_worker,
                origin_response_budget_ms,
            };

            tracing::debug!(
                task_id = meta.task_id,
                target = %target,
                ?route_tier,
                ?drain_worker,
                "sending forwarded task"
            );
            let sent = tokio::time::timeout_at(
                forwarded_task.deadline,
                self.inner.transport.send(target.clone(), fwd),
            )
            .await;
            match sent {
                Err(_) => {
                    return meta.reject(RejectionReason::DeadlineExceeded);
                }
                Ok(Ok(resp)) => {
                    if let Some(reason) = resp.rejected_reason()
                        && reason.is_retryable_at_forward()
                    {
                        self.inner.metrics.record_forward_retryable();
                        tracing::debug!(
                            task_id = meta.task_id,
                            target = %target,
                            ?reason,
                            "peer rejected forwarded task with retryable reason"
                        );
                        last_retryable_rejection = Some(reason);
                        continue;
                    }
                    self.inner.metrics.record_forward_success();
                    return resp.into_task_outcome(meta.arrived_at);
                }
                Ok(Err(ForwardError::Retryable(err))) => {
                    self.inner.metrics.record_forward_retryable();
                    tracing::debug!(
                        task_id = meta.task_id,
                        target = %target,
                        error = %err,
                        "retryable forward transport failure"
                    );
                    saw_transport_failure = true;
                    continue;
                }
                Ok(Err(ForwardError::Fatal(err))) => {
                    self.inner.metrics.record_forward_fatal();
                    tracing::warn!(
                        task_id = meta.task_id,
                        target = %target,
                        error = %err,
                        "fatal forward transport failure"
                    );
                    saw_transport_failure = true;
                    continue;
                }
            }
        }

        // Remotes exhausted (retryable rejections or all transport failures):
        // try local overflow, but gate on render admission first (no wasted
        // profile I/O when degraded) and re-check the deadline before rendering.
        let exhaustion_reason = last_retryable_rejection.unwrap_or({
            if saw_transport_failure {
                RejectionReason::ForwardFailed
            } else {
                RejectionReason::NoCapacity
            }
        });

        if self.can_start_render() {
            if tokio::time::Instant::now() >= forwarded_task.deadline {
                return meta.reject(RejectionReason::DeadlineExceeded);
            }
            tracing::debug!(
                task_id = meta.task_id,
                ?exhaustion_reason,
                "forward candidates exhausted; trying local overflow fallback"
            );
            let prepared_profile = match self.prepare_local_profile(&forwarded_task).await {
                Ok(prepared) => prepared,
                Err(err) => {
                    return meta.fail(
                        err.to_string(),
                        crate::types::FailureKind::from_renderer_error(&err),
                    );
                }
            };
            match self
                .process_local_task(
                    forwarded_task,
                    prepared_profile,
                    RouteTier::Tier4Overflow,
                    None,
                )
                .await
            {
                Ok(outcome) => return outcome,
                Err(ProcessError::RenderAdmissionClosed(task)) => {
                    // Renderer degraded at the last boundary: shed as a counted
                    // render-admission shed.
                    return self.renderer_degraded_reject(&task);
                }
                Err(err @ ProcessError::QueueDisconnected) => {
                    return meta.process_error_outcome(err);
                }
                // Local queue is full: fall through to the exhaustion rejection.
                Err(ProcessError::QueueFull(_)) => {}
            }
        }

        tracing::debug!(
            task_id = meta.task_id,
            ?exhaustion_reason,
            "rejecting task after forward failover exhausted"
        );
        meta.reject(exhaustion_reason)
    }

    fn record_ingress_outcome(&self, outcome: TaskOutcome) -> TaskOutcome {
        self.inner.metrics.record_ingress(&outcome);
        outcome
    }

    fn record_forwarded_outcome(&self, outcome: TaskOutcome) -> TaskOutcome {
        self.inner.metrics.record_forwarded(&outcome);
        outcome
    }
}

fn forward_budget_too_small(task: &InternalTask) -> bool {
    task.deadline
        .saturating_duration_since(tokio::time::Instant::now())
        < std::time::Duration::from_millis(MIN_FORWARD_BUDGET_MS)
}

/// The identity fields every terminal outcome carries, captured once per
/// request so reject/fail sites don't repeat the same four arguments.
#[derive(Clone)]
struct TaskMeta {
    task_id: crate::types::TaskId,
    request_id: RequestId,
    arrived_at: tokio::time::Instant,
    had_source: bool,
}

impl TaskMeta {
    fn of(task: &InternalTask) -> Self {
        Self {
            task_id: task.id,
            request_id: task.request_id.clone(),
            arrived_at: task.arrived_at,
            had_source: task.has_source(),
        }
    }

    fn outcome(&self, result: TaskResult) -> TaskOutcome {
        TaskOutcome {
            task_id: self.task_id,
            request_id: self.request_id.clone(),
            arrived_at: self.arrived_at,
            had_source: self.had_source,
            deadline_stage: None,
            result,
        }
    }

    fn reject(&self, reason: RejectionReason) -> TaskOutcome {
        self.outcome(TaskResult::Rejected { reason })
    }

    fn fail(&self, error: impl Into<String>, kind: crate::types::FailureKind) -> TaskOutcome {
        self.outcome(TaskResult::Failed {
            error: error.into(),
            kind,
        })
    }

    fn process_error_outcome(&self, err: ProcessError) -> TaskOutcome {
        match err {
            ProcessError::QueueFull(_) => self.reject(RejectionReason::QueueFull),
            ProcessError::RenderAdmissionClosed(_) => self.reject(RejectionReason::NoCapacity),
            ProcessError::QueueDisconnected => self.fail(
                "worker queue disconnected",
                crate::types::FailureKind::Other,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::renderer::{NoopProfilePreparer, PreparedProfile, ProfilePreparer};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use tokio::sync::Notify;
    use tokio::time::Instant;

    use crate::config::{CostRange, Tier1Strategy};
    use crate::renderer::{BoxRenderer, Renderer, RendererOutput};
    use crate::types::{
        ClusterView, ImageFormat, InternalTask, NodeStateView, PixelRatio, RenderOutput,
        RenderRequest, RendererError, Scale, SourceHash, StyleId, StyleRevision, TaskId, WorkerId,
        WorkerView,
    };
    use crate::wire::{ForwardResponse, OutcomeHeader, OutcomeResult, WireTask};

    struct NoopGossip;

    #[async_trait]
    impl GossipBus for NoopGossip {
        async fn set(&self, _node_id: NodeId, _key: String, _value: String) {}

        async fn view(&self) -> ClusterView {
            ClusterView {
                members: vec![NodeId::from_index(1)],
                states: HashMap::new(),
                generated_at: Instant::now(),
            }
        }
    }

    #[derive(Default)]
    struct CapturingGossip {
        kvs: Mutex<crate::types::NodeKvs>,
        changed: Notify,
    }

    #[async_trait]
    impl GossipBus for CapturingGossip {
        async fn set(&self, _node_id: NodeId, key: String, value: String) {
            lock_unpoisoned(&self.kvs).insert(key, value);
            self.changed.notify_waiters();
        }

        async fn set_many(&self, _node_id: NodeId, kvs: crate::types::NodeKvs) {
            lock_unpoisoned(&self.kvs).extend(kvs);
            self.changed.notify_waiters();
        }

        async fn view(&self) -> ClusterView {
            ClusterView {
                members: Vec::new(),
                states: HashMap::new(),
                generated_at: Instant::now(),
            }
        }
    }

    struct CountingViewGossip {
        calls: Arc<AtomicUsize>,
        view: ClusterView,
        delay: Duration,
    }

    #[async_trait]
    impl GossipBus for CountingViewGossip {
        async fn set(&self, _node_id: NodeId, _key: String, _value: String) {}

        async fn view(&self) -> ClusterView {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.view.clone()
        }
    }

    struct NoopTransport;

    #[async_trait]
    impl Transport for NoopTransport {
        async fn send(
            &self,
            _target: NodeId,
            _fwd: ForwardRequest,
        ) -> Result<ForwardResponse, ForwardError> {
            Err(ForwardError::Fatal("noop transport".to_string()))
        }
    }

    struct CompletingTransport {
        expected_target: NodeId,
        sends: Arc<AtomicUsize>,
    }

    struct HangingTransport {
        started: Arc<Notify>,
    }

    #[async_trait]
    impl Transport for HangingTransport {
        async fn send(
            &self,
            _target: NodeId,
            _fwd: ForwardRequest,
        ) -> Result<ForwardResponse, ForwardError> {
            self.started.notify_one();
            std::future::pending().await
        }
    }

    #[async_trait]
    impl Transport for CompletingTransport {
        async fn send(
            &self,
            target: NodeId,
            fwd: ForwardRequest,
        ) -> Result<ForwardResponse, ForwardError> {
            assert_eq!(target, self.expected_target);
            self.sends.fetch_add(1, Ordering::SeqCst);
            let format = fwd.task.output_format;
            let had_source = fwd.task.source.is_some() || fwd.task.request.has_addlayer_source();
            Ok(ForwardResponse {
                outcome: OutcomeHeader {
                    task_id: fwd.task.id,
                    request_id: fwd.task.request_id,
                    style_id: fwd.task.style.id,
                    had_source,
                    image_format: Some(format),
                    result: OutcomeResult::Completed {
                        node_id: target,
                        worker_id: Some(0),
                        route_tier: fwd.route_tier,
                        render_started_ms: 0,
                        cpu_started_ms: 0,
                        cpu_completed_ms: 0,
                        completed_ms: 0,
                        style_swap: false,
                        cold_start: false,
                        source_loaded: false,
                        admitted_at_overflow: false,
                        render_observation: None,
                    },
                },
                output: Some(RenderOutput {
                    bytes: vec![42].into(),
                    format,
                }),
            })
        }
    }

    #[tokio::test]
    async fn cluster_view_cache_reuses_recent_snapshot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let gossip = CountingViewGossip {
            calls: calls.clone(),
            view: ClusterView {
                members: vec![NodeId::from_index(1)],
                states: HashMap::new(),
                generated_at: Instant::now(),
            },
            delay: Duration::ZERO,
        };
        let cache = ClusterViewCache::new(Duration::from_secs(1));

        let first = cache
            .get_or_load(&gossip, Instant::now() + Duration::from_secs(1))
            .await
            .expect("initial view");
        let second = cache
            .get_or_load(&gossip, Instant::now() + Duration::from_secs(1))
            .await
            .expect("cached view");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.generated_at, second.generated_at);
    }

    #[tokio::test]
    async fn cluster_view_cache_coalesces_concurrent_initial_loads() {
        let calls = Arc::new(AtomicUsize::new(0));
        let gossip = CountingViewGossip {
            calls: Arc::clone(&calls),
            view: ClusterView {
                members: vec![NodeId::from_index(1)],
                states: HashMap::new(),
                generated_at: Instant::now(),
            },
            delay: Duration::from_millis(10),
        };
        let cache = ClusterViewCache::new(Duration::from_secs(1));

        let deadline = Instant::now() + Duration::from_secs(1);
        let (first, second) = tokio::join!(
            cache.get_or_load(&gossip, deadline),
            cache.get_or_load(&gossip, deadline)
        );
        let first = first.expect("initial view leader");
        let second = second.expect("initial view follower");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_view_cache_initial_load_is_bounded_by_request_deadline() {
        let calls = Arc::new(AtomicUsize::new(0));
        let slow = CountingViewGossip {
            calls: Arc::clone(&calls),
            view: ClusterView {
                members: vec![NodeId::from_index(1)],
                states: HashMap::new(),
                generated_at: Instant::now(),
            },
            delay: Duration::from_secs(10),
        };
        let cache = ClusterViewCache::new(Duration::from_secs(1));

        let expired = cache
            .get_or_load(&slow, Instant::now() + Duration::from_millis(50))
            .await;
        assert!(expired.is_none(), "an initial load has no stale fallback");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Timing out the leader must clear `loading`; otherwise every later
        // request would wait forever for a notification that can never arrive.
        let fast = CountingViewGossip {
            calls: Arc::clone(&calls),
            view: ClusterView {
                members: vec![NodeId::from_index(2)],
                states: HashMap::new(),
                generated_at: Instant::now(),
            },
            delay: Duration::ZERO,
        };
        let recovered = cache
            .get_or_load(&fast, Instant::now() + Duration::from_secs(1))
            .await
            .expect("a later request can become the loader");
        assert_eq!(recovered.members, vec![NodeId::from_index(2)]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_view_cache_does_not_serve_unbounded_stale_state_on_timeout() {
        let gossip = CountingViewGossip {
            calls: Arc::new(AtomicUsize::new(0)),
            view: ClusterView {
                members: vec![NodeId::from_index(1)],
                states: HashMap::new(),
                generated_at: Instant::now(),
            },
            delay: Duration::ZERO,
        };
        let cache = ClusterViewCache::new(Duration::from_millis(10));
        cache
            .get_or_load(&gossip, Instant::now() + Duration::from_secs(1))
            .await
            .expect("initial view");

        // One TTL of stale grace is allowed during a refresh/deadline race;
        // beyond that, returning an obsolete membership snapshot is unsafe.
        tokio::time::advance(Duration::from_millis(21)).await;
        assert!(cache.get_or_load(&gossip, Instant::now()).await.is_none());
    }

    #[test]
    fn cluster_view_cache_ttl_tracks_publish_cadence_with_bounds() {
        assert_eq!(
            cluster_view_cache_ttl(Duration::from_millis(50)),
            Duration::from_millis(50)
        );
        assert_eq!(
            cluster_view_cache_ttl(Duration::from_secs(1)),
            MAX_CLUSTER_VIEW_CACHE_TTL
        );
        assert_eq!(
            cluster_view_cache_ttl(Duration::ZERO),
            MIN_CLUSTER_VIEW_CACHE_TTL
        );
    }

    #[tokio::test]
    async fn publisher_gossips_render_admission_state_with_worker_snapshot() {
        let gossip = Arc::new(CapturingGossip::default());
        let node = node_with_catalog_and_cache(registered_catalog(), Vec::new(), gossip.clone(), 0);
        node.set_render_admission_probe(Arc::new(|| false));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let notified = gossip.changed.notified();
                if lock_unpoisoned(&gossip.kvs)
                    .get(RENDER_ADMISSION_GOSSIP_KEY)
                    .is_some_and(|value| value == "false")
                {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("publisher advertises degraded render admission promptly");

        let view = NodeStateView::from_kvs(NodeId::from_index(1), &*lock_unpoisoned(&gossip.kvs));
        assert!(!view.accepts_new_renders);
    }

    struct StaticGossip {
        node_id: NodeId,
    }

    #[async_trait]
    impl GossipBus for StaticGossip {
        async fn set(&self, _node_id: NodeId, _key: String, _value: String) {}

        async fn view(&self) -> ClusterView {
            ClusterView {
                members: vec![self.node_id.clone()],
                states: HashMap::from([(
                    self.node_id.clone(),
                    NodeStateView {
                        id: self.node_id.clone(),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: None,
                            queue_depth: 0,
                        }],
                    },
                )]),
                generated_at: Instant::now(),
            }
        }
    }

    struct CountingRenderer {
        renders: Arc<AtomicUsize>,
    }

    struct StyleRejectingRenderer;

    struct FailureRecordingPreparer {
        failures: Arc<AtomicUsize>,
    }

    struct BlockingRenderer {
        render_started: Option<Arc<Notify>>,
        render_continue: Option<Arc<Notify>>,
    }

    struct BlockingSecondPreparer {
        calls: AtomicUsize,
        second_started: Arc<Notify>,
        second_continue: Arc<Notify>,
    }

    struct BlockingPreparer {
        started: Arc<Notify>,
        continue_prepare: Arc<Notify>,
    }

    #[async_trait]
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
            self.renders.fetch_add(1, Ordering::SeqCst);
            Ok(RenderOutput {
                bytes: vec![task.request_id.as_str().len() as u8].into(),
                format: task.output_format,
            }
            .into())
        }
    }

    #[async_trait]
    impl Renderer for StyleRejectingRenderer {
        async fn setup_profile(
            &mut self,
            task: &InternalTask,
            _prepared: Option<PreparedProfile>,
        ) -> Result<(), RendererError> {
            Err(RendererError::StyleLoadFailed {
                style_id: task.style.id.clone(),
                source: "semantic style validation failed".to_string(),
            })
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, _task: &InternalTask) -> Result<RendererOutput, RendererError> {
            panic!("render must not run after style setup fails")
        }
    }

    #[async_trait]
    impl ProfilePreparer for FailureRecordingPreparer {
        fn mark_style_load_failed(&self, _revision: &StyleRevision) {
            self.failures.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Renderer for BlockingRenderer {
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
            if let Some(notify) = &self.render_started {
                notify.notify_one();
            }
            if let Some(notify) = &self.render_continue {
                notify.notified().await;
            }
            Ok(RenderOutput {
                bytes: vec![task.id as u8].into(),
                format: task.output_format,
            }
            .into())
        }
    }

    #[async_trait]
    impl ProfilePreparer for BlockingSecondPreparer {
        async fn prepare_profile(
            &self,
            _task: &InternalTask,
        ) -> Result<Option<PreparedProfile>, RendererError> {
            let call = self.calls.fetch_add(1, Ordering::AcqRel) + 1;
            if call == 2 {
                self.second_started.notify_one();
                self.second_continue.notified().await;
            }
            Ok(None)
        }
    }

    #[async_trait]
    impl ProfilePreparer for BlockingPreparer {
        async fn prepare_profile(
            &self,
            _task: &InternalTask,
        ) -> Result<Option<PreparedProfile>, RendererError> {
            self.started.notify_one();
            self.continue_prepare.notified().await;
            Ok(None)
        }
    }

    fn node_with_catalog(style_catalog: Arc<StyleCatalog>) -> Node {
        node_with_catalog_and_cache(style_catalog, Vec::new(), Arc::new(NoopGossip), 0)
    }

    fn node_with_catalog_and_cache(
        style_catalog: Arc<StyleCatalog>,
        renderers: Vec<BoxRenderer>,
        gossip: Arc<dyn GossipBus>,
        render_output_cache_capacity_bytes: u64,
    ) -> Node {
        node_with_catalog_cache_and_preparer(
            style_catalog,
            renderers,
            gossip,
            render_output_cache_capacity_bytes,
            Arc::new(NoopProfilePreparer),
        )
    }

    fn node_with_catalog_cache_and_preparer(
        style_catalog: Arc<StyleCatalog>,
        renderers: Vec<BoxRenderer>,
        gossip: Arc<dyn GossipBus>,
        render_output_cache_capacity_bytes: u64,
        profile_preparer: Arc<dyn ProfilePreparer>,
    ) -> Node {
        node_with_catalog_cache_preparer_and_transport(
            style_catalog,
            renderers,
            gossip,
            Arc::new(NoopTransport),
            render_output_cache_capacity_bytes,
            profile_preparer,
        )
    }

    fn node_with_catalog_cache_preparer_and_transport(
        style_catalog: Arc<StyleCatalog>,
        renderers: Vec<BoxRenderer>,
        gossip: Arc<dyn GossipBus>,
        transport: Arc<dyn Transport>,
        render_output_cache_capacity_bytes: u64,
        profile_preparer: Arc<dyn ProfilePreparer>,
    ) -> Node {
        Node::spawn(NodeSpawn {
            id: NodeId::from_index(1),
            renderers,
            profile_preparer,
            gossip,
            transport,
            style_catalog,
            activity: Arc::new(ProfileActivityTracker::new()),
            routing: RoutingConfig {
                tier1_strategy: Tier1Strategy::PowerOfTwo,
                tier3_enabled: false,
                drain_max_queue: 1,
            },
            costs: CostConfig {
                style_setup_cost: CostRange::fixed(Duration::from_millis(1)),
                source_load_cost: CostRange::fixed(Duration::from_millis(1)),
                render_cpu_cost: CostRange::fixed(Duration::from_millis(1)),
                render_resource_cost: CostRange::fixed(Duration::ZERO),
                first_render_resource_cost: CostRange::fixed(Duration::ZERO),
                hop_latency: Duration::ZERO,
                sla: Duration::from_secs(1),
            },
            gossip_cfg: GossipConfig {
                publish_interval: Duration::from_secs(60),
            },
            bl_capacity: 1,
            queue_capacity: 1,
            render_permits: 1,
            cpu_render_permits: 1,
            source_cache_capacity: 1,
            render_output_cache_capacity_bytes,
            dispatcher_seed: 0,
        })
    }

    fn registered_catalog() -> Arc<StyleCatalog> {
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(
            StyleId("cached/style".to_string()),
            crate::style_catalog::StyleDefinition::new("https://styles.test/style.json", 1),
        );
        catalog
    }

    fn internal_task(id: TaskId, request_id: &str) -> InternalTask {
        let now = Instant::now();
        InternalTask {
            id,
            request_id: RequestId::from_string(request_id),
            style: StyleRevision {
                id: StyleId("cached/style".to_string()),
                version: 1,
            },
            source: None,
            request: RenderRequest::Tile {
                z: 0,
                x: 0,
                y: 0,
                tile_size: 512,
            },
            pixel_ratio: PixelRatio::X1,
            output_format: ImageFormat::Png,
            arrived_at: now,
            deadline: now + Duration::from_secs(1),
            forwarding_hops: 0,
        }
    }

    fn forwarded_task(id: TaskId, request_id: &str, worker: WorkerId) -> ForwardRequest {
        ForwardRequest {
            task: internal_task(id, request_id).to_wire(Instant::now()),
            route_tier: RouteTier::Tier2HrwBl,
            drain_worker: Some(worker),
            origin_response_budget_ms: 1_000,
        }
    }

    #[tokio::test]
    async fn profile_preparation_runs_before_worker_queue_admission() {
        let first_render_started = Arc::new(Notify::new());
        let first_render_continue = Arc::new(Notify::new());
        let second_prepare_started = Arc::new(Notify::new());
        let second_prepare_continue = Arc::new(Notify::new());
        let preparer = Arc::new(BlockingSecondPreparer {
            calls: AtomicUsize::new(0),
            second_started: second_prepare_started.clone(),
            second_continue: second_prepare_continue.clone(),
        });
        let catalog = registered_catalog();
        let node = Node::spawn(NodeSpawn {
            id: NodeId::from_index(1),
            renderers: vec![
                Box::new(BlockingRenderer {
                    render_started: Some(first_render_started.clone()),
                    render_continue: Some(first_render_continue.clone()),
                }),
                Box::new(BlockingRenderer {
                    render_started: None,
                    render_continue: None,
                }),
            ],
            profile_preparer: preparer,
            gossip: Arc::new(NoopGossip),
            transport: Arc::new(NoopTransport),
            style_catalog: catalog,
            activity: Arc::new(ProfileActivityTracker::new()),
            routing: RoutingConfig {
                tier1_strategy: Tier1Strategy::PowerOfTwo,
                tier3_enabled: false,
                drain_max_queue: 1,
            },
            costs: CostConfig {
                style_setup_cost: CostRange::fixed(Duration::from_millis(1)),
                source_load_cost: CostRange::fixed(Duration::from_millis(1)),
                render_cpu_cost: CostRange::fixed(Duration::from_millis(1)),
                render_resource_cost: CostRange::fixed(Duration::ZERO),
                first_render_resource_cost: CostRange::fixed(Duration::ZERO),
                hop_latency: Duration::ZERO,
                sla: Duration::from_secs(1),
            },
            gossip_cfg: GossipConfig {
                publish_interval: Duration::from_secs(60),
            },
            bl_capacity: 1,
            queue_capacity: 2,
            render_permits: 1,
            cpu_render_permits: 1,
            source_cache_capacity: 1,
            render_output_cache_capacity_bytes: 0,
            dispatcher_seed: 0,
        });

        let first = tokio::spawn({
            let node = node.clone();
            async move { node.handle_forwarded(forwarded_task(1, "first", 0)).await }
        });
        first_render_started.notified().await;

        let second = tokio::spawn({
            let node = node.clone();
            async move { node.handle_forwarded(forwarded_task(2, "second", 1)).await }
        });
        second_prepare_started.notified().await;

        assert_eq!(
            node.worker_snapshot()[1].queue_depth,
            0,
            "style preparation should not reserve the target worker queue"
        );

        second_prepare_continue.notify_waiters();
        first_render_continue.notify_waiters();

        let first = first.await.expect("first task joins");
        let second = second.await.expect("second task joins");
        assert!(matches!(first.result, TaskResult::Completed { .. }));
        assert!(matches!(second.result, TaskResult::Completed { .. }));
    }

    #[tokio::test]
    async fn handle_incoming_serves_repeated_render_from_output_cache() {
        let renders = Arc::new(AtomicUsize::new(0));
        let node = node_with_catalog_and_cache(
            registered_catalog(),
            vec![Box::new(CountingRenderer {
                renders: renders.clone(),
            })],
            Arc::new(StaticGossip {
                node_id: NodeId::from_index(1),
            }),
            1024 * 1024,
        );

        let first = node.handle_incoming(internal_task(1, "first")).await;
        let second = node.handle_incoming(internal_task(2, "second")).await;

        assert!(matches!(first.result, TaskResult::Completed { .. }));
        assert_eq!(renders.load(Ordering::SeqCst), 1);
        let TaskResult::Completed { info, output } = second.result else {
            panic!("second request should be completed from cache");
        };
        assert_eq!(info.route_tier, RouteTier::RenderCacheHit);
        assert_eq!(output.bytes.as_ref(), &[5]);
        assert_eq!(renders.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn degraded_node_sheds_output_cache_miss_before_rendering() {
        let renders = Arc::new(AtomicUsize::new(0));
        let node = node_with_catalog_and_cache(
            registered_catalog(),
            vec![Box::new(CountingRenderer {
                renders: renders.clone(),
            })],
            Arc::new(StaticGossip {
                node_id: NodeId::from_index(1),
            }),
            1024 * 1024,
        );
        // Renderer cannot start a native render (externally degraded).
        node.set_render_admission_probe(Arc::new(|| false));

        let outcome = node.handle_incoming(internal_task(1, "miss")).await;

        assert!(
            matches!(
                outcome.result,
                TaskResult::Rejected {
                    reason: RejectionReason::NoCapacity
                }
            ),
            "degraded miss is shed as NoCapacity, got {:?}",
            outcome.result
        );
        assert_eq!(
            renders.load(Ordering::SeqCst),
            0,
            "shed happens before any native render, profile prep, or dispatch"
        );
    }

    #[tokio::test]
    async fn degraded_ingress_forwards_cache_miss_to_healthy_peer_and_caches_result() {
        let local_id = NodeId::from_index(1);
        let remote_id = NodeId::from_index(2);
        let renders = Arc::new(AtomicUsize::new(0));
        let sends = Arc::new(AtomicUsize::new(0));
        let target_profile = internal_task(0, "profile").worker_profile();
        let view = ClusterView {
            members: vec![local_id.clone(), remote_id.clone()],
            states: HashMap::from([
                (
                    local_id.clone(),
                    NodeStateView {
                        id: local_id,
                        // Model the bounded gossip-propagation race: the local
                        // worker still looks healthy and warm in this snapshot,
                        // even though the live admission probe below is closed.
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(target_profile),
                            queue_depth: 0,
                        }],
                    },
                ),
                (
                    remote_id.clone(),
                    NodeStateView {
                        id: remote_id.clone(),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: None,
                            queue_depth: 0,
                        }],
                    },
                ),
            ]),
            generated_at: Instant::now(),
        };
        let node = node_with_catalog_cache_preparer_and_transport(
            registered_catalog(),
            vec![Box::new(CountingRenderer {
                renders: renders.clone(),
            })],
            Arc::new(CountingViewGossip {
                calls: Arc::new(AtomicUsize::new(0)),
                view,
                delay: Duration::ZERO,
            }),
            Arc::new(CompletingTransport {
                expected_target: remote_id,
                sends: sends.clone(),
            }),
            1024 * 1024,
            Arc::new(NoopProfilePreparer),
        );
        node.set_render_admission_probe(Arc::new(|| false));

        let first = node
            .handle_incoming(internal_task(1, "forwarded-miss"))
            .await;
        let TaskResult::Completed { output, .. } = first.result else {
            panic!("healthy peer should complete the degraded ingress miss");
        };
        assert_eq!(output.bytes.as_ref(), &[42]);
        assert_eq!(sends.load(Ordering::SeqCst), 1);
        assert_eq!(renders.load(Ordering::SeqCst), 0);

        // The degraded ingress stores the peer response in its own output
        // cache, so the next exact request needs neither peer nor native work.
        let second = node
            .handle_incoming(internal_task(2, "local-cache-hit"))
            .await;
        assert!(matches!(
            second.result,
            TaskResult::Completed {
                info: crate::types::CompletedInfo {
                    route_tier: RouteTier::RenderCacheHit,
                    ..
                },
                ..
            }
        ));
        assert_eq!(sends.load(Ordering::SeqCst), 1);
        assert_eq!(renders.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn node_deadline_bounds_a_transport_that_never_returns() {
        let local_id = NodeId::from_index(1);
        let remote_id = NodeId::from_index(2);
        let target_profile = internal_task(0, "profile").worker_profile();
        let view = ClusterView {
            members: vec![local_id.clone(), remote_id.clone()],
            states: HashMap::from([
                (
                    local_id.clone(),
                    NodeStateView {
                        id: local_id,
                        accepts_new_renders: false,
                        workers: Vec::new(),
                    },
                ),
                (
                    remote_id.clone(),
                    NodeStateView {
                        id: remote_id,
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(target_profile),
                            queue_depth: 0,
                        }],
                    },
                ),
            ]),
            generated_at: Instant::now(),
        };
        let started = Arc::new(Notify::new());
        let node = node_with_catalog_cache_preparer_and_transport(
            registered_catalog(),
            Vec::new(),
            Arc::new(CountingViewGossip {
                calls: Arc::new(AtomicUsize::new(0)),
                view,
                delay: Duration::ZERO,
            }),
            Arc::new(HangingTransport {
                started: started.clone(),
            }),
            0,
            Arc::new(NoopProfilePreparer),
        );

        let request = tokio::spawn({
            let node = node.clone();
            async move {
                node.handle_incoming(internal_task(1, "hanging-forward"))
                    .await
            }
        });
        started.notified().await;
        tokio::time::advance(Duration::from_secs(1)).await;

        let outcome = request
            .await
            .expect("forward request joins at its deadline");
        assert!(matches!(
            outcome.result,
            TaskResult::Rejected {
                reason: RejectionReason::DeadlineExceeded,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn health_change_during_profile_io_is_rechecked_before_native_admission() {
        let renders = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let prepare_started = Arc::new(Notify::new());
        let continue_prepare = Arc::new(Notify::new());
        let node = node_with_catalog_cache_and_preparer(
            registered_catalog(),
            vec![Box::new(CountingRenderer {
                renders: renders.clone(),
            })],
            Arc::new(StaticGossip {
                node_id: NodeId::from_index(1),
            }),
            1024 * 1024,
            Arc::new(BlockingPreparer {
                started: prepare_started.clone(),
                continue_prepare: continue_prepare.clone(),
            }),
        );
        let probe = ready.clone();
        node.set_render_admission_probe(Arc::new(move || probe.load(Ordering::SeqCst)));

        let request = tokio::spawn({
            let node = node.clone();
            async move {
                node.handle_forwarded(forwarded_task(1, "health-race", 0))
                    .await
            }
        });
        prepare_started.notified().await;

        // The output-cache leader was acquired while healthy, but provider
        // evidence/slot loss arrives during profile I/O. The final common
        // admission check must observe the new state.
        ready.store(false, Ordering::SeqCst);
        continue_prepare.notify_one();

        let outcome = request.await.expect("forward task joins");
        assert!(matches!(
            outcome.result,
            TaskResult::Rejected {
                reason: RejectionReason::NoCapacity
            }
        ));
        assert_eq!(
            renders.load(Ordering::SeqCst),
            0,
            "no native render starts after health closes during profile I/O"
        );
    }

    #[tokio::test]
    async fn degraded_node_still_serves_output_cache_hit() {
        let renders = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let node = node_with_catalog_and_cache(
            registered_catalog(),
            vec![Box::new(CountingRenderer {
                renders: renders.clone(),
            })],
            Arc::new(StaticGossip {
                node_id: NodeId::from_index(1),
            }),
            1024 * 1024,
        );
        let probe = ready.clone();
        node.set_render_admission_probe(Arc::new(move || probe.load(Ordering::SeqCst)));

        // Warm the cache while the renderer is healthy.
        let first = node.handle_incoming(internal_task(1, "warm")).await;
        assert!(matches!(first.result, TaskResult::Completed { .. }));
        assert_eq!(renders.load(Ordering::SeqCst), 1);

        // The renderer now degrades: an exact hit is still served, with no new
        // native render (cache reachability is preserved behind the gate).
        ready.store(false, Ordering::SeqCst);
        let hit = node
            .handle_incoming(internal_task(2, "hit-while-degraded"))
            .await;
        let TaskResult::Completed { info, output } = hit.result else {
            panic!("degraded node should still serve the cache hit");
        };
        assert_eq!(info.route_tier, RouteTier::RenderCacheHit);
        // The cached bytes are the warm render's output (`CountingRenderer`
        // emits the request-id length), proving the hit is served from cache
        // rather than re-rendered for the degraded request.
        assert_eq!(output.bytes.as_ref(), &["warm".len() as u8]);
        assert_eq!(
            renders.load(Ordering::SeqCst),
            1,
            "no new render is started while degraded"
        );
    }

    #[tokio::test]
    async fn degraded_shed_releases_flight_so_render_recovers_when_ready() {
        let renders = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let node = node_with_catalog_and_cache(
            registered_catalog(),
            vec![Box::new(CountingRenderer {
                renders: renders.clone(),
            })],
            Arc::new(StaticGossip {
                node_id: NodeId::from_index(1),
            }),
            1024 * 1024,
        );
        let probe = ready.clone();
        node.set_render_admission_probe(Arc::new(move || probe.load(Ordering::SeqCst)));

        // Degraded: the miss is shed and its temporary single-flight leader is
        // released on drop.
        let shed = node.handle_incoming(internal_task(1, "shed")).await;
        assert!(matches!(
            shed.result,
            TaskResult::Rejected {
                reason: RejectionReason::NoCapacity
            }
        ));
        assert_eq!(renders.load(Ordering::SeqCst), 0);

        // Recovered: the same cache key renders. A leaked flight entry would
        // strand this request on the follower-wait path forever, so a clean
        // render proves the shed released the flight.
        ready.store(true, Ordering::SeqCst);
        let rendered = node.handle_incoming(internal_task(2, "recovered")).await;
        assert!(
            matches!(rendered.result, TaskResult::Completed { .. }),
            "render recovers once the renderer can start renders again, got {:?}",
            rendered.result
        );
        assert_eq!(renders.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn provider_outage_preserves_cache_sheds_miss_and_recovers_without_restart() {
        let renders = Arc::new(AtomicUsize::new(0));
        let provider = crate::renderer::file_source::ProviderHealthTracker::new();
        let supervisor = crate::renderer::actor::RendererActorSupervisor::with_provider_health(
            1,
            provider.clone(),
        );
        let node = node_with_catalog_and_cache(
            registered_catalog(),
            vec![Box::new(CountingRenderer {
                renders: renders.clone(),
            })],
            Arc::new(StaticGossip {
                node_id: NodeId::from_index(1),
            }),
            1024 * 1024,
        );
        let health = supervisor.clone();
        node.set_render_admission_probe(Arc::new(move || health.can_start_render()));

        // Establish the warm output that must survive the upstream outage.
        let warm = node.handle_incoming(internal_task(1, "warm-cache")).await;
        assert!(matches!(warm.result, TaskResult::Completed { .. }));
        assert_eq!(renders.load(Ordering::SeqCst), 1);

        // A regular FileSource retry correlates the unavailable native slot
        // with an external provider failure. The pod stays ready/live so the
        // process and its in-memory cache are not discarded, but it stops
        // starting fresh native work.
        let retry = provider.begin_retry();
        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);
        assert_eq!(
            supervisor.health(),
            crate::renderer::actor::RendererHealth::ExternalDegraded
        );
        assert!(supervisor.is_ready());
        assert!(supervisor.is_livable());
        assert!(!supervisor.can_start_render());

        let cached = node
            .handle_incoming(internal_task(2, "hit-during-outage"))
            .await;
        assert!(matches!(
            cached.result,
            TaskResult::Completed {
                info: crate::types::CompletedInfo {
                    route_tier: RouteTier::RenderCacheHit,
                    ..
                },
                ..
            }
        ));

        let mut cold = internal_task(3, "miss-during-outage");
        let RenderRequest::Tile { x, .. } = &mut cold.request else {
            panic!("test task is a tile request");
        };
        *x = 1;
        let shed = node.handle_incoming(cold.clone()).await;
        assert!(matches!(
            shed.result,
            TaskResult::Rejected {
                reason: RejectionReason::NoCapacity
            }
        ));
        assert_eq!(
            renders.load(Ordering::SeqCst),
            1,
            "the outage miss must not feed another native render"
        );

        // The retry evidence ends before autonomous actor repair completes;
        // health briefly becomes internally unrecoverable. Restoring the slot
        // models the repair tick proven separately by the real-actor test, and
        // the previously shed flight must then render without a process restart.
        drop(retry);
        assert_eq!(
            supervisor.health(),
            crate::renderer::actor::RendererHealth::InternalUnrecoverable
        );
        supervisor.set_slot_available(&mut slot_available, true);
        assert_eq!(
            supervisor.health(),
            crate::renderer::actor::RendererHealth::Full
        );

        cold.id = 4;
        cold.request_id = RequestId::from_string("recovered-miss");
        cold.arrived_at = Instant::now();
        cold.deadline = cold.arrived_at + Duration::from_secs(1);
        let recovered = node.handle_incoming(cold).await;
        assert!(matches!(recovered.result, TaskResult::Completed { .. }));
        assert_eq!(renders.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn handle_forwarded_serves_repeated_render_from_output_cache() {
        let renders = Arc::new(AtomicUsize::new(0));
        let node = node_with_catalog_and_cache(
            registered_catalog(),
            vec![Box::new(CountingRenderer {
                renders: renders.clone(),
            })],
            Arc::new(NoopGossip),
            1024 * 1024,
        );

        let first = node
            .handle_forwarded(ForwardRequest {
                task: internal_task(1, "first").to_wire(Instant::now()),
                route_tier: RouteTier::Tier2HrwBl,
                drain_worker: Some(0),
                origin_response_budget_ms: 0,
            })
            .await;
        let second = node
            .handle_forwarded(ForwardRequest {
                task: internal_task(2, "second").to_wire(Instant::now()),
                route_tier: RouteTier::Tier2HrwBl,
                drain_worker: Some(0),
                origin_response_budget_ms: 0,
            })
            .await;

        assert!(matches!(first.result, TaskResult::Completed { .. }));
        assert_eq!(renders.load(Ordering::SeqCst), 1);
        let TaskResult::Completed { info, output } = second.result else {
            panic!("second forwarded request should be completed from cache");
        };
        assert_eq!(info.route_tier, RouteTier::RenderCacheHit);
        assert_eq!(output.bytes.as_ref(), &[5]);
        assert_eq!(renders.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_ingress_and_forwarded_requests_share_one_render() {
        let render_started = Arc::new(Notify::new());
        let render_continue = Arc::new(Notify::new());
        let node = node_with_catalog_and_cache(
            registered_catalog(),
            vec![Box::new(BlockingRenderer {
                render_started: Some(render_started.clone()),
                render_continue: Some(render_continue.clone()),
            })],
            Arc::new(StaticGossip {
                node_id: NodeId::from_index(1),
            }),
            1024 * 1024,
        );

        let ingress = tokio::spawn({
            let node = node.clone();
            async move { node.handle_incoming(internal_task(1, "ingress")).await }
        });
        render_started.notified().await;

        let forwarded = tokio::spawn({
            let node = node.clone();
            async move {
                node.handle_forwarded(forwarded_task(2, "forwarded", 0))
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !forwarded.is_finished(),
            "forwarded duplicate should wait for the active render"
        );

        render_continue.notify_waiters();
        let ingress = tokio::time::timeout(Duration::from_secs(1), ingress)
            .await
            .expect("ingress render should complete")
            .expect("ingress task should join");
        let forwarded = tokio::time::timeout(Duration::from_secs(1), forwarded)
            .await
            .expect("forwarded follower should complete")
            .expect("forwarded task should join");

        assert!(matches!(ingress.result, TaskResult::Completed { .. }));
        let TaskResult::Completed { info, output } = forwarded.result else {
            panic!("forwarded duplicate should complete from cache");
        };
        assert_eq!(info.route_tier, RouteTier::RenderCacheHit);
        assert_eq!(output.bytes.as_ref(), &[1]);
    }

    #[tokio::test]
    async fn local_style_load_failure_is_reported_to_profile_preparer() {
        let failures = Arc::new(AtomicUsize::new(0));
        let node = node_with_catalog_cache_and_preparer(
            registered_catalog(),
            vec![Box::new(StyleRejectingRenderer)],
            Arc::new(NoopGossip),
            0,
            Arc::new(FailureRecordingPreparer {
                failures: failures.clone(),
            }),
        );

        let outcome = node
            .handle_forwarded(ForwardRequest {
                task: internal_task(1, "style-rejected").to_wire(Instant::now()),
                route_tier: RouteTier::Tier2HrwBl,
                drain_worker: Some(0),
                origin_response_budget_ms: 0,
            })
            .await;

        assert!(matches!(
            outcome.result,
            TaskResult::Failed {
                kind: crate::types::FailureKind::StyleUnavailable,
                ..
            }
        ));
        assert_eq!(failures.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn handle_forwarded_unknown_style_uses_unknown_style_rejection() {
        let node = node_with_catalog(Arc::new(StyleCatalog::new()));

        let outcome = node
            .handle_forwarded(ForwardRequest {
                task: WireTask {
                    id: 42,
                    request_id: crate::types::RequestId::from_string("node-test"),
                    style: StyleRevision {
                        id: StyleId("missing/style".to_string()),
                        version: 1,
                    },
                    source: None,
                    request: RenderRequest::Tile {
                        z: 0,
                        x: 0,
                        y: 0,
                        tile_size: 512,
                    },
                    scale: Scale::X2,
                    output_format: ImageFormat::Png,
                    remaining_budget_ms: 1_000,
                    forwarding_hops: 0,
                },
                route_tier: RouteTier::Tier2HrwBl,
                drain_worker: None,
                origin_response_budget_ms: 0,
            })
            .await;

        assert!(matches!(
            outcome.result,
            TaskResult::Rejected {
                reason: RejectionReason::UnknownStyle
            }
        ));
    }
}
