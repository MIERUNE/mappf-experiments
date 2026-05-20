//! `Node` — request/response entry point composing dispatcher + worker pool +
//! gossip publisher.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::Instrument;

use crate::activity::ProfileActivityTracker;
use crate::config::{CostConfig, GossipConfig, RoutingConfig};
use crate::dispatcher::{Dispatcher, DispatcherSpawn};
use crate::gossip::GossipBus;
use crate::metrics::NodeMetrics;
use crate::render_cache::{RenderOutputCache, cache_hit_outcome};
use crate::renderer::{BoxRenderer, PreparedProfile, ProfilePreparer};
use crate::style_catalog::StyleCatalog;
use crate::transport::{ForwardError, Transport};
use crate::types::{
    ClusterView, Decision, InternalTask, NodeId, ProcessError, RejectionReason, RequestId,
    RouteTier, TaskOutcome, TaskResult, WorkerView,
};
use crate::wire::ForwardRequest;
use crate::worker_pool::{PoolSnapshotter, WorkerPool, WorkerPoolSpawn};

const MIN_FORWARD_BUDGET_MS: u64 = 200;
const MAX_FORWARDING_HOPS: u8 = 1;
const MAX_CLUSTER_VIEW_CACHE_TTL: Duration = Duration::from_millis(10);
const MIN_CLUSTER_VIEW_CACHE_TTL: Duration = Duration::from_millis(1);

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
}

struct ClusterViewCache {
    ttl: Duration,
    cached: Mutex<Option<CachedClusterView>>,
}

struct CachedClusterView {
    expires_at: Instant,
    view: ClusterView,
}

impl ClusterViewCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            cached: Mutex::new(None),
        }
    }

    async fn get_or_load(&self, gossip: &dyn GossipBus) -> ClusterView {
        let now = Instant::now();
        {
            let cached = self.cached.lock().await;
            if let Some(cached_view) = cached.as_ref().filter(|cached| cached.expires_at > now) {
                // TODO: if this clone shows up in profiles, store Arc<ClusterView>
                // and pass a borrow/Arc through the dispatcher boundary.
                return cached_view.view.clone();
            }
        }

        let view = gossip.view().await;
        let mut cached = self.cached.lock().await;
        if let Some(cached_view) = cached
            .as_ref()
            .filter(|cached| cached.expires_at > Instant::now())
        {
            return cached_view.view.clone();
        }
        *cached = Some(CachedClusterView {
            expires_at: Instant::now() + self.ttl,
            view: view.clone(),
        });
        view
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
            tokio::spawn(async move {
                let mut last_sent = crate::types::NodeKvs::new();
                loop {
                    let kvs = snap.snapshot_kvs();
                    for (k, v) in kvs.iter() {
                        if last_sent.get(k) != Some(v) {
                            gossip
                                .set(publisher_node_id.clone(), k.clone(), v.clone())
                                .await;
                        }
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
            }),
        }
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
        let arrived_at = task.arrived_at;
        let task_id = task.id;
        let request_id = task.request_id.clone();
        let had_source = task.source.is_some();

        if tokio::time::Instant::now() >= task.deadline {
            tracing::debug!(
                task_id,
                style_id = %task.style.id.as_str(),
                "rejecting incoming task after deadline"
            );
            return self.record_ingress_outcome(reject(
                task_id,
                request_id,
                arrived_at,
                had_source,
                RejectionReason::DeadlineExceeded,
            ));
        }

        if let Some(outcome) = self.cached_task_outcome(&task) {
            tracing::debug!(
                task_id,
                style_id = %task.style.id.as_str(),
                "serving incoming task from render output cache"
            );
            self.inner.metrics.record_render_output_cache_hit();
            return self.record_ingress_outcome(outcome);
        }
        if self.inner.render_output_cache.is_enabled_for(&task) {
            self.inner.metrics.record_render_output_cache_miss();
        }

        let cache_task = task.clone();
        let view = self
            .inner
            .view_cache
            .get_or_load(self.inner.gossip.as_ref())
            .await;
        let outcome = match self.inner.dispatcher.decide(&task, view) {
            Decision::Local {
                route_tier,
                worker_hint,
            } => {
                tracing::debug!(
                    task_id,
                    style_id = %task.style.id.as_str(),
                    ?route_tier,
                    ?worker_hint,
                    "routing task locally"
                );
                let prepared_profile = match self.prepare_local_profile(&task).await {
                    Ok(prepared) => prepared,
                    Err(err) => {
                        return self.record_ingress_outcome(fail(
                            task_id,
                            request_id,
                            arrived_at,
                            had_source,
                            err.to_string(),
                            crate::types::FailureKind::from_renderer_error(&err),
                        ));
                    }
                };
                match self
                    .inner
                    .pool
                    .process(task, prepared_profile, route_tier, worker_hint)
                    .await
                {
                    Ok(o) => o,
                    Err(err) => {
                        outcome_from_process_error(task_id, request_id, arrived_at, had_source, err)
                    }
                }
            }
            Decision::Forward {
                route_tier,
                candidates,
            } => {
                tracing::debug!(
                    task_id,
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
                    task_id,
                    style_id = %task.style.id.as_str(),
                    ?reason,
                    "dispatcher rejected task"
                );
                reject(task_id, request_id, arrived_at, had_source, reason)
            }
        };
        self.maybe_insert_render_output_cache(&cache_task, &outcome);
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
        } = fwd;
        let now = tokio::time::Instant::now();
        let task_id = wire_task.id;
        let request_id = wire_task.request_id.clone();
        let had_source = wire_task.source.is_some();
        if !self.inner.style_catalog.accepts_revision(&wire_task.style) {
            tracing::debug!(
                task_id,
                style_id = %wire_task.style.id.as_str(),
                version = wire_task.style.version,
                "rejecting forwarded task with unknown style revision"
            );
            return self.record_forwarded_outcome(reject(
                task_id,
                request_id,
                now,
                had_source,
                RejectionReason::UnknownStyle,
            ));
        }
        let task = wire_task.into_internal(now);
        let arrived_at = task.arrived_at;
        if now >= task.deadline {
            tracing::debug!(
                task_id,
                style_id = %task.style.id.as_str(),
                "rejecting forwarded task after deadline"
            );
            return self.record_forwarded_outcome(reject(
                task_id,
                request_id,
                arrived_at,
                had_source,
                RejectionReason::DeadlineExceeded,
            ));
        }
        if let Some(outcome) = self.cached_task_outcome(&task) {
            tracing::debug!(
                task_id,
                style_id = %task.style.id.as_str(),
                "serving forwarded task from render output cache"
            );
            self.inner.metrics.record_render_output_cache_hit();
            return self.record_forwarded_outcome(outcome);
        }
        if self.inner.render_output_cache.is_enabled_for(&task) {
            self.inner.metrics.record_render_output_cache_miss();
        }
        let cache_task = task.clone();
        let prepared_profile = match self.prepare_local_profile(&task).await {
            Ok(prepared) => prepared,
            Err(err) => {
                return self.record_forwarded_outcome(fail(
                    task_id,
                    request_id,
                    arrived_at,
                    had_source,
                    err.to_string(),
                    crate::types::FailureKind::from_renderer_error(&err),
                ));
            }
        };
        let outcome = match self
            .inner
            .pool
            .process(task, prepared_profile, route_tier, drain_worker)
            .await
        {
            Ok(o) => o,
            Err(err) => {
                outcome_from_process_error(task_id, request_id, arrived_at, had_source, err)
            }
        };
        self.maybe_insert_render_output_cache(&cache_task, &outcome);
        self.record_forwarded_outcome(outcome)
    }

    fn cached_task_outcome(&self, task: &InternalTask) -> Option<TaskOutcome> {
        self.inner
            .render_output_cache
            .get(task)
            .map(|output| cache_hit_outcome(self.inner.id.clone(), task, output))
    }

    async fn prepare_local_profile(
        &self,
        task: &InternalTask,
    ) -> Result<Option<PreparedProfile>, crate::types::RendererError> {
        self.inner.profile_preparer.prepare_profile(task).await
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

    fn maybe_insert_render_output_cache(&self, task: &InternalTask, outcome: &TaskOutcome) {
        if self
            .inner
            .render_output_cache
            .insert_from_outcome(task, outcome)
        {
            self.inner.metrics.record_render_output_cache_insert();
        }
    }

    async fn forward_with_failover(
        &self,
        task: InternalTask,
        route_tier: RouteTier,
        candidates: Vec<crate::types::ForwardCandidate>,
    ) -> TaskOutcome {
        let task_id = task.id;
        let request_id = task.request_id.clone();
        let arrived_at = task.arrived_at;
        let had_source = task.source.is_some();
        let fallback_task = task.clone();
        let mut forwarded_task = task;

        if forwarded_task.forwarding_hops >= MAX_FORWARDING_HOPS {
            tracing::debug!(
                task_id,
                hops = forwarded_task.forwarding_hops,
                "rejecting task at forward hop limit"
            );
            return reject(
                task_id,
                request_id,
                arrived_at,
                had_source,
                RejectionReason::HopLimitExceeded,
            );
        }

        if forward_budget_too_small(&forwarded_task) {
            tracing::debug!(task_id, "rejecting task with too little forward budget");
            return reject(
                task_id,
                request_id,
                arrived_at,
                had_source,
                RejectionReason::DeadlineTooClose,
            );
        }

        forwarded_task.forwarding_hops = forwarded_task.forwarding_hops.saturating_add(1);

        let mut last_retryable_rejection: Option<RejectionReason> = None;
        let mut saw_transport_failure = false;

        for candidate in candidates {
            if forward_budget_too_small(&forwarded_task) {
                tracing::debug!(task_id, "rejecting task with too little forward budget");
                return reject(
                    task_id,
                    request_id,
                    arrived_at,
                    had_source,
                    RejectionReason::DeadlineTooClose,
                );
            }

            let target = candidate.node_id;
            let drain_worker = candidate.drain_worker;
            let fwd = ForwardRequest {
                task: forwarded_task
                    .clone()
                    .to_wire_with_hop_latency(tokio::time::Instant::now(), self.inner.hop_latency),
                route_tier,
                drain_worker,
            };

            tracing::debug!(
                task_id,
                target = %target,
                ?route_tier,
                ?drain_worker,
                "sending forwarded task"
            );
            match self.inner.transport.send(target.clone(), fwd).await {
                Ok(resp) => {
                    if let Some(reason) = resp.rejected_reason()
                        && reason.is_retryable_at_forward()
                    {
                        self.inner.metrics.record_forward_retryable();
                        tracing::debug!(
                            task_id,
                            target = %target,
                            ?reason,
                            "peer rejected forwarded task with retryable reason"
                        );
                        last_retryable_rejection = Some(reason);
                        continue;
                    }
                    self.inner.metrics.record_forward_success();
                    return resp.into_task_outcome(arrived_at);
                }
                Err(ForwardError::Retryable(err)) => {
                    self.inner.metrics.record_forward_retryable();
                    tracing::debug!(
                        task_id,
                        target = %target,
                        error = %err,
                        "retryable forward transport failure"
                    );
                    saw_transport_failure = true;
                    continue;
                }
                Err(ForwardError::Fatal(err)) => {
                    self.inner.metrics.record_forward_fatal();
                    tracing::warn!(
                        task_id,
                        target = %target,
                        error = %err,
                        "fatal forward transport failure"
                    );
                    saw_transport_failure = true;
                    continue;
                }
            }
        }

        if let Some(reason) = last_retryable_rejection {
            tracing::debug!(
                task_id,
                ?reason,
                "all forward candidates rejected retryably; trying local overflow fallback"
            );
            let prepared_profile = match self.prepare_local_profile(&fallback_task).await {
                Ok(prepared) => prepared,
                Err(err) => {
                    return fail(
                        task_id,
                        request_id,
                        arrived_at,
                        had_source,
                        err.to_string(),
                        crate::types::FailureKind::from_renderer_error(&err),
                    );
                }
            };
            return match self
                .inner
                .pool
                .process(
                    fallback_task,
                    prepared_profile,
                    RouteTier::Tier4Overflow,
                    None,
                )
                .await
            {
                Ok(outcome) => outcome,
                Err(err) => match err {
                    ProcessError::QueueFull(_) => {
                        reject(task_id, request_id, arrived_at, had_source, reason)
                    }
                    ProcessError::QueueDisconnected => fail(
                        task_id,
                        request_id,
                        arrived_at,
                        had_source,
                        "worker queue disconnected",
                        crate::types::FailureKind::Other,
                    ),
                },
            };
        }

        let reason = if saw_transport_failure {
            RejectionReason::ForwardFailed
        } else {
            RejectionReason::NoCapacity
        };
        tracing::debug!(
            task_id,
            ?reason,
            "rejecting task after forward failover exhausted"
        );
        reject(task_id, request_id, arrived_at, had_source, reason)
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

fn outcome_from_process_error(
    task_id: crate::types::TaskId,
    request_id: RequestId,
    arrived_at: tokio::time::Instant,
    had_source: bool,
    err: ProcessError,
) -> TaskOutcome {
    match err {
        ProcessError::QueueFull(_) => reject(
            task_id,
            request_id,
            arrived_at,
            had_source,
            RejectionReason::QueueFull,
        ),
        ProcessError::QueueDisconnected => fail(
            task_id,
            request_id,
            arrived_at,
            had_source,
            "worker queue disconnected",
            crate::types::FailureKind::Other,
        ),
    }
}

fn outcome(
    task_id: crate::types::TaskId,
    request_id: RequestId,
    arrived_at: tokio::time::Instant,
    had_source: bool,
    result: TaskResult,
) -> TaskOutcome {
    TaskOutcome {
        task_id,
        request_id,
        arrived_at,
        had_source,
        deadline_stage: None,
        result,
    }
}

fn reject(
    task_id: crate::types::TaskId,
    request_id: RequestId,
    arrived_at: tokio::time::Instant,
    had_source: bool,
    reason: RejectionReason,
) -> TaskOutcome {
    outcome(
        task_id,
        request_id,
        arrived_at,
        had_source,
        TaskResult::Rejected { reason },
    )
}

fn fail(
    task_id: crate::types::TaskId,
    request_id: RequestId,
    arrived_at: tokio::time::Instant,
    had_source: bool,
    error: impl Into<String>,
    kind: crate::types::FailureKind,
) -> TaskOutcome {
    outcome(
        task_id,
        request_id,
        arrived_at,
        had_source,
        TaskResult::Failed {
            error: error.into(),
            kind,
        },
    )
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
    use crate::renderer::{BoxRenderer, Renderer};
    use crate::types::{
        ClusterView, ImageFormat, InternalTask, NodeStateView, PixelRatio, RenderOutput,
        RenderRequest, RendererError, Scale, SourceHash, StyleId, StyleRevision, TaskId, WorkerId,
        WorkerView,
    };
    use crate::wire::{ForwardResponse, WireTask};

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

    struct CountingViewGossip {
        calls: Arc<AtomicUsize>,
        view: ClusterView,
    }

    #[async_trait]
    impl GossipBus for CountingViewGossip {
        async fn set(&self, _node_id: NodeId, _key: String, _value: String) {}

        async fn view(&self) -> ClusterView {
            self.calls.fetch_add(1, Ordering::SeqCst);
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
        };
        let cache = ClusterViewCache::new(Duration::from_secs(1));

        let first = cache.get_or_load(&gossip).await;
        let second = cache.get_or_load(&gossip).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.generated_at, second.generated_at);
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

    struct BlockingRenderer {
        render_started: Option<Arc<Notify>>,
        render_continue: Option<Arc<Notify>>,
    }

    struct BlockingSecondPreparer {
        calls: AtomicUsize,
        second_started: Arc<Notify>,
        second_continue: Arc<Notify>,
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

        async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError> {
            self.renders.fetch_add(1, Ordering::SeqCst);
            Ok(RenderOutput {
                bytes: vec![task.request_id.as_str().len() as u8].into(),
                format: task.output_format,
            })
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

        async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError> {
            if let Some(notify) = &self.render_started {
                notify.notify_one();
            }
            if let Some(notify) = &self.render_continue {
                notify.notified().await;
            }
            Ok(RenderOutput {
                bytes: vec![task.id as u8].into(),
                format: task.output_format,
            })
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

    fn node_with_catalog(style_catalog: Arc<StyleCatalog>) -> Node {
        node_with_catalog_and_cache(style_catalog, Vec::new(), Arc::new(NoopGossip), 0)
    }

    fn node_with_catalog_and_cache(
        style_catalog: Arc<StyleCatalog>,
        renderers: Vec<BoxRenderer>,
        gossip: Arc<dyn GossipBus>,
        render_output_cache_capacity_bytes: u64,
    ) -> Node {
        Node::spawn(NodeSpawn {
            id: NodeId::from_index(1),
            renderers,
            profile_preparer: Arc::new(NoopProfilePreparer),
            gossip,
            transport: Arc::new(NoopTransport),
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
                render_cost: CostRange::fixed(Duration::from_millis(1)),
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
                render_cost: CostRange::fixed(Duration::from_millis(1)),
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
            })
            .await;
        let second = node
            .handle_forwarded(ForwardRequest {
                task: internal_task(2, "second").to_wire(Instant::now()),
                route_tier: RouteTier::Tier2HrwBl,
                drain_worker: Some(0),
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
