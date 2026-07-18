//! Production node metrics: a Prometheus `Registry` of counters/histograms
//! (`NodeMetrics`) plus the label helpers used to render them.

use std::time::Duration;

use prometheus::{
    Encoder, HistogramVec, IntCounter, IntCounterVec, Registry, TextEncoder, proto::MetricFamily,
};

use crate::types::{
    DeadlineStage, FailureKind, ImageFormat, RejectionReason, RenderMode, RenderObservation,
    RouteTier, Scale, TaskOutcome, TaskResult,
};
use crate::util::{counter_vec, gauge_vec, histogram_vec_buckets};

type BoxedCollector = Box<dyn prometheus::core::Collector>;

/// Register a fixed metric family set consistently. Metric construction and
/// registration are startup invariants; a duplicate or invalid descriptor is
/// a programming error, not a recoverable runtime condition.
fn register_collectors<const N: usize>(registry: &Registry, collectors: [BoxedCollector; N]) {
    for collector in collectors {
        registry.register(collector).expect("register biei metric");
    }
}

/// One renderer worker's gauge sample. Primitives only, so the dynamic gauge
/// schema can live here in `metrics` without depending on the worker/profile
/// types — the caller (HTTP layer) extracts these from its worker snapshot.
pub struct WorkerGaugeSample {
    pub worker: String,
    pub render_mode: &'static str,
    pub scale: &'static str,
    pub queue_depth: i64,
    pub loaded: bool,
}

/// Runtime gauge inputs sampled at scrape time (not stored in the registry).
pub struct RuntimeGauges {
    pub node_id: String,
    pub workers: Vec<WorkerGaugeSample>,
    /// Live membership size, if this node tracks membership.
    pub membership_live: Option<i64>,
    pub cpu_permits_inuse: i64,
    pub draining: bool,
    pub renderer_total: i64,
    pub renderer_available: i64,
    pub renderer_orphaned: i64,
    pub renderer_health: &'static str,
    pub renderer_replacements_succeeded: u64,
    pub renderer_replacements_exhausted: u64,
    pub renderer_replacements_failed: u64,
}

pub const ROUTE_TIERS: [RouteTier; 5] = [
    RouteTier::RenderCacheHit,
    RouteTier::Tier1WarmTracking,
    RouteTier::Tier2HrwBl,
    RouteTier::Tier3DrainSwap,
    RouteTier::Tier4Overflow,
];

pub const REJECTION_REASONS: [RejectionReason; 8] = [
    RejectionReason::QueueFull,
    RejectionReason::NoCapacity,
    RejectionReason::DrainTooSlow,
    RejectionReason::UnknownStyle,
    RejectionReason::HopLimitExceeded,
    RejectionReason::ForwardFailed,
    RejectionReason::DeadlineTooClose,
    RejectionReason::DeadlineExceeded,
];

pub fn route_tier_label(tier: RouteTier) -> &'static str {
    match tier {
        RouteTier::RenderCacheHit => "render_cache_hit",
        RouteTier::Tier1WarmTracking => "tier1_warm_tracking",
        RouteTier::Tier2HrwBl => "tier2_hrw_bl",
        RouteTier::Tier3DrainSwap => "tier3_drain_swap",
        RouteTier::Tier4Overflow => "tier4_overflow",
    }
}

pub fn rejection_reason_label(reason: RejectionReason) -> &'static str {
    match reason {
        RejectionReason::QueueFull => "queue_full",
        RejectionReason::NoCapacity => "no_capacity",
        RejectionReason::DrainTooSlow => "drain_too_slow",
        RejectionReason::UnknownStyle => "unknown_style",
        RejectionReason::HopLimitExceeded => "hop_limit_exceeded",
        RejectionReason::ForwardFailed => "forward_failed",
        RejectionReason::DeadlineTooClose => "deadline_too_close",
        RejectionReason::DeadlineExceeded => "deadline_exceeded",
    }
}

pub struct NodeMetrics {
    registry: Registry,
    completed: IntCounterVec,
    rejected: IntCounterVec,
    failed: IntCounterVec,
    request_duration: HistogramVec,
    native_render_duration: HistogramVec,
    /// Deprecated metric-name compatibility alias for native render residency.
    cpu_render_duration: HistogramVec,
    render_duration: HistogramVec,
    render_timeout_lower_bound: HistogramVec,
    style_setup_duration: HistogramVec,
    source_setup_duration: HistogramVec,
    profile_prepare_duration: HistogramVec,
    style_swaps: IntCounterVec,
    cold_starts: IntCounterVec,
    source_cache: IntCounterVec,
    render_output_cache: IntCounterVec,
    forwards: IntCounterVec,
    admission_overflow: IntCounterVec,
    deadline_exceeded: IntCounterVec,
    // Would-be renders shed because the renderer cannot start native work
    // (degraded). Recorded exactly at the shed decision — no health re-check —
    // so it is a faithful count even though the rejection itself rides the wire
    // as the generic `NoCapacity` reason.
    render_admission_shed: IntCounter,
}

impl NodeMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let completed = counter_vec(
            "biei_tasks_completed_total",
            "Completed tasks by ingress scope and route tier.",
            &["scope", "route_tier"],
        );
        let rejected = counter_vec(
            "biei_tasks_rejected_total",
            "Rejected tasks by ingress scope and reason.",
            &["scope", "reason"],
        );
        let failed = counter_vec(
            "biei_tasks_failed_total",
            "Failed tasks by ingress scope.",
            &["scope"],
        );
        let request_duration = histogram_vec_buckets(
            "biei_request_duration_seconds",
            "End-to-end task duration from node arrival to completion.",
            LATENCY_BUCKETS,
            &["scope", "route_tier"],
        );
        let native_render_duration = histogram_vec_buckets(
            "biei_native_render_duration_seconds",
            "Native renderStill wall time, including in-render FileSource waits.",
            CPU_RENDER_BUCKETS,
            &["scope"],
        );
        let cpu_render_duration = histogram_vec_buckets(
            "biei_cpu_render_duration_seconds",
            "Deprecated alias of biei_native_render_duration_seconds; includes FileSource waits and is not CPU service time.",
            CPU_RENDER_BUCKETS,
            &["scope"],
        );
        let render_duration = histogram_vec_buckets(
            "biei_render_duration_seconds",
            "Native render and output encoding duration by bounded render shape and worker state.",
            CPU_RENDER_BUCKETS,
            &["scope", "render_mode", "scale", "format", "size", "state"],
        );
        let render_timeout_lower_bound = histogram_vec_buckets(
            "biei_render_timeout_lower_bound_seconds",
            "Elapsed request residency for render timeouts; censored lower-bound evidence, not a successful render distribution.",
            LATENCY_BUCKETS,
            &["scope"],
        );
        let style_setup_duration = histogram_vec_buckets(
            "biei_style_setup_duration_seconds",
            "Worker profile/style setup duration for cold starts and style swaps.",
            LATENCY_BUCKETS,
            &["scope", "render_mode", "scale", "state"],
        );
        let source_setup_duration = histogram_vec_buckets(
            "biei_source_setup_duration_seconds",
            "Worker source setup duration on modeled or addlayer source-cache misses.",
            LATENCY_BUCKETS,
            &["scope", "render_mode", "scale"],
        );
        let profile_prepare_duration = histogram_vec_buckets(
            "biei_profile_prepare_duration_seconds",
            "Pre-worker style and addlayer profile preparation duration.",
            LATENCY_BUCKETS,
            &["outcome"],
        );
        let style_swaps = counter_vec(
            "biei_style_swaps_total",
            "Completed tasks that swapped style/profile.",
            &["scope"],
        );
        let cold_starts = counter_vec(
            "biei_cold_starts_total",
            "Completed tasks that used a cold renderer slot.",
            &["scope"],
        );
        let source_cache = counter_vec(
            "biei_source_cache_total",
            "Addlayer source cache hits and misses.",
            &["outcome"],
        );
        let render_output_cache = counter_vec(
            "biei_render_output_cache_total",
            "Rendered image cache lookups and insertions.",
            &["outcome"],
        );
        let forwards = counter_vec(
            "biei_forwards_total",
            "Internal forward attempts by outcome.",
            &["outcome"],
        );
        let admission_overflow = counter_vec(
            "biei_admission_overflow_total",
            "Completed tasks admitted while the chosen worker was already at or above BL.",
            &["scope"],
        );
        let deadline_exceeded = counter_vec(
            "biei_deadline_exceeded_total",
            "Deadline rejections by worker stage.",
            &["stage"],
        );
        let render_admission_shed = IntCounter::new(
            "biei_render_admission_shed_total",
            "Would-be renders shed because the local renderer cannot start native work.",
        )
        .expect("valid render admission shed counter");

        register_collectors(
            &registry,
            [
                Box::new(completed.clone()) as BoxedCollector,
                Box::new(rejected.clone()),
                Box::new(failed.clone()),
                Box::new(request_duration.clone()),
                Box::new(native_render_duration.clone()),
                Box::new(cpu_render_duration.clone()),
                Box::new(render_duration.clone()),
                Box::new(render_timeout_lower_bound.clone()),
                Box::new(style_setup_duration.clone()),
                Box::new(source_setup_duration.clone()),
                Box::new(profile_prepare_duration.clone()),
                Box::new(style_swaps.clone()),
                Box::new(cold_starts.clone()),
                Box::new(source_cache.clone()),
                Box::new(render_output_cache.clone()),
                Box::new(forwards.clone()),
                Box::new(admission_overflow.clone()),
                Box::new(deadline_exceeded.clone()),
                Box::new(render_admission_shed.clone()),
            ],
        );

        let metrics = Self {
            registry,
            completed,
            rejected,
            failed,
            request_duration,
            native_render_duration,
            cpu_render_duration,
            render_duration,
            render_timeout_lower_bound,
            style_setup_duration,
            source_setup_duration,
            profile_prepare_duration,
            style_swaps,
            cold_starts,
            source_cache,
            render_output_cache,
            forwards,
            admission_overflow,
            deadline_exceeded,
            render_admission_shed,
        };
        metrics.init_zero_series();
        metrics
    }

    /// Count a would-be render shed because the renderer is degraded. Called at
    /// the shed decision so the count never depends on a later health re-check.
    pub fn record_render_admission_shed(&self) {
        self.render_admission_shed.inc();
    }

    pub fn record_ingress(&self, outcome: &TaskOutcome) {
        self.record_outcome("ingress", outcome);
    }

    pub fn record_forwarded(&self, outcome: &TaskOutcome) {
        self.record_outcome("forwarded", outcome);
    }

    pub fn record_forward_success(&self) {
        self.forwards.with_label_values(&["success"]).inc();
    }

    pub fn record_forward_retryable(&self) {
        self.forwards.with_label_values(&["retryable"]).inc();
    }

    pub fn record_forward_fatal(&self) {
        self.forwards.with_label_values(&["fatal"]).inc();
    }

    pub fn record_render_output_cache_hit(&self) {
        self.render_output_cache.with_label_values(&["hit"]).inc();
    }

    pub fn record_render_output_cache_miss(&self) {
        self.render_output_cache.with_label_values(&["miss"]).inc();
    }

    pub fn record_render_output_cache_coalesced(&self) {
        self.render_output_cache
            .with_label_values(&["coalesced"])
            .inc();
    }

    pub fn record_render_output_cache_insert(&self) {
        self.render_output_cache
            .with_label_values(&["insert"])
            .inc();
    }

    pub fn record_profile_prepare(&self, duration: Duration, succeeded: bool) {
        self.profile_prepare_duration
            .with_label_values(&[if succeeded { "success" } else { "failure" }])
            .observe(seconds(duration));
    }

    pub fn gather(&self) -> Vec<MetricFamily> {
        // Node-scoped metrics plus the process-global Rust FileSource metrics
        // (empty until the file source is registered).
        let mut families = self.registry.gather();
        families.extend(crate::renderer::file_source::gather_metrics());
        families
    }

    pub fn render_prometheus(&self) -> String {
        let families = self.gather();
        encode_metric_families(&families)
    }

    /// Renders the stored counters/histograms plus the caller-sampled runtime
    /// gauges (queue depth, loaded workers, membership, CPU permits, drain) into
    /// the Prometheus text exposition format. The gauge schema lives here rather
    /// than in the HTTP adapter.
    pub fn render_prometheus_with_runtime(&self, runtime: &RuntimeGauges) -> String {
        let node = runtime.node_id.as_str();
        let registry = Registry::new();
        let queue_depth = gauge_vec(
            "biei_queue_depth",
            "Current queued tasks per renderer worker.",
            &["node", "worker", "render_mode", "scale"],
        );
        let worker_loaded = gauge_vec(
            "biei_worker_loaded",
            "Whether a renderer worker has a loaded profile.",
            &["node", "worker"],
        );
        let membership_size = gauge_vec(
            "biei_membership_size",
            "Current membership size by state.",
            &["node", "state"],
        );
        let cpu_permits_inuse = gauge_vec(
            "biei_cpu_permits_inuse",
            "Currently held CPU/GPU render-stage permits.",
            &["node"],
        );
        let drain_state = gauge_vec(
            "biei_drain_state",
            "Whether the node is draining.",
            &["node"],
        );
        let renderer_slots = gauge_vec(
            "biei_renderer_slots",
            "Configured and currently available renderer slots.",
            &["node", "state"],
        );
        let renderer_orphaned = gauge_vec(
            "biei_renderer_orphan_threads",
            "Detached native renderer threads that have not returned.",
            &["node"],
        );
        let renderer_health = gauge_vec(
            "biei_renderer_health",
            "Current renderer health state (one-hot).",
            &["node", "state"],
        );
        let renderer_replacements = counter_vec(
            "biei_renderer_replacements_total",
            "Renderer actor replacement attempts by outcome.",
            &["node", "outcome"],
        );

        register_collectors(
            &registry,
            [
                Box::new(queue_depth.clone()) as BoxedCollector,
                Box::new(worker_loaded.clone()),
                Box::new(membership_size.clone()),
                Box::new(cpu_permits_inuse.clone()),
                Box::new(drain_state.clone()),
                Box::new(renderer_slots.clone()),
                Box::new(renderer_orphaned.clone()),
                Box::new(renderer_health.clone()),
                Box::new(renderer_replacements.clone()),
            ],
        );

        for worker in &runtime.workers {
            queue_depth
                .with_label_values(&[node, &worker.worker, worker.render_mode, worker.scale])
                .set(worker.queue_depth);
            worker_loaded
                .with_label_values(&[node, &worker.worker])
                .set(i64::from(worker.loaded));
        }
        if let Some(live) = runtime.membership_live {
            membership_size.with_label_values(&[node, "live"]).set(live);
        }
        cpu_permits_inuse
            .with_label_values(&[node])
            .set(runtime.cpu_permits_inuse);
        drain_state
            .with_label_values(&[node])
            .set(i64::from(runtime.draining));
        renderer_slots
            .with_label_values(&[node, "total"])
            .set(runtime.renderer_total);
        renderer_slots
            .with_label_values(&[node, "available"])
            .set(runtime.renderer_available);
        renderer_orphaned
            .with_label_values(&[node])
            .set(runtime.renderer_orphaned);
        for state in ["full", "external_degraded", "internal_unrecoverable"] {
            renderer_health
                .with_label_values(&[node, state])
                .set(i64::from(state == runtime.renderer_health));
        }
        for (outcome, value) in [
            ("success", runtime.renderer_replacements_succeeded),
            ("exhausted", runtime.renderer_replacements_exhausted),
            ("spawn_failed", runtime.renderer_replacements_failed),
        ] {
            renderer_replacements
                .with_label_values(&[node, outcome])
                .inc_by(value);
        }

        let mut families = self.gather();
        families.extend(registry.gather());
        encode_metric_families(&families)
    }

    fn record_outcome(&self, scope: &'static str, outcome: &TaskOutcome) {
        match &outcome.result {
            TaskResult::Completed { info, .. } => {
                let route_tier = route_tier_label(info.route_tier);
                self.completed.with_label_values(&[scope, route_tier]).inc();
                self.request_duration
                    .with_label_values(&[scope, route_tier])
                    .observe(seconds(
                        info.completed_at.duration_since(outcome.arrived_at),
                    ));
                if info.route_tier != RouteTier::RenderCacheHit {
                    let render_seconds =
                        seconds(info.cpu_completed_at.duration_since(info.cpu_started_at));
                    self.native_render_duration
                        .with_label_values(&[scope])
                        .observe(render_seconds);
                    self.cpu_render_duration
                        .with_label_values(&[scope])
                        .observe(render_seconds);
                    if let Some(observation) = &info.render_observation {
                        let state = render_state_label(info.cold_start, info.style_swap);
                        self.render_duration
                            .with_label_values(&[
                                scope,
                                render_mode_label(observation.render_mode),
                                observation.scale.as_gossip_value(),
                                image_format_label(observation.output_format),
                                render_size_label(observation),
                                state,
                            ])
                            .observe(render_seconds);
                        if let Some(duration) = observation.style_setup_duration {
                            self.style_setup_duration
                                .with_label_values(&[
                                    scope,
                                    render_mode_label(observation.render_mode),
                                    observation.scale.as_gossip_value(),
                                    state,
                                ])
                                .observe(seconds(duration));
                        }
                        if let Some(duration) = observation.source_setup_duration {
                            self.source_setup_duration
                                .with_label_values(&[
                                    scope,
                                    render_mode_label(observation.render_mode),
                                    observation.scale.as_gossip_value(),
                                ])
                                .observe(seconds(duration));
                        }
                    }
                }
                if info.style_swap {
                    self.style_swaps.with_label_values(&[scope]).inc();
                }
                if info.cold_start {
                    self.cold_starts.with_label_values(&[scope]).inc();
                }
                if outcome.had_source {
                    let label = if info.source_loaded { "miss" } else { "hit" };
                    self.source_cache.with_label_values(&[label]).inc();
                }
                if info.admitted_at_overflow {
                    self.admission_overflow.with_label_values(&[scope]).inc();
                }
            }
            TaskResult::Rejected { reason } => {
                self.rejected
                    .with_label_values(&[scope, rejection_reason_label(*reason)])
                    .inc();
                if *reason == RejectionReason::DeadlineExceeded
                    && let Some(stage) = outcome.deadline_stage
                {
                    self.deadline_exceeded
                        .with_label_values(&[deadline_stage_label(stage)])
                        .inc();
                }
            }
            TaskResult::Failed { kind, .. } => {
                self.failed.with_label_values(&[scope]).inc();
                if *kind == FailureKind::RenderTimeout {
                    self.render_timeout_lower_bound
                        .with_label_values(&[scope])
                        .observe(seconds(
                            tokio::time::Instant::now()
                                .saturating_duration_since(outcome.arrived_at),
                        ));
                }
            }
        }
    }

    fn init_zero_series(&self) {
        for scope in ["ingress", "forwarded"] {
            self.failed.with_label_values(&[scope]).inc_by(0);
            self.style_swaps.with_label_values(&[scope]).inc_by(0);
            self.cold_starts.with_label_values(&[scope]).inc_by(0);
            self.admission_overflow
                .with_label_values(&[scope])
                .inc_by(0);
            self.native_render_duration.with_label_values(&[scope]);
            self.cpu_render_duration.with_label_values(&[scope]);
            self.render_timeout_lower_bound.with_label_values(&[scope]);
            for tier in ROUTE_TIERS {
                let tier = route_tier_label(tier);
                self.completed.with_label_values(&[scope, tier]).inc_by(0);
                self.request_duration.with_label_values(&[scope, tier]);
            }
            for reason in REJECTION_REASONS {
                self.rejected
                    .with_label_values(&[scope, rejection_reason_label(reason)])
                    .inc_by(0);
            }
        }
        for outcome in ["hit", "miss"] {
            self.source_cache.with_label_values(&[outcome]).inc_by(0);
        }
        for outcome in ["success", "failure"] {
            self.profile_prepare_duration.with_label_values(&[outcome]);
        }
        for outcome in ["hit", "miss", "coalesced", "insert"] {
            self.render_output_cache
                .with_label_values(&[outcome])
                .inc_by(0);
        }
        for outcome in ["success", "retryable", "fatal"] {
            self.forwards.with_label_values(&[outcome]).inc_by(0);
        }
        for stage in DEADLINE_STAGES {
            self.deadline_exceeded
                .with_label_values(&[deadline_stage_label(stage)])
                .inc_by(0);
        }
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn render_mode_label(mode: RenderMode) -> &'static str {
    mode.as_gossip_value()
}

fn image_format_label(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Png => "png",
        ImageFormat::Webp => "webp",
        ImageFormat::Jpeg => "jpeg",
    }
}

fn render_state_label(cold_start: bool, style_swap: bool) -> &'static str {
    if cold_start {
        "cold"
    } else if style_swap {
        "swap"
    } else {
        "warm"
    }
}

fn render_size_label(observation: &RenderObservation) -> &'static str {
    let scale = match observation.scale {
        Scale::X1 => 1_u32,
        Scale::X2 => 2_u32,
    };
    match u32::from(observation.width.max(observation.height)).saturating_mul(scale) {
        0..=256 => "le_256px",
        257..=512 => "le_512px",
        513..=1_024 => "le_1024px",
        1_025..=2_048 => "le_2048px",
        _ => "gt_2048px",
    }
}

pub const LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0,
];

const CPU_RENDER_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0,
    3.0, 5.0, 10.0,
];

const DEADLINE_STAGES: [DeadlineStage; 5] = [
    DeadlineStage::AcquireRenderPermit,
    DeadlineStage::StyleSwap,
    DeadlineStage::EnsureSource,
    DeadlineStage::AcquireCpuPermit,
    DeadlineStage::Render,
];

pub fn deadline_stage_label(stage: DeadlineStage) -> &'static str {
    match stage {
        DeadlineStage::AcquireRenderPermit => "acquire_render_permit",
        DeadlineStage::StyleSwap => "style_swap",
        DeadlineStage::EnsureSource => "ensure_source",
        DeadlineStage::AcquireCpuPermit => "acquire_cpu_permit",
        DeadlineStage::Render => "render",
    }
}

pub fn encode_metric_families(families: &[MetricFamily]) -> String {
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    if encoder.encode(families, &mut buf).is_err() {
        return String::new();
    }
    String::from_utf8(buf).unwrap_or_default()
}

fn seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        CompletedInfo, ImageFormat, NodeId, RenderMode, RenderObservation, RenderOutput, RequestId,
        Scale, TaskOutcome,
    };
    use tokio::time::Instant;

    fn completed_outcome(route_tier: RouteTier) -> TaskOutcome {
        let now = Instant::now();
        TaskOutcome {
            task_id: 1,
            request_id: RequestId::from_string("metrics-test"),
            arrived_at: now,
            had_source: false,
            deadline_stage: None,
            result: TaskResult::Completed {
                info: CompletedInfo {
                    node_id: NodeId::from("node-a"),
                    worker_id: Some(2),
                    route_tier,
                    started_at: now,
                    cpu_started_at: now + Duration::from_millis(2),
                    cpu_completed_at: now + Duration::from_millis(7),
                    completed_at: now + Duration::from_millis(10),
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
                    render_observation: Some(RenderObservation {
                        render_mode: RenderMode::Static,
                        scale: Scale::X2,
                        output_format: ImageFormat::Webp,
                        width: 512,
                        height: 512,
                        style_setup_duration: None,
                        source_setup_duration: None,
                    }),
                },
                output: RenderOutput {
                    bytes: bytes::Bytes::new(),
                    format: ImageFormat::Png,
                },
            },
        }
    }

    #[test]
    fn node_metrics_tracks_ingress_and_forwarded_scopes_separately() {
        let metrics = NodeMetrics::default();
        metrics.record_ingress(&completed_outcome(RouteTier::Tier2HrwBl));
        metrics.record_forwarded(&TaskOutcome {
            task_id: 2,
            request_id: RequestId::from_string("metrics-test"),
            arrived_at: Instant::now(),
            had_source: false,
            deadline_stage: None,
            result: TaskResult::Rejected {
                reason: RejectionReason::QueueFull,
            },
        });

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains(
            "biei_tasks_completed_total{route_tier=\"tier2_hrw_bl\",scope=\"ingress\"} 1"
        ));
        assert!(
            rendered
                .contains("biei_tasks_rejected_total{reason=\"queue_full\",scope=\"forwarded\"} 1")
        );
        assert!(rendered.contains("biei_request_duration_seconds_bucket"));
        assert!(rendered.contains("biei_cpu_render_duration_seconds_bucket"));
        assert!(rendered.contains("biei_native_render_duration_seconds_bucket"));
    }

    #[test]
    fn detailed_render_histograms_keep_ingress_and_forwarded_scopes_separate() {
        let metrics = NodeMetrics::default();
        let outcome = completed_outcome(RouteTier::Tier2HrwBl);
        metrics.record_ingress(&outcome);
        metrics.record_forwarded(&outcome);

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains(
            "biei_render_duration_seconds_count{format=\"webp\",render_mode=\"static\",scale=\"2x\",scope=\"ingress\",size=\"le_1024px\",state=\"warm\"} 1"
        ));
        assert!(rendered.contains(
            "biei_render_duration_seconds_count{format=\"webp\",render_mode=\"static\",scale=\"2x\",scope=\"forwarded\",size=\"le_1024px\",state=\"warm\"} 1"
        ));
    }

    #[test]
    fn render_timeout_is_exported_as_censored_tail_evidence() {
        let metrics = NodeMetrics::default();
        metrics.record_ingress(&TaskOutcome {
            task_id: 7,
            request_id: RequestId::from_string("timeout-metrics-test"),
            arrived_at: Instant::now() - Duration::from_secs(5),
            had_source: false,
            deadline_stage: None,
            result: TaskResult::Failed {
                kind: FailureKind::RenderTimeout,
                error: "render timeout".to_owned(),
            },
        });

        let rendered = metrics.render_prometheus();
        assert!(
            rendered.contains("biei_render_timeout_lower_bound_seconds_count{scope=\"ingress\"} 1")
        );
    }

    #[test]
    fn node_metrics_records_missing_production_counters() {
        let metrics = NodeMetrics::default();
        let mut outcome = completed_outcome(RouteTier::Tier1WarmTracking);
        outcome.had_source = true;
        if let TaskResult::Completed { info, .. } = &mut outcome.result {
            info.style_swap = true;
            info.cold_start = true;
            info.source_loaded = true;
            info.admitted_at_overflow = true;
            let observation = info
                .render_observation
                .as_mut()
                .expect("render observation");
            observation.style_setup_duration = Some(Duration::from_millis(4));
            observation.source_setup_duration = Some(Duration::from_millis(3));
        }
        metrics.record_ingress(&outcome);
        metrics.record_ingress(&TaskOutcome {
            task_id: 3,
            request_id: RequestId::from_string("deadline-test"),
            arrived_at: Instant::now(),
            had_source: false,
            deadline_stage: Some(DeadlineStage::AcquireCpuPermit),
            result: TaskResult::Rejected {
                reason: RejectionReason::DeadlineExceeded,
            },
        });
        metrics.record_forward_success();
        metrics.record_forward_retryable();
        metrics.record_forward_fatal();
        metrics.record_profile_prepare(Duration::from_millis(2), true);
        metrics.record_profile_prepare(Duration::from_millis(5), false);

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("biei_style_swaps_total{scope=\"ingress\"} 1"));
        assert!(rendered.contains("biei_cold_starts_total{scope=\"ingress\"} 1"));
        assert!(rendered.contains("biei_source_cache_total{outcome=\"miss\"} 1"));
        assert!(rendered.contains("biei_admission_overflow_total{scope=\"ingress\"} 1"));
        assert!(rendered.contains("biei_deadline_exceeded_total{stage=\"acquire_cpu_permit\"} 1"));
        assert!(rendered.contains("biei_forwards_total{outcome=\"success\"} 1"));
        assert!(rendered.contains("biei_forwards_total{outcome=\"retryable\"} 1"));
        assert!(rendered.contains("biei_forwards_total{outcome=\"fatal\"} 1"));
        assert!(rendered.contains(
            "biei_render_duration_seconds_count{format=\"webp\",render_mode=\"static\",scale=\"2x\",scope=\"ingress\",size=\"le_1024px\",state=\"cold\"} 1"
        ));
        assert!(rendered.contains(
            "biei_style_setup_duration_seconds_count{render_mode=\"static\",scale=\"2x\",scope=\"ingress\",state=\"cold\"} 1"
        ));
        assert!(rendered.contains(
            "biei_source_setup_duration_seconds_count{render_mode=\"static\",scale=\"2x\",scope=\"ingress\"} 1"
        ));
        assert!(
            rendered.contains("biei_profile_prepare_duration_seconds_count{outcome=\"success\"} 1")
        );
        assert!(
            rendered.contains("biei_profile_prepare_duration_seconds_count{outcome=\"failure\"} 1")
        );
    }

    #[test]
    fn render_cache_hit_records_request_latency_but_not_cpu_render_latency() {
        let metrics = NodeMetrics::default();
        metrics.record_ingress(&completed_outcome(RouteTier::RenderCacheHit));

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains(
            "biei_request_duration_seconds_count{route_tier=\"render_cache_hit\",scope=\"ingress\"} 1"
        ));
        assert!(rendered.contains("biei_cpu_render_duration_seconds_count{scope=\"ingress\"} 0"));
        assert!(
            rendered.contains("biei_native_render_duration_seconds_count{scope=\"ingress\"} 0")
        );
    }

    #[test]
    fn metric_labels_are_stable_snake_case() {
        assert_eq!(
            route_tier_label(RouteTier::Tier3DrainSwap),
            "tier3_drain_swap"
        );
        assert_eq!(
            rejection_reason_label(RejectionReason::DeadlineTooClose),
            "deadline_too_close"
        );
    }
}
