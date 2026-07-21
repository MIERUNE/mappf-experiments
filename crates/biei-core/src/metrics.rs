//! Production node metrics: a Prometheus `Registry` of counters/histograms
//! (`NodeMetrics`) plus the label helpers used to render them.

use std::sync::OnceLock;
use std::time::Duration;

pub use mmpf_common::metrics::encode_metric_families;
use mmpf_common::metrics::{counter_vec, histogram_vec_buckets, register_collectors};
use prometheus::{HistogramVec, IntCounter, IntCounterVec, Registry, proto::MetricFamily};

use crate::types::{
    DeadlineStage, FailureKind, ImageFormat, RejectionReason, RenderMode, RenderObservation,
    RouteTier, Scale, TaskOutcome, TaskResult,
};

const ROUTE_TIERS: [RouteTier; 5] = [
    RouteTier::RenderCacheHit,
    RouteTier::Tier1WarmTracking,
    RouteTier::Tier2HrwBl,
    RouteTier::Tier3DrainSwap,
    RouteTier::Tier4Overflow,
];

const REJECTION_REASONS: [RejectionReason; 9] = [
    RejectionReason::QueueFull,
    RejectionReason::NoCapacity,
    RejectionReason::RendererDegraded,
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
        RejectionReason::RendererDegraded => "renderer_degraded",
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
    failed_by_kind: IntCounterVec,
    request_duration: HistogramVec,
    native_render_duration: HistogramVec,
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
    // so it is a faithful count of the typed `RendererDegraded` rejection.
    render_admission_shed: IntCounter,
    // Optional process-global metrics folded into every scrape (e.g. the Rust
    // FileSource families). Installed by the composition root so `biei-core`
    // stays independent of any MapLibre-backed collector; left unset in
    // embedders (e.g. the simulator) that have no such source.
    extra_metrics: OnceLock<Box<dyn Fn() -> Vec<MetricFamily> + Send + Sync>>,
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
        let failed_by_kind = counter_vec(
            "biei_tasks_failed_by_kind_total",
            "Failed tasks by ingress scope and bounded failure kind.",
            &["scope", "kind"],
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
            NATIVE_RENDER_BUCKETS,
            &["scope"],
        );
        let render_duration = histogram_vec_buckets(
            "biei_render_duration_seconds",
            "Native render and output encoding duration by bounded render shape and worker state.",
            NATIVE_RENDER_BUCKETS,
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
                Box::new(completed.clone()) as Box<dyn prometheus::core::Collector>,
                Box::new(rejected.clone()),
                Box::new(failed.clone()),
                Box::new(failed_by_kind.clone()),
                Box::new(request_duration.clone()),
                Box::new(native_render_duration.clone()),
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
            "register biei metric",
        );

        let metrics = Self {
            registry,
            completed,
            rejected,
            failed,
            failed_by_kind,
            request_duration,
            native_render_duration,
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
            extra_metrics: OnceLock::new(),
        };
        metrics.init_zero_series();
        metrics
    }

    /// Count a would-be render shed because the renderer is degraded. Called at
    /// the shed decision so the count never depends on a later health re-check.
    pub(crate) fn record_render_admission_shed(&self) {
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

    pub(crate) fn record_render_output_cache_hit(&self) {
        self.render_output_cache.with_label_values(&["hit"]).inc();
    }

    pub(crate) fn record_render_output_cache_miss(&self) {
        self.render_output_cache.with_label_values(&["miss"]).inc();
    }

    pub(crate) fn record_render_output_cache_coalesced(&self) {
        self.render_output_cache
            .with_label_values(&["coalesced"])
            .inc();
    }

    pub(crate) fn record_render_output_cache_insert(&self) {
        self.render_output_cache
            .with_label_values(&["insert"])
            .inc();
    }

    pub fn record_profile_prepare(&self, duration: Duration, succeeded: bool) {
        self.profile_prepare_duration
            .with_label_values(&[if succeeded { "success" } else { "failure" }])
            .observe(seconds(duration));
    }

    /// Install a process-global metrics source folded into every scrape (e.g.
    /// the Rust FileSource families). Idempotent: only the first source wins.
    pub fn set_extra_metrics_source(
        &self,
        source: Box<dyn Fn() -> Vec<MetricFamily> + Send + Sync>,
    ) {
        let _ = self.extra_metrics.set(source);
    }

    pub fn gather(&self) -> Vec<MetricFamily> {
        // Node-scoped metrics plus any injected process-global families (e.g.
        // the Rust FileSource metrics), empty until a source is installed.
        let mut families = self.registry.gather();
        if let Some(source) = self.extra_metrics.get() {
            families.extend(source());
        }
        families
    }

    pub fn render_prometheus(&self) -> String {
        let families = self.gather();
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
                        info.completed_at
                            .saturating_duration_since(outcome.arrived_at),
                    ));
                if info.route_tier != RouteTier::RenderCacheHit {
                    let render_seconds = seconds(
                        info.native_render_completed_at
                            .saturating_duration_since(info.native_render_started_at),
                    );
                    self.native_render_duration
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
                self.failed_by_kind
                    .with_label_values(&[scope, kind.as_label()])
                    .inc();
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
            for kind in FailureKind::ALL {
                self.failed_by_kind
                    .with_label_values(&[scope, kind.as_label()])
                    .inc_by(0);
            }
            self.style_swaps.with_label_values(&[scope]).inc_by(0);
            self.cold_starts.with_label_values(&[scope]).inc_by(0);
            self.admission_overflow
                .with_label_values(&[scope])
                .inc_by(0);
            self.native_render_duration.with_label_values(&[scope]);
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

const NATIVE_RENDER_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0,
    3.0, 5.0, 10.0,
];

const DEADLINE_STAGES: [DeadlineStage; 5] = [
    DeadlineStage::AcquireRenderPermit,
    DeadlineStage::StyleSwap,
    DeadlineStage::EnsureSource,
    DeadlineStage::AcquireNativeRenderPermit,
    DeadlineStage::Render,
];

pub fn deadline_stage_label(stage: DeadlineStage) -> &'static str {
    match stage {
        DeadlineStage::AcquireRenderPermit => "acquire_render_permit",
        DeadlineStage::StyleSwap => "style_swap",
        DeadlineStage::EnsureSource => "ensure_source",
        DeadlineStage::AcquireNativeRenderPermit => "acquire_native_render_permit",
        DeadlineStage::Render => "render",
    }
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
                    native_render_started_at: now + Duration::from_millis(2),
                    native_render_completed_at: now + Duration::from_millis(7),
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
        // The bounded per-kind counter records the failure alongside the total.
        assert!(rendered.contains(
            "biei_tasks_failed_by_kind_total{kind=\"render_timeout\",scope=\"ingress\"} 1"
        ));
        // Other kinds stay present at zero (init) so dashboards see a stable set.
        assert!(rendered.contains(
            "biei_tasks_failed_by_kind_total{kind=\"style_unavailable\",scope=\"ingress\"} 0"
        ));
        assert!(rendered.contains("biei_tasks_failed_total{scope=\"ingress\"} 1"));
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
            deadline_stage: Some(DeadlineStage::AcquireNativeRenderPermit),
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
        assert!(
            rendered
                .contains("biei_deadline_exceeded_total{stage=\"acquire_native_render_permit\"} 1")
        );
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
    fn render_cache_hit_records_request_latency_but_not_native_render_latency() {
        let metrics = NodeMetrics::default();
        metrics.record_ingress(&completed_outcome(RouteTier::RenderCacheHit));

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains(
            "biei_request_duration_seconds_count{route_tier=\"render_cache_hit\",scope=\"ingress\"} 1"
        ));
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
        assert_eq!(
            rejection_reason_label(RejectionReason::RendererDegraded),
            "renderer_degraded"
        );
    }
}
