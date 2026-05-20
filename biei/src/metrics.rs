//! Production node metrics: a Prometheus `Registry` of counters/histograms
//! (`NodeMetrics`) plus the label helpers used to render them.

use std::time::Duration;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
    proto::MetricFamily,
};

use crate::types::{DeadlineStage, RejectionReason, RouteTier, TaskOutcome, TaskResult};

/// One renderer worker's gauge sample. Primitives only, so the dynamic gauge
/// schema can live here in `metrics` without depending on the worker/profile
/// types — the caller (HTTP layer) extracts these from its worker snapshot.
pub struct WorkerGaugeSample {
    pub worker: String,
    pub style_id: String,
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
    cpu_render_duration: HistogramVec,
    style_swaps: IntCounterVec,
    cold_starts: IntCounterVec,
    source_cache: IntCounterVec,
    render_output_cache: IntCounterVec,
    forwards: IntCounterVec,
    admission_overflow: IntCounterVec,
    deadline_exceeded: IntCounterVec,
}

impl NodeMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let completed = IntCounterVec::new(
            prometheus::Opts::new(
                "biei_tasks_completed_total",
                "Completed tasks by ingress scope and route tier.",
            ),
            &["scope", "route_tier"],
        )
        .expect("valid completed counter");
        let rejected = IntCounterVec::new(
            prometheus::Opts::new(
                "biei_tasks_rejected_total",
                "Rejected tasks by ingress scope and reason.",
            ),
            &["scope", "reason"],
        )
        .expect("valid rejected counter");
        let failed = IntCounterVec::new(
            prometheus::Opts::new("biei_tasks_failed_total", "Failed tasks by ingress scope."),
            &["scope"],
        )
        .expect("valid failed counter");
        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                "biei_request_duration_seconds",
                "End-to-end task duration from node arrival to completion.",
            )
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["scope", "route_tier"],
        )
        .expect("valid request duration histogram");
        let cpu_render_duration = HistogramVec::new(
            HistogramOpts::new(
                "biei_cpu_render_duration_seconds",
                "CPU/GPU-heavy render-stage duration.",
            )
            .buckets(CPU_RENDER_BUCKETS.to_vec()),
            &["scope"],
        )
        .expect("valid cpu render duration histogram");
        let style_swaps = IntCounterVec::new(
            prometheus::Opts::new(
                "biei_style_swaps_total",
                "Completed tasks that swapped style/profile.",
            ),
            &["scope"],
        )
        .expect("valid style swap counter");
        let cold_starts = IntCounterVec::new(
            prometheus::Opts::new(
                "biei_cold_starts_total",
                "Completed tasks that used a cold renderer slot.",
            ),
            &["scope"],
        )
        .expect("valid cold start counter");
        let source_cache = IntCounterVec::new(
            prometheus::Opts::new(
                "biei_source_cache_total",
                "Addlayer source cache hits and misses.",
            ),
            &["outcome"],
        )
        .expect("valid source cache counter");
        let render_output_cache = IntCounterVec::new(
            prometheus::Opts::new(
                "biei_render_output_cache_total",
                "Rendered image cache lookups and insertions.",
            ),
            &["outcome"],
        )
        .expect("valid render output cache counter");
        let forwards = IntCounterVec::new(
            prometheus::Opts::new(
                "biei_forwards_total",
                "Internal forward attempts by outcome.",
            ),
            &["outcome"],
        )
        .expect("valid forwards counter");
        let admission_overflow = IntCounterVec::new(
            prometheus::Opts::new(
                "biei_admission_overflow_total",
                "Completed tasks admitted while the chosen worker was already at or above BL.",
            ),
            &["scope"],
        )
        .expect("valid admission overflow counter");
        let deadline_exceeded = IntCounterVec::new(
            prometheus::Opts::new(
                "biei_deadline_exceeded_total",
                "Deadline rejections by worker stage.",
            ),
            &["stage"],
        )
        .expect("valid deadline stage counter");

        for collector in [
            Box::new(completed.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(rejected.clone()),
            Box::new(failed.clone()),
            Box::new(request_duration.clone()),
            Box::new(cpu_render_duration.clone()),
            Box::new(style_swaps.clone()),
            Box::new(cold_starts.clone()),
            Box::new(source_cache.clone()),
            Box::new(render_output_cache.clone()),
            Box::new(forwards.clone()),
            Box::new(admission_overflow.clone()),
            Box::new(deadline_exceeded.clone()),
        ] {
            registry
                .register(collector)
                .expect("register static biei metric");
        }

        let metrics = Self {
            registry,
            completed,
            rejected,
            failed,
            request_duration,
            cpu_render_duration,
            style_swaps,
            cold_starts,
            source_cache,
            render_output_cache,
            forwards,
            admission_overflow,
            deadline_exceeded,
        };
        metrics.init_zero_series();
        metrics
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

    pub fn record_render_output_cache_insert(&self) {
        self.render_output_cache
            .with_label_values(&["insert"])
            .inc();
    }

    pub fn gather(&self) -> Vec<MetricFamily> {
        self.registry.gather()
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
        let queue_depth = IntGaugeVec::new(
            Opts::new(
                "biei_queue_depth",
                "Current queued tasks per renderer worker.",
            ),
            &["node", "worker", "style_id", "render_mode", "scale"],
        )
        .expect("valid queue gauge");
        let worker_loaded = IntGaugeVec::new(
            Opts::new(
                "biei_worker_loaded",
                "Whether a renderer worker has a loaded profile.",
            ),
            &["node", "worker"],
        )
        .expect("valid worker-loaded gauge");
        let membership_size = IntGaugeVec::new(
            Opts::new("biei_membership_size", "Current membership size by state."),
            &["node", "state"],
        )
        .expect("valid membership gauge");
        let cpu_permits_inuse = IntGaugeVec::new(
            Opts::new(
                "biei_cpu_permits_inuse",
                "Currently held CPU/GPU render-stage permits.",
            ),
            &["node"],
        )
        .expect("valid cpu permits gauge");
        let drain_state = IntGaugeVec::new(
            Opts::new("biei_drain_state", "Whether the node is draining."),
            &["node"],
        )
        .expect("valid drain-state gauge");

        for collector in [
            Box::new(queue_depth.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(worker_loaded.clone()),
            Box::new(membership_size.clone()),
            Box::new(cpu_permits_inuse.clone()),
            Box::new(drain_state.clone()),
        ] {
            registry
                .register(collector)
                .expect("register dynamic biei metric");
        }

        for worker in &runtime.workers {
            queue_depth
                .with_label_values(&[
                    node,
                    &worker.worker,
                    &worker.style_id,
                    worker.render_mode,
                    worker.scale,
                ])
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
                    self.cpu_render_duration
                        .with_label_values(&[scope])
                        .observe(seconds(
                            info.cpu_completed_at.duration_since(info.cpu_started_at),
                        ));
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
            TaskResult::Failed { .. } => {
                self.failed.with_label_values(&[scope]).inc();
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
            self.cpu_render_duration.with_label_values(&[scope]);
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
        for outcome in ["hit", "miss", "insert"] {
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

pub const LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0,
];

const CPU_RENDER_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0,
    3.0,
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
    use crate::types::{CompletedInfo, ImageFormat, NodeId, RenderOutput, RequestId, TaskOutcome};
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

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("biei_style_swaps_total{scope=\"ingress\"} 1"));
        assert!(rendered.contains("biei_cold_starts_total{scope=\"ingress\"} 1"));
        assert!(rendered.contains("biei_source_cache_total{outcome=\"miss\"} 1"));
        assert!(rendered.contains("biei_admission_overflow_total{scope=\"ingress\"} 1"));
        assert!(rendered.contains("biei_deadline_exceeded_total{stage=\"acquire_cpu_permit\"} 1"));
        assert!(rendered.contains("biei_forwards_total{outcome=\"success\"} 1"));
        assert!(rendered.contains("biei_forwards_total{outcome=\"retryable\"} 1"));
        assert!(rendered.contains("biei_forwards_total{outcome=\"fatal\"} 1"));
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
