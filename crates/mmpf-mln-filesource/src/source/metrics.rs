//! Prometheus instrumentation for the Rust network FileSource.

use std::sync::OnceLock;

use maplibre_native::file_source::{
    ErrorReason, Priority, ResourceKind, ResourceRequest, Response, Usage,
};
use mmpf_common::metrics::{counter_vec, gauge_vec, histogram_vec};
use prometheus::{HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Registry};

use super::cache;

/// Process-global metrics for the network file source. Kept in a module-local
/// registry (the source itself is process-global, unlike per-node
/// `NodeMetrics`); `gather_metrics` feeds them into the `/_internal/metrics`
/// exposition.
pub(super) struct FsMetrics {
    pub(super) registry: Registry,
    pub(super) requests_total: IntCounterVec,
    pub(super) response_bytes_total: IntCounterVec,
    pub(super) retries_total: IntCounterVec,
    pub(super) retry_sequences_inflight: IntGauge,
    pub(super) slow_attempts_inflight: IntGauge,
    pub(super) negative_cache_total: IntCounterVec,
    pub(super) singleflight_total: IntCounterVec,
    pub(super) refresh_deferred_total: IntCounterVec,
    pub(super) refresh_deferred_inflight: IntGaugeVec,
    pub(super) duration_seconds: HistogramVec,
    pub(super) admission_wait_seconds: HistogramVec,
    pub(super) body_wait_seconds: HistogramVec,
    pub(super) upstream_attempts_total: IntCounterVec,
    pub(super) upstream_attempt_duration_seconds: HistogramVec,
    pub(super) inflight: IntGaugeVec,
    pub(super) bodies_inflight: IntGaugeVec,
}

impl FsMetrics {
    fn new() -> Self {
        let registry = Registry::new();
        let requests_total = counter_vec(
            "mmpf_mln_resource_requests_total",
            "Resource requests handled by the Rust network FileSource.",
            &["kind", "priority", "usage", "outcome"],
        );
        let response_bytes_total = counter_vec(
            "mmpf_mln_resource_response_bytes_total",
            "Resource response bytes returned to MapLibre Native by the Rust network FileSource.",
            &["kind", "outcome"],
        );
        let retries_total = counter_vec(
            "mmpf_mln_resource_retries_total",
            "Upstream resource request retries by resource kind and reason.",
            &["kind", "reason"],
        );
        let retry_sequences_inflight = IntGauge::new(
            "mmpf_mln_resource_retry_sequences_inflight",
            "Render-blocking resource requests currently retrying a transient upstream failure.",
        )
        .expect("valid retry-sequence gauge");
        let slow_attempts_inflight = IntGauge::new(
            "mmpf_mln_resource_slow_attempts_inflight",
            "Render-blocking upstream attempts still in flight after the provider-health threshold.",
        )
        .expect("valid slow-attempt gauge");
        let negative_cache_total = counter_vec(
            "mmpf_mln_resource_negative_cache_total",
            "Negative resource cache operations by resource kind.",
            &["kind", "operation"],
        );
        let singleflight_total = counter_vec(
            "mmpf_mln_resource_singleflight_total",
            "Cross-renderer resource single-flight participation by role.",
            &["kind", "role"],
        );
        let refresh_deferred_total = counter_vec(
            "mmpf_mln_resource_refresh_deferred_total",
            "Fresh cache hits whose network refresh was deferred until expiry.",
            &["kind"],
        );
        let refresh_deferred_inflight = gauge_vec(
            "mmpf_mln_resource_refresh_deferred_inflight",
            "Network FileSource requests currently sleeping until cache expiry.",
            &["kind"],
        );
        let duration_seconds = histogram_vec(
            "mmpf_mln_resource_request_duration_seconds",
            "Rust network FileSource request duration by resource kind.",
            &["kind"],
        );
        let admission_wait_seconds = histogram_vec(
            "mmpf_mln_resource_admission_wait_seconds",
            "Time spent waiting for a Rust network FileSource admission permit.",
            &["kind", "priority"],
        );
        let body_wait_seconds = histogram_vec(
            "mmpf_mln_resource_body_wait_seconds",
            "Time spent waiting for a shared FileSource response-body permit.",
            &["kind"],
        );
        let upstream_attempts_total = counter_vec(
            "mmpf_mln_resource_upstream_attempts_total",
            "Actual upstream HTTP attempts made by the Rust network FileSource.",
            &["kind", "priority", "outcome"],
        );
        let upstream_attempt_duration_seconds = histogram_vec(
            "mmpf_mln_resource_upstream_attempt_duration_seconds",
            "Actual upstream HTTP network-pending duration (send and response chunks), excluding lane/body admission and retry backoff.",
            &["kind", "priority"],
        );
        let inflight = gauge_vec(
            "mmpf_mln_resource_inflight",
            "Upstream resource fetches currently in flight.",
            &["priority"],
        );
        let bodies_inflight = gauge_vec(
            "mmpf_mln_resource_bodies_inflight",
            "Resource response bodies currently being downloaded under a body permit.",
            &["kind"],
        );
        for collector in [
            Box::new(requests_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(response_bytes_total.clone()),
            Box::new(retries_total.clone()),
            Box::new(retry_sequences_inflight.clone()),
            Box::new(slow_attempts_inflight.clone()),
            Box::new(negative_cache_total.clone()),
            Box::new(singleflight_total.clone()),
            Box::new(refresh_deferred_total.clone()),
            Box::new(refresh_deferred_inflight.clone()),
            Box::new(duration_seconds.clone()),
            Box::new(admission_wait_seconds.clone()),
            Box::new(body_wait_seconds.clone()),
            Box::new(upstream_attempts_total.clone()),
            Box::new(upstream_attempt_duration_seconds.clone()),
            Box::new(inflight.clone()),
            Box::new(bodies_inflight.clone()),
        ] {
            registry.register(collector).expect("register fs metric");
        }
        Self {
            registry,
            requests_total,
            response_bytes_total,
            retries_total,
            retry_sequences_inflight,
            slow_attempts_inflight,
            negative_cache_total,
            singleflight_total,
            refresh_deferred_total,
            refresh_deferred_inflight,
            duration_seconds,
            admission_wait_seconds,
            body_wait_seconds,
            upstream_attempts_total,
            upstream_attempt_duration_seconds,
            inflight,
            bodies_inflight,
        }
    }
}

pub(super) fn fs_metrics() -> &'static FsMetrics {
    static METRICS: OnceLock<FsMetrics> = OnceLock::new();
    METRICS.get_or_init(FsMetrics::new)
}

/// Set once the file source has been registered; gates the metrics exposition
/// so the endpoint doesn't force registry creation for an unused source.
static METRICS_STARTED: OnceLock<()> = OnceLock::new();

/// Metric families for the `/_internal/metrics` exposition. Empty until the
/// file source is registered — callers just append.
pub fn gather_metrics() -> Vec<prometheus::proto::MetricFamily> {
    if METRICS_STARTED.get().is_none() {
        return Vec::new();
    }
    let mut families = fs_metrics().registry.gather();
    families.extend(cache::gather_metrics());
    families
}

pub(super) fn mark_metrics_started() {
    let _ = METRICS_STARTED.set(());
}

/// Bounded label for a resource kind. `ResourceKind` is a cxx shared enum
/// (struct + consts), so compare with `==` rather than `match` patterns.
pub(super) fn kind_label(kind: ResourceKind) -> &'static str {
    if kind == ResourceKind::Style {
        "style"
    } else if kind == ResourceKind::Source {
        "source"
    } else if kind == ResourceKind::Tile {
        "tile"
    } else if kind == ResourceKind::Glyphs {
        "glyphs"
    } else if kind == ResourceKind::SpriteImage {
        "sprite_image"
    } else if kind == ResourceKind::SpriteJSON {
        "sprite_json"
    } else if kind == ResourceKind::Image {
        "image"
    } else {
        "other"
    }
}

pub(super) fn priority_label(priority: Priority) -> &'static str {
    if priority == Priority::Low {
        "low"
    } else {
        "regular"
    }
}

pub(super) fn usage_label(usage: Usage) -> &'static str {
    if usage == Usage::Offline {
        "offline"
    } else {
        "online"
    }
}

pub(super) struct RequestObservation {
    kind: &'static str,
    priority: &'static str,
    usage: &'static str,
    started: std::time::Instant,
    pub(super) outcome: &'static str,
    pub(super) response_bytes: usize,
}

impl RequestObservation {
    pub(super) fn new(request: &ResourceRequest) -> Self {
        Self {
            kind: kind_label(request.kind),
            priority: priority_label(request.priority),
            usage: usage_label(request.usage),
            started: std::time::Instant::now(),
            outcome: "cancelled",
            response_bytes: 0,
        }
    }
}

impl Drop for RequestObservation {
    fn drop(&mut self) {
        let metrics = fs_metrics();
        metrics
            .duration_seconds
            .with_label_values(&[self.kind])
            .observe(self.started.elapsed().as_secs_f64());
        metrics
            .requests_total
            .with_label_values(&[self.kind, self.priority, self.usage, self.outcome])
            .inc();
        metrics
            .response_bytes_total
            .with_label_values(&[self.kind, self.outcome])
            .inc_by(self.response_bytes as u64);
    }
}

pub(super) struct UpstreamAttemptObservation {
    kind: &'static str,
    priority: &'static str,
    network_duration: std::time::Duration,
    pub(super) outcome: &'static str,
}

impl UpstreamAttemptObservation {
    pub(super) fn new(request: &ResourceRequest) -> Self {
        Self {
            kind: kind_label(request.kind),
            priority: priority_label(request.priority),
            network_duration: std::time::Duration::ZERO,
            outcome: "cancelled",
        }
    }

    pub(super) fn add_network_duration(&mut self, duration: std::time::Duration) {
        self.network_duration = self.network_duration.saturating_add(duration);
    }
}

impl Drop for UpstreamAttemptObservation {
    fn drop(&mut self) {
        let metrics = fs_metrics();
        metrics
            .upstream_attempts_total
            .with_label_values(&[self.kind, self.priority, self.outcome])
            .inc();
        metrics
            .upstream_attempt_duration_seconds
            .with_label_values(&[self.kind, self.priority])
            .observe(self.network_duration.as_secs_f64());
    }
}

pub(super) struct InflightGuard(IntGauge);

pub(super) struct BodyInflightGuard(IntGauge);

pub(super) struct DeferredRefreshGuard(IntGauge);

impl DeferredRefreshGuard {
    pub(super) fn new(kind: &'static str) -> Self {
        let gauge = fs_metrics()
            .refresh_deferred_inflight
            .with_label_values(&[kind]);
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for DeferredRefreshGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

impl InflightGuard {
    pub(super) fn new(priority_lane: &'static str) -> Self {
        let gauge = fs_metrics().inflight.with_label_values(&[priority_lane]);
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

impl BodyInflightGuard {
    pub(super) fn new(kind: ResourceKind) -> Self {
        let gauge = fs_metrics()
            .bodies_inflight
            .with_label_values(&[kind_label(kind)]);
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for BodyInflightGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

pub(super) fn outcome_label(response: &Response) -> &'static str {
    if response.no_content {
        return "no_content";
    }
    let Some(error) = &response.error else {
        return "ok";
    };
    let reason = error.reason;
    if reason == ErrorReason::NotFound {
        "not_found"
    } else if reason == ErrorReason::Server {
        "server"
    } else if reason == ErrorReason::Connection {
        "connection"
    } else if reason == ErrorReason::RateLimit {
        "rate_limit"
    } else {
        "other"
    }
}
