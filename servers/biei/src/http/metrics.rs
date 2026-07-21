//! Production HTTP metrics composition.
//!
//! `biei-core` owns the node counters and histograms. This module samples the
//! server-only runtime state, constructs its gauge families, and composes both
//! sets into the Prometheus response.

use biei_core::gossip::GossipBus;
use biei_core::metrics::NodeMetrics;
use mmpf_common::metrics::{counter_vec, encode_metric_families, gauge_vec, register_collectors};
use prometheus::core::Collector;
use prometheus::{IntCounterVec, Registry, proto::MetricFamily};

struct WorkerGaugeSample {
    worker: String,
    render_mode: &'static str,
    scale: &'static str,
    queue_depth: i64,
    loaded: bool,
}

struct RuntimeGauges {
    node_id: String,
    workers: Vec<WorkerGaugeSample>,
    /// Live membership size, if this node tracks membership.
    membership_live: Option<i64>,
    native_render_permits_inuse: i64,
    draining: bool,
    renderer_total: i64,
    renderer_available: i64,
    renderer_orphaned: i64,
    renderer_health: &'static str,
    renderer_replacements_succeeded: u64,
    renderer_replacements_exhausted: u64,
    renderer_replacements_failed: u64,
}

#[derive(Clone)]
pub(crate) struct HttpMetrics {
    node: biei_core::node::Node,
    membership: Option<crate::membership::Membership>,
    drain: Option<crate::drain::DrainController>,
    renderer_supervisor: crate::renderer::actor::RendererActorSupervisor,
    /// Server-owned HTTP request tally, keyed by a bounded endpoint/status
    /// vocabulary. Complements `NodeMetrics`, whose counters only begin once a
    /// task exists — this covers method/URI-length/parse/admission/route
    /// rejections that never reach core. `IntCounterVec` is `Arc`-backed, so
    /// cloning `HttpMetrics` into the routers and the scrape shares one tally.
    http_requests: IntCounterVec,
}

impl HttpMetrics {
    pub(crate) fn new(
        node: biei_core::node::Node,
        membership: Option<crate::membership::Membership>,
        drain: Option<crate::drain::DrainController>,
        renderer_supervisor: crate::renderer::actor::RendererActorSupervisor,
    ) -> Self {
        Self {
            node,
            membership,
            drain,
            renderer_supervisor,
            http_requests: counter_vec(
                "biei_http_requests_total",
                "HTTP responses by bounded endpoint and status code.",
                &["endpoint", "status"],
            ),
        }
    }

    /// Record one completed HTTP response. `endpoint` comes from the fixed
    /// [`RequestEndpoint`] vocabulary; `status` maps to a fixed label via
    /// [`status_label`], so both are safe labels — never a raw path, id, style,
    /// or source — with no per-request allocation.
    pub(crate) fn record_request(&self, endpoint: RequestEndpoint, status: u16) {
        self.http_requests
            .with_label_values(&[endpoint.as_label(), status_label(status)])
            .inc();
    }

    pub(crate) async fn render_prometheus(&self) -> String {
        let node_id = self.node.id();
        let workers = self
            .node
            .worker_snapshot()
            .iter()
            .map(|worker| {
                let profile = worker.loaded_profile.as_ref();
                WorkerGaugeSample {
                    worker: worker.id.to_string(),
                    render_mode: profile
                        .map(|profile| profile.render_mode.as_gossip_value())
                        .unwrap_or("none"),
                    scale: profile
                        .map(|profile| profile.scale.as_gossip_value())
                        .unwrap_or("none"),
                    queue_depth: worker.queue_depth as i64,
                    loaded: worker.loaded_profile.is_some(),
                }
            })
            .collect();
        let membership_live = match &self.membership {
            Some(membership) => Some(membership.view().await.members.len() as i64),
            None => None,
        };
        let renderer = self.renderer_supervisor.snapshot();
        let runtime = RuntimeGauges {
            node_id: node_id.as_str().to_string(),
            workers,
            membership_live,
            native_render_permits_inuse: self.node.native_render_permits_inuse() as i64,
            draining: self.drain.as_ref().is_some_and(|drain| drain.is_draining()),
            renderer_total: renderer.total_slots as i64,
            renderer_available: renderer.available_slots as i64,
            renderer_orphaned: renderer.orphaned_threads as i64,
            renderer_health: renderer.health.as_str(),
            renderer_replacements_succeeded: renderer.replacements_succeeded,
            renderer_replacements_exhausted: renderer.replacements_exhausted,
            renderer_replacements_failed: renderer.replacements_failed,
        };
        // The scrape reads the tally before this request's own increment (the
        // middleware records after the response), so a `/metrics` scrape never
        // includes itself and cannot recursively distort its own measurement.
        //
        // `Collector::collect` (unlike `Registry::gather`) yields the family even
        // with no series; the text encoder rejects an empty family and would
        // blank the whole scrape, so drop it until at least one request lands.
        let request_families: Vec<MetricFamily> = self
            .http_requests
            .collect()
            .into_iter()
            .filter(|family| !family.get_metric().is_empty())
            .collect();
        render_prometheus_with_runtime(&self.node.metrics(), &runtime, request_families)
    }
}

/// Fixed, low-cardinality classification of an HTTP request for the
/// `biei_http_requests_total` tally. Never carries a raw path or user input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestEndpoint {
    Health,
    Ready,
    Metrics,
    InternalForward,
    Render,
    NotFound,
}

impl RequestEndpoint {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            RequestEndpoint::Health => "health",
            RequestEndpoint::Ready => "ready",
            RequestEndpoint::Metrics => "metrics",
            RequestEndpoint::InternalForward => "internal_forward",
            RequestEndpoint::Render => "render",
            RequestEndpoint::NotFound => "not_found",
        }
    }
}

/// Maps an emitted HTTP status to a fixed label. The biei HTTP layer only emits
/// this bounded set of codes, so a `match` to `&'static str` keeps the metric
/// output identical while avoiding a per-request `to_string` allocation and
/// hard-bounding label cardinality (any unexpected code collapses to `"other"`
/// rather than minting a new series).
fn status_label(status: u16) -> &'static str {
    match status {
        200 => "200",
        400 => "400",
        404 => "404",
        405 => "405",
        408 => "408",
        413 => "413",
        414 => "414",
        415 => "415",
        429 => "429",
        500 => "500",
        502 => "502",
        503 => "503",
        504 => "504",
        _ => "other",
    }
}

fn render_prometheus_with_runtime(
    metrics: &NodeMetrics,
    runtime: &RuntimeGauges,
    request_families: Vec<MetricFamily>,
) -> String {
    let mut families = metrics.gather();
    families.extend(runtime_metric_families(runtime));
    families.extend(request_families);
    encode_metric_families(&families)
}

fn runtime_metric_families(runtime: &RuntimeGauges) -> Vec<MetricFamily> {
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
    let native_render_permits_inuse = gauge_vec(
        "biei_native_render_permits_inuse",
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
            Box::new(queue_depth.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(worker_loaded.clone()),
            Box::new(membership_size.clone()),
            Box::new(native_render_permits_inuse.clone()),
            Box::new(drain_state.clone()),
            Box::new(renderer_slots.clone()),
            Box::new(renderer_orphaned.clone()),
            Box::new(renderer_health.clone()),
            Box::new(renderer_replacements.clone()),
        ],
        "register biei metric",
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
    native_render_permits_inuse
        .with_label_values(&[node])
        .set(runtime.native_render_permits_inuse);
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

    registry.gather()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_label_is_bounded_and_allocation_free() {
        // Every code the HTTP layer emits maps to its numeric string (identical
        // to the previous `to_string` output, so dashboards are unaffected)...
        assert_eq!(status_label(200), "200");
        assert_eq!(status_label(404), "404");
        assert_eq!(status_label(503), "503");
        assert_eq!(status_label(504), "504");
        // ...and anything unexpected collapses to a single bounded label rather
        // than minting a new series.
        assert_eq!(status_label(418), "other");
        assert_eq!(status_label(0), "other");
    }

    #[test]
    fn runtime_metrics_preserve_prometheus_contract() {
        let runtime = RuntimeGauges {
            node_id: "node-a".to_string(),
            workers: vec![WorkerGaugeSample {
                worker: "7".to_string(),
                render_mode: "static",
                scale: "2x",
                queue_depth: 3,
                loaded: true,
            }],
            membership_live: Some(4),
            native_render_permits_inuse: 2,
            draining: true,
            renderer_total: 5,
            renderer_available: 3,
            renderer_orphaned: 1,
            renderer_health: "external_degraded",
            renderer_replacements_succeeded: 11,
            renderer_replacements_exhausted: 12,
            renderer_replacements_failed: 13,
        };

        let rendered =
            render_prometheus_with_runtime(&NodeMetrics::default(), &runtime, Vec::new());

        for expected in [
            "# HELP biei_queue_depth Current queued tasks per renderer worker.",
            "# TYPE biei_queue_depth gauge",
            "biei_queue_depth{node=\"node-a\",render_mode=\"static\",scale=\"2x\",worker=\"7\"} 3",
            "biei_worker_loaded{node=\"node-a\",worker=\"7\"} 1",
            "biei_membership_size{node=\"node-a\",state=\"live\"} 4",
            "biei_native_render_permits_inuse{node=\"node-a\"} 2",
            "biei_drain_state{node=\"node-a\"} 1",
            "biei_renderer_slots{node=\"node-a\",state=\"total\"} 5",
            "biei_renderer_slots{node=\"node-a\",state=\"available\"} 3",
            "biei_renderer_orphan_threads{node=\"node-a\"} 1",
            "biei_renderer_health{node=\"node-a\",state=\"full\"} 0",
            "biei_renderer_health{node=\"node-a\",state=\"external_degraded\"} 1",
            "biei_renderer_health{node=\"node-a\",state=\"internal_unrecoverable\"} 0",
            "biei_renderer_replacements_total{node=\"node-a\",outcome=\"success\"} 11",
            "biei_renderer_replacements_total{node=\"node-a\",outcome=\"exhausted\"} 12",
            "biei_renderer_replacements_total{node=\"node-a\",outcome=\"spawn_failed\"} 13",
            "# TYPE biei_tasks_completed_total counter",
        ] {
            assert!(
                rendered.lines().any(|line| line == expected),
                "missing `{expected}` in:\n{rendered}"
            );
        }
    }
}
