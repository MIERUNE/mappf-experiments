use mmpf_common::metrics::{counter_vec, gauge_vec, register_collectors};
use prometheus::{GaugeVec, IntCounterVec, IntGaugeVec, Opts, Registry, proto::MetricFamily};

/// Makes revocation lag observable while the read tier serves last-known-good
/// authorization. This crate does not impose one maximum age because the
/// acceptable limit is deployment policy. Alert on `unvalidated_seconds`
/// together with `snapshot_loaded`, not age alone: refresh is request-driven,
/// so an idle registry can age without serving stale grants.
pub(super) struct AuthMetrics {
    registry: Registry,
    snapshot_age_seconds: GaugeVec,
    unvalidated_seconds: GaugeVec,
    snapshot_revision: IntGaugeVec,
    snapshot_loaded: IntGaugeVec,
    refresh_total: IntCounterVec,
}

impl AuthMetrics {
    pub(super) fn new() -> Self {
        let registry = Registry::new();
        let snapshot_age_seconds = GaugeVec::new(
            Opts::new(
                "mmpf_auth_registry_snapshot_age_seconds",
                "Seconds since the auth registry snapshot being served was last validated.",
            ),
            &["registry_id"],
        )
        .expect("auth registry snapshot age metric must be valid");
        let unvalidated_seconds = GaugeVec::new(
            Opts::new(
                "mmpf_auth_registry_unvalidated_seconds",
                "Seconds the served auth registry snapshot has gone unvalidated because refresh keeps failing; zero when refresh is healthy.",
            ),
            &["registry_id"],
        )
        .expect("auth registry unvalidated duration metric must be valid");
        let snapshot_revision = gauge_vec(
            "mmpf_auth_registry_revision",
            "Revision of the auth registry snapshot being served.",
            &["registry_id"],
        );
        let snapshot_loaded = gauge_vec(
            "mmpf_auth_registry_snapshot_loaded",
            "Whether a configured auth registry has a usable snapshot; zero means requests against it fail closed.",
            &["registry_id"],
        );
        let refresh_total = counter_vec(
            "mmpf_auth_registry_refresh_total",
            "Auth registry refresh attempts by outcome.",
            &["registry_id", "outcome"],
        );
        register_collectors(
            &registry,
            [
                Box::new(snapshot_age_seconds.clone()) as Box<dyn prometheus::core::Collector>,
                Box::new(unvalidated_seconds.clone()),
                Box::new(snapshot_revision.clone()),
                Box::new(snapshot_loaded.clone()),
                Box::new(refresh_total.clone()),
            ],
            "register auth registry metric",
        );
        Self {
            registry,
            snapshot_age_seconds,
            unvalidated_seconds,
            snapshot_revision,
            snapshot_loaded,
            refresh_total,
        }
    }

    /// Outcomes are bounded; detailed failures stay in the warning logs.
    pub(super) fn record_refresh(&self, registry_id: &str, outcome: &'static str) {
        self.refresh_total
            .with_label_values(&[registry_id, outcome])
            .inc();
    }

    pub(super) fn observe_snapshot(
        &self,
        registry_id: &str,
        loaded: bool,
        age_seconds: f64,
        unvalidated_seconds: f64,
        revision: u64,
    ) {
        let labels = [registry_id];
        self.snapshot_loaded
            .with_label_values(&labels)
            .set(i64::from(loaded));
        self.snapshot_age_seconds
            .with_label_values(&labels)
            .set(age_seconds);
        self.unvalidated_seconds
            .with_label_values(&labels)
            .set(unvalidated_seconds);
        self.snapshot_revision
            .with_label_values(&labels)
            .set(revision.min(i64::MAX as u64) as i64);
    }

    pub(super) fn gather(&self) -> Vec<MetricFamily> {
        self.registry.gather()
    }
}
