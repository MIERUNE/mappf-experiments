//! Canonical operational endpoint paths shared by MMPF services.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Public liveness probe endpoint.
pub const PUBLIC_LIVENESS_PATH: &str = "/livez";
/// Public readiness probe endpoint.
pub const PUBLIC_READINESS_PATH: &str = "/readyz";
/// Cluster-internal liveness probe endpoint.
pub const INTERNAL_LIVENESS_PATH: &str = "/_internal/healthz";
/// Cluster-internal readiness probe endpoint.
pub const INTERNAL_READINESS_PATH: &str = "/_internal/readyz";
/// Cluster-internal Prometheus metrics endpoint.
pub const INTERNAL_METRICS_PATH: &str = "/_internal/metrics";
/// Versioned cluster-internal bounded JSON status endpoint.
///
/// `/_internal` is the listener trust boundary. `operations/v1` distinguishes
/// this stable consumer contract from deliberately unstable peer protocols on
/// the same listener.
pub const INTERNAL_STATUS_PATH: &str = "/_internal/operations/v1/status";
/// Status snapshots may be coalesced briefly by a private management client.
pub const OPERATIONAL_STATUS_CACHE_CONTROL: &str = "private, max-age=2, must-revalidate";

/// Current schema of the common operational snapshot envelope.
pub const OPERATIONAL_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
/// Maximum node identities returned in one service-owned membership list.
pub const MAX_OPERATIONAL_MEMBERS: usize = 256;

/// Common envelope for a service-owned operational status payload.
///
/// The payload remains service-specific; this envelope only provides the
/// provenance needed by an optional aggregator to reason about freshness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalSnapshot<T> {
    pub schema_version: u32,
    pub service: String,
    pub observer_node_id: String,
    pub observed_at_unix_ms: u64,
    pub status: T,
}

impl<T> OperationalSnapshot<T> {
    pub fn observed_now(
        service: impl Into<String>,
        observer_node_id: impl Into<String>,
        status: T,
    ) -> Self {
        Self {
            schema_version: OPERATIONAL_SNAPSHOT_SCHEMA_VERSION,
            service: service.into(),
            observer_node_id: observer_node_id.into(),
            observed_at_unix_ms: unix_time_ms(SystemTime::now()),
            status,
        }
    }
}

fn unix_time_ms(time: SystemTime) -> u64 {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operational_snapshot_records_bounded_provenance() {
        let snapshot = OperationalSnapshot::observed_now("biei", "node-a", ());

        assert_eq!(snapshot.schema_version, 1);
        assert_eq!(snapshot.service, "biei");
        assert_eq!(snapshot.observer_node_id, "node-a");
        assert!(snapshot.observed_at_unix_ms > 0);
    }
}
