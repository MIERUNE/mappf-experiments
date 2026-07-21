//! Canonical operational endpoint paths shared by MMPF services.

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
