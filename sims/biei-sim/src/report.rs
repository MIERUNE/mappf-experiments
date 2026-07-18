use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};

use crate::churn::ChurnReport;
use crate::config::{SimConfig, SourceProvider, StyleDist};
use crate::metrics::Report;
use biei_core::config::{BlCapacityPolicy, Tier1Strategy};

pub const REPORT_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Serialize)]
pub struct RunReport {
    pub schema_version: u32,
    pub simulator: &'static str,
    pub simulator_version: &'static str,
    pub config: Value,
    pub churn: Option<ChurnReport>,
    pub result: ReportSnapshot,
}

impl RunReport {
    pub fn new(config: &SimConfig, result: &Report, churn: Option<ChurnReport>) -> Self {
        Self {
            schema_version: REPORT_SCHEMA_VERSION,
            simulator: "biei-sim",
            simulator_version: env!("CARGO_PKG_VERSION"),
            config: config_snapshot(config),
            churn,
            result: ReportSnapshot::from(result),
        }
    }

    pub fn write_json(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let bytes = serde_json::to_vec_pretty(self).context("serialize simulator report")?;
        fs::write(path, bytes).with_context(|| format!("write report {}", path.display()))
    }
}

#[derive(Debug, Serialize)]
pub struct ReportSnapshot {
    pub total: usize,
    pub completed: usize,
    pub rejected: usize,
    pub failed: usize,
    pub rejection_by_reason: BTreeMap<String, usize>,
    pub failure_by_error: BTreeMap<String, usize>,
    pub sla_violations: usize,
    pub sla_ms: f64,
    pub throughput: f64,
    pub latency_p50_ms: f64,
    pub latency_p90_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub latency_max_ms: f64,
    pub latency_histogram: Vec<LatencyHistogramBucketSnapshot>,
    pub cold_starts: usize,
    pub style_swaps: usize,
    pub overflow_admissions: usize,
    pub tasks_with_sources: usize,
    pub source_loads: usize,
    pub source_hits: usize,
    pub tier_counts: BTreeMap<String, usize>,
    pub elapsed_ms: f64,
    pub native_render_permits_total: usize,
    pub native_render_busy_ms: f64,
    pub native_render_avg_inflight: f64,
    pub native_render_peak_inflight: usize,
    pub native_render_utilization_pct: f64,
}

#[derive(Debug, Serialize)]
pub struct LatencyHistogramBucketSnapshot {
    pub upper_bound_ms: Option<f64>,
    pub count: usize,
}

impl From<&Report> for ReportSnapshot {
    fn from(report: &Report) -> Self {
        Self {
            total: report.total,
            completed: report.completed,
            rejected: report.rejected,
            failed: report.failed,
            rejection_by_reason: report
                .rejection_by_reason
                .iter()
                .map(|(reason, count)| (format!("{reason:?}"), *count))
                .collect(),
            failure_by_error: report
                .failure_by_error
                .iter()
                .map(|(error, count)| (error.clone(), *count))
                .collect(),
            sla_violations: report.sla_violations,
            sla_ms: millis(report.sla),
            throughput: report.throughput,
            latency_p50_ms: millis(report.latency_p50),
            latency_p90_ms: millis(report.latency_p90),
            latency_p95_ms: millis(report.latency_p95),
            latency_p99_ms: millis(report.latency_p99),
            latency_max_ms: millis(report.latency_max),
            latency_histogram: report
                .latency_histogram
                .iter()
                .map(|bucket| LatencyHistogramBucketSnapshot {
                    upper_bound_ms: bucket.upper_bound.map(millis),
                    count: bucket.count,
                })
                .collect(),
            cold_starts: report.cold_starts,
            style_swaps: report.style_swaps,
            overflow_admissions: report.overflow_admissions,
            tasks_with_sources: report.tasks_with_sources,
            source_loads: report.source_loads,
            source_hits: report.source_hits,
            tier_counts: report
                .tier_counts
                .iter()
                .map(|(tier, count)| (format!("{tier:?}"), *count))
                .collect(),
            elapsed_ms: millis(report.elapsed),
            native_render_permits_total: report.native_render_permits_total,
            native_render_busy_ms: millis(report.native_render_busy),
            native_render_avg_inflight: report.native_render_avg_inflight,
            native_render_peak_inflight: report.native_render_peak_inflight,
            native_render_utilization_pct: report.native_render_utilization_pct,
        }
    }
}

fn config_snapshot(config: &SimConfig) -> Value {
    // Keep these patterns exhaustive. A new configuration field must make
    // this function fail to compile rather than silently disappearing from
    // the reproducibility snapshot.
    let SimConfig {
        node_count,
        cpu_cores_per_node,
        cluster,
        costs,
        workload,
        gossip,
        routing,
        seed,
    } = config;
    let biei_core::config::ClusterConfig {
        renderer_slots_per_node,
        render_permits_per_node,
        cpu_render_permits_per_node,
        bl_capacity,
        queue_capacity_multiplier,
        source_cache_capacity,
        render_output_cache_capacity_bytes,
    } = cluster;
    let biei_core::config::CostConfig {
        style_setup_cost,
        source_load_cost,
        render_cpu_cost,
        render_resource_cost,
        first_render_resource_cost,
        hop_latency,
        sla,
    } = costs;
    let crate::config::WorkloadConfig {
        duration,
        total_rate,
        style_count,
        style_distribution,
        new_style_rate,
        burst_pattern,
        source_pattern,
        warmup,
        style_shift,
        tile_style_count,
    } = workload;
    let biei_core::config::GossipConfig { publish_interval } = gossip;
    let biei_core::config::RoutingConfig {
        tier1_strategy,
        tier3_enabled,
        drain_max_queue,
    } = routing;

    json!({
        "node_count": node_count,
        "cpu_cores_per_node": cpu_cores_per_node,
        "seed": seed,
        "cluster": {
            "renderer_slots_per_node": renderer_slots_per_node,
            "render_permits_per_node": render_permits_per_node,
            "effective_render_permits_per_node": cluster.resolved_render_permits_per_node(),
            "native_render_permits_per_node": cpu_render_permits_per_node,
            "effective_native_render_permits_per_node": cluster.resolved_cpu_render_permits_per_node(),
            "bl_capacity": match bl_capacity {
                BlCapacityPolicy::Fixed(value) => json!({"kind": "fixed", "value": value}),
                BlCapacityPolicy::Auto => json!({"kind": "auto"}),
            },
            "queue_capacity_multiplier": queue_capacity_multiplier,
            "source_cache_capacity": source_cache_capacity,
            "render_output_cache_capacity_bytes": render_output_cache_capacity_bytes,
        },
        "costs": {
            "style_setup_ms": range_snapshot(*style_setup_cost),
            "source_load_ms": range_snapshot(*source_load_cost),
            "render_cpu_ms": range_snapshot(*render_cpu_cost),
            "render_resource_ms": range_snapshot(*render_resource_cost),
            "first_render_resource_ms": range_snapshot(*first_render_resource_cost),
            "hop_latency_ms": millis(*hop_latency),
            "sla_ms": millis(*sla),
        },
        "workload": {
            "duration_ms": millis(*duration),
            "total_rate": total_rate,
            "style_count": style_count,
            "style_distribution": style_dist_snapshot(style_distribution),
            "new_style_rate": new_style_rate,
            "warmup_ms": millis(*warmup),
            "tile_style_count": tile_style_count,
            "burst_pattern": burst_pattern.as_ref().map(|burst| json!({
                "period_ms": millis(burst.period),
                "duration_ms": millis(burst.duration),
                "multiplier": burst.multiplier,
                "style_focus": burst.style_focus,
            })),
            "style_shift": style_shift.as_ref().map(|shift| json!({
                "at_ms": millis(shift.at),
                "with": shift.with,
            })),
            "source_pattern": source_pattern.as_ref().map(|pattern| json!({
                "probability": pattern.probability,
                "provider": source_provider_snapshot(&pattern.provider),
            })),
        },
        "gossip": { "publish_interval_ms": millis(*publish_interval) },
        "routing": {
            "tier1_strategy": match tier1_strategy {
                Tier1Strategy::WeightedRandom => "weighted_random",
                Tier1Strategy::PowerOfTwo => "power_of_two",
            },
            "tier3_enabled": tier3_enabled,
            "drain_max_queue": drain_max_queue,
        },
    })
}

fn range_snapshot(range: biei_core::config::CostRange) -> Value {
    json!({ "min": millis(range.min), "max": millis(range.max) })
}

fn style_dist_snapshot(dist: &StyleDist) -> Value {
    match dist {
        StyleDist::Uniform => json!({ "kind": "uniform" }),
        StyleDist::Zipf { alpha } => json!({ "kind": "zipf", "alpha": alpha }),
        StyleDist::Custom(weights) => json!({ "kind": "custom", "weights": weights }),
    }
}

fn source_provider_snapshot(provider: &SourceProvider) -> Value {
    match provider {
        SourceProvider::Shared {
            source_count,
            distribution,
        } => json!({
            "kind": "shared",
            "source_count": source_count,
            "distribution": style_dist_snapshot(distribution),
        }),
        SourceProvider::PeriodicRefresh {
            source_count,
            interval,
            jitter,
        } => json!({
            "kind": "periodic_refresh",
            "source_count": source_count,
            "interval_ms": millis(*interval),
            "jitter_ms": millis(*jitter),
        }),
        SourceProvider::OneShot => json!({ "kind": "one_shot" }),
        SourceProvider::Mixed(providers) => json!({
            "kind": "mixed",
            "providers": providers.iter().map(|(weight, provider)| json!({
                "weight": weight,
                "provider": source_provider_snapshot(provider),
            })).collect::<Vec<_>>(),
        }),
    }
}

fn millis(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::{REPORT_SCHEMA_VERSION, RunReport};
    use crate::{config::SimConfig, metrics::MetricsCollector};

    #[test]
    fn report_contains_schema_config_and_result() {
        let config = SimConfig::default();
        let result = MetricsCollector::new().report(config.costs.sla);
        let report = RunReport::new(&config, &result, None);
        let value = serde_json::to_value(report).expect("serialize report");

        assert_eq!(value["schema_version"], REPORT_SCHEMA_VERSION);
        assert_eq!(value["config"]["node_count"], config.node_count);
        assert_eq!(
            value["config"]["cpu_cores_per_node"],
            config.cpu_cores_per_node
        );
        assert_eq!(value["config"]["costs"]["render_cpu_ms"]["min"], 20.0);
        assert_eq!(value["config"]["costs"]["render_resource_ms"]["min"], 165.0);
        assert_eq!(
            value["config"]["costs"]["first_render_resource_ms"]["min"],
            480.0
        );
        assert_eq!(value["result"]["total"], 0);
        assert_eq!(
            value["config"]["cluster"]["effective_native_render_permits_per_node"],
            config.cluster.resolved_cpu_render_permits_per_node()
        );
        assert_eq!(
            value["result"]["latency_histogram"]
                .as_array()
                .expect("latency histogram")
                .len(),
            18
        );
    }
}
