//! Simulation-only configuration: workload generation, RNG seed, and the
//! top-level `SimConfig` aggregate. Production deployments don't need these
//! — they'd assemble `ClusterConfig` / `CostConfig` / `GossipConfig` /
//! `RoutingConfig` differently.

use std::time::Duration;

use anyhow::{Result, ensure};
use biei_core::config::{
    BlCapacityPolicy, ClusterConfig, CostConfig, CostRange, GossipConfig, RoutingConfig,
    Tier1Strategy,
};
#[derive(Clone, Debug)]
pub enum StyleDist {
    Uniform,
    Zipf { alpha: f64 },
    Custom(Vec<f64>),
}

#[derive(Clone, Debug)]
pub struct BurstPattern {
    pub period: Duration,
    pub duration: Duration,
    pub multiplier: f64,
    pub style_focus: Option<u32>,
}

/// Source generation for tasks. The static image API allows at most one
/// `addlayer` per request, so each task carries at most one source. The
/// `probability` decides whether this request has an addlayer at all; the
/// `provider` decides which addlayer pool it's drawn from (use `Mixed` for
/// "one API used by multiple services").
#[derive(Clone, Debug)]
pub struct SourcePattern {
    /// Fraction of tasks that carry an addlayer source.
    pub probability: f64,
    pub provider: SourceProvider,
}

#[derive(Clone, Debug)]
pub enum SourceProvider {
    /// Shared source pool of fixed size; hash stable per index.
    /// CachePolicy::Cacheable.
    Shared {
        source_count: usize,
        distribution: StyleDist,
    },
    /// Periodically-refreshed shared source (e.g. weather radar). Hash
    /// changes every `interval ± jitter`. CachePolicy::Cacheable.
    PeriodicRefresh {
        source_count: usize,
        interval: Duration,
        jitter: Duration,
    },
    /// Unique per task — never reuse. CachePolicy::OneShot.
    OneShot,
    /// Weighted choice over sub-providers. Models a single API endpoint
    /// serving multiple services, each with its own addlayer pattern.
    Mixed(Vec<(f64, Box<SourceProvider>)>),
}

#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub duration: Duration,
    pub total_rate: f64,
    pub style_count: usize,
    pub style_distribution: StyleDist,
    pub new_style_rate: f64,
    pub burst_pattern: Option<BurstPattern>,
    pub source_pattern: Option<SourcePattern>,
    /// Lead-in period whose tasks generate load but are excluded from the
    /// aggregated `Report`. Models the fact that production traffic ramps
    /// up against an already-warm cluster — measuring the cold-start
    /// transient as steady-state behaviour is misleading.
    pub warmup: Duration,
    /// One-time mid-sim style distribution shift. Models viral / breaking-
    /// news style switches where what was the top style becomes cold and a
    /// previously-mid style suddenly dominates. `None` keeps the
    /// distribution stable.
    pub style_shift: Option<StyleShift>,
    /// Number of low-numbered styles that generate tile-mode requests.
    /// Remaining styles generate static image requests. Simulator traffic
    /// uses @2x only, so routing separation is style + Static/Tile + @2x.
    pub tile_style_count: usize,
}

/// At `start + at`, swap the workload's rank-0 (top) style with `with` so
/// the cluster has to migrate warm workers between two styles mid-run.
#[derive(Clone, Debug)]
pub struct StyleShift {
    pub at: Duration,
    pub with: u32,
}

/// Top-level simulator configuration. Aggregates the production-shared
/// configuration blocks plus simulation-only knobs.
#[derive(Clone, Debug)]
pub struct SimConfig {
    /// Number of synthetic nodes instantiated by the simulator. Production
    /// discovers peers from membership/gossip and does not use this.
    pub node_count: usize,
    /// Actual CPU service capacity per node. Native-render concurrency is a
    /// separate cluster setting because resource waits do not consume a core.
    pub cpu_cores_per_node: usize,
    pub cluster: ClusterConfig,
    pub costs: CostConfig,
    pub workload: WorkloadConfig,
    pub gossip: GossipConfig,
    pub routing: RoutingConfig,
    pub seed: u64,
}

impl SimConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.node_count > 0, "simulation needs at least one node");
        ensure!(
            self.cpu_cores_per_node > 0,
            "cpu_cores_per_node must be greater than zero"
        );
        ensure!(
            self.cluster.renderer_slots_per_node > 0,
            "renderer_slots_per_node must be greater than zero"
        );
        ensure!(
            self.cluster.queue_capacity_multiplier > 0,
            "queue_capacity_multiplier must be greater than zero"
        );
        if let Some(permits) = self.cluster.render_permits_per_node {
            ensure!(
                permits > 0,
                "render_permits_per_node must be greater than zero"
            );
        }
        if let Some(permits) = self.cluster.native_render_permits_per_node {
            ensure!(
                permits > 0,
                "native_render_permits_per_node must be greater than zero"
            );
        }
        self.cluster
            .validate_standby_ratio()
            .map_err(anyhow::Error::msg)?;

        ensure!(
            self.workload.total_rate.is_finite() && self.workload.total_rate >= 0.0,
            "workload total_rate must be finite and non-negative"
        );
        ensure!(
            self.workload.new_style_rate.is_finite() && self.workload.new_style_rate >= 0.0,
            "workload new_style_rate must be finite and non-negative"
        );
        ensure!(
            self.workload.style_count > 0,
            "workload style_count must be greater than zero"
        );
        ensure!(
            self.workload.tile_style_count <= self.workload.style_count,
            "workload tile_style_count must not exceed style_count"
        );
        ensure!(
            self.workload.warmup <= self.workload.duration,
            "workload warmup must not exceed duration"
        );
        validate_style_distribution(&self.workload.style_distribution, "workload style")?;

        if let Some(burst) = &self.workload.burst_pattern {
            ensure!(!burst.period.is_zero(), "burst period must be non-zero");
            ensure!(
                burst.duration <= burst.period,
                "burst duration must not exceed its period"
            );
            ensure!(
                burst.multiplier.is_finite() && burst.multiplier >= 0.0,
                "burst multiplier must be finite and non-negative"
            );
        }
        if let Some(pattern) = &self.workload.source_pattern {
            ensure!(
                pattern.probability.is_finite() && (0.0..=1.0).contains(&pattern.probability),
                "source probability must be finite and in [0, 1]"
            );
            validate_source_provider(&pattern.provider, "source provider")?;
        }

        validate_cost_range("style_setup_cost", self.costs.style_setup_cost)?;
        validate_cost_range("source_load_cost", self.costs.source_load_cost)?;
        validate_cost_range("render_cpu_cost", self.costs.render_cpu_cost)?;
        validate_cost_range("render_resource_cost", self.costs.render_resource_cost)?;
        validate_cost_range(
            "first_render_resource_cost",
            self.costs.first_render_resource_cost,
        )?;
        ensure!(!self.costs.sla.is_zero(), "SLA must be non-zero");
        ensure!(
            !self.gossip.publish_interval.is_zero(),
            "gossip publish_interval must be non-zero"
        );
        Ok(())
    }
}

fn validate_style_distribution(distribution: &StyleDist, label: &str) -> Result<()> {
    match distribution {
        StyleDist::Uniform => Ok(()),
        StyleDist::Zipf { alpha } => {
            ensure!(
                alpha.is_finite() && *alpha > 0.0,
                "{label} Zipf alpha must be finite and greater than zero"
            );
            Ok(())
        }
        StyleDist::Custom(weights) => validate_weights(weights, label),
    }
}

fn validate_source_provider(provider: &SourceProvider, label: &str) -> Result<()> {
    match provider {
        SourceProvider::Shared {
            source_count,
            distribution,
        } => {
            ensure!(
                *source_count > 0,
                "{label} shared source_count must be greater than zero"
            );
            validate_style_distribution(distribution, label)
        }
        SourceProvider::PeriodicRefresh {
            source_count,
            interval,
            ..
        } => {
            ensure!(
                *source_count > 0,
                "{label} periodic source_count must be greater than zero"
            );
            ensure!(
                !interval.is_zero(),
                "{label} periodic interval must be non-zero"
            );
            Ok(())
        }
        SourceProvider::OneShot => Ok(()),
        SourceProvider::Mixed(choices) => {
            ensure!(
                !choices.is_empty(),
                "{label} mixed choices must not be empty"
            );
            let weights: Vec<_> = choices.iter().map(|(weight, _)| *weight).collect();
            validate_weights(&weights, label)?;
            for (index, (_, child)) in choices.iter().enumerate() {
                validate_source_provider(child, &format!("{label} choice {index}"))?;
            }
            Ok(())
        }
    }
}

fn validate_weights(weights: &[f64], label: &str) -> Result<()> {
    ensure!(!weights.is_empty(), "{label} weights must not be empty");
    ensure!(
        weights
            .iter()
            .all(|weight| weight.is_finite() && *weight >= 0.0),
        "{label} weights must be finite and non-negative"
    );
    ensure!(
        weights.iter().any(|weight| *weight > 0.0),
        "{label} weights must contain a positive value"
    );
    Ok(())
}

fn validate_cost_range(label: &str, range: CostRange) -> Result<()> {
    ensure!(
        range.min <= range.max,
        "{label} minimum must not exceed maximum"
    );
    Ok(())
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            node_count: 2,
            cpu_cores_per_node: 16,
            cluster: ClusterConfig {
                renderer_slots_per_node: 16,
                render_permits_per_node: None,
                native_render_permits_per_node: None,
                bl_capacity: BlCapacityPolicy::Auto,
                queue_capacity_multiplier: 4,
                source_cache_capacity: 32,
                render_output_cache_capacity_bytes: 0,
            },
            costs: CostConfig {
                style_setup_cost: CostRange::new(
                    Duration::from_millis(200),
                    Duration::from_millis(300),
                ),
                source_load_cost: CostRange::new(
                    Duration::from_millis(30),
                    Duration::from_millis(70),
                ),
                // Initial observed point values: ~20 ms CPU, ~165 ms warm
                // in-render resource wait, and ~480 ms first-render wait. They
                // are not distributions or sizing evidence; M12 replaces them
                // with a provenance-bearing production profile.
                render_cpu_cost: CostRange::fixed(Duration::from_millis(20)),
                render_resource_cost: CostRange::fixed(Duration::from_millis(165)),
                first_render_resource_cost: CostRange::fixed(Duration::from_millis(480)),
                hop_latency: Duration::from_millis(5),
                sla: Duration::from_millis(1000),
            },
            workload: WorkloadConfig {
                duration: Duration::from_secs(30),
                total_rate: 100.0,
                style_count: 15,
                style_distribution: StyleDist::Zipf { alpha: 1.2 },
                new_style_rate: 0.01,
                burst_pattern: None,
                source_pattern: Some(SourcePattern {
                    probability: 0.3,
                    provider: SourceProvider::Shared {
                        source_count: 20,
                        distribution: StyleDist::Zipf { alpha: 0.8 },
                    },
                }),
                warmup: Duration::from_secs(2),
                style_shift: None,
                tile_style_count: 2,
            },
            gossip: GossipConfig {
                publish_interval: Duration::from_millis(50),
            },
            routing: RoutingConfig {
                tier1_strategy: Tier1Strategy::PowerOfTwo,
                tier3_enabled: true,
                drain_max_queue: 10,
            },
            seed: 0xDEAD_BEEF,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured(update: impl FnOnce(&mut SimConfig)) -> SimConfig {
        let mut config = SimConfig::default();
        update(&mut config);
        config
    }

    #[test]
    fn default_configuration_is_valid() {
        SimConfig::default().validate().expect("default config");
    }

    #[test]
    fn rejects_values_that_would_be_silently_normalized() {
        let config = configured(|config| config.cpu_cores_per_node = 0);
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("cpu_cores_per_node")
        );

        let config = configured(|config| config.cluster.render_permits_per_node = Some(0));
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("render_permits_per_node")
        );

        let config = configured(|config| config.workload.style_count = 0);
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("style_count")
        );
    }

    #[test]
    fn rejects_invalid_stochastic_inputs_before_sampling() {
        let config = configured(|config| config.workload.total_rate = f64::NAN);
        assert!(config.validate().is_err());

        let config = configured(|config| {
            config.workload.style_distribution = StyleDist::Zipf { alpha: 0.0 };
        });
        assert!(config.validate().is_err());

        let config = configured(|config| {
            config.workload.style_distribution = StyleDist::Custom(vec![0.0, 0.0]);
        });
        assert!(config.validate().is_err());

        let config = configured(|config| {
            config.workload.source_pattern = Some(SourcePattern {
                probability: 1.1,
                provider: SourceProvider::OneShot,
            });
        });
        assert!(config.validate().is_err());

        let config = configured(|config| {
            config.workload.source_pattern = Some(SourcePattern {
                probability: 1.0,
                provider: SourceProvider::Mixed(vec![(
                    f64::NAN,
                    Box::new(SourceProvider::OneShot),
                )]),
            });
        });
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_incoherent_timing_and_cost_ranges() {
        let config = configured(|config| {
            config.workload.warmup = config.workload.duration + Duration::from_secs(1);
        });
        assert!(config.validate().is_err());

        let config = configured(|config| {
            config.workload.burst_pattern = Some(BurstPattern {
                period: Duration::from_secs(1),
                duration: Duration::from_secs(2),
                multiplier: 2.0,
                style_focus: None,
            });
        });
        assert!(config.validate().is_err());

        let config = configured(|config| {
            config.costs.render_cpu_cost =
                CostRange::new(Duration::from_millis(2), Duration::from_millis(1));
        });
        assert!(config.validate().is_err());
    }
}
