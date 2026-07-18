//! Simulation-only configuration: workload generation, RNG seed, and the
//! top-level `SimConfig` aggregate. Production deployments don't need these
//! — they'd assemble `ClusterConfig` / `CostConfig` / `GossipConfig` /
//! `RoutingConfig` differently.

use std::time::Duration;

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

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            node_count: 2,
            cpu_cores_per_node: 16,
            cluster: ClusterConfig {
                renderer_slots_per_node: 16,
                render_permits_per_node: None,
                cpu_render_permits_per_node: None,
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
