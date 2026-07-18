//! Production-shared configuration types. These are useful both in the
//! simulator and in a real deployment that embeds the routing/worker code.
//! Simulation-only knobs (workload generation, RNG seed, gossip-backend
//! selection) live in `sims/biei-sim/src/config.rs`.

use std::time::Duration;

use rand::{Rng, RngExt};

#[derive(Clone, Copy, Debug)]
pub struct CostRange {
    pub min: Duration,
    pub max: Duration,
}

impl CostRange {
    pub const fn fixed(d: Duration) -> Self {
        Self { min: d, max: d }
    }

    pub const fn new(min: Duration, max: Duration) -> Self {
        Self { min, max }
    }

    pub fn sample<R: Rng>(&self, rng: &mut R) -> Duration {
        if self.min >= self.max {
            return self.min;
        }
        let min_us = self.min.as_micros() as u64;
        let max_us = self.max.as_micros() as u64;
        Duration::from_micros(rng.random_range(min_us..=max_us))
    }

    pub fn mid(&self) -> Duration {
        (self.min + self.max) / 2
    }

    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            min: self.min.saturating_add(other.min),
            max: self.max.saturating_add(other.max),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum BlCapacityPolicy {
    Fixed(usize),
    Auto,
}

#[derive(Clone, Copy, Debug)]
pub enum Tier1Strategy {
    WeightedRandom,
    PowerOfTwo,
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// Warm renderer slots per node. Each slot owns one renderer actor and can
    /// keep a WorkerProfile loaded even when it is not currently executing.
    pub renderer_slots_per_node: usize,
    /// Maximum concurrently executing renderer slots per node. `None` means
    /// all renderer slots may run at once. Values above `renderer_slots_per_node` are
    /// capped because extra permits cannot be used without slots.
    pub render_permits_per_node: Option<usize>,
    /// Optional native `renderStill` residency limit per node. The historical
    /// name is retained for compatibility, but the permit includes FileSource
    /// waits and is not a pure CPU/GPU service limit. `None` means the same
    /// value as `render_permits_per_node`.
    pub cpu_render_permits_per_node: Option<usize>,
    /// Policy for the per-slot soft queue limit. This is the BL used by
    /// routing to prefer targets likely to stay within SLA.
    pub bl_capacity: BlCapacityPolicy,
    /// Hard per-slot queue limit is `soft_limit * queue_capacity_multiplier`.
    /// The hard limit is the backpressure boundary: requests may overflow the
    /// soft limit to preserve service continuity, but must not exceed this.
    pub queue_capacity_multiplier: usize,
    /// Per-slot LRU source cache size.
    pub source_cache_capacity: usize,
    /// Node-local rendered image cache capacity in bytes. `0` disables it.
    pub render_output_cache_capacity_bytes: u64,
}

/// Resolved per-slot queue limits.
///
/// `soft` is the SLA-oriented routing threshold (BL). Crossing it is allowed
/// in overflow paths.
/// `hard` is the admission/backpressure cap. Crossing it rejects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueLimits {
    pub soft: usize,
    pub hard: usize,
}

#[derive(Clone, Debug)]
pub struct CostConfig {
    pub style_setup_cost: CostRange,
    /// Cost of loading one source datum (geometry parse / index build).
    /// Style application is folded into `render_cpu_cost`.
    pub source_load_cost: CostRange,
    /// CPU service demand for native render and encode work. This is distinct
    /// from wall-clock native-render residency: MapLibre can wait for network
    /// resources without consuming a CPU core.
    pub render_cpu_cost: CostRange,
    /// In-render resource critical-path wait for an already-warm profile.
    pub render_resource_cost: CostRange,
    /// In-render resource critical-path wait for the first render after a
    /// profile setup. This replaces, rather than adds to, the warm wait.
    pub first_render_resource_cost: CostRange,
    pub hop_latency: Duration,
    pub sla: Duration,
}

impl CostConfig {
    /// Wall-clock native-render residency for a warm profile.
    pub fn warm_render_cost(&self) -> CostRange {
        self.render_cpu_cost
            .saturating_add(self.render_resource_cost)
    }

    /// Wall-clock native-render residency for the first render after setup.
    pub fn first_render_cost(&self) -> CostRange {
        self.render_cpu_cost
            .saturating_add(self.first_render_resource_cost)
    }
}

#[derive(Clone, Debug)]
pub struct GossipConfig {
    pub publish_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct RoutingConfig {
    pub tier1_strategy: Tier1Strategy,
    pub tier3_enabled: bool,
    pub drain_max_queue: usize,
}

/// SLA-oriented soft queue limit per renderer slot: `min(S/P, L/P - 1)`.
/// This is the BL from the routing algorithm.
pub fn compute_bl_capacity(s: Duration, p: Duration, l: Duration) -> usize {
    let p_us = p.as_micros().max(1) as u64;
    let by_latency = (s.as_micros() as u64) / p_us;
    let by_sla = ((l.as_micros() as u64) / p_us).saturating_sub(1);
    by_latency.min(by_sla).max(1) as usize
}

impl ClusterConfig {
    pub const STANDBY_RATIO_ERROR: f64 = 1.5;

    pub fn resolved_render_permits_per_node(&self) -> usize {
        self.render_permits_per_node
            .unwrap_or(self.renderer_slots_per_node)
            .max(1)
            .min(self.renderer_slots_per_node.max(1))
    }

    pub fn resolved_cpu_render_permits_per_node(&self) -> usize {
        self.cpu_render_permits_per_node
            .unwrap_or_else(|| self.resolved_render_permits_per_node())
            .max(1)
            .min(self.resolved_render_permits_per_node())
    }

    pub fn standby_ratio(&self) -> f64 {
        self.renderer_slots_per_node as f64 / self.resolved_render_permits_per_node() as f64
    }

    pub fn validate_standby_ratio(&self) -> Result<(), String> {
        let ratio = self.standby_ratio();
        if ratio > Self::STANDBY_RATIO_ERROR {
            Err(format!(
                "renderer_slots/render_permits ratio is {ratio:.2}x; ratios above {:.1}x are rejected by the production guardrail",
                Self::STANDBY_RATIO_ERROR
            ))
        } else {
            Ok(())
        }
    }

    pub fn resolved_bl_capacity(&self, costs: &CostConfig) -> usize {
        match self.bl_capacity {
            BlCapacityPolicy::Fixed(n) => n,
            BlCapacityPolicy::Auto => compute_bl_capacity(
                costs.style_setup_cost.mid(),
                costs.warm_render_cost().mid(),
                costs.sla,
            ),
        }
    }

    pub fn resolved_queue_limits(&self, costs: &CostConfig) -> QueueLimits {
        let soft = self.resolved_bl_capacity(costs);
        let hard = soft
            .saturating_mul(self.queue_capacity_multiplier.max(1))
            .max(soft);
        QueueLimits { soft, hard }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn queue_capacity_is_bl_times_multiplier() {
        let cluster = ClusterConfig {
            renderer_slots_per_node: 1,
            render_permits_per_node: None,
            cpu_render_permits_per_node: None,
            bl_capacity: BlCapacityPolicy::Fixed(7),
            queue_capacity_multiplier: 3,
            source_cache_capacity: 1,
            render_output_cache_capacity_bytes: 0,
        };
        let costs = CostConfig {
            style_setup_cost: CostRange::fixed(Duration::from_millis(100)),
            source_load_cost: CostRange::fixed(Duration::ZERO),
            render_cpu_cost: CostRange::fixed(Duration::from_millis(10)),
            render_resource_cost: CostRange::fixed(Duration::ZERO),
            first_render_resource_cost: CostRange::fixed(Duration::ZERO),
            hop_latency: Duration::ZERO,
            sla: Duration::from_secs(1),
        };

        assert_eq!(
            cluster.resolved_queue_limits(&costs),
            QueueLimits { soft: 7, hard: 21 }
        );
    }

    #[test]
    fn auto_bl_uses_warm_wall_clock_residency_not_cpu_service_alone() {
        let cluster = ClusterConfig {
            renderer_slots_per_node: 1,
            render_permits_per_node: None,
            cpu_render_permits_per_node: None,
            bl_capacity: BlCapacityPolicy::Auto,
            queue_capacity_multiplier: 2,
            source_cache_capacity: 1,
            render_output_cache_capacity_bytes: 0,
        };
        let costs = CostConfig {
            style_setup_cost: CostRange::fixed(Duration::from_millis(250)),
            source_load_cost: CostRange::fixed(Duration::ZERO),
            render_cpu_cost: CostRange::fixed(Duration::from_millis(20)),
            render_resource_cost: CostRange::fixed(Duration::from_millis(165)),
            first_render_resource_cost: CostRange::fixed(Duration::from_millis(480)),
            hop_latency: Duration::ZERO,
            sla: Duration::from_secs(5),
        };

        // CPU-only P=20 ms would produce BL=12. Including the measured warm
        // resource critical path yields P_w=185 ms and the safe soft bound 1.
        assert_eq!(cluster.resolved_bl_capacity(&costs), 1);
        assert_eq!(
            cluster.resolved_queue_limits(&costs),
            QueueLimits { soft: 1, hard: 2 }
        );
    }

    #[test]
    fn render_permits_default_to_worker_slots_and_cap_at_slots() {
        let mut cluster = ClusterConfig {
            renderer_slots_per_node: 10,
            render_permits_per_node: None,
            cpu_render_permits_per_node: None,
            bl_capacity: BlCapacityPolicy::Fixed(1),
            queue_capacity_multiplier: 1,
            source_cache_capacity: 1,
            render_output_cache_capacity_bytes: 0,
        };

        assert_eq!(cluster.resolved_render_permits_per_node(), 10);
        assert_eq!(cluster.resolved_cpu_render_permits_per_node(), 10);
        assert_eq!(cluster.standby_ratio(), 1.0);
        assert!(cluster.validate_standby_ratio().is_ok());

        cluster.render_permits_per_node = Some(6);
        assert_eq!(cluster.resolved_render_permits_per_node(), 6);
        assert_eq!(cluster.resolved_cpu_render_permits_per_node(), 6);

        cluster.cpu_render_permits_per_node = Some(4);
        assert_eq!(cluster.resolved_cpu_render_permits_per_node(), 4);

        cluster.cpu_render_permits_per_node = Some(12);
        assert_eq!(cluster.resolved_cpu_render_permits_per_node(), 6);

        assert!(cluster.validate_standby_ratio().is_err());

        cluster.render_permits_per_node = Some(12);
        assert_eq!(cluster.resolved_render_permits_per_node(), 10);
        assert_eq!(cluster.resolved_cpu_render_permits_per_node(), 10);
        assert!(cluster.validate_standby_ratio().is_ok());
    }
}
