//! Reusable simulation scenarios. Each public function returns a fully-
//! configured `SimConfig` for a specific verification or sweep point.
//! `main.rs` only drives them and formats results.

use std::time::Duration;

use crate::config::{SimConfig, SourcePattern, SourceProvider, StyleDist, StyleShift};
use biei::config::BlCapacityPolicy;

// ---------------------------------------------------------------------------
// Shared baselines
// ---------------------------------------------------------------------------

/// Production-sized cluster used by sanity, chitchat, and source-pattern
/// scenarios: 4 nodes × 16 renderer slots, 15 styles Zipf 1.2, 1000 req/s, 30s.
pub fn production_base() -> SimConfig {
    let mut cfg = SimConfig {
        node_count: 4,
        ..Default::default()
    };
    cfg.cluster.renderer_slots_per_node = 16;
    cfg.workload.style_count = 15;
    cfg.workload.style_distribution = StyleDist::Zipf { alpha: 1.2 };
    cfg.workload.new_style_rate = 0.0;
    cfg.workload.total_rate = 1000.0;
    cfg.workload.duration = Duration::from_secs(30);
    cfg
}

// ---------------------------------------------------------------------------
// Verification scenarios
// ---------------------------------------------------------------------------

/// Production-realistic load with no overload expected (reject = 0, SLA clean).
pub fn production_sanity() -> SimConfig {
    let mut cfg = production_base();
    cfg.workload.source_pattern = None;
    cfg
}

/// Forces Tier 3 (drain-and-swap): tight BL, saturating rate on a small
/// cluster.
pub fn tier3_drain_swap() -> SimConfig {
    let mut cfg = SimConfig {
        node_count: 1,
        ..Default::default()
    };
    cfg.cluster.renderer_slots_per_node = 3;
    cfg.cluster.bl_capacity = BlCapacityPolicy::Fixed(2);
    cfg.workload.style_count = 5;
    cfg.workload.style_distribution = StyleDist::Zipf { alpha: 1.0 };
    cfg.workload.new_style_rate = 0.0;
    cfg.workload.total_rate = 25.0;
    cfg.workload.duration = Duration::from_secs(15);
    cfg.workload.source_pattern = None;
    cfg
}

/// Under-provisioned cluster (active hot-style count exceeds worker count) so
/// Tier 4 fires; expect high reject but completed tasks within SLA.
pub fn tier4_underprovisioned() -> SimConfig {
    let mut cfg = SimConfig {
        node_count: 1,
        ..Default::default()
    };
    cfg.cluster.renderer_slots_per_node = 4;
    cfg.cluster.bl_capacity = BlCapacityPolicy::Fixed(2);
    cfg.workload.style_count = 12;
    cfg.workload.style_distribution = StyleDist::Uniform;
    cfg.workload.new_style_rate = 0.0;
    cfg.workload.total_rate = 80.0;
    cfg.workload.duration = Duration::from_secs(15);
    cfg.workload.source_pattern = None;
    cfg
}

// ---------------------------------------------------------------------------
// Source pattern sweep
// ---------------------------------------------------------------------------

/// Labelled source-pattern variants exercised by the source-pattern sweep.
pub fn source_pattern_scenarios() -> Vec<(&'static str, Option<SourcePattern>)> {
    vec![
        ("None (baseline)", None),
        (
            "Shared(20, Zipf 0.8) — high reuse",
            Some(source_patterns::shared_high_reuse()),
        ),
        (
            "Shared(100, Uniform) — low reuse",
            Some(source_patterns::shared_low_reuse()),
        ),
        (
            "OneShot — no cache benefit",
            Some(source_patterns::one_shot()),
        ),
        (
            "PeriodicRefresh(1, 2s) — periodic spikes",
            Some(source_patterns::periodic_refresh_demo()),
        ),
        (
            "Mixed services: 50% Shared / 30% Periodic / 20% OneShot",
            Some(source_patterns::mixed_services()),
        ),
        (
            "JP map: 20% w/ addlayer (60% 行政界 / 40% 雨雲)",
            Some(source_patterns::jp_map()),
        ),
    ]
}

/// `production_base` with the supplied source pattern applied.
pub fn with_source_pattern(pattern: Option<SourcePattern>) -> SimConfig {
    let mut cfg = production_base();
    cfg.workload.source_pattern = pattern;
    cfg
}

pub mod source_patterns {
    use super::*;

    pub fn shared_high_reuse() -> SourcePattern {
        SourcePattern {
            probability: 0.5,
            provider: SourceProvider::Shared {
                source_count: 20,
                distribution: StyleDist::Zipf { alpha: 0.8 },
            },
        }
    }

    pub fn shared_low_reuse() -> SourcePattern {
        SourcePattern {
            probability: 0.5,
            provider: SourceProvider::Shared {
                source_count: 100,
                distribution: StyleDist::Uniform,
            },
        }
    }

    pub fn one_shot() -> SourcePattern {
        SourcePattern {
            probability: 0.5,
            provider: SourceProvider::OneShot,
        }
    }

    pub fn periodic_refresh_demo() -> SourcePattern {
        SourcePattern {
            probability: 0.5,
            provider: SourceProvider::PeriodicRefresh {
                source_count: 1,
                interval: Duration::from_secs(2),
                jitter: Duration::from_millis(200),
            },
        }
    }

    pub fn mixed_services() -> SourcePattern {
        SourcePattern {
            probability: 0.6,
            provider: SourceProvider::Mixed(vec![
                (
                    0.5,
                    Box::new(SourceProvider::Shared {
                        source_count: 20,
                        distribution: StyleDist::Zipf { alpha: 1.0 },
                    }),
                ),
                (
                    0.3,
                    Box::new(SourceProvider::PeriodicRefresh {
                        source_count: 1,
                        interval: Duration::from_secs(5),
                        jitter: Duration::from_millis(500),
                    }),
                ),
                (0.2, Box::new(SourceProvider::OneShot)),
            ]),
        }
    }

    /// Realistic Japan map service: ~20% of requests carry an addlayer.
    /// Of those, 60% are 行政界 (3 types: 市区町村 / 都道府県 / 国, fairly
    /// even distribution) and 40% are 雨雲 (single source, refreshed every
    /// 5 minutes).
    pub fn jp_map() -> SourcePattern {
        SourcePattern {
            probability: 0.2,
            provider: SourceProvider::Mixed(vec![
                (
                    0.6,
                    Box::new(SourceProvider::Shared {
                        source_count: 3,
                        distribution: StyleDist::Zipf { alpha: 0.5 },
                    }),
                ),
                (
                    0.4,
                    Box::new(SourceProvider::PeriodicRefresh {
                        source_count: 1,
                        interval: Duration::from_secs(300),
                        jitter: Duration::from_secs(30),
                    }),
                ),
            ]),
        }
    }
}

// ---------------------------------------------------------------------------
// Parameter sweeps
// ---------------------------------------------------------------------------

/// BL capacity × Zipf α sweep. Loaded to ~75% utilisation so BL becomes
/// the binding constraint — otherwise tight/loose BL all look equally fine.
pub fn bl_alpha_sweep(bl: usize, alpha: f64) -> SimConfig {
    let mut cfg = SimConfig::default();
    cfg.cluster.bl_capacity = BlCapacityPolicy::Fixed(bl);
    cfg.workload.style_distribution = StyleDist::Zipf { alpha };
    cfg.workload.new_style_rate = 0.0;
    cfg.workload.source_pattern = None;
    // ~22.5k req/s/slot × 32 slots × 0.0175ms = 1828 req/s cap.
    // 1400 req/s ≈ 77% util — high enough that BL bounds bite.
    cfg.workload.total_rate = 1400.0;
    cfg.workload.duration = Duration::from_secs(15);
    cfg
}

/// Cluster sizing sweep: 10 styles Zipf 1.2 at 1000 req/s, vary renderer slot count.
pub fn cluster_sizing_sweep(node_count: usize, renderer_slots_per_node: usize) -> SimConfig {
    let mut cfg = SimConfig {
        node_count,
        ..Default::default()
    };
    cfg.cluster.renderer_slots_per_node = renderer_slots_per_node;
    cfg.workload.style_count = 10;
    cfg.workload.style_distribution = StyleDist::Zipf { alpha: 1.2 };
    cfg.workload.new_style_rate = 0.0;
    cfg.workload.total_rate = 1000.0;
    cfg.workload.source_pattern = None;
    cfg.workload.duration = Duration::from_secs(15);
    cfg
}

/// Style distribution sweep: vary `StyleDist` at 100 req/s for 20s.
pub fn style_dist_sweep(dist: StyleDist) -> SimConfig {
    let mut cfg = SimConfig::default();
    cfg.workload.total_rate = 100.0;
    cfg.workload.new_style_rate = 0.0;
    cfg.workload.style_distribution = dist;
    cfg.workload.duration = Duration::from_secs(20);
    cfg
}

// ---------------------------------------------------------------------------
// Production sizing: large-scale 16-core nodes
// ---------------------------------------------------------------------------

/// Large-scale production layout:
///   - 25 nodes × 20 warm renderer slots = 500 slots total
///   - 16 render permits/node = 400 concurrent renders total (16-core VMs)
///   - 16 base styles (主要 8 + サブ 8, modelled with Zipf 1.2 skew)
///   - JP map source pattern (20% addlayer w/ 行政界 + 雨雲)
///
/// `total_rate` sweeps utilisation; `publish_interval` trades gossip overhead
/// against Tier 1 freshness.
pub fn large_scale_16cores(total_rate: f64, publish_interval: Duration) -> SimConfig {
    let mut cfg = SimConfig {
        node_count: 25,
        ..Default::default()
    };
    cfg.cluster.renderer_slots_per_node = 20;
    cfg.cluster.render_permits_per_node = Some(16);
    cfg.cluster.queue_capacity_multiplier = 4;
    cfg.workload.style_count = 16;
    cfg.workload.tile_style_count = 2;
    cfg.workload.style_distribution = StyleDist::Zipf { alpha: 1.2 };
    cfg.workload.new_style_rate = 0.0;
    cfg.workload.total_rate = total_rate;
    cfg.workload.duration = Duration::from_secs(8);
    cfg.workload.source_pattern = Some(source_patterns::jp_map());
    cfg.gossip.publish_interval = publish_interval;
    cfg
}

/// `large_scale_16cores` with fixed 16 render permits/node but a variable
/// number of warm renderer slots/node. Used to measure standby slot ratios.
pub fn large_scale_16cores_with_slots(
    total_rate: f64,
    publish_interval: Duration,
    renderer_slots_per_node: usize,
) -> SimConfig {
    let mut cfg = large_scale_16cores(total_rate, publish_interval);
    cfg.cluster.renderer_slots_per_node = renderer_slots_per_node;
    cfg.cluster.render_permits_per_node = Some(16);
    cfg
}

/// `large_scale_16cores` with fixed warm slots and CPU render permits, but a
/// variable task execution permit count. Used to test I/O overlap above core
/// count without increasing render/encode parallelism.
pub fn large_scale_16cores_with_execution_permits(
    total_rate: f64,
    publish_interval: Duration,
    renderer_slots_per_node: usize,
    render_permits_per_node: usize,
    cpu_render_permits_per_node: usize,
) -> SimConfig {
    let mut cfg = large_scale_16cores(total_rate, publish_interval);
    cfg.cluster.renderer_slots_per_node = renderer_slots_per_node;
    cfg.cluster.render_permits_per_node = Some(render_permits_per_node);
    cfg.cluster.cpu_render_permits_per_node = Some(cpu_render_permits_per_node);
    cfg
}

/// `large_scale_16cores` with an explicit overflow-band multiplier. Used by
/// the multiplier sweep to compare 2×/4×/8× admission limits.
pub fn large_scale_16cores_with_multiplier(
    total_rate: f64,
    publish_interval: Duration,
    queue_capacity_multiplier: usize,
) -> SimConfig {
    let mut cfg = large_scale_16cores(total_rate, publish_interval);
    cfg.cluster.queue_capacity_multiplier = queue_capacity_multiplier;
    cfg
}

/// `large_scale_16cores` with a mid-sim top-style swap. Extends duration so
/// metrics have a meaningful post-shift window after warmup exclusion.
pub fn large_scale_style_shift(
    total_rate: f64,
    publish_interval: Duration,
    shift_at: Duration,
    swap_with: u32,
) -> SimConfig {
    let mut cfg = large_scale_16cores(total_rate, publish_interval);
    cfg.workload.duration = Duration::from_secs(10);
    cfg.workload.style_shift = Some(StyleShift {
        at: shift_at,
        with: swap_with,
    });
    cfg
}
