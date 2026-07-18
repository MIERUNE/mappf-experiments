//! Opt-in legacy production sweeps used by the no-subcommand CLI mode.

use std::time::Duration;

use biei_core::types::RouteTier;
use biei_sim::{Simulation, scenarios};

#[derive(Debug, Default, Eq, PartialEq)]
struct SweepSelection {
    standby: bool,
    execution: bool,
    steady: bool,
    multiplier: bool,
    style_shift: bool,
}

impl SweepSelection {
    fn from_lookup(mut is_set: impl FnMut(&str) -> bool) -> Self {
        Self {
            standby: is_set("RUN_STANDBY_SWEEP"),
            execution: is_set("RUN_EXECUTION_SWEEP"),
            steady: is_set("RUN_STEADY_SWEEP"),
            multiplier: is_set("RUN_MULTIPLIER_SWEEP"),
            style_shift: is_set("RUN_STYLE_SHIFT"),
        }
    }

    fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var_os(name).is_some())
    }
}

pub(super) async fn run_selected() {
    run_large_scale_sweep().await;

    let selection = SweepSelection::from_env();
    if selection.standby {
        run_standby_sweep().await;
    }
    if selection.execution {
        run_execution_sweep().await;
    }
    if selection.steady {
        run_steady_sweep().await;
    }
    if selection.multiplier {
        run_multiplier_sweep().await;
    }
    if selection.style_shift {
        run_style_shift_scenario().await;
    }
}

async fn run_large_scale_sweep() {
    let full = std::env::var_os("RUN_LARGE_SCALE_FULL").is_some();
    println!(
        "\n--- Large-scale production{}: 25 nodes × 20 slots / 16 permits (500 warm, 400 active) ---",
        if full { "" } else { " smoke" }
    );
    println!("16 styles Zipf 1.2, @2x only, 2 tile-mode styles, JP map sources (20% addlayer)");
    println!(
        "{:<32} | submitted | reject | util  | ovrfl% | tier1   | swaps  | src hit  | p50    p90    p99    max",
        "scenario"
    );
    println!("{}", "-".repeat(140));
    // Capacity is now the minimum of native residency and CPU service; the
    // old CPU-only estimate is intentionally not used for sizing.
    let large_scenarios: Vec<(&str, f64, Duration)> = if full {
        vec![
            ("5k req/s", 5_000.0, Duration::from_millis(50)),
            ("10k req/s", 10_000.0, Duration::from_millis(50)),
            ("15k req/s", 15_000.0, Duration::from_millis(50)),
            ("20k req/s", 20_000.0, Duration::from_millis(50)),
        ]
    } else {
        vec![
            ("3k req/s, 1s smoke", 3_000.0, Duration::from_millis(100)),
            ("8k req/s, 1s smoke", 8_000.0, Duration::from_millis(50)),
        ]
    };
    for (label, rate, pub_iv) in large_scenarios {
        let mut cfg = scenarios::large_scale_16cores(rate, pub_iv);
        if !full {
            cfg.workload.duration = Duration::from_secs(1);
            cfg.workload.warmup = Duration::ZERO;
        }
        let r = Simulation::new(cfg).run().await;
        let pct = |n: usize| n as f64 / r.total.max(1) as f64 * 100.0;
        let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
        let hit = if r.tasks_with_sources > 0 {
            format!(
                "{:5.1}%",
                r.source_hits as f64 / r.tasks_with_sources as f64 * 100.0
            )
        } else {
            String::from("  n/a")
        };
        let pct_complete = |n: usize| n as f64 / r.completed.max(1) as f64 * 100.0;
        println!(
            "{:<32} | {:>9} | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>8} | {:>5?} {:>5?} {:>6?} {:>6?}",
            label,
            r.total,
            pct(r.rejected),
            r.native_render_utilization_pct,
            pct_complete(r.overflow_admissions),
            pct(tier(RouteTier::Tier1WarmTracking)),
            pct(r.style_swaps),
            hit,
            r.latency_p50,
            r.latency_p90,
            r.latency_p99,
            r.latency_max,
        );
    }
}

/// Longer steady-state point for methodology checks. This is intentionally
/// opt-in so the default large-scale smoke remains fast.
async fn run_steady_sweep() {
    println!("\n--- Steady large-scale point (10k req/s, 30s duration, 5s warmup) ---");
    println!(
        "{:<22} | submitted | reject | util  | ovrfl% | tier1   | swaps  | p50     p99      max",
        "scenario"
    );
    println!("{}", "-".repeat(112));
    let mut cfg = scenarios::large_scale_16cores(10_000.0, Duration::from_millis(50));
    cfg.workload.duration = Duration::from_secs(30);
    cfg.workload.warmup = Duration::from_secs(5);
    let r = Simulation::new(cfg).run().await;
    let pct = |n: usize| n as f64 / r.total.max(1) as f64 * 100.0;
    let pct_complete = |n: usize| n as f64 / r.completed.max(1) as f64 * 100.0;
    let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
    println!(
        "{:<22} | {:>9} | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5?} {:>6?} {:>6?}",
        "10k req/s",
        r.total,
        pct(r.rejected),
        r.native_render_utilization_pct,
        pct_complete(r.overflow_admissions),
        pct(tier(RouteTier::Tier1WarmTracking)),
        pct(r.style_swaps),
        r.latency_p50,
        r.latency_p99,
        r.latency_max,
    );
}

/// Sweep warm renderer slot count while keeping render permits fixed. This
/// isolates whether standby slots improve warm coverage enough to justify
/// their memory cost.
async fn run_standby_sweep() {
    println!("\n--- Standby renderer slot sweep (large-scale 10k req/s) ---");
    println!("25 nodes, 16 render permits/node fixed. Vary warm renderer slots/node.");
    println!(
        "{:<14} | submitted | reject | util  | ovrfl% | tier1   | swaps  | p99      max",
        "slots/node"
    );
    println!("{}", "-".repeat(112));

    let mut results = Vec::new();
    for &slots in &[16usize, 20, 24, 32] {
        let cfg =
            scenarios::large_scale_16cores_with_slots(10_000.0, Duration::from_millis(50), slots);
        let r = Simulation::new(cfg).run().await;
        let pct = |n: usize| n as f64 / r.total.max(1) as f64 * 100.0;
        let pct_complete = |n: usize| n as f64 / r.completed.max(1) as f64 * 100.0;
        let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
        let label = format!("{} ({:.2}x)", slots, slots as f64 / 16.0);
        println!(
            "{:<14} | {:>9} | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>7?} {:>7?}",
            label,
            r.total,
            pct(r.rejected),
            r.native_render_utilization_pct,
            pct_complete(r.overflow_admissions),
            pct(tier(RouteTier::Tier1WarmTracking)),
            pct(r.style_swaps),
            r.latency_p99,
            r.latency_max,
        );
        results.push((label, r));
    }

    biei_sim::sweep::write_csv("sweep_standby_slots.csv", &results).expect("csv write");
    println!(
        "→ written: sweep_standby_slots.csv ({} rows)",
        results.len()
    );
}

/// Sweep task execution permits above the native-render residency limit. CPU
/// service is independently capped by the simulated core semaphore.
async fn run_execution_sweep() {
    println!("\n--- Execution permit sweep (large-scale 10k req/s) ---");
    println!("25 nodes, 32 warm slots/node, 16 native render permits/node fixed.");
    println!(
        "{:<14} | submitted | reject | util  | ovrfl% | tier1   | swaps  | p99      max",
        "exec/node"
    );
    println!("{}", "-".repeat(112));

    let mut results = Vec::new();
    for &execution_permits in &[16usize, 20, 24, 32] {
        let cfg = scenarios::large_scale_16cores_with_execution_permits(
            10_000.0,
            Duration::from_millis(50),
            32,
            execution_permits,
            16,
        );
        let r = Simulation::new(cfg).run().await;
        let pct = |n: usize| n as f64 / r.total.max(1) as f64 * 100.0;
        let pct_complete = |n: usize| n as f64 / r.completed.max(1) as f64 * 100.0;
        let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
        let label = format!(
            "{} ({:.2}x)",
            execution_permits,
            execution_permits as f64 / 16.0
        );
        println!(
            "{:<14} | {:>9} | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>7?} {:>7?}",
            label,
            r.total,
            pct(r.rejected),
            r.native_render_utilization_pct,
            pct_complete(r.overflow_admissions),
            pct(tier(RouteTier::Tier1WarmTracking)),
            pct(r.style_swaps),
            r.latency_p99,
            r.latency_max,
        );
        results.push((label, r));
    }

    biei_sim::sweep::write_csv("sweep_execution_permits.csv", &results).expect("csv write");
    println!(
        "→ written: sweep_execution_permits.csv ({} rows)",
        results.len()
    );
}

/// Inject a mid-sim top-style swap and observe how the cluster re-balances:
/// the previously-top style stops receiving traffic and a previously mid-rank
/// style suddenly dominates. Reveals how aggressively the system can
/// re-warm renderer slots without rejecting.
async fn run_style_shift_scenario() {
    println!("\n--- Style-shift scenario (large-scale, mid-sim top-style swap) ---");
    println!("Pre-shift: style 0 is top (Zipf 1.2). At t=4s, style 0 ↔ style 8 swap.");
    println!(
        "{:<22} | submitted | reject | ovrfl% | tier1   | swaps  | p50    p99      max",
        "rate"
    );
    println!("{}", "-".repeat(110));
    for &rate in &[5_000.0_f64, 10_000.0] {
        let cfg = scenarios::large_scale_style_shift(
            rate,
            Duration::from_millis(50),
            Duration::from_secs(4),
            8,
        );
        let r = Simulation::new(cfg).run().await;
        let pct = |n: usize| n as f64 / r.total.max(1) as f64 * 100.0;
        let pct_complete = |n: usize| n as f64 / r.completed.max(1) as f64 * 100.0;
        let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
        println!(
            "{:<22} | {:>9} | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5?} {:>6?} {:>6?}",
            format!("{:.0}k req/s", rate / 1_000.0),
            r.total,
            pct(r.rejected),
            pct_complete(r.overflow_admissions),
            pct(tier(RouteTier::Tier1WarmTracking)),
            pct(r.style_swaps),
            r.latency_p50,
            r.latency_p99,
            r.latency_max,
        );
    }
}

/// Sweep the overflow-band multiplier (hard queue limit / soft queue limit) at a
/// fixed production scale and rate. Shows the trade-off: tight multipliers
/// reject early but keep p99 in line; loose multipliers absorb transients
/// at the cost of tail latency.
async fn run_multiplier_sweep() {
    println!("\n--- Overflow multiplier sweep (large-scale 10k req/s) ---");
    println!(
        "Trade-off: smaller mult → more reject, tighter SLA. Larger mult → fewer reject, longer tail."
    );
    println!(
        "{:<14} | submitted | reject | ovrfl% | tier1   | p50     p99      max",
        "multiplier"
    );
    println!("{}", "-".repeat(96));
    for &mult in &[2usize, 4, 6, 8] {
        let cfg = scenarios::large_scale_16cores_with_multiplier(
            10_000.0,
            Duration::from_millis(50),
            mult,
        );
        let r = Simulation::new(cfg).run().await;
        let pct = |n: usize| n as f64 / r.total.max(1) as f64 * 100.0;
        let pct_complete = |n: usize| n as f64 / r.completed.max(1) as f64 * 100.0;
        let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
        println!(
            "{:<14} | {:>9} | {:>5.1}% | {:>5.1}% | {:>5.1}% | {:>5?} {:>6?} {:>6?}",
            format!("{}× (cap={})", mult, mult * 14),
            r.total,
            pct(r.rejected),
            pct_complete(r.overflow_admissions),
            pct(tier(RouteTier::Tier1WarmTracking)),
            r.latency_p50,
            r.latency_p99,
            r.latency_max,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::SweepSelection;

    #[test]
    fn optional_sweep_flags_keep_their_legacy_environment_mapping() {
        let cases = [
            (
                "RUN_STANDBY_SWEEP",
                SweepSelection {
                    standby: true,
                    ..SweepSelection::default()
                },
            ),
            (
                "RUN_EXECUTION_SWEEP",
                SweepSelection {
                    execution: true,
                    ..SweepSelection::default()
                },
            ),
            (
                "RUN_STEADY_SWEEP",
                SweepSelection {
                    steady: true,
                    ..SweepSelection::default()
                },
            ),
            (
                "RUN_MULTIPLIER_SWEEP",
                SweepSelection {
                    multiplier: true,
                    ..SweepSelection::default()
                },
            ),
            (
                "RUN_STYLE_SHIFT",
                SweepSelection {
                    style_shift: true,
                    ..SweepSelection::default()
                },
            ),
        ];

        for (enabled, expected) in cases {
            assert_eq!(
                SweepSelection::from_lookup(|name| name == enabled),
                expected,
                "wrong mapping for {enabled}",
            );
        }

        assert_eq!(
            SweepSelection::from_lookup(|_| false),
            SweepSelection::default(),
        );
    }
}
