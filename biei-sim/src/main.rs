//! Demo runner — executes every scenario in sequence and prints the resulting
//! metrics tables. CSV outputs land alongside the binary.

use std::time::Duration;

use biei::types::RouteTier;
use biei_sim::{
    Simulation,
    config::{SimConfig, StyleDist},
    scenarios,
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tokio::time::pause();

    if std::env::var_os("RUN_LARGE_SCALE_ONLY").is_some() {
        run_large_scale_sweep().await;
        if std::env::var_os("RUN_STANDBY_SWEEP").is_some() {
            run_standby_sweep().await;
        }
        if std::env::var_os("RUN_EXECUTION_SWEEP").is_some() {
            run_execution_sweep().await;
        }
        if std::env::var_os("RUN_STEADY_SWEEP").is_some() {
            run_steady_sweep().await;
        }
        if std::env::var_os("RUN_MULTIPLIER_SWEEP").is_some() {
            run_multiplier_sweep().await;
        }
        if std::env::var_os("RUN_STYLE_SHIFT").is_some() {
            run_style_shift_scenario().await;
        }
        return;
    }

    // ---- Default config: full report ------------------------------------
    let config = SimConfig::default();
    print_config(&config);
    let report = Simulation::new(config).run().await;
    println!("{}", report.to_human_readable());

    // ---- Production sanity ----------------------------------------------
    println!("--- Sanity: production-realistic config (no overload expected) ---");
    println!("4 nodes × 16 renderer slots, 15 styles Zipf α=1.2, 1000 req/s");
    {
        let cfg = scenarios::production_sanity();
        let sla = cfg.costs.sla;
        let r = Simulation::new(cfg).run().await;
        let pct = |n: usize| n as f64 / r.completed.max(1) as f64 * 100.0;
        let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
        let rejected = r.rejected;
        println!(
            "submitted={} completed={} rejected={} | tier1={:.1}% tier2={:.1}% tier3={:.1}% | swaps={:.1}% | p50={:?} p99={:?} max={:?} (SLA={:?})",
            r.total,
            r.completed,
            rejected,
            pct(tier(RouteTier::Tier1WarmTracking)),
            pct(tier(RouteTier::Tier2HrwBl)),
            pct(tier(RouteTier::Tier3DrainSwap)),
            pct(r.style_swaps),
            r.latency_p50,
            r.latency_p99,
            r.latency_max,
            sla,
        );
    }

    // ---- Source pattern sweep -------------------------------------------
    println!("\n--- Source pattern sweep (rate=1000, production sizing, 30s) ---");
    println!(
        "{:<54} | w/src | hits | loads | p50      p99      max",
        "source pattern"
    );
    println!("{}", "-".repeat(110));
    for (label, pat) in scenarios::source_pattern_scenarios() {
        let cfg = scenarios::with_source_pattern(pat);
        let r = Simulation::new(cfg).run().await;
        let hit_pct = if r.tasks_with_sources > 0 {
            format!(
                "{:5.1}%",
                r.source_hits as f64 / r.tasks_with_sources as f64 * 100.0
            )
        } else {
            String::from("  n/a")
        };
        println!(
            "{:<54} | {:>5} | {:>4} ({}) | {:>5} | {:>7?} {:>7?} {:>7?}",
            label,
            r.tasks_with_sources,
            r.source_hits,
            hit_pct,
            r.source_loads,
            r.latency_p50,
            r.latency_p99,
            r.latency_max,
        );
    }

    // ---- BL × Zipf α sweep ----------------------------------------------
    println!("\n--- BL capacity × Zipf α sweep (S=250ms, P=17.5ms → S/P≈14, ~77% util) ---");
    println!("Expect: best p99 + lowest reject near BL ≈ S/P ≈ 14");
    println!("{:<22} | reject | p50      p99      tier1   ovrfl", "label");
    println!("{}", "-".repeat(80));
    let mut bl_alpha_results = Vec::new();
    for &bl in &[1usize, 2, 5, 10, 25, 50, 100] {
        for &alpha in &[0.5_f64, 1.0, 1.5, 2.0] {
            let cfg = scenarios::bl_alpha_sweep(bl, alpha);
            let label = format!("bl={},alpha={}", bl, alpha);
            let r = Simulation::new(cfg).run().await;
            let pct = |n: usize| n as f64 / r.total.max(1) as f64 * 100.0;
            let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
            println!(
                "{:<22} | {:>5.1}% | {:>7?} {:>7?} {:>5.1}% {:>5.1}%",
                label,
                pct(r.rejected),
                r.latency_p50,
                r.latency_p99,
                pct(tier(RouteTier::Tier1WarmTracking)),
                pct(tier(RouteTier::Tier4Overflow)),
            );
            bl_alpha_results.push((label, r));
        }
    }
    biei_sim::sweep::write_csv("sweep_bl_alpha.csv", &bl_alpha_results).expect("csv write");
    println!(
        "→ written: sweep_bl_alpha.csv ({} rows)",
        bl_alpha_results.len()
    );

    // ---- Production cluster sizing sweep --------------------------------
    println!("\n--- Production cluster sizing sweep (10 styles Zipf 1.2, rate=1000) ---");
    println!("Small clusters (<32 slots) are stress-only and excluded from the primary axis");
    println!("{:<22} | reject | sla_viol | p99      ovrfl", "label");
    println!("{}", "-".repeat(80));
    let mut sizing_results = Vec::new();
    for &(node_count, renderer_slots_per_node) in
        &[(8usize, 4usize), (16, 4), (32, 4), (16, 16), (25, 16)]
    {
        let cfg = scenarios::cluster_sizing_sweep(node_count, renderer_slots_per_node);
        let actual_slots = node_count * renderer_slots_per_node;
        let label = format!("slots={}", actual_slots);
        let r = Simulation::new(cfg).run().await;
        let pct = |n: usize| n as f64 / r.total.max(1) as f64 * 100.0;
        let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
        println!(
            "{:<22} | {:>5.1}% | {:>5.2}%   | {:>7?} {:>5.1}%",
            label,
            pct(r.rejected),
            r.sla_violations as f64 / r.completed.max(1) as f64 * 100.0,
            r.latency_p99,
            pct(tier(RouteTier::Tier4Overflow)),
        );
        sizing_results.push((label, r));
    }
    biei_sim::sweep::write_csv("sweep_cluster_sizing.csv", &sizing_results).expect("csv write");
    println!(
        "→ written: sweep_cluster_sizing.csv ({} rows)",
        sizing_results.len()
    );

    if std::env::var_os("RUN_SMALL_CLUSTER_STRESS").is_some() {
        println!("\n--- Small-cluster stress sweep (non-production) ---");
        println!("{:<22} | reject | sla_viol | p99      ovrfl", "label");
        println!("{}", "-".repeat(80));
        let mut stress_results = Vec::new();
        for &(node_count, renderer_slots_per_node) in &[(1usize, 4usize), (2, 4), (3, 4), (4, 4)] {
            let cfg = scenarios::cluster_sizing_sweep(node_count, renderer_slots_per_node);
            let actual_slots = node_count * renderer_slots_per_node;
            let label = format!("slots={}", actual_slots);
            let r = Simulation::new(cfg).run().await;
            let pct = |n: usize| n as f64 / r.total.max(1) as f64 * 100.0;
            let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
            println!(
                "{:<22} | {:>5.1}% | {:>5.2}%   | {:>7?} {:>5.1}%",
                label,
                pct(r.rejected),
                r.sla_violations as f64 / r.completed.max(1) as f64 * 100.0,
                r.latency_p99,
                pct(tier(RouteTier::Tier4Overflow)),
            );
            stress_results.push((label, r));
        }
        biei_sim::sweep::write_csv("sweep_small_cluster_stress.csv", &stress_results)
            .expect("csv write");
        println!(
            "→ written: sweep_small_cluster_stress.csv ({} rows)",
            stress_results.len()
        );
    } else {
        println!("Small-cluster stress sweep skipped. Set RUN_SMALL_CLUSTER_STRESS=1 to run it.");
    }

    if std::env::var_os("RUN_LARGE_SCALE").is_some() {
        run_large_scale_sweep().await;
        if std::env::var_os("RUN_STANDBY_SWEEP").is_some() {
            run_standby_sweep().await;
        }
        if std::env::var_os("RUN_EXECUTION_SWEEP").is_some() {
            run_execution_sweep().await;
        }
        if std::env::var_os("RUN_STEADY_SWEEP").is_some() {
            run_steady_sweep().await;
        }
        if std::env::var_os("RUN_MULTIPLIER_SWEEP").is_some() {
            run_multiplier_sweep().await;
        }
        if std::env::var_os("RUN_STYLE_SHIFT").is_some() {
            run_style_shift_scenario().await;
        }
    } else {
        println!("\n--- Large-scale production sizing skipped ---");
        println!("Set RUN_LARGE_SCALE=1 for a short 25-node smoke sweep.");
        println!("Set RUN_LARGE_SCALE_FULL=1 as well for the full high-rate sweep.");
        println!(
            "Set RUN_STANDBY_SWEEP=1, RUN_EXECUTION_SWEEP=1, RUN_STEADY_SWEEP=1, RUN_MULTIPLIER_SWEEP=1, or RUN_STYLE_SHIFT=1 for extra large-scale sweeps."
        );
        println!(
            "Set RUN_LARGE_SCALE_ONLY=1 to skip the general report and run only large-scale benchmarks."
        );
    }

    // ---- Style distribution sweep ---------------------------------------
    println!("\n--- Style distribution sweep (rate=100, 50 styles, 20s) ---");
    println!(
        "{:<28} | tier1   | swaps        | cold | p50      p99      max",
        "distribution"
    );
    println!("{}", "-".repeat(96));
    for dist in [
        StyleDist::Uniform,
        StyleDist::Zipf { alpha: 0.5 },
        StyleDist::Zipf { alpha: 1.0 },
        StyleDist::Zipf { alpha: 2.0 },
    ] {
        let cfg = scenarios::style_dist_sweep(dist.clone());
        let r = Simulation::new(cfg).run().await;
        let pct = |n: usize| n as f64 / r.completed.max(1) as f64 * 100.0;
        let tier1 = r
            .tier_counts
            .get(&RouteTier::Tier1WarmTracking)
            .copied()
            .unwrap_or(0);
        println!(
            "{:<28} | {:5.1}% | {:>4} ({:5.1}%) | {:>4} | {:>7?} {:>7?} {:>7?}",
            format!("{:?}", dist),
            pct(tier1),
            r.style_swaps,
            pct(r.style_swaps),
            r.cold_starts,
            r.latency_p50,
            r.latency_p99,
            r.latency_max,
        );
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
    // With render_cost mid ≈ 17.5ms × 400 permits, cluster cap ≈ 23k req/s.
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
            r.cpu_render_utilization_pct,
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
        r.cpu_render_utilization_pct,
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
            r.cpu_render_utilization_pct,
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

/// Sweep task execution permits above the CPU render bottleneck. This models
/// I/O overlap: more requests may progress through setup/source loading while
/// render/encode remains capped at the core-like CPU permit count.
async fn run_execution_sweep() {
    println!("\n--- Execution permit sweep (large-scale 10k req/s) ---");
    println!("25 nodes, 32 warm slots/node, 16 CPU render permits/node fixed.");
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
            r.cpu_render_utilization_pct,
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

fn print_config(config: &SimConfig) {
    let slots_total = config.cluster.renderer_slots_per_node * config.node_count;
    let permits_per_node = config.cluster.resolved_render_permits_per_node();
    let permits_total = permits_per_node * config.node_count;
    let cpu_permits_per_node = config.cluster.resolved_cpu_render_permits_per_node();
    let cpu_permits_total = cpu_permits_per_node * config.node_count;
    let p_mid = config.costs.render_cost.mid().as_secs_f64();
    let max_throughput = cpu_permits_total as f64 / p_mid;
    let queue_limits = config.cluster.resolved_queue_limits(&config.costs);

    println!("--- Config ---");
    println!("nodes:             {}", config.node_count);
    println!(
        "renderer slots:    {} per node ({} total)",
        config.cluster.renderer_slots_per_node, slots_total
    );
    println!(
        "render permits:    {} per node ({} total)",
        permits_per_node, permits_total
    );
    println!(
        "CPU render permits:{} per node ({} total)",
        cpu_permits_per_node, cpu_permits_total
    );
    println!(
        "style setup (S):   {:?}–{:?}",
        config.costs.style_setup_cost.min, config.costs.style_setup_cost.max
    );
    println!(
        "render cost (P):   {:?}–{:?}",
        config.costs.render_cost.min, config.costs.render_cost.max
    );
    println!("hop latency:       {:?}", config.costs.hop_latency);
    println!("sla (L):           {:?}", config.costs.sla);
    println!(
        "soft queue limit:  {} per slot (BL/SLA target)",
        queue_limits.soft
    );
    println!(
        "hard queue limit:  {} per slot (backpressure cap)",
        queue_limits.hard
    );
    println!(
        "gossip:            chitchat publish={:?}",
        config.gossip.publish_interval
    );
    println!(
        "styles:            {} initial (+{}/s new)",
        config.workload.style_count, config.workload.new_style_rate
    );
    println!(
        "style dist:        {:?}",
        config.workload.style_distribution
    );
    if let Some(b) = &config.workload.burst_pattern {
        println!(
            "burst:             period={:?}, dur={:?}, x{}, focus={:?}",
            b.period, b.duration, b.multiplier, b.style_focus
        );
    }
    println!("arrival rate:      {:.2} req/s", config.workload.total_rate);
    println!(
        "max throughput:    {:.2} req/s (= total_cpu_render_permits / E[P])",
        max_throughput
    );
    println!("duration:          {:?}", config.workload.duration);
    println!();
}
