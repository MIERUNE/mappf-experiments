//! Simulator CLI. The legacy no-subcommand mode still runs the existing
//! scenario suite; `run` produces a reproducible JSON report.

mod legacy_sweeps;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use biei_core::types::RouteTier;
use biei_sim::{
    Simulation, SimulationOptions, calibrated_costs,
    calibration::{
        CalibrationExportOptions, CalibrationProvenance, export_calibration_profile,
        parse_match_labels, read_bearer_token,
    },
    calibration_runner::{CalibrationExerciseOptions, run_calibration_exercise},
    churn::ChurnPlan,
    config::{SimConfig, StyleDist},
    scenarios, visualization,
};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "biei-sim", about = "Biei cluster simulator")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Export an immutable, provenance-bearing production calibration profile.
    Calibration {
        #[command(subcommand)]
        command: CalibrationCommand,
    },
    /// Run one simulation and write a reproducible JSON report.
    Run {
        #[arg(long, default_value = "biei-sim-report.json")]
        report: PathBuf,
        #[arg(long)]
        churn_plan: Option<PathBuf>,
        /// Sampling interval on the post-warmup measured-request clock.
        #[arg(long, default_value_t = 1_000)]
        sample_every_requests: u64,
        #[arg(long)]
        nodes: Option<usize>,
        #[arg(long)]
        rate: Option<f64>,
        #[arg(long)]
        styles: Option<usize>,
        #[arg(long)]
        new_style_rate: Option<f64>,
        #[arg(long)]
        duration_seconds: Option<u64>,
        #[arg(long)]
        warmup_seconds: Option<u64>,
        /// Derive provisional global cost ranges from an exported profile;
        /// measured node/permit provenance is applied, while hop latency and
        /// SLA stay at their configured values.
        #[arg(long)]
        cost_profile: Option<PathBuf>,
        /// Verified resource-warm reference profile supplying CPU service
        /// demand (two-window fusion); requires --cost-profile for the
        /// realistic-traffic walls that become resource waits.
        #[arg(long, requires = "cost_profile")]
        cpu_profile: Option<PathBuf>,
        /// Also write a self-contained HTML report.
        #[arg(long)]
        html: Option<PathBuf>,
    },
    /// Convert a JSON report into a self-contained HTML file.
    Visualize {
        input: PathBuf,
        #[arg(short, long, default_value = "biei-sim-report.html")]
        output: PathBuf,
    },
}

#[derive(Subcommand)]
enum CalibrationCommand {
    /// Generate a bounded warmup and measurement window against a running biei.
    Exercise {
        /// Full tile/static URL to request; repeat to cover multiple shapes.
        #[arg(long = "url", required = true)]
        urls: Vec<String>,
        #[arg(long, default_value_t = 2)]
        warmup_requests_per_url: usize,
        #[arg(long, default_value_t = 100)]
        requests_per_url: usize,
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        #[arg(long, default_value_t = 30)]
        timeout_seconds: u64,
        /// Wait before and after measurement so Prometheus scrapes both
        /// counter boundaries. Use at least the configured scrape interval.
        #[arg(long, default_value_t = 30)]
        scrape_settle_seconds: u64,
    },
    /// Query time-bounded Prometheus histograms and write an M12a JSON profile.
    Export {
        /// Prometheus root URL. `/api/v1/query` is appended when absent.
        #[arg(long)]
        prometheus_url: String,
        /// Collection-window start as Unix seconds.
        #[arg(long)]
        start_unix_seconds: u64,
        /// Collection-window end and Prometheus evaluation time as Unix seconds.
        #[arg(long)]
        end_unix_seconds: u64,
        /// Additional exact Prometheus series matcher (`NAME=VALUE`), repeatable.
        #[arg(long = "match-label", value_name = "NAME=VALUE")]
        match_labels: Vec<String>,
        /// File containing a bearer token; its contents are never serialized.
        #[arg(long)]
        bearer_token_file: Option<PathBuf>,
        #[arg(long)]
        deployment_revision: String,
        /// Production node architecture, for example `x86_64` or `aarch64`.
        #[arg(long)]
        architecture: String,
        /// Operator-defined machine/CPU identity, for example `GKE c3-standard-8`.
        #[arg(long)]
        hardware_profile: String,
        #[arg(long)]
        cpu_cores_per_node: usize,
        #[arg(long)]
        renderer_slots_per_node: usize,
        #[arg(long)]
        execution_permits_per_node: usize,
        #[arg(long)]
        native_render_permits_per_node: usize,
        /// Maximum request concurrency in the captured workload. Use 1 for a
        /// CPU-reference window; the importer rejects concurrent references.
        #[arg(long)]
        capture_concurrency: usize,
        /// Free-form provenance note; do not place credentials here.
        #[arg(long)]
        notes: Option<String>,
        #[arg(long, default_value_t = 30)]
        timeout_seconds: u64,
        #[arg(short, long, default_value = "biei-calibration-profile.json")]
        output: PathBuf,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Some(Command::Calibration { command }) => match command {
            CalibrationCommand::Exercise {
                urls,
                warmup_requests_per_url,
                requests_per_url,
                concurrency,
                timeout_seconds,
                scrape_settle_seconds,
            } => {
                let summary = run_calibration_exercise(CalibrationExerciseOptions {
                    urls,
                    warmup_requests_per_url,
                    requests_per_url,
                    concurrency,
                    timeout: Duration::from_secs(timeout_seconds),
                    scrape_settle: Duration::from_secs(scrape_settle_seconds),
                })
                .await?;
                println!(
                    "measured={} successful={} statuses={:?}",
                    summary.requests,
                    summary.successful(),
                    summary.status_counts,
                );
                if summary.successful() != summary.requests {
                    bail!(
                        "calibration exercise had non-success responses: {:?}",
                        summary.status_counts
                    );
                }
                println!(
                    "collection window: --start-unix-seconds {} --end-unix-seconds {}",
                    summary.start_unix_seconds, summary.end_unix_seconds,
                );
            }
            CalibrationCommand::Export {
                prometheus_url,
                start_unix_seconds,
                end_unix_seconds,
                match_labels,
                bearer_token_file,
                deployment_revision,
                architecture,
                hardware_profile,
                cpu_cores_per_node,
                renderer_slots_per_node,
                execution_permits_per_node,
                native_render_permits_per_node,
                capture_concurrency,
                notes,
                timeout_seconds,
                output,
            } => {
                let parsed_match_labels = parse_match_labels(match_labels)?;
                let bearer_token = bearer_token_file.map(read_bearer_token).transpose()?;
                let profile = export_calibration_profile(CalibrationExportOptions {
                    prometheus_url,
                    start_unix_seconds,
                    end_unix_seconds,
                    match_labels: parsed_match_labels,
                    bearer_token,
                    timeout: Duration::from_secs(timeout_seconds),
                    provenance: CalibrationProvenance {
                        deployment_revision,
                        architecture,
                        hardware_profile,
                        cpu_cores_per_node,
                        renderer_slots_per_node,
                        execution_permits_per_node,
                        native_render_permits_per_node,
                        capture_concurrency: Some(capture_concurrency),
                        notes,
                    },
                })
                .await?;
                for warning in &profile.warnings {
                    eprintln!("warning: {warning}");
                }
                println!(
                    "exported {} histogram series across {} metric families",
                    profile.series_count(),
                    profile.histograms.len()
                );
                profile.write_new_json(&output)?;
                println!("written: {}", output.display());
            }
        },
        Some(Command::Visualize { input, output }) => {
            visualization::write_visualization(input, &output)?;
            println!("written: {}", output.display());
        }
        Some(Command::Run {
            report,
            churn_plan,
            sample_every_requests,
            nodes,
            rate,
            styles,
            new_style_rate,
            duration_seconds,
            warmup_seconds,
            cost_profile,
            cpu_profile,
            html,
        }) => {
            tokio::time::pause();
            let mut config = SimConfig::default();
            let cpu_reference = cpu_profile
                .map(|path| -> Result<_> {
                    let profile = calibrated_costs::load_calibration_profile(&path)?;
                    Ok((path, profile))
                })
                .transpose()?;
            let calibration = cost_profile
                .map(|path| -> Result<_> {
                    let profile = calibrated_costs::load_calibration_profile(&path)?;
                    let derived = match &cpu_reference {
                        Some((_, reference)) => calibrated_costs::derive_costs_with_cpu_reference(
                            &profile,
                            reference,
                            &config.costs,
                        )?,
                        None => calibrated_costs::derive_costs(&profile, &config.costs)?,
                    };
                    Ok((path, profile, derived))
                })
                .transpose()?;
            if let Some((path, profile, derived)) = &calibration {
                calibrated_costs::apply_profile_provenance(profile, &mut config)?;
                config.costs = derived.costs.clone();
                for note in &derived.notes {
                    eprintln!("calibration: {note}");
                }
                eprintln!("calibrated costs from {}", path.display());
            }
            if let Some(nodes) = nodes {
                config.node_count = nodes;
            }
            if let Some(rate) = rate {
                config.workload.total_rate = rate;
            }
            if let Some(styles) = styles {
                config.workload.style_count = styles;
                config.workload.tile_style_count = config.workload.tile_style_count.min(styles);
            }
            if let Some(rate) = new_style_rate {
                config.workload.new_style_rate = rate;
            }
            if let Some(seconds) = duration_seconds {
                config.workload.duration = Duration::from_secs(seconds);
            }
            if let Some(seconds) = warmup_seconds {
                config.workload.warmup = Duration::from_secs(seconds);
            }
            let churn_plan = Some(
                churn_plan
                    .map(ChurnPlan::from_path)
                    .transpose()?
                    .unwrap_or(ChurnPlan { events: Vec::new() }),
            );
            let mut simulation = Simulation::new(config);
            if let Some((_, _, derived)) = &calibration {
                simulation = simulation.with_calibration_model(derived.sampling_model.clone());
            }
            let mut run_report = simulation
                .run_report(SimulationOptions {
                    churn_plan,
                    sample_every_requests,
                })
                .await?;
            if let Some((path, profile, derived)) = &calibration {
                // Reports must say which measured evidence sized their costs.
                run_report.config["cost_profile"] = serde_json::json!({
                    "path": path.display().to_string(),
                    "exported_at_unix_seconds": profile.exported_at_unix_seconds,
                    "collection": profile.collection,
                    "provenance": profile.provenance,
                    "coverage": derived.coverage,
                    "derivation_notes": derived.notes,
                });
                if let Some((cpu_path, cpu_reference)) = &cpu_reference {
                    run_report.config["cpu_profile"] = serde_json::json!({
                        "path": cpu_path.display().to_string(),
                        "exported_at_unix_seconds": cpu_reference.exported_at_unix_seconds,
                        "collection": cpu_reference.collection,
                        "provenance": cpu_reference.provenance,
                    });
                }
            }
            if let Some(churn) = &run_report.churn
                && !churn.unapplied_events.is_empty()
            {
                eprintln!(
                    "warning: {} churn event(s) were not reached by the {} measured requests; preserving them in the report",
                    churn.unapplied_events.len(),
                    churn.submitted_measured,
                );
            }
            println!(
                "completed={} rejected={} p99={:.1}ms",
                run_report.result.completed,
                run_report.result.rejected,
                run_report.result.latency_p99_ms,
            );
            run_report.write_json(&report)?;
            println!("written: {}", report.display());
            if let Some(html) = html {
                visualization::write_visualization(&report, &html)?;
                println!("written: {}", html.display());
            }
        }
        None => run_legacy().await,
    }
    Ok(())
}

async fn run_legacy() {
    tokio::time::pause();

    if std::env::var_os("RUN_LARGE_SCALE_ONLY").is_some() {
        legacy_sweeps::run_selected().await;
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
        legacy_sweeps::run_selected().await;
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

fn print_config(config: &SimConfig) {
    let slots_total = config.cluster.renderer_slots_per_node * config.node_count;
    let permits_per_node = config.cluster.resolved_render_permits_per_node();
    let permits_total = permits_per_node * config.node_count;
    let cpu_permits_per_node = config.cluster.resolved_cpu_render_permits_per_node();
    let cpu_permits_total = cpu_permits_per_node * config.node_count;
    let warm_residency_mid = config.costs.warm_render_cost().mid().as_secs_f64();
    let cpu_mid = config.costs.render_cpu_cost.mid().as_secs_f64();
    let native_capacity = cpu_permits_total as f64 / warm_residency_mid;
    let cpu_capacity = (config.cpu_cores_per_node * config.node_count) as f64 / cpu_mid;
    let max_throughput = native_capacity.min(cpu_capacity);
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
        "native permits:    {} per node ({} total)",
        cpu_permits_per_node, cpu_permits_total
    );
    println!(
        "CPU cores:         {} per node ({} total)",
        config.cpu_cores_per_node,
        config.cpu_cores_per_node * config.node_count
    );
    println!(
        "style setup (S):   {:?}–{:?}",
        config.costs.style_setup_cost.min, config.costs.style_setup_cost.max
    );
    println!(
        "render CPU:        {:?}–{:?}",
        config.costs.render_cpu_cost.min, config.costs.render_cpu_cost.max
    );
    println!(
        "warm resource I/O: {:?}–{:?}",
        config.costs.render_resource_cost.min, config.costs.render_resource_cost.max
    );
    println!(
        "first resource I/O:{:?}–{:?}",
        config.costs.first_render_resource_cost.min, config.costs.first_render_resource_cost.max
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
        "max throughput:    {:.2} req/s (min(native residency, CPU service))",
        max_throughput,
    );
    println!("duration:          {:?}", config.workload.duration);
    println!();
}
