//! Simulator CLI for calibration, reproducible runs, and visualization.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Result, bail};
use biei_sim::{
    Simulation, SimulationOptions, calibrated_costs,
    calibration::{
        CalibrationExportOptions, CalibrationProvenance, export_calibration_profile,
        parse_match_labels, read_bearer_token,
    },
    calibration_runner::{CalibrationExerciseOptions, run_calibration_exercise},
    churn::ChurnPlan,
    config::SimConfig,
    visualization,
};
use clap::{Parser, Subcommand};
use mmpf_common::path::same_file_target;

#[derive(Parser)]
#[command(name = "biei-sim", about = "Biei cluster simulator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
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
        /// Realistic-traffic profile supplying render-wall distributions;
        /// requires a separately verified --cpu-profile.
        #[arg(long, requires = "cpu_profile")]
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
        Command::Calibration { command } => match command {
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
                let token_inputs = bearer_token_file
                    .as_deref()
                    .map(|path| vec![(path, "bearer-token file")])
                    .unwrap_or_default();
                reject_output_collisions(&[(output.as_path(), "output")], &token_inputs)?;
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
        Command::Visualize { input, output } => {
            reject_output_collisions(
                &[(output.as_path(), "output")],
                &[(input.as_path(), "input")],
            )?;
            visualization::write_visualization(input, &output)?;
            println!("written: {}", output.display());
        }
        Command::Run {
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
        } => {
            // Reject output paths that alias an input or each other before any
            // input is read, so a run cannot succeed and then truncate the very
            // artifacts (trace/churn/calibration) needed to reproduce it.
            let mut outputs: Vec<(&Path, &str)> = vec![(report.as_path(), "report")];
            if let Some(html) = &html {
                outputs.push((html.as_path(), "html"));
            }
            let mut inputs: Vec<(&Path, &str)> = Vec::new();
            if let Some(path) = &churn_plan {
                inputs.push((path.as_path(), "churn-plan"));
            }
            if let Some(path) = &cost_profile {
                inputs.push((path.as_path(), "cost-profile"));
            }
            if let Some(path) = &cpu_profile {
                inputs.push((path.as_path(), "cpu-profile"));
            }
            reject_output_collisions(&outputs, &inputs)?;

            tokio::time::pause();
            let mut config = SimConfig::default();
            let cpu_reference = cpu_profile
                .map(|path| -> Result<_> {
                    let profile = calibrated_costs::load_calibration_profile(&path)?;
                    Ok((path, profile))
                })
                .transpose()?;
            let calibration = match (cost_profile, cpu_reference.as_ref()) {
                (Some(path), Some((_, reference))) => {
                    let profile = calibrated_costs::load_calibration_profile(&path)?;
                    let derived = calibrated_costs::derive_costs_with_cpu_reference(
                        &profile,
                        reference,
                        &config.costs,
                    )?;
                    Some((path, profile, derived))
                }
                (None, None) => None,
                _ => unreachable!("clap requires cost and CPU profiles together"),
            };
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
    }
    Ok(())
}

/// Rejects any output that resolves to the same file as another output or an
/// input, before the run reads inputs or truncates outputs. Resolution uses
/// filesystem identity, so `./`, `..`, and symlink aliases are caught — not just
/// lexically-equal path strings.
fn reject_output_collisions(outputs: &[(&Path, &str)], inputs: &[(&Path, &str)]) -> Result<()> {
    for (index, (output, output_name)) in outputs.iter().enumerate() {
        for (other, other_name) in outputs.iter().skip(index + 1) {
            if same_file_target(output, other) {
                bail!(
                    "outputs `{output_name}` and `{other_name}` resolve to the same file ({}); \
                     refusing to overwrite",
                    output.display()
                );
            }
        }
        for (input, input_name) in inputs {
            if same_file_target(output, input) {
                bail!(
                    "output `{output_name}` would overwrite input `{input_name}` ({}); \
                     choose a distinct output path",
                    output.display()
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Cli, reject_output_collisions};
    use clap::Parser;

    #[test]
    fn rejects_output_aliasing_input_and_other_output() {
        let dir = std::env::temp_dir();
        let report = dir.join("biei_sim_collision_report.json");
        let html = dir.join("biei_sim_collision_report.html");
        let trace = dir.join("biei_sim_collision_report.json"); // same as report

        // Output equal to an input is rejected before any write.
        assert!(
            reject_output_collisions(
                &[(report.as_path(), "report")],
                &[(trace.as_path(), "churn-plan")],
            )
            .is_err()
        );
        // Two outputs resolving to the same file are rejected.
        assert!(
            reject_output_collisions(
                &[(report.as_path(), "report"), (report.as_path(), "html")],
                &[],
            )
            .is_err()
        );
        // Distinct outputs and inputs are accepted.
        assert!(
            reject_output_collisions(
                &[(report.as_path(), "report"), (html.as_path(), "html")],
                &[(dir.join("input.json").as_path(), "cost-profile")],
            )
            .is_ok()
        );
    }

    #[test]
    fn rejects_dot_relative_alias_of_the_same_output() {
        let dir = std::env::temp_dir();
        let direct = dir.join("biei_sim_dot_alias.json");
        let dotted = dir.join(".").join("biei_sim_dot_alias.json");
        assert!(
            reject_output_collisions(
                &[(direct.as_path(), "report"), (dotted.as_path(), "html")],
                &[],
            )
            .is_err(),
            "`./` alias of the same file must be detected"
        );
    }

    #[test]
    fn command_is_required() {
        assert!(Cli::try_parse_from(["biei-sim"]).is_err());
    }

    #[test]
    fn calibration_profiles_must_be_supplied_as_a_pair() {
        assert!(
            Cli::try_parse_from(["biei-sim", "run", "--cost-profile", "traffic.json"]).is_err()
        );
        assert!(Cli::try_parse_from(["biei-sim", "run", "--cpu-profile", "cpu.json"]).is_err());
        assert!(
            Cli::try_parse_from([
                "biei-sim",
                "run",
                "--cost-profile",
                "traffic.json",
                "--cpu-profile",
                "cpu.json",
            ])
            .is_ok()
        );
    }
}
