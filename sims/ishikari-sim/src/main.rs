use std::{
    fs::File,
    future::Future,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use ishikari_sim::{
    BackendLatencyConfig, BackendLatencyProfile, ChurnConfig, ChurnPlan, ChurnReport,
    ClusterConfig, EntryAffinity, HttpExecutionMode, HttpReplayConfig, HttpReplayTarget,
    ModeledCluster, PopulationCdf, SimCluster, TileCatalog, TimedConfig, TimedReport, TraceEntry,
    Workload, WorkloadConfig, read_trace, run_churn_trace, run_http_replay,
    run_modeled_churn_trace, run_sweep, run_timed_trace, viewport_batch_ranges, write_trace_entry,
    write_visualization,
};
use mmpf_common::path::same_file_target;
use reqwest::Url;
use serde::Serialize;

const REPORT_SCHEMA_VERSION: u32 = 1;

#[derive(Serialize)]
struct RunReport {
    schema_version: u32,
    execution_mode: &'static str,
    cache_mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_tiles: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timing: Option<TimedReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend_latency_profile: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    churn_plan: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    churn: Option<ChurnReport>,
    trace: TraceSource,
    cluster: ClusterConfig,
    result: ishikari_sim::SimReport,
}

struct SimulationExecution {
    modeled_result: Option<ishikari_sim::SimReport>,
    catalog_tiles: Option<usize>,
    timing: Option<TimedReport>,
    churn: Option<ChurnReport>,
    trace_source: TraceSource,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TraceSource {
    Generated {
        census: PathBuf,
        steps: u64,
        workload: WorkloadConfig,
        output: PathBuf,
    },
    Replay {
        input: PathBuf,
        requests: usize,
    },
}

#[derive(Parser)]
#[command(about = "Generate deterministic traces and run an in-process Ishikari cluster")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    simulation: SimulationArgs,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a self-contained HTML visualization from a simulation report.
    Visualize(VisualizeArgs),
    /// Run a versioned, replay-only modeled-cache parameter sweep.
    Sweep(SweepArgs),
    /// Replay a trace through direct nodes or a Gateway and capture calibration metrics.
    ReplayHttp(ReplayHttpArgs),
}

#[derive(clap::Args)]
struct VisualizeArgs {
    /// Simulation report JSON generated with --report.
    input: PathBuf,
    /// Destination HTML path; defaults to the input path with an .html extension.
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(clap::Args)]
struct SweepArgs {
    /// Versioned JSON sweep specification.
    spec: PathBuf,
    /// Destination JSONL file containing one self-contained document per run.
    #[arg(short, long)]
    output: PathBuf,
}

#[derive(clap::Args)]
struct ReplayHttpArgs {
    /// Existing simulator JSONL trace to replay.
    trace: PathBuf,
    /// Ordered direct-node public URL; entry_node indexes this repeated option.
    #[arg(long = "node-url", conflicts_with = "gateway_url")]
    node_urls: Vec<Url>,
    /// Single public Gateway URL; recorded entry_node values are ignored.
    #[arg(long, conflicts_with = "node_urls")]
    gateway_url: Option<Url>,
    /// Per-node internal Prometheus endpoint scraped before and after replay.
    #[arg(long = "metrics-url")]
    metrics_urls: Vec<Url>,
    /// Execute each viewport batch concurrently while preserving batch boundaries.
    #[arg(long)]
    viewport_batches: bool,
    /// Per-request timeout in milliseconds. Requests are never retried.
    #[arg(long, default_value_t = 30_000)]
    request_timeout_ms: u64,
    /// Destination versioned JSON report.
    #[arg(short, long)]
    output: PathBuf,
}

impl ReplayHttpArgs {
    fn into_config(self) -> Result<(HttpReplayConfig, PathBuf)> {
        let target = match (self.gateway_url, self.node_urls.is_empty()) {
            (Some(gateway_url), true) => HttpReplayTarget::Gateway { gateway_url },
            (None, false) => HttpReplayTarget::DirectNodes {
                node_urls: self.node_urls,
            },
            (Some(_), false) => bail!("--gateway-url conflicts with --node-url"),
            (None, true) => bail!("provide --gateway-url or at least one --node-url"),
        };
        reject_output_collisions(
            &[(self.output.as_path(), "output")],
            &[(self.trace.as_path(), "trace")],
        )?;
        Ok((
            HttpReplayConfig {
                trace_path: self.trace,
                target,
                mode: if self.viewport_batches {
                    HttpExecutionMode::ViewportBatches
                } else {
                    HttpExecutionMode::Serial
                },
                metrics_urls: self.metrics_urls,
                request_timeout: std::time::Duration::from_millis(self.request_timeout_ms),
            },
            self.output,
        ))
    }
}

#[derive(clap::Args)]
struct SimulationArgs {
    #[arg(
        long,
        default_value = "sims/ishikari-sim/data/census_2020_1km_population.geojson"
    )]
    census: PathBuf,
    /// Write a generated JSONL trace to this path.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Replay an existing JSONL trace instead of generating one.
    #[arg(long, conflicts_with = "output", requires = "simulate")]
    input_trace: Option<PathBuf>,
    #[arg(long, default_value = "mierune/omt")]
    tileset: String,
    #[arg(long, default_value_t = 50)]
    users: usize,
    #[arg(long, default_value_t = 1_000)]
    steps: u64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 4)]
    min_zoom: u8,
    #[arg(long, default_value_t = 15)]
    max_zoom: u8,
    #[arg(long, default_value_t = 13.0)]
    focus_zoom: f64,
    #[arg(long, default_value_t = 1.8)]
    zoom_sigma: f64,
    #[arg(long, default_value_t = 0.07)]
    session_reset_probability: f64,
    /// Per non-reset step, replace panning with a one-level zoom at the same center.
    #[arg(long, default_value_t = 0.0)]
    zoom_walk_probability: f64,
    #[arg(long, default_value_t = 1.0)]
    move_step_tiles: f64,
    #[arg(long, default_value_t = 3)]
    nodes: usize,
    #[arg(long, value_enum, default_value_t = AffinityArg::PerRequest)]
    entry_affinity: AffinityArg,
    /// Execute the generated or input trace through an in-process Ishikari cluster.
    #[arg(long)]
    simulate: bool,
    /// Use real payload caches or metadata-only logical-capacity caches.
    #[arg(long, value_enum, default_value_t = CacheModeArg::Real)]
    cache_mode: CacheModeArg,
    /// Poll each viewport's new tiles concurrently under paused Tokio time.
    #[arg(long, requires = "simulate")]
    viewport_batches: bool,
    /// Replay VUs concurrently with virtual think time and collect latency percentiles.
    #[arg(long, requires = "simulate", conflicts_with = "viewport_batches")]
    phase2: bool,
    /// Apply node add/remove events while replaying a trace.
    #[arg(
        long,
        requires_all = ["simulate", "input_trace"],
        conflicts_with = "phase2"
    )]
    churn_plan: Option<PathBuf>,
    /// Cumulative churn metrics sampling interval.
    #[arg(long, default_value_t = 1_000, requires = "churn_plan")]
    churn_sample_every_requests: u64,
    #[arg(long, default_value = "data")]
    tileset_sources: String,
    #[arg(long, default_value_t = 3)]
    candidate_count: usize,
    #[arg(long, default_value_t = 512)]
    tile_group_size: u64,
    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size_bytes: u64,
    #[arg(long, default_value_t = 4)]
    max_fetch_chunks: u64,
    /// Scheduler delay used by each real-cache node to merge nearby chunk fetches.
    /// Zero removes the intentional delay. Modeled-cache execution only records it.
    #[arg(long, default_value_t = 10)]
    chunk_fetch_merge_window_ms: u64,
    /// Process-wide object-storage range-fetch limit per simulated node.
    #[arg(long, default_value_t = 32)]
    backend_fetch_concurrency: usize,
    /// Active plus queued object-storage range-fetch groups admitted per node.
    /// Defaults to four times the active fetch concurrency.
    #[arg(long)]
    backend_fetch_max_inflight: Option<usize>,
    /// Fixed range-fetch delay, or the median when sigma is non-zero.
    #[arg(long, default_value_t = 0)]
    artificial_backend_delay_ms: u64,
    /// Lognormal sigma for the artificial range-fetch delay.
    #[arg(long, default_value_t = 0.0)]
    artificial_backend_delay_sigma: f64,
    /// Transfer-time slope added per MiB in each artificial range fetch.
    #[arg(long, default_value_t = 0.0)]
    artificial_backend_transfer_ms_per_mib: f64,
    /// Load an empirical object-storage latency model for Phase 2.
    #[arg(long, requires = "phase2")]
    backend_latency_profile: Option<PathBuf>,
    /// Simulated latency added to an in-process peer request.
    #[arg(long, default_value_t = 0)]
    peer_latency_ms: u64,
    /// Production chitchat gossip interval used by real-cache simulation.
    #[arg(long, default_value_t = 200)]
    gossip_interval_ms: u64,
    /// Virtual one-way latency added to each in-memory gossip message.
    #[arg(long, default_value_t = 1)]
    gossip_hop_latency_ms: u64,
    #[arg(long, default_value_t = 1_200)]
    think_time_ms: u64,
    #[arg(long, default_value_t = 500)]
    think_jitter_ms: u64,
    #[arg(long, default_value_t = 1)]
    request_overhead_ms: u64,
    #[arg(long, default_value_t = 10_000)]
    request_timeout_ms: u64,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    tile_cache_max_bytes: u64,
    /// Cache successful peer responses at the entry node, or only at the HRW owner.
    #[arg(long, value_enum, default_value_t = PeerTileCacheArg::Entry)]
    peer_tile_cache: PeerTileCacheArg,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    chunk_cache_max_bytes: u64,
    #[arg(long, requires = "simulate")]
    report: Option<PathBuf>,
}

impl SimulationArgs {
    fn validate(&self) -> Result<()> {
        if self.output.is_none() && self.input_trace.is_none() {
            bail!("--output is required when generating a trace");
        }
        if self.cache_mode == CacheModeArg::Modeled && !self.simulate {
            bail!("--cache-mode modeled requires --simulate");
        }
        if self.phase2 && (self.cache_mode != CacheModeArg::Real || self.input_trace.is_none()) {
            bail!("--phase2 requires --cache-mode real and --input-trace");
        }
        if self.churn_plan.is_some() && self.churn_sample_every_requests == 0 {
            bail!("--churn-sample-every-requests must be greater than zero");
        }
        Ok(())
    }

    fn validate_artifact_paths(&self) -> Result<()> {
        let mut outputs = Vec::new();
        if let Some(output) = &self.output {
            outputs.push((output.as_path(), "trace output"));
        }
        if let Some(report) = &self.report {
            outputs.push((report.as_path(), "report"));
        }

        let mut inputs = Vec::new();
        if let Some(input_trace) = &self.input_trace {
            inputs.push((input_trace.as_path(), "input trace"));
        } else {
            inputs.push((self.census.as_path(), "census"));
        }
        if let Some(profile) = &self.backend_latency_profile {
            inputs.push((profile.as_path(), "backend latency profile"));
        }
        if let Some(churn_plan) = &self.churn_plan {
            inputs.push((churn_plan.as_path(), "churn plan"));
        }

        reject_output_collisions(&outputs, &inputs)
    }

    fn uses_paused_time(&self) -> bool {
        self.simulate && self.cache_mode == CacheModeArg::Real
    }

    fn cluster_config(&self) -> Result<ClusterConfig> {
        let profile = self
            .backend_latency_profile
            .as_ref()
            .map(|path| BackendLatencyProfile::from_path(path))
            .transpose()?;
        let backend_latency = match profile {
            Some(profile) => BackendLatencyConfig {
                seed: self.seed,
                ..profile.model
            },
            None => BackendLatencyConfig {
                median_ms: self.artificial_backend_delay_ms,
                lognormal_sigma: self.artificial_backend_delay_sigma,
                transfer_ms_per_mib: self.artificial_backend_transfer_ms_per_mib,
                seed: self.seed,
            },
        };
        Ok(ClusterConfig {
            node_count: self.nodes,
            tileset_sources: self.tileset_sources.clone(),
            candidate_count: self.candidate_count,
            tile_group_size: self.tile_group_size,
            chunk_size_bytes: self.chunk_size_bytes,
            max_fetch_chunks: self.max_fetch_chunks,
            chunk_fetch_merge_window_ms: self.chunk_fetch_merge_window_ms,
            backend_fetch_concurrency: self.backend_fetch_concurrency,
            backend_fetch_max_inflight: self
                .backend_fetch_max_inflight
                .unwrap_or_else(|| self.backend_fetch_concurrency.max(1).saturating_mul(4)),
            backend_latency,
            peer_latency_ms: self.peer_latency_ms,
            gossip_interval_ms: self.gossip_interval_ms,
            gossip_hop_latency_ms: self.gossip_hop_latency_ms,
            tile_cache_max_bytes: self.tile_cache_max_bytes,
            chunk_cache_max_bytes: self.chunk_cache_max_bytes,
            cache_peer_tiles: self.peer_tile_cache == PeerTileCacheArg::Entry,
        })
    }

    fn timed_config(&self) -> TimedConfig {
        TimedConfig {
            think_time_ms: self.think_time_ms,
            think_jitter_ms: self.think_jitter_ms,
            request_overhead_ms: self.request_overhead_ms,
            request_timeout_ms: self.request_timeout_ms,
            seed: self.seed,
        }
    }

    fn churn_config(&self) -> ChurnConfig {
        ChurnConfig {
            seed: self.seed,
            entry_affinity: match self.entry_affinity {
                AffinityArg::PerRequest => EntryAffinity::PerRequest,
                AffinityArg::PerSession => EntryAffinity::PerSession,
            },
            sample_every_requests: self.churn_sample_every_requests,
        }
    }

    fn execution_mode(&self) -> &'static str {
        if self.phase2 {
            "phase2"
        } else if self.churn_plan.is_some() {
            "churn"
        } else if self.viewport_batches {
            "viewport_batches"
        } else {
            "serial"
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum AffinityArg {
    PerRequest,
    PerSession,
}

#[derive(Clone, Copy, Eq, PartialEq, ValueEnum)]
enum CacheModeArg {
    Real,
    Modeled,
}

#[derive(Clone, Copy, Eq, PartialEq, ValueEnum)]
enum PeerTileCacheArg {
    Entry,
    OwnerOnly,
}

impl CacheModeArg {
    fn label(self) -> &'static str {
        match self {
            Self::Real => "real",
            Self::Modeled => "modeled",
        }
    }
}

/// Rejects output/output and output/input aliases before any artifact is read
/// or opened for writing. Identity resolution is shared with Biei; labels and
/// errors remain CLI-owned.
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

async fn cleanup_on_error<T>(
    result: Result<T>,
    cleanup: impl Future<Output = Result<()>>,
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => match cleanup.await {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(error.context(format!(
                "simulated cluster cleanup also failed: {cleanup_error:#}"
            ))),
        },
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(command) = cli.command {
        match command {
            Command::Visualize(args) => {
                let output = args
                    .output
                    .unwrap_or_else(|| args.input.with_extension("html"));
                reject_output_collisions(
                    &[(output.as_path(), "output")],
                    &[(args.input.as_path(), "input report")],
                )?;
                write_visualization(&args.input, &output)?;
                eprintln!("wrote visualization to {}", output.display());
            }
            Command::Sweep(args) => {
                reject_output_collisions(
                    &[(args.output.as_path(), "output")],
                    &[(args.spec.as_path(), "sweep spec")],
                )?;
                run_sweep(&args.spec, &args.output).await?;
                eprintln!("wrote sweep results to {}", args.output.display());
            }
            Command::ReplayHttp(args) => {
                let (config, output) = args.into_config()?;
                let report = run_http_replay(config).await?;
                let successful = report.is_success();
                let file = File::create(&output)
                    .with_context(|| format!("create HTTP replay report {}", output.display()))?;
                let mut writer = BufWriter::new(file);
                serde_json::to_writer_pretty(&mut writer, &report)
                    .context("serialize HTTP replay report")?;
                writer.flush().context("flush HTTP replay report")?;
                eprintln!("wrote HTTP replay report to {}", output.display());
                if !successful {
                    bail!("HTTP replay completed with failures; inspect the report");
                }
            }
        }
        return Ok(());
    }
    let args = cli.simulation;
    args.validate()?;
    args.validate_artifact_paths()?;
    if args.uses_paused_time() {
        tokio::time::pause();
    }
    let cluster_config = args.cluster_config()?;
    let mut cluster = if args.simulate && args.cache_mode == CacheModeArg::Real {
        Some(SimCluster::new(cluster_config.clone()).await?)
    } else {
        None
    };
    let execution = run_simulation(&args, &cluster_config, &mut cluster).await;
    let SimulationExecution {
        modeled_result,
        catalog_tiles,
        timing,
        churn,
        trace_source,
    } = if let Some(cluster) = cluster.as_ref() {
        cleanup_on_error(execution, cluster.shutdown()).await?
    } else {
        execution?
    };

    let result = if let Some(result) = modeled_result {
        Some(result)
    } else if let Some(cluster) = cluster.take() {
        Some(cluster.report().await?)
    } else {
        None
    };
    if let Some(result) = result {
        let report = RunReport {
            schema_version: REPORT_SCHEMA_VERSION,
            execution_mode: args.execution_mode(),
            cache_mode: args.cache_mode.label(),
            catalog_tiles,
            timing,
            backend_latency_profile: args.backend_latency_profile,
            churn_plan: args.churn_plan,
            churn,
            trace: trace_source,
            cluster: cluster_config,
            result,
        };
        if let Some(path) = args.report {
            let file = File::create(&path)
                .with_context(|| format!("create report file {}", path.display()))?;
            let mut report_writer = BufWriter::new(file);
            serde_json::to_writer_pretty(&mut report_writer, &report)
                .context("serialize simulation report")?;
            report_writer.flush().context("flush simulation report")?;
        } else {
            eprintln!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

async fn run_simulation(
    args: &SimulationArgs,
    cluster_config: &ClusterConfig,
    cluster: &mut Option<SimCluster>,
) -> Result<SimulationExecution> {
    let mut modeled_result = None;
    let mut catalog_tiles = None;
    let mut timing = None;
    let mut churn = None;
    let trace_source = if let Some(input) = &args.input_trace {
        let file =
            File::open(input).with_context(|| format!("open trace file {}", input.display()))?;
        let entries = read_trace(BufReader::new(file))?;
        match args.cache_mode {
            CacheModeArg::Real => {
                let cluster = cluster.as_mut().expect("real simulation cluster");
                if args.phase2 {
                    timing = Some(run_timed_trace(cluster, &entries, args.timed_config()).await?);
                } else if let Some(path) = &args.churn_plan {
                    let plan = ChurnPlan::from_path(path)?;
                    churn = Some(
                        run_churn_trace(
                            cluster,
                            &entries,
                            args.viewport_batches,
                            &plan,
                            args.churn_config(),
                        )
                        .await?,
                    );
                } else {
                    replay_trace(cluster, &entries, args.viewport_batches).await?;
                }
            }
            CacheModeArg::Modeled => {
                let (result, tile_count, churn_report) =
                    run_modeled_simulation(args, cluster_config, &entries).await?;
                catalog_tiles = Some(tile_count);
                modeled_result = Some(result);
                churn = churn_report;
            }
        }
        eprintln!(
            "replayed {} requests from {}",
            entries.len(),
            input.display()
        );
        TraceSource::Replay {
            input: input.clone(),
            requests: entries.len(),
        }
    } else {
        let source = generate_trace(args, cluster.as_mut()).await?;
        if args.cache_mode == CacheModeArg::Modeled {
            let output = args
                .output
                .as_ref()
                .expect("validated generated trace output");
            let file = File::open(output)
                .with_context(|| format!("open generated trace file {}", output.display()))?;
            let entries = read_trace(BufReader::new(file))?;
            let (result, tile_count, churn_report) =
                run_modeled_simulation(args, cluster_config, &entries).await?;
            catalog_tiles = Some(tile_count);
            modeled_result = Some(result);
            churn = churn_report;
            eprintln!("simulated {} generated requests", entries.len());
        }
        source
    };

    Ok(SimulationExecution {
        modeled_result,
        catalog_tiles,
        timing,
        churn,
        trace_source,
    })
}

async fn run_modeled_simulation(
    args: &SimulationArgs,
    cluster_config: &ClusterConfig,
    entries: &[TraceEntry],
) -> Result<(ishikari_sim::SimReport, usize, Option<ChurnReport>)> {
    let catalog = TileCatalog::build(&args.tileset_sources, entries).await?;
    let tile_count = catalog.len();
    let mut modeled = ModeledCluster::new(cluster_config.clone(), catalog)?;
    let churn = if let Some(path) = &args.churn_plan {
        let plan = ChurnPlan::from_path(path)?;
        Some(run_modeled_churn_trace(
            &mut modeled,
            entries,
            args.viewport_batches,
            &plan,
            args.churn_config(),
        )?)
    } else {
        replay_modeled_trace(&mut modeled, entries, args.viewport_batches)?;
        None
    };
    Ok((modeled.report(), tile_count, churn))
}

async fn generate_trace(
    args: &SimulationArgs,
    mut cluster: Option<&mut SimCluster>,
) -> Result<TraceSource> {
    let census = File::open(&args.census)
        .with_context(|| format!("open census file {}", args.census.display()))?;
    let population = Arc::new(PopulationCdf::from_reader(BufReader::new(census))?);
    let workload_config = WorkloadConfig {
        tileset: args.tileset.clone(),
        users: args.users,
        seed: args.seed,
        min_zoom: args.min_zoom,
        max_zoom: args.max_zoom,
        focus_zoom: args.focus_zoom,
        zoom_sigma: args.zoom_sigma,
        session_reset_probability: args.session_reset_probability,
        zoom_walk_probability: args.zoom_walk_probability,
        move_step_tiles: args.move_step_tiles,
        node_count: args.nodes,
        entry_affinity: match args.entry_affinity {
            AffinityArg::PerRequest => EntryAffinity::PerRequest,
            AffinityArg::PerSession => EntryAffinity::PerSession,
        },
    };
    let output_path = args
        .output
        .as_ref()
        .context("--output is required when generating a trace")?;
    let output = File::create(output_path)
        .with_context(|| format!("create trace file {}", output_path.display()))?;
    let mut writer = BufWriter::new(output);
    let mut workload = Workload::new(workload_config.clone(), population.clone())?;
    let mut request_count = 0_u64;

    for step in 0..args.steps {
        for user in 0..workload_config.users {
            let entries = workload.step(step, user)?;
            if let Some(cluster) = cluster.as_deref_mut() {
                serve_entries(cluster, &entries, args.viewport_batches).await?;
            }
            for entry in &entries {
                write_trace_entry(&mut writer, entry)?;
                request_count += 1;
            }
        }
    }
    writer.flush().context("flush trace")?;
    eprintln!(
        "wrote {request_count} requests from {} population points (weight {:.0}) to {}",
        population.point_count(),
        population.total_weight(),
        output_path.display()
    );

    Ok(TraceSource::Generated {
        census: args.census.clone(),
        steps: args.steps,
        workload: workload_config,
        output: output_path.clone(),
    })
}

async fn replay_trace(
    cluster: &mut SimCluster,
    entries: &[TraceEntry],
    viewport_batches: bool,
) -> Result<()> {
    if viewport_batches {
        for range in viewport_batch_ranges(entries)? {
            cluster.serve_viewport(&entries[range]).await?;
        }
    } else {
        for entry in entries {
            cluster.serve(entry).await?;
        }
    }
    Ok(())
}

async fn serve_entries(
    cluster: &mut SimCluster,
    entries: &[TraceEntry],
    viewport_batches: bool,
) -> Result<()> {
    if viewport_batches {
        cluster.serve_viewport(entries).await
    } else {
        for entry in entries {
            cluster.serve(entry).await?;
        }
        Ok(())
    }
}

fn replay_modeled_trace(
    cluster: &mut ModeledCluster,
    entries: &[TraceEntry],
    viewport_batches: bool,
) -> Result<()> {
    if viewport_batches {
        for range in viewport_batch_ranges(entries)? {
            cluster.serve_viewport(&entries[range])?;
        }
    } else {
        for entry in entries {
            cluster.serve(entry)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::{Cli, cleanup_on_error, reject_output_collisions};
    use anyhow::anyhow;
    use clap::Parser;

    #[tokio::test]
    async fn execution_error_waits_for_cleanup_and_remains_primary() {
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleanup_observation = Arc::clone(&cleaned_up);

        let error = cleanup_on_error::<()>(Err(anyhow!("replay failed")), async move {
            tokio::task::yield_now().await;
            cleanup_observation.store(true, Ordering::Relaxed);
            Ok(())
        })
        .await
        .expect_err("execution must fail");

        assert!(cleaned_up.load(Ordering::Relaxed));
        assert_eq!(error.to_string(), "replay failed");
    }

    #[tokio::test]
    async fn cleanup_failure_is_attached_to_the_execution_error() {
        let error = cleanup_on_error::<()>(Err(anyhow!("timed replay failed")), async {
            Err(anyhow!("membership shutdown failed"))
        })
        .await
        .expect_err("execution and cleanup must fail");
        let message = format!("{error:#}");

        assert!(message.contains("timed replay failed"));
        assert!(message.contains("membership shutdown failed"));
    }

    #[tokio::test]
    async fn successful_execution_defers_cleanup_to_report_construction() {
        let result = cleanup_on_error(Ok(7), async {
            panic!("successful execution must not run early cleanup");
            #[allow(unreachable_code)]
            Ok(())
        })
        .await
        .expect("successful execution");

        assert_eq!(result, 7);
    }

    #[test]
    fn generated_trace_and_report_must_not_alias() {
        let path = std::env::temp_dir().join("ishikari-sim-generated-report-collision.jsonl");
        let cli = Cli::try_parse_from([
            "ishikari-sim",
            "--output",
            path.to_str().expect("UTF-8 test path"),
            "--simulate",
            "--report",
            path.to_str().expect("UTF-8 test path"),
        ])
        .expect("parse simulation arguments");

        assert!(cli.simulation.validate_artifact_paths().is_err());
    }

    #[test]
    fn report_must_not_alias_the_input_trace_through_dot() {
        let directory = std::env::temp_dir();
        let input = directory.join("ishikari-sim-input-report-collision.jsonl");
        let report = directory
            .join(".")
            .join("ishikari-sim-input-report-collision.jsonl");
        let cli = Cli::try_parse_from([
            "ishikari-sim",
            "--input-trace",
            input.to_str().expect("UTF-8 test path"),
            "--simulate",
            "--report",
            report.to_str().expect("UTF-8 test path"),
        ])
        .expect("parse simulation arguments");

        assert!(cli.simulation.validate_artifact_paths().is_err());
    }

    #[test]
    fn distinct_artifact_paths_are_accepted() {
        let directory = std::env::temp_dir();
        let output = directory.join("ishikari-sim-distinct-output.json");
        let input = directory.join("ishikari-sim-distinct-input.json");

        assert!(
            reject_output_collisions(
                &[(output.as_path(), "output")],
                &[(input.as_path(), "input")],
            )
            .is_ok()
        );
    }
}
