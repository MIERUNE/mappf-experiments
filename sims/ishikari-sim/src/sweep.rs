use std::{
    collections::HashSet,
    fs::{self, File},
    hash::Hash,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    ChurnConfig, ChurnPlan, ClusterConfig, EntryAffinity, ModeledCluster, SimReport, TileCatalog,
    read_trace, run_modeled_churn_trace,
};

const SWEEP_SCHEMA_VERSION: u32 = 1;
const DEFAULT_MAX_RUNS: usize = 10_000;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Deserialize)]
struct SweepSpec {
    schema_version: u32,
    trace: PathBuf,
    #[serde(default)]
    viewport_batches: bool,
    #[serde(default)]
    entry_affinity: EntryAffinity,
    #[serde(default = "default_entry_seeds")]
    entry_seeds: Vec<u64>,
    #[serde(default = "default_sample_every_requests")]
    sample_every_requests: u64,
    #[serde(default = "default_max_runs")]
    max_runs: usize,
    #[serde(default)]
    base_cluster: ClusterConfig,
    #[serde(default)]
    grid: SweepGrid,
}

#[derive(Debug, Default, Deserialize)]
struct SweepGrid {
    #[serde(default)]
    node_count: Vec<usize>,
    #[serde(default)]
    candidate_count: Vec<usize>,
    #[serde(default)]
    tile_group_size: Vec<u64>,
    #[serde(default)]
    chunk_size_bytes: Vec<u64>,
    #[serde(default)]
    max_fetch_chunks: Vec<u64>,

    #[serde(default)]
    tile_cache_max_bytes: Vec<u64>,
    #[serde(default)]
    chunk_cache_max_bytes: Vec<u64>,
    #[serde(default)]
    cache_peer_tiles: Vec<bool>,
}

#[derive(Serialize)]
struct SweepRunRecord {
    schema_version: u32,
    kind: &'static str,
    run_index: usize,
    run_count: usize,
    run_id: String,
    simulator_version: &'static str,
    execution_mode: &'static str,
    cache_mode: &'static str,
    entry_seed: u64,
    entry_affinity: EntryAffinity,
    viewport_batches: bool,
    sample_every_requests: u64,
    trace: FileFingerprint,
    sweep_spec: FileFingerprint,
    catalog_tiles: usize,
    cluster: ClusterConfig,
    churn: crate::ChurnReport,
    result: SimReport,
}

#[derive(Clone, Serialize)]
struct FileFingerprint {
    path: PathBuf,
    bytes: u64,
    fnv1a64: String,
}

/// Runs a versioned, replay-only modeled-cache parameter sweep.
///
/// The output is JSONL with one self-contained run document per line. Each line
/// is flushed before the next run starts, so an interrupted sweep leaves a valid
/// prefix that can still be analyzed.
pub async fn run_sweep(spec_path: &Path, output_path: &Path) -> Result<()> {
    let spec_bytes =
        fs::read(spec_path).with_context(|| format!("read sweep spec {}", spec_path.display()))?;
    let mut spec: SweepSpec = serde_json::from_slice(&spec_bytes)
        .with_context(|| format!("parse sweep spec {}", spec_path.display()))?;
    spec.validate()?;

    let base_dir = spec_path.parent().unwrap_or_else(|| Path::new("."));
    let trace_path = resolve_path(base_dir, &spec.trace);
    ensure!(
        output_path != spec_path && output_path != trace_path,
        "sweep output must not overwrite the sweep spec or trace"
    );
    let trace_file = File::open(&trace_path)
        .with_context(|| format!("open sweep trace {}", trace_path.display()))?;
    let entries = read_trace(BufReader::new(trace_file))?;

    let tileset_source = PathBuf::from(&spec.base_cluster.tileset_sources);
    if tileset_source.is_relative() {
        spec.base_cluster.tileset_sources = resolve_path(base_dir, &tileset_source)
            .to_string_lossy()
            .into_owned();
    }

    let run_count = spec.run_count()?;
    let configs = spec.expanded_configs()?;

    let catalog = Arc::new(TileCatalog::build(&spec.base_cluster.tileset_sources, &entries).await?);
    let catalog_tiles = catalog.len();
    let trace_fingerprint = fingerprint_file(&trace_path)?;
    let spec_fingerprint = FileFingerprint {
        path: spec_path.to_path_buf(),
        bytes: spec_bytes.len() as u64,
        fnv1a64: format_hash(fnv1a64(&spec_bytes)),
    };

    let output = File::create(output_path)
        .with_context(|| format!("create sweep output {}", output_path.display()))?;
    let mut writer = BufWriter::new(output);
    let plan = ChurnPlan::empty();
    let mut run_index = 0;

    for config in configs {
        for &entry_seed in &spec.entry_seeds {
            let mut cluster = ModeledCluster::new(config.clone(), Arc::clone(&catalog))?;
            let churn = run_modeled_churn_trace(
                &mut cluster,
                &entries,
                spec.viewport_batches,
                &plan,
                ChurnConfig {
                    seed: entry_seed,
                    entry_affinity: spec.entry_affinity,
                    sample_every_requests: spec.sample_every_requests,
                },
            )?;
            let run_id = run_id(entry_seed, &config)?;
            let record = SweepRunRecord {
                schema_version: SWEEP_SCHEMA_VERSION,
                kind: "ishikari_sim_sweep_run",
                run_index,
                run_count,
                run_id,
                simulator_version: env!("CARGO_PKG_VERSION"),
                execution_mode: if spec.viewport_batches {
                    "sweep_viewport_batches"
                } else {
                    "sweep_serial"
                },
                cache_mode: "modeled",
                entry_seed,
                entry_affinity: spec.entry_affinity,
                viewport_batches: spec.viewport_batches,
                sample_every_requests: spec.sample_every_requests,
                trace: trace_fingerprint.clone(),
                sweep_spec: spec_fingerprint.clone(),
                catalog_tiles,
                cluster: config.clone(),
                churn,
                result: cluster.report(),
            };
            serde_json::to_writer(&mut writer, &record).context("serialize sweep run")?;
            writer.write_all(b"\n").context("write sweep newline")?;
            writer.flush().context("flush sweep run")?;
            run_index += 1;
        }
    }

    Ok(())
}

impl SweepSpec {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == SWEEP_SCHEMA_VERSION,
            "unsupported sweep schema version {}; expected {SWEEP_SCHEMA_VERSION}",
            self.schema_version
        );
        ensure!(
            !self.entry_seeds.is_empty(),
            "entry_seeds must not be empty"
        );
        ensure!(
            self.sample_every_requests > 0,
            "sample_every_requests must be greater than zero"
        );
        ensure!(self.max_runs > 0, "max_runs must be greater than zero");
        validate_unique("entry_seeds", &self.entry_seeds)?;
        self.grid.validate()?;
        let run_count = self.run_count()?;
        ensure!(
            run_count <= self.max_runs,
            "sweep expands to {run_count} runs, exceeding max_runs={}",
            self.max_runs
        );
        Ok(())
    }

    fn run_count(&self) -> Result<usize> {
        self.grid
            .config_count()?
            .checked_mul(self.entry_seeds.len())
            .context("sweep run count overflow")
    }

    fn expanded_configs(&self) -> Result<Vec<ClusterConfig>> {
        let mut configs = vec![self.base_cluster.clone()];
        expand_axis(&mut configs, &self.grid.node_count, |config, value| {
            config.node_count = value;
        });
        expand_axis(&mut configs, &self.grid.candidate_count, |config, value| {
            config.candidate_count = value
        });
        expand_axis(&mut configs, &self.grid.tile_group_size, |config, value| {
            config.tile_group_size = value
        });
        expand_axis(
            &mut configs,
            &self.grid.chunk_size_bytes,
            |config, value| config.chunk_size_bytes = value,
        );
        expand_axis(
            &mut configs,
            &self.grid.max_fetch_chunks,
            |config, value| config.max_fetch_chunks = value,
        );

        expand_axis(
            &mut configs,
            &self.grid.tile_cache_max_bytes,
            |config, value| config.tile_cache_max_bytes = value,
        );
        expand_axis(
            &mut configs,
            &self.grid.chunk_cache_max_bytes,
            |config, value| config.chunk_cache_max_bytes = value,
        );
        expand_axis(
            &mut configs,
            &self.grid.cache_peer_tiles,
            |config, value| config.cache_peer_tiles = value,
        );
        for (index, config) in configs.iter().enumerate() {
            config
                .validate()
                .with_context(|| format!("invalid sweep cluster configuration {index}"))?;
        }
        Ok(configs)
    }
}

impl SweepGrid {
    fn config_count(&self) -> Result<usize> {
        [
            self.node_count.len(),
            self.candidate_count.len(),
            self.tile_group_size.len(),
            self.chunk_size_bytes.len(),
            self.max_fetch_chunks.len(),
            self.tile_cache_max_bytes.len(),
            self.chunk_cache_max_bytes.len(),
            self.cache_peer_tiles.len(),
        ]
        .into_iter()
        .try_fold(1_usize, |count, axis| {
            count
                .checked_mul(axis.max(1))
                .context("sweep configuration count overflow")
        })
    }

    fn validate(&self) -> Result<()> {
        validate_unique("grid.node_count", &self.node_count)?;
        validate_unique("grid.candidate_count", &self.candidate_count)?;
        validate_unique("grid.tile_group_size", &self.tile_group_size)?;
        validate_unique("grid.chunk_size_bytes", &self.chunk_size_bytes)?;
        validate_unique("grid.max_fetch_chunks", &self.max_fetch_chunks)?;

        validate_unique("grid.tile_cache_max_bytes", &self.tile_cache_max_bytes)?;
        validate_unique("grid.chunk_cache_max_bytes", &self.chunk_cache_max_bytes)?;
        validate_unique("grid.cache_peer_tiles", &self.cache_peer_tiles)
    }
}

fn expand_axis<T: Copy>(
    configs: &mut Vec<ClusterConfig>,
    values: &[T],
    set: impl Fn(&mut ClusterConfig, T),
) {
    if values.is_empty() {
        return;
    }
    let previous = std::mem::take(configs);
    configs.reserve(previous.len().saturating_mul(values.len()));
    for config in previous {
        for &value in values {
            let mut expanded = config.clone();
            set(&mut expanded, value);
            configs.push(expanded);
        }
    }
}

fn validate_unique<T: Copy + Eq + Hash>(name: &str, values: &[T]) -> Result<()> {
    let mut seen = HashSet::with_capacity(values.len());
    ensure!(
        values.iter().all(|value| seen.insert(*value)),
        "{name} contains duplicate values"
    );
    Ok(())
}

fn resolve_path(base_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

fn fingerprint_file(path: &Path) -> Result<FileFingerprint> {
    let mut file =
        File::open(path).with_context(|| format!("open {} for hashing", path.display()))?;
    let mut buffer = [0_u8; 64 * 1024];
    let mut hash = FNV_OFFSET_BASIS;
    let mut bytes = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            break;
        }
        bytes = bytes.saturating_add(read as u64);
        hash = fnv1a64_continue(hash, &buffer[..read]);
    }
    Ok(FileFingerprint {
        path: path.to_path_buf(),
        bytes,
        fnv1a64: format_hash(hash),
    })
}

fn run_id(entry_seed: u64, config: &ClusterConfig) -> Result<String> {
    let bytes =
        serde_json::to_vec(&(entry_seed, config)).context("serialize sweep run identity")?;
    Ok(format_hash(fnv1a64(&bytes)))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv1a64_continue(FNV_OFFSET_BASIS, bytes)
}

fn fnv1a64_continue(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn format_hash(hash: u64) -> String {
    format!("fnv1a64:{hash:016x}")
}

fn default_entry_seeds() -> Vec<u64> {
    vec![1]
}

const fn default_sample_every_requests() -> u64 {
    1_000
}

const fn default_max_runs() -> usize {
    DEFAULT_MAX_RUNS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_spec(json: &str) -> SweepSpec {
        serde_json::from_str(json).expect("parse sweep spec")
    }

    #[test]
    fn cluster_defaults_and_grid_expand_in_canonical_order() {
        let spec = parse_spec(
            r#"{
                "schema_version": 1,
                "trace": "trace.jsonl",
                "grid": {
                    "node_count": [2, 3],
                    "cache_peer_tiles": [true, false]
                }
            }"#,
        );
        spec.validate().expect("valid spec");
        let configs = spec.expanded_configs().expect("expanded configs");

        assert_eq!(spec.entry_seeds, [1]);
        assert_eq!(spec.run_count().expect("run count"), 4);
        assert_eq!(configs.len(), 4);
        assert_eq!(configs[0].node_count, 2);
        assert!(configs[0].cache_peer_tiles);
        assert_eq!(configs[1].node_count, 2);
        assert!(!configs[1].cache_peer_tiles);
        assert_eq!(configs[2].node_count, 3);
        assert!(configs[2].cache_peer_tiles);
        assert_eq!(configs[0].chunk_fetch_merge_window_ms, 10);
    }

    #[test]
    fn rejects_unknown_schema_and_duplicate_axes() {
        let unknown = parse_spec(r#"{"schema_version":2,"trace":"trace.jsonl"}"#);
        assert!(
            unknown
                .validate()
                .unwrap_err()
                .to_string()
                .contains("schema version 2")
        );

        let duplicate = parse_spec(
            r#"{
                "schema_version": 1,
                "trace": "trace.jsonl",
                "grid": {"node_count": [3, 3]}
            }"#,
        );
        assert!(
            duplicate
                .validate()
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );

        let too_large = parse_spec(
            r#"{
                "schema_version": 1,
                "trace": "trace.jsonl",
                "entry_seeds": [1, 2],
                "max_runs": 3,
                "grid": {"node_count": [2, 3]}
            }"#,
        );
        assert!(
            too_large
                .validate()
                .unwrap_err()
                .to_string()
                .contains("exceeding max_runs=3")
        );
    }

    #[test]
    fn fnv_hash_is_stable() {
        assert_eq!(format_hash(fnv1a64(b"hello")), "fnv1a64:a430d84680aabd0b");
    }
}
