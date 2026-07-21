//! Object-store fetch implementation for chunked reads.

use std::{
    ops::Range,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use mmpf_common::resource_templates::{
    NamespaceKeyPolicy, NamespacedEntries, NamespacedEntriesPolicy,
};
use mmpf_common::rng::{splitmix64, uniform_open};
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path as ObjectPath,
};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::debug;
use url::Url;

use crate::{
    interned::TilesetId,
    metrics::NodeMetrics,
    storage::{ObjectStoreRegistry, store_registry::redacted_source_label},
};

pub(super) const BACKEND_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MIB_BYTES: f64 = (1024 * 1024) as f64;

/// Deterministic latency injected before an object-store range fetch.
///
/// Production uses [`Self::fixed`]. The simulator can use [`Self::lognormal`]
/// to replay an empirical time-to-first-byte distribution plus a transfer-time
/// term proportional to the requested range size.
#[derive(Clone, Copy, Debug)]
pub struct BackendLatencyModel {
    median_ms: f64,
    lognormal_sigma: f64,
    transfer_ms_per_mib: f64,
    seed: u64,
}

impl BackendLatencyModel {
    pub fn fixed(delay_ms: u64) -> Self {
        Self {
            median_ms: delay_ms as f64,
            lognormal_sigma: 0.0,
            transfer_ms_per_mib: 0.0,
            seed: 0,
        }
    }

    #[cfg_attr(not(any(test, feature = "simulator-support")), allow(dead_code))]
    pub fn lognormal(
        median_ms: f64,
        lognormal_sigma: f64,
        transfer_ms_per_mib: f64,
        seed: u64,
    ) -> Result<Self> {
        if !median_ms.is_finite() || median_ms < 0.0 {
            bail!("backend latency median must be finite and non-negative");
        }
        if !lognormal_sigma.is_finite() || lognormal_sigma < 0.0 {
            bail!("backend latency sigma must be finite and non-negative");
        }
        if !transfer_ms_per_mib.is_finite() || transfer_ms_per_mib < 0.0 {
            bail!("backend transfer latency must be finite and non-negative");
        }
        Ok(Self {
            median_ms,
            lognormal_sigma,
            transfer_ms_per_mib,
            seed,
        })
    }

    fn delay(self, sequence: u64, range_start: u64, range_bytes: u64) -> Duration {
        let base_ms = if self.lognormal_sigma == 0.0 {
            self.median_ms
        } else {
            let first = uniform_open(splitmix64(
                self.seed ^ sequence.rotate_left(17) ^ range_start.rotate_left(31),
            ));
            let second = uniform_open(splitmix64(
                self.seed.rotate_left(29) ^ sequence ^ range_start.rotate_left(43),
            ));
            let standard_normal =
                (-2.0 * first.ln()).sqrt() * (std::f64::consts::TAU * second).cos();
            self.median_ms * (self.lognormal_sigma * standard_normal).exp()
        };
        let transfer_ms = range_bytes as f64 / MIB_BYTES * self.transfer_ms_per_mib;
        Duration::from_secs_f64(((base_ms + transfer_ms).max(0.0) / 1_000.0).min(86_400.0))
    }
}

/// Errors produced while fetching raw backend chunks.
#[derive(Clone, Debug, Error)]
pub(crate) enum ChunkFetchError {
    #[error("object not found")]
    NotFound,
    #[error("{0}")]
    Overloaded(String),
    /// The backend read exceeded `PROVIDER_FETCH_TIMEOUT` / the range-read
    /// timeout. Kept as a typed variant so callers classify timeouts without
    /// matching on the message string.
    #[error("{0}")]
    Timeout(String),
    /// The object-store operation failed for a reason other than authoritative
    /// absence. Preserved separately from local range and coordination errors.
    #[error("{0}")]
    Backend(String),
    #[error("{0}")]
    Message(String),
}

/// One object-store root that backs some set of tilesets.
struct TilesetSource {
    object_store: Arc<dyn ObjectStore>,
    base_path: ObjectPath,
}

/// Resolves a tileset key to the object-store root that backs it.
///
/// `TILESET_SOURCES` accepts the same `namespace=url;…;default=url` form as the style
/// and sprite templates (a bare URL is the default root). A namespaced tileset
/// key whose first segment matches a configured namespace is served from that
/// root with the namespace stripped (`regional/streets` → `{root}/streets.pmtiles`);
/// any other key falls to the default root with its full path preserved
/// (`analysis/hrnowc` → `{default}/analysis/hrnowc.pmtiles`).
#[derive(Clone)]
struct TilesetSources {
    entries: NamespacedEntries<Arc<TilesetSource>>,
}

impl TilesetSources {
    fn parse(spec: &str, registry: &ObjectStoreRegistry) -> Result<Self> {
        let entries = NamespacedEntries::parse(
            spec,
            NamespacedEntriesPolicy {
                config_name: "TILESET_SOURCES",
                entry_name: "source",
                namespace_keys: NamespaceKeyPolicy::AsciiIdentifier,
            },
        )
        .map_err(anyhow::Error::new)?
        .try_map(|namespace, source_url| {
            let source_name = namespace.unwrap_or("default");
            build_source(&source_url, registry)
                .with_context(|| format!("failed to configure tileset source {source_name:?}"))
                .map(Arc::new)
        })?;
        Ok(Self { entries })
    }

    /// Returns the backing store and object path for a tileset key, or `None`
    /// when no namespace matches and no default root is configured.
    fn resolve(&self, tileset_id: &str) -> Option<(Arc<dyn ObjectStore>, ObjectPath)> {
        let selected = self.entries.select(tileset_id)?;
        let source = selected.value();
        Some((
            source.object_store.clone(),
            object_path_under(&source.base_path, selected.relative_key()),
        ))
    }
}

#[derive(Clone)]
pub(super) struct ChunkFetcher {
    sources: TilesetSources,
    chunk_size: u64,
    backend_latency: BackendLatencyModel,
    backend_fetch_permits: Arc<Semaphore>,
    fetch_sequence: Arc<AtomicU64>,
    received_bytes: Arc<AtomicU64>,
    metrics: NodeMetrics,
}

impl ChunkFetcher {
    pub(super) fn new(
        tileset_sources: String,
        chunk_size: u64,
        backend_fetch_concurrency: usize,
        backend_latency: BackendLatencyModel,
        registry: &ObjectStoreRegistry,
        metrics: NodeMetrics,
    ) -> Result<Self> {
        let sources = TilesetSources::parse(&tileset_sources, registry)?;
        let backend_fetch_concurrency = backend_fetch_concurrency.max(1);
        metrics.set_backend_fetch_concurrency_limit(backend_fetch_concurrency);
        Ok(Self {
            sources,
            chunk_size,
            backend_latency,
            backend_fetch_permits: Arc::new(Semaphore::new(backend_fetch_concurrency)),
            fetch_sequence: Arc::new(AtomicU64::new(0)),
            received_bytes: Arc::new(AtomicU64::new(0)),
            metrics,
        })
    }

    pub(super) fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub(super) fn received_bytes(&self) -> u64 {
        self.received_bytes.load(Ordering::Relaxed)
    }

    pub(super) async fn fetch_chunk_group(
        &self,
        tileset_id: &TilesetId,
        chunk_range: Range<u64>,
        archive_len: u64,
    ) -> std::result::Result<Bytes, ChunkFetchError> {
        if chunk_range.start >= chunk_range.end {
            return Ok(Bytes::new());
        }

        let start_chunk = chunk_range.start;
        let end_chunk = chunk_range.end;
        // Chunk indices derive from PMTiles offsets read off the backend; guard the
        // span math so a corrupt archive yields a clean error rather than a wrapped
        // (and possibly reversed) range.
        let (Some(range_start), Some(range_end)) = (
            start_chunk.checked_mul(self.chunk_size),
            end_chunk.checked_mul(self.chunk_size),
        ) else {
            return Err(ChunkFetchError::Message(format!(
                "chunk range overflow: start_chunk={start_chunk} end_chunk={end_chunk} chunk_size={}",
                self.chunk_size
            )));
        };
        let range_end = range_end.min(archive_len);
        if range_start >= range_end {
            return Err(ChunkFetchError::Message(format!(
                "chunk range start {range_start} does not precede end {range_end} (archive_len={archive_len})"
            )));
        }
        let prefetched_chunks = end_chunk - start_chunk;
        debug!(
            tileset_id = %tileset_id,
            start_chunk = start_chunk,
            end_chunk = end_chunk,
            prefetched_chunks = prefetched_chunks,
            prefetched_bytes = range_end - range_start,
            "fetching backend chunks"
        );

        self.fetch_range(
            tileset_id,
            range_start..range_end,
            prefetched_chunks,
            RangeLengthPolicy::Exact,
        )
        .await
    }

    /// Fetches a bounded non-cacheable range before the archive length is known.
    /// `object_store` defines a bounded range past EOF as the available
    /// remainder, so valid short archives need no preceding metadata request.
    pub(super) async fn fetch_exact_range(
        &self,
        tileset_id: &TilesetId,
        range: Range<u64>,
    ) -> std::result::Result<Bytes, ChunkFetchError> {
        if range.start >= range.end {
            return Ok(Bytes::new());
        }
        self.fetch_range(tileset_id, range, 1, RangeLengthPolicy::AllowShortAtEof)
            .await
    }

    async fn fetch_range(
        &self,
        tileset_id: &TilesetId,
        range: Range<u64>,
        fetched_chunks: u64,
        range_length_policy: RangeLengthPolicy,
    ) -> std::result::Result<Bytes, ChunkFetchError> {
        let (object_store, path) = self.sources.resolve(tileset_id.as_str()).ok_or_else(|| {
            ChunkFetchError::Message(format!(
                "no data source configured for tileset {tileset_id}"
            ))
        })?;
        let range_start = range.start;
        let requested_range_end = range.end;
        let group_started_at = tokio::time::Instant::now();
        let deadline = group_started_at + BACKEND_FETCH_TIMEOUT;

        // The coordinator bounds all admitted groups process-wide; this second
        // semaphore bounds the subset actively using object-store capacity.
        // The metric guard is cancellation-safe while a group waits here.
        let queue_started_at = tokio::time::Instant::now();
        let waiting = BackendFetchGaugeGuard::new(self.metrics.clone(), "waiting");
        let permit = match tokio::time::timeout_at(
            deadline,
            self.backend_fetch_permits.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                return Err(ChunkFetchError::Message(
                    "backend fetch admission closed".into(),
                ));
            }
            Err(error) => {
                drop(waiting);
                self.metrics
                    .record_backend_fetch_queue(queue_started_at.elapsed());
                self.metrics.record_backend_fetch(
                    "timeout",
                    group_started_at.elapsed(),
                    fetched_chunks,
                    0,
                );
                return Err(ChunkFetchError::Timeout(format!(
                    "timed out waiting for backend fetch admission: {error}"
                )));
            }
        };
        drop(waiting);
        self.metrics
            .record_backend_fetch_queue(queue_started_at.elapsed());
        let _active = BackendFetchGaugeGuard::new(self.metrics.clone(), "active");
        let _permit = permit;

        // Tokio's clock is wall-clock backed in production and virtual under
        // the simulator's paused-time runtime, so this metric remains useful in
        // both environments. Queue time is recorded separately above.
        let fetch_started_at = tokio::time::Instant::now();

        let sequence = self.fetch_sequence.fetch_add(1, Ordering::Relaxed);
        let fetch_result = tokio::time::timeout_at(deadline, async {
            let requested_bytes = requested_range_end.saturating_sub(range_start);
            let backend_delay = self
                .backend_latency
                .delay(sequence, range_start, requested_bytes);
            if !backend_delay.is_zero() {
                tokio::time::sleep(backend_delay).await;
            }
            object_store
                .get_range(&path, range_start..requested_range_end)
                .await
        })
        .await;
        let record_backend_fetch = |outcome: &str, bytes: u64| {
            self.metrics.record_backend_fetch(
                outcome,
                fetch_started_at.elapsed(),
                fetched_chunks,
                bytes,
            );
        };
        let bytes = match fetch_result {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                record_backend_fetch(backend_fetch_outcome(&error), 0);
                return Err(ChunkFetchError::from(error));
            }
            Err(error) => {
                record_backend_fetch("timeout", 0);
                return Err(ChunkFetchError::Timeout(format!(
                    "timed out fetching object-store range {range_start}..{requested_range_end}: {error}"
                )));
            }
        };
        let requested_bytes = requested_range_end.saturating_sub(range_start);
        let expected_len = usize::try_from(requested_bytes).map_err(|_| {
            ChunkFetchError::Message(format!(
                "backend range length does not fit memory: range={range_start}..{requested_range_end}"
            ))
        })?;
        let invalid_length = bytes.len() > expected_len
            || (range_length_policy == RangeLengthPolicy::Exact && bytes.len() != expected_len);
        if invalid_length {
            let received_bytes = bytes.len() as u64;
            self.received_bytes
                .fetch_add(received_bytes, Ordering::Relaxed);
            record_backend_fetch("error", received_bytes);
            return Err(ChunkFetchError::Message(format!(
                "unexpected object-store range length: range={range_start}..{requested_range_end} expected_bytes={expected_len} actual_bytes={} policy={range_length_policy:?}",
                bytes.len()
            )));
        }
        self.received_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        record_backend_fetch("success", bytes.len() as u64);
        debug!(
            tileset_id = %tileset_id,
            range_start,
            requested_range_end,
            fetched_chunks,
            backend_fetched_bytes = bytes.len(),
            duration_ms = fetch_started_at.elapsed().as_millis() as u64,
            "fetched backend bytes"
        );

        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RangeLengthPolicy {
    Exact,
    AllowShortAtEof,
}

struct BackendFetchGaugeGuard {
    metrics: NodeMetrics,
    state: &'static str,
}

impl BackendFetchGaugeGuard {
    fn new(metrics: NodeMetrics, state: &'static str) -> Self {
        metrics.adjust_backend_fetch_concurrency(state, 1);
        Self { metrics, state }
    }
}

impl Drop for BackendFetchGaugeGuard {
    fn drop(&mut self) {
        self.metrics
            .adjust_backend_fetch_concurrency(self.state, -1);
    }
}

fn backend_fetch_outcome(error: &ObjectStoreError) -> &'static str {
    if matches!(error, ObjectStoreError::NotFound { .. }) {
        "not_found"
    } else {
        "error"
    }
}

impl From<ObjectStoreError> for ChunkFetchError {
    fn from(error: ObjectStoreError) -> Self {
        if matches!(error, ObjectStoreError::NotFound { .. }) {
            return Self::NotFound;
        }
        Self::Backend(format!("object-store backend failure: {error}"))
    }
}

/// Builds an object path under `base` from a `/`-delimited key, appending the
/// `.pmtiles` extension to the final segment. object_store's `Path::join`
/// encodes its whole argument as one segment, so each segment is joined
/// separately.
fn object_path_under(base: &ObjectPath, relative_key: &str) -> ObjectPath {
    let mut path = base.clone();
    let mut parts = relative_key.split('/').peekable();
    while let Some(part) = parts.next() {
        if parts.peek().is_some() {
            path = path.join(part);
        } else {
            path = path.join(format!("{part}.pmtiles"));
        }
    }
    path
}

fn build_source(source_url: &str, registry: &ObjectStoreRegistry) -> Result<TilesetSource> {
    let url = normalize_source_url(source_url)?;
    // The registry dedups stores by bucket/host, so multiple namespaces (or the
    // provider layer) backed by the same bucket share one store and pool.
    let (object_store, base_path) = registry
        .resolve(&url)
        .with_context(|| format!("failed to resolve {}", redacted_source_label(&url)))?;
    Ok(TilesetSource {
        object_store,
        base_path,
    })
}

fn normalize_source_url(source_url: &str) -> Result<Url> {
    if let Ok(url) = Url::parse(source_url) {
        return Ok(url);
    }

    let path = std::fs::canonicalize(PathBuf::from(source_url)).map_err(|error| {
        anyhow!(
            "failed to resolve configured local data path ({:?})",
            error.kind()
        )
    })?;
    Url::from_directory_path(path)
        .map_err(|_| anyhow!("failed to convert local path to file:// URL"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        BACKEND_FETCH_TIMEOUT, BackendLatencyModel, ChunkFetchError, ChunkFetcher, TilesetSources,
        object_path_under,
    };
    use crate::{interned::TilesetId, metrics::NodeMetrics, storage::ObjectStoreRegistry};
    use object_store::{Error as ObjectStoreError, path::Path as ObjectPath};

    #[test]
    fn source_configuration_errors_do_not_echo_raw_values() {
        let secret = "do-not-log-this-password";
        let spec = format!("regional=http://alice:{secret}@[invalid");

        let error = match TilesetSources::parse(&spec, &ObjectStoreRegistry::without_options()) {
            Ok(_) => panic!("malformed source must fail"),
            Err(error) => error,
        };
        let diagnostic = format!("{error:#}");

        assert!(diagnostic.contains("tileset source \"regional\""));
        assert!(diagnostic.contains("configured local data path"));
        for sensitive in [secret, "alice", "[invalid", &spec] {
            assert!(
                !diagnostic.contains(sensitive),
                "error leaked {sensitive:?}: {diagnostic}"
            );
        }
    }

    #[test]
    fn object_store_failures_remain_typed_as_backend_errors() {
        let error = ObjectStoreError::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("service unavailable")),
        };

        assert!(matches!(
            ChunkFetchError::from(error),
            ChunkFetchError::Backend(message) if message.contains("service unavailable")
        ));
    }

    #[test]
    fn builds_nested_object_paths_with_extension() {
        let base = ObjectPath::from("prefix");
        assert_eq!(
            object_path_under(&base, "voyager").as_ref(),
            "prefix/voyager.pmtiles"
        );
        assert_eq!(
            object_path_under(&base, "analysis/hrnowc").as_ref(),
            "prefix/analysis/hrnowc.pmtiles"
        );
        assert_eq!(
            object_path_under(&ObjectPath::default(), "japan").as_ref(),
            "japan.pmtiles"
        );
    }

    #[test]
    fn fixed_backend_latency_is_constant() {
        let model = BackendLatencyModel::fixed(125);

        assert_eq!(model.delay(0, 0, 1), std::time::Duration::from_millis(125));
        assert_eq!(
            model.delay(99, 4_000_000, 4 * 1024 * 1024),
            std::time::Duration::from_millis(125)
        );
    }

    #[test]
    fn backend_latency_model_is_deterministic_and_size_aware() {
        let model = BackendLatencyModel::lognormal(55.0, 0.9, 6.0, 7).unwrap();

        let first = model.delay(11, 2_000_000, 1024 * 1024);
        assert_eq!(first, model.delay(11, 2_000_000, 1024 * 1024));
        assert_eq!(
            model
                .delay(11, 2_000_000, 2 * 1024 * 1024)
                .checked_sub(first)
                .expect("larger transfer delay must not be shorter"),
            std::time::Duration::from_millis(6)
        );
    }

    #[test]
    fn backend_latency_distribution_matches_configured_shape() {
        let model = BackendLatencyModel::lognormal(55.0, 0.9, 0.0, 23).unwrap();
        let mut samples: Vec<_> = (0..10_001)
            .map(|sequence| model.delay(sequence, sequence * 1024, 1024 * 1024))
            .collect();
        samples.sort_unstable();
        let median_ms = samples[samples.len() / 2].as_secs_f64() * 1_000.0;
        let mean_ms = samples.iter().map(|value| value.as_secs_f64()).sum::<f64>() * 1_000.0
            / samples.len() as f64;

        assert!((52.0..58.0).contains(&median_ms), "median={median_ms}");
        assert!((78.0..88.0).contains(&mean_ms), "mean={mean_ms}");
    }

    #[tokio::test(start_paused = true)]
    async fn backend_duration_metric_follows_tokio_virtual_time() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ishikari-fetcher-virtual-time-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("fixture.pmtiles"), b"abcdefgh").unwrap();

        let metrics = NodeMetrics::new();
        let fetcher = ChunkFetcher::new(
            directory.to_string_lossy().into_owned(),
            4,
            32,
            BackendLatencyModel::fixed(100),
            &ObjectStoreRegistry::without_options(),
            metrics.clone(),
        )
        .unwrap();
        let bytes = fetcher
            .fetch_chunk_group(&TilesetId::try_new("fixture").unwrap(), 0..1, 8)
            .await
            .unwrap();

        assert_eq!(bytes.as_ref(), b"abcd");
        assert_eq!(fetcher.received_bytes(), 4);
        assert!(
            metrics
                .encode()
                .contains("ishikari_backend_fetch_bytes_total 4")
        );
        let duration = metrics.histogram_snapshot().backend_fetch_duration_seconds;
        assert_eq!(duration.count, 1);
        assert!((duration.sum - 0.1).abs() < 1e-9, "sum={}", duration.sum);

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn process_wide_limit_serializes_distinct_tilesets() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ishikari-fetcher-concurrency-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("first.pmtiles"), b"abcdefgh").unwrap();
        std::fs::write(directory.join("second.pmtiles"), b"ijklmnop").unwrap();

        let metrics = NodeMetrics::new();
        let fetcher = ChunkFetcher::new(
            directory.to_string_lossy().into_owned(),
            4,
            1,
            BackendLatencyModel::fixed(100),
            &ObjectStoreRegistry::without_options(),
            metrics.clone(),
        )
        .unwrap();
        let first_fetcher = fetcher.clone();
        let first = tokio::spawn(async move {
            first_fetcher
                .fetch_chunk_group(&TilesetId::try_new("first").unwrap(), 0..1, 8)
                .await
        });
        tokio::task::yield_now().await;
        let second = tokio::spawn(async move {
            fetcher
                .fetch_chunk_group(&TilesetId::try_new("second").unwrap(), 0..1, 8)
                .await
        });
        tokio::task::yield_now().await;

        let saturated = metrics.encode();
        assert!(saturated.contains("ishikari_backend_fetch_concurrency{state=\"active\"} 1"));
        assert!(saturated.contains("ishikari_backend_fetch_concurrency{state=\"waiting\"} 1"));

        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(first.await.unwrap().unwrap().as_ref(), b"abcd");
        assert_eq!(second.await.unwrap().unwrap().as_ref(), b"ijkl");

        let encoded = metrics.encode();
        assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"active\"} 0"));
        assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"waiting\"} 0"));
        let queue = metrics
            .histogram_snapshot()
            .backend_fetch_queue_duration_seconds;
        assert_eq!(queue.count, 2);
        assert!(queue.sum >= 0.1, "queue sum={}", queue.sum);

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn backend_deadline_includes_waiting_for_the_active_fetch_permit() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ishikari-fetcher-deadline-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("first.pmtiles"), b"abcdefgh").unwrap();
        std::fs::write(directory.join("second.pmtiles"), b"ijklmnop").unwrap();

        let fetcher = ChunkFetcher::new(
            directory.to_string_lossy().into_owned(),
            4,
            1,
            BackendLatencyModel::fixed(9_000),
            &ObjectStoreRegistry::without_options(),
            NodeMetrics::new(),
        )
        .unwrap();
        let first_fetcher = fetcher.clone();
        let first = tokio::spawn(async move {
            first_fetcher
                .fetch_chunk_group(&TilesetId::try_new("first").unwrap(), 0..1, 8)
                .await
        });
        tokio::task::yield_now().await;

        let started_at = tokio::time::Instant::now();
        let error = fetcher
            .fetch_chunk_group(&TilesetId::try_new("second").unwrap(), 0..1, 8)
            .await
            .expect_err("second fetch must exhaust its end-to-end deadline");
        assert!(matches!(error, ChunkFetchError::Timeout(_)));
        assert_eq!(started_at.elapsed(), BACKEND_FETCH_TIMEOUT);
        assert_eq!(first.await.unwrap().unwrap().as_ref(), b"abcd");

        std::fs::remove_dir_all(directory).unwrap();
    }
}
