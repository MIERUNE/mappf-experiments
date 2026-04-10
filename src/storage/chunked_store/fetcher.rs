//! Object-store fetch implementation for chunked reads.

use std::{
    collections::HashSet,
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
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path as ObjectPath,
};
use thiserror::Error;
use tracing::debug;
use url::Url;

use crate::{interned::TilesetId, metrics::NodeMetrics, storage::ObjectStoreRegistry};

const BACKEND_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors produced while fetching raw backend chunks.
#[derive(Clone, Debug, Error)]
pub enum ChunkFetchError {
    #[error("object not found")]
    NotFound,
    /// The backend read exceeded `PROVIDER_FETCH_TIMEOUT` / the range-read
    /// timeout. Kept as a typed variant so callers classify timeouts without
    /// matching on the message string.
    #[error("{0}")]
    Timeout(String),
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
    namespaces: Vec<(String, Arc<TilesetSource>)>,
    default: Option<Arc<TilesetSource>>,
}

impl TilesetSources {
    fn parse(spec: &str, registry: &ObjectStoreRegistry) -> Result<Self> {
        let mut namespaces = Vec::new();
        let mut default = None;
        for (namespace, url) in parse_source_entries(spec)? {
            let source = Arc::new(build_source(&url, registry)?);
            match namespace {
                None => default = Some(source),
                Some(name) => namespaces.push((name, source)),
            }
        }
        Ok(Self {
            namespaces,
            default,
        })
    }

    /// Returns the backing store and object path for a tileset key, or `None`
    /// when no namespace matches and no default root is configured.
    fn resolve(&self, tileset_id: &str) -> Option<(Arc<dyn ObjectStore>, ObjectPath)> {
        let names: Vec<&str> = self
            .namespaces
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        let (selected, relative) = select_namespace(tileset_id, &names);
        let source = match selected {
            Some(index) => &self.namespaces[index].1,
            None => self.default.as_ref()?,
        };
        Some((
            source.object_store.clone(),
            object_path_under(&source.base_path, relative),
        ))
    }
}

#[derive(Clone)]
pub struct ChunkFetcher {
    sources: TilesetSources,
    chunk_size: u64,
    artificial_backend_delay: Duration,
    received_bytes: Arc<AtomicU64>,
    metrics: NodeMetrics,
}

impl ChunkFetcher {
    pub fn new(
        tileset_sources: String,
        chunk_size: u64,
        artificial_backend_delay_ms: u64,
        registry: &ObjectStoreRegistry,
        metrics: NodeMetrics,
    ) -> Result<Self> {
        let sources = TilesetSources::parse(&tileset_sources, registry)?;
        Ok(Self {
            sources,
            chunk_size,
            artificial_backend_delay: Duration::from_millis(artificial_backend_delay_ms),
            received_bytes: Arc::new(AtomicU64::new(0)),
            metrics,
        })
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn received_bytes(&self) -> u64 {
        self.received_bytes.load(Ordering::Relaxed)
    }

    pub async fn fetch_chunk_group(
        &self,
        tileset_id: &TilesetId,
        chunk_range: Range<u64>,
        archive_len: u64,
    ) -> std::result::Result<Bytes, ChunkFetchError> {
        if chunk_range.start >= chunk_range.end {
            return Ok(Bytes::new());
        }

        let (object_store, path) = self.sources.resolve(tileset_id.as_str()).ok_or_else(|| {
            ChunkFetchError::Message(format!(
                "no data source configured for tileset {tileset_id}"
            ))
        })?;
        let fetch_started_at = std::time::Instant::now();
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
        if range_start > range_end {
            return Err(ChunkFetchError::Message(format!(
                "chunk range start {range_start} exceeds end {range_end} (archive_len={archive_len})"
            )));
        }
        let prefetched_chunks = end_chunk - start_chunk;
        let prefetched_bytes = range_end - range_start;
        debug!(
            tileset_id = %tileset_id,
            start_chunk = start_chunk,
            end_chunk = end_chunk,
            prefetched_chunks = prefetched_chunks,
            prefetched_bytes = prefetched_bytes,
            "fetching backend chunks"
        );

        if !self.artificial_backend_delay.is_zero() {
            tokio::time::sleep(self.artificial_backend_delay).await;
        }

        let fetch_result = tokio::time::timeout(
            BACKEND_FETCH_TIMEOUT,
            object_store.get_range(&path, range_start..range_end),
        )
        .await;
        let bytes = match fetch_result {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                let outcome = backend_fetch_outcome(&error);
                self.metrics.record_backend_fetch(
                    outcome,
                    fetch_started_at.elapsed(),
                    prefetched_chunks,
                    prefetched_bytes,
                );
                return Err(ChunkFetchError::from(error));
            }
            Err(error) => {
                self.metrics.record_backend_fetch(
                    "timeout",
                    fetch_started_at.elapsed(),
                    prefetched_chunks,
                    prefetched_bytes,
                );
                return Err(ChunkFetchError::Timeout(format!(
                    "timed out fetching chunk range from object store: path={path} range={range_start}..{range_end}: {error}"
                )));
            }
        };
        let expected_len = (range_end - range_start) as usize;
        if bytes.len() != expected_len {
            self.metrics.record_backend_fetch(
                "error",
                fetch_started_at.elapsed(),
                prefetched_chunks,
                prefetched_bytes,
            );
            return Err(ChunkFetchError::Message(format!(
                "short range read from object store: path={path} range={range_start}..{range_end} expected_bytes={expected_len} actual_bytes={}",
                bytes.len()
            )));
        }
        self.received_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.metrics.record_backend_fetch(
            "success",
            fetch_started_at.elapsed(),
            prefetched_chunks,
            bytes.len() as u64,
        );
        debug!(
            tileset_id = %tileset_id,
            start_chunk = start_chunk,
            end_chunk = end_chunk - 1,
            backend_fetched_bytes = bytes.len(),
            duration_ms = fetch_started_at.elapsed().as_millis() as u64,
            "fetched backend chunk bytes"
        );

        Ok(bytes)
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
        Self::Message(format!(
            "failed to fetch chunk range from object store: {error}"
        ))
    }
}

/// Parses a `TILESET_SOURCES` spec into `(namespace, url)` entries without building any
/// object stores. `None` namespace is the default root.
fn parse_source_entries(spec: &str) -> Result<Vec<(Option<String>, String)>> {
    let mut entries = Vec::new();
    let mut seen_default = false;
    let mut seen_namespaces = HashSet::new();

    for entry in spec.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (namespace, url) = match entry.split_once('=') {
            Some((key, value)) if is_namespace_key(key.trim()) => {
                let key = key.trim();
                if key == "default" {
                    (None, value.trim())
                } else {
                    (Some(key.to_string()), value.trim())
                }
            }
            // A bare URL (or anything with no namespace key before `=`, such as a
            // URL with a query string) is the default root.
            _ => (None, entry),
        };
        match &namespace {
            None => {
                if seen_default {
                    bail!("TILESET_SOURCES has multiple default sources");
                }
                seen_default = true;
            }
            Some(name) => {
                if !seen_namespaces.insert(name.clone()) {
                    bail!("TILESET_SOURCES has duplicate namespace {name:?}");
                }
            }
        }
        entries.push((namespace, url.to_string()));
    }

    if entries.is_empty() {
        bail!("TILESET_SOURCES must define at least one source");
    }
    Ok(entries)
}

/// Selects the namespace source for a tileset key. Returns the matched namespace
/// index (or `None` for the default root) and the key relative to that root: the
/// namespace is stripped on a match, otherwise the whole key is used.
fn select_namespace<'a>(tileset_id: &'a str, namespace_names: &[&str]) -> (Option<usize>, &'a str) {
    if let Some((namespace, rest)) = tileset_id.split_once('/')
        && let Some(index) = namespace_names.iter().position(|name| *name == namespace)
    {
        return (Some(index), rest);
    }
    (None, tileset_id)
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
    let (object_store, base_path) = registry.resolve(&url)?;
    Ok(TilesetSource {
        object_store,
        base_path,
    })
}

fn is_namespace_key(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn normalize_source_url(source_url: &str) -> Result<Url> {
    if let Ok(url) = Url::parse(source_url) {
        return Ok(url);
    }

    let path = std::fs::canonicalize(PathBuf::from(source_url))
        .with_context(|| format!("failed to resolve local data path {source_url}"))?;
    Url::from_directory_path(path)
        .map_err(|_| anyhow!("failed to convert local path to file:// URL"))
}

#[cfg(test)]
mod tests {
    use super::{object_path_under, parse_source_entries, select_namespace};
    use object_store::path::Path as ObjectPath;

    #[test]
    fn parses_default_and_namespaced_entries() {
        let entries = parse_source_entries("carto=gs://a;regional=gs://b;default=gs://c").unwrap();
        assert_eq!(
            entries,
            vec![
                (Some("carto".to_string()), "gs://a".to_string()),
                (Some("regional".to_string()), "gs://b".to_string()),
                (None, "gs://c".to_string()),
            ]
        );
    }

    #[test]
    fn bare_url_is_the_default_source() {
        assert_eq!(
            parse_source_entries("gs://bucket/prefix").unwrap(),
            vec![(None, "gs://bucket/prefix".to_string())]
        );
        // A URL with a query string has no namespace key before `=`.
        assert_eq!(
            parse_source_entries("https://h/p?t=1").unwrap(),
            vec![(None, "https://h/p?t=1".to_string())]
        );
    }

    #[test]
    fn rejects_duplicate_and_empty_specs() {
        assert!(parse_source_entries("carto=gs://a;carto=gs://b").is_err());
        assert!(parse_source_entries("gs://a;default=gs://b").is_err());
        assert!(parse_source_entries("   ").is_err());
    }

    #[test]
    fn namespace_match_strips_prefix_else_default_keeps_whole_key() {
        let names = ["carto", "regional"];
        assert_eq!(
            select_namespace("carto/voyager", &names),
            (Some(0), "voyager")
        );
        assert_eq!(
            select_namespace("regional/streets", &names),
            (Some(1), "streets")
        );
        // No namespace match -> default root with the full key (nested path).
        assert_eq!(
            select_namespace("analysis/hrnowc", &names),
            (None, "analysis/hrnowc")
        );
        assert_eq!(select_namespace("japan", &names), (None, "japan"));
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
}
