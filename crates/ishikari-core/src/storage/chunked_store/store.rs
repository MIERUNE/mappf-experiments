//! Object-store and cache backed chunked byte-range reader.

use std::{
    collections::HashMap,
    ops::{Range, RangeInclusive},
    time::Duration,
};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::interned::TilesetId;
use crate::metrics::NodeMetrics;
use crate::storage::ObjectStoreRegistry;

use super::{
    cache::{ChunkCache, ChunkCacheKey},
    coordinator::ChunkFetchCoordinator,
    fetcher::{BackendLatencyModel, ChunkFetchError, ChunkFetcher},
};

/// Chunked byte-range reader backed by an object store.
#[derive(Clone)]
pub(crate) struct ChunkedStore {
    cache: ChunkCache,
    coordinator: ChunkFetchCoordinator,
}

pub(crate) struct ChunkedStoreConfig {
    pub tileset_sources: String,
    pub chunk_size: u64,
    pub max_fetch_chunks: u64,
    pub chunk_fetch_merge_window: Duration,
    pub backend_fetch_concurrency: usize,
    pub backend_fetch_max_inflight: usize,
    pub backend_latency: BackendLatencyModel,
    pub chunk_cache_max_bytes: u64,
}

/// Whether a range read was satisfied immediately or waited for backend work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChunkReadSource {
    Cache,
    Backend,
}

/// Bytes returned by a chunked range read together with request provenance.
pub(crate) struct ChunkRead {
    pub bytes: Bytes,
    pub source: ChunkReadSource,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(super) enum ChunkRangeError {
    #[error("byte range overflow: start={start} length={length}")]
    ByteRangeOverflow { start: u64, length: usize },
    #[error("byte range exceeds archive: start={start} end={end} archive_len={archive_len}")]
    OutsideArchive {
        start: u64,
        end: u64,
        archive_len: u64,
    },
    #[error("invalid chunk group: start_chunk={start_chunk} end_chunk={end_chunk}")]
    InvalidChunkGroup { start_chunk: u64, end_chunk: u64 },
    #[error("chunk range arithmetic overflow: chunk_index={chunk_index} chunk_size={chunk_size}")]
    ChunkArithmeticOverflow { chunk_index: u64, chunk_size: u64 },
    #[error("chunk {chunk_index} does not intersect archive length {archive_len}")]
    ChunkOutsideArchive { chunk_index: u64, archive_len: u64 },
    #[error(
        "fetched bytes omit chunk {chunk_index}: slice={relative_start}..{relative_end} bytes={bytes_len}"
    )]
    SliceOutsideFetchedBytes {
        chunk_index: u64,
        relative_start: usize,
        relative_end: usize,
        bytes_len: usize,
    },
}

impl ChunkedStore {
    /// Creates a chunked object-store reader over the configured tileset sources.
    pub(crate) fn new(
        config: ChunkedStoreConfig,
        registry: &ObjectStoreRegistry,
        metrics: NodeMetrics,
    ) -> Result<Self> {
        let fetcher = ChunkFetcher::new(
            config.tileset_sources,
            config.chunk_size,
            config.backend_fetch_concurrency,
            config.backend_latency,
            registry,
            metrics.clone(),
        )?;
        Ok(Self {
            cache: ChunkCache::new(config.chunk_cache_max_bytes),
            coordinator: ChunkFetchCoordinator::new(
                fetcher,
                config.max_fetch_chunks,
                config.chunk_fetch_merge_window,
                config.backend_fetch_max_inflight,
                metrics,
            ),
        })
    }

    /// Returns the configured fixed chunk size in bytes.
    pub(crate) fn chunk_size(&self) -> u64 {
        self.coordinator.chunk_size()
    }

    pub(crate) fn received_bytes(&self) -> u64 {
        self.coordinator.received_bytes()
    }

    /// Reads a tileset byte range through the shared chunk cache and inflight fetcher.
    pub(crate) async fn read_bytes(
        &self,
        tileset_id: &TilesetId,
        start: u64,
        length: usize,
        archive_len: Option<u64>,
    ) -> std::result::Result<ChunkRead, ChunkFetchError> {
        let end = validate_byte_range(start, length, archive_len)
            .map_err(|error| ChunkFetchError::Message(error.to_string()))?;
        if length == 0 {
            return Ok(ChunkRead {
                bytes: Bytes::new(),
                source: ChunkReadSource::Cache,
            });
        }
        let Some(archive_len) = archive_len else {
            let bytes = self
                .coordinator
                .fetch_exact_range(tileset_id, start..end)
                .await?;
            return Ok(ChunkRead {
                bytes,
                source: ChunkReadSource::Backend,
            });
        };

        let chunk_range = byte_range_to_chunk_range(start, end, self.chunk_size());
        let first_chunk = *chunk_range.start();
        if first_chunk == *chunk_range.end() {
            return self
                .read_single_chunk(tileset_id, first_chunk, start, length, archive_len)
                .await;
        }

        let mut missing_chunks = Vec::new();
        let mut resolved_chunks = HashMap::new();
        for chunk_index in chunk_range.clone() {
            if let Some(chunk) = self.chunk_cache_get(tileset_id, chunk_index) {
                self.coordinator.metrics().record_chunk_cache("hit");
                resolved_chunks.insert(chunk_index, chunk);
            } else {
                self.coordinator.metrics().record_chunk_cache("miss");
                missing_chunks.push(chunk_index);
            }
        }

        if !missing_chunks.is_empty() {
            let fetched = self
                .coordinator
                .fetch_chunks(self.clone(), tileset_id, &missing_chunks, archive_len)
                .await?;
            resolved_chunks.extend(fetched);
        }

        let source = if missing_chunks.is_empty() {
            ChunkReadSource::Cache
        } else {
            ChunkReadSource::Backend
        };
        let bytes = self
            .read_resolved_bytes(start, length, &resolved_chunks)
            .map_err(|error| ChunkFetchError::Message(error.to_string()))?;
        Ok(ChunkRead { bytes, source })
    }

    async fn read_single_chunk(
        &self,
        tileset_id: &TilesetId,
        chunk_index: u64,
        start: u64,
        length: usize,
        archive_len: u64,
    ) -> std::result::Result<ChunkRead, ChunkFetchError> {
        let (chunk, source) = if let Some(chunk) = self.chunk_cache_get(tileset_id, chunk_index) {
            self.coordinator.metrics().record_chunk_cache("hit");
            (chunk, ChunkReadSource::Cache)
        } else {
            self.coordinator.metrics().record_chunk_cache("miss");
            let mut fetched = self
                .coordinator
                .fetch_chunks(self.clone(), tileset_id, &[chunk_index], archive_len)
                .await?;
            let chunk = fetched.remove(&chunk_index).ok_or_else(|| {
                ChunkFetchError::Message("resolved chunk missing after fetch".to_string())
            })?;
            (chunk, ChunkReadSource::Backend)
        };
        let bytes = self
            .slice_single_chunk(chunk_index, start, length, &chunk)
            .map_err(|error| ChunkFetchError::Message(error.to_string()))?;
        Ok(ChunkRead { bytes, source })
    }

    /// Returns the current weighted byte size of the chunk cache.
    pub(crate) fn chunk_cache_weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    pub(crate) fn chunk_cache_get(
        &self,
        tileset_id: &TilesetId,
        chunk_index: u64,
    ) -> Option<Bytes> {
        self.cache.get(&ChunkCacheKey::new(tileset_id, chunk_index))
    }

    fn read_resolved_bytes(
        &self,
        start: u64,
        length: usize,
        chunks: &HashMap<u64, Bytes>,
    ) -> Result<Bytes> {
        let end = start
            .checked_add(length as u64)
            .context("byte range overflow computing cached read")?;
        let chunk_range = byte_range_to_chunk_range(start, end, self.chunk_size());
        let chunk_offset = (start % self.chunk_size()) as usize;
        let first_chunk = *chunk_range.start();
        let last_chunk = *chunk_range.end();

        if first_chunk == last_chunk {
            let chunk = chunks
                .get(&first_chunk)
                .context("resolved chunk missing after fetch")?;
            return self.slice_single_chunk(first_chunk, start, length, chunk);
        }

        let mut bytes = BytesMut::with_capacity(length);
        let mut remaining = length;
        let mut current_offset = chunk_offset;
        for chunk_idx in chunk_range {
            let chunk = chunks
                .get(&chunk_idx)
                .context("resolved chunk missing after fetch")?;
            self.coordinator
                .metrics()
                .record_chunk_cache("post_fetch_hit");
            if current_offset > chunk.len() {
                anyhow::bail!(
                    "cached chunk offset exceeds chunk length: chunk_index={} chunk_len={} chunk_offset={}",
                    chunk_idx,
                    chunk.len(),
                    current_offset
                );
            }
            let take = remaining.min(chunk.len() - current_offset);
            let current_end = current_offset
                .checked_add(take)
                .context("byte range overflow computing reconstructed chunk slice")?;
            bytes.extend_from_slice(&chunk[current_offset..current_end]);
            remaining -= take;
            current_offset = 0;
        }

        if remaining != 0 {
            anyhow::bail!("failed to reconstruct tileset bytes from chunk cache");
        }

        Ok(bytes.freeze())
    }

    fn slice_single_chunk(
        &self,
        chunk_index: u64,
        start: u64,
        length: usize,
        chunk: &Bytes,
    ) -> Result<Bytes> {
        self.coordinator
            .metrics()
            .record_chunk_cache("post_fetch_hit");
        let chunk_offset = (start % self.chunk_size()) as usize;
        let chunk_end = chunk_offset
            .checked_add(length)
            .context("byte range overflow computing chunk slice")?;
        if chunk_end > chunk.len() {
            anyhow::bail!(
                "cached chunk is shorter than requested range: chunk_index={} chunk_len={} chunk_offset={} length={}",
                chunk_index,
                chunk.len(),
                chunk_offset,
                length
            );
        }
        Ok(chunk.slice(chunk_offset..chunk_end))
    }

    pub(super) fn cache_chunk_group(
        &self,
        tileset_id: &TilesetId,
        chunk_range: Range<u64>,
        archive_len: u64,
        bytes: Bytes,
    ) -> std::result::Result<HashMap<u64, Bytes>, ChunkRangeError> {
        let slices = chunk_slices(chunk_range, self.chunk_size(), archive_len, bytes.len())?;
        let mut chunks = HashMap::with_capacity(slices.len());

        for (chunk_index, slice) in slices {
            let chunk = bytes.slice(slice);
            self.cache
                .put(ChunkCacheKey::new(tileset_id, chunk_index), chunk.clone());
            chunks.insert(chunk_index, chunk);
        }

        Ok(chunks)
    }
}

fn validate_byte_range(
    start: u64,
    length: usize,
    archive_len: Option<u64>,
) -> std::result::Result<u64, ChunkRangeError> {
    let length_u64 =
        u64::try_from(length).map_err(|_| ChunkRangeError::ByteRangeOverflow { start, length })?;
    let end = start
        .checked_add(length_u64)
        .ok_or(ChunkRangeError::ByteRangeOverflow { start, length })?;
    if let Some(archive_len) = archive_len
        && (start > archive_len || end > archive_len)
    {
        return Err(ChunkRangeError::OutsideArchive {
            start,
            end,
            archive_len,
        });
    }
    Ok(end)
}

fn chunk_slices(
    chunk_range: Range<u64>,
    chunk_size: u64,
    archive_len: u64,
    bytes_len: usize,
) -> std::result::Result<Vec<(u64, Range<usize>)>, ChunkRangeError> {
    if chunk_range.start >= chunk_range.end {
        return Err(ChunkRangeError::InvalidChunkGroup {
            start_chunk: chunk_range.start,
            end_chunk: chunk_range.end,
        });
    }

    let range_start = chunk_range.start.checked_mul(chunk_size).ok_or(
        ChunkRangeError::ChunkArithmeticOverflow {
            chunk_index: chunk_range.start,
            chunk_size,
        },
    )?;
    let mut slices = Vec::new();
    for chunk_index in chunk_range {
        let absolute_start = chunk_index.checked_mul(chunk_size).ok_or(
            ChunkRangeError::ChunkArithmeticOverflow {
                chunk_index,
                chunk_size,
            },
        )?;
        let next_chunk =
            chunk_index
                .checked_add(1)
                .ok_or(ChunkRangeError::ChunkArithmeticOverflow {
                    chunk_index,
                    chunk_size,
                })?;
        let absolute_end = next_chunk
            .checked_mul(chunk_size)
            .ok_or(ChunkRangeError::ChunkArithmeticOverflow {
                chunk_index: next_chunk,
                chunk_size,
            })?
            .min(archive_len);
        if absolute_start >= absolute_end {
            return Err(ChunkRangeError::ChunkOutsideArchive {
                chunk_index,
                archive_len,
            });
        }

        let relative_start = usize::try_from(absolute_start - range_start).map_err(|_| {
            ChunkRangeError::SliceOutsideFetchedBytes {
                chunk_index,
                relative_start: usize::MAX,
                relative_end: usize::MAX,
                bytes_len,
            }
        })?;
        let relative_end = usize::try_from(absolute_end - range_start).map_err(|_| {
            ChunkRangeError::SliceOutsideFetchedBytes {
                chunk_index,
                relative_start,
                relative_end: usize::MAX,
                bytes_len,
            }
        })?;
        if relative_start > relative_end || relative_end > bytes_len {
            return Err(ChunkRangeError::SliceOutsideFetchedBytes {
                chunk_index,
                relative_start,
                relative_end,
                bytes_len,
            });
        }
        slices.push((chunk_index, relative_start..relative_end));
    }
    Ok(slices)
}

/// Maps a byte range to the owning fixed-size chunk index range.
fn byte_range_to_chunk_range(start: u64, end: u64, chunk_size: u64) -> RangeInclusive<u64> {
    let first_chunk = chunk_index(start, chunk_size);
    let last_chunk = chunk_index(end.saturating_sub(1), chunk_size);
    first_chunk..=last_chunk
}

fn chunk_index(offset: u64, chunk_size: u64) -> u64 {
    offset / chunk_size
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StoreFixture {
        directory: std::path::PathBuf,
        store: ChunkedStore,
        tileset_id: TilesetId,
        metrics: NodeMetrics,
    }

    impl Drop for StoreFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    fn store_fixture(test_name: &str, data: &[u8], chunk_size: u64) -> StoreFixture {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ishikari-{test_name}-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("fixture.pmtiles"), data).unwrap();

        let metrics = NodeMetrics::new();
        let store = ChunkedStore::new(
            ChunkedStoreConfig {
                tileset_sources: directory.to_string_lossy().into_owned(),
                chunk_size,
                max_fetch_chunks: 4,
                chunk_fetch_merge_window: Duration::ZERO,
                backend_fetch_concurrency: 1,
                backend_fetch_max_inflight: 4,
                backend_latency: BackendLatencyModel::fixed(0),
                chunk_cache_max_bytes: chunk_size.saturating_mul(4),
            },
            &ObjectStoreRegistry::without_options(),
            metrics.clone(),
        )
        .unwrap();

        StoreFixture {
            directory,
            store,
            tileset_id: TilesetId::try_new("fixture").unwrap(),
            metrics,
        }
    }

    #[test]
    fn validates_archive_end_before_chunk_planning() {
        assert_eq!(validate_byte_range(90, 10, Some(100)), Ok(100));
        assert_eq!(validate_byte_range(100, 0, Some(100)), Ok(100));
        assert_eq!(
            validate_byte_range(100, 1, Some(100)),
            Err(ChunkRangeError::OutsideArchive {
                start: 100,
                end: 101,
                archive_len: 100,
            })
        );
    }

    #[test]
    fn rejects_byte_range_overflow() {
        assert_eq!(
            validate_byte_range(u64::MAX, 1, Some(u64::MAX)),
            Err(ChunkRangeError::ByteRangeOverflow {
                start: u64::MAX,
                length: 1,
            })
        );
    }

    #[test]
    fn rejects_reversed_non_intersecting_and_short_chunk_slices() {
        assert!(matches!(
            chunk_slices(Range { start: 2, end: 1 }, 16, 64, 0),
            Err(ChunkRangeError::InvalidChunkGroup { .. })
        ));
        assert_eq!(
            chunk_slices(4..5, 16, 64, 0),
            Err(ChunkRangeError::ChunkOutsideArchive {
                chunk_index: 4,
                archive_len: 64,
            })
        );
        assert_eq!(
            chunk_slices(3..4, 16, 64, 15),
            Err(ChunkRangeError::SliceOutsideFetchedBytes {
                chunk_index: 3,
                relative_start: 0,
                relative_end: 16,
                bytes_len: 15,
            })
        );
    }

    #[test]
    fn plans_a_final_partial_chunk_without_panicking() {
        assert_eq!(chunk_slices(2..3, 16, 40, 8), Ok(vec![(2, 0..8)]));
    }

    #[tokio::test]
    async fn single_chunk_cache_hit_returns_a_zero_copy_slice() {
        let fixture = store_fixture("single-chunk-hit", b"abcdefghijklmnop", 8);
        let cached = Bytes::from_static(b"abcdefgh");
        fixture
            .store
            .cache
            .put(ChunkCacheKey::new(&fixture.tileset_id, 0), cached.clone());

        let read = fixture
            .store
            .read_bytes(&fixture.tileset_id, 2, 4, Some(16))
            .await
            .expect("cached single-chunk read");

        assert_eq!(read.bytes.as_ref(), b"cdef");
        assert_eq!(read.source, ChunkReadSource::Cache);
        assert!(std::ptr::eq(
            read.bytes.as_ptr(),
            cached.slice(2..6).as_ptr()
        ));
        assert_eq!(fixture.store.received_bytes(), 0);
        let snapshot = fixture.metrics.snapshot();
        assert_eq!(snapshot.chunk_cache_hits, 1);
        assert_eq!(snapshot.chunk_cache_misses, 0);
        assert_eq!(snapshot.chunk_cache_post_fetch_hits, 1);
    }

    #[tokio::test]
    async fn single_chunk_cache_miss_fetches_and_returns_a_zero_copy_slice() {
        let fixture = store_fixture("single-chunk-miss", b"abcdefghijklmnop", 8);

        let read = fixture
            .store
            .read_bytes(&fixture.tileset_id, 2, 4, Some(16))
            .await
            .expect("single-chunk backend read");
        let cached = fixture
            .store
            .chunk_cache_get(&fixture.tileset_id, 0)
            .expect("fetched chunk must be cached");

        assert_eq!(read.bytes.as_ref(), b"cdef");
        assert_eq!(read.source, ChunkReadSource::Backend);
        assert!(std::ptr::eq(
            read.bytes.as_ptr(),
            cached.slice(2..6).as_ptr()
        ));
        assert_eq!(fixture.store.received_bytes(), 8);
        let snapshot = fixture.metrics.snapshot();
        assert_eq!(snapshot.chunk_cache_hits, 0);
        assert_eq!(snapshot.chunk_cache_misses, 1);
        assert_eq!(snapshot.chunk_cache_post_fetch_hits, 1);
        assert_eq!(snapshot.chunk_fetch_queued, 1);
    }

    #[tokio::test]
    async fn single_chunk_read_ending_at_chunk_boundary_does_not_fetch_the_next_chunk() {
        let fixture = store_fixture("single-chunk-boundary", b"abcdefghijklmnop", 8);

        let read = fixture
            .store
            .read_bytes(&fixture.tileset_id, 4, 4, Some(16))
            .await
            .expect("range ending exactly at chunk boundary");

        assert_eq!(read.bytes.as_ref(), b"efgh");
        assert_eq!(read.source, ChunkReadSource::Backend);
        assert_eq!(fixture.store.received_bytes(), 8);
        assert!(
            fixture
                .store
                .chunk_cache_get(&fixture.tileset_id, 0)
                .is_some()
        );
        assert!(
            fixture
                .store
                .chunk_cache_get(&fixture.tileset_id, 1)
                .is_none()
        );
    }

    #[tokio::test]
    async fn single_chunk_read_handles_a_short_final_chunk() {
        let fixture = store_fixture("single-chunk-final", b"abcdefghijk", 8);

        let read = fixture
            .store
            .read_bytes(&fixture.tileset_id, 9, 2, Some(11))
            .await
            .expect("range within short final chunk");
        let cached = fixture
            .store
            .chunk_cache_get(&fixture.tileset_id, 1)
            .expect("short final chunk must be cached");

        assert_eq!(read.bytes.as_ref(), b"jk");
        assert_eq!(read.source, ChunkReadSource::Backend);
        assert_eq!(cached.as_ref(), b"ijk");
        assert!(std::ptr::eq(
            read.bytes.as_ptr(),
            cached.slice(1..3).as_ptr()
        ));
        assert_eq!(fixture.store.received_bytes(), 3);
    }

    #[tokio::test]
    async fn unknown_length_fetches_only_the_exact_range_without_caching_a_partial_chunk() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ishikari-exact-bootstrap-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("fixture.pmtiles"), b"abcdefgh").unwrap();

        let store = ChunkedStore::new(
            ChunkedStoreConfig {
                tileset_sources: directory.to_string_lossy().into_owned(),
                chunk_size: 1024 * 1024,
                max_fetch_chunks: 4,
                chunk_fetch_merge_window: Duration::ZERO,
                backend_fetch_concurrency: 1,
                backend_fetch_max_inflight: 4,
                backend_latency: BackendLatencyModel::fixed(0),
                chunk_cache_max_bytes: 1024 * 1024,
            },
            &ObjectStoreRegistry::without_options(),
            NodeMetrics::new(),
        )
        .unwrap();
        let tileset_id = TilesetId::try_new("fixture").unwrap();

        let initial = store
            .read_bytes(&tileset_id, 0, 16_384, None)
            .await
            .expect("unknown-length range is capped to the object");
        assert_eq!(initial.bytes.as_ref(), b"abcdefgh");
        assert_eq!(initial.source, ChunkReadSource::Backend);
        assert_eq!(store.received_bytes(), 8);
        assert!(store.chunk_cache_get(&tileset_id, 0).is_none());

        let bounded = store
            .read_bytes(&tileset_id, 0, 8, Some(8))
            .await
            .expect("archive-bounded chunk range");
        assert_eq!(bounded.bytes.as_ref(), b"abcdefgh");
        assert!(store.chunk_cache_get(&tileset_id, 0).is_some());

        let missing = TilesetId::try_new("missing").unwrap();
        assert!(matches!(
            store.read_bytes(&missing, 0, 16_384, None).await,
            Err(ChunkFetchError::NotFound)
        ));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn group_limit_sheds_before_spawning_or_waiting_for_backend_work() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ishikari-group-limit-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("first.pmtiles"), b"abcdefgh").unwrap();
        std::fs::write(directory.join("second.pmtiles"), b"ijklmnop").unwrap();

        let store = ChunkedStore::new(
            ChunkedStoreConfig {
                tileset_sources: directory.to_string_lossy().into_owned(),
                chunk_size: 1024 * 1024,
                max_fetch_chunks: 4,
                chunk_fetch_merge_window: Duration::ZERO,
                backend_fetch_concurrency: 1,
                backend_fetch_max_inflight: 1,
                backend_latency: BackendLatencyModel::fixed(100),
                chunk_cache_max_bytes: 1024 * 1024,
            },
            &ObjectStoreRegistry::without_options(),
            NodeMetrics::new(),
        )
        .unwrap();
        let first_store = store.clone();
        let first = tokio::spawn(async move {
            first_store
                .read_bytes(&TilesetId::try_new("first").unwrap(), 0, 4, None)
                .await
        });
        tokio::task::yield_now().await;

        let error = match store
            .read_bytes(&TilesetId::try_new("second").unwrap(), 0, 4, None)
            .await
        {
            Ok(_) => panic!("second distinct group must be shed"),
            Err(error) => error,
        };
        assert!(matches!(error, ChunkFetchError::Overloaded(_)));

        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(first.await.unwrap().unwrap().bytes.as_ref(), b"abcd");
        assert_eq!(
            store
                .read_bytes(&TilesetId::try_new("second").unwrap(), 0, 4, None)
                .await
                .expect("group permit must be released")
                .bytes
                .as_ref(),
            b"ijkl"
        );

        std::fs::remove_dir_all(directory).unwrap();
    }
}
