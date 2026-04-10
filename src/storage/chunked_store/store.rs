//! Object-store and cache backed chunked byte-range reader.

use std::ops::RangeInclusive;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};

use crate::interned::TilesetId;
use crate::metrics::NodeMetrics;
use crate::storage::ObjectStoreRegistry;

use super::{
    cache::{ChunkCache, ChunkCacheKey},
    coordinator::ChunkFetchCoordinator,
    fetcher::{ChunkFetchError, ChunkFetcher},
};

/// Chunked byte-range reader backed by an object store.
#[derive(Clone)]
pub struct ChunkedStore {
    cache: ChunkCache,
    coordinator: ChunkFetchCoordinator,
}

impl ChunkedStore {
    /// Creates a chunked object-store reader over the configured tileset sources.
    pub fn new(
        tileset_sources: String,
        chunk_size: u64,
        max_fetch_chunks: u64,
        artificial_backend_delay_ms: u64,
        chunk_cache_max_bytes: u64,
        registry: &ObjectStoreRegistry,
        metrics: NodeMetrics,
    ) -> Result<Self> {
        let fetcher = ChunkFetcher::new(
            tileset_sources,
            chunk_size,
            artificial_backend_delay_ms,
            registry,
            metrics.clone(),
        )?;
        Ok(Self {
            cache: ChunkCache::new(chunk_cache_max_bytes),
            coordinator: ChunkFetchCoordinator::new(fetcher, max_fetch_chunks, metrics),
        })
    }

    /// Returns the configured fixed chunk size in bytes.
    pub fn chunk_size(&self) -> u64 {
        self.coordinator.chunk_size()
    }

    pub fn received_bytes(&self) -> u64 {
        self.coordinator.received_bytes()
    }

    /// Reads a tileset byte range through the shared chunk cache and inflight fetcher.
    pub async fn read_bytes(
        &self,
        tileset_id: &TilesetId,
        start: u64,
        length: usize,
        archive_len: Option<u64>,
    ) -> std::result::Result<Bytes, ChunkFetchError> {
        if length == 0 {
            return Ok(Bytes::new());
        }

        let end = start.checked_add(length as u64).ok_or_else(|| {
            ChunkFetchError::Message(format!(
                "byte range overflow: start={start} length={length}"
            ))
        })?;
        let chunk_range = byte_range_to_chunk_range(start, end, self.chunk_size());
        let mut missing_chunks = Vec::new();
        for chunk_index in chunk_range.clone() {
            if self.chunk_cache_get(tileset_id, chunk_index).is_some() {
                self.coordinator.metrics().record_chunk_cache("hit");
            } else {
                self.coordinator.metrics().record_chunk_cache("miss");
                missing_chunks.push(chunk_index);
            }
        }

        if !missing_chunks.is_empty() {
            let last_missing_chunk = *missing_chunks
                .last()
                .expect("missing_chunks must be non-empty here");
            let fetch_end =
                archive_len.unwrap_or_else(|| (last_missing_chunk + 1) * self.chunk_size());
            self.coordinator
                .fetch_chunks(self.clone(), tileset_id, &missing_chunks, fetch_end)
                .await?;
        }

        self.read_cached_bytes(tileset_id, start, length)
            .map_err(|error| ChunkFetchError::Message(error.to_string()))
    }

    /// Returns the current weighted byte size of the chunk cache.
    pub fn chunk_cache_weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    pub fn chunk_cache_get(&self, tileset_id: &TilesetId, chunk_index: u64) -> Option<Bytes> {
        self.cache.get(&ChunkCacheKey::new(tileset_id, chunk_index))
    }

    pub fn read_cached_bytes(
        &self,
        tileset_id: &TilesetId,
        start: u64,
        length: usize,
    ) -> Result<Bytes> {
        let end = start
            .checked_add(length as u64)
            .context("byte range overflow computing cached read")?;
        let chunk_range = byte_range_to_chunk_range(start, end, self.chunk_size());
        let chunk_offset = (start % self.chunk_size()) as usize;
        let first_chunk = *chunk_range.start();
        let last_chunk = *chunk_range.end();

        if first_chunk == last_chunk {
            let chunk = self
                .chunk_cache_get(tileset_id, first_chunk)
                .context("chunk missing from cache after fetch")?;
            self.coordinator
                .metrics()
                .record_chunk_cache("post_fetch_hit");
            if chunk_offset + length > chunk.len() {
                anyhow::bail!(
                    "cached chunk is shorter than requested range: chunk_index={} chunk_len={} chunk_offset={} length={}",
                    first_chunk,
                    chunk.len(),
                    chunk_offset,
                    length
                );
            }
            return Ok(chunk.slice(chunk_offset..chunk_offset + length));
        }

        let mut bytes = BytesMut::with_capacity(length);
        let mut remaining = length;
        let mut current_offset = chunk_offset;
        for chunk_idx in chunk_range {
            let chunk = self
                .chunk_cache_get(tileset_id, chunk_idx)
                .context("chunk missing from cache after fetch")?;
            self.coordinator
                .metrics()
                .record_chunk_cache("post_fetch_hit");
            let take = remaining.min(chunk.len().saturating_sub(current_offset));
            bytes.extend_from_slice(&chunk[current_offset..current_offset + take]);
            remaining -= take;
            current_offset = 0;
        }

        if remaining != 0 {
            anyhow::bail!("failed to reconstruct tileset bytes from chunk cache");
        }

        Ok(bytes.freeze())
    }

    pub fn cache_chunk_group(
        &self,
        tileset_id: &TilesetId,
        chunk_range: std::ops::Range<u64>,
        archive_len: u64,
        bytes: Bytes,
    ) -> Result<()> {
        let chunk_size = self.chunk_size();
        let range_start = chunk_range.start * chunk_size;

        for chunk_index in chunk_range.start..chunk_range.end {
            let absolute_start = chunk_index * chunk_size;
            let absolute_end = ((chunk_index + 1) * chunk_size).min(archive_len);
            let relative_start = (absolute_start - range_start) as usize;
            let relative_end = (absolute_end - range_start) as usize;
            self.cache.put(
                ChunkCacheKey::new(tileset_id, chunk_index),
                bytes.slice(relative_start..relative_end),
            );
        }

        Ok(())
    }
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
