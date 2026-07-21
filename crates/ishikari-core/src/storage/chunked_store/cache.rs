//! Per-object chunk caches used by chunked byte-range readers.

use bytes::Bytes;
use moka::{policy::EvictionPolicy, sync::Cache};

use crate::{
    cache_policy::{chunk_cache_entry_weight, effective_chunk_cache_capacity},
    interned::TilesetId,
};

/// Identifies a cached fixed-size chunk within an object.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct ChunkCacheKey {
    pub tileset_id: TilesetId,
    pub chunk_index: u64,
}

impl ChunkCacheKey {
    /// Builds a chunk cache key from an object id and fixed-size chunk index.
    pub(super) fn new(tileset_id: &TilesetId, chunk_index: u64) -> Self {
        Self {
            tileset_id: tileset_id.clone(),
            chunk_index,
        }
    }
}

/// Per-node cache of backend tileset chunks.
#[derive(Clone)]
pub(super) struct ChunkCache {
    cache: Cache<ChunkCacheKey, Bytes>,
}

impl ChunkCache {
    /// Creates a chunk cache with a byte-based capacity limit.
    pub(super) fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .eviction_policy(EvictionPolicy::lru())
            .max_capacity(effective_chunk_cache_capacity(max_capacity_bytes))
            .weigher(chunk_cache_weight)
            .build();
        Self { cache }
    }

    /// Returns a cached chunk if present.
    pub(super) fn get(&self, key: &ChunkCacheKey) -> Option<Bytes> {
        self.cache.get(key)
    }

    /// Inserts or replaces a cached chunk.
    pub(super) fn put(&self, key: ChunkCacheKey, data: Bytes) {
        self.cache.insert(key, data);
    }

    /// Returns the current weighted byte size of the chunk cache.
    ///
    /// Flushes pending maintenance first so the value reflects recent inserts
    /// and evictions rather than moka's lazily-updated estimate.
    pub(super) fn weighted_size(&self) -> u64 {
        self.cache.run_pending_tasks();
        self.cache.weighted_size()
    }
}

/// Estimates the weight of a cached chunk entry.
fn chunk_cache_weight(_key: &ChunkCacheKey, value: &Bytes) -> u32 {
    chunk_cache_entry_weight(value.len())
}
