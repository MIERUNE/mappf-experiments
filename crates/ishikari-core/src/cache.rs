//! Per-node L1 caches for tiles and small resources.

use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::{Expiry, sync::Cache};

use crate::{
    cache_policy::tile_cache_entry_weight,
    storage::{TilesetInfo, generation::ArchiveKey},
};

/// Identifies a cached tile payload within a tileset.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TileCacheKey {
    pub archive: ArchiveKey,
    pub tile_id: u64,
}

impl TileCacheKey {
    /// Builds a tile cache key from a tileset id and PMTiles tile id.
    pub(crate) fn new(archive: &ArchiveKey, tile_id: u64) -> Self {
        Self {
            archive: archive.clone(),
            tile_id,
        }
    }
}

/// Cache entry for a tile, including negative lookups.
#[derive(Clone)]
pub(crate) enum CachedTile {
    Found {
        bytes: bytes::Bytes,
        content_type: &'static str,
        content_encoding: Option<&'static str>,
    },
    NotFound,
}

/// Per-node L1 cache of tile payloads.
#[derive(Clone)]
pub(crate) struct TileCache {
    cache: Cache<TileCacheKey, CachedTile>,
}

/// Per-node cache of tileset metadata.
#[derive(Clone)]
pub(crate) struct TilesetInfoCache {
    cache: Cache<ArchiveKey, Arc<TilesetInfo>>,
}

/// Per-entry expiry policy for the tile cache.
///
/// Positive (`Found`) entries never expire on their own because their key
/// includes the object generation. Replacing a logical archive selects a new
/// key; old entries become unreachable and leave through capacity eviction.
/// Negative (`NotFound`) entries still expire after `negative_ttl`, limiting how
/// long an added tile stays hidden within one generation. A cache *hit* does
/// not extend that lifetime (`expire_after_read` keeps the default).
struct TileExpiry {
    negative_ttl: Duration,
}

impl TileExpiry {
    fn ttl_for(&self, value: &CachedTile) -> Option<Duration> {
        match value {
            CachedTile::Found { .. } => None,
            CachedTile::NotFound => Some(self.negative_ttl),
        }
    }
}

impl Expiry<TileCacheKey, CachedTile> for TileExpiry {
    fn expire_after_create(
        &self,
        _key: &TileCacheKey,
        value: &CachedTile,
        _created_at: Instant,
    ) -> Option<Duration> {
        self.ttl_for(value)
    }

    fn expire_after_update(
        &self,
        _key: &TileCacheKey,
        value: &CachedTile,
        _updated_at: Instant,
        _current: Option<Duration>,
    ) -> Option<Duration> {
        // Recompute from the new value so a NotFound→Found transition (tile
        // published) clears the short TTL and vice versa.
        self.ttl_for(value)
    }
}

impl TileCache {
    /// Creates a tile cache with a byte-based capacity limit. Negative entries
    /// expire after `negative_ttl`; positive entries live until eviction.
    pub(crate) fn new(max_capacity_bytes: u64, negative_ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .weigher(tile_cache_weight)
            .expire_after(TileExpiry { negative_ttl })
            .build();
        Self { cache }
    }

    /// Returns a cached tile payload if present.
    pub(crate) fn get(&self, key: &TileCacheKey) -> Option<CachedTile> {
        self.cache.get(key)
    }

    /// Inserts or replaces a cached tile payload.
    pub(crate) fn put(&self, key: TileCacheKey, value: CachedTile) {
        self.cache.insert(key, value);
    }

    /// Returns the current weighted byte size of the tile cache.
    ///
    /// Flushes pending maintenance first so the value reflects recent inserts
    /// and evictions rather than moka's lazily-updated estimate.
    pub(crate) fn weighted_size(&self) -> u64 {
        self.cache.run_pending_tasks();
        self.cache.weighted_size()
    }
}

impl TilesetInfoCache {
    /// Creates a tileset metadata cache with a byte-based capacity limit.
    pub(crate) fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .weigher(resource_cache_weight)
            .build();
        Self { cache }
    }

    /// Returns a cached tileset metadata bundle if present.
    pub(crate) fn get(&self, archive: &ArchiveKey) -> Option<Arc<TilesetInfo>> {
        self.cache.get(archive)
    }

    /// Inserts or replaces a cached tileset metadata bundle.
    pub(crate) fn put(&self, archive: &ArchiveKey, info: Arc<TilesetInfo>) {
        self.cache.insert(archive.clone(), info);
    }

    /// Returns the current weighted byte size of cached tileset metadata.
    pub(crate) fn weighted_size(&self) -> u64 {
        self.cache.run_pending_tasks();
        self.cache.weighted_size()
    }
}

/// Estimates the weight of a cached tile entry.
fn tile_cache_weight(_key: &TileCacheKey, value: &CachedTile) -> u32 {
    let payload_bytes = match value {
        CachedTile::Found { bytes, .. } => Some(bytes.len()),
        CachedTile::NotFound => None,
    };
    tile_cache_entry_weight(payload_bytes)
}

/// Estimates the weight of cached tileset metadata.
fn resource_cache_weight(archive: &ArchiveKey, info: &Arc<TilesetInfo>) -> u32 {
    let total = std::mem::size_of_val(archive).saturating_add(info.approx_byte_size());
    total.min(u32::MAX as usize) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        interned::TilesetId,
        storage::{ArchiveGeneration, ArchiveKey},
    };

    fn archive_key(tileset_id: &TilesetId) -> ArchiveKey {
        ArchiveKey::new(
            tileset_id,
            ArchiveGeneration::from_wire("e:test").expect("valid test generation"),
        )
    }

    fn found() -> CachedTile {
        CachedTile::Found {
            bytes: bytes::Bytes::from_static(b"tile"),
            content_type: "application/x-protobuf",
            content_encoding: None,
        }
    }

    #[test]
    fn only_negative_entries_expire() {
        let expiry = TileExpiry {
            negative_ttl: Duration::from_secs(60),
        };
        // Absent tiles get the short TTL; present tiles never expire on their own.
        assert_eq!(
            expiry.ttl_for(&CachedTile::NotFound),
            Some(Duration::from_secs(60))
        );
        assert_eq!(expiry.ttl_for(&found()), None);
    }

    #[test]
    fn expiry_recomputes_on_update() {
        let expiry = TileExpiry {
            negative_ttl: Duration::from_secs(30),
        };
        let now = Instant::now();
        let tileset_id = TilesetId::try_new("demo/streets").unwrap();
        let key = TileCacheKey::new(&archive_key(&tileset_id), 42);
        // A NotFound→Found update (tile published) must clear the negative TTL.
        assert_eq!(
            expiry.expire_after_update(&key, &found(), now, Some(Duration::from_secs(30))),
            None
        );
        // A Found→NotFound update must (re)apply the negative TTL.
        assert_eq!(
            expiry.expire_after_update(&key, &CachedTile::NotFound, now, None),
            Some(Duration::from_secs(30))
        );
    }
}
