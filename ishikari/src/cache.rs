//! Per-node L1 caches for tiles and small resources.

use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::{Expiry, sync::Cache};

use crate::{interned::TilesetId, storage::TilesetInfo};

/// Identifies a cached tile payload within a tileset.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TileCacheKey {
    pub tileset_id: TilesetId,
    pub tile_id: u64,
}

impl TileCacheKey {
    /// Builds a tile cache key from a tileset id and PMTiles tile id.
    pub fn new(tileset_id: &TilesetId, tile_id: u64) -> Self {
        Self {
            tileset_id: tileset_id.clone(),
            tile_id,
        }
    }
}

/// Identifies a cached resource.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ResourceCacheKey {
    TilesetInfo { tileset_id: TilesetId },
}

impl ResourceCacheKey {
    /// Builds the cache key for a cached tileset metadata resource.
    pub fn tileset_info(tileset_id: &TilesetId) -> Self {
        Self::TilesetInfo {
            tileset_id: tileset_id.clone(),
        }
    }
}

/// Cacheable resources.
#[derive(Clone)]
pub enum Resource {
    TilesetInfo(Arc<TilesetInfo>),
}

/// Cache entry for a tile, including negative lookups.
#[derive(Clone)]
pub enum CachedTile {
    Found {
        bytes: bytes::Bytes,
        content_type: &'static str,
        content_encoding: Option<&'static str>,
    },
    NotFound,
}

/// Per-node L1 cache of tile payloads.
#[derive(Clone)]
pub struct TileCache {
    cache: Cache<TileCacheKey, CachedTile>,
}

/// Per-node cache of resources such as [`TilesetInfo`].
#[derive(Clone)]
pub struct ResourceCache {
    cache: Cache<ResourceCacheKey, Resource>,
}

/// Per-entry expiry policy for the tile cache.
///
/// Positive (`Found`) entries never expire on their own — PMTiles archives are
/// treated as immutable, so a tile that exists keeps its bytes until capacity
/// eviction. Negative (`NotFound`) entries expire after `negative_ttl`: absence
/// is the *mutable* state (a tile can be published later, or a whole archive
/// republished), so bounding the negative lifetime caps how long a newly-added
/// tile stays hidden — and stops a caller from poisoning the cache with lookups
/// of not-yet-existing tiles to delay their rollout. A cache *hit* does not
/// extend the entry (`expire_after_read` keeps the default), so hammering an
/// absent tile cannot keep its negative entry alive past `negative_ttl`.
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
    pub fn new(max_capacity_bytes: u64, negative_ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .weigher(tile_cache_weight)
            .expire_after(TileExpiry { negative_ttl })
            .build();
        Self { cache }
    }

    /// Returns a cached tile payload if present.
    pub fn get(&self, key: &TileCacheKey) -> Option<CachedTile> {
        self.cache.get(key)
    }

    /// Inserts or replaces a cached tile payload.
    pub fn put(&self, key: TileCacheKey, value: CachedTile) {
        self.cache.insert(key, value);
    }

    /// Returns the current weighted byte size of the tile cache.
    ///
    /// Flushes pending maintenance first so the value reflects recent inserts
    /// and evictions rather than moka's lazily-updated estimate.
    pub fn weighted_size(&self) -> u64 {
        self.cache.run_pending_tasks();
        self.cache.weighted_size()
    }
}

impl ResourceCache {
    /// Creates a resource cache with a byte-based capacity limit.
    pub fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .weigher(resource_cache_weight)
            .build();
        Self { cache }
    }

    /// Returns a cached tileset metadata bundle if present.
    pub fn get_tileset_info(&self, tileset_id: &TilesetId) -> Option<Arc<TilesetInfo>> {
        let key = ResourceCacheKey::tileset_info(tileset_id);
        self.cache.get(&key).map(|Resource::TilesetInfo(info)| info)
    }

    /// Inserts or replaces a cached tileset metadata bundle.
    pub fn put_tileset_info(&self, tileset_id: &TilesetId, info: Arc<TilesetInfo>) {
        self.cache.insert(
            ResourceCacheKey::tileset_info(tileset_id),
            Resource::TilesetInfo(info),
        );
    }
}

/// Estimates the weight of a cached tile entry.
fn tile_cache_weight(key: &TileCacheKey, value: &CachedTile) -> u32 {
    let value_size = match value {
        CachedTile::Found { bytes, .. } => bytes.len(),
        CachedTile::NotFound => 0,
    };
    let total = std::mem::size_of_val(key).saturating_add(value_size);
    total.min(u32::MAX as usize) as u32
}

/// Estimates the weight of a cached resource entry.
fn resource_cache_weight(key: &ResourceCacheKey, value: &Resource) -> u32 {
    match (key, value) {
        (ResourceCacheKey::TilesetInfo { tileset_id }, Resource::TilesetInfo(info)) => {
            let total = std::mem::size_of_val(tileset_id).saturating_add(info.approx_byte_size());
            total.min(u32::MAX as usize) as u32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let key = TileCacheKey::new(&TilesetId::new_unchecked("demo/streets"), 42);
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
