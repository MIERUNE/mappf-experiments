//! Caches for PMTiles archive bootstraps and leaf directories.

use std::sync::Arc;
use std::time::Duration;

use moka::sync::Cache;

use super::{
    format::{Directory, Header},
    metadata::Metadata,
};
use crate::interned::TilesetId;

const ARCHIVE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const LEAF_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// How long an authoritatively-absent archive is remembered. Deliberately tiny:
/// long enough to collapse a burst of requests for the same missing archive into
/// one backend probe, but short enough that a newly-provisioned archive becomes
/// visible almost immediately — so the negative cache cannot itself be used to
/// delay a tileset's rollout.
const ABSENT_ARCHIVE_TTL: Duration = Duration::from_secs(1);
/// Bounded entry count for the absence cache, so enumerating distinct missing
/// ids cannot grow it without limit (each entry is also cleared after 1s).
const ABSENT_ARCHIVE_MAX_ENTRIES: u64 = 100_000;

/// Shared caches for per-tileset PMTiles bootstraps and leaf directories.
#[derive(Clone)]
pub struct ArchiveCache {
    archives: Cache<TilesetId, ArchiveBootstrap>,
    leafs: Cache<LeafCacheKey, Arc<Directory>>,
    /// Short-TTL set of tileset ids known to have no archive in object storage.
    absent: Cache<TilesetId, ()>,
}

/// Cached PMTiles header, root directory, and optional metadata section.
#[derive(Clone)]
pub struct ArchiveBootstrap {
    pub header: Header,
    pub root: Arc<Directory>,
    pub metadata: Option<Arc<Metadata>>,
}

impl ArchiveBootstrap {
    /// Builds an archive bootstrap from header, root directory, and optional metadata.
    pub fn new(header: Header, root: Arc<Directory>, metadata: Option<Arc<Metadata>>) -> Self {
        Self {
            header,
            root,
            metadata,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LeafCacheKey {
    pub tileset_id: TilesetId,
    pub offset: u64,
}

impl LeafCacheKey {
    /// Builds a leaf cache key from a tileset id and absolute leaf offset.
    pub fn new(tileset_id: &TilesetId, offset: u64) -> Self {
        Self {
            tileset_id: tileset_id.clone(),
            offset,
        }
    }
}

impl ArchiveCache {
    /// Creates the shared PMTiles metadata caches.
    pub fn new() -> Self {
        Self {
            archives: Cache::builder()
                .max_capacity(ARCHIVE_CACHE_MAX_BYTES)
                .weigher(
                    |tileset_id: &TilesetId, archive: &ArchiveBootstrap| -> u32 {
                        (std::mem::size_of_val(tileset_id)
                            + std::mem::size_of::<Header>()
                            + archive.root.approx_byte_size()
                            + archive
                                .metadata
                                .as_ref()
                                .map_or(0, |metadata| metadata.approx_byte_size()))
                        .min(u32::MAX as usize) as u32
                    },
                )
                .build(),
            leafs: Cache::builder()
                .max_capacity(LEAF_CACHE_MAX_BYTES)
                .weigher(|key: &LeafCacheKey, directory: &Arc<Directory>| -> u32 {
                    (std::mem::size_of_val(key) + directory.approx_byte_size())
                        .min(u32::MAX as usize) as u32
                })
                .build(),
            absent: Cache::builder()
                .max_capacity(ABSENT_ARCHIVE_MAX_ENTRIES)
                .time_to_live(ABSENT_ARCHIVE_TTL)
                .build(),
        }
    }

    /// Returns the cached bootstrap for a tileset if present.
    pub fn get(&self, tileset_id: &TilesetId) -> Option<ArchiveBootstrap> {
        self.archives.get(tileset_id)
    }

    /// Inserts or replaces the cached header/root index for a tileset. Clears any
    /// stale absence entry so a just-provisioned archive is served immediately.
    pub fn put(&self, tileset_id: &TilesetId, archive: ArchiveBootstrap) {
        self.absent.invalidate(tileset_id);
        self.archives.insert(tileset_id.clone(), archive);
    }

    /// Whether this tileset was recently found absent in object storage.
    pub fn is_known_absent(&self, tileset_id: &TilesetId) -> bool {
        self.absent.get(tileset_id).is_some()
    }

    /// Records that a tileset has no archive in object storage, for a short TTL.
    pub fn mark_absent(&self, tileset_id: &TilesetId) {
        self.absent.insert(tileset_id.clone(), ());
    }

    /// Replaces the cached metadata for a tileset while preserving header and root.
    pub fn put_metadata(&self, tileset_id: &TilesetId, metadata: Arc<Metadata>) {
        if let Some(mut archive) = self.archives.get(tileset_id) {
            archive.metadata = Some(metadata);
            self.archives.insert(tileset_id.clone(), archive);
        }
    }

    /// Returns a cached leaf directory if present.
    pub fn get_leaf(&self, key: &LeafCacheKey) -> Option<Arc<Directory>> {
        self.leafs.get(key)
    }

    /// Inserts or replaces a cached leaf directory.
    pub fn put_leaf(&self, key: LeafCacheKey, directory: Arc<Directory>) {
        self.leafs.insert(key, directory);
    }
}

#[cfg(test)]
mod tests {
    use super::ArchiveCache;
    use crate::interned::TilesetId;

    #[test]
    fn records_absence_per_tileset() {
        let cache = ArchiveCache::new();
        let missing = TilesetId::new_unchecked("maybe/later");
        let other = TilesetId::new_unchecked("something/else");
        assert!(!cache.is_known_absent(&missing));

        cache.mark_absent(&missing);
        cache.absent.run_pending_tasks();
        assert!(cache.is_known_absent(&missing));
        // Absence is per-id, so it never shadows a different tileset.
        assert!(!cache.is_known_absent(&other));
    }
}
