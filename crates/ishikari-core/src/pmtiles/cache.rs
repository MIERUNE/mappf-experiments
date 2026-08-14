//! Caches for PMTiles archive bootstraps and leaf directories.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use moka::{ops::compute::Op, sync::Cache};

use crate::interned::TilesetId;
use crate::storage::generation::{ArchiveGeneration, ArchiveKey};
use mmpf_pmtiles::{ArchiveBootstrap, Directory, Metadata};

pub(crate) const DEFAULT_ARCHIVE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const DEFAULT_LEAF_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
#[cfg(any(test, feature = "simulator-support"))]
pub(crate) const DEFAULT_ARCHIVE_REVALIDATION_INTERVAL: Duration = Duration::from_mins(5);

/// How long an authoritatively-absent archive is remembered. Deliberately tiny:
/// long enough to collapse a burst of requests for the same missing archive into
/// one backend probe, but short enough that a newly-provisioned archive becomes
/// visible almost immediately — so the negative cache cannot itself be used to
/// delay a tileset's rollout.
const ABSENT_ARCHIVE_TTL: Duration = Duration::from_secs(1);
pub(crate) const ARCHIVE_REVALIDATION_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
/// Bounded entry count for the absence cache, so enumerating distinct missing
/// ids cannot grow it without limit (each entry is also cleared after 1s).
const ABSENT_ARCHIVE_MAX_ENTRIES: u64 = 100_000;

/// Shared caches for per-tileset PMTiles bootstraps and leaf directories.
#[derive(Clone)]
pub(crate) struct ArchiveCache {
    archives: Cache<TilesetId, CachedArchive>,
    leafs: Cache<LeafCacheKey, Arc<Directory>>,
    archive_revalidation_interval: Duration,
    /// Short-TTL set of tileset ids known to have no archive in object storage.
    absent: Cache<TilesetId, ()>,
}

#[derive(Clone)]
struct CachedArchive {
    generation: ArchiveGeneration,
    archive: ArchiveBootstrap,
    bootstrap: Bytes,
    revalidate_after: Instant,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct LeafCacheKey {
    pub(crate) archive: ArchiveKey,
    pub(crate) offset: u64,
    pub(crate) length: usize,
}

impl LeafCacheKey {
    /// Builds a leaf cache key from the complete byte-range identity.
    pub(crate) fn new(archive: &ArchiveKey, offset: u64, length: usize) -> Self {
        Self {
            archive: archive.clone(),
            offset,
            length,
        }
    }
}

impl ArchiveCache {
    /// Creates the shared PMTiles metadata caches.
    pub(crate) fn new(
        archive_max_bytes: u64,
        leaf_max_bytes: u64,
        archive_revalidation_interval: Duration,
    ) -> Self {
        Self {
            archives: Cache::builder()
                .max_capacity(archive_max_bytes)
                .weigher(|tileset_id: &TilesetId, cached: &CachedArchive| -> u32 {
                    (std::mem::size_of_val(tileset_id)
                        + std::mem::size_of::<CachedArchive>()
                        + cached.bootstrap.len()
                        + cached.archive.root.approx_byte_size()
                        + cached
                            .archive
                            .metadata
                            .as_ref()
                            .map_or(0, |metadata| metadata.approx_byte_size()))
                    .min(u32::MAX as usize) as u32
                })
                .build(),
            leafs: Cache::builder()
                .max_capacity(leaf_max_bytes)
                .weigher(|key: &LeafCacheKey, directory: &Arc<Directory>| -> u32 {
                    (std::mem::size_of_val(key) + directory.approx_byte_size())
                        .min(u32::MAX as usize) as u32
                })
                .build(),
            archive_revalidation_interval,
            absent: Cache::builder()
                .max_capacity(ABSENT_ARCHIVE_MAX_ENTRIES)
                .time_to_live(ABSENT_ARCHIVE_TTL)
                .build(),
        }
    }

    /// Returns the cached bootstrap for a tileset if present.
    pub(crate) fn get(&self, tileset_id: &TilesetId) -> Option<VersionedArchive> {
        self.archives
            .get(tileset_id)
            .filter(|cached| Instant::now() < cached.revalidate_after)
            .map(|cached| VersionedArchive {
                key: ArchiveKey::new(tileset_id, cached.generation),
                archive: cached.archive,
            })
    }

    /// Whether the logical pointer exists but is due for a direct backend
    /// observation. A due entry is not refreshed from a peer, because another
    /// pod may still hold the same old generation.
    pub(crate) fn requires_revalidation(&self, tileset_id: &TilesetId) -> bool {
        self.archives
            .get(tileset_id)
            .is_some_and(|cached| Instant::now() >= cached.revalidate_after)
    }

    /// Makes a stale coherent generation temporarily reusable after a failed
    /// authoritative check. This bounds retry amplification without turning an
    /// object-store interruption into immediate loss of the warm serving set.
    pub(crate) fn defer_expired_revalidation(&self, tileset_id: &TilesetId) {
        let now = Instant::now();
        let retry_after = now
            .checked_add(ARCHIVE_REVALIDATION_FAILURE_COOLDOWN)
            .expect("fixed revalidation cooldown fits Instant");
        self.archives
            .entry(tileset_id.clone())
            .and_compute_with(move |existing| match existing {
                Some(existing) if existing.value().revalidate_after <= now => {
                    let mut cached = existing.into_value();
                    cached.revalidate_after = retry_after;
                    Op::Put(cached)
                }
                Some(_) | None => Op::Nop,
            });
    }

    /// Returns the immutable raw bootstrap bytes retained with the decoded entry.
    pub(crate) fn get_bootstrap_bytes(&self, archive: &ArchiveKey) -> Option<Bytes> {
        self.archives
            .get(&archive.tileset_id)
            .filter(|cached| cached.generation == archive.generation)
            .map(|cached| cached.bootstrap)
    }

    /// Installs a bootstrap without discarding metadata already learned for the
    /// same immutable archive key. Clears any stale absence entry so a
    /// just-provisioned archive is served immediately.
    pub(crate) fn put(
        &self,
        tileset_id: &TilesetId,
        generation: ArchiveGeneration,
        mut archive: ArchiveBootstrap,
        bootstrap: Bytes,
    ) {
        self.absent.invalidate(tileset_id);
        let revalidate_after = Instant::now()
            .checked_add(self.archive_revalidation_interval)
            .expect("validated archive revalidation interval fits Instant");
        self.archives
            .entry(tileset_id.clone())
            .and_compute_with(move |existing| {
                if archive.metadata.is_none()
                    && let Some(existing) = existing
                    && existing.value().generation == generation
                {
                    archive.metadata = existing.into_value().archive.metadata;
                }
                Op::Put(CachedArchive {
                    generation,
                    archive,
                    bootstrap,
                    revalidate_after,
                })
            });
    }

    /// Whether this tileset was recently found absent in object storage.
    pub(crate) fn is_known_absent(&self, tileset_id: &TilesetId) -> bool {
        self.absent.get(tileset_id).is_some()
    }

    /// Records that a tileset has no archive in object storage, for a short TTL.
    pub(crate) fn mark_absent(&self, tileset_id: &TilesetId) {
        self.absent.insert(tileset_id.clone(), ());
    }

    /// Returns the weighted byte sizes of archive bootstrap and leaf caches.
    pub(crate) fn weighted_sizes(&self) -> (u64, u64) {
        self.archives.run_pending_tasks();
        self.leafs.run_pending_tasks();
        (self.archives.weighted_size(), self.leafs.weighted_size())
    }

    /// Replaces the cached metadata for a tileset while preserving header and root.
    pub(crate) fn put_metadata(&self, archive: &ArchiveKey, metadata: Arc<Metadata>) {
        self.archives
            .entry(archive.tileset_id.clone())
            .and_compute_with(move |existing| match existing {
                Some(existing) if existing.value().generation == archive.generation => {
                    let mut cached = existing.into_value();
                    cached.archive.metadata = Some(metadata);
                    Op::Put(cached)
                }
                Some(_) | None => Op::Nop,
            });
    }

    /// Returns a cached leaf directory if present.
    pub(crate) fn get_leaf(&self, key: &LeafCacheKey) -> Option<Arc<Directory>> {
        self.leafs.get(key)
    }

    /// Inserts or replaces a cached leaf directory.
    pub(crate) fn put_leaf(&self, key: LeafCacheKey, directory: Arc<Directory>) {
        self.leafs.insert(key, directory);
    }

    /// Drops the current bootstrap only when it still names the failed
    /// generation. Generation-keyed leaf entries may age out naturally.
    pub(crate) fn invalidate_generation(&self, archive: &ArchiveKey) {
        let generation = archive.generation.clone();
        self.archives
            .entry(archive.tileset_id.clone())
            .and_compute_with(move |existing| match existing {
                Some(existing) if existing.value().generation == generation => Op::Remove,
                Some(_) | None => Op::Nop,
            });
    }
}

#[derive(Clone)]
pub(crate) struct VersionedArchive {
    pub(crate) key: ArchiveKey,
    pub(crate) archive: ArchiveBootstrap,
}

#[cfg(test)]
mod tests {
    use super::{ArchiveCache, LeafCacheKey};
    use crate::interned::TilesetId;
    use crate::storage::{ArchiveGeneration, ArchiveKey};
    use bytes::Bytes;
    use mmpf_pmtiles::{ArchiveBootstrap, Compression, Directory, Header, Metadata, TileType};
    use std::{sync::Arc, time::Duration};

    fn test_cache() -> ArchiveCache {
        ArchiveCache::new(
            1024 * 1024,
            1024 * 1024,
            super::DEFAULT_ARCHIVE_REVALIDATION_INTERVAL,
        )
    }

    fn generation() -> ArchiveGeneration {
        ArchiveGeneration::from_wire("e:test").expect("valid test generation")
    }

    fn archive_key(tileset_id: &TilesetId) -> ArchiveKey {
        ArchiveKey::new(tileset_id, generation())
    }

    fn archive(metadata: Option<Arc<Metadata>>) -> ArchiveBootstrap {
        ArchiveBootstrap::new(
            Header {
                version: 3,
                root_offset: 127,
                root_length: 0,
                metadata_offset: 127,
                metadata_length: 0,
                leaf_offset: 127,
                leaf_length: 0,
                data_offset: 127,
                data_length: 0,
                n_addressed_tiles: 0,
                n_tile_entries: 0,
                n_tile_contents: 0,
                clustered: false,
                internal_compression: Compression::None,
                tile_compression: Compression::None,
                tile_type: TileType::Mvt,
                min_zoom: 0,
                max_zoom: 0,
                min_longitude: 0.0,
                min_latitude: 0.0,
                max_longitude: 0.0,
                max_latitude: 0.0,
                center_zoom: 0,
                center_longitude: 0.0,
                center_latitude: 0.0,
            },
            Arc::new(Directory {
                entries: Vec::new(),
            }),
            metadata,
        )
        .expect("valid test archive")
    }

    #[test]
    fn records_absence_per_tileset() {
        let cache = test_cache();
        let missing = TilesetId::try_new("maybe/later").unwrap();
        let other = TilesetId::try_new("something/else").unwrap();
        assert!(!cache.is_known_absent(&missing));

        cache.mark_absent(&missing);
        cache.absent.run_pending_tasks();
        assert!(cache.is_known_absent(&missing));
        // Absence is per-id, so it never shadows a different tileset.
        assert!(!cache.is_known_absent(&other));
    }

    #[test]
    fn leaf_identity_includes_the_requested_length() {
        let tileset_id = TilesetId::try_new("archive").unwrap();
        let archive = archive_key(&tileset_id);
        assert_ne!(
            LeafCacheKey::new(&archive, 128, 64),
            LeafCacheKey::new(&archive, 128, 65)
        );
    }

    #[test]
    fn sparse_bootstrap_cannot_replace_cached_metadata() {
        let cache = test_cache();
        let tileset_id = TilesetId::try_new("archive").unwrap();
        let metadata = Arc::new(Metadata {
            name: Some("enriched".to_string()),
            ..Metadata::default()
        });

        cache.put(
            &tileset_id,
            generation(),
            archive(Some(metadata)),
            Bytes::from_static(b"first bootstrap"),
        );
        cache.put(
            &tileset_id,
            generation(),
            archive(None),
            Bytes::from_static(b"second bootstrap"),
        );

        assert_eq!(
            cache
                .get(&tileset_id)
                .and_then(|archive| archive.archive.metadata)
                .and_then(|metadata| metadata.name.clone())
                .as_deref(),
            Some("enriched")
        );
        assert_eq!(
            cache
                .get_bootstrap_bytes(&archive_key(&tileset_id))
                .as_deref(),
            Some(b"second bootstrap".as_slice())
        );
    }

    #[test]
    fn metadata_installation_enriches_an_existing_bootstrap() {
        let cache = test_cache();
        let tileset_id = TilesetId::try_new("archive").unwrap();
        let key = archive_key(&tileset_id);
        cache.put(
            &tileset_id,
            generation(),
            archive(None),
            Bytes::from_static(b"bootstrap"),
        );

        cache.put_metadata(
            &key,
            Arc::new(Metadata {
                name: Some("enriched".to_string()),
                ..Metadata::default()
            }),
        );

        assert_eq!(
            cache
                .get(&tileset_id)
                .and_then(|archive| archive.archive.metadata)
                .and_then(|metadata| metadata.name.clone())
                .as_deref(),
            Some("enriched")
        );
    }

    #[test]
    fn raw_bootstrap_bytes_are_generation_bound() {
        let cache = test_cache();
        let tileset_id = TilesetId::try_new("archive").unwrap();
        let first = archive_key(&tileset_id);
        cache.put(
            &tileset_id,
            first.generation.clone(),
            archive(None),
            Bytes::from_static(b"first"),
        );
        let second_generation = ArchiveGeneration::from_wire("e:second").unwrap();
        let second = ArchiveKey::new(&tileset_id, second_generation.clone());
        cache.put(
            &tileset_id,
            second_generation,
            archive(None),
            Bytes::from_static(b"second"),
        );

        assert!(cache.get_bootstrap_bytes(&first).is_none());
        assert_eq!(
            cache.get_bootstrap_bytes(&second).as_deref(),
            Some(b"second".as_slice())
        );
    }

    #[test]
    fn expired_bootstrap_requires_revalidation() {
        let cache = ArchiveCache::new(1024 * 1024, 1024 * 1024, Duration::ZERO);
        let tileset_id = TilesetId::try_new("archive").unwrap();
        cache.put(
            &tileset_id,
            generation(),
            archive(None),
            Bytes::from_static(b"bootstrap"),
        );

        assert!(cache.get(&tileset_id).is_none());

        cache.defer_expired_revalidation(&tileset_id);
        assert!(cache.get(&tileset_id).is_some());
    }
}
