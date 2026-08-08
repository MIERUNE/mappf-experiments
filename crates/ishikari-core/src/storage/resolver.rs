//! Tileset serving, forwarding, and cache orchestration.

use std::sync::Arc;

#[cfg(feature = "simulator-support")]
use super::peer::InternalFetchResponse;
use anyhow::Result;
use bytes::Bytes;
#[cfg(feature = "simulator-support")]
use bytes::{BufMut, BytesMut};
use thiserror::Error;
use tracing::{debug, warn};

use super::{
    chunked_store::{BackendLatencyModel, ChunkedStore, ChunkedStoreConfig},
    generation::{ArchiveGeneration, ArchiveKey},
    peer::{
        InternalTileSource, InternalTransport, Peer, PeerBackend, PeerDirectory, PeerFetchError,
        ProviderRequest, ProviderRouteOutcome,
    },
    pmtiles::{DistributedPmtilesStorage, PmtilesReadSource},
    routing::HrwRouter,
    tuning::ResolverTuning,
};
use crate::{
    cache::{CachedTile, TileCache, TileCacheKey, TilesetInfoCache},
    interned::{ResourceRoutingKey, TilesetId},
    metrics::NodeMetrics,
    pmtiles::{
        ArchiveResource, BootstrapTransfer, DEFAULT_ARCHIVE_CACHE_MAX_BYTES,
        DEFAULT_LEAF_CACHE_MAX_BYTES, Header, LocalLeafError, Metadata, Reader as PmtilesReader,
        StorageError, TileData,
    },
};

const DEFAULT_RESOURCE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Byte-weight ceilings for the resolver's tileset metadata and decoded
/// PMTiles index caches. Production supplies these from its aggregate cache
/// budget; simulators and direct library users can retain the defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceCacheCapacities {
    pub resource_max_bytes: u64,
    pub archive_max_bytes: u64,
    pub leaf_max_bytes: u64,
}

impl Default for ResourceCacheCapacities {
    fn default() -> Self {
        Self {
            resource_max_bytes: DEFAULT_RESOURCE_CACHE_MAX_BYTES,
            archive_max_bytes: DEFAULT_ARCHIVE_CACHE_MAX_BYTES,
            leaf_max_bytes: DEFAULT_LEAF_CACHE_MAX_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TilesetInfo {
    pub header: Header,
    pub metadata: Arc<Metadata>,
    pub generation: ArchiveGeneration,
}

/// Tile bytes bound to the object generation used for their PMTiles lookup.
pub struct ResolvedTile {
    pub data: TileData,
    pub generation: ArchiveGeneration,
}

impl TilesetInfo {
    /// Estimates the heap footprint of cached tileset metadata.
    pub(crate) fn approx_byte_size(&self) -> usize {
        std::mem::size_of::<Header>() + self.metadata.approx_byte_size()
    }
}

/// Whether an archive exists, as resolved by a header-only presence check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchivePresence {
    Present,
    Absent,
}

/// Runtime configuration for constructing a [`ResourceResolver`].
pub struct ResourceResolverConfig {
    pub self_node_id: String,
    pub peer_directory: Arc<dyn PeerDirectory>,
    /// Concrete internal peer transport, injected by the composition root so the
    /// core does not depend on a specific HTTP client.
    pub transport: Arc<dyn InternalTransport>,
    pub tileset_sources: String,
    pub tuning: ResolverTuning,
    pub cache_capacities: ResourceCacheCapacities,
    pub artificial_backend_delay_ms: u64,
    pub object_store_registry: Arc<super::ObjectStoreRegistry>,
    pub metrics: NodeMetrics,
}

/// Storage and cache configuration shared by production and in-process resolvers.
pub struct ResourceResolverStorageConfig {
    pub tileset_sources: String,
    pub tuning: ResolverTuning,
    pub cache_capacities: ResourceCacheCapacities,
    pub backend_latency: BackendLatencyModel,
    pub peer_tile_cache_policy: PeerTileCachePolicy,
    pub object_store_registry: Arc<super::ObjectStoreRegistry>,
    pub metrics: NodeMetrics,
}

/// High-level resource resolver that combines routing, forwarding, and caches.
pub struct ResourceResolver {
    peer_backend: PeerBackend,
    pmtiles: Arc<PmtilesReader<DistributedPmtilesStorage>>,
    resource_cache: TilesetInfoCache,
    tile_cache: TileCache,
    peer_tile_cache_policy: PeerTileCachePolicy,
}

/// Whether a successful peer response is also retained in the entry node's L1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerTileCachePolicy {
    /// Keep a replicated near-entry hot tier (the production default).
    EntryAndOwner,
    /// Keep positive tile bytes only on the HRW owner.
    #[cfg_attr(not(feature = "simulator-support"), allow(dead_code))]
    OwnerOnly,
}

enum CachedTileLookup {
    Found(ResolvedTile),
    NotFound,
    None,
}

/// Where a routed tile response was served from, for metrics.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TileSource {
    /// Positive hit in the entry node's L1 tile cache.
    SelfTileCache,
    /// Resolved locally using PMTiles/index and chunk caches only.
    SelfChunkCache,
    /// Resolved locally after waiting for object-storage work.
    SelfBackend,
    /// Negative hit in the local L1 tile cache.
    NegativeCache,
    /// Fetched from a peer that used only its caches.
    PeerCache,
    /// Fetched from a peer that waited for object-storage work.
    PeerBackend,
    /// Local PMTiles lookup missed and inserted a negative cache entry.
    SelfMiss,
    /// A reachable owner authoritatively reported the tile absent; a negative
    /// cache entry was inserted locally.
    PeerMiss,
}

impl TileSource {
    /// Returns the stable source category used by metrics and simulation reports.
    pub fn report_label(self) -> &'static str {
        match self {
            Self::SelfTileCache | Self::SelfChunkCache => "self_cache",
            Self::SelfBackend => "self_backend",
            Self::NegativeCache | Self::SelfMiss | Self::PeerMiss => "miss",
            Self::PeerCache => "peer_cache",
            Self::PeerBackend => "peer_backend",
        }
    }

    /// Returns whether this was a positive hit in the entry node's L1 tile cache.
    ///
    /// Negative-cache hits remain misses in aggregate hit-rate reports; their
    /// cache behavior remains visible through bounded tile-cache outcomes.
    pub fn is_l1_hit(self) -> bool {
        matches!(self, Self::SelfTileCache)
    }
}

impl ResourceResolver {
    /// Builds the resource resolver and its local caches.
    pub fn new(config: ResourceResolverConfig) -> Result<Self> {
        let ResourceResolverConfig {
            self_node_id,
            peer_directory,
            transport,
            tileset_sources,
            tuning,
            cache_capacities,
            artificial_backend_delay_ms,
            object_store_registry,
            metrics,
        } = config;
        let router = HrwRouter::new(tuning.candidate_count(), tuning.tile_group_size());
        let peer_backend = PeerBackend::with_dependencies(
            self_node_id,
            peer_directory,
            router,
            transport,
            metrics.clone(),
        );
        Self::build_with_peer_backend(
            ResourceResolverStorageConfig {
                tileset_sources,
                tuning,
                cache_capacities,
                backend_latency: BackendLatencyModel::fixed(artificial_backend_delay_ms),
                peer_tile_cache_policy: PeerTileCachePolicy::EntryAndOwner,
                object_store_registry,
                metrics,
            },
            peer_backend,
        )
    }

    /// Builds a resolver around an injected peer backend.
    #[cfg(feature = "simulator-support")]
    pub fn with_peer_backend(
        config: ResourceResolverStorageConfig,
        peer_backend: PeerBackend,
    ) -> Result<Self> {
        Self::build_with_peer_backend(config, peer_backend)
    }

    fn build_with_peer_backend(
        config: ResourceResolverStorageConfig,
        peer_backend: PeerBackend,
    ) -> Result<Self> {
        let tuning = config.tuning;
        let cache_capacities = config.cache_capacities;
        config
            .metrics
            .set_chunk_config(tuning.chunk_size_bytes(), tuning.max_fetch_chunks());
        let chunked_store = ChunkedStore::new(
            ChunkedStoreConfig {
                tileset_sources: config.tileset_sources,
                chunk_size: tuning.chunk_size_bytes(),
                max_fetch_chunks: tuning.max_fetch_chunks(),
                chunk_fetch_merge_window: tuning.chunk_fetch_merge_window(),
                backend_fetch_concurrency: tuning.backend_fetch_concurrency(),
                backend_fetch_max_inflight: tuning.backend_fetch_max_inflight(),
                backend_latency: config.backend_latency,
                chunk_cache_max_bytes: tuning.chunk_cache_max_bytes(),
            },
            &config.object_store_registry,
            config.metrics.clone(),
        )?;
        let pmtiles_storage = DistributedPmtilesStorage::new(chunked_store, peer_backend.clone());
        let pmtiles = Arc::new(PmtilesReader::with_index_cache_capacities(
            pmtiles_storage,
            cache_capacities.archive_max_bytes,
            cache_capacities.leaf_max_bytes,
            tuning.archive_revalidation_interval(),
        )?);
        Ok(Self {
            peer_backend,
            pmtiles,
            resource_cache: TilesetInfoCache::new(cache_capacities.resource_max_bytes),
            tile_cache: TileCache::new(tuning.tile_cache_max_bytes(), tuning.tile_negative_ttl()),
            peer_tile_cache_policy: config.peer_tile_cache_policy,
        })
    }

    /// Returns stable tile-cache metric outcomes for this resolver's insertion policy.
    pub fn cache_outcomes(&self, source: TileSource) -> &'static [&'static str] {
        match source {
            TileSource::SelfTileCache => &["hit"],
            TileSource::NegativeCache => &["negative"],
            TileSource::SelfChunkCache | TileSource::SelfBackend => &["miss", "insert"],
            TileSource::PeerCache | TileSource::PeerBackend
                if self.peer_tile_cache_policy == PeerTileCachePolicy::EntryAndOwner =>
            {
                &["miss", "insert"]
            }
            TileSource::PeerCache | TileSource::PeerBackend => &["miss"],
            TileSource::SelfMiss | TileSource::PeerMiss => &["miss", "negative"],
        }
    }

    /// Returns the current weighted byte size of the tileset-resource cache.
    pub fn resource_cache_weighted_size(&self) -> u64 {
        self.resource_cache.weighted_size()
    }

    /// Returns weighted byte sizes for archive-bootstrap and leaf-directory caches.
    pub fn pmtiles_index_cache_weighted_sizes(&self) -> (u64, u64) {
        self.pmtiles.index_cache_weighted_sizes()
    }

    /// Returns the current weighted byte size of the tile cache.
    pub fn tile_cache_weighted_size(&self) -> u64 {
        self.tile_cache.weighted_size()
    }

    /// Returns the current weighted byte size of the chunk cache.
    pub fn chunk_cache_weighted_size(&self) -> u64 {
        self.pmtiles.storage().chunk_cache_weighted_size()
    }

    pub fn received_bytes(&self) -> u64 {
        self.pmtiles.storage().received_bytes()
    }

    /// Routes a typed non-PMTiles provider resource by its stable HRW placement key.
    ///
    /// Returns `None` when the local node should fetch the resource itself.
    pub async fn route_provider_resource(
        &self,
        request: &ProviderRequest<'_>,
    ) -> Result<Option<ProviderRouteOutcome>> {
        self.peer_backend.route_provider_request(request).await
    }

    /// Routes a typed generated-tile resource by the normal tile-group HRW
    /// policy. `None` means this node should produce the resource locally.
    pub async fn route_derived_resource(
        &self,
        routing_key: &ResourceRoutingKey,
        tile_id: u64,
        internal_path: &str,
    ) -> Result<Option<Bytes>> {
        self.peer_backend
            .route_derived_resource(routing_key, tile_id, internal_path)
            .await
    }

    /// Serves an external tile request addressed by PMTiles tile id.
    pub async fn route_tile(
        &self,
        tileset_id: TilesetId,
        tile_id: u64,
    ) -> Result<(Option<ResolvedTile>, TileSource), TilesetError> {
        debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "tile request"
        );

        let Some(archive) = self
            .pmtiles
            .archive_key(&tileset_id)
            .await
            .map_err(internal_tileset_error)?
        else {
            return Ok((None, TileSource::SelfMiss));
        };

        match self.load_cached_tile(&archive, tile_id) {
            CachedTileLookup::Found(tile) => {
                return Ok((Some(tile), TileSource::SelfTileCache));
            }
            CachedTileLookup::NotFound => return Ok((None, TileSource::NegativeCache)),
            CachedTileLookup::None => {}
        }

        let candidates = self.peer_backend.route_tile(&tileset_id, tile_id).await;

        if candidates.is_empty()
            || candidates
                .first()
                .is_some_and(|peer| self.peer_backend.is_self(&peer.peer))
        {
            return self.load_local_tile_with_source(&tileset_id, tile_id).await;
        }

        for peer in candidates {
            if self.peer_backend.is_self(&peer.peer) {
                return self.load_local_tile_with_source(&tileset_id, tile_id).await;
            }

            match self
                .load_tile_from_peer(&peer.peer, &archive, tile_id)
                .await
            {
                Ok(Some((tile, peer_source))) => {
                    let source = match peer_source {
                        InternalTileSource::Cache => TileSource::PeerCache,
                        InternalTileSource::Backend => TileSource::PeerBackend,
                    };
                    return Ok((Some(tile), source));
                }
                Err(TilesetError::Miss) => {
                    debug!(
                        peer_id = %peer.peer.id,
                        tileset_id = %tileset_id,
                        tile_id = tile_id,
                        "peer reported tile absent; falling back to the locally pinned generation"
                    );
                }
                // The peer served tile bytes but the tileset header could not be
                // resolved here — inconclusive, so fall back rather than
                // negative-caching a tile the peer actually has.
                Ok(None) => {
                    debug!(
                        peer_id = %peer.peer.id,
                        tileset_id = %tileset_id,
                        tile_id = tile_id,
                        "peer returned tile without resolvable header; trying fallback"
                    );
                }
                Err(error) if error.is_retryable() => {
                    warn!(peer_id = %peer.peer.id, error = %error, "tile forward failed; trying fallback");
                }
                Err(error) => return Err(error),
            }
        }

        self.load_local_tile_with_source(&tileset_id, tile_id).await
    }

    /// Loads a tile from local storage and tags whether it was found.
    async fn load_local_tile_with_source(
        &self,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<(Option<ResolvedTile>, TileSource), TilesetError> {
        let (tile, read_source) = self.load_local_tile(tileset_id, tile_id).await?;
        let source = if tile.is_some() && read_source == PmtilesReadSource::Cache {
            TileSource::SelfChunkCache
        } else if tile.is_some() {
            TileSource::SelfBackend
        } else {
            TileSource::SelfMiss
        };
        Ok((tile, source))
    }

    /// Serves an internal tile request and reports whether caches were sufficient.
    pub async fn load_tile_by_id_with_source(
        &self,
        tileset_id: TilesetId,
        tile_id: u64,
    ) -> Result<(Option<ResolvedTile>, TileSource), TilesetError> {
        debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "internal tile request"
        );

        let Some(archive) = self
            .pmtiles
            .archive_key(&tileset_id)
            .await
            .map_err(internal_tileset_error)?
        else {
            return Ok((None, TileSource::SelfMiss));
        };
        match self.load_cached_tile(&archive, tile_id) {
            CachedTileLookup::Found(tile) => {
                return Ok((Some(tile), TileSource::SelfTileCache));
            }
            CachedTileLookup::NotFound => return Ok((None, TileSource::NegativeCache)),
            CachedTileLookup::None => {}
        }

        self.load_local_tile_with_source(&tileset_id, tile_id).await
    }

    /// Loads tileset metadata, reusing the local resource cache when present.
    pub async fn load_tileset_info(
        &self,
        tileset_id: TilesetId,
    ) -> Result<Option<Arc<TilesetInfo>>, TilesetError> {
        let Some(archive) = self
            .pmtiles
            .archive_key(&tileset_id)
            .await
            .map_err(internal_tileset_error)?
        else {
            return Ok(None);
        };
        if let Some(info) = self.resource_cache.get(&archive) {
            debug!(
                tileset_id = %tileset_id,
                "tileset info cache hit"
            );
            return Ok(Some(info));
        }

        debug!(
            tileset_id = %tileset_id,
            "tileset info request"
        );

        let Some(info) = self.read_tileset_info(&tileset_id).await? else {
            return Ok(None);
        };
        let archive = ArchiveKey::new(&tileset_id, info.generation.clone());
        let info = Arc::new(info);
        self.resource_cache.put(&archive, info.clone());
        Ok(Some(info))
    }

    /// Reports whether an archive exists without loading its metadata.
    ///
    /// Presence only needs the archive header, whose read is single-flighted and
    /// whose absence is cached by the reader. Unlike [`Self::load_tileset_info`],
    /// this skips the follow-up metadata fetch, so a cold detail-archive presence
    /// probe costs one object-store lookup instead of two.
    pub async fn archive_presence(
        &self,
        tileset_id: TilesetId,
    ) -> Result<ArchivePresence, TilesetError> {
        let header = self
            .pmtiles
            .header(&tileset_id)
            .await
            .map_err(internal_tileset_error)?;
        Ok(match header {
            Some(_) => ArchivePresence::Present,
            None => ArchivePresence::Absent,
        })
    }

    /// Loads local raw bootstrap bytes for internal forwarding, optionally including metadata.
    pub async fn load_bootstrap_bytes(
        &self,
        tileset_id: TilesetId,
        include_metadata: bool,
    ) -> Result<Option<BootstrapTransfer>, TilesetError> {
        self.pmtiles
            .load_bootstrap_bytes_local(&tileset_id, include_metadata)
            .await
            .map_err(internal_tileset_error)
    }

    /// Loads local raw PMTiles leaf bytes for internal forwarding.
    pub async fn load_leaf_bytes(
        &self,
        tileset_id: TilesetId,
        offset: u64,
        length: usize,
    ) -> Result<Option<ArchiveResource<Bytes>>, LeafBytesError> {
        match self
            .pmtiles
            .load_leaf_bytes_local(&tileset_id, offset, length)
            .await
        {
            Ok(leaf) => Ok(leaf),
            Err(LocalLeafError::InvalidRange) => Err(LeafBytesError::InvalidRange),
            Err(LocalLeafError::Reader(error)) => {
                Err(LeafBytesError::Tileset(internal_tileset_error(error)))
            }
        }
    }

    /// Executes the internal peer protocol without HTTP for the simulator.
    #[cfg(feature = "simulator-support")]
    #[doc(hidden)]
    pub async fn fetch_internal_for_simulator(
        &self,
        path: &str,
    ) -> Result<InternalFetchResponse, PeerFetchError> {
        if let Some(rest) = path.strip_prefix("/_internal/tiles/") {
            let (tileset, tile_id) = rest.rsplit_once('/').ok_or_else(|| {
                PeerFetchError::Fatal(format!("invalid internal tile path {path}"))
            })?;
            let tileset_id = decode_internal_tileset(tileset)?;
            let tile_id = tile_id.parse::<u64>().map_err(|error| {
                PeerFetchError::Fatal(format!("invalid internal tile id: {error}"))
            })?;
            let (tile, source) = self
                .load_tile_by_id_with_source(tileset_id, tile_id)
                .await
                .map_err(simulator_fetch_error)?;
            let source = match source {
                TileSource::SelfTileCache | TileSource::SelfChunkCache => InternalTileSource::Cache,
                TileSource::SelfBackend => InternalTileSource::Backend,
                _ => return Err(PeerFetchError::NotFound),
            };
            return tile
                .map(|tile| {
                    let mut response = InternalFetchResponse::tile(tile.data.bytes, source);
                    response.archive_generation = Some(tile.generation);
                    response
                })
                .ok_or(PeerFetchError::NotFound);
        }

        let Some(rest) = path.strip_prefix("/_internal/pmtiles/") else {
            return Err(PeerFetchError::Fatal(format!(
                "unsupported simulator internal path {path}"
            )));
        };
        let (path_only, query) = rest
            .split_once('?')
            .map_or((rest, None), |(path, query)| (path, Some(query)));
        let (tileset, operation) = path_only.split_once('/').ok_or_else(|| {
            PeerFetchError::Fatal(format!("invalid internal PMTiles path {path}"))
        })?;
        let tileset_id = decode_internal_tileset(tileset)?;

        if operation == "bootstrap" {
            let include_metadata = query == Some("metadata=true");
            let transfer = self
                .load_bootstrap_bytes(tileset_id, include_metadata)
                .await
                .map_err(simulator_fetch_error)?
                .ok_or(PeerFetchError::NotFound)?;
            if let Some(metadata) = transfer.metadata {
                let mut body =
                    BytesMut::with_capacity(8 + transfer.bootstrap.len() + metadata.len());
                body.put_u64_le(transfer.bootstrap.len() as u64);
                body.extend_from_slice(&transfer.bootstrap);
                body.extend_from_slice(&metadata);
                let mut response = InternalFetchResponse::bytes(body.freeze());
                response.archive_generation = Some(transfer.generation);
                return Ok(response);
            }
            let mut response = InternalFetchResponse::bytes(transfer.bootstrap);
            response.archive_generation = Some(transfer.generation);
            return Ok(response);
        }

        if let Some(arguments) = operation.strip_prefix("leaf/") {
            let (offset, length) = arguments.split_once('/').ok_or_else(|| {
                PeerFetchError::Fatal(format!("invalid internal leaf path {path}"))
            })?;
            let offset = offset.parse::<u64>().map_err(|error| {
                PeerFetchError::Fatal(format!("invalid internal leaf offset: {error}"))
            })?;
            let length = length.parse::<usize>().map_err(|error| {
                PeerFetchError::Fatal(format!("invalid internal leaf length: {error}"))
            })?;
            return self
                .load_leaf_bytes(tileset_id, offset, length)
                .await
                .map_err(|error| match error {
                    LeafBytesError::InvalidRange => {
                        PeerFetchError::Fatal("invalid leaf range".to_string())
                    }
                    LeafBytesError::Tileset(error) => simulator_fetch_error(error),
                })?
                .map(|leaf| {
                    let mut response = InternalFetchResponse::bytes(leaf.value);
                    response.archive_generation = Some(leaf.generation);
                    response
                })
                .ok_or(PeerFetchError::NotFound);
        }

        Err(PeerFetchError::Fatal(format!(
            "unsupported simulator PMTiles path {path}"
        )))
    }

    /// Loads the common header and metadata inputs shared by tileset HTTP endpoints.
    async fn read_tileset_info(
        &self,
        tileset_id: &TilesetId,
    ) -> Result<Option<TilesetInfo>, TilesetError> {
        let info = self
            .pmtiles
            .info(tileset_id)
            .await
            .map_err(internal_tileset_error)?;
        let Some(info) = info else {
            return Ok(None);
        };
        let (header, metadata) = info.value;
        Ok(Some(TilesetInfo {
            header,
            metadata,
            generation: info.generation,
        }))
    }

    /// Fetches a tile from the local PMTiles-backed storage path.
    async fn load_local_tile(
        &self,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<(Option<ResolvedTile>, PmtilesReadSource), TilesetError> {
        let (tile, source) = self
            .pmtiles
            .storage()
            .observe_reads(self.pmtiles.get_tile(tileset_id, tile_id))
            .await;
        let tile = tile.map_err(internal_tileset_error)?;

        let Some(resource) = tile else {
            return Ok((None, source));
        };
        let archive = ArchiveKey::new(tileset_id, resource.generation.clone());
        let Some(tile) = resource.value else {
            self.cache_tile_miss(&archive, tile_id);
            return Ok((None, source));
        };
        self.cache_tile_hit(&archive, tile_id, &tile);
        Ok((
            Some(ResolvedTile {
                data: tile,
                generation: resource.generation,
            }),
            source,
        ))
    }

    /// Forwards a tile request to the selected peer over the internal HTTP API.
    async fn load_tile_from_peer(
        &self,
        peer: &Peer,
        archive: &ArchiveKey,
        tile_id: u64,
    ) -> Result<Option<(ResolvedTile, InternalTileSource)>, TilesetError> {
        let response = self
            .peer_backend
            .fetch_tile_bytes(peer, &archive.tileset_id, tile_id)
            .await
            .map_err(|error| match error {
                PeerFetchError::NotFound => TilesetError::Miss,
                PeerFetchError::Retryable(message) => TilesetError::retryable_upstream(message),
                PeerFetchError::ProviderNotFound => {
                    TilesetError::Upstream("provider resource not found".to_string())
                }
                PeerFetchError::ProviderGone => {
                    TilesetError::Upstream("provider resource gone".to_string())
                }
                PeerFetchError::Fatal(message) => TilesetError::Upstream(message),
            })?;

        if response.archive_generation.as_ref() != Some(&archive.generation) {
            return Ok(None);
        }
        let header = self
            .pmtiles
            .header(&archive.tileset_id)
            .await
            .map_err(internal_tileset_error)?;
        let Some(header) = header.filter(|header| header.generation == archive.generation) else {
            return Ok(None);
        };
        let tile = TileData {
            bytes: response.bytes,
            content_type: header.value.tile_type.content_type(),
            content_encoding: header.value.tile_compression.content_encoding(),
        };
        if self.peer_tile_cache_policy == PeerTileCachePolicy::EntryAndOwner {
            self.cache_tile_hit(archive, tile_id, &tile);
        }
        Ok(Some((
            ResolvedTile {
                data: tile,
                generation: archive.generation.clone(),
            },
            response.tile_source.unwrap_or(InternalTileSource::Backend),
        )))
    }

    /// Returns a tile from the local L1 tile cache when present.
    fn load_cached_tile(&self, archive: &ArchiveKey, tile_id: u64) -> CachedTileLookup {
        let Some(entry) = self.tile_cache.get(&TileCacheKey::new(archive, tile_id)) else {
            return CachedTileLookup::None;
        };
        tracing::debug!(
            tileset_id = %archive.tileset_id,
            tile_id = tile_id,
            "tile cache hit"
        );
        match entry {
            CachedTile::Found {
                bytes,
                content_type,
                content_encoding,
            } => CachedTileLookup::Found(ResolvedTile {
                data: TileData {
                    bytes,
                    content_type,
                    content_encoding,
                },
                generation: archive.generation.clone(),
            }),
            CachedTile::NotFound => CachedTileLookup::NotFound,
        }
    }

    /// Stores a positive tile cache entry in the local L1 tile cache.
    fn cache_tile_hit(&self, archive: &ArchiveKey, tile_id: u64, tile: &TileData) {
        self.tile_cache.put(
            TileCacheKey::new(archive, tile_id),
            CachedTile::Found {
                bytes: tile.bytes.clone(),
                content_type: tile.content_type,
                content_encoding: tile.content_encoding,
            },
        );
    }

    /// Stores a negative tile cache entry in the local L1 tile cache.
    fn cache_tile_miss(&self, archive: &ArchiveKey, tile_id: u64) {
        self.tile_cache
            .put(TileCacheKey::new(archive, tile_id), CachedTile::NotFound);
    }
}

/// Errors returned while serving validated local leaf bytes.
#[derive(Debug, Error)]
pub enum LeafBytesError {
    #[error("invalid leaf range")]
    InvalidRange,
    #[error(transparent)]
    Tileset(#[from] TilesetError),
}

/// Errors returned by the tileset service before HTTP status mapping.
#[derive(Debug, Error)]
pub enum TilesetError {
    #[error("{0}")]
    Upstream(String),
    #[error("{0}")]
    RetryableUpstream(String),
    #[error("{0}")]
    Timeout(String),
    #[error("{0}")]
    Overloaded(String),
    #[error("forward miss")]
    Miss,
    /// The archive was replaced while this lookup was reading it, and the single
    /// internal restart also observed a change. Retrying is the correct client
    /// response: the next attempt admits the new generation. A larger internal
    /// retry count cannot fix sustained publication churn, so this is surfaced
    /// instead of absorbed.
    #[error("{0}")]
    Superseded(String),
    #[error("{0}")]
    Internal(String),
}

impl TilesetError {
    /// Wraps an upstream error that should trigger peer fallback.
    fn retryable_upstream(message: String) -> Self {
        Self::RetryableUpstream(message)
    }

    /// Whether a peer failure should fall back to local storage rather than
    /// failing the request.
    ///
    /// `Superseded` belongs here: the archive changed under *that* peer's read, so
    /// this node may well resolve the tile itself, and the same condition returned
    /// to a client is a retryable `503`. Treating it as fatal would abort a request
    /// that local storage could have served.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::RetryableUpstream(_) | Self::Overloaded(_) | Self::Superseded(_)
        )
    }
}

fn format_error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

#[cfg(test)]
mod tile_source_tests {
    use super::TileSource;

    #[test]
    fn aggregate_report_projection_is_bounded_and_stable() {
        let cases = [
            (TileSource::SelfTileCache, "self_cache", true),
            (TileSource::SelfChunkCache, "self_cache", false),
            (TileSource::SelfBackend, "self_backend", false),
            (TileSource::NegativeCache, "miss", false),
            (TileSource::PeerCache, "peer_cache", false),
            (TileSource::PeerBackend, "peer_backend", false),
            (TileSource::SelfMiss, "miss", false),
            (TileSource::PeerMiss, "miss", false),
        ];

        for (source, label, is_l1_hit) in cases {
            assert_eq!(source.report_label(), label);
            assert_eq!(source.is_l1_hit(), is_l1_hit);
        }
    }
}

fn internal_tileset_error(error: anyhow::Error) -> TilesetError {
    // Classify by the typed error in the chain, not by matching the message:
    // backend, deadline, and admission failures retain distinct storage variants.
    let storage_error = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<StorageError>());
    let message = format_error_chain(&error);
    match storage_error {
        Some(StorageError::Timeout(_)) => TilesetError::Timeout(message),
        Some(StorageError::Overloaded(_)) => TilesetError::Overloaded(message),
        Some(StorageError::Backend(_)) => TilesetError::retryable_upstream(message),
        Some(StorageError::GenerationChanged) => TilesetError::Superseded(message),
        _ => TilesetError::Internal(message),
    }
}

#[cfg(test)]
mod error_classification_tests {
    use super::*;

    #[test]
    fn backend_storage_failure_is_retryable_upstream() {
        let error = anyhow::Error::new(StorageError::Backend(
            "object-store service unavailable".to_string(),
        ))
        .context("failed to read PMTiles header");

        assert!(matches!(
            internal_tileset_error(error),
            TilesetError::RetryableUpstream(message)
                if message.contains("object-store service unavailable")
        ));
    }

    /// `is_retryable` gates peer-failure fallback to local storage. It is a
    /// `matches!`, so a new variant is silently non-retryable until listed —
    /// which is how `Superseded` was first missed. This pins every variant's
    /// answer so the next addition has to make a deliberate choice.
    #[test]
    fn every_error_states_whether_a_peer_failure_falls_back_locally() {
        let message = || "m".to_string();
        for (error, retryable) in [
            (TilesetError::RetryableUpstream(message()), true),
            (TilesetError::Overloaded(message()), true),
            // The archive changed under that peer's read; this node may still
            // resolve the tile, and clients see a retryable 503.
            (TilesetError::Superseded(message()), true),
            (TilesetError::Upstream(message()), false),
            (TilesetError::Timeout(message()), false),
            (TilesetError::Internal(message()), false),
            (TilesetError::Miss, false),
        ] {
            assert_eq!(
                error.is_retryable(),
                retryable,
                "{error:?} must {}fall back to local storage",
                if retryable { "" } else { "not " }
            );
        }
    }

    /// A generation change that escapes the reader's single restart must classify
    /// as retryable, not as an internal fault: it is an ordinary consequence of
    /// republishing, and `Internal` would surface it as `500`.
    #[test]
    fn generation_change_classifies_as_superseded_not_internal() {
        let error = anyhow::Error::new(StorageError::GenerationChanged)
            .context("failed to read PMTiles tile");

        assert!(matches!(
            internal_tileset_error(error),
            TilesetError::Superseded(message) if message.contains("failed to read PMTiles tile")
        ));
    }

    #[test]
    fn local_storage_message_remains_internal() {
        let error = anyhow::Error::new(StorageError::Message("invalid archive range".to_string()));

        assert!(matches!(
            internal_tileset_error(error),
            TilesetError::Internal(message) if message.contains("invalid archive range")
        ));
    }
}

#[cfg(feature = "simulator-support")]
fn decode_internal_tileset(encoded: &str) -> Result<TilesetId, PeerFetchError> {
    let decoded = encoded.replace("%2F", "/").replace("%2f", "/");
    if decoded.contains('%') {
        return Err(PeerFetchError::Fatal(
            "unsupported percent encoding in internal tileset path".to_string(),
        ));
    }
    TilesetId::try_new(&decoded)
        .map_err(|error| PeerFetchError::Fatal(format!("invalid internal tileset id: {error}")))
}

#[cfg(feature = "simulator-support")]
fn simulator_fetch_error(error: TilesetError) -> PeerFetchError {
    match error {
        TilesetError::RetryableUpstream(message)
        | TilesetError::Timeout(message)
        | TilesetError::Overloaded(message)
        | TilesetError::Superseded(message) => PeerFetchError::Retryable(message),
        TilesetError::Miss => PeerFetchError::NotFound,
        TilesetError::Upstream(message) | TilesetError::Internal(message) => {
            PeerFetchError::Fatal(message)
        }
    }
}

#[cfg(all(test, feature = "simulator-support"))]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use super::*;
    use crate::metrics::NodeMetrics;
    use crate::storage::ObjectStoreRegistry;
    use crate::storage::peer::Peer;
    use crate::storage::peer::{
        FetchFuture, InternalTransport, PeerBackend, PeerDirectory, PeerFetchError, PeerFuture,
    };
    use crate::storage::routing::HrwRouter;

    /// Peer directory returning a fixed peer set (none of them the local node).
    struct StaticDirectory {
        peers: Vec<Peer>,
    }

    impl PeerDirectory for StaticDirectory {
        fn peers(&self) -> PeerFuture<'_> {
            Box::pin(std::future::ready(self.peers.clone().into()))
        }
    }

    /// Transport that counts tile fetches and always reports the tile absent.
    #[derive(Default)]
    struct NotFoundTransport {
        calls: AtomicUsize,
    }

    impl InternalTransport for NotFoundTransport {
        fn fetch<'a>(&'a self, _peer: &'a Peer, _path: &'a str) -> FetchFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(Err(PeerFetchError::NotFound)))
        }
    }

    fn peer(id: &str, port: u16) -> Peer {
        Peer {
            id: id.to_string(),
            addr: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    fn resolver_with_transport(transport: Arc<NotFoundTransport>) -> ResourceResolver {
        // Three peer owners, none of which is the local node, so every candidate
        // forces the peer-forwarding path rather than a local fallback.
        let peers = vec![
            peer("node-a", 8001),
            peer("node-b", 8002),
            peer("node-c", 8003),
        ];
        let metrics = NodeMetrics::new();
        let peer_backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticDirectory { peers }),
            HrwRouter::new(3, 512),
            transport,
            metrics.clone(),
        );
        // The local path is never read (the peer 404 short-circuits before any
        // local resolve), but it is resolved eagerly at construction, so point
        // it at a directory that exists.
        let tileset_sources = std::env::temp_dir().to_string_lossy().into_owned();
        let tuning = crate::storage::ResolverTuningInput {
            candidate_count: 3,
            tile_group_size: 512,
            chunk_size_bytes: 1024 * 1024,
            max_fetch_chunks: 4,
            chunk_fetch_merge_window: Duration::from_millis(10),
            backend_fetch_concurrency: 32,
            backend_fetch_max_inflight: 128,
            tile_cache_max_bytes: 1024 * 1024,
            chunk_cache_max_bytes: 1024 * 1024,
            tile_negative_ttl: Duration::from_secs(60),
            archive_revalidation_interval: Duration::from_secs(300),
        }
        .resolve()
        .expect("valid resolver tuning");
        ResourceResolver::with_peer_backend(
            ResourceResolverStorageConfig {
                tileset_sources,
                tuning,
                cache_capacities: ResourceCacheCapacities::default(),
                backend_latency: BackendLatencyModel::fixed(0),
                peer_tile_cache_policy: PeerTileCachePolicy::EntryAndOwner,
                object_store_registry: Arc::new(ObjectStoreRegistry::without_options()),
                metrics,
            },
            peer_backend,
        )
        .expect("build resolver")
    }

    #[tokio::test]
    async fn peer_miss_falls_back_to_the_shared_backend_generation() {
        let transport = Arc::new(NotFoundTransport::default());
        let resolver = resolver_with_transport(transport.clone());
        let tileset = TilesetId::try_new("demo/streets").unwrap();

        // A peer 404 may describe an older generation, so it is not authoritative
        // for a mutable logical archive. Fall back to shared object storage.
        let (tile, source) = resolver
            .route_tile(tileset.clone(), 700)
            .await
            .expect("route tile");
        assert!(tile.is_none());
        assert_eq!(source, TileSource::SelfMiss);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);

        // The short archive-absence cache avoids another peer/backend probe.
        let (tile, source) = resolver
            .route_tile(tileset, 700)
            .await
            .expect("route tile again");
        assert!(tile.is_none());
        assert_eq!(source, TileSource::SelfMiss);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn archive_presence_reports_absent_without_error_for_missing_archive() {
        let transport = Arc::new(NotFoundTransport::default());
        let resolver = resolver_with_transport(transport);
        let tileset = TilesetId::try_new("demo/missing").unwrap();

        // A missing archive resolves to `Absent` (not an error), matching the
        // header-`None` path that `load_tileset_info` also treats as absence.
        assert_eq!(
            resolver.archive_presence(tileset.clone()).await.unwrap(),
            ArchivePresence::Absent
        );
        assert!(resolver.load_tileset_info(tileset).await.unwrap().is_none());
    }
}
