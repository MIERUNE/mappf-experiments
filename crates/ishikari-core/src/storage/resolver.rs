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
        BootstrapTransfer, DEFAULT_ARCHIVE_CACHE_MAX_BYTES, DEFAULT_LEAF_CACHE_MAX_BYTES, Header,
        LocalLeafError, Metadata, Reader as PmtilesReader, StorageError, TileData,
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
    Found(TileData),
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
    ) -> Result<(Option<TileData>, TileSource), TilesetError> {
        debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "tile request"
        );

        match self.load_cached_tile(&tileset_id, tile_id) {
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
                .load_tile_from_peer(&peer.peer, &tileset_id, tile_id)
                .await
            {
                Ok(Some((tile, peer_source))) => {
                    let source = match peer_source {
                        InternalTileSource::Cache => TileSource::PeerCache,
                        InternalTileSource::Backend => TileSource::PeerBackend,
                    };
                    return Ok((Some(tile), source));
                }
                // A reachable owner resolves the tile against the same shared
                // object storage as every other node, so its "not found" is
                // authoritative: no other candidate and no local re-resolution
                // can find it. Record a negative L1 entry and stop, rather than
                // forwarding the same 404 to the remaining candidates and then
                // resolving it locally — which turned every absent tile (common
                // in sparse archives) into up to `candidate_count` peer
                // round-trips plus a full local backend resolve. (Staleness of
                // that negative entry is bounded by cache eviction today; a TTL
                // on negative entries is the separate, correct fix.)
                Err(TilesetError::Miss) => {
                    debug!(
                        peer_id = %peer.peer.id,
                        tileset_id = %tileset_id,
                        tile_id = tile_id,
                        "peer reported tile absent; negative-caching and stopping"
                    );
                    self.cache_tile_miss(&tileset_id, tile_id);
                    return Ok((None, TileSource::PeerMiss));
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
    ) -> Result<(Option<TileData>, TileSource), TilesetError> {
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
    ) -> Result<(Option<TileData>, TileSource), TilesetError> {
        debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "internal tile request"
        );

        match self.load_cached_tile(&tileset_id, tile_id) {
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
        if let Some(info) = self.resource_cache.get(&tileset_id) {
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

        let Some((header, metadata)) = self.read_tileset_info(&tileset_id).await? else {
            return Ok(None);
        };
        let info = Arc::new(TilesetInfo { header, metadata });
        self.resource_cache.put(&tileset_id, info.clone());
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
        // A cached full tileset info means the archive is definitely present.
        if self.resource_cache.get(&tileset_id).is_some() {
            return Ok(ArchivePresence::Present);
        }
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
    ) -> Result<Option<Bytes>, LeafBytesError> {
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
                .map(|tile| InternalFetchResponse::tile(tile.bytes, source))
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
                return Ok(InternalFetchResponse::bytes(body.freeze()));
            }
            return Ok(InternalFetchResponse::bytes(transfer.bootstrap));
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
                .map(InternalFetchResponse::bytes)
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
    ) -> Result<Option<(Header, Arc<Metadata>)>, TilesetError> {
        let header = self
            .pmtiles
            .header(tileset_id)
            .await
            .map_err(internal_tileset_error)?;
        let Some(header) = header else {
            return Ok(None);
        };

        let metadata = self
            .pmtiles
            .metadata(tileset_id)
            .await
            .map_err(internal_tileset_error)?;
        let Some(metadata) = metadata else {
            return Ok(None);
        };

        Ok(Some((header, metadata)))
    }

    /// Fetches a tile from the local PMTiles-backed storage path.
    async fn load_local_tile(
        &self,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<(Option<TileData>, PmtilesReadSource), TilesetError> {
        let (tile, source) = self
            .pmtiles
            .storage()
            .observe_reads(self.pmtiles.get_tile(tileset_id, tile_id))
            .await;
        let tile = tile.map_err(internal_tileset_error)?;

        let Some(tile) = tile else {
            self.cache_tile_miss(tileset_id, tile_id);
            return Ok((None, source));
        };

        self.cache_tile_hit(tileset_id, tile_id, &tile);
        Ok((Some(tile), source))
    }

    /// Forwards a tile request to the selected peer over the internal HTTP API.
    async fn load_tile_from_peer(
        &self,
        peer: &Peer,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Option<(TileData, InternalTileSource)>, TilesetError> {
        let response = self
            .peer_backend
            .fetch_tile_bytes(peer, tileset_id, tile_id)
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

        let header = self
            .pmtiles
            .header(tileset_id)
            .await
            .map_err(internal_tileset_error)?;
        let Some(header) = header else {
            return Ok(None);
        };
        let tile = TileData {
            bytes: response.bytes,
            content_type: header.tile_type.content_type(),
            content_encoding: header.tile_compression.content_encoding(),
        };
        if self.peer_tile_cache_policy == PeerTileCachePolicy::EntryAndOwner {
            self.cache_tile_hit(tileset_id, tile_id, &tile);
        }
        Ok(Some((
            tile,
            response.tile_source.unwrap_or(InternalTileSource::Backend),
        )))
    }

    /// Returns a tile from the local L1 tile cache when present.
    fn load_cached_tile(&self, tileset_id: &TilesetId, tile_id: u64) -> CachedTileLookup {
        let Some(entry) = self.tile_cache.get(&TileCacheKey::new(tileset_id, tile_id)) else {
            return CachedTileLookup::None;
        };
        tracing::debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "tile cache hit"
        );
        match entry {
            CachedTile::Found {
                bytes,
                content_type,
                content_encoding,
            } => CachedTileLookup::Found(TileData {
                bytes,
                content_type,
                content_encoding,
            }),
            CachedTile::NotFound => CachedTileLookup::NotFound,
        }
    }

    /// Stores a positive tile cache entry in the local L1 tile cache.
    fn cache_tile_hit(&self, tileset_id: &TilesetId, tile_id: u64, tile: &TileData) {
        self.tile_cache.put(
            TileCacheKey::new(tileset_id, tile_id),
            CachedTile::Found {
                bytes: tile.bytes.clone(),
                content_type: tile.content_type,
                content_encoding: tile.content_encoding,
            },
        );
    }

    /// Stores a negative tile cache entry in the local L1 tile cache.
    fn cache_tile_miss(&self, tileset_id: &TilesetId, tile_id: u64) {
        self.tile_cache
            .put(TileCacheKey::new(tileset_id, tile_id), CachedTile::NotFound);
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
    #[error("{0}")]
    Internal(String),
}

impl TilesetError {
    /// Wraps an upstream error that should trigger peer fallback.
    fn retryable_upstream(message: String) -> Self {
        Self::RetryableUpstream(message)
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::RetryableUpstream(_) | Self::Overloaded(_))
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
        | TilesetError::Overloaded(message) => PeerFetchError::Retryable(message),
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
    async fn authoritative_peer_miss_stops_after_one_forward_and_negative_caches() {
        let transport = Arc::new(NotFoundTransport::default());
        let resolver = resolver_with_transport(transport.clone());
        let tileset = TilesetId::try_new("demo/streets").unwrap();

        // A reachable owner's 404 is authoritative: exactly one forward, no
        // walk over the remaining candidates and no local re-resolution.
        let (tile, source) = resolver
            .route_tile(tileset.clone(), 700)
            .await
            .expect("route tile");
        assert!(tile.is_none());
        assert_eq!(source, TileSource::PeerMiss);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);

        // The negative entry is now cached locally, so a repeat is a negative
        // L1 hit with no further peer forwarding.
        let (tile, source) = resolver
            .route_tile(tileset, 700)
            .await
            .expect("route tile again");
        assert!(tile.is_none());
        assert_eq!(source, TileSource::NegativeCache);
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
