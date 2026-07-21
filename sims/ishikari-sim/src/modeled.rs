use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::File,
    future::Future,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use ishikari_core::{
    cache_policy::{
        chunk_cache_entry_weight, effective_chunk_cache_capacity, tile_cache_entry_weight,
    },
    metrics::NodeMetricsSnapshot,
    pmtiles::{
        BootstrapTransfer, Reader as PmtilesReader, Storage as PmtilesStorage, StorageError,
    },
    storage::{HrwRouter, Peer, ResolverTuning, TileSource, TilesetId, plan_chunk_fetch_ranges},
};
use mmpf_pmtiles::{TileCoord, TileId, TileLookupTrace as TileAccessPlan};
use moka::{policy::EvictionPolicy, sync::Cache};

use crate::{
    TraceEntry,
    config::ClusterConfig,
    report::{ClusterObservation, NodeReport, SchedulerReport, SimReport, SourceCounts},
    topology::{simulated_peer, simulated_peers},
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TileKey {
    tileset_id: TilesetId,
    tile_id: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ChunkKey {
    tileset_id: TilesetId,
    chunk_index: u64,
}

#[derive(Clone, Copy)]
enum ModeledTile {
    Found { length: u32, weight: u32 },
    NotFound { weight: u32 },
}

impl ModeledTile {
    fn weight(self) -> u32 {
        match self {
            Self::Found { weight, .. } | Self::NotFound { weight } => weight,
        }
    }

    fn length(self) -> Option<u32> {
        match self {
            Self::Found { length, .. } => Some(length),
            Self::NotFound { .. } => None,
        }
    }
}

#[derive(Clone, Copy)]
struct ModeledChunk {
    weight: u32,
}

/// PMTiles access plans for every unique tile in a trace.
pub struct TileCatalog {
    entries: HashMap<TileKey, Option<TileAccessPlan>>,
}

impl TileCatalog {
    /// Builds a catalog from a local PMTiles root without reading tile payloads.
    pub async fn build(tileset_source: &str, trace: &[TraceEntry]) -> Result<Self> {
        let storage = LocalCatalogStorage::new(tileset_source)?;
        let reader = Arc::new(PmtilesReader::new(storage)?);
        let mut keys = BTreeSet::new();
        for entry in trace {
            keys.insert(tile_key(entry)?);
        }

        let mut entries = HashMap::with_capacity(keys.len());
        for key in keys {
            let plan = reader
                .plan_tile_access(&key.tileset_id, key.tile_id)
                .await
                .with_context(|| {
                    format!("resolve modeled tile {} id={}", key.tileset_id, key.tile_id)
                })?;
            entries.insert(key, plan);
        }
        Ok(Self { entries })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn get(&self, key: &TileKey) -> Option<&TileAccessPlan> {
        self.entries.get(key).and_then(Option::as_ref)
    }

    fn contains(&self, key: &TileKey) -> bool {
        self.entries.contains_key(key)
    }
}

#[derive(Clone)]
struct LocalCatalogStorage {
    root: Arc<PathBuf>,
}

impl LocalCatalogStorage {
    fn new(source: &str) -> Result<Self> {
        ensure!(!source.is_empty(), "modeled tileset source is empty");
        if source.contains(';') || source.contains('=') || source.contains("://") {
            bail!("modeled cache currently requires one local tileset root, got {source}");
        }
        Ok(Self {
            root: Arc::new(PathBuf::from(source)),
        })
    }

    fn archive_path(&self, tileset_id: &TilesetId) -> PathBuf {
        self.root.join(format!("{tileset_id}.pmtiles"))
    }
}

impl PmtilesStorage for LocalCatalogStorage {
    async fn read_range(
        &self,
        tileset_id: &TilesetId,
        start: u64,
        length: usize,
        _archive_len: Option<u64>,
    ) -> Result<Bytes, StorageError> {
        let path = self.archive_path(tileset_id);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(error) => {
                return Err(StorageError::Message(format!(
                    "open {}: {error}",
                    path.display()
                )));
            }
        };
        file.seek(SeekFrom::Start(start)).map_err(|error| {
            StorageError::Message(format!("seek {} to {start}: {error}", path.display()))
        })?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes).map_err(|error| {
            StorageError::Message(format!(
                "read {} range {start}..{}: {error}",
                path.display(),
                start.saturating_add(length as u64)
            ))
        })?;
        Ok(Bytes::from(bytes))
    }

    fn fetch_bootstrap_bytes<'a>(
        &'a self,
        _tileset_id: &'a TilesetId,
        _include_metadata: bool,
    ) -> impl Future<Output = Result<Option<BootstrapTransfer>>> + Send + 'a {
        std::future::ready(Ok(None))
    }

    fn fetch_leaf_bytes<'a>(
        &'a self,
        _tileset_id: &'a TilesetId,
        _offset: u64,
        _length: usize,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send + 'a {
        std::future::ready(Ok(None))
    }
}

struct ModelNode {
    id: String,
    tile_cache: Cache<TileKey, ModeledTile>,
    chunk_cache: Cache<ChunkKey, ModeledChunk>,
    loaded_bootstraps: HashSet<TilesetId>,
    absent_archives: HashSet<TilesetId>,
    loaded_leaves: HashSet<(TilesetId, u64, u32)>,
    requests: u64,
    served_bytes: u64,
    by_source: SourceCounts,
    backend_bytes: u64,
    metrics: NodeMetricsSnapshot,
}

impl ModelNode {
    fn new(id: String, tuning: ResolverTuning) -> Self {
        Self {
            id,
            tile_cache: Cache::builder()
                .max_capacity(tuning.tile_cache_max_bytes())
                .weigher(|_key: &TileKey, value: &ModeledTile| value.weight())
                .build(),
            chunk_cache: Cache::builder()
                .eviction_policy(EvictionPolicy::lru())
                .max_capacity(effective_chunk_cache_capacity(
                    tuning.chunk_cache_max_bytes(),
                ))
                .weigher(|_key: &ChunkKey, value: &ModeledChunk| value.weight)
                .build(),
            loaded_bootstraps: HashSet::new(),
            absent_archives: HashSet::new(),
            loaded_leaves: HashSet::new(),
            requests: 0,
            served_bytes: 0,
            by_source: SourceCounts::default(),
            backend_bytes: 0,
            metrics: NodeMetricsSnapshot::default(),
        }
    }

    fn put_tile(&self, key: TileKey, length: Option<u32>) {
        let weight = tile_cache_entry_weight(length.map(|length| length as usize));
        let value = length.map_or(ModeledTile::NotFound { weight }, |length| {
            ModeledTile::Found { length, weight }
        });
        self.tile_cache.insert(key, value);
    }

    fn finish_maintenance(&self) {
        self.tile_cache.run_pending_tasks();
        self.chunk_cache.run_pending_tasks();
    }
}

struct PendingTile {
    entry_node: usize,
    owner_node: usize,
    key: TileKey,
    plan: TileAccessPlan,
    backend_waited: bool,
}

struct BootstrapRequest {
    requester_node: usize,
    tileset_id: TilesetId,
    length: u32,
    archive_len: u64,
    request_indices: Vec<usize>,
}

struct LeafRequest {
    requester_node: usize,
    tileset_id: TilesetId,
    bootstrap_length: u32,
    offset: u64,
    length: u32,
    archive_len: u64,
    request_indices: Vec<usize>,
}

#[derive(Clone, Copy)]
enum ModeledPeerResource {
    Tile,
    Bootstrap,
    Leaf,
}

#[derive(Clone)]
struct PlannedRange {
    node: usize,
    tileset_id: TilesetId,
    offset: u64,
    length: u32,
    archive_len: u64,
    request_indices: Vec<usize>,
}

/// Metadata-only cluster model with production HRW and Moka eviction policies.
pub struct ModeledCluster {
    config: ClusterConfig,
    resolver_tuning: ResolverTuning,
    catalog: Arc<TileCatalog>,
    peers: Vec<Peer>,
    router: HrwRouter,
    nodes: Vec<ModelNode>,
    retired_nodes: Vec<NodeReport>,
    next_node_index: usize,
    report: SimReport,
    by_source: SourceCounts,
}

impl ModeledCluster {
    pub fn new(config: ClusterConfig, catalog: impl Into<Arc<TileCatalog>>) -> Result<Self> {
        let resolver_tuning = config.validate()?;
        let next_node_index = config.node_count;
        let peers = simulated_peers(config.node_count);
        let nodes = peers
            .iter()
            .map(|peer| ModelNode::new(peer.id.clone(), resolver_tuning))
            .collect();
        Ok(Self {
            router: HrwRouter::new(
                resolver_tuning.candidate_count(),
                resolver_tuning.tile_group_size(),
            ),
            config,
            resolver_tuning,
            catalog: catalog.into(),
            peers,
            nodes,
            retired_nodes: Vec::new(),
            next_node_index,
            report: SimReport::default(),
            by_source: SourceCounts::default(),
        })
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn active_node_ids(&self) -> Vec<String> {
        self.nodes.iter().map(|node| node.id.clone()).collect()
    }

    pub fn add_node(&mut self) -> Result<String> {
        let peer = simulated_peer(self.next_node_index)?;
        let id = peer.id.clone();
        self.nodes
            .push(ModelNode::new(id.clone(), self.resolver_tuning));
        self.peers.push(peer);
        self.next_node_index += 1;
        Ok(id)
    }

    pub fn remove_node(&mut self, id: &str) -> Result<()> {
        ensure!(self.nodes.len() > 1, "cannot remove the last active node");
        let index = self
            .nodes
            .iter()
            .position(|node| node.id == id)
            .with_context(|| format!("active node {id} does not exist"))?;
        let node = self.nodes.remove(index);
        self.peers.remove(index);
        self.retired_nodes.push(modeled_node_report(&node, false));
        Ok(())
    }

    pub fn serve(&mut self, entry: &TraceEntry) -> Result<()> {
        let entry_node = entry
            .entry_node
            .context("trace has no entry_node; generate it with node_count > 0")?;
        self.serve_on(entry, entry_node)
    }

    pub fn serve_on(&mut self, entry: &TraceEntry, entry_node: usize) -> Result<()> {
        self.serve_viewport_on(std::slice::from_ref(entry), &[entry_node])
    }

    pub fn serve_viewport(&mut self, entries: &[TraceEntry]) -> Result<()> {
        let entry_nodes = entries
            .iter()
            .map(|entry| {
                entry
                    .entry_node
                    .context("trace has no entry_node; generate it with node_count > 0")
            })
            .collect::<Result<Vec<_>>>()?;
        self.serve_viewport_on(entries, &entry_nodes)
    }

    pub fn serve_viewport_on(
        &mut self,
        entries: &[TraceEntry],
        entry_nodes: &[usize],
    ) -> Result<()> {
        ensure!(
            entries.len() == entry_nodes.len(),
            "entry node assignment length does not match viewport"
        );
        let mut pending = Vec::new();
        for (entry, &entry_node) in entries.iter().zip(entry_nodes) {
            self.prepare(entry, entry_node, &mut pending)?;
        }

        self.process_bootstraps(&mut pending);
        self.process_leaves(&mut pending);
        self.process_tile_ranges(&mut pending);

        for tile in pending {
            let Some(location) = tile.plan.tile else {
                self.nodes[tile.owner_node].put_tile(tile.key.clone(), None);
                if tile.entry_node != tile.owner_node {
                    self.record_peer_request(
                        tile.entry_node,
                        tile.owner_node,
                        ModeledPeerResource::Tile,
                        false,
                    );
                }
                self.nodes[tile.entry_node].put_tile(tile.key, None);
                let source = if tile.entry_node == tile.owner_node {
                    TileSource::SelfMiss
                } else {
                    TileSource::PeerMiss
                };
                self.record(tile.entry_node, source, None);
                continue;
            };
            let length = location.length;
            self.nodes[tile.owner_node].put_tile(tile.key.clone(), Some(length));
            if tile.entry_node != tile.owner_node {
                self.ensure_post_peer_header(tile.entry_node, &tile.key.tileset_id, &tile.plan);
                if self.config.cache_peer_tiles {
                    self.nodes[tile.entry_node].put_tile(tile.key, Some(length));
                }
                self.record_peer_request(
                    tile.entry_node,
                    tile.owner_node,
                    ModeledPeerResource::Tile,
                    true,
                );
                self.report.peer_bytes += u64::from(length);
            }
            let source = match (tile.entry_node == tile.owner_node, tile.backend_waited) {
                (true, false) => TileSource::SelfChunkCache,
                (true, true) => TileSource::SelfBackend,
                (false, false) => TileSource::PeerCache,
                (false, true) => TileSource::PeerBackend,
            };
            self.record(tile.entry_node, source, Some(u64::from(length)));
        }
        self.finish_cache_maintenance();
        Ok(())
    }

    fn finish_cache_maintenance(&self) {
        for node in &self.nodes {
            node.finish_maintenance();
        }
    }

    fn prepare(
        &mut self,
        entry: &TraceEntry,
        entry_node: usize,
        pending: &mut Vec<PendingTile>,
    ) -> Result<()> {
        ensure!(
            entry_node < self.nodes.len(),
            "entry_node {entry_node} is outside the modeled cluster"
        );
        self.nodes[entry_node].requests += 1;
        self.report.requests += 1;
        let key = tile_key(entry)?;

        if let Some(cached) = self.nodes[entry_node].tile_cache.get(&key) {
            let source = if cached.length().is_some() {
                TileSource::SelfTileCache
            } else {
                TileSource::NegativeCache
            };
            if source.is_l1_hit() {
                self.report.l1_cache_hits += 1;
            }
            self.record(entry_node, source, cached.length().map(u64::from));
            return Ok(());
        }

        let owner_node = self
            .router
            .route_tile(&self.peers, key.tileset_id.as_ref(), key.tile_id)
            .first()
            .and_then(|candidate| {
                self.nodes
                    .iter()
                    .position(|node| node.id == candidate.peer.id)
            })
            .context("HRW returned no modeled owner")?;

        if owner_node != entry_node
            && let Some(cached) = self.nodes[owner_node].tile_cache.get(&key)
        {
            if let Some(length) = cached.length() {
                self.record_peer_request(entry_node, owner_node, ModeledPeerResource::Tile, true);
                let plan = self.catalog.get(&key).cloned().with_context(|| {
                    format!(
                        "modeled catalog has no access plan for cached {} id={}",
                        key.tileset_id, key.tile_id
                    )
                })?;
                self.ensure_post_peer_header(entry_node, &key.tileset_id, &plan);
                if self.config.cache_peer_tiles {
                    self.nodes[entry_node].put_tile(key, Some(length));
                }
                self.report.peer_bytes += u64::from(length);
                self.record(entry_node, TileSource::PeerCache, Some(u64::from(length)));
            } else {
                self.record_peer_request(entry_node, owner_node, ModeledPeerResource::Tile, false);
                self.nodes[entry_node].put_tile(key, None);
                self.record(entry_node, TileSource::PeerMiss, None);
            }
            return Ok(());
        }

        if !self.catalog.contains(&key) {
            bail!(
                "modeled catalog is missing {} id={}",
                key.tileset_id,
                key.tile_id
            );
        }
        let Some(plan) = self.catalog.get(&key).cloned() else {
            self.process_absent_archive(owner_node, &key.tileset_id);
            self.nodes[owner_node].put_tile(key.clone(), None);
            if owner_node != entry_node {
                self.record_peer_request(entry_node, owner_node, ModeledPeerResource::Tile, false);
            }
            self.nodes[entry_node].put_tile(key, None);
            let source = if owner_node == entry_node {
                TileSource::SelfMiss
            } else {
                TileSource::PeerMiss
            };
            self.record(entry_node, source, None);
            return Ok(());
        };

        pending.push(PendingTile {
            entry_node,
            owner_node,
            key,
            plan,
            backend_waited: false,
        });
        Ok(())
    }

    /// Models the production bootstrap path for an archive that does not
    /// exist. A remote group-zero probe returns not-found, after which the
    /// requesting reader falls back to its own backend and negative-caches the
    /// archive locally. Each node therefore performs at most one backend probe
    /// during the modeled run, while non-owner readers still make one peer
    /// attempt before their local fallback.
    fn process_absent_archive(&mut self, requester_node: usize, tileset_id: &TilesetId) {
        if self.nodes[requester_node]
            .absent_archives
            .contains(tileset_id)
        {
            return;
        }

        let index_owner = self.index_owner(tileset_id);
        if index_owner != requester_node {
            self.record_peer_request(
                requester_node,
                index_owner,
                ModeledPeerResource::Bootstrap,
                false,
            );
            if !self.nodes[index_owner].absent_archives.contains(tileset_id) {
                self.record_absent_archive_backend_probe(index_owner);
                self.nodes[index_owner]
                    .absent_archives
                    .insert(tileset_id.clone());
            }
        }

        self.record_absent_archive_backend_probe(requester_node);
        self.nodes[requester_node]
            .absent_archives
            .insert(tileset_id.clone());
    }

    fn record_absent_archive_backend_probe(&mut self, node_index: usize) {
        let node = &mut self.nodes[node_index];
        node.metrics.backend_fetches += 1;
        node.metrics.backend_fetch_not_found += 1;
    }

    fn record_exact_bootstrap_backend_probe(&mut self, node_index: usize, bytes: u64) {
        let node = &mut self.nodes[node_index];
        node.backend_bytes += bytes;
        node.metrics.backend_fetches += 1;
        node.metrics.backend_fetch_successes += 1;
        node.metrics.backend_fetched_chunks += 1;
    }

    fn record_peer_request(
        &mut self,
        requester_node: usize,
        owner_node: usize,
        resource: ModeledPeerResource,
        success: bool,
    ) {
        self.report.peer_requests += 1;
        let requester = &mut self.nodes[requester_node].metrics;
        if success {
            requester.peer_forward_successes += 1;
        } else {
            requester.peer_forward_not_found += 1;
        }
        match resource {
            ModeledPeerResource::Tile => requester.peer_tile_fetches += 1,
            ModeledPeerResource::Bootstrap => requester.peer_bootstrap_fetches += 1,
            ModeledPeerResource::Leaf => requester.peer_leaf_fetches += 1,
        }

        let owner = &mut self.nodes[owner_node].metrics;
        match resource {
            ModeledPeerResource::Tile => owner.internal_tile_requests += 1,
            ModeledPeerResource::Bootstrap => owner.internal_bootstrap_requests += 1,
            ModeledPeerResource::Leaf => owner.internal_leaf_requests += 1,
        }
    }

    fn process_bootstraps(&mut self, pending: &mut [PendingTile]) {
        // Reader-level bootstrap single-flight collapses concurrent tile
        // lookups for one tileset on one requesting node into one request.
        let mut grouped = BTreeMap::<(usize, TilesetId), BootstrapRequest>::new();
        for (request_index, tile) in pending.iter().enumerate() {
            if self.nodes[tile.owner_node]
                .loaded_bootstraps
                .contains(&tile.key.tileset_id)
            {
                continue;
            }
            grouped
                .entry((tile.owner_node, tile.key.tileset_id.clone()))
                .and_modify(|request| request.request_indices.push(request_index))
                .or_insert_with(|| BootstrapRequest {
                    requester_node: tile.owner_node,
                    tileset_id: tile.key.tileset_id.clone(),
                    length: tile.plan.bootstrap.length,
                    archive_len: tile.plan.archive_len,
                    request_indices: vec![request_index],
                });
        }
        let requests = grouped.into_values().collect::<Vec<_>>();
        for request_index in self.process_bootstrap_requests(&requests) {
            pending[request_index].backend_waited = true;
        }
    }

    fn process_leaves(&mut self, pending: &mut [PendingTile]) {
        let max_depth = pending
            .iter()
            .map(|tile| tile.plan.leaves.len())
            .max()
            .unwrap_or(0);
        for depth in 0..max_depth {
            // Leaf single-flight has the same requester-local ownership as the
            // decoded bootstrap cache. Raw leaf bytes may come from the
            // independently routed group-zero owner.
            let mut grouped = BTreeMap::<(usize, TilesetId, u64, u32), LeafRequest>::new();
            for (request_index, tile) in pending.iter().enumerate() {
                let Some(leaf) = tile.plan.leaves.get(depth) else {
                    continue;
                };
                let loaded_key = (tile.key.tileset_id.clone(), leaf.offset, leaf.length);
                if self.nodes[tile.owner_node]
                    .loaded_leaves
                    .contains(&loaded_key)
                {
                    continue;
                }
                grouped
                    .entry((
                        tile.owner_node,
                        tile.key.tileset_id.clone(),
                        leaf.offset,
                        leaf.length,
                    ))
                    .and_modify(|request| request.request_indices.push(request_index))
                    .or_insert_with(|| LeafRequest {
                        requester_node: tile.owner_node,
                        tileset_id: tile.key.tileset_id.clone(),
                        bootstrap_length: tile.plan.bootstrap.length,
                        offset: leaf.offset,
                        length: leaf.length,
                        archive_len: tile.plan.archive_len,
                        request_indices: vec![request_index],
                    });
            }
            let requests = grouped.into_values().collect::<Vec<_>>();
            for request_index in self.process_leaf_requests(&requests) {
                pending[request_index].backend_waited = true;
            }
        }
    }

    /// Executes requester-local decoded-bootstrap loads through the production
    /// group-zero routing domain. Only a local fallback contributes to the
    /// requesting tile's backend source; work done by a remote index owner is
    /// still counted globally but does not reclassify the tile response.
    fn process_bootstrap_requests(&mut self, requests: &[BootstrapRequest]) -> BTreeSet<usize> {
        let mut owners = Vec::with_capacity(requests.len());
        let mut cold_index_probes = BTreeMap::new();
        let mut probe_waiters = BTreeSet::new();
        for request in requests {
            let index_owner = self.index_owner(&request.tileset_id);
            let local = index_owner == request.requester_node;
            if !self.nodes[index_owner]
                .loaded_bootstraps
                .contains(&request.tileset_id)
            {
                cold_index_probes
                    .entry((index_owner, request.tileset_id.clone()))
                    .and_modify(
                        |(_, request_indices, has_local_waiter): &mut (u64, Vec<usize>, bool)| {
                            if local {
                                request_indices.extend(request.request_indices.iter().copied());
                                *has_local_waiter = true;
                            }
                        },
                    )
                    .or_insert((
                        request.archive_len.min(u64::from(request.length)),
                        if local {
                            request.request_indices.clone()
                        } else {
                            Vec::new()
                        },
                        local,
                    ));
            }
            owners.push(index_owner);
        }

        // Before the archive length is known, production performs one exact
        // bootstrap probe and retains those immutable bytes with the decoded
        // archive. Bootstrap singleflight collapses concurrent probes on the
        // same index owner and tileset.
        for ((index_owner, _tileset_id), (bytes, request_indices, local)) in cold_index_probes {
            self.record_exact_bootstrap_backend_probe(index_owner, bytes);
            if local {
                probe_waiters.extend(request_indices);
            }
        }

        let backend_waiters = probe_waiters;
        for (request, index_owner) in requests.iter().zip(owners) {
            self.nodes[index_owner]
                .loaded_bootstraps
                .insert(request.tileset_id.clone());
            self.nodes[request.requester_node]
                .loaded_bootstraps
                .insert(request.tileset_id.clone());
            if index_owner != request.requester_node {
                self.record_peer_request(
                    request.requester_node,
                    index_owner,
                    ModeledPeerResource::Bootstrap,
                    true,
                );
                self.report.peer_bytes += request.archive_len.min(u64::from(request.length));
            }
        }
        backend_waiters
    }

    fn process_leaf_requests(&mut self, requests: &[LeafRequest]) -> BTreeSet<usize> {
        // A remote leaf handler first needs its own decoded bootstrap. This is
        // local work on the index owner and cannot affect the tile owner's
        // source label.
        let mut bootstrap_owners = BTreeMap::new();
        for request in requests {
            let index_owner = self.index_owner(&request.tileset_id);
            if index_owner != request.requester_node
                && !self.nodes[index_owner]
                    .loaded_bootstraps
                    .contains(&request.tileset_id)
            {
                bootstrap_owners
                    .entry((index_owner, request.tileset_id.clone()))
                    .or_insert(request.archive_len.min(u64::from(request.bootstrap_length)));
            }
        }
        for ((node, tileset_id), bytes) in bootstrap_owners {
            self.record_exact_bootstrap_backend_probe(node, bytes);
            self.nodes[node].loaded_bootstraps.insert(tileset_id);
        }

        let mut ranges = Vec::with_capacity(requests.len());
        let mut owners = Vec::with_capacity(requests.len());
        for request in requests {
            let index_owner = self.index_owner(&request.tileset_id);
            let local = index_owner == request.requester_node;
            ranges.push(PlannedRange {
                node: index_owner,
                tileset_id: request.tileset_id.clone(),
                offset: request.offset,
                length: request.length,
                archive_len: request.archive_len,
                request_indices: if local {
                    request.request_indices.clone()
                } else {
                    Vec::new()
                },
            });
            owners.push(index_owner);
        }

        let backend_waiters = self.process_ranges(&ranges);
        for (request, index_owner) in requests.iter().zip(owners) {
            self.nodes[request.requester_node].loaded_leaves.insert((
                request.tileset_id.clone(),
                request.offset,
                request.length,
            ));
            if index_owner != request.requester_node {
                self.record_peer_request(
                    request.requester_node,
                    index_owner,
                    ModeledPeerResource::Leaf,
                    true,
                );
                self.report.peer_bytes += u64::from(request.length);
            }
        }
        backend_waiters
    }

    fn ensure_post_peer_header(
        &mut self,
        requester_node: usize,
        tileset_id: &TilesetId,
        plan: &TileAccessPlan,
    ) {
        if self.nodes[requester_node]
            .loaded_bootstraps
            .contains(tileset_id)
        {
            return;
        }
        self.process_bootstrap_requests(&[BootstrapRequest {
            requester_node,
            tileset_id: tileset_id.clone(),
            length: plan.bootstrap.length,
            archive_len: plan.archive_len,
            request_indices: Vec::new(),
        }]);
    }

    fn index_owner(&self, tileset_id: &TilesetId) -> usize {
        self.router
            .route_tile(&self.peers, tileset_id.as_ref(), 0)
            .first()
            .and_then(|candidate| {
                self.nodes
                    .iter()
                    .position(|node| node.id == candidate.peer.id)
            })
            .expect("non-empty modeled cluster has a group-zero owner")
    }

    fn process_tile_ranges(&mut self, pending: &mut [PendingTile]) {
        let ranges: Vec<_> = pending
            .iter()
            .enumerate()
            .filter_map(|(request_index, tile)| {
                let location = tile.plan.tile?;
                Some(PlannedRange {
                    node: tile.owner_node,
                    tileset_id: tile.key.tileset_id.clone(),
                    offset: location.offset,
                    length: location.length,
                    archive_len: tile.plan.archive_len,
                    request_indices: vec![request_index],
                })
            })
            .collect();
        for request_index in self.process_ranges(&ranges) {
            pending[request_index].backend_waited = true;
        }
    }

    fn process_ranges(&mut self, ranges: &[PlannedRange]) -> BTreeSet<usize> {
        let mut backend_waiters = BTreeSet::new();
        let mut grouped: BTreeMap<(usize, TilesetId), Vec<&PlannedRange>> = BTreeMap::new();
        for range in ranges {
            grouped
                .entry((range.node, range.tileset_id.clone()))
                .or_default()
                .push(range);
        }

        for ((node_index, tileset_id), ranges) in grouped {
            let node = &mut self.nodes[node_index];
            let mut missing = BTreeSet::new();
            let mut waiters: HashMap<u64, u64> = HashMap::new();
            let archive_len = ranges
                .iter()
                .map(|range| range.archive_len)
                .max()
                .unwrap_or(0);

            for range in ranges {
                let mut range_missed = false;
                for chunk_index in chunks_for_range(
                    range.offset,
                    u64::from(range.length),
                    self.resolver_tuning.chunk_size_bytes(),
                ) {
                    let key = ChunkKey {
                        tileset_id: tileset_id.clone(),
                        chunk_index,
                    };
                    if node.chunk_cache.get(&key).is_some() {
                        node.metrics.chunk_cache_hits += 1;
                    } else {
                        range_missed = true;
                        node.metrics.chunk_cache_misses += 1;
                        *waiters.entry(chunk_index).or_default() += 1;
                        if missing.insert(chunk_index) {
                            node.metrics.chunk_fetch_queued += 1;
                        } else {
                            node.metrics.chunk_fetch_joined_pending += 1;
                        }
                    }
                    node.metrics.chunk_cache_post_fetch_hits += 1;
                }
                if range_missed {
                    backend_waiters.extend(range.request_indices.iter().copied());
                }
            }

            if missing.is_empty() {
                continue;
            }
            let fetches =
                plan_chunk_fetch_ranges(&missing, self.resolver_tuning.max_fetch_chunks());
            node.metrics.chunk_dispatch_window += 1;
            node.metrics.chunk_dispatch_pending_chunks += missing.len() as u64;
            for fetch in fetches {
                let start = fetch.start * self.resolver_tuning.chunk_size_bytes();
                let end = (fetch.end * self.resolver_tuning.chunk_size_bytes()).min(archive_len);
                let bytes = end.saturating_sub(start);
                node.backend_bytes += bytes;
                node.metrics.backend_fetches += 1;
                node.metrics.backend_fetch_successes += 1;
                node.metrics.backend_fetched_chunks += fetch.end - fetch.start;
                node.metrics.chunk_waiters_released += (fetch.start..fetch.end)
                    .map(|chunk| waiters.get(&chunk).copied().unwrap_or(0))
                    .sum::<u64>();
                for chunk_index in fetch {
                    let chunk_start = chunk_index * self.resolver_tuning.chunk_size_bytes();
                    let chunk_end = ((chunk_index + 1) * self.resolver_tuning.chunk_size_bytes())
                        .min(archive_len);
                    let length = chunk_end.saturating_sub(chunk_start);
                    let weight = chunk_cache_entry_weight(length as usize);
                    node.chunk_cache.insert(
                        ChunkKey {
                            tileset_id: tileset_id.clone(),
                            chunk_index,
                        },
                        ModeledChunk { weight },
                    );
                }
            }
        }
        backend_waiters
    }

    fn record(&mut self, node_index: usize, source: TileSource, bytes: Option<u64>) {
        let node = &mut self.nodes[node_index];
        node.by_source.increment(source);
        self.by_source.increment(source);
        if let Some(bytes) = bytes {
            node.served_bytes += bytes;
            self.report.found += 1;
            self.report.served_bytes += bytes;
        } else {
            self.report.not_found += 1;
        }
    }

    pub fn request_count(&self) -> u64 {
        self.report.requests
    }

    pub fn observation(&self) -> ClusterObservation {
        let mut metrics = NodeMetricsSnapshot::default();
        for node in &self.retired_nodes {
            metrics.merge(&node.metrics);
        }
        for node in &self.nodes {
            metrics.merge(&node.metrics);
        }
        ClusterObservation {
            requests: self.report.requests,
            active_nodes: self.nodes.len(),
            virtual_elapsed_ms: None,
            gossip_messages: 0,
            gossip_bytes: 0,
            membership_converged_nodes: self.nodes.len(),
            membership_stale_nodes: 0,
            membership_missing_peer_refs: 0,
            membership_extra_peer_refs: 0,
            membership_min_peer_count: self.nodes.len(),
            membership_max_peer_count: self.nodes.len(),
            cache_hits: self.report.l1_cache_hits,
            by_source: self.by_source.to_report_map(),
            node_requests: self
                .nodes
                .iter()
                .map(|node| (node.id.clone(), node.requests))
                .collect(),
            peer_requests: self.report.peer_requests,
            peer_unavailable_requests: 0,
            peer_retryable_failures: metrics.peer_forward_retryable,
            peer_backoff_skips: metrics.peer_forward_backoff_skips,
            backend_fetches: metrics.backend_fetches,
            backend_bytes: self
                .retired_nodes
                .iter()
                .map(|node| node.backend_bytes)
                .sum::<u64>()
                + self
                    .nodes
                    .iter()
                    .map(|node| node.backend_bytes)
                    .sum::<u64>(),
            served_bytes: self.report.served_bytes,
            tile_cache_bytes: self
                .nodes
                .iter()
                .map(|node| node.tile_cache.weighted_size())
                .sum(),
            chunk_cache_bytes: self
                .nodes
                .iter()
                .map(|node| node.chunk_cache.weighted_size())
                .sum(),
        }
    }

    pub fn report(mut self) -> SimReport {
        self.finish_cache_maintenance();
        let mut metrics = NodeMetricsSnapshot::default();
        let active_nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|node| {
                metrics.merge(&node.metrics);
                modeled_node_report(node, true)
            })
            .collect();
        for node in &self.retired_nodes {
            metrics.merge(&node.metrics);
        }
        self.report.backend_bytes = self
            .retired_nodes
            .iter()
            .chain(&active_nodes)
            .map(|node| node.backend_bytes)
            .sum();
        self.report.tile_cache_bytes = active_nodes.iter().map(|node| node.tile_cache_bytes).sum();
        self.report.chunk_cache_bytes =
            active_nodes.iter().map(|node| node.chunk_cache_bytes).sum();
        self.report.metrics = metrics;
        self.report.by_source = self.by_source.to_report_map();
        let mut nodes = self.retired_nodes;
        nodes.extend(active_nodes);
        self.report.nodes = nodes;
        self.report.set_node_request_load();
        self.report.finalize_derived_metrics();
        self.report
    }
}

fn modeled_node_report(node: &ModelNode, active: bool) -> NodeReport {
    NodeReport {
        id: node.id.clone(),
        active,
        requests: node.requests,
        served_bytes: node.served_bytes,
        by_source: node.by_source.to_report_map(),
        backend_bytes: node.backend_bytes,
        tile_cache_bytes: node.tile_cache.weighted_size(),
        chunk_cache_bytes: node.chunk_cache.weighted_size(),
        metrics: node.metrics,
        scheduler: SchedulerReport::default(),
        histograms: Default::default(),
    }
}

fn tile_key(entry: &TraceEntry) -> Result<TileKey> {
    let tileset_id = TilesetId::try_new(&entry.tileset).context("invalid trace tileset")?;
    let tile_id = TileId::from(
        TileCoord::new(entry.z, entry.x, entry.y).context("invalid trace tile coordinate")?,
    )
    .value();
    Ok(TileKey {
        tileset_id,
        tile_id,
    })
}

fn chunks_for_range(start: u64, length: u64, chunk_size: u64) -> std::ops::RangeInclusive<u64> {
    let first = start / chunk_size;
    let last = start.saturating_add(length).saturating_sub(1) / chunk_size;
    first..=last
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, mem::size_of};

    use ishikari_core::storage::HrwRouter;
    use mmpf_pmtiles::{ArchiveRange, TileLocation, TileLookupTrace as TileAccessPlan};

    use super::{
        BootstrapRequest, ModeledCluster, ModeledTile, TileCatalog, chunks_for_range, tile_key,
    };
    use crate::{TraceEntry, config::ClusterConfig, topology::simulated_peers};

    const BOOTSTRAP_LENGTH: u32 = 16_384;
    const LEAF_LENGTH: u32 = 512;
    const TILE_LENGTH: u32 = 128 * 1024;
    const ARCHIVE_LENGTH: u64 = 4 * 1024 * 1024;

    fn distinct_role_fixture(
        tile_present: bool,
    ) -> (TraceEntry, TileCatalog, ClusterConfig, usize, usize, usize) {
        let config = ClusterConfig {
            node_count: 3,
            candidate_count: 1,
            tile_group_size: 1,
            cache_peer_tiles: false,
            ..ClusterConfig::default()
        };
        let peers = simulated_peers(config.node_count);
        let router = HrwRouter::new(config.candidate_count, config.tile_group_size);
        let index_owner_id = &router.route_tile(&peers, "japan", 0)[0].peer.id;
        let index_owner = peers
            .iter()
            .position(|peer| &peer.id == index_owner_id)
            .expect("index owner is in peer list");

        let (mut entry, key, tile_owner) = (1..=8)
            .find_map(|z| {
                let width = 1_u32 << z;
                (0..width).find_map(|x| {
                    (0..width).find_map(|y| {
                        let entry = TraceEntry {
                            step: 0,
                            user: 0,
                            ordinal: 0,
                            tileset: "japan".to_string(),
                            z,
                            x,
                            y,
                            entry_node: None,
                        };
                        let key = tile_key(&entry).expect("valid generated tile coordinate");
                        let owner_id =
                            &router.route_tile(&peers, key.tileset_id.as_ref(), key.tile_id)[0]
                                .peer
                                .id;
                        let owner = peers
                            .iter()
                            .position(|peer| &peer.id == owner_id)
                            .expect("tile owner is in peer list");
                        (owner != index_owner).then_some((entry, key, owner))
                    })
                })
            })
            .expect("three-node fixture has distinct index and tile owners");
        let entry_node = (0..config.node_count)
            .find(|node| *node != index_owner && *node != tile_owner)
            .expect("three-node fixture has a distinct entry node");
        entry.entry_node = Some(entry_node);
        let plan = TileAccessPlan {
            bootstrap: ArchiveRange {
                offset: 0,
                length: BOOTSTRAP_LENGTH,
            },
            leaves: vec![ArchiveRange {
                offset: 1024 * 1024,
                length: LEAF_LENGTH,
            }],
            tile: tile_present.then_some(TileLocation {
                offset: 2 * 1024 * 1024,
                length: TILE_LENGTH,
                archive_len: ARCHIVE_LENGTH,
            }),
            archive_len: ARCHIVE_LENGTH,
        };
        let catalog = TileCatalog {
            entries: HashMap::from([(key, Some(plan))]),
        };
        (entry, catalog, config, entry_node, index_owner, tile_owner)
    }

    fn tile_fixture() -> (TraceEntry, TileCatalog) {
        let entry = TraceEntry {
            step: 0,
            user: 0,
            ordinal: 0,
            tileset: "japan".to_string(),
            z: 0,
            x: 0,
            y: 0,
            entry_node: Some(0),
        };
        let key = tile_key(&entry).expect("tile key");
        let catalog = TileCatalog {
            entries: HashMap::from([(
                key,
                Some(TileAccessPlan {
                    bootstrap: ArchiveRange {
                        offset: 0,
                        length: 16_384,
                    },
                    leaves: Vec::new(),
                    tile: Some(TileLocation {
                        offset: 2 * 1024 * 1024,
                        length: 128 * 1024,
                        archive_len: 4 * 1024 * 1024,
                    }),
                    archive_len: 4 * 1024 * 1024,
                }),
            )]),
        };
        (entry, catalog)
    }

    fn absent_tile_fixture() -> (TraceEntry, TileCatalog) {
        let (entry, mut catalog) = tile_fixture();
        let key = tile_key(&entry).expect("tile key");
        catalog
            .entries
            .get_mut(&key)
            .and_then(Option::as_mut)
            .expect("lookup trace")
            .tile = None;
        (entry, catalog)
    }

    #[test]
    fn maps_byte_ranges_to_chunks() {
        assert_eq!(chunks_for_range(0, 1, 1024), 0..=0);
        assert_eq!(chunks_for_range(1023, 2, 1024), 0..=1);
        assert_eq!(chunks_for_range(2048, 1024, 1024), 2..=2);
    }

    #[test]
    fn modeled_cluster_constructs_with_normalized_resolver_boundaries() {
        let cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 1,
                candidate_count: 0,
                tile_group_size: 0,
                max_fetch_chunks: 0,
                chunk_fetch_merge_window_ms: 0,
                backend_fetch_concurrency: 0,
                ..ClusterConfig::default()
            },
            TileCatalog {
                entries: HashMap::new(),
            },
        )
        .expect("normalized modeled cluster");

        assert_eq!(cluster.config.candidate_count, 0);
        assert_eq!(cluster.resolver_tuning.candidate_count(), 1);
        assert_eq!(cluster.resolver_tuning.tile_group_size(), 1);
        assert_eq!(cluster.resolver_tuning.max_fetch_chunks(), 1);
        assert_eq!(cluster.resolver_tuning.backend_fetch_concurrency(), 1);
        assert!(cluster.resolver_tuning.chunk_fetch_merge_window().is_zero());
    }

    #[test]
    fn modeled_cache_uses_logical_weight_without_tile_payloads() {
        assert!(size_of::<ModeledTile>() <= 16);

        let (entry, catalog) = tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 1,
                tile_cache_max_bytes: 8 * 1024 * 1024 * 1024,
                chunk_cache_max_bytes: 1024 * 1024 * 1024,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");

        cluster.serve(&entry).expect("cold request");
        cluster.serve(&entry).expect("cached request");
        let report = cluster.report();

        assert_eq!(report.by_source.get("self_backend"), Some(&1));
        assert_eq!(report.by_source.get("self_cache"), Some(&1));
        assert_eq!(report.l1_cache_hits, 1);
        // One exact bootstrap probe plus the tile-body fetch.
        assert_eq!(report.metrics.backend_fetches, 2);
        assert!(report.tile_cache_bytes >= 128 * 1024);
        assert!(report.chunk_cache_bytes <= 1024 * 1024 * 1024);
    }

    #[test]
    fn peer_source_distinguishes_backend_wait_from_chunk_cache() {
        let (mut entry, catalog) = tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 2,
                tile_cache_max_bytes: 1,
                chunk_cache_max_bytes: 1024 * 1024 * 1024,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");
        let key = tile_key(&entry).expect("tile key");
        let owner = cluster
            .router
            .route_tile(&cluster.peers, key.tileset_id.as_ref(), key.tile_id)
            .first()
            .and_then(|candidate| {
                cluster
                    .nodes
                    .iter()
                    .position(|node| node.id == candidate.peer.id)
            })
            .expect("HRW owner");
        entry.entry_node = Some(1 - owner);

        cluster.serve(&entry).expect("cold peer request");
        cluster.serve(&entry).expect("chunk-cached peer request");
        let report = cluster.report();

        assert_eq!(report.by_source.get("peer_backend"), Some(&1));
        assert_eq!(report.by_source.get("peer_cache"), Some(&1));
    }

    #[test]
    fn distinct_index_and_tile_owners_account_every_cold_transfer() {
        let (entry, catalog, config, entry_node, index_owner, tile_owner) =
            distinct_role_fixture(true);
        let mut cluster = ModeledCluster::new(config, catalog).expect("modeled cluster");

        cluster.serve(&entry).expect("cold request");

        let tileset_id = tile_key(&entry).expect("tile key").tileset_id;
        assert!(
            cluster.nodes[entry_node]
                .loaded_bootstraps
                .contains(&tileset_id)
        );
        assert!(
            cluster.nodes[index_owner]
                .loaded_bootstraps
                .contains(&tileset_id)
        );
        assert!(
            cluster.nodes[tile_owner]
                .loaded_bootstraps
                .contains(&tileset_id)
        );
        assert!(cluster.nodes[tile_owner].loaded_leaves.contains(&(
            tileset_id.clone(),
            1024 * 1024,
            LEAF_LENGTH,
        )));
        assert!(!cluster.nodes[index_owner].loaded_leaves.contains(&(
            tileset_id,
            1024 * 1024,
            LEAF_LENGTH,
        )));

        let report = cluster.report();
        // bootstrap owner->index, leaf owner->index, tile entry->owner, then
        // the entry's post-peer header bootstrap entry->index.
        assert_eq!(report.peer_requests, 4);
        assert_eq!(
            report.peer_bytes,
            u64::from(2 * BOOTSTRAP_LENGTH + LEAF_LENGTH + TILE_LENGTH)
        );
        assert_eq!(report.by_source.get("peer_backend"), Some(&1));
        assert_eq!(report.metrics.peer_forward_successes, 4);
        assert_eq!(report.metrics.peer_tile_fetches, 1);
        assert_eq!(report.metrics.peer_bootstrap_fetches, 2);
        assert_eq!(report.metrics.peer_leaf_fetches, 1);
        assert_eq!(report.metrics.internal_tile_requests, 1);
        assert_eq!(report.metrics.internal_bootstrap_requests, 2);
        assert_eq!(report.metrics.internal_leaf_requests, 1);
        // Exact bootstrap probe + leaf + tile.
        assert_eq!(report.metrics.backend_fetches, 3);
    }

    #[test]
    fn remote_owner_cache_hit_still_loads_the_entry_header() {
        let (entry, catalog, config, entry_node, _index_owner, tile_owner) =
            distinct_role_fixture(true);
        let mut cluster = ModeledCluster::new(config, catalog).expect("modeled cluster");

        cluster
            .serve_on(&entry, tile_owner)
            .expect("warm owner tile");
        let requests_before = cluster.report.peer_requests;
        let bytes_before = cluster.report.peer_bytes;
        assert!(
            !cluster.nodes[entry_node]
                .loaded_bootstraps
                .contains(&tile_key(&entry).expect("tile key").tileset_id)
        );

        cluster
            .serve_on(&entry, entry_node)
            .expect("remote owner cache hit");

        assert_eq!(cluster.report.peer_requests - requests_before, 2);
        assert_eq!(
            cluster.report.peer_bytes - bytes_before,
            u64::from(BOOTSTRAP_LENGTH + TILE_LENGTH)
        );
        assert_eq!(
            cluster.by_source.to_report_map().get("peer_cache"),
            Some(&1)
        );
    }

    #[test]
    fn remote_missing_tile_skips_the_post_peer_header_transfer() {
        let (entry, catalog, config, entry_node, _index_owner, _tile_owner) =
            distinct_role_fixture(false);
        let mut cluster = ModeledCluster::new(config, catalog).expect("modeled cluster");

        cluster.serve(&entry).expect("remote missing tile");

        assert!(
            !cluster.nodes[entry_node]
                .loaded_bootstraps
                .contains(&tile_key(&entry).expect("tile key").tileset_id)
        );
        let report = cluster.report();
        assert_eq!(report.peer_requests, 3);
        assert_eq!(report.peer_bytes, u64::from(BOOTSTRAP_LENGTH + LEAF_LENGTH));
        assert_eq!(report.by_source.get("miss"), Some(&1));
    }

    #[test]
    fn absent_archive_models_group_zero_probe_and_local_fallback() {
        let config = ClusterConfig {
            node_count: 3,
            candidate_count: 3,
            tile_group_size: 1,
            ..ClusterConfig::default()
        };
        let peers = simulated_peers(config.node_count);
        let router = HrwRouter::new(config.candidate_count, config.tile_group_size);
        let mut entries_by_owner = vec![None; config.node_count];
        let mut catalog_entries = HashMap::new();
        'coordinates: for z in 1..=8 {
            let width = 1_u32 << z;
            for x in 0..width {
                for y in 0..width {
                    let mut entry = TraceEntry {
                        step: 0,
                        user: 0,
                        ordinal: 0,
                        tileset: "missing".to_string(),
                        z,
                        x,
                        y,
                        entry_node: None,
                    };
                    let key = tile_key(&entry).expect("valid generated tile coordinate");
                    let owner_id = &router.route_tile(&peers, key.tileset_id.as_ref(), key.tile_id)
                        [0]
                    .peer
                    .id;
                    let owner = peers
                        .iter()
                        .position(|peer| &peer.id == owner_id)
                        .expect("tile owner is in peer list");
                    if entries_by_owner[owner].is_none() {
                        entry.entry_node = Some(owner);
                        entries_by_owner[owner] = Some(entry);
                        catalog_entries.insert(key, None);
                    }
                    if entries_by_owner.iter().all(Option::is_some) {
                        break 'coordinates;
                    }
                }
            }
        }
        assert!(entries_by_owner.iter().all(Option::is_some));
        let mut cluster = ModeledCluster::new(
            config,
            TileCatalog {
                entries: catalog_entries,
            },
        )
        .expect("modeled cluster");

        for entry in entries_by_owner.into_iter().flatten() {
            cluster.serve(&entry).expect("missing archive request");
        }

        let tileset_id = ishikari_core::storage::TilesetId::try_new("missing").unwrap();
        assert!(
            cluster
                .nodes
                .iter()
                .all(|node| node.absent_archives.contains(&tileset_id))
        );
        let report = cluster.report();
        assert_eq!(report.peer_requests, 2);
        assert_eq!(report.peer_bytes, 0);
        assert_eq!(report.metrics.peer_forward_not_found, 2);
        assert_eq!(report.metrics.peer_bootstrap_fetches, 2);
        assert_eq!(report.metrics.internal_bootstrap_requests, 2);
        assert_eq!(report.metrics.backend_fetches, 3);
        assert_eq!(report.metrics.backend_fetch_not_found, 3);
        assert_eq!(report.by_source.get("miss"), Some(&3));
    }

    #[test]
    fn group_zero_owner_follows_membership_without_moving_decoded_state() {
        let config = ClusterConfig {
            node_count: 2,
            candidate_count: 1,
            ..ClusterConfig::default()
        };
        let router = HrwRouter::new(config.candidate_count, config.tile_group_size);
        let peers_before = simulated_peers(2);
        let peers_after = simulated_peers(3);
        let (tileset_id, old_owner, new_owner) = (0..10_000)
            .find_map(|index| {
                let tileset = format!("archive-{index}");
                let tileset_id = ishikari_core::storage::TilesetId::try_new(&tileset)
                    .expect("valid generated tileset id");
                let old_id = &router.route_tile(&peers_before, tileset_id.as_ref(), 0)[0]
                    .peer
                    .id;
                let new_id = &router.route_tile(&peers_after, tileset_id.as_ref(), 0)[0]
                    .peer
                    .id;
                if old_id == new_id {
                    return None;
                }
                let old_owner = peers_before
                    .iter()
                    .position(|peer| &peer.id == old_id)
                    .expect("old owner is in peer list");
                let new_owner = peers_after
                    .iter()
                    .position(|peer| &peer.id == new_id)
                    .expect("new owner is in peer list");
                Some((tileset_id, old_owner, new_owner))
            })
            .expect("adding a third node changes some group-zero owners");
        assert_eq!(new_owner, 2, "only the added node can displace the winner");

        let mut cluster = ModeledCluster::new(
            config,
            TileCatalog {
                entries: HashMap::new(),
            },
        )
        .expect("modeled cluster");
        cluster.nodes[old_owner]
            .loaded_bootstraps
            .insert(tileset_id.clone());
        let requester = 1 - old_owner;
        cluster.add_node().expect("add third node");

        cluster.process_bootstrap_requests(&[BootstrapRequest {
            requester_node: requester,
            tileset_id: tileset_id.clone(),
            length: BOOTSTRAP_LENGTH,
            archive_len: ARCHIVE_LENGTH,
            request_indices: Vec::new(),
        }]);

        assert_eq!(cluster.index_owner(&tileset_id), new_owner);
        assert!(
            cluster.nodes[old_owner]
                .loaded_bootstraps
                .contains(&tileset_id)
        );
        assert!(
            cluster.nodes[new_owner]
                .loaded_bootstraps
                .contains(&tileset_id)
        );
        assert!(
            cluster.nodes[requester]
                .loaded_bootstraps
                .contains(&tileset_id)
        );
        assert_eq!(cluster.nodes[old_owner].backend_bytes, 0);
        assert_eq!(
            cluster.nodes[new_owner].backend_bytes,
            u64::from(BOOTSTRAP_LENGTH)
        );
        assert_eq!(cluster.report.peer_requests, 1);
        assert_eq!(cluster.report.peer_bytes, u64::from(BOOTSTRAP_LENGTH));
    }

    #[test]
    fn owner_only_policy_does_not_populate_entry_l1() {
        let (mut entry, catalog) = tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 2,
                tile_cache_max_bytes: 1024 * 1024,
                chunk_cache_max_bytes: 1024 * 1024 * 1024,
                cache_peer_tiles: false,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");
        let key = tile_key(&entry).expect("tile key");
        let owner = cluster
            .router
            .route_tile(&cluster.peers, key.tileset_id.as_ref(), key.tile_id)
            .first()
            .and_then(|candidate| {
                cluster
                    .nodes
                    .iter()
                    .position(|node| node.id == candidate.peer.id)
            })
            .expect("HRW owner");
        entry.entry_node = Some(1 - owner);

        cluster.serve(&entry).expect("cold peer request");
        cluster.serve(&entry).expect("owner-cached peer request");
        let report = cluster.report();

        assert_eq!(report.by_source.get("peer_backend"), Some(&1));
        assert_eq!(report.by_source.get("peer_cache"), Some(&1));
        assert_eq!(report.by_source.get("self_cache"), None);
        assert_eq!(report.l1_cache_hits, 0);
    }

    #[test]
    fn first_remote_absence_counts_one_peer_attempt() {
        let (mut entry, catalog) = absent_tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 2,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");
        let key = tile_key(&entry).expect("tile key");
        let owner = cluster
            .router
            .route_tile(&cluster.peers, key.tileset_id.as_ref(), key.tile_id)
            .first()
            .and_then(|candidate| {
                cluster
                    .nodes
                    .iter()
                    .position(|node| node.id == candidate.peer.id)
            })
            .expect("HRW owner");
        entry.entry_node = Some(1 - owner);

        cluster.serve(&entry).expect("first remote miss");
        cluster.serve(&entry).expect("entry negative-cache hit");
        let report = cluster.report();

        assert_eq!(report.peer_requests, 1);
        assert_eq!(report.peer_bytes, 0);
        assert_eq!(report.not_found, 2);
    }

    #[test]
    fn remote_owner_negative_cache_hit_counts_one_peer_attempt() {
        let (mut entry, catalog) = absent_tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 2,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");
        let key = tile_key(&entry).expect("tile key");
        let owner = cluster
            .router
            .route_tile(&cluster.peers, key.tileset_id.as_ref(), key.tile_id)
            .first()
            .and_then(|candidate| {
                cluster
                    .nodes
                    .iter()
                    .position(|node| node.id == candidate.peer.id)
            })
            .expect("HRW owner");

        entry.entry_node = Some(owner);
        cluster.serve(&entry).expect("owner-local miss");
        entry.entry_node = Some(1 - owner);
        cluster
            .serve(&entry)
            .expect("remote owner negative-cache hit");
        cluster.serve(&entry).expect("entry negative-cache hit");
        let report = cluster.report();

        assert_eq!(report.peer_requests, 1);
        assert_eq!(report.peer_bytes, 0);
        assert_eq!(report.not_found, 3);
    }

    #[test]
    fn modeled_leaf_cache_identity_includes_range_length() {
        let (first_entry, mut catalog) = tile_fixture();
        let mut second_entry = first_entry.clone();
        second_entry.z = 1;
        let first_key = tile_key(&first_entry).expect("first tile key");
        let second_key = tile_key(&second_entry).expect("second tile key");
        let first_plan = catalog
            .entries
            .get_mut(&first_key)
            .and_then(Option::as_mut)
            .expect("first plan");
        first_plan.leaves = vec![ArchiveRange {
            offset: 16_384,
            length: 100,
        }];
        catalog.entries.insert(
            second_key,
            Some(TileAccessPlan {
                bootstrap: ArchiveRange {
                    offset: 0,
                    length: 16_384,
                },
                leaves: vec![ArchiveRange {
                    offset: 16_384,
                    length: 200,
                }],
                tile: Some(TileLocation {
                    offset: 3 * 1024 * 1024,
                    length: 64 * 1024,
                    archive_len: 4 * 1024 * 1024,
                }),
                archive_len: 4 * 1024 * 1024,
            }),
        );
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 1,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");

        cluster.serve(&first_entry).expect("first leaf range");
        cluster.serve(&second_entry).expect("second leaf range");

        assert_eq!(cluster.nodes[0].loaded_leaves.len(), 2);
    }

    #[test]
    fn modeled_cache_enforces_capacity_at_viewport_boundaries() {
        let (entry, catalog) = tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 1,
                tile_cache_max_bytes: 1,
                chunk_cache_max_bytes: 1,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");

        cluster.serve(&entry).expect("cold request");

        assert!(cluster.nodes[0].tile_cache.weighted_size() <= 1);
        assert!(cluster.nodes[0].chunk_cache.weighted_size() <= 1);
    }

    #[test]
    fn modeled_node_lifecycle_uses_empty_new_node_and_keeps_history() {
        let catalog = TileCatalog {
            entries: HashMap::new(),
        };
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 2,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");

        assert_eq!(cluster.add_node().expect("add node"), "node-2");
        assert_eq!(cluster.nodes[2].tile_cache.weighted_size(), 0);
        cluster.remove_node("node-1").expect("remove node");

        assert_eq!(cluster.active_node_ids(), ["node-0", "node-2"]);
        let report = cluster.report();
        assert!(
            report
                .nodes
                .iter()
                .any(|node| node.id == "node-1" && !node.active)
        );
    }

    #[test]
    fn modeled_membership_is_reported_as_instantly_converged() {
        let cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 3,
                ..ClusterConfig::default()
            },
            TileCatalog {
                entries: HashMap::new(),
            },
        )
        .expect("modeled cluster");

        let observation = cluster.observation();
        assert_eq!(observation.membership_converged_nodes, 3);
        assert_eq!(observation.membership_stale_nodes, 0);
        assert_eq!(observation.membership_min_peer_count, 3);
        assert_eq!(observation.membership_max_peer_count, 3);
        assert_eq!(observation.virtual_elapsed_ms, None);
        assert_eq!(observation.gossip_messages, 0);
    }
}
