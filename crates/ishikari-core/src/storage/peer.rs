//! Peer routing and internal HTTP transport.

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use bytes::Bytes;
use thiserror::Error;
use tokio::{sync::watch, time::Instant};
use tracing::{debug, warn};

use crate::{
    interned::{ResourceRoutingKey, TilesetId},
    metrics::NodeMetrics,
    pmtiles::BootstrapTransfer,
};
use mmpf_pmtiles::{DEFAULT_MAX_DECOMPRESSED_BYTES, MIN_BOOTSTRAP_BYTES};

use super::routing::{HrwRouter, ScoredPeer};
use mmpf_common::sync::lock_unpoisoned;

/// Reachable peer information supplied by a runtime membership adapter.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct Peer {
    pub id: String,
    pub addr: SocketAddr,
}

/// One-gossip-tick cache of Ishikari's projected routable peer set.
///
/// Runtime adapters own cluster inspection, while this core type owns the
/// production/simulation cache semantics used by resource routing.
#[derive(Clone)]
pub struct PeerSnapshotCache {
    inner: Arc<PeerSnapshotCacheInner>,
}

struct PeerSnapshotCacheInner {
    state: Mutex<PeerSnapshotCacheState>,
    changed: watch::Sender<u64>,
    ttl: Duration,
}

#[derive(Default)]
struct PeerSnapshotCacheState {
    cached: Option<CachedPeerSnapshot>,
    loading: bool,
}

struct CachedPeerSnapshot {
    stored_at: Instant,
    peers: Arc<[Peer]>,
}

impl PeerSnapshotCache {
    pub fn new(ttl: Duration) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            inner: Arc::new(PeerSnapshotCacheInner {
                state: Mutex::new(PeerSnapshotCacheState::default()),
                changed,
                ttl,
            }),
        }
    }

    pub fn get(&self) -> Option<Arc<[Peer]>> {
        fresh_peer_snapshot(&lock_unpoisoned(&self.inner.state), self.inner.ttl)
    }

    fn store(&self, peers: Arc<[Peer]>) {
        let mut state = lock_unpoisoned(&self.inner.state);
        state.cached = Some(CachedPeerSnapshot {
            stored_at: Instant::now(),
            peers,
        });
        state.loading = false;
        drop(state);
        self.notify_changed();
    }

    pub async fn get_or_load<F, Fut>(&self, load: F) -> Arc<[Peer]>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Arc<[Peer]>>,
    {
        let mut load = Some(load);
        loop {
            if let Some(peers) = self.get() {
                return peers;
            }

            // Subscribe before the locked recheck so completion between the
            // first read and waiting cannot be lost.
            let mut changed = self.inner.changed.subscribe();
            let should_load = {
                let mut state = lock_unpoisoned(&self.inner.state);
                if let Some(peers) = fresh_peer_snapshot(&state, self.inner.ttl) {
                    return peers;
                }
                if state.loading {
                    false
                } else {
                    state.loading = true;
                    true
                }
            };

            if should_load {
                let guard = PeerSnapshotLoad::new(self);
                let peers = load.take().expect("peer snapshot loader called once")().await;
                guard.complete(peers.clone());
                return peers;
            }

            if changed.changed().await.is_err() {
                // The sender lives with `self`, so this is only defensive.
                continue;
            }
        }
    }

    fn notify_changed(&self) {
        self.inner.changed.send_modify(|version| {
            *version = version.wrapping_add(1);
        });
    }
}

fn fresh_peer_snapshot(state: &PeerSnapshotCacheState, ttl: Duration) -> Option<Arc<[Peer]>> {
    state
        .cached
        .as_ref()
        .and_then(|snapshot| (snapshot.stored_at.elapsed() < ttl).then(|| snapshot.peers.clone()))
}

struct PeerSnapshotLoad<'a> {
    cache: &'a PeerSnapshotCache,
    complete: bool,
}

impl<'a> PeerSnapshotLoad<'a> {
    fn new(cache: &'a PeerSnapshotCache) -> Self {
        Self {
            cache,
            complete: false,
        }
    }

    fn complete(mut self, peers: Arc<[Peer]>) {
        self.cache.store(peers);
        self.complete = true;
    }
}

impl Drop for PeerSnapshotLoad<'_> {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        lock_unpoisoned(&self.cache.inner.state).loading = false;
        self.cache.notify_changed();
    }
}

/// Peer-backed internal transport for routed resources.
#[derive(Clone)]
pub struct PeerBackend {
    self_node_id: String,
    peer_directory: Arc<dyn PeerDirectory>,
    router: HrwRouter,
    transport: Arc<dyn InternalTransport>,
    retryable_failures: Arc<Mutex<HashMap<String, HashMap<&'static str, Instant>>>>,
    inflight_fetches: Arc<Mutex<HashMap<(String, String), usize>>>,
    metrics: NodeMetrics,
}

const PEER_RETRY_BACKOFF: Duration = Duration::from_secs(1);
const MAX_INTERNAL_BINARY_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_INTERNAL_GLYPH_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_INTERNAL_JSON_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_INTERNAL_SPRITE_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_INTERNAL_BOOTSTRAP_RESPONSE_BYTES: usize =
    std::mem::size_of::<u64>() + MIN_BOOTSTRAP_BYTES + DEFAULT_MAX_DECOMPRESSED_BYTES;

pub type PeerFuture<'a> = Pin<Box<dyn Future<Output = Arc<[Peer]>> + Send + 'a>>;
pub type FetchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<InternalFetchResponse, PeerFetchError>> + Send + 'a>>;

pub const TILE_SOURCE_HEADER: &str = "x-ishikari-tile-source";
pub const PROVIDER_CACHE_CONTROL_HEADER: &str = "x-ishikari-provider-cache-control";
pub const PROVIDER_AGE_HEADER: &str = "x-ishikari-provider-age";
pub const PROVIDER_ETAG_HEADER: &str = "x-ishikari-provider-etag";
pub const PROVIDER_LAST_MODIFIED_HEADER: &str = "x-ishikari-provider-last-modified";
pub const PROVIDER_NEGATIVE_HEADER: &str = "x-ishikari-provider-negative";

/// Bounded provider-resource category used by routing, metrics, and diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderResourceKind {
    Style,
    Glyph,
    Sprite,
}

impl ProviderResourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Style => "style",
            Self::Glyph => "glyph",
            Self::Sprite => "sprite",
        }
    }
}

/// Bounded sprite representation accepted by Ishikari's provider endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderSpriteVariant {
    Json,
    Png,
    Json2x,
    Png2x,
}

impl ProviderSpriteVariant {
    pub fn suffix(self) -> &'static str {
        match self {
            Self::Json => ".json",
            Self::Png => ".png",
            Self::Json2x => "@2x.json",
            Self::Png2x => "@2x.png",
        }
    }
}

/// Typed provider request used for HRW placement and internal peer forwarding.
///
/// Logical route components remain separate from the complete upstream URL so
/// authorization can continue to operate on the parsed resource identity. The
/// upstream URL is retained only to preserve the existing HRW placement key and
/// local provider-fetch/cache identity; diagnostics intentionally omit it.
pub struct ProviderRequest<'a> {
    resource: ProviderResource<'a>,
    upstream_url: &'a str,
}

enum ProviderResource<'a> {
    Style {
        style_key: &'a str,
    },
    Glyph {
        fontstack: &'a str,
        range: &'a str,
    },
    Sprite {
        style_key: &'a str,
        variant: ProviderSpriteVariant,
    },
}

impl<'a> ProviderRequest<'a> {
    pub fn style(style_key: &'a str, upstream_url: &'a str) -> Self {
        Self {
            resource: ProviderResource::Style { style_key },
            upstream_url,
        }
    }

    pub fn glyph(fontstack: &'a str, range: &'a str, upstream_url: &'a str) -> Self {
        Self {
            resource: ProviderResource::Glyph { fontstack, range },
            upstream_url,
        }
    }

    pub fn sprite(
        style_key: &'a str,
        variant: ProviderSpriteVariant,
        upstream_url: &'a str,
    ) -> Self {
        Self {
            resource: ProviderResource::Sprite { style_key, variant },
            upstream_url,
        }
    }

    pub fn kind(&self) -> ProviderResourceKind {
        match self.resource {
            ProviderResource::Style { .. } => ProviderResourceKind::Style,
            ProviderResource::Glyph { .. } => ProviderResourceKind::Glyph,
            ProviderResource::Sprite { .. } => ProviderResourceKind::Sprite,
        }
    }

    pub fn upstream_url(&self) -> &str {
        self.upstream_url
    }

    fn placement_key(&self) -> String {
        match self.resource {
            ProviderResource::Style { .. } => format!("style:{}", self.upstream_url),
            ProviderResource::Glyph { .. } => format!("glyph:{}", self.upstream_url),
            ProviderResource::Sprite { variant, .. } => {
                format!("sprite:{}:{}", variant.suffix(), self.upstream_url)
            }
        }
    }

    fn internal_path(&self) -> String {
        match self.resource {
            ProviderResource::Style { style_key } => format!(
                "/_internal/provider/styles/{}/style.json",
                provider_path_encode_segments(style_key)
            ),
            ProviderResource::Glyph { fontstack, range } => format!(
                "/_internal/provider/fonts/{}/{}.pbf",
                provider_path_encode(fontstack),
                range
            ),
            ProviderResource::Sprite { style_key, variant } => format!(
                "/_internal/provider/styles/{}/sprite{}",
                provider_path_encode_segments(style_key),
                variant.suffix()
            ),
        }
    }

    fn logical_identity(&self) -> ProviderLogicalIdentity<'_> {
        ProviderLogicalIdentity(&self.resource)
    }
}

impl fmt::Debug for ProviderRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderRequest")
            .field("kind", &self.kind())
            .field("logical_identity", &self.logical_identity().to_string())
            .finish_non_exhaustive()
    }
}

struct ProviderLogicalIdentity<'a>(&'a ProviderResource<'a>);

impl fmt::Display for ProviderLogicalIdentity<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ProviderResource::Style { style_key } => formatter.write_str(style_key),
            ProviderResource::Glyph { fontstack, range } => {
                write!(formatter, "{fontstack}/{range}")
            }
            ProviderResource::Sprite { style_key, variant } => {
                write!(formatter, "{style_key}/sprite{}", variant.suffix())
            }
        }
    }
}

fn provider_path_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b',') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn provider_path_encode_segments(value: &str) -> String {
    value
        .split('/')
        .map(provider_path_encode)
        .collect::<Vec<_>>()
        .join("/")
}

/// Bounded marker for authoritative provider negatives on the internal wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InternalProviderNegative {
    NotFound,
    Gone,
}

impl InternalProviderNegative {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not-found",
            Self::Gone => "gone",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "not-found" => Some(Self::NotFound),
            "gone" => Some(Self::Gone),
            _ => None,
        }
    }
}

/// Tile provenance reported by the node that resolved an internal request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InternalTileSource {
    Cache,
    Backend,
}

impl InternalTileSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::Backend => "backend",
        }
    }

    /// Parses the internal tile-source header value produced by [`Self::as_str`].
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "cache" => Some(Self::Cache),
            "backend" => Some(Self::Backend),
            _ => None,
        }
    }
}

/// Body and optional metadata returned by Ishikari's internal transport.
pub struct InternalFetchResponse {
    pub bytes: Bytes,
    pub tile_source: Option<InternalTileSource>,
    pub provider_cache_control: Option<String>,
    pub provider_age_seconds: Option<u64>,
    pub provider_etag: Option<String>,
    /// HTTP-date, exactly as forwarded on the internal wire.
    pub provider_last_modified: Option<String>,
    /// Standard representation metadata; unlike cache policy this does not use
    /// an Ishikari-private header.
    pub content_encoding: Option<String>,
}

/// Authoritative outcome returned by a routed provider owner.
///
/// This deliberately models only the provider outcomes callers may preserve;
/// arbitrary peer HTTP status codes never cross into core routing.
pub enum ProviderRouteOutcome {
    Resource(InternalFetchResponse),
    NotFound,
    Gone,
}

impl InternalFetchResponse {
    #[cfg(feature = "simulator-support")]
    pub(crate) fn bytes(bytes: Bytes) -> Self {
        Self {
            bytes,
            tile_source: None,
            provider_cache_control: None,
            provider_age_seconds: None,
            provider_etag: None,
            provider_last_modified: None,
            content_encoding: None,
        }
    }

    #[cfg(any(test, feature = "simulator-support"))]
    pub(crate) fn tile(bytes: Bytes, source: InternalTileSource) -> Self {
        Self {
            bytes,
            tile_source: Some(source),
            provider_cache_control: None,
            provider_age_seconds: None,
            provider_etag: None,
            provider_last_modified: None,
            content_encoding: None,
        }
    }
}

/// Supplies the current routable peer set independently of gossip transport.
pub trait PeerDirectory: Send + Sync {
    fn peers(&self) -> PeerFuture<'_>;
}

/// Fetches a path from a selected peer independently of the routing policy.
///
/// Callers construct only Ishikari's typed `/_internal/*` paths; implementations
/// must not reinterpret the path as an arbitrary upstream URL.
pub trait InternalTransport: Send + Sync {
    fn fetch<'a>(&'a self, peer: &'a Peer, path: &'a str) -> FetchFuture<'a>;
}

/// Errors returned while fetching internal resources from a peer.
#[derive(Debug, Error)]
pub enum PeerFetchError {
    #[error("peer resource not found")]
    NotFound,
    #[error("provider resource not found")]
    ProviderNotFound,
    #[error("provider resource gone")]
    ProviderGone,
    #[error("{0}")]
    Retryable(String),
    #[error("{0}")]
    Fatal(String),
}

impl PeerFetchError {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

impl PeerBackend {
    /// Creates a peer backend with injected discovery and transport implementations.
    pub fn with_dependencies(
        self_node_id: String,
        peer_directory: Arc<dyn PeerDirectory>,
        router: HrwRouter,
        transport: Arc<dyn InternalTransport>,
        metrics: NodeMetrics,
    ) -> Self {
        Self {
            self_node_id,
            peer_directory,
            router,
            transport,
            retryable_failures: Arc::new(Mutex::new(HashMap::new())),
            inflight_fetches: Arc::new(Mutex::new(HashMap::new())),
            metrics,
        }
    }

    async fn route_tileset_for(&self, tileset_id: &TilesetId, kind: &str) -> Vec<ScoredPeer> {
        let peers = self.peer_directory.peers().await;
        self.route_with_backoff(&peers, peer_resource_label(kind), |peers| {
            self.router.route_tileset(peers, tileset_id.as_ref())
        })
    }

    /// Returns the routed candidate peers for a tile request.
    pub(super) async fn route_tile(&self, tileset_id: &TilesetId, tile_id: u64) -> Vec<ScoredPeer> {
        self.route_tile_for(tileset_id, tile_id, "tile").await
    }

    async fn route_tile_for(
        &self,
        tileset_id: &TilesetId,
        tile_id: u64,
        kind: &str,
    ) -> Vec<ScoredPeer> {
        let peers = self.peer_directory.peers().await;
        self.route_with_backoff(&peers, peer_resource_label(kind), |peers| {
            self.router.route_tile(peers, tileset_id.as_ref(), tile_id)
        })
    }

    async fn route_derived_tile(
        &self,
        routing_key: &ResourceRoutingKey,
        tile_id: u64,
    ) -> Vec<ScoredPeer> {
        let peers = self.peer_directory.peers().await;
        self.route_with_backoff(&peers, peer_resource_label("derived"), |peers| {
            self.router.route_tile(peers, routing_key.as_ref(), tile_id)
        })
    }

    async fn route_provider_for(&self, request: &ProviderRequest<'_>) -> Vec<ScoredPeer> {
        let peers = self.peer_directory.peers().await;
        let placement_key = request.placement_key();
        self.route_with_backoff(&peers, request.kind().as_str(), |peers| {
            self.router.route_key(peers, &placement_key)
        })
    }

    /// Returns whether the given peer is the local node.
    pub(super) fn is_self(&self, peer: &Peer) -> bool {
        peer.id == self.self_node_id
    }

    /// Routes a bootstrap request across candidate peers, returning the first successful result.
    pub(super) async fn route_bootstrap(
        &self,
        tileset_id: &TilesetId,
        include_metadata: bool,
    ) -> Result<Option<BootstrapTransfer>> {
        let key = encode_tileset_path(tileset_id);
        let path = if include_metadata {
            format!("/_internal/pmtiles/{key}/bootstrap?metadata=true")
        } else {
            format!("/_internal/pmtiles/{key}/bootstrap")
        };
        let result = self
            .route_fetch_optional(tileset_id, &path, "bootstrap")
            .await?;
        match result {
            Some(bytes) => {
                let transfer = decode_bootstrap_wire(bytes, include_metadata)?;
                Ok(Some(transfer))
            }
            None => Ok(None),
        }
    }

    /// Routes a leaf request across candidate peers, returning the first successful result.
    pub(super) async fn route_leaf(
        &self,
        tileset_id: &TilesetId,
        offset: u64,
        length: usize,
    ) -> Result<Option<Bytes>> {
        let key = encode_tileset_path(tileset_id);
        let path = format!("/_internal/pmtiles/{key}/leaf/{offset}/{length}");
        self.route_fetch_optional(tileset_id, &path, "leaf").await
    }

    /// Fetches tile bytes from a peer over the internal tile endpoint.
    pub(super) async fn fetch_tile_bytes(
        &self,
        peer: &Peer,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<InternalFetchResponse, PeerFetchError> {
        let key = encode_tileset_path(tileset_id);
        let path = format!("/_internal/tiles/{key}/{tile_id}");
        self.fetch_from_peer(peer, &path, "tile").await
    }

    /// Routes a typed provider-resource request across its HRW candidates.
    ///
    /// The generated internal path names a bounded endpoint that resolves the
    /// upstream resource from local provider config, so forwarding cannot become
    /// an arbitrary URL fetcher.
    pub(super) async fn route_provider_request(
        &self,
        request: &ProviderRequest<'_>,
    ) -> Result<Option<ProviderRouteOutcome>> {
        let candidates = self.route_provider_for(request).await;
        let path = request.internal_path();
        self.route_provider_response_candidates(candidates, request, &path)
            .await
    }

    async fn route_provider_response_candidates(
        &self,
        candidates: Vec<ScoredPeer>,
        request: &ProviderRequest<'_>,
        path: &str,
    ) -> Result<Option<ProviderRouteOutcome>> {
        let kind = request.kind().as_str();
        let logical_identity = request.logical_identity();
        if candidates.is_empty()
            || candidates
                .first()
                .is_some_and(|peer| self.is_self(&peer.peer))
        {
            debug!(provider_identity = %logical_identity, kind, "using local provider read");
            return Ok(None);
        }

        for peer in candidates {
            if self.is_self(&peer.peer) {
                debug!(
                    provider_identity = %logical_identity,
                    peer_id = %peer.peer.id,
                    kind = kind,
                    "reached local provider owner; falling back local"
                );
                return Ok(None);
            }

            debug!(
                provider_identity = %logical_identity,
                peer_id = %peer.peer.id,
                kind = kind,
                "forwarding provider request to peer"
            );
            match self.fetch_from_peer(&peer.peer, path, kind).await {
                Ok(response) => return Ok(Some(ProviderRouteOutcome::Resource(response))),
                Err(PeerFetchError::ProviderNotFound) => {
                    debug!(
                        provider_identity = %logical_identity,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        "provider owner reported resource not found"
                    );
                    return Ok(Some(ProviderRouteOutcome::NotFound));
                }
                Err(PeerFetchError::ProviderGone) => {
                    debug!(
                        provider_identity = %logical_identity,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        "provider owner reported resource gone"
                    );
                    return Ok(Some(ProviderRouteOutcome::Gone));
                }
                Err(error) if error.is_retryable() => {
                    warn!(
                        provider_identity = %logical_identity,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        error = %error,
                        "provider forward failed; trying next candidate"
                    );
                }
                Err(error) => {
                    warn!(
                        provider_identity = %logical_identity,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        error = %error,
                        "provider forward failed; falling back local"
                    );
                    return Ok(None);
                }
            }
        }

        debug!(
            provider_identity = %logical_identity,
            kind = kind,
            "all provider forwards failed; falling back local"
        );
        Ok(None)
    }

    /// Routes a typed derived resource using the same Hilbert-group HRW
    /// placement as stored tiles. The caller owns the internal wire format;
    /// `None` means local fallback, including a peer returning 404 for the
    /// typed endpoint.
    pub(super) async fn route_derived_resource(
        &self,
        routing_key: &ResourceRoutingKey,
        tile_id: u64,
        path: &str,
    ) -> Result<Option<Bytes>> {
        let candidates = self.route_derived_tile(routing_key, tile_id).await;
        Ok(self
            .route_fetch_optional_response_candidates(
                candidates,
                routing_key.as_ref(),
                path,
                "derived",
            )
            .await?
            .map(|response| response.bytes))
    }

    async fn route_fetch_optional_response_candidates(
        &self,
        candidates: Vec<ScoredPeer>,
        routing_key: &str,
        path: &str,
        kind: &str,
    ) -> Result<Option<InternalFetchResponse>> {
        if candidates.is_empty()
            || candidates
                .first()
                .is_some_and(|peer| self.is_self(&peer.peer))
        {
            debug!(routing_key, kind, "using local resource read");
            return Ok(None);
        }

        for peer in candidates {
            if self.is_self(&peer.peer) {
                debug!(
                    routing_key,
                    peer_id = %peer.peer.id,
                    kind = kind,
                    "reached local resource owner; falling back local"
                );
                return Ok(None);
            }

            debug!(
                routing_key,
                peer_id = %peer.peer.id,
                kind = kind,
                "forwarding resource request to peer"
            );
            match self.fetch_from_peer(&peer.peer, path, kind).await {
                Ok(response) => {
                    debug!(
                        routing_key,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        body_len = response.bytes.len(),
                        "received resource bytes from peer"
                    );
                    return Ok(Some(response));
                }
                Err(PeerFetchError::NotFound) => {
                    debug!(
                        routing_key,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        "peer does not serve the typed resource; falling back local"
                    );
                    return Ok(None);
                }
                Err(error) if error.is_retryable() => {
                    warn!(
                        routing_key,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        error = %error,
                        "provider forward failed; trying next candidate"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(
                        routing_key,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        error = %error,
                        "provider forward failed; falling back local"
                    );
                    return Ok(None);
                }
            }
        }

        debug!(
            routing_key,
            kind = kind,
            "all resource forwards failed; falling back local"
        );
        Ok(None)
    }

    /// Routes a request across tileset candidate peers, returning `None` to signal local fallback.
    async fn route_fetch_optional(
        &self,
        tileset_id: &TilesetId,
        path: &str,
        kind: &str,
    ) -> Result<Option<Bytes>> {
        let candidates = self.route_tileset_for(tileset_id, kind).await;
        Ok(self
            .route_fetch_optional_response_candidates(candidates, tileset_id.as_ref(), path, kind)
            .await?
            .map(|response| response.bytes))
    }

    async fn fetch_from_peer(
        &self,
        peer: &Peer,
        path: &str,
        kind: &str,
    ) -> Result<InternalFetchResponse, PeerFetchError> {
        let (inflight_guard, duplicate) = PeerFetchGuard::enter(
            Arc::clone(&self.inflight_fetches),
            (peer.id.clone(), path.to_string()),
        );
        let resource = peer_resource_label(kind);
        if duplicate {
            self.metrics.record_peer_fetch_duplicate_inflight(resource);
        }
        let result = self.transport.fetch(peer, path).await;
        drop(inflight_guard);
        let outcome = match &result {
            Ok(_) => "success",
            Err(PeerFetchError::NotFound) => "not_found",
            Err(PeerFetchError::ProviderNotFound) => "provider_not_found",
            Err(PeerFetchError::ProviderGone) => "provider_gone",
            Err(PeerFetchError::Retryable(_)) => "retryable",
            Err(PeerFetchError::Fatal(_)) => "fatal",
        };
        self.metrics.record_peer_forward(outcome);
        self.metrics.record_peer_fetch(resource, outcome);

        let mut failures = lock_unpoisoned(&self.retryable_failures);
        if result.as_ref().is_err_and(PeerFetchError::is_retryable) {
            failures
                .entry(peer.id.clone())
                .or_default()
                .insert(resource, Instant::now() + PEER_RETRY_BACKOFF);
        } else if let Some(resources) = failures.get_mut(&peer.id) {
            resources.remove(resource);
            if resources.is_empty() {
                failures.remove(&peer.id);
            }
        }
        result
    }

    fn route_with_backoff(
        &self,
        peers: &[Peer],
        resource: &'static str,
        route: impl Fn(&[Peer]) -> Vec<ScoredPeer>,
    ) -> Vec<ScoredPeer> {
        let preferred = route(peers);
        let available = self.available_peers(peers, resource);
        let Cow::Owned(available) = available else {
            return preferred;
        };

        // Count only suppressed peers that HRW would actually have selected as
        // candidates. Backed-off peers outside the candidate set do not avoid a
        // forward and therefore must not increase the backoff metric.
        for candidate in &preferred {
            if !available.iter().any(|peer| peer.id == candidate.peer.id) {
                self.metrics.record_peer_forward("backoff");
            }
        }
        route(&available)
    }

    fn available_peers<'a>(&self, peers: &'a [Peer], resource: &'static str) -> Cow<'a, [Peer]> {
        let now = Instant::now();
        let mut failures = lock_unpoisoned(&self.retryable_failures);
        failures.retain(|_, resources| {
            resources.retain(|_, retry_at| *retry_at > now);
            !resources.is_empty()
        });
        if failures.is_empty() {
            return Cow::Borrowed(peers);
        }
        if !failures
            .values()
            .any(|resources| resources.contains_key(resource))
        {
            return Cow::Borrowed(peers);
        }
        let available = peers
            .iter()
            .filter(|peer| {
                !failures
                    .get(&peer.id)
                    .is_some_and(|resources| resources.contains_key(resource))
            })
            .cloned()
            .collect::<Vec<_>>();
        Cow::Owned(available)
    }
}

struct PeerFetchGuard {
    inflight: Arc<Mutex<HashMap<(String, String), usize>>>,
    key: (String, String),
}

impl PeerFetchGuard {
    fn enter(
        inflight: Arc<Mutex<HashMap<(String, String), usize>>>,
        key: (String, String),
    ) -> (Self, bool) {
        let duplicate = {
            let mut requests = lock_unpoisoned(&inflight);
            let count = requests.entry(key.clone()).or_default();
            let duplicate = *count > 0;
            *count += 1;
            duplicate
        };
        (Self { inflight, key }, duplicate)
    }
}

impl Drop for PeerFetchGuard {
    fn drop(&mut self) {
        let mut requests = lock_unpoisoned(&self.inflight);
        let Some(count) = requests.get_mut(&self.key) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            requests.remove(&self.key);
        }
    }
}

fn peer_resource_label(kind: &str) -> &'static str {
    match kind {
        "tile" => "tile",
        "bootstrap" => "bootstrap",
        "leaf" => "leaf",
        "style" => "style",
        "glyph" => "glyph",
        "sprite" => "sprite",
        "derived" => "derived",
        _ => "other",
    }
}

/// Classifies a typed internal forwarding path into a bounded metric label.
pub fn internal_resource_kind(path: &str) -> Option<&'static str> {
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    if path.starts_with("/_internal/tiles/") {
        return Some("tile");
    }
    if path.starts_with("/_internal/derived/") {
        return Some("derived");
    }
    if path.starts_with("/_internal/pmtiles/") {
        if path.ends_with("/bootstrap") {
            return Some("bootstrap");
        }
        if path.contains("/leaf/") {
            return Some("leaf");
        }
    }
    if path.starts_with("/_internal/provider/fonts/") {
        return Some("glyph");
    }
    if path.starts_with("/_internal/provider/styles/") {
        if path.ends_with("/style.json") {
            return Some("style");
        }
        if path.contains("/sprite") {
            return Some("sprite");
        }
        return Some("other");
    }
    None
}

/// Returns the total request deadline for a typed internal peer route.
///
/// Provider owners may spend the full upstream deadline before returning, and
/// derived resources can compose multiple source requests. Keeping this policy
/// beside route classification lets production and in-process transports use
/// identical deadlines.
pub fn internal_peer_request_timeout(path: &str) -> Duration {
    if path.starts_with("/_internal/derived/") {
        Duration::from_secs(30)
    } else if path.starts_with("/_internal/provider/") {
        Duration::from_secs(20)
    } else {
        Duration::from_secs(10)
    }
}

/// Returns the maximum accepted response body for a typed internal route.
///
/// Unknown routes retain the generic binary ceiling so the transport remains
/// bounded without changing its existing fallback behavior.
pub fn internal_response_body_limit(path: &str) -> usize {
    match internal_resource_kind(path) {
        Some("bootstrap") => MAX_INTERNAL_BOOTSTRAP_RESPONSE_BYTES,
        Some("glyph") => MAX_INTERNAL_GLYPH_RESPONSE_BYTES,
        Some("style") => MAX_INTERNAL_JSON_RESPONSE_BYTES,
        Some("sprite") => MAX_INTERNAL_SPRITE_RESPONSE_BYTES,
        Some("tile" | "leaf" | "derived" | "other") | None => MAX_INTERNAL_BINARY_RESPONSE_BYTES,
        Some(_) => MAX_INTERNAL_BINARY_RESPONSE_BYTES,
    }
}

/// Percent-encodes a tileset key for embedding in an internal URL path.
///
/// Validated tileset keys contain only `[A-Za-z0-9._-]` plus at most one `/`
/// namespace separator, so encoding `/` to `%2F` is enough to keep the key
/// inside a single path segment. The peer's axum router percent-decodes it
/// back before validating.
fn encode_tileset_path(tileset_id: &TilesetId) -> String {
    tileset_id.as_str().replace('/', "%2F")
}

/// Decodes the bootstrap wire format received from a peer.
///
/// Without metadata: raw bootstrap bytes.
/// With metadata: `[8 bytes: bootstrap_len as u64 LE][bootstrap][metadata]`.
fn decode_bootstrap_wire(body: Bytes, include_metadata: bool) -> Result<BootstrapTransfer> {
    if !include_metadata {
        anyhow::ensure!(
            body.len() <= MIN_BOOTSTRAP_BYTES,
            "bootstrap transfer exceeds the accepted bootstrap size"
        );
        return Ok(BootstrapTransfer {
            bootstrap: body,
            metadata: None,
        });
    }
    const PREFIX_BYTES: usize = std::mem::size_of::<u64>();
    const MAX_TRANSFER_BYTES: usize =
        PREFIX_BYTES + MIN_BOOTSTRAP_BYTES + DEFAULT_MAX_DECOMPRESSED_BYTES;

    anyhow::ensure!(
        body.len() <= MAX_TRANSFER_BYTES,
        "bootstrap transfer exceeds the accepted metadata envelope"
    );
    anyhow::ensure!(body.len() >= PREFIX_BYTES, "bootstrap transfer too short");
    let bootstrap_len = usize::try_from(u64::from_le_bytes(
        body[..PREFIX_BYTES]
            .try_into()
            .expect("length prefix has exactly eight bytes"),
    ))
    .map_err(|_| anyhow::anyhow!("bootstrap length exceeds the platform size"))?;
    anyhow::ensure!(
        bootstrap_len <= MIN_BOOTSTRAP_BYTES,
        "bootstrap length exceeds the accepted bootstrap size"
    );
    let bootstrap_end = PREFIX_BYTES
        .checked_add(bootstrap_len)
        .ok_or_else(|| anyhow::anyhow!("bootstrap transfer length overflows usize"))?;
    anyhow::ensure!(body.len() >= bootstrap_end, "bootstrap transfer truncated");
    let bootstrap = body.slice(PREFIX_BYTES..bootstrap_end);
    let metadata = if body.len() > bootstrap_end {
        Some(body.slice(bootstrap_end..))
    } else {
        None
    };
    Ok(BootstrapTransfer {
        bootstrap,
        metadata,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        net::SocketAddr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use bytes::{BufMut, Bytes, BytesMut};
    use mmpf_pmtiles::{DEFAULT_MAX_DECOMPRESSED_BYTES, MIN_BOOTSTRAP_BYTES};
    use tokio::sync::{Barrier, Semaphore};

    use super::{
        FetchFuture, InternalFetchResponse, InternalTileSource, InternalTransport,
        PEER_RETRY_BACKOFF, PeerBackend, PeerDirectory, PeerFetchError, PeerFuture,
        PeerSnapshotCache, ProviderRequest, ProviderRouteOutcome, ProviderSpriteVariant,
        decode_bootstrap_wire, internal_resource_kind, internal_response_body_limit,
    };
    use crate::{
        interned::{ResourceRoutingKey, TilesetId},
        metrics::NodeMetrics,
        storage::routing::HrwRouter,
    };

    use super::Peer;

    fn tileset_id(value: &str) -> TilesetId {
        TilesetId::try_new(value).expect("valid test tileset id")
    }

    fn derived_routing_key() -> ResourceRoutingKey {
        ResourceRoutingKey::for_derived_resource("hillshade", &tileset_id("mapterhorn/planet"))
            .expect("valid test resource routing key")
    }

    #[test]
    fn bootstrap_wire_rejects_untrusted_lengths_without_panicking() {
        for declared_len in [u64::MAX, (MIN_BOOTSTRAP_BYTES as u64) + 1, 4] {
            let mut frame = BytesMut::with_capacity(10);
            frame.put_u64_le(declared_len);
            frame.extend_from_slice(&[1, 2]);

            let error = decode_bootstrap_wire(frame.freeze(), true)
                .err()
                .expect("invalid bootstrap length must fail");
            assert!(
                error.to_string().contains("bootstrap"),
                "unexpected error for {declared_len}: {error:#}"
            );
        }
    }

    #[test]
    fn bootstrap_wire_accepts_the_maximum_bootstrap_boundary() {
        let bootstrap = vec![7; MIN_BOOTSTRAP_BYTES];
        let mut frame = BytesMut::with_capacity(8 + bootstrap.len() + 2);
        frame.put_u64_le(bootstrap.len() as u64);
        frame.extend_from_slice(&bootstrap);
        frame.extend_from_slice(&[8, 9]);

        let transfer = decode_bootstrap_wire(frame.freeze(), true)
            .expect("maximum bootstrap and bounded metadata must decode");
        assert_eq!(transfer.bootstrap.as_ref(), bootstrap);
        assert_eq!(transfer.metadata.as_deref(), Some([8, 9].as_slice()));
    }

    #[test]
    fn bootstrap_only_wire_rejects_oversized_payloads() {
        let body = Bytes::from(vec![0; MIN_BOOTSTRAP_BYTES + 1]);
        let error = decode_bootstrap_wire(body, false)
            .err()
            .expect("oversized bootstrap-only transfer must fail");
        assert!(error.to_string().contains("accepted bootstrap size"));
    }

    #[test]
    fn peer_snapshot_cache_reuses_live_snapshots_and_expires_zero_ttl() {
        let peers: Arc<[Peer]> = vec![Peer {
            id: "node-a".to_string(),
            addr: "127.0.0.1:9090".parse().unwrap(),
        }]
        .into();
        let cache = PeerSnapshotCache::new(Duration::from_secs(1));
        cache.store(peers.clone());

        let cached = cache.get().expect("live snapshot");
        assert!(Arc::ptr_eq(&cached, &peers));

        let expired = PeerSnapshotCache::new(Duration::ZERO);
        expired.store(peers);
        assert!(expired.get().is_none());
    }

    #[tokio::test]
    async fn peer_snapshot_cache_coalesces_concurrent_loads() {
        const CALLERS: usize = 8;
        let cache = PeerSnapshotCache::new(Duration::from_secs(60));
        let expected: Arc<[Peer]> = vec![Peer {
            id: "node-a".to_string(),
            addr: "127.0.0.1:9090".parse().unwrap(),
        }]
        .into();
        let loads = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(CALLERS + 1));
        let release = Arc::new(Semaphore::new(0));
        let mut tasks = Vec::new();

        for _ in 0..CALLERS {
            let cache = cache.clone();
            let expected = expected.clone();
            let loads = loads.clone();
            let start = start.clone();
            let release = release.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                cache
                    .get_or_load(|| async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        release.acquire().await.unwrap().forget();
                        expected
                    })
                    .await
            }));
        }

        start.wait().await;
        while loads.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        for _ in 0..CALLERS {
            tokio::task::yield_now().await;
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        release.add_permits(1);

        for task in tasks {
            let peers = task.await.unwrap();
            assert!(Arc::ptr_eq(&peers, &expected));
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_peer_snapshot_load_releases_waiters() {
        let cache = PeerSnapshotCache::new(Duration::from_secs(60));
        let started = Arc::new(Semaphore::new(0));
        let task = {
            let cache = cache.clone();
            let started = started.clone();
            tokio::spawn(async move {
                cache
                    .get_or_load(|| async move {
                        started.add_permits(1);
                        std::future::pending::<Arc<[Peer]>>().await
                    })
                    .await
            })
        };
        started.acquire().await.unwrap().forget();
        task.abort();
        let _ = task.await;

        let expected: Arc<[Peer]> = vec![Peer {
            id: "node-b".to_string(),
            addr: "127.0.0.1:9091".parse().unwrap(),
        }]
        .into();
        let peers = tokio::time::timeout(
            Duration::from_secs(1),
            cache.get_or_load(|| std::future::ready(expected.clone())),
        )
        .await
        .expect("cancelled loader must release the cache");
        assert!(Arc::ptr_eq(&peers, &expected));
    }

    struct StaticPeerDirectory {
        peers: Vec<Peer>,
    }

    impl PeerDirectory for StaticPeerDirectory {
        fn peers(&self) -> PeerFuture<'_> {
            Box::pin(std::future::ready(self.peers.clone().into()))
        }
    }

    #[derive(Default)]
    struct RecordingTransport {
        calls: Mutex<Vec<(String, String)>>,
        retry_peers: BTreeSet<String>,
        not_found_peers: BTreeSet<String>,
        provider_not_found_peers: BTreeSet<String>,
        provider_gone_peers: BTreeSet<String>,
        fatal_peers: BTreeSet<String>,
    }

    struct BlockingTransport {
        started: Barrier,
        release: Semaphore,
    }

    impl BlockingTransport {
        fn new() -> Self {
            Self {
                started: Barrier::new(3),
                release: Semaphore::new(0),
            }
        }
    }

    impl InternalTransport for BlockingTransport {
        fn fetch<'a>(&'a self, _peer: &'a Peer, _path: &'a str) -> FetchFuture<'a> {
            Box::pin(async move {
                self.started.wait().await;
                self.release
                    .acquire()
                    .await
                    .expect("release semaphore closed")
                    .forget();
                Ok(InternalFetchResponse::tile(
                    Bytes::from_static(b"peer response"),
                    InternalTileSource::Cache,
                ))
            })
        }
    }

    impl InternalTransport for RecordingTransport {
        fn fetch<'a>(&'a self, peer: &'a Peer, path: &'a str) -> FetchFuture<'a> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("calls lock")
                    .push((peer.id.clone(), path.to_string()));
                if self.retry_peers.contains(&peer.id) {
                    return Err(PeerFetchError::Retryable("injected failure".into()));
                }
                if self.not_found_peers.contains(&peer.id) {
                    return Err(PeerFetchError::NotFound);
                }
                if self.provider_not_found_peers.contains(&peer.id) {
                    return Err(PeerFetchError::ProviderNotFound);
                }
                if self.provider_gone_peers.contains(&peer.id) {
                    return Err(PeerFetchError::ProviderGone);
                }
                if self.fatal_peers.contains(&peer.id) {
                    return Err(PeerFetchError::Fatal("injected failure".into()));
                }
                Ok(InternalFetchResponse::tile(
                    Bytes::from_static(b"peer response"),
                    InternalTileSource::Cache,
                ))
            })
        }
    }

    fn peer(id: &str, port: u16) -> Peer {
        Peer {
            id: id.to_string(),
            addr: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    #[test]
    fn provider_requests_preserve_placement_and_wire_contracts_without_debugging_urls() {
        let style = ProviderRequest::style(
            "carto/voyager light",
            "https://user:secret@styles.example/voyager/style.json?token=secret",
        );
        assert_eq!(
            style.placement_key(),
            "style:https://user:secret@styles.example/voyager/style.json?token=secret"
        );
        assert_eq!(
            style.internal_path(),
            "/_internal/provider/styles/carto/voyager%20light/style.json"
        );
        assert_eq!(style.logical_identity().to_string(), "carto/voyager light");
        assert!(!format!("{style:?}").contains("styles.example"));

        let glyph = ProviderRequest::glyph(
            "Noto Sans JP,Arial",
            "0-255",
            "https://glyphs.example/Noto%20Sans%20JP,Arial/0-255.pbf",
        );
        assert_eq!(
            glyph.placement_key(),
            "glyph:https://glyphs.example/Noto%20Sans%20JP,Arial/0-255.pbf"
        );
        assert_eq!(
            glyph.internal_path(),
            "/_internal/provider/fonts/Noto%20Sans%20JP,Arial/0-255.pbf"
        );
        assert_eq!(
            glyph.logical_identity().to_string(),
            "Noto Sans JP,Arial/0-255"
        );

        let sprite = ProviderRequest::sprite(
            "carto/voyager",
            ProviderSpriteVariant::Png2x,
            "s3://bucket/sprites/voyager@2x.png",
        );
        assert_eq!(
            sprite.placement_key(),
            "sprite:@2x.png:s3://bucket/sprites/voyager@2x.png"
        );
        assert_eq!(
            sprite.internal_path(),
            "/_internal/provider/styles/carto/voyager/sprite@2x.png"
        );
        assert_eq!(
            sprite.logical_identity().to_string(),
            "carto/voyager/sprite@2x.png"
        );
    }

    #[test]
    fn classifies_internal_resource_paths_with_bounded_labels() {
        assert_eq!(
            internal_resource_kind("/_internal/tiles/demo%2Fterrain/42"),
            Some("tile")
        );
        assert_eq!(
            internal_resource_kind("/_internal/pmtiles/demo/bootstrap?metadata=true"),
            Some("bootstrap")
        );
        assert_eq!(
            internal_resource_kind("/_internal/pmtiles/demo/leaf/128/256"),
            Some("leaf")
        );
        assert_eq!(
            internal_resource_kind("/_internal/provider/styles/base/sprite@2x.png"),
            Some("sprite")
        );
        assert_eq!(
            internal_resource_kind("/_internal/derived/mapterhorn%2Fplanet/hillshade/8/226/100"),
            Some("derived")
        );
        assert_eq!(internal_resource_kind("/_internal/metrics"), None);
    }

    #[test]
    fn internal_response_limits_are_bounded_by_resource_kind() {
        assert_eq!(
            internal_response_body_limit("/_internal/provider/fonts/Test/0-255.pbf"),
            1024 * 1024
        );
        assert_eq!(
            internal_response_body_limit("/_internal/provider/styles/demo/style.json"),
            2 * 1024 * 1024
        );
        assert_eq!(
            internal_response_body_limit("/_internal/provider/styles/demo/sprite.png"),
            8 * 1024 * 1024
        );
        assert_eq!(
            internal_response_body_limit("/_internal/tiles/demo/42"),
            64 * 1024 * 1024
        );
        assert_eq!(
            internal_response_body_limit("/_internal/pmtiles/demo/bootstrap?metadata=true"),
            std::mem::size_of::<u64>() + MIN_BOOTSTRAP_BYTES + DEFAULT_MAX_DECOMPRESSED_BYTES
        );
        assert_eq!(
            internal_response_body_limit("/_internal/unknown"),
            64 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn injected_directory_drives_production_hrw_routing() {
        let peers = vec![peer("node-a", 8001), peer("node-b", 8002)];
        let router = HrwRouter::new(2, 512);
        let expected = router.route_tile(&peers, "demo/terrain", 700);
        let backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticPeerDirectory { peers }),
            router,
            Arc::new(RecordingTransport::default()),
            NodeMetrics::new(),
        );

        let actual = backend.route_tile(&tileset_id("demo/terrain"), 700).await;

        assert_eq!(
            actual
                .iter()
                .map(|candidate| &candidate.peer.id)
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|candidate| &candidate.peer.id)
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn typed_resource_uses_tile_group_hrw_owner() {
        let peers = vec![peer("node-a", 8001), peer("node-b", 8002)];
        let router = HrwRouter::new(2, 512);
        let routing_key = derived_routing_key();
        let tile_id = 700;
        let expected_owner = router.route_tile(&peers, routing_key.as_ref(), tile_id)[0]
            .peer
            .id
            .clone();
        let transport = Arc::new(RecordingTransport::default());
        let metrics = NodeMetrics::new();
        let backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticPeerDirectory { peers }),
            router,
            transport.clone(),
            metrics.clone(),
        );
        let path = "/_internal/derived/mapterhorn%2Fplanet/hillshade/8/226/100";

        let bytes = backend
            .route_derived_resource(&routing_key, tile_id, path)
            .await
            .expect("route")
            .expect("peer body");

        assert_eq!(bytes, Bytes::from_static(b"peer response"));
        assert_eq!(
            *transport.calls.lock().expect("calls lock"),
            vec![(expected_owner, path.to_string())]
        );
        assert!(
            metrics
                .encode()
                .contains("ishikari_peer_fetch_total{outcome=\"success\",resource=\"derived\"} 1")
        );
    }

    #[tokio::test]
    async fn missing_typed_internal_route_falls_back_local() {
        let target = peer("old-node", 8001);
        let transport = Arc::new(RecordingTransport {
            not_found_peers: BTreeSet::from([target.id.clone()]),
            ..RecordingTransport::default()
        });
        let backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticPeerDirectory {
                peers: vec![target],
            }),
            HrwRouter::new(1, 512),
            transport,
            NodeMetrics::new(),
        );

        let routed = backend
            .route_derived_resource(
                &derived_routing_key(),
                700,
                "/_internal/derived/mapterhorn%2Fplanet/hillshade/8/226/100",
            )
            .await
            .expect("route");

        assert_eq!(routed, None);
    }

    #[tokio::test]
    async fn provider_not_found_and_gone_are_authoritative() {
        for (target_id, expected, metric_outcome) in [
            (
                "not-found-owner",
                ProviderRouteOutcome::NotFound,
                "provider_not_found",
            ),
            ("gone-owner", ProviderRouteOutcome::Gone, "provider_gone"),
        ] {
            let target = peer(target_id, 8001);
            let transport = Arc::new(RecordingTransport {
                provider_not_found_peers: BTreeSet::from_iter(
                    (target_id == "not-found-owner").then(|| target.id.clone()),
                ),
                provider_gone_peers: BTreeSet::from_iter(
                    (target_id == "gone-owner").then(|| target.id.clone()),
                ),
                ..RecordingTransport::default()
            });
            let metrics = NodeMetrics::new();
            let backend = PeerBackend::with_dependencies(
                "entry".to_string(),
                Arc::new(StaticPeerDirectory {
                    peers: vec![target],
                }),
                HrwRouter::new(1, 512),
                transport.clone(),
                metrics.clone(),
            );

            let request =
                ProviderRequest::style("missing", "https://example.test/missing/style.json");
            let outcome = backend
                .route_provider_request(&request)
                .await
                .expect("provider route")
                .expect("authoritative peer outcome");

            assert!(matches!(
                (&outcome, &expected),
                (
                    ProviderRouteOutcome::NotFound,
                    ProviderRouteOutcome::NotFound
                ) | (ProviderRouteOutcome::Gone, ProviderRouteOutcome::Gone)
            ));
            assert_eq!(transport.calls.lock().expect("calls lock").len(), 1);
            assert!(metrics.encode().contains(&format!(
                "ishikari_peer_fetch_total{{outcome=\"{metric_outcome}\",resource=\"style\"}} 1"
            )));
        }
    }

    #[tokio::test]
    async fn unmarked_provider_not_found_and_gone_fall_back_local() {
        for (target_id, is_not_found) in [("unmarked-404", true), ("unmarked-410", false)] {
            let target = peer(target_id, 8001);
            let transport = Arc::new(RecordingTransport {
                not_found_peers: BTreeSet::from_iter(is_not_found.then(|| target.id.clone())),
                fatal_peers: BTreeSet::from_iter((!is_not_found).then(|| target.id.clone())),
                ..RecordingTransport::default()
            });
            let backend = PeerBackend::with_dependencies(
                "entry".to_string(),
                Arc::new(StaticPeerDirectory {
                    peers: vec![target],
                }),
                HrwRouter::new(1, 512),
                transport.clone(),
                NodeMetrics::new(),
            );

            let request =
                ProviderRequest::style("missing", "https://example.test/missing/style.json");
            assert!(
                backend
                    .route_provider_request(&request)
                    .await
                    .expect("provider route")
                    .is_none()
            );
            assert_eq!(transport.calls.lock().expect("calls lock").len(), 1);
        }
    }

    #[tokio::test]
    async fn provider_retryable_failures_try_candidates_then_fall_back_local() {
        let peers = vec![peer("node-a", 8001), peer("node-b", 8002)];
        let request = ProviderRequest::style("retry", "https://example.test/retry/style.json");
        let key = request.placement_key();
        let path = request.internal_path();
        let router = HrwRouter::new(2, 512);
        let routed = router.route_key(&peers, &key);
        let first_peer = routed[0].peer.id.clone();
        let transport = Arc::new(RecordingTransport {
            retry_peers: BTreeSet::from([first_peer]),
            ..RecordingTransport::default()
        });
        let backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticPeerDirectory {
                peers: peers.clone(),
            }),
            router,
            transport.clone(),
            NodeMetrics::new(),
        );

        let outcome = backend
            .route_provider_request(&request)
            .await
            .expect("provider route")
            .expect("second candidate response");
        assert!(matches!(outcome, ProviderRouteOutcome::Resource(_)));
        {
            let calls = transport.calls.lock().expect("calls lock");
            assert_eq!(
                calls
                    .iter()
                    .map(|(peer, _)| peer.as_str())
                    .collect::<Vec<_>>(),
                routed
                    .iter()
                    .map(|candidate| candidate.peer.id.as_str())
                    .collect::<Vec<_>>()
            );
            assert!(calls.iter().all(|(_, called_path)| called_path == &path));
        }

        let retry_all = Arc::new(RecordingTransport {
            retry_peers: peers.iter().map(|peer| peer.id.clone()).collect(),
            ..RecordingTransport::default()
        });
        let backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticPeerDirectory { peers }),
            HrwRouter::new(2, 512),
            retry_all.clone(),
            NodeMetrics::new(),
        );

        assert!(
            backend
                .route_provider_request(&request)
                .await
                .expect("provider route")
                .is_none(),
            "exhausted retryable candidates must retain local fallback"
        );
        assert_eq!(retry_all.calls.lock().expect("calls lock").len(), 2);
    }

    #[tokio::test]
    async fn unmarked_gone_non_provider_internal_route_still_falls_back_local() {
        let target = peer("old-node", 8001);
        let transport = Arc::new(RecordingTransport {
            fatal_peers: BTreeSet::from([target.id.clone()]),
            ..RecordingTransport::default()
        });
        let backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticPeerDirectory {
                peers: vec![target],
            }),
            HrwRouter::new(1, 512),
            transport,
            NodeMetrics::new(),
        );

        let routed = backend
            .route_derived_resource(
                &derived_routing_key(),
                700,
                "/_internal/derived/mapterhorn%2Fplanet/hillshade/8/226/100",
            )
            .await
            .expect("route");

        assert_eq!(routed, None);
    }

    #[tokio::test]
    async fn injected_transport_receives_encoded_internal_tile_path() {
        let transport = Arc::new(RecordingTransport::default());
        let backend = PeerBackend::with_dependencies(
            "node-a".to_string(),
            Arc::new(StaticPeerDirectory { peers: Vec::new() }),
            HrwRouter::new(1, 512),
            transport.clone(),
            NodeMetrics::new(),
        );

        let bytes = backend
            .fetch_tile_bytes(&peer("node-b", 8002), &tileset_id("demo/terrain"), 42)
            .await
            .expect("peer fetch");

        assert_eq!(bytes.bytes, Bytes::from_static(b"peer response"));
        assert_eq!(bytes.tile_source, Some(InternalTileSource::Cache));
        assert_eq!(
            *transport.calls.lock().expect("calls lock"),
            vec![(
                "node-b".to_string(),
                "/_internal/tiles/demo%2Fterrain/42".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn identical_concurrent_peer_fetches_are_measured_and_cleaned_up() {
        let transport = Arc::new(BlockingTransport::new());
        let metrics = NodeMetrics::new();
        let backend = PeerBackend::with_dependencies(
            "node-a".to_string(),
            Arc::new(StaticPeerDirectory { peers: Vec::new() }),
            HrwRouter::new(1, 512),
            transport.clone(),
            metrics.clone(),
        );
        let target = peer("node-b", 8002);
        let tileset = tileset_id("demo/terrain");

        let first = tokio::spawn({
            let backend = backend.clone();
            let target = target.clone();
            let tileset = tileset.clone();
            async move { backend.fetch_tile_bytes(&target, &tileset, 42).await }
        });
        let second = tokio::spawn({
            let backend = backend.clone();
            let target = target.clone();
            let tileset = tileset.clone();
            async move { backend.fetch_tile_bytes(&target, &tileset, 42).await }
        });

        transport.started.wait().await;
        assert_eq!(metrics.snapshot().peer_tile_duplicate_inflight, 1);
        assert_eq!(
            backend
                .inflight_fetches
                .lock()
                .expect("inflight fetch mutex poisoned")
                .values()
                .copied()
                .collect::<Vec<_>>(),
            vec![2]
        );

        transport.release.add_permits(2);
        first.await.expect("first task").expect("first fetch");
        second.await.expect("second task").expect("second fetch");
        assert!(
            backend
                .inflight_fetches
                .lock()
                .expect("inflight fetch mutex poisoned")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn backoff_metric_counts_only_suppressed_hrw_candidates() {
        let peers = vec![
            peer("node-a", 8001),
            peer("node-b", 8002),
            peer("node-c", 8003),
        ];
        let router = HrwRouter::new(1, 512);
        let request = ProviderRequest::style("base", "https://example.test/base.json");
        let preferred = router.route_key(&peers, &request.placement_key())[0]
            .peer
            .id
            .clone();
        let non_candidate = peers
            .iter()
            .find(|peer| peer.id != preferred)
            .expect("non-candidate peer")
            .id
            .clone();
        let metrics = NodeMetrics::new();
        let backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticPeerDirectory {
                peers: peers.clone(),
            }),
            router,
            Arc::new(RecordingTransport::default()),
            metrics.clone(),
        );

        {
            let mut failures = backend
                .retryable_failures
                .lock()
                .expect("retryable failures lock");
            failures
                .entry(non_candidate)
                .or_default()
                .insert("style", tokio::time::Instant::now() + PEER_RETRY_BACKOFF);
        }
        let routed = backend.route_provider_for(&request).await;
        assert_eq!(routed[0].peer.id, preferred);
        assert_eq!(metrics.snapshot().peer_forward_backoff_skips, 0);

        {
            let mut failures = backend
                .retryable_failures
                .lock()
                .expect("retryable failures lock");
            failures
                .entry(preferred.clone())
                .or_default()
                .insert("style", tokio::time::Instant::now() + PEER_RETRY_BACKOFF);
        }
        let routed = backend.route_provider_for(&request).await;
        assert_ne!(routed[0].peer.id, preferred);
        assert_eq!(metrics.snapshot().peer_forward_backoff_skips, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_transport_failure_backs_off_only_the_failed_resource_kind() {
        let peers = vec![peer("node-a", 8001), peer("node-b", 8002)];
        let router = HrwRouter::new(2, 512);
        let routed = router.route_tileset(&peers, "demo/terrain");
        let first_peer = routed[0].peer.id.clone();
        let transport = Arc::new(RecordingTransport {
            retry_peers: BTreeSet::from([first_peer.clone()]),
            ..RecordingTransport::default()
        });
        let metrics = NodeMetrics::new();
        let backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticPeerDirectory { peers }),
            router,
            transport.clone(),
            metrics.clone(),
        );

        let result = backend
            .route_leaf(&tileset_id("demo/terrain"), 128, 256)
            .await
            .expect("routed leaf");

        assert_eq!(result, Some(Bytes::from_static(b"peer response")));
        {
            let calls = transport.calls.lock().expect("calls lock");
            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0].0, routed[0].peer.id);
            assert_eq!(calls[1].0, routed[1].peer.id);
            assert!(
                calls
                    .iter()
                    .all(|(_, path)| path == "/_internal/pmtiles/demo%2Fterrain/leaf/128/256")
            );
        }

        let during_backoff = backend
            .route_tileset_for(&tileset_id("demo/terrain"), "leaf")
            .await;
        assert!(
            during_backoff
                .iter()
                .all(|candidate| candidate.peer.id != first_peer)
        );

        let unrelated_tiles = backend.route_tile(&tileset_id("demo/terrain"), 700).await;
        assert!(
            unrelated_tiles
                .iter()
                .any(|candidate| candidate.peer.id == first_peer)
        );

        tokio::time::advance(PEER_RETRY_BACKOFF).await;
        let after_backoff = backend
            .route_tileset_for(&tileset_id("demo/terrain"), "leaf")
            .await;
        assert!(
            after_backoff
                .iter()
                .any(|candidate| candidate.peer.id == first_peer)
        );

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.peer_forward_retryable, 1);
        assert_eq!(snapshot.peer_forward_successes, 1);
        assert_eq!(snapshot.peer_forward_backoff_skips, 1);
        let encoded = metrics.encode();
        assert!(
            encoded
                .contains("ishikari_peer_fetch_total{outcome=\"retryable\",resource=\"leaf\"} 1")
        );
        assert!(
            encoded.contains("ishikari_peer_fetch_total{outcome=\"success\",resource=\"leaf\"} 1")
        );
    }
}
