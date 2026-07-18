//! Peer routing and internal HTTP transport.

use std::{
    borrow::Cow,
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use bytes::Bytes;
use reqwest::{Client, StatusCode, header};
use thiserror::Error;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::{
    interned::TilesetId,
    membership::{Membership, Peer},
    metrics::NodeMetrics,
    pmtiles::BootstrapTransfer,
};

use super::routing::{HrwRouter, ScoredPeer};

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
const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Provider owners may spend the full 15-second upstream deadline before
/// returning metadata and bytes. Leave transport overhead so the requester
/// does not retry another owner while the first is still completing.
const PROVIDER_PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const DERIVED_PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub type PeerFuture<'a> = Pin<Box<dyn Future<Output = Arc<[Peer]>> + Send + 'a>>;
pub type FetchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<InternalFetchResponse, PeerFetchError>> + Send + 'a>>;

pub(crate) const TILE_SOURCE_HEADER: &str = "x-ishikari-tile-source";
pub(crate) const PROVIDER_CACHE_CONTROL_HEADER: &str = "x-ishikari-provider-cache-control";
pub(crate) const PROVIDER_AGE_HEADER: &str = "x-ishikari-provider-age";
pub(crate) const PROVIDER_ETAG_HEADER: &str = "x-ishikari-provider-etag";
pub(crate) const PROVIDER_LAST_MODIFIED_HEADER: &str = "x-ishikari-provider-last-modified";

/// Tile provenance reported by the node that resolved an internal request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InternalTileSource {
    Cache,
    Backend,
}

impl InternalTileSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::Backend => "backend",
        }
    }

    fn parse(value: &str) -> Option<Self> {
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

impl InternalFetchResponse {
    #[cfg(feature = "simulator-support")]
    pub fn bytes(bytes: Bytes) -> Self {
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
    pub fn tile(bytes: Bytes, source: InternalTileSource) -> Self {
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

#[derive(Clone)]
struct MembershipPeerDirectory {
    membership: Membership,
}

impl PeerDirectory for MembershipPeerDirectory {
    fn peers(&self) -> PeerFuture<'_> {
        Box::pin(self.membership.peers())
    }
}

#[derive(Clone)]
struct HttpInternalTransport {
    http_client: Client,
}

impl InternalTransport for HttpInternalTransport {
    fn fetch<'a>(&'a self, peer: &'a Peer, path: &'a str) -> FetchFuture<'a> {
        Box::pin(async move {
            let url = format!("http://{}{}", peer.addr, path);
            let mut request = self
                .http_client
                .get(url)
                .timeout(peer_request_timeout(path));
            if let Some(id) = crate::request_id::current() {
                request = request.header(crate::request_id::HEADER, id);
            }
            let response = request.send().await.map_err(|error| {
                if error.is_connect() || error.is_timeout() {
                    PeerFetchError::Retryable(error.to_string())
                } else {
                    PeerFetchError::Fatal(error.to_string())
                }
            })?;

            let status = response.status();
            if status == StatusCode::NOT_FOUND {
                return Err(PeerFetchError::NotFound);
            }
            if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                return Err(PeerFetchError::Retryable(format!("peer returned {status}")));
            }
            if !status.is_success() {
                return Err(PeerFetchError::Fatal(format!(
                    "peer returned unexpected status {status}"
                )));
            }

            let tile_source = response
                .headers()
                .get(TILE_SOURCE_HEADER)
                .and_then(|value| value.to_str().ok())
                .and_then(InternalTileSource::parse);
            let provider_cache_control = response
                .headers()
                .get(PROVIDER_CACHE_CONTROL_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let provider_age_seconds = response
                .headers()
                .get(PROVIDER_AGE_HEADER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok());
            let provider_etag = response
                .headers()
                .get(PROVIDER_ETAG_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let provider_last_modified = response
                .headers()
                .get(PROVIDER_LAST_MODIFIED_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let content_encoding = response
                .headers()
                .get(header::CONTENT_ENCODING)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let bytes = response
                .bytes()
                .await
                .map_err(|error| PeerFetchError::Fatal(error.to_string()))?;
            Ok(InternalFetchResponse {
                bytes,
                tile_source,
                provider_cache_control,
                provider_age_seconds,
                provider_etag,
                provider_last_modified,
                content_encoding,
            })
        })
    }
}

fn peer_request_timeout(path: &str) -> Duration {
    if path.starts_with("/_internal/derived/") {
        DERIVED_PEER_REQUEST_TIMEOUT
    } else if path.starts_with("/_internal/provider/") {
        PROVIDER_PEER_REQUEST_TIMEOUT
    } else {
        PEER_REQUEST_TIMEOUT
    }
}

/// Errors returned while fetching internal resources from a peer.
#[derive(Debug, Error)]
pub enum PeerFetchError {
    #[error("peer resource not found")]
    NotFound,
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
    /// Creates the peer backend used for internal forwarding.
    pub fn new(
        self_node_id: String,
        membership: Membership,
        router: HrwRouter,
        http_client: Client,
        metrics: NodeMetrics,
    ) -> Self {
        Self::with_dependencies(
            self_node_id,
            Arc::new(MembershipPeerDirectory { membership }),
            router,
            Arc::new(HttpInternalTransport { http_client }),
            metrics,
        )
    }

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
    pub async fn route_tile(&self, tileset_id: &TilesetId, tile_id: u64) -> Vec<ScoredPeer> {
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

    async fn route_key_for(&self, key: &str, kind: &str) -> Vec<ScoredPeer> {
        let peers = self.peer_directory.peers().await;
        self.route_with_backoff(&peers, peer_resource_label(kind), |peers| {
            self.router.route_key(peers, key)
        })
    }

    /// Returns whether the given peer is the local node.
    pub fn is_self(&self, peer: &Peer) -> bool {
        peer.id == self.self_node_id
    }

    /// Routes a bootstrap request across candidate peers, returning the first successful result.
    pub async fn route_bootstrap(
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
    pub async fn route_leaf(
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
    pub async fn fetch_tile_bytes(
        &self,
        peer: &Peer,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<InternalFetchResponse, PeerFetchError> {
        let key = encode_tileset_path(tileset_id);
        let path = format!("/_internal/tiles/{key}/{tile_id}");
        self.fetch_from_peer(peer, &path, "tile").await
    }

    /// Routes a provider-resource request across key candidate peers.
    ///
    /// The `path` must name a typed internal endpoint that resolves the upstream
    /// resource from local provider config. It intentionally does not carry a
    /// raw upstream URL, so internal forwarding cannot become an arbitrary URL
    /// fetcher.
    pub async fn route_fetch_optional_by_key(
        &self,
        key: &str,
        path: &str,
        kind: &str,
    ) -> Result<Option<InternalFetchResponse>> {
        let candidates = self.route_key_for(key, kind).await;
        self.route_fetch_optional_response_candidates(candidates, key, path, kind)
            .await
    }

    /// Routes a typed internal resource using the same Hilbert-group HRW
    /// placement as stored tiles. The caller owns the internal wire format;
    /// `None` means local fallback (including an older peer returning 404).
    pub async fn route_fetch_optional_by_tile(
        &self,
        routing_id: &TilesetId,
        tile_id: u64,
        path: &str,
        kind: &str,
    ) -> Result<Option<Bytes>> {
        let candidates = self.route_tile_for(routing_id, tile_id, kind).await;
        Ok(self
            .route_fetch_optional_response_candidates(candidates, routing_id.as_ref(), path, kind)
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
            Err(PeerFetchError::Retryable(_)) => "retryable",
            Err(PeerFetchError::Fatal(_)) => "fatal",
        };
        self.metrics.record_peer_forward(outcome);
        self.metrics.record_peer_fetch(resource, outcome);

        let mut failures = self
            .retryable_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        let mut failures = self
            .retryable_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
            let mut requests = inflight
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        let mut requests = self
            .inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        return Ok(BootstrapTransfer {
            bootstrap: body,
            metadata: None,
        });
    }
    anyhow::ensure!(body.len() >= 8, "bootstrap transfer too short");
    let bootstrap_len = u64::from_le_bytes(body[..8].try_into().unwrap()) as usize;
    anyhow::ensure!(
        body.len() >= 8 + bootstrap_len,
        "bootstrap transfer truncated"
    );
    let bootstrap = body.slice(8..8 + bootstrap_len);
    let metadata = if body.len() > 8 + bootstrap_len {
        Some(body.slice(8 + bootstrap_len..))
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
        sync::{Arc, Mutex},
    };

    use bytes::Bytes;
    use tokio::sync::{Barrier, Semaphore};

    use super::{
        DERIVED_PEER_REQUEST_TIMEOUT, FetchFuture, InternalFetchResponse, InternalTileSource,
        InternalTransport, PEER_REQUEST_TIMEOUT, PEER_RETRY_BACKOFF, PROVIDER_PEER_REQUEST_TIMEOUT,
        PeerBackend, PeerDirectory, PeerFetchError, PeerFuture, internal_resource_kind,
        peer_request_timeout,
    };
    use crate::{
        interned::TilesetId, membership::Peer, metrics::NodeMetrics, storage::routing::HrwRouter,
    };

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
    fn derived_fetches_have_a_longer_peer_timeout() {
        assert_eq!(
            peer_request_timeout("/_internal/derived/mapterhorn%2Fplanet/hillshade/8/226/100"),
            DERIVED_PEER_REQUEST_TIMEOUT
        );
        assert_eq!(
            peer_request_timeout("/_internal/tiles/mierune%2Fomt/700"),
            PEER_REQUEST_TIMEOUT
        );
        assert_eq!(
            peer_request_timeout("/_internal/provider/fonts/Test/0-255.pbf"),
            PROVIDER_PEER_REQUEST_TIMEOUT
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

        let actual = backend
            .route_tile(&TilesetId::new_unchecked("demo/terrain"), 700)
            .await;

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
        let routing_id = TilesetId::new_unchecked("derived:hillshade:mapterhorn/planet");
        let tile_id = 700;
        let expected_owner = router.route_tile(&peers, routing_id.as_ref(), tile_id)[0]
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
            .route_fetch_optional_by_tile(&routing_id, tile_id, path, "derived")
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
            calls: Mutex::new(Vec::new()),
            retry_peers: BTreeSet::new(),
            not_found_peers: BTreeSet::from([target.id.clone()]),
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
            .route_fetch_optional_by_tile(
                &TilesetId::new_unchecked("derived:hillshade:mapterhorn/planet"),
                700,
                "/_internal/derived/mapterhorn%2Fplanet/hillshade/8/226/100",
                "derived",
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
            .fetch_tile_bytes(
                &peer("node-b", 8002),
                &TilesetId::new_unchecked("demo/terrain"),
                42,
            )
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
        let tileset = TilesetId::new_unchecked("demo/terrain");

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
        let key = "style:https://example.test/base.json";
        let preferred = router.route_key(&peers, key)[0].peer.id.clone();
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
        let routed = backend.route_key_for(key, "style").await;
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
        let routed = backend.route_key_for(key, "style").await;
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
            calls: Mutex::new(Vec::new()),
            retry_peers: BTreeSet::from([first_peer.clone()]),
            not_found_peers: BTreeSet::new(),
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
            .route_leaf(&TilesetId::new_unchecked("demo/terrain"), 128, 256)
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
            .route_tileset_for(&TilesetId::new_unchecked("demo/terrain"), "leaf")
            .await;
        assert!(
            during_backoff
                .iter()
                .all(|candidate| candidate.peer.id != first_peer)
        );

        let unrelated_tiles = backend
            .route_tile(&TilesetId::new_unchecked("demo/terrain"), 700)
            .await;
        assert!(
            unrelated_tiles
                .iter()
                .any(|candidate| candidate.peer.id == first_peer)
        );

        tokio::time::advance(PEER_RETRY_BACKOFF).await;
        let after_backoff = backend
            .route_tileset_for(&TilesetId::new_unchecked("demo/terrain"), "leaf")
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
