//! Peer routing and internal HTTP transport.

use std::{future::Future, pin::Pin, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use reqwest::{Client, StatusCode};
use thiserror::Error;
use tracing::{debug, warn};

use crate::{
    interned::TilesetId,
    membership::{Membership, Peer},
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
}

pub type PeerFuture<'a> = Pin<Box<dyn Future<Output = Arc<[Peer]>> + Send + 'a>>;
pub type FetchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<InternalFetchResponse, PeerFetchError>> + Send + 'a>>;

pub(crate) const TILE_SOURCE_HEADER: &str = "x-ishikari-tile-source";

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
}

impl InternalFetchResponse {
    #[cfg(feature = "simulator-support")]
    pub fn bytes(bytes: Bytes) -> Self {
        Self {
            bytes,
            tile_source: None,
        }
    }

    #[cfg(any(test, feature = "simulator-support"))]
    pub fn tile(bytes: Bytes, source: InternalTileSource) -> Self {
        Self {
            bytes,
            tile_source: Some(source),
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
            let mut request = self.http_client.get(url);
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
            let bytes = response
                .bytes()
                .await
                .map_err(|error| PeerFetchError::Fatal(error.to_string()))?;
            Ok(InternalFetchResponse { bytes, tile_source })
        })
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
    ) -> Self {
        Self::with_dependencies(
            self_node_id,
            Arc::new(MembershipPeerDirectory { membership }),
            router,
            Arc::new(HttpInternalTransport { http_client }),
        )
    }

    /// Creates a peer backend with injected discovery and transport implementations.
    pub fn with_dependencies(
        self_node_id: String,
        peer_directory: Arc<dyn PeerDirectory>,
        router: HrwRouter,
        transport: Arc<dyn InternalTransport>,
    ) -> Self {
        Self {
            self_node_id,
            peer_directory,
            router,
            transport,
        }
    }

    /// Returns the routed candidate peers for a tileset.
    pub async fn route_tileset(&self, tileset_id: &TilesetId) -> Vec<ScoredPeer> {
        let peers = self.peer_directory.peers().await;
        self.router.route_tileset(&peers, tileset_id.as_ref())
    }

    /// Returns the routed candidate peers for a tile request.
    pub async fn route_tile(&self, tileset_id: &TilesetId, tile_id: u64) -> Vec<ScoredPeer> {
        let peers = self.peer_directory.peers().await;
        self.router.route_tile(&peers, tileset_id.as_ref(), tile_id)
    }

    /// Returns the routed candidate peers for an arbitrary provider-resource key.
    pub async fn route_key(&self, key: &str) -> Vec<ScoredPeer> {
        let peers = self.peer_directory.peers().await;
        self.router.route_key(&peers, key)
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
        self.transport.fetch(peer, &path).await
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
    ) -> Result<Option<Bytes>> {
        let candidates = self.route_key(key).await;

        if candidates.is_empty()
            || candidates
                .first()
                .is_some_and(|peer| self.is_self(&peer.peer))
        {
            debug!(key = key, kind = kind, "using local provider read");
            return Ok(None);
        }

        for peer in candidates {
            if self.is_self(&peer.peer) {
                debug!(
                    key = key,
                    peer_id = %peer.peer.id,
                    kind = kind,
                    "reached local node; falling back local"
                );
                return Ok(None);
            }

            debug!(
                key = key,
                peer_id = %peer.peer.id,
                kind = kind,
                "forwarding provider request to peer"
            );
            match self.transport.fetch(&peer.peer, path).await {
                Ok(response) => {
                    debug!(
                        key = key,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        body_len = response.bytes.len(),
                        "received provider bytes from peer"
                    );
                    return Ok(Some(response.bytes));
                }
                Err(PeerFetchError::NotFound) => {
                    debug!(
                        key = key,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        "peer reported missing provider resource"
                    );
                    return Ok(None);
                }
                Err(error) if error.is_retryable() => {
                    warn!(
                        key = key,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        error = %error,
                        "provider forward failed; trying next candidate"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(
                        key = key,
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
            key = key,
            kind = kind,
            "all provider forwards failed; falling back local"
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
        let candidates = self.route_tileset(tileset_id).await;

        if candidates.is_empty()
            || candidates
                .first()
                .is_some_and(|peer| self.is_self(&peer.peer))
        {
            debug!(tileset_id = %tileset_id, kind = kind, "using local read");
            return Ok(None);
        }

        for peer in candidates {
            if self.is_self(&peer.peer) {
                debug!(
                    tileset_id = %tileset_id,
                    peer_id = %peer.peer.id,
                    kind = kind,
                    "reached local node; falling back local"
                );
                return Ok(None);
            }

            debug!(
                tileset_id = %tileset_id,
                peer_id = %peer.peer.id,
                kind = kind,
                "forwarding request to peer"
            );
            match self.transport.fetch(&peer.peer, path).await {
                Ok(response) => {
                    debug!(
                        tileset_id = %tileset_id,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        body_len = response.bytes.len(),
                        "received bytes from peer"
                    );
                    return Ok(Some(response.bytes));
                }
                Err(PeerFetchError::NotFound) => {
                    debug!(
                        tileset_id = %tileset_id,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        "peer reported missing"
                    );
                    return Ok(None);
                }
                Err(error) if error.is_retryable() => {
                    warn!(
                        tileset_id = %tileset_id,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        error = %error,
                        "forward failed; trying next candidate"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(
                        tileset_id = %tileset_id,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        error = %error,
                        "forward failed; falling back local"
                    );
                    return Ok(None);
                }
            }
        }

        debug!(
            tileset_id = %tileset_id,
            kind = kind,
            "all forwards failed; falling back local"
        );
        Ok(None)
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

    use super::{
        FetchFuture, InternalFetchResponse, InternalTileSource, InternalTransport, PeerBackend,
        PeerDirectory, PeerFetchError, PeerFuture,
    };
    use crate::{interned::TilesetId, membership::Peer, storage::routing::HrwRouter};

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
    async fn injected_transport_receives_encoded_internal_tile_path() {
        let transport = Arc::new(RecordingTransport::default());
        let backend = PeerBackend::with_dependencies(
            "node-a".to_string(),
            Arc::new(StaticPeerDirectory { peers: Vec::new() }),
            HrwRouter::new(1, 512),
            transport.clone(),
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
    async fn retryable_transport_failure_uses_next_hrw_candidate() {
        let peers = vec![peer("node-a", 8001), peer("node-b", 8002)];
        let router = HrwRouter::new(2, 512);
        let routed = router.route_tileset(&peers, "demo/terrain");
        let first_peer = routed[0].peer.id.clone();
        let transport = Arc::new(RecordingTransport {
            calls: Mutex::new(Vec::new()),
            retry_peers: BTreeSet::from([first_peer]),
        });
        let backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticPeerDirectory { peers }),
            router,
            transport.clone(),
        );

        let result = backend
            .route_leaf(&TilesetId::new_unchecked("demo/terrain"), 128, 256)
            .await
            .expect("routed leaf");

        assert_eq!(result, Some(Bytes::from_static(b"peer response")));
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
}
