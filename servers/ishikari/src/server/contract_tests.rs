//! Router-level HTTP contract tests.
//!
//! These exercise the fully assembled public and internal routers (including
//! the shared middleware stack) over a single-node membership with a real
//! local HTTP upstream, so header wiring — public `Cache-Control`/`Age`
//! (default and upstream-derived), internal `x-ishikari-provider-*` metadata,
//! and internal-path non-exposure — is asserted at the same altitude the
//! Gateway sees it.

use std::{
    future::IntoFuture,
    io::{Read, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use bytes::Bytes;
use tower::util::ServiceExt;

use super::{
    AppState, ServerRuntimeConfig, cache, internal_router, public_router, with_common_layers,
    with_public_layers,
};
use crate::provider::ProviderConfig;
use crate::{
    drain::DrainController,
    membership::{Membership, MembershipConfig},
};
use ishikari_core::{
    metrics::NodeMetrics,
    storage::{
        InternalProviderNegative, ObjectStoreRegistry, PROVIDER_AGE_HEADER,
        PROVIDER_CACHE_CONTROL_HEADER, PROVIDER_ETAG_HEADER, PROVIDER_LAST_MODIFIED_HEADER,
        PROVIDER_NEGATIVE_HEADER, Peer, PeerDirectory, PeerFuture, ResolverTuningInput,
        ResourceCacheCapacities, ResourceResolver, ResourceResolverConfig,
    },
};

/// The style fixture's upstream policy and the normalized form Ishikari must
/// emit for it (shared `s-maxage` filled from `max-age`).
const STYLE_NORMALIZED_CACHE_CONTROL: &str =
    "public, max-age=123, s-maxage=123, stale-while-revalidate=60";
/// Upstream validators on the glyph fixture; glyph bytes are served verbatim,
/// so these must pass through to the public response unchanged.
const GLYPH_UPSTREAM_ETAG: &str = "\"glyph-v1\"";
const GLYPH_UPSTREAM_LAST_MODIFIED: &str = "Sat, 01 Feb 2025 10:00:00 GMT";
const PMTILES_HEADER_SIZE: usize = 127;
const PMTILES_FIXTURE_SIZE: usize = 16 * 1024;
const PMTILES_LEAF_BYTES: &[u8] = b"leaf";

/// Writes a minimal PMTiles v3 archive containing one gzip-compressed empty MVT
/// at z0/0/0. Building it in the harness keeps the router contract test
/// self-contained and avoids checking in an opaque binary fixture.
fn write_mvt_pmtiles_fixture(path: &std::path::Path) {
    fn write_varint(mut value: u64, output: &mut Vec<u8>) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&[]).expect("encode empty MVT");
    let tile = encoder.finish().expect("finish empty MVT");

    // One root entry, encoded column-wise: count, tile-id delta, run length,
    // byte length, and offset+1. It covers tile id 0 and points at data offset 0.
    let mut root = Vec::new();
    for value in [1, 0, 1, tile.len() as u64, 1] {
        write_varint(value, &mut root);
    }

    let root_offset = PMTILES_HEADER_SIZE as u64;
    let leaf_offset = root_offset + root.len() as u64;
    let data_offset = leaf_offset + PMTILES_LEAF_BYTES.len() as u64;
    let mut archive = vec![0; PMTILES_FIXTURE_SIZE];
    archive[..7].copy_from_slice(b"PMTiles");
    archive[7] = 3;
    archive[8..16].copy_from_slice(&root_offset.to_le_bytes());
    archive[16..24].copy_from_slice(&(root.len() as u64).to_le_bytes());
    archive[24..32].copy_from_slice(&leaf_offset.to_le_bytes());
    archive[32..40].copy_from_slice(&0_u64.to_le_bytes());
    archive[40..48].copy_from_slice(&leaf_offset.to_le_bytes());
    archive[48..56].copy_from_slice(&(PMTILES_LEAF_BYTES.len() as u64).to_le_bytes());
    archive[56..64].copy_from_slice(&data_offset.to_le_bytes());
    archive[64..72].copy_from_slice(&(PMTILES_FIXTURE_SIZE as u64 - data_offset).to_le_bytes());
    for range in [72..80, 80..88, 88..96] {
        archive[range].copy_from_slice(&1_u64.to_le_bytes());
    }
    archive[96] = 1; // clustered
    archive[97] = 1; // internal compression: none
    archive[98] = 2; // tile compression: gzip
    archive[99] = 1; // tile type: MVT
    archive[100] = 0; // min zoom
    archive[101] = 0; // max zoom
    archive[PMTILES_HEADER_SIZE..PMTILES_HEADER_SIZE + root.len()].copy_from_slice(&root);
    archive[leaf_offset as usize..data_offset as usize].copy_from_slice(PMTILES_LEAF_BYTES);
    archive[data_offset as usize..data_offset as usize + tile.len()].copy_from_slice(&tile);

    std::fs::write(path, archive).expect("write PMTiles fixture");
}

/// Serves the provider fixtures over real HTTP. The style carries an explicit
/// upstream `Cache-Control`; glyphs and sprites carry none, so they must fall
/// back to the resource defaults.
async fn spawn_upstream() -> (
    SocketAddr,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let uncached_requests = Arc::new(AtomicUsize::new(0));
    let invalid_requests = Arc::new(AtomicUsize::new(0));
    let aged_requests = Arc::new(AtomicUsize::new(0));
    let revalidated_requests = Arc::new(AtomicUsize::new(0));
    let fallback_requests = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route(
            "/styles/base/style.json",
            get(|| async {
                let mut headers = HeaderMap::new();
                headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
                // Repeated fields must be combined before policy parsing.
                headers.append(
                    header::CACHE_CONTROL,
                    "public, max-age=123".parse().unwrap(),
                );
                headers.append(
                    header::CACHE_CONTROL,
                    "stale-while-revalidate=60".parse().unwrap(),
                );
                headers.insert(header::AGE, "10".parse().unwrap());
                headers.insert(header::CONTENT_ENCODING, "gzip".parse().unwrap());
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder
                    .write_all(
                        br#"{"version":8,"sources":{"base":{"type":"vector","url":"/fixture"}},"layers":[]}"#,
                    )
                    .unwrap();
                (headers, encoder.finish().unwrap())
            }),
        )
        .route(
            "/styles/revalidated/style.json",
            get({
                let requests = Arc::clone(&revalidated_requests);
                move |headers: HeaderMap| {
                    let requests = Arc::clone(&requests);
                    async move {
                        let request_index = requests.fetch_add(1, Ordering::Relaxed);
                        if request_index == 0 {
                            return (
                                StatusCode::OK,
                                [
                                    (header::CONTENT_TYPE, "application/json"),
                                    (
                                        header::CACHE_CONTROL,
                                        "public, max-age=0, stale-while-revalidate=60",
                                    ),
                                    (header::ETAG, "\"revalidated-v1\""),
                                ],
                                r#"{"version":8,"name":"revalidated","sources":{},"layers":[]}"#,
                            )
                                .into_response();
                        }
                        if headers
                            .get(header::IF_NONE_MATCH)
                            .is_some_and(|value| value == "\"revalidated-v1\"")
                        {
                            return (
                                StatusCode::NOT_MODIFIED,
                                [
                                    (
                                        header::CACHE_CONTROL,
                                        "public, max-age=120, stale-while-revalidate=60",
                                    ),
                                    (header::ETAG, "\"revalidated-v1\""),
                                ],
                            )
                                .into_response();
                        }
                        StatusCode::PRECONDITION_FAILED.into_response()
                    }
                }
            }),
        )
        .route(
            "/regional/base/style.json",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    r#"{"version":8,"name":"regional","sources":{},"layers":[]}"#,
                )
            }),
        )
        .route(
            "/styles/uncached/style.json",
            get({
                let requests = Arc::clone(&uncached_requests);
                move || {
                    let requests = Arc::clone(&requests);
                    async move {
                        requests.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        (
                            [
                                (header::CONTENT_TYPE, "application/json"),
                                (header::CACHE_CONTROL, "no-store"),
                            ],
                            r#"{"version":8,"sources":{},"layers":[]}"#,
                        )
                    }
                }
            }),
        )
        .route(
            "/styles/invalid/style.json",
            get({
                let requests = Arc::clone(&invalid_requests);
                move || {
                    let requests = Arc::clone(&requests);
                    async move {
                        requests.fetch_add(1, Ordering::Relaxed);
                        (
                            [
                                (header::CONTENT_TYPE, "application/json"),
                                (header::CACHE_CONTROL, "public, max-age=3600"),
                            ],
                            "not-json",
                        )
                    }
                }
            }),
        )
        .route(
            // Carries a transported `Age` far larger than the style default
            // TTL but no `Cache-Control`: it must still be cached under the
            // default policy (age is charged only against explicit upstream
            // freshness), so a second request is served without a re-fetch.
            "/styles/aged/style.json",
            get({
                let requests = Arc::clone(&aged_requests);
                move || {
                    let requests = Arc::clone(&requests);
                    async move {
                        requests.fetch_add(1, Ordering::Relaxed);
                        (
                            [
                                (header::CONTENT_TYPE, "application/json"),
                                (header::AGE, "100000"),
                            ],
                            r#"{"version":8,"sources":{},"layers":[]}"#,
                        )
                    }
                }
            }),
        )
        .route(
            "/styles/base/sprite.json",
            get(|| async { ([(header::CONTENT_TYPE, "application/json")], "{}") }),
        )
        .route(
            "/styles/base/sprite.png",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "image/png")],
                    &b"\x89PNG\r\n\x1a\n"[..],
                )
            }),
        )
        .route(
            "/styles/origin-style/style.json",
            get(|| async { StatusCode::NOT_FOUND }),
        )
        .route(
            "/styles/origin-sprite/sprite.png",
            get(|| async { StatusCode::GONE }),
        )
        .route(
            "/fonts/Origin404/0-255.pbf",
            get(|| async { StatusCode::NOT_FOUND }),
        )
        .route(
            "/styles/{fallback}/style.json",
            get({
                let requests = Arc::clone(&fallback_requests);
                move || {
                    let requests = Arc::clone(&requests);
                    async move {
                        requests.fetch_add(1, Ordering::Relaxed);
                        (
                            [(header::CONTENT_TYPE, "application/json")],
                            r#"{"version":8,"name":"local fallback","sources":{},"layers":[]}"#,
                        )
                    }
                }
            }),
        )
        .route(
            "/fonts/TestFont/0-255.pbf",
            get(|| async {
                (
                    [
                        (header::CONTENT_TYPE, "application/x-protobuf"),
                        (header::ETAG, GLYPH_UPSTREAM_ETAG),
                        (header::LAST_MODIFIED, GLYPH_UPSTREAM_LAST_MODIFIED),
                    ],
                    &b"glyph-bytes"[..],
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let addr = listener.local_addr().expect("upstream addr");
    tokio::spawn(axum::serve(listener, router).into_future());
    (
        addr,
        uncached_requests,
        invalid_requests,
        aged_requests,
        revalidated_requests,
        fallback_requests,
    )
}

struct Harness {
    public: Router,
    internal: Router,
    membership_owner: mmpf_cluster::ClusterOwner,
    tiles_dir: PathBuf,
    uncached_upstream_requests: Arc<AtomicUsize>,
    invalid_upstream_requests: Arc<AtomicUsize>,
    aged_upstream_requests: Arc<AtomicUsize>,
    revalidated_upstream_requests: Arc<AtomicUsize>,
    fallback_upstream_requests: Arc<AtomicUsize>,
}

impl Harness {
    async fn get(&self, router: &Router, path: &str) -> (StatusCode, HeaderMap, Bytes) {
        self.get_with(router, path, &[]).await
    }

    async fn get_with(
        &self,
        router: &Router,
        path: &str,
        request_headers: &[(header::HeaderName, &str)],
    ) -> (StatusCode, HeaderMap, Bytes) {
        let mut request = Request::builder().uri(path);
        for (name, value) in request_headers {
            request = request.header(name, *value);
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::empty()).expect("request"))
            .await
            .expect("router response");
        let status = response.status();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        (status, headers, body)
    }

    async fn cleanup(self) {
        self.membership_owner
            .shutdown()
            .await
            .expect("membership shutdown");
        let _ = std::fs::remove_dir_all(&self.tiles_dir);
    }
}

struct StaticPeerDirectory {
    peers: Arc<[Peer]>,
}

impl PeerDirectory for StaticPeerDirectory {
    fn peers(&self) -> PeerFuture<'_> {
        Box::pin(std::future::ready(Arc::clone(&self.peers)))
    }
}

async fn spawn_provider_peer() -> SocketAddr {
    let router = Router::new()
        .route(
            "/_internal/provider/styles/marked-404/style.json",
            get(|| async {
                (
                    StatusCode::NOT_FOUND,
                    [(
                        PROVIDER_NEGATIVE_HEADER,
                        InternalProviderNegative::NotFound.as_str(),
                    )],
                    "",
                )
            }),
        )
        .route(
            "/_internal/provider/styles/marked-410/style.json",
            get(|| async {
                (
                    StatusCode::GONE,
                    [(
                        PROVIDER_NEGATIVE_HEADER,
                        InternalProviderNegative::Gone.as_str(),
                    )],
                    "",
                )
            }),
        )
        .route(
            "/_internal/provider/styles/unmarked-404/style.json",
            get(|| async { StatusCode::NOT_FOUND }),
        )
        .route(
            "/_internal/provider/styles/unmarked-410/style.json",
            get(|| async { StatusCode::GONE }),
        )
        .route(
            "/_internal/provider/styles/mismatched-404/style.json",
            get(|| async {
                (
                    StatusCode::NOT_FOUND,
                    [(
                        PROVIDER_NEGATIVE_HEADER,
                        InternalProviderNegative::Gone.as_str(),
                    )],
                    "",
                )
            }),
        )
        .route(
            "/_internal/provider/styles/mismatched-410/style.json",
            get(|| async {
                (
                    StatusCode::GONE,
                    [(
                        PROVIDER_NEGATIVE_HEADER,
                        InternalProviderNegative::NotFound.as_str(),
                    )],
                    "",
                )
            }),
        )
        .route(
            "/_internal/provider/styles/unknown-marker/style.json",
            get(|| async {
                (
                    StatusCode::NOT_FOUND,
                    [(PROVIDER_NEGATIVE_HEADER, "unknown")],
                    "",
                )
            }),
        )
        .route(
            "/_internal/provider/styles/remote-retry/style.json",
            get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind provider peer");
    let addr = listener.local_addr().expect("provider peer addr");
    tokio::spawn(axum::serve(listener, router).into_future());
    addr
}

async fn harness(label: &str) -> Harness {
    harness_config(label, None, false).await
}

async fn harness_with_provider_peer(label: &str, provider_peer: Option<SocketAddr>) -> Harness {
    harness_config(label, provider_peer, false).await
}

async fn harness_with_auth(label: &str) -> Harness {
    harness_config(label, None, true).await
}

async fn harness_config(
    label: &str,
    provider_peer: Option<SocketAddr>,
    auth_enabled: bool,
) -> Harness {
    let (
        upstream,
        uncached_upstream_requests,
        invalid_upstream_requests,
        aged_upstream_requests,
        revalidated_upstream_requests,
        fallback_upstream_requests,
    ) = spawn_upstream().await;

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let tiles_dir = std::env::temp_dir().join(format!(
        "ishikari-contract-{label}-{}-{suffix}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tiles_dir).expect("tiles dir");
    write_mvt_pmtiles_fixture(&tiles_dir.join("fixture.pmtiles"));
    let delivery_auth = if auth_enabled {
        let auth_root = tiles_dir.join("auth");
        std::fs::create_dir_all(&auth_root).expect("auth root");
        let snapshot = serde_json::json!({
            "schema_version": 1,
            "registry_id": "public",
            "revision": 1,
            "credentials": [
                {
                    "credential_sha256": mmpf_auth::credential_sha256("public", "secret"),
                    "principal_id": "contract-browser",
                    "enabled": true,
                    "namespaces": ["base", "fixture"],
                    "actions": ["read"],
                    "allow_missing_origin": true
                },
                {
                    "credential_sha256": mmpf_auth::credential_sha256("public", "style-only"),
                    "principal_id": "contract-style-only",
                    "enabled": true,
                    "namespaces": ["base"],
                    "actions": ["read"],
                    "allow_missing_origin": true
                },
                {
                    "credential_sha256": mmpf_auth::credential_sha256("public", "encoded-alias"),
                    "principal_id": "contract-encoded-alias",
                    "enabled": true,
                    "namespaces": ["%62ase"],
                    "actions": ["read"],
                    "allow_missing_origin": true
                }
            ]
        });
        std::fs::write(
            auth_root.join("current.json"),
            serde_json::to_vec(&snapshot).expect("auth snapshot JSON"),
        )
        .expect("auth snapshot");
        let root_url = url::Url::from_directory_path(&auth_root)
            .expect("auth root URL")
            .to_string();
        let catalog =
            mmpf_auth::RegistryCatalog::parse(&format!("public={root_url}")).expect("auth catalog");
        mmpf_auth::DeliveryAuth::new(catalog, std::iter::empty::<(String, String)>())
    } else {
        None
    };

    // Membership still backs operational state. Provider-route contract tests
    // can inject a fixed remote owner into the resolver independently.
    let (membership, membership_owner) = Membership::spawn(MembershipConfig {
        node_id: "contract-node".to_string(),
        gossip_endpoint: mmpf_cluster::GossipEndpoint::standalone(
            "127.0.0.1:0".parse().expect("addr"),
            "127.0.0.1:0".parse().expect("addr"),
        ),
        http_advertise_addr: "127.0.0.1:0".parse().expect("addr"),
        seed_nodes: Vec::new(),
        gossip_interval: Duration::from_millis(200),
    })
    .await
    .expect("membership");

    let peer_directory: Arc<dyn PeerDirectory> = match provider_peer {
        Some(addr) => Arc::new(StaticPeerDirectory {
            peers: vec![Peer {
                id: "provider-peer".to_string(),
                addr,
            }]
            .into(),
        }),
        None => Arc::new(membership.clone()),
    };
    let registry = Arc::new(ObjectStoreRegistry::without_options());
    let metrics = NodeMetrics::new();
    let resolver = ResourceResolver::new(ResourceResolverConfig {
        self_node_id: "contract-node".to_string(),
        peer_directory,
        transport: Arc::new(
            crate::internal_transport::HttpInternalTransport::new().expect("internal transport"),
        ),
        tileset_sources: tiles_dir.to_string_lossy().into_owned(),
        tuning: ResolverTuningInput {
            candidate_count: 1,
            tile_group_size: 512,
            chunk_size_bytes: PMTILES_FIXTURE_SIZE as u64,
            max_fetch_chunks: 4,
            chunk_fetch_merge_window: Duration::from_millis(10),
            backend_fetch_concurrency: 4,
            backend_fetch_max_inflight: 16,
            tile_cache_max_bytes: 1024 * 1024,
            chunk_cache_max_bytes: 1024 * 1024,
            tile_negative_ttl: Duration::from_secs(60),
        }
        .resolve()
        .expect("valid resolver tuning"),
        cache_capacities: ResourceCacheCapacities::default(),
        artificial_backend_delay_ms: 0,
        object_store_registry: Arc::clone(&registry),
        metrics: metrics.clone(),
    })
    .expect("resolver");

    let provider = ProviderConfig::new(
        Some(format!(
            "regional=http://{upstream}/regional/{{style_id}}/style.json;default=http://{upstream}/styles/{{style_id}}/style.json"
        )),
        Some(format!(
            "http://{upstream}/fonts/{{fontstack}}/{{range}}.pbf"
        )),
        Some(format!("http://{upstream}/styles/{{style_id}}/sprite")),
    )
    .expect("provider config");

    let state = AppState::new(
        membership.clone(),
        metrics,
        Arc::new(resolver),
        DrainController::new(),
        provider,
        registry,
        ServerRuntimeConfig {
            gossip_bootstrap_readiness: mmpf_cluster::BootstrapReadinessGate::new(
                false,
                Duration::from_secs(30),
            ),
            delivery_auth,
            mapterhorn: None,
            cpu_work_concurrency: 1,
            cpu_work_max_inflight: 4,
            derived_negative_ttl: Duration::from_secs(60),
            cache_capacities: crate::options::CacheCapacities::default(),
        },
    );

    Harness {
        public: with_public_layers(public_router(), state.clone()),
        internal: with_common_layers(internal_router(), state),
        membership_owner,
        tiles_dir,
        uncached_upstream_requests,
        invalid_upstream_requests,
        aged_upstream_requests,
        revalidated_upstream_requests,
        fallback_upstream_requests,
    }
}

#[tokio::test]
async fn remote_provider_negatives_require_an_exact_private_marker() {
    let provider_peer = spawn_provider_peer().await;
    let harness = harness_with_provider_peer("provider-negative", Some(provider_peer)).await;

    for (path, expected_status) in [
        ("/styles/marked-404/style.json", StatusCode::NOT_FOUND),
        ("/styles/marked-410/style.json", StatusCode::GONE),
    ] {
        let (status, _, _) = harness.get(&harness.public, path).await;
        assert_eq!(status, expected_status, "path: {path}");
        assert_eq!(
            harness.fallback_upstream_requests.load(Ordering::Relaxed),
            0,
            "correctly marked remote negatives must not request local fallback"
        );
    }

    for path in [
        "/styles/unmarked-404/style.json",
        "/styles/unmarked-410/style.json",
        "/styles/mismatched-404/style.json",
        "/styles/mismatched-410/style.json",
        "/styles/unknown-marker/style.json",
    ] {
        let expected_fallbacks = harness.fallback_upstream_requests.load(Ordering::Relaxed) + 1;
        let (status, _, body) = harness.get(&harness.public, path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unsafe marker must fall back locally for {path}: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(
            harness.fallback_upstream_requests.load(Ordering::Relaxed),
            expected_fallbacks,
            "unsafe marker must request local fallback for {path}"
        );
    }

    let expected_fallbacks = harness.fallback_upstream_requests.load(Ordering::Relaxed) + 1;
    let (status, _, body) = harness
        .get(&harness.public, "/styles/remote-retry/style.json")
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "retryable peer status should fall back locally: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        harness.fallback_upstream_requests.load(Ordering::Relaxed),
        expected_fallbacks,
        "retryable peer failures must retain local-origin fallback"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn delivery_auth_is_optional_and_covers_only_public_content_routes() {
    let harness = harness_with_auth("delivery-auth-boundary").await;

    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/style.json")
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
    assert_eq!(headers[header::CACHE_CONTROL], "private, no-store");

    assert_eq!(
        harness.get(&harness.public, "/readyz").await.0,
        StatusCode::OK,
        "delivery auth must not protect operational probes"
    );
    assert_eq!(
        harness
            .get(
                &harness.internal,
                "/_internal/provider/styles/base/style.json"
            )
            .await
            .0,
        StatusCode::OK,
        "peer traffic uses the internal trust boundary, not browser credentials"
    );

    let (status, _, _) = harness
        .get_with(
            &harness.public,
            "/styles/base/style.json?access_token=public.secret",
            &[(header::AUTHORIZATION, "Bearer public.secret")],
        )
        .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "mixed transports are ambiguous"
    );

    let (status, _, _) = harness
        .get(
            &harness.public,
            "/styles/regional/base/style.json?access_token=public.secret",
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "namespace grants are exact");

    harness.cleanup().await;
}

#[tokio::test]
async fn delivery_auth_uses_the_percent_decoded_namespace_identity() {
    let harness = harness_with_auth("delivery-auth-canonical-namespace").await;

    assert_eq!(
        harness
            .get(
                &harness.public,
                "/styles/%62ase/style.json?access_token=public.secret",
            )
            .await
            .0,
        StatusCode::OK,
        "the canonical base grant should authorize an equivalent encoded path"
    );
    let (status, headers, _) = harness
        .get(
            &harness.public,
            "/styles/%62ase/style.json?access_token=public.encoded-alias",
        )
        .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a raw encoded alias grant must not authorize the decoded base namespace"
    );
    assert_eq!(headers[header::CACHE_CONTROL], "private, no-store");

    harness.cleanup().await;
}

#[tokio::test]
async fn verified_query_tokens_propagate_only_to_generated_ishikari_urls() {
    let harness = harness_with_auth("delivery-auth-propagation").await;

    let (status, _, body) = harness
        .get(
            &harness.public,
            "/styles/base/style.json?access_token=public.secret",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let style: serde_json::Value = serde_json::from_slice(&body).expect("style JSON");
    assert!(
        style["glyphs"]
            .as_str()
            .is_some_and(|url| url.ends_with("?access_token=public.secret"))
    );
    assert!(
        style["sprite"]
            .as_str()
            .is_some_and(|url| url.ends_with("?access_token=public.secret"))
    );
    assert!(
        style["sources"]["base"]["url"]
            .as_str()
            .is_some_and(|url| url.ends_with("/tilesets/fixture?access_token=public.secret"))
    );

    let (status, _, body) = harness
        .get(
            &harness.public,
            "/tilesets/fixture?access_token=public.secret",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let tilejson: serde_json::Value = serde_json::from_slice(&body).expect("TileJSON");
    assert!(tilejson["tiles"][0].as_str().is_some_and(|url| {
        url.ends_with("/tilesets/fixture/{z}/{x}/{y}?access_token=public.secret")
    }));

    let (status, _, body) = harness
        .get_with(
            &harness.public,
            "/styles/base/style.json",
            &[(header::AUTHORIZATION, "Bearer public.secret")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !String::from_utf8_lossy(&body).contains("access_token"),
        "a header credential must never be converted into a URL credential"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn warmed_resource_cache_never_bypasses_current_token_namespace_grants() {
    let harness = harness_with_auth("delivery-auth-cache-noninterference").await;

    let broad_tile = "/tilesets/fixture/0/0/0?access_token=public.secret";
    assert_eq!(
        harness.get(&harness.public, broad_tile).await.0,
        StatusCode::OK,
        "the broad credential should populate the shared tile cache"
    );
    assert_eq!(
        harness.get(&harness.public, broad_tile).await.0,
        StatusCode::OK,
        "the broad credential should be able to consume the warm entry"
    );

    let (status, _, style_body) = harness
        .get(
            &harness.public,
            "/styles/base/style.json?access_token=public.style-only",
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the weaker credential still grants its exact style namespace"
    );
    let style: serde_json::Value =
        serde_json::from_slice(&style_body).expect("style JSON for weaker credential");
    assert!(
        style["sources"]["base"]["url"]
            .as_str()
            .is_some_and(|url| url.ends_with("/tilesets/fixture?access_token=public.style-only")),
        "generated URLs must retain the currently verified credential"
    );

    assert_eq!(
        harness
            .get(
                &harness.public,
                "/tilesets/fixture/0/0/0?access_token=public.style-only",
            )
            .await
            .0,
        StatusCode::FORBIDDEN,
        "a warm resource entry must not bypass the weaker credential's namespace grants"
    );
    assert_eq!(
        harness.get(&harness.public, broad_tile).await.0,
        StatusCode::OK,
        "a denied request must not poison the authorized cached entry"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn internal_provider_handlers_mark_only_fetcher_negatives() {
    let harness = harness("internal-provider-negative-marker").await;

    for (path, expected_status, expected_marker) in [
        (
            "/_internal/provider/styles/origin-style/style.json",
            StatusCode::NOT_FOUND,
            InternalProviderNegative::NotFound,
        ),
        (
            "/_internal/provider/fonts/Origin404/0-255.pbf",
            StatusCode::NOT_FOUND,
            InternalProviderNegative::NotFound,
        ),
        (
            "/_internal/provider/styles/origin-sprite/sprite.png",
            StatusCode::GONE,
            InternalProviderNegative::Gone,
        ),
    ] {
        let (status, headers, _) = harness.get(&harness.internal, path).await;
        assert_eq!(status, expected_status, "path: {path}");
        assert_eq!(
            headers[PROVIDER_NEGATIVE_HEADER],
            expected_marker.as_str(),
            "path: {path}"
        );
    }

    let (status, headers, _) = harness
        .get(
            &harness.internal,
            "/_internal/provider/styles/not-a-provider-resource",
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        headers.get(PROVIDER_NEGATIVE_HEADER).is_none(),
        "path-resolution 404 must remain unmarked"
    );

    let (status, headers, _) = harness
        .get(&harness.public, "/styles/origin-style/style.json")
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        headers.get(PROVIDER_NEGATIVE_HEADER).is_none(),
        "the private marker must never be emitted on the public route"
    );

    harness.cleanup().await;
}

#[tokio::test]
async fn concurrent_uncacheable_requests_share_the_leader_body() {
    let harness = harness("uncacheable-singleflight").await;
    let path = "/styles/uncached/style.json";
    let (first, second, third) = tokio::join!(
        harness.get(&harness.public, path),
        harness.get(&harness.public, path),
        harness.get(&harness.public, path),
    );
    for (status, headers, _) in [first, second, third] {
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    }
    assert_eq!(
        harness.uncached_upstream_requests.load(Ordering::Relaxed),
        1,
        "followers must reuse the uncacheable leader representation"
    );

    // It remains uncacheable for a later, independent request.
    assert_eq!(harness.get(&harness.public, path).await.0, StatusCode::OK);
    assert_eq!(
        harness.uncached_upstream_requests.load(Ordering::Relaxed),
        2
    );
    harness.cleanup().await;
}

#[tokio::test]
async fn transported_age_without_explicit_freshness_still_caches() {
    let harness = harness("aged-default").await;
    let path = "/styles/aged/style.json";

    let (status, _, _) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    // A second request is served from the provider cache: the transported
    // `Age: 100000` must not be charged against the invented default TTL, which
    // would otherwise evict on insert and re-fetch on every request.
    let (status, _, _) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        harness.aged_upstream_requests.load(Ordering::Relaxed),
        1,
        "default-TTL responses must cache regardless of transported Age"
    );
    harness.cleanup().await;
}

#[tokio::test]
async fn stale_provider_revalidation_reuses_bytes_on_origin_304() {
    let harness = harness("provider-revalidation").await;
    let path = "/styles/revalidated/style.json";

    let (status, initial_headers, initial_body) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        initial_headers[header::CACHE_CONTROL],
        "public, max-age=0, s-maxage=0, stale-while-revalidate=60"
    );
    assert_eq!(
        harness
            .revalidated_upstream_requests
            .load(Ordering::Relaxed),
        1
    );

    // The immediately stale body is served without waiting while one background
    // request revalidates it with the origin ETag.
    let (status, _, stale_body) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stale_body, initial_body);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (_, _, metrics) = harness
                .get(&harness.internal, "/_internal/metrics")
                .await;
            if String::from_utf8_lossy(&metrics).contains(
                "ishikari_provider_resource_cache_total{outcome=\"revalidated\",resource=\"style\"} 1",
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background conditional revalidation");

    // The 304 changed freshness to 120 seconds without sending another body.
    // A third request must use that refreshed entry rather than hit the origin.
    let (status, refreshed_headers, refreshed_body) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(refreshed_body, initial_body);
    assert_eq!(
        refreshed_headers[header::CACHE_CONTROL],
        "public, max-age=120, s-maxage=120, stale-while-revalidate=60"
    );
    assert_eq!(
        harness
            .revalidated_upstream_requests
            .load(Ordering::Relaxed),
        2
    );

    let (status, _, metrics) = harness.get(&harness.internal, "/_internal/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&metrics).contains(
        "ishikari_provider_resource_cache_total{outcome=\"revalidated\",resource=\"style\"} 1"
    ));
    harness.cleanup().await;
}

#[tokio::test]
async fn invalid_style_json_never_enters_the_provider_cache() {
    let harness = harness("invalid-style").await;
    let path = "/styles/invalid/style.json";
    assert_eq!(
        harness.get(&harness.public, path).await.0,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        harness.get(&harness.public, path).await.0,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        harness.invalid_upstream_requests.load(Ordering::Relaxed),
        2,
        "invalid successful origin responses must not be cached"
    );
    harness.cleanup().await;
}

#[tokio::test]
async fn public_provider_responses_carry_cache_policy_and_age() {
    let harness = harness("public").await;

    // Style: upstream policy is honored and normalized, body is rewritten JSON.
    let (status, headers, body) = harness
        .get(&harness.public, "/styles/base/style.json")
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        headers[header::CACHE_CONTROL],
        STYLE_NORMALIZED_CACHE_CONTROL
    );
    assert_eq!(headers[header::AGE], "10");
    assert_eq!(headers[header::VARY], "Origin, X-Forwarded-Proto");
    assert!(headers.get(header::CONTENT_ENCODING).is_none());
    assert!(
        headers.get(PROVIDER_CACHE_CONTROL_HEADER).is_none(),
        "internal metadata must not leak on the public port"
    );
    let style: serde_json::Value = serde_json::from_slice(&body).expect("style JSON");
    assert_eq!(style["version"], 8);

    // A repeat request is served from the provider cache with the same policy.
    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/style.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers[header::CACHE_CONTROL],
        STYLE_NORMALIZED_CACHE_CONTROL
    );
    assert!(headers.contains_key(header::AGE));

    // Glyphs and sprites have no upstream policy: the asset default applies.
    // Glyph bytes are verbatim, so upstream validators pass through unchanged.
    let (status, headers, body) = harness
        .get(&harness.public, "/fonts/TestFont/0-255.pbf")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], cache::GLYPH_SPRITE);
    assert_eq!(headers[header::AGE], "0");
    assert_eq!(headers[header::ETAG], GLYPH_UPSTREAM_ETAG);
    assert_eq!(headers[header::LAST_MODIFIED], GLYPH_UPSTREAM_LAST_MODIFIED);
    assert_eq!(body.as_ref(), b"glyph-bytes");

    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/sprite.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], cache::GLYPH_SPRITE);
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");

    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/sprite.png")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], cache::GLYPH_SPRITE);
    assert_eq!(headers[header::CONTENT_TYPE], "image/png");

    harness.cleanup().await;
}

#[tokio::test]
async fn internal_provider_responses_carry_typed_metadata_not_public_headers() {
    let harness = harness("internal").await;

    let (status, headers, _) = harness
        .get(
            &harness.internal,
            "/_internal/provider/styles/base/style.json",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers[PROVIDER_CACHE_CONTROL_HEADER],
        STYLE_NORMALIZED_CACHE_CONTROL
    );
    assert_eq!(headers[PROVIDER_AGE_HEADER], "10");
    assert_eq!(headers[header::CONTENT_ENCODING], "gzip");
    assert!(
        headers.get(header::CACHE_CONTROL).is_none(),
        "internal responses must not carry public caching headers"
    );

    let (status, headers, body) = harness
        .get(
            &harness.internal,
            "/_internal/provider/fonts/TestFont/0-255.pbf",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[PROVIDER_CACHE_CONTROL_HEADER], cache::GLYPH_SPRITE);
    assert!(headers.contains_key(PROVIDER_AGE_HEADER));
    assert_eq!(headers[PROVIDER_ETAG_HEADER], GLYPH_UPSTREAM_ETAG);
    assert_eq!(
        headers[PROVIDER_LAST_MODIFIED_HEADER],
        GLYPH_UPSTREAM_LAST_MODIFIED
    );
    assert!(headers.get(header::CACHE_CONTROL).is_none());
    assert_eq!(body.as_ref(), b"glyph-bytes");

    harness.cleanup().await;
}

#[tokio::test]
async fn conditional_requests_return_304_with_cache_metadata() {
    let harness = harness("conditional").await;

    // Matching If-None-Match on a verbatim resource: 304, no body, but the
    // full cache metadata so downstream caches can refresh their entry.
    let (status, headers, body) = harness
        .get_with(
            &harness.public,
            "/fonts/TestFont/0-255.pbf",
            &[(header::IF_NONE_MATCH, GLYPH_UPSTREAM_ETAG)],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty());
    assert_eq!(headers[header::CACHE_CONTROL], cache::GLYPH_SPRITE);
    assert!(headers.contains_key(header::AGE));
    assert_eq!(headers[header::ETAG], GLYPH_UPSTREAM_ETAG);

    // Non-matching validator: full 200 body.
    let (status, _, body) = harness
        .get_with(
            &harness.public,
            "/fonts/TestFont/0-255.pbf",
            &[(header::IF_NONE_MATCH, "\"stale\"")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"glyph-bytes");

    // If-Modified-Since at the upstream Last-Modified date matches.
    let (status, _, _) = harness
        .get_with(
            &harness.public,
            "/fonts/TestFont/0-255.pbf",
            &[(header::IF_MODIFIED_SINCE, GLYPH_UPSTREAM_LAST_MODIFIED)],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);

    // `*` matches any existing representation, even when the origin supplied
    // no entity tag.
    let (status, headers, _) = harness
        .get_with(
            &harness.public,
            "/styles/base/sprite.json",
            &[(header::IF_NONE_MATCH, "*")],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(headers.get(header::ETAG).is_none());

    // Style: the derived ETag identifies the rewritten body, is stable across
    // requests, and round-trips through If-None-Match.
    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/style.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    let style_etag = headers[header::ETAG].to_str().expect("etag").to_owned();
    assert_ne!(style_etag, GLYPH_UPSTREAM_ETAG);
    assert!(
        headers.get(header::LAST_MODIFIED).is_none(),
        "derived body must not reuse the upstream Last-Modified"
    );

    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/style.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::ETAG], style_etag.as_str());

    let (status, headers, body) = harness
        .get_with(
            &harness.public,
            "/styles/base/style.json",
            &[(header::IF_NONE_MATCH, style_etag.as_str())],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty());
    assert_eq!(
        headers[header::CACHE_CONTROL],
        STYLE_NORMALIZED_CACHE_CONTROL
    );
    assert_eq!(headers[header::VARY], "Origin, X-Forwarded-Proto");

    harness.cleanup().await;
}

#[tokio::test]
async fn rewritten_styles_vary_with_the_effective_origin() {
    let harness = harness("style-origin").await;
    let path = "/styles/base/style.json";
    let forwarded_proto = header::HeaderName::from_static("x-forwarded-proto");

    let (status, first_headers, first_body) = harness
        .get_with(
            &harness.public,
            path,
            &[
                (header::HOST, "one.example"),
                (forwarded_proto.clone(), "https"),
            ],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_headers[header::VARY], "Origin, X-Forwarded-Proto");
    let first: serde_json::Value = serde_json::from_slice(&first_body).expect("style JSON");
    assert_eq!(
        first["glyphs"],
        "https://one.example/fonts/{fontstack}/{range}.pbf"
    );

    let (status, second_headers, second_body) = harness
        .get_with(
            &harness.public,
            path,
            &[(header::HOST, "two.example"), (forwarded_proto, "https")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let second: serde_json::Value = serde_json::from_slice(&second_body).expect("style JSON");
    assert_eq!(
        second["glyphs"],
        "https://two.example/fonts/{fontstack}/{range}.pbf"
    );
    assert_ne!(first_body, second_body);
    assert_ne!(first_headers[header::ETAG], second_headers[header::ETAG]);

    harness.cleanup().await;
}

#[tokio::test]
async fn namespaced_style_uses_the_matching_upstream_template() {
    let harness = harness("namespaced-style").await;

    let (status, headers, body) = harness
        .get(&harness.public, "/styles/regional/base/style.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], cache::STYLE);
    let style: serde_json::Value = serde_json::from_slice(&body).expect("style JSON");
    assert_eq!(style["name"], "regional");

    harness.cleanup().await;
}

#[tokio::test]
async fn tile_routes_serve_stored_mvt_and_negotiate_mlt() {
    let harness = harness("tile-negotiation").await;
    let path = "/tilesets/fixture/0/0/0";

    let (status, headers, stored) = harness.get(&harness.public, path).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&stored)
    );
    assert_eq!(
        headers[header::CONTENT_TYPE],
        "application/vnd.mapbox-vector-tile"
    );
    assert_eq!(headers[header::CONTENT_ENCODING], "gzip");
    assert_eq!(headers[header::CACHE_CONTROL], cache::TILE);
    assert_eq!(headers[header::VARY], "Accept");
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(stored.as_ref())
        .read_to_end(&mut decoded)
        .expect("decode stored MVT");
    assert!(decoded.is_empty());

    for (path, request_headers) in [
        ("/tilesets/fixture/0/0/0.mlt", Vec::new()),
        (
            path,
            vec![(header::ACCEPT, "application/vnd.maplibre-tile")],
        ),
    ] {
        let (status, headers, mlt) = harness
            .get_with(&harness.public, path, &request_headers)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers[header::CONTENT_TYPE],
            "application/vnd.maplibre-tile"
        );
        assert_eq!(headers[header::CONTENT_ENCODING], "gzip");
        assert_eq!(headers[header::CACHE_CONTROL], cache::TILE);
        assert_eq!(headers[header::VARY], "Accept");
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(mlt.as_ref())
            .read_to_end(&mut decoded)
            .expect("decode negotiated MLT");
        assert!(decoded.is_empty());
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn tilejson_emits_a_derived_etag_and_answers_conditional_requests() {
    let harness = harness("tilejson-conditional").await;
    let path = "/tilesets/fixture";

    let (status, headers, body) = harness.get(&harness.public, path).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(headers[header::CACHE_CONTROL], cache::TILEJSON);
    assert_eq!(headers[header::VARY], "Origin, X-Forwarded-Proto");
    // TileJSON is a derived representation: strong ETag, no Last-Modified.
    let etag = headers[header::ETAG].to_str().expect("etag").to_owned();
    assert!(
        etag.starts_with('"'),
        "expected a strong ETag, got {etag:?}"
    );
    assert!(headers.get(header::LAST_MODIFIED).is_none());
    let document: serde_json::Value = serde_json::from_slice(&body).expect("tilejson JSON");
    assert_eq!(document["tilejson"], "3.0.0");

    // The ETag is stable across requests with the same origin.
    let (_, headers, _) = harness.get(&harness.public, path).await;
    assert_eq!(headers[header::ETAG].to_str().unwrap(), etag);

    // A matching If-None-Match yields 304 with cache metadata, no body.
    let (status, headers, body) = harness
        .get_with(&harness.public, path, &[(header::IF_NONE_MATCH, &etag)])
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty());
    assert_eq!(headers[header::CACHE_CONTROL], cache::TILEJSON);
    assert_eq!(headers[header::ETAG].to_str().unwrap(), etag);
    assert_eq!(headers[header::VARY], "Origin, X-Forwarded-Proto");

    // A non-matching validator serves the full document.
    let (status, _, body) = harness
        .get_with(
            &harness.public,
            path,
            &[(header::IF_NONE_MATCH, "\"stale\"")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.is_empty());

    harness.cleanup().await;
}

#[tokio::test]
async fn internal_leaf_ranges_are_bounded_by_the_declared_leaf_section() {
    let harness = harness("internal-leaf-range").await;
    let leaf_offset = PMTILES_HEADER_SIZE + 5; // five one-byte root-directory varints

    let (status, _, body) = harness
        .get(
            &harness.internal,
            &format!(
                "/_internal/pmtiles/fixture/leaf/{leaf_offset}/{}",
                PMTILES_LEAF_BYTES.len()
            ),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, PMTILES_LEAF_BYTES);

    for path in [
        format!("/_internal/pmtiles/fixture/leaf/{leaf_offset}/0"),
        format!(
            "/_internal/pmtiles/fixture/leaf/{}/2",
            leaf_offset + PMTILES_LEAF_BYTES.len() - 1
        ),
        format!(
            "/_internal/pmtiles/fixture/leaf/{}/2",
            PMTILES_FIXTURE_SIZE - 1
        ),
        format!("/_internal/pmtiles/fixture/leaf/{}/1", u64::MAX),
    ] {
        let (status, _, body) = harness.get(&harness.internal, &path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "path: {path}");
        assert_eq!(body, "invalid leaf range", "path: {path}");
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn internal_metrics_expose_every_material_cache() {
    let harness = harness("cache-metrics").await;

    let (status, _, body) = harness.get(&harness.internal, "/_internal/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let body = String::from_utf8_lossy(&body);
    for cache in [
        "tile", "chunk", "resource", "archive", "leaf", "provider", "mlt", "derived", "dem",
    ] {
        assert!(
            body.contains(&format!("ishikari_cache_bytes{{cache=\"{cache}\"}}")),
            "missing cache gauge for {cache}: {body}"
        );
    }

    harness.cleanup().await;
}

#[tokio::test]
async fn internal_paths_are_not_exposed_on_the_public_router() {
    let harness = harness("exposure").await;

    // Sanity: the public router itself works.
    let (status, _, _) = harness.get(&harness.public, "/livez").await;
    assert_eq!(status, StatusCode::OK);

    for path in [
        "/_internal/metrics",
        "/_internal/healthz",
        "/_internal/cluster",
        "/_internal/provider/styles/base/style.json",
        "/_internal/provider/fonts/TestFont/0-255.pbf",
        "/_internal/tiles/demo/0",
    ] {
        let (status, _, _) = harness.get(&harness.public, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path} must 404 publicly");
    }

    harness.cleanup().await;
}
