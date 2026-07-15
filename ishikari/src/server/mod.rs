//! HTTP app wiring and shared state.

use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::{
    drain::{self, DrainController},
    membership::Membership,
    metrics::NodeMetrics,
    request_id, server,
    server::provider::ProviderConfig,
    server::tileset::mapterhorn::MapterhornResolver,
    server::upstream::ProviderFetchCache,
    storage::{ObjectStoreRegistry, ResourceResolver},
};
use anyhow::{Context, Result};
use axum::{
    Json, Router, ServiceExt,
    extract::{MatchedPath, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use tokio::net::TcpListener;
use tracing::Instrument;

pub(crate) type HttpError = (StatusCode, String);

pub struct TileRuntimeConfig {
    pub mapterhorn: Option<Arc<MapterhornResolver>>,
    pub cpu_work_concurrency: usize,
    /// Maximum admitted CPU-work units (holding a permit or queued for one)
    /// before new work is shed with 503.
    pub cpu_work_max_inflight: usize,
    pub derived_negative_ttl: Duration,
}

/// RAII reservation in the CPU-work admission counter. Reserving fails (a shed)
/// when the counter is already at its ceiling; the reservation is released on
/// drop — including when the awaiting future is cancelled before it acquires a
/// permit — so the count can never leak.
struct CpuWorkSlot {
    inflight: Arc<AtomicUsize>,
}

impl CpuWorkSlot {
    fn try_reserve(inflight: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        let previous = inflight.fetch_add(1, Ordering::Relaxed);
        if previous >= max {
            inflight.fetch_sub(1, Ordering::Relaxed);
            None
        } else {
            Some(Self {
                inflight: inflight.clone(),
            })
        }
    }
}

impl Drop for CpuWorkSlot {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Admission ticket for one unit of CPU-bound request work. Holds both a
/// concurrency permit and an in-flight slot; dropping it (e.g. at the end of the
/// `spawn_blocking` closure) releases both.
pub(crate) struct CpuWorkPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _slot: CpuWorkSlot,
}

struct DerivedTileExpiry {
    negative_ttl: Duration,
}

impl
    moka::Expiry<server::tileset::terrain::DerivedTileKey, server::tileset::terrain::DerivedOutcome>
    for DerivedTileExpiry
{
    fn expire_after_create(
        &self,
        _key: &server::tileset::terrain::DerivedTileKey,
        value: &server::tileset::terrain::DerivedOutcome,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        matches!(value, server::tileset::terrain::DerivedOutcome::Absent)
            .then_some(self.negative_ttl)
    }
}

struct DecodedDemExpiry {
    negative_ttl: Duration,
}

impl
    moka::Expiry<
        (crate::interned::TilesetId, u64),
        Option<Arc<server::tileset::terrain::dem::DemTile>>,
    > for DecodedDemExpiry
{
    fn expire_after_create(
        &self,
        _key: &(crate::interned::TilesetId, u64),
        value: &Option<Arc<server::tileset::terrain::dem::DemTile>>,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        value.is_none().then_some(self.negative_ttl)
    }
}

#[derive(Clone)]
pub struct AppState {
    membership: Membership,
    pub(crate) metrics: NodeMetrics,
    resource_resolver: Arc<ResourceResolver>,
    drain: DrainController,
    provider: ProviderConfig,
    provider_fetch_cache: ProviderFetchCache,
    object_store_registry: Arc<ObjectStoreRegistry>,
    /// Per-pod cache of transcoded MLT tiles, keyed by (tileset, tile id).
    /// Populated lazily on first `.mlt` request; see `server::tileset::mlt`.
    mlt_cache: moka::future::Cache<(crate::interned::TilesetId, u64), bytes::Bytes>,
    /// Generated contour/hillshade MVTs. Async cache initialization single-flights
    /// the 3x3 source fetch and CPU generation for each derived tile.
    derived_tile_cache: moka::future::Cache<
        server::tileset::terrain::DerivedTileKey,
        server::tileset::terrain::DerivedOutcome,
    >,
    /// Decoded Terrarium DEM tiles, shared across derived products and
    /// neighboring derived tiles (each 3x3 window overlaps its neighbors in six
    /// of nine sources), so each source tile is WebP-decoded roughly once.
    dem_tile_cache: moka::future::Cache<
        (crate::interned::TilesetId, u64),
        Option<Arc<server::tileset::terrain::dem::DemTile>>,
    >,
    /// Shared bound for all CPU-heavy blocking work initiated by request paths.
    cpu_work_semaphore: Arc<tokio::sync::Semaphore>,
    /// Count of admitted CPU-work units (holding a permit or queued for one).
    /// Work beyond `cpu_work_max_inflight` is shed with 503 so an extreme flood
    /// fails fast instead of growing the wait queue and blocking backlog.
    cpu_work_inflight: Arc<AtomicUsize>,
    cpu_work_max_inflight: usize,
    derived_negative_ttl: Duration,
    /// Mapterhorn composite resolver, when a composite tileset is configured.
    mapterhorn: Option<Arc<MapterhornResolver>>,
}

impl AppState {
    pub fn new(
        membership: Membership,
        metrics: NodeMetrics,
        resource_resolver: Arc<ResourceResolver>,
        drain: DrainController,
        provider: ProviderConfig,
        object_store_registry: Arc<ObjectStoreRegistry>,
        tile_runtime: TileRuntimeConfig,
    ) -> Self {
        let TileRuntimeConfig {
            mapterhorn,
            cpu_work_concurrency,
            cpu_work_max_inflight,
            derived_negative_ttl,
        } = tile_runtime;
        Self {
            membership,
            metrics,
            resource_resolver,
            drain,
            provider,
            provider_fetch_cache: ProviderFetchCache::new(),
            object_store_registry,
            mapterhorn,
            // Bounded, byte-weighted: first `.mlt` request transcodes, the rest
            // hit this cache. 64 MiB ≈ a few hundred warm MLT tiles per pod.
            mlt_cache: moka::future::Cache::builder()
                .max_capacity(64 * 1024 * 1024)
                .weigher(|_key, value: &bytes::Bytes| {
                    u32::try_from(value.len()).unwrap_or(u32::MAX)
                })
                .build(),
            derived_tile_cache: moka::future::Cache::builder()
                .max_capacity(128 * 1024 * 1024)
                .weigher(
                    |_key: &server::tileset::terrain::DerivedTileKey,
                     value: &server::tileset::terrain::DerivedOutcome| {
                        match value {
                            server::tileset::terrain::DerivedOutcome::Tile(tile) => {
                                u32::try_from(tile.bytes.len()).unwrap_or(u32::MAX)
                            }
                            server::tileset::terrain::DerivedOutcome::Absent => 1,
                        }
                    },
                )
                .expire_after(DerivedTileExpiry {
                    negative_ttl: derived_negative_ttl,
                })
                .build(),
            // 64 MiB ≈ 64 decoded 512px DEM tiles (f32) — an 8x8-source-tile
            // working set, plenty for a viewport of derived tiles.
            dem_tile_cache: moka::future::Cache::builder()
                .max_capacity(64 * 1024 * 1024)
                .weigher(
                    |_key: &(crate::interned::TilesetId, u64),
                     value: &Option<Arc<server::tileset::terrain::dem::DemTile>>| {
                        value.as_ref().map_or(1, |tile| {
                            u32::try_from(tile.byte_size()).unwrap_or(u32::MAX)
                        })
                    },
                )
                .expire_after(DecodedDemExpiry {
                    negative_ttl: derived_negative_ttl,
                })
                .build(),
            cpu_work_semaphore: Arc::new(tokio::sync::Semaphore::new(
                cpu_work_concurrency.max(1),
            )),
            cpu_work_inflight: Arc::new(AtomicUsize::new(0)),
            cpu_work_max_inflight: cpu_work_max_inflight.max(cpu_work_concurrency.max(1)),
            derived_negative_ttl,
        }
    }
}

impl AppState {
    /// Per-pod transcoded-MLT cache, keyed by `(tileset, tile id)`.
    pub(crate) fn mlt_cache(
        &self,
    ) -> &moka::future::Cache<(crate::interned::TilesetId, u64), bytes::Bytes> {
        &self.mlt_cache
    }

    /// The configured Mapterhorn composite resolver, if any.
    pub(crate) fn mapterhorn(&self) -> Option<&Arc<MapterhornResolver>> {
        self.mapterhorn.as_ref()
    }

    pub(crate) fn derived_tile_cache(
        &self,
    ) -> &moka::future::Cache<
        server::tileset::terrain::DerivedTileKey,
        server::tileset::terrain::DerivedOutcome,
    > {
        &self.derived_tile_cache
    }

    /// Decoded-DEM cache backing derived terrain generation.
    pub(crate) fn dem_tile_cache(
        &self,
    ) -> &moka::future::Cache<
        (crate::interned::TilesetId, u64),
        Option<Arc<server::tileset::terrain::dem::DemTile>>,
    > {
        &self.dem_tile_cache
    }

    /// Admits one unit of CPU-bound work: reserves an in-flight slot (shedding
    /// with `503` when the backlog is already at its ceiling) and then waits for
    /// a concurrency permit. Bounds both the semaphore wait queue and the
    /// blocking-pool backlog, so an extreme flood fails fast instead of growing
    /// memory without limit. Hold the returned permit for the whole blocking job
    /// (move it into the `spawn_blocking` closure).
    pub(crate) async fn admit_cpu_work(&self) -> Result<CpuWorkPermit, HttpError> {
        let slot = CpuWorkSlot::try_reserve(&self.cpu_work_inflight, self.cpu_work_max_inflight)
            .ok_or_else(|| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "server overloaded".to_string(),
                )
            })?;
        let permit = self
            .cpu_work_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cpu work is shutting down".to_string(),
                )
            })?;
        Ok(CpuWorkPermit {
            _permit: permit,
            _slot: slot,
        })
    }

    pub(crate) fn derived_negative_ttl(&self) -> Duration {
        self.derived_negative_ttl
    }
}

/// Applies the shared middleware stack (drain gate, metrics, request-id) to a
/// router and binds the `AppState`.
fn with_common_layers(router: Router<AppState>, state: AppState) -> Router {
    router
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(
            state.drain.clone(),
            reject_when_draining,
        ))
        .layer(middleware::from_fn_with_state(
            state.metrics.clone(),
            track_http_metrics,
        ))
        .layer(middleware::from_fn(propagate_request_id))
        .with_state(state)
}

/// Public-facing routes (served on the Gateway-fronted port): content plus the
/// top-level `/livez` `/readyz` health endpoints (k8s convention, matching the
/// sibling `biei` service). Metrics, `/_internal/*` and peer-to-peer forwarding
/// live only on the internal router so they are never reachable on the public
/// port.
fn public_router() -> Router<AppState> {
    Router::new()
        // Top-level health, mirrored as `/_internal/{healthz,readyz}` on the
        // internal port. Liveness is `/livez`, readiness is `/readyz`.
        .route("/livez", get(healthz))
        .route("/readyz", get(readyz))
        .route(
            "/tilesets/{tileset_id}",
            get(server::tileset::tilejson_handler),
        )
        .route(
            "/tilesets/{tileset_id}/preview",
            get(server::tileset::preview_handler),
        )
        .route(
            "/tilesets/{tileset_id}/preview.json",
            get(server::tileset::preview_style_handler),
        )
        .route(
            "/tilesets/{tileset_id}/{z}/{x}/{y}",
            get(server::tileset::tile_handler),
        )
        .route(
            "/tilesets/{tileset_id}/derived/{product}",
            get(server::tileset::derived_tilejson_handler),
        )
        .route(
            "/tilesets/{tileset_id}/derived/{product}/{z}/{x}/{y}",
            get(server::tileset::derived_tile_handler),
        )
        // Namespaced tileset keys ({namespace}/{tileset_id}). Static `preview`
        // / `preview.json` second segments take priority over the namespaced
        // TileJSON route, so they stay reachable as flat-tileset previews.
        .route(
            "/tilesets/{namespace}/{tileset_id}",
            get(server::tileset::namespaced_tilejson_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/preview",
            get(server::tileset::namespaced_preview_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/preview.json",
            get(server::tileset::namespaced_preview_style_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/{z}/{x}/{y}",
            get(server::tileset::namespaced_tile_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/derived/{product}",
            get(server::tileset::namespaced_derived_tilejson_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/derived/{product}/{z}/{x}/{y}",
            get(server::tileset::namespaced_derived_tile_handler),
        )
        .route("/styles/{*style_path}", get(server::style::style_handler))
        .route(
            "/fonts/{fontstack}/{range}",
            get(server::glyph::glyph_handler),
        )
}

/// Cluster-internal routes (served on a separate port that is NOT exposed
/// through the Gateway): operational endpoints and peer-to-peer forwarding.
/// All operational endpoints are namespaced under `/_internal/`
/// (`healthz`/`readyz`/`metrics`), matching the sibling `biei` service.
fn internal_router() -> Router<AppState> {
    Router::new()
        .route("/_internal/healthz", get(healthz))
        .route("/_internal/readyz", get(readyz))
        .route("/_internal/metrics", get(metrics_handler))
        .route("/_internal/cluster", get(cluster_handler))
        .route(
            "/_internal/tiles/{tileset_id}/{tile_id}",
            get(server::tileset::internal_tile_handler),
        )
        .route(
            "/_internal/pmtiles/{tileset_id}/bootstrap",
            get(server::internal::internal_bootstrap_handler),
        )
        .route(
            "/_internal/pmtiles/{tileset_id}/leaf/{offset}/{length}",
            get(server::internal::internal_leaf_handler),
        )
        .route(
            "/_internal/provider/styles/{*style_path}",
            get(server::style::internal_style_handler),
        )
        .route(
            "/_internal/provider/fonts/{fontstack}/{range}",
            get(server::glyph::internal_glyph_handler),
        )
}

/// Builds a `200 OK` response carrying `body` with the given content type and an
/// optional `Cache-Control`. Shared by the glyph / sprite / internal handlers so
/// the status/header boilerplate lives in one place.
pub(crate) fn bytes_response(
    body: impl Into<axum::body::Body>,
    content_type: &'static str,
    cache_control: Option<&'static str>,
) -> Response {
    let mut out = Response::new(body.into());
    *out.status_mut() = StatusCode::OK;
    out.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Some(cache_control) = cache_control {
        out.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(cache_control),
        );
    }
    out
}

/// Serves the public router on `public_addr` (Gateway-fronted) and the internal
/// router on `internal_addr` (cluster-internal: metrics, peer forwarding). Both
/// shut down gracefully on the shared `shutdown` signal.
pub async fn run_http_server(
    state: AppState,
    public_addr: SocketAddr,
    internal_addr: SocketAddr,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let public = with_common_layers(public_router(), state.clone());
    let internal = with_common_layers(internal_router(), state);

    let public_listener = TcpListener::bind(public_addr)
        .await
        .with_context(|| format!("failed to bind public {public_addr}"))?;
    let internal_listener = TcpListener::bind(internal_addr)
        .await
        .with_context(|| format!("failed to bind internal {internal_addr}"))?;

    // Fan the single shutdown signal out to both servers.
    let (sd_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let mut rx_pub = sd_tx.subscribe();
    let mut rx_internal = sd_tx.subscribe();
    tokio::spawn(async move {
        shutdown.await;
        let _ = sd_tx.send(());
    });

    let public_srv = axum::serve(
        public_listener,
        ServiceExt::<axum::http::Request<axum::body::Body>>::into_make_service(public),
    )
    .with_graceful_shutdown(async move {
        let _ = rx_pub.recv().await;
    });
    let internal_srv = axum::serve(
        internal_listener,
        ServiceExt::<axum::http::Request<axum::body::Body>>::into_make_service(internal),
    )
    .with_graceful_shutdown(async move {
        let _ = rx_internal.recv().await;
    });

    // try_join! so an unexpected listener error surfaces immediately and the
    // other server is dropped, rather than blocking until both finish.
    tokio::try_join!(
        async { public_srv.await.context("public http server failed") },
        async { internal_srv.await.context("internal http server failed") },
    )?;
    Ok(())
}

pub(crate) fn get_origin(headers: &HeaderMap) -> String {
    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty());
    let origin_parts = origin.and_then(split_origin);
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .or_else(|| origin_parts.map(|(origin_scheme, _)| origin_scheme))
        // Reflect only real web schemes. A spoofed `X-Forwarded-Proto` such as
        // `https://attacker/x?` would otherwise be interpolated as the scheme and
        // point emitted glyph/sprite/tile URLs off-origin.
        .filter(|value| is_reflectable_scheme(value))
        .unwrap_or("http");
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| is_reflectable_host(value))
        .or_else(|| {
            origin_parts
                .map(|(_, origin_host)| origin_host)
                .filter(|value| is_reflectable_host(value))
        })
        .unwrap_or("127.0.0.1:8080");
    format!("{scheme}://{host}")
}

/// Whether a client-supplied `Host`/`Origin` host is safe to interpolate into
/// emitted URLs (TileJSON `tiles`, style `glyphs`/`sprite`/source URLs). A spoofed
/// `Host` is otherwise reflected verbatim — a header-injection / reflected-URL
/// vector — so restrict it to the characters a real authority can contain.
fn is_reflectable_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 255
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b':' | b'_'))
}

/// Whether a client-supplied forwarded scheme is safe to reflect into emitted
/// URLs. Only `http`/`https`; anything else falls back to the default.
fn is_reflectable_scheme(scheme: &str) -> bool {
    scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
}

/// Reports whether this node process is alive.
async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Reports whether this node is ready to receive traffic.
async fn readyz(State(state): State<AppState>) -> StatusCode {
    // Chitchat initialization completes before the HTTP server starts. A node
    // can serve from object storage even when it is the cluster's only member,
    // so readiness is gated only by the process-local drain state.
    if state.drain.is_draining() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

/// Serves the default 404 response for unknown routes.
async fn not_found() -> (StatusCode, &'static str) {
    (StatusCode::NOT_FOUND, "not found\n")
}

/// Returns the current cluster membership snapshot.
async fn cluster_handler(State(state): State<AppState>) -> Json<crate::membership::ClusterView> {
    Json(state.membership.cluster_view().await)
}

/// Rejects new data and peer-forwarding requests with `503` while draining, so
/// callers fail over quickly. In-flight requests already past this layer finish
/// normally, and operational endpoints stay available.
async fn reject_when_draining(
    State(drain): State<DrainController>,
    request: Request,
    next: Next,
) -> Response {
    if drain.is_draining() && drain::is_drainable_path(request.uri().path()) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            "draining\n",
        )
            .into_response();
    }
    next.run(request).await
}

/// Accepts or generates an `X-Request-Id`, scopes it for the task, attaches it
/// to a tracing span, and echoes it on the response.
async fn propagate_request_id(request: Request, next: Next) -> Response {
    let incoming = request
        .headers()
        .get(request_id::HEADER)
        .and_then(|value| value.to_str().ok());
    let id = request_id::accept_or_generate(incoming);

    let header_value = HeaderValue::from_str(&id).ok();
    let span = tracing::info_span!("request", request_id = %id);
    let mut response = request_id::REQUEST_ID
        .scope(id, next.run(request).instrument(span))
        .await;

    if let Some(value) = header_value {
        response
            .headers_mut()
            .insert(HeaderName::from_static(request_id::HEADER), value);
    }
    response
}

/// Records each request against its matched route pattern and status code.
async fn track_http_metrics(
    State(metrics): State<NodeMetrics>,
    matched: Option<MatchedPath>,
    request: Request,
    next: Next,
) -> Response {
    let endpoint = matched
        .as_ref()
        .map(MatchedPath::as_str)
        .unwrap_or("unknown")
        .to_string();
    let response = next.run(request).await;
    metrics.record_http(&endpoint, response.status().as_u16());
    response
}

/// Serves the Prometheus exposition, refreshing point-in-time gauges first.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let view = state.membership.cluster_view().await;
    state
        .metrics
        .set_membership(view.live_ids.len() as i64, view.dead_ids.len() as i64);
    state.metrics.set_drain(state.drain.is_draining());
    state
        .metrics
        .set_cache_bytes("tile", state.resource_resolver.tile_cache_weighted_size());
    state
        .metrics
        .set_cache_bytes("chunk", state.resource_resolver.chunk_cache_weighted_size());
    state
        .metrics
        .set_cache_bytes("provider", state.provider_fetch_cache.weighted_size());
    state
        .metrics
        .sync_backend_fetch_bytes(state.resource_resolver.received_bytes());
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.encode(),
    )
}

/// Splits an Origin header into scheme and host components.
fn split_origin(origin: &str) -> Option<(&str, &str)> {
    let (scheme, rest) = origin.split_once("://")?;
    let host = rest.split('/').next()?;
    if scheme.is_empty() || host.is_empty() {
        return None;
    }
    Some((scheme, host))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::{
        CpuWorkSlot, DecodedDemExpiry, DerivedTileExpiry, get_origin, is_reflectable_host,
    };
    use axum::http::{HeaderValue, header};
    use moka::Expiry;

    use crate::server::tileset::terrain::DerivedOutcome;

    #[test]
    fn cpu_work_admission_sheds_at_ceiling_and_releases_on_drop() {
        let inflight = Arc::new(AtomicUsize::new(0));
        // Fill the two slots.
        let first = CpuWorkSlot::try_reserve(&inflight, 2).expect("first slot");
        let second = CpuWorkSlot::try_reserve(&inflight, 2).expect("second slot");
        // The third is shed while the counter is at its ceiling, and the failed
        // reservation must not leave the counter inflated.
        assert!(CpuWorkSlot::try_reserve(&inflight, 2).is_none());
        assert_eq!(inflight.load(std::sync::atomic::Ordering::Relaxed), 2);
        // Freeing one slot re-opens admission.
        drop(first);
        let third = CpuWorkSlot::try_reserve(&inflight, 2).expect("slot after release");
        drop(second);
        drop(third);
        assert_eq!(inflight.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn rejects_hosts_with_injection_chars() {
        assert!(is_reflectable_host("ishikari-demo.mierune.dev"));
        assert!(is_reflectable_host("127.0.0.1:8080"));
        assert!(!is_reflectable_host("evil.test/path"));
        assert!(!is_reflectable_host("evil.test foo"));
        assert!(!is_reflectable_host(""));
    }

    #[test]
    fn get_origin_does_not_reflect_a_spoofed_host() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("good.example:8080"));
        assert_eq!(get_origin(&headers), "http://good.example:8080");

        // A `Host` carrying a path separator is dropped, not reflected verbatim.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("a.test/evil"));
        assert_eq!(get_origin(&headers), "http://127.0.0.1:8080");
    }

    #[test]
    fn get_origin_rejects_spoofed_forwarded_scheme() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("good.example"));
        // A forwarded-proto that smuggles an authority is not reflected as the
        // scheme; it falls back to the default `http`.
        headers.insert(
            "x-forwarded-proto",
            HeaderValue::from_static("https://attacker.example/x?"),
        );
        assert_eq!(get_origin(&headers), "http://good.example");

        // A legitimate forwarded scheme is honored.
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert_eq!(get_origin(&headers), "https://good.example");
    }

    #[test]
    fn only_negative_derived_results_expire() {
        let expiry = DerivedTileExpiry {
            negative_ttl: Duration::from_secs(45),
        };
        let key = crate::server::tileset::terrain::DerivedTileKey::for_test();
        assert_eq!(
            expiry.expire_after_create(&key, &DerivedOutcome::Absent, Instant::now(),),
            Some(Duration::from_secs(45))
        );
        assert_eq!(
            expiry.expire_after_create(
                &key,
                &DerivedOutcome::Tile(crate::pmtiles::TileData {
                    bytes: bytes::Bytes::new(),
                    content_type: "application/vnd.mapbox-vector-tile",
                    content_encoding: None,
                }),
                Instant::now(),
            ),
            None
        );
    }

    #[test]
    fn absent_decoded_dems_expire() {
        let expiry = DecodedDemExpiry {
            negative_ttl: Duration::from_secs(30),
        };
        let key = (crate::interned::TilesetId::new_unchecked("terrain"), 1);
        assert_eq!(
            expiry.expire_after_create(&key, &None, Instant::now()),
            Some(Duration::from_secs(30))
        );
    }
}

pub(crate) mod cache;
pub(crate) mod glyph;
pub mod internal;
pub mod provider;
pub(crate) mod sprite;
pub(crate) mod style;
pub mod tileset;
pub(crate) mod upstream;
