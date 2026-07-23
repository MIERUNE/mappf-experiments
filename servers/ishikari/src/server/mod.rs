//! HTTP app wiring and shared state.

use std::{future::Future, net::SocketAddr, time::Duration};

use crate::drain::{self, DrainController};
use crate::{membership::ClusterView, request_id, server};
use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{MatchedPath, Request, State},
    http::{HeaderName, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use ishikari_core::metrics::NodeMetrics;
use mmpf_http::operational::{
    INTERNAL_LIVENESS_PATH, INTERNAL_METRICS_PATH, INTERNAL_READINESS_PATH, PUBLIC_LIVENESS_PATH,
    PUBLIC_READINESS_PATH,
};
use tokio::net::TcpListener;
use tracing::Instrument;

pub(crate) type HttpError = (StatusCode, String);

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

fn with_public_layers(router: Router<AppState>, state: AppState) -> Router {
    let router = router.route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth::authorize_delivery,
    ));
    with_common_layers(router, state)
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
        .route(PUBLIC_LIVENESS_PATH, get(healthz))
        .route(PUBLIC_READINESS_PATH, get(readyz))
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
        .route(INTERNAL_LIVENESS_PATH, get(healthz))
        .route(INTERNAL_READINESS_PATH, get(readyz))
        .route(INTERNAL_METRICS_PATH, get(metrics_handler))
        .route("/_internal/cluster", get(cluster_handler))
        .route(
            "/_internal/tiles/{tileset_id}/{tile_id}",
            get(server::tileset::internal_tile_handler),
        )
        .route(
            "/_internal/derived/{tileset_id}/{product}/{z}/{x}/{y}",
            get(server::tileset::internal_derived_tile_handler),
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

/// Serves the public router on `public_addr` (Gateway-fronted) and the internal
/// router on `internal_addr` (cluster-internal: metrics, peer forwarding). Both
/// shut down gracefully on the shared `shutdown` signal.
// Keep the complete shutdown path below the deployment's 25-second
// `terminationGracePeriodSeconds`: runtime drain publication/propagation uses at
// most four seconds and final membership teardown uses at most three.
pub(crate) const HTTP_SHUTDOWN_GRACE: Duration = Duration::from_secs(15);

pub(crate) async fn run_http_server(
    state: AppState,
    public_addr: SocketAddr,
    internal_addr: SocketAddr,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let public = with_public_layers(public_router(), state.clone());
    let internal = with_common_layers(internal_router(), state);

    let public_listener = TcpListener::bind(public_addr)
        .await
        .with_context(|| format!("failed to bind public {public_addr}"))?;
    let internal_listener = TcpListener::bind(internal_addr)
        .await
        .with_context(|| format!("failed to bind internal {internal_addr}"))?;

    // Give admitted requests a finite grace, then drop surviving connections so
    // pod termination remains bounded. No `ConnectInfo` is used, so the plain
    // `Router` serves identically to the previous `into_make_service` form.
    mmpf_http::serve::serve_dual(
        (public_listener, public),
        (internal_listener, internal),
        shutdown,
        Some(HTTP_SHUTDOWN_GRACE),
    )
    .await
}

/// Reports whether this node process is alive.
async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Reports whether this node is ready to receive traffic.
async fn readyz(State(state): State<AppState>) -> StatusCode {
    if state.drain.is_draining() || !state.is_gossip_bootstrap_ready().await {
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
async fn cluster_handler(State(state): State<AppState>) -> Json<ClusterView> {
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

    let header_value = HeaderValue::from_str(id.as_str()).ok();
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
    let started = std::time::Instant::now();
    let internal_resource = ishikari_core::storage::internal_resource_kind(request.uri().path());
    // Keep the cheap, owned `MatchedPath` (`Arc<str>`) across the await rather
    // than borrowing a label while the request is moved into `next.run`.
    let response = next.run(request).await;
    let endpoint = matched
        .as_ref()
        .map(MatchedPath::as_str)
        .unwrap_or("unknown");
    // Exclude the scrape itself: its handler performs cache-gauge maintenance,
    // and recording that work in the exported histogram makes scrape latency
    // self-referential on the following scrape.
    if endpoint == INTERNAL_METRICS_PATH {
        metrics.record_http_request(endpoint, response.status().as_str());
    } else {
        metrics.record_http(endpoint, response.status().as_str(), started.elapsed());
    }
    if let Some(resource) = internal_resource {
        let outcome = match response.status() {
            status if status.is_success() => "success",
            StatusCode::NOT_FOUND => "not_found",
            StatusCode::TOO_MANY_REQUESTS
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => "retryable",
            _ => "error",
        };
        metrics.record_internal_resource_request(resource, outcome);
    }
    response
}

/// Serves the Prometheus exposition, refreshing point-in-time gauges first.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let view = state.membership.cluster_view().await;
    // Moka updates weighted size through deferred maintenance. Flush once for
    // concurrent scrapes and run the independent caches in parallel.
    if let Some(_guard) = state.try_start_cache_maintenance() {
        tokio::join!(
            state.mlt_cache.run_pending_tasks(),
            state.derived_tile_cache.run_pending_tasks(),
            state.dem_tile_cache.run_pending_tasks(),
        );
    }
    state
        .metrics
        .set_membership(view.live_ids.len() as i64, view.dead_ids.len() as i64);
    state.metrics.set_drain(state.drain.is_draining());
    let cpu_work = state.cpu_work_gate.snapshot();
    state.metrics.set_cpu_work(
        cpu_work.inflight,
        cpu_work.running,
        cpu_work.concurrency,
        cpu_work.max_inflight,
    );
    state
        .metrics
        .set_cache_bytes("tile", state.resource_resolver.tile_cache_weighted_size());
    state
        .metrics
        .set_cache_bytes("chunk", state.resource_resolver.chunk_cache_weighted_size());
    state.metrics.set_cache_bytes(
        "resource",
        state.resource_resolver.resource_cache_weighted_size(),
    );
    let (archive_cache_bytes, leaf_cache_bytes) =
        state.resource_resolver.pmtiles_index_cache_weighted_sizes();
    state
        .metrics
        .set_cache_bytes("archive", archive_cache_bytes);
    state.metrics.set_cache_bytes("leaf", leaf_cache_bytes);
    state
        .metrics
        .set_cache_bytes("provider", state.provider_fetcher.weighted_size());
    state
        .metrics
        .set_cache_bytes("mlt", state.mlt_cache.weighted_size());
    state
        .metrics
        .set_cache_bytes("derived", state.derived_tile_cache.weighted_size());
    state
        .metrics
        .set_cache_bytes("dem", state.dem_tile_cache.weighted_size());
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.encode(),
    )
}

mod auth;
pub(crate) mod cache;
pub(crate) mod conditional;
#[cfg(test)]
mod contract_tests;
mod cpu_work;
pub(crate) mod glyph;
pub(crate) mod internal;
pub(crate) mod provider;
mod provider_body;
mod provider_cache_policy;
mod response;
pub(crate) use response::{apply_origin_vary, bytes_response, derived_json_response, get_origin};
mod state;
pub(crate) use state::{AppState, ServerRuntimeConfig};
pub(crate) mod sprite;
pub(crate) mod style;
pub(crate) mod tileset;
pub(crate) mod upstream;
