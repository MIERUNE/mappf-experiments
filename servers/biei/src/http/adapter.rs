//! Axum adapter for production HTTP ingress.
//!
//! URL parsing and response classification stay in `http::ingress`; this module
//! only binds a socket and converts that small internal response shape into an
//! HTTP response.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::body::Body;
use axum::body::to_bytes;
use axum::extract::{Extension, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderValue, Method, Request, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{any, get, post};
use mmpf_cluster::BootstrapReadinessGate;
use mmpf_http::operational::{
    INTERNAL_LIVENESS_PATH, INTERNAL_METRICS_PATH, INTERNAL_READINESS_PATH, PUBLIC_LIVENESS_PATH,
    PUBLIC_READINESS_PATH,
};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::http::REQUEST_ID_HEADER;
use crate::http::ingress::HttpIngress;
use crate::http::metrics::{HttpMetrics, RequestEndpoint};
use crate::http::request_id_from_headers;
use crate::http::response::{IngressResponse, PRIVATE_NO_STORE_CACHE_CONTROL};
use biei_core::types::RequestId;

const MAX_INTERNAL_FORWARD_BODY_BYTES: usize = 10 * 1024 * 1024;
const INTERNAL_FORWARD_BODY_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_SHUTDOWN_GRACE: Duration = Duration::from_secs(12);
const MAX_PUBLIC_PATH_BYTES: usize = 8192;

#[derive(Clone)]
pub(crate) struct ShutdownSignal {
    rx: watch::Receiver<bool>,
}

pub(crate) fn shutdown_channel() -> (watch::Sender<bool>, ShutdownSignal) {
    let (tx, rx) = watch::channel(false);
    (tx, ShutdownSignal { rx })
}

impl ShutdownSignal {
    pub(crate) fn is_triggered(&self) -> bool {
        *self.rx.borrow()
    }

    async fn wait(mut self) {
        if *self.rx.borrow() {
            return;
        }
        let _ = self.rx.changed().await;
    }
}

#[derive(Clone)]
struct HttpServerState {
    ingress: Option<HttpIngress>,
    drain: Option<crate::drain::DrainController>,
    membership: Option<MembershipReadiness>,
    internal_forward: Option<crate::http::internal::InternalForwardEndpoint>,
    metrics: Option<HttpMetrics>,
    renderer_supervisor: Option<crate::renderer::actor::RendererActorSupervisor>,
}

#[derive(Clone)]
struct MembershipReadiness {
    membership: crate::membership::Membership,
    bootstrap: BootstrapReadinessGate,
}

impl MembershipReadiness {
    async fn is_ready(&self) -> bool {
        let observed = self.bootstrap.observe_with_logging(false);
        if observed.is_ready() {
            return true;
        }

        let has_peer = self.membership.has_other_live_node().await;
        self.bootstrap.observe_with_logging(has_peer).is_ready()
    }
}

// Single-node / local: one listener serving the combined router (public render
// plus health/metrics/forward). Not fronted by a Gateway, so no port split.
pub(crate) async fn serve_with_shutdown(
    ingress: HttpIngress,
    bind: SocketAddr,
    shutdown: Option<ShutdownSignal>,
) -> anyhow::Result<()> {
    let drain = ingress.drain_controller();
    let renderer_supervisor = ingress.renderer_supervisor();
    let metrics = Some(HttpMetrics::new(
        ingress.node(),
        None,
        drain.clone(),
        renderer_supervisor.clone(),
    ));
    serve_with_state(
        HttpServerState {
            drain,
            ingress: Some(ingress),
            membership: None,
            internal_forward: None,
            metrics,
            renderer_supervisor: Some(renderer_supervisor),
        },
        bind,
        None,
        shutdown,
    )
    .await
}

// Cluster: a Gateway-fronted public listener (`public_bind`) serving render plus
// top-level `/livez` `/readyz`, and a separate cluster-internal listener
// (`internal_bind`) serving `/_internal/*`, metrics and peer forwarding. The
// internal port is never exposed through the Gateway.
pub(crate) async fn serve_with_shutdown_and_membership_and_internal_forward(
    ingress: HttpIngress,
    public_bind: SocketAddr,
    internal_bind: SocketAddr,
    shutdown: Option<ShutdownSignal>,
    membership: crate::membership::Membership,
    gossip_bootstrap_readiness: BootstrapReadinessGate,
    internal_forward: Option<crate::http::internal::InternalForwardEndpoint>,
) -> anyhow::Result<()> {
    let drain = ingress.drain_controller();
    let renderer_supervisor = ingress.renderer_supervisor();
    let metrics = Some(HttpMetrics::new(
        ingress.node(),
        Some(membership.clone()),
        drain.clone(),
        renderer_supervisor.clone(),
    ));
    let membership = MembershipReadiness {
        membership,
        bootstrap: gossip_bootstrap_readiness,
    };
    serve_with_state(
        HttpServerState {
            drain,
            ingress: Some(ingress),
            membership: Some(membership),
            internal_forward,
            metrics,
            renderer_supervisor: Some(renderer_supervisor),
        },
        public_bind,
        Some(internal_bind),
        shutdown,
    )
    .await
}

async fn serve_with_state(
    state: HttpServerState,
    public_bind: SocketAddr,
    internal_bind: Option<SocketAddr>,
    shutdown: Option<ShutdownSignal>,
) -> anyhow::Result<()> {
    let Some(internal_bind) = internal_bind else {
        // Single listener, combined router.
        let listener = TcpListener::bind(public_bind)
            .await
            .with_context(|| format!("bind HTTP listener on {public_bind}"))?;
        let server = axum::serve(listener, combined_router(state));
        if let Some(signal) = shutdown {
            let force_shutdown = signal.clone();
            tokio::select! {
                result = server.with_graceful_shutdown(signal.wait()) => {
                    result.context("serve HTTP listener")?;
                }
                () = shutdown_grace_elapsed(force_shutdown) => tracing::warn!(
                    grace_ms = HTTP_SHUTDOWN_GRACE.as_millis(),
                    "HTTP shutdown grace elapsed; dropping active connections"
                ),
            }
        } else {
            server.await.context("serve HTTP listener")?;
        }
        return Ok(());
    };

    let public_listener = TcpListener::bind(public_bind)
        .await
        .with_context(|| format!("bind public HTTP listener on {public_bind}"))?;
    let internal_listener = TcpListener::bind(internal_bind)
        .await
        .with_context(|| format!("bind internal listener on {internal_bind}"))?;
    let public = public_router(state.clone());
    let internal = internal_router(state);

    // The shared harness owns the shutdown fan-out and `try_join!`. With a
    // shutdown signal we force-drop remaining connections after
    // `HTTP_SHUTDOWN_GRACE`; without one there is nothing to fan out, so the
    // shutdown future never fires and both listeners serve until an error.
    let force_grace = shutdown.as_ref().map(|_| HTTP_SHUTDOWN_GRACE);
    let shutdown = async move {
        match shutdown {
            Some(signal) => signal.wait().await,
            None => std::future::pending::<()>().await,
        }
    };
    mmpf_http::serve::serve_dual(
        (public_listener, public),
        (internal_listener, internal),
        shutdown,
        force_grace,
    )
    .await
}

async fn shutdown_grace_elapsed(signal: ShutdownSignal) {
    signal.wait().await;
    tokio::time::sleep(HTTP_SHUTDOWN_GRACE).await;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ListenerScope {
    Standalone,
    Public,
    Internal,
}

#[derive(Clone, Copy)]
enum EndpointClassification {
    Fixed(RequestEndpoint),
    Render,
}

impl EndpointClassification {
    fn with_status(self, status: StatusCode) -> RequestEndpoint {
        match self {
            Self::Fixed(endpoint) => endpoint,
            Self::Render if status == StatusCode::NOT_FOUND => RequestEndpoint::NotFound,
            Self::Render => RequestEndpoint::Render,
        }
    }
}

#[derive(Clone)]
struct RequestMetricsState {
    metrics: Option<HttpMetrics>,
    scope: ListenerScope,
}

/// Middleware that tallies every HTTP response into `biei_http_requests_total`,
/// so early rejections (method, URI length, parse, ingress admission, unknown
/// route) that never create a core task are still observable and reconcilable
/// with core totals. Records after the response so a `/metrics` scrape never
/// counts its own in-flight request.
async fn record_request_metrics(
    State(state): State<RequestMetricsState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // A `Router::layer` middleware runs outside routing, so `MatchedPath` is not
    // yet set. Classify the borrowed path before moving the request instead of
    // allocating a path String on every request.
    let classification = classify_path(state.scope, request.uri().path());
    let response = next.run(request).await;
    if let Some(metrics) = state.metrics.as_ref() {
        let endpoint = classification.with_status(response.status());
        metrics.record_request(endpoint, response.status().as_u16());
    }
    response
}

/// Map a trusted listener scope and borrowed request path onto a fixed
/// classification. Scope is router configuration, never attacker input, so a
/// public refusal cannot impersonate an internal endpoint label.
fn classify_path(scope: ListenerScope, path: &str) -> EndpointClassification {
    let fixed = |endpoint| EndpointClassification::Fixed(endpoint);
    match scope {
        ListenerScope::Public => {
            if path == PUBLIC_LIVENESS_PATH {
                fixed(RequestEndpoint::Health)
            } else if path == PUBLIC_READINESS_PATH {
                fixed(RequestEndpoint::Ready)
            } else if path == "/metrics" || path == "/_internal" || path.starts_with("/_internal/")
            {
                fixed(RequestEndpoint::NotFound)
            } else {
                EndpointClassification::Render
            }
        }
        ListenerScope::Internal => {
            if path == INTERNAL_LIVENESS_PATH {
                fixed(RequestEndpoint::Health)
            } else if path == INTERNAL_READINESS_PATH {
                fixed(RequestEndpoint::Ready)
            } else if path == INTERNAL_METRICS_PATH {
                fixed(RequestEndpoint::Metrics)
            } else if path == "/_internal/forward" {
                fixed(RequestEndpoint::InternalForward)
            } else {
                fixed(RequestEndpoint::NotFound)
            }
        }
        ListenerScope::Standalone => {
            if path == PUBLIC_LIVENESS_PATH || path == INTERNAL_LIVENESS_PATH {
                fixed(RequestEndpoint::Health)
            } else if path == PUBLIC_READINESS_PATH || path == INTERNAL_READINESS_PATH {
                fixed(RequestEndpoint::Ready)
            } else if path == INTERNAL_METRICS_PATH {
                fixed(RequestEndpoint::Metrics)
            } else if path == "/_internal/forward" {
                fixed(RequestEndpoint::InternalForward)
            } else if path == "/metrics" || path == "/_internal" || path.starts_with("/_internal/")
            {
                fixed(RequestEndpoint::NotFound)
            } else {
                EndpointClassification::Render
            }
        }
    }
}

#[cfg(test)]
fn classify_endpoint(scope: ListenerScope, path: &str, status: StatusCode) -> RequestEndpoint {
    classify_path(scope, path).with_status(status)
}

/// Dynamic public content only. Future public authentication can wrap this
/// subrouter without inspecting path strings or touching operational routes.
fn public_content_routes() -> Router<HttpServerState> {
    Router::new().fallback(public_render)
}

/// Operational and internal routes retained on the standalone listener. They
/// remain explicit even though single-node deployments use one socket.
fn standalone_operational_routes() -> Router<HttpServerState> {
    Router::new()
        .route(PUBLIC_LIVENESS_PATH, get(healthz))
        .route(PUBLIC_READINESS_PATH, get(readyz))
        .route(INTERNAL_LIVENESS_PATH, get(healthz))
        .route(INTERNAL_READINESS_PATH, get(readyz))
        .route(INTERNAL_METRICS_PATH, get(metricsz))
        .route("/_internal/forward", post(forwardz))
}

async fn enforce_public_response_cache_policy(request: Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    if !response.status().is_success() {
        response.headers_mut().insert(
            CACHE_CONTROL,
            HeaderValue::from_static(PRIVATE_NO_STORE_CACHE_CONTROL),
        );
    }
    response
}

/// Establish one correlation ID before any public or standalone route can exit,
/// make it available to ingress, and stamp the final response exactly once.
async fn attach_public_request_id(mut request: Request<Body>, next: Next) -> Response {
    let request_id = request_id_from_headers(request.headers()).unwrap_or_default();
    request.extensions_mut().insert(request_id.clone());

    let mut response = next.run(request).await;
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(request_id.as_str()).expect("validated request ID is a header value"),
    );
    response
}

fn finish_public_router(
    routes: Router<HttpServerState>,
    state: HttpServerState,
    scope: ListenerScope,
) -> Router {
    let metrics_state = RequestMetricsState {
        metrics: state.metrics.clone(),
        scope,
    };
    routes
        .method_not_allowed_fallback(public_method_not_allowed)
        .layer(middleware::from_fn_with_state(
            metrics_state,
            record_request_metrics,
        ))
        .layer(middleware::from_fn(enforce_public_response_cache_policy))
        .layer(middleware::from_fn(attach_public_request_id))
        .with_state(state)
}

/// Combined listener composition for single-node deployments. Public content
/// and operational routes are distinct route sets even when served on one port.
fn combined_router_with_public_content(
    state: HttpServerState,
    public_content: Router<HttpServerState>,
) -> Router {
    finish_public_router(
        standalone_operational_routes().merge(public_content),
        state,
        ListenerScope::Standalone,
    )
}

fn combined_router(state: HttpServerState) -> Router {
    combined_router_with_public_content(state, public_content_routes())
}

fn cluster_public_operational_routes() -> Router<HttpServerState> {
    Router::new()
        .route(PUBLIC_LIVENESS_PATH, get(healthz))
        .route(PUBLIC_READINESS_PATH, get(readyz))
        // Refuse internal namespaces explicitly so the public content fallback
        // can never interpret them as render paths.
        .route("/metrics", any(public_not_found))
        .route("/metrics/", any(public_not_found))
        .route("/_internal", any(public_not_found))
        .route("/_internal/", any(public_not_found))
        .route("/_internal/{*rest}", any(public_not_found))
}

/// Gateway-fronted public listener: public content plus top-level health probes.
/// `/_internal/*` (metrics, peer forwarding) remains on the internal port.
fn public_router_with_public_content(
    state: HttpServerState,
    public_content: Router<HttpServerState>,
) -> Router {
    finish_public_router(
        cluster_public_operational_routes().merge(public_content),
        state,
        ListenerScope::Public,
    )
}

fn public_router(state: HttpServerState) -> Router {
    public_router_with_public_content(state, public_content_routes())
}

/// Cluster-internal listener: metrics, peer forwarding and health. Never
/// exposed through the Gateway; serves no public render paths. Everything is
/// namespaced under `/_internal/*` (matching the sibling `ishikari` service) —
/// top-level `/livez` `/readyz` live only on the public port.
fn internal_router(state: HttpServerState) -> Router {
    Router::new()
        .route(INTERNAL_LIVENESS_PATH, get(healthz))
        .route(INTERNAL_READINESS_PATH, get(readyz))
        .route(INTERNAL_METRICS_PATH, get(metricsz))
        .route("/_internal/forward", post(forwardz))
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(
            RequestMetricsState {
                metrics: state.metrics.clone(),
                scope: ListenerScope::Internal,
            },
            record_request_metrics,
        ))
        .with_state(state)
}

/// Plain-text `404` retained for the cluster-internal listener.
async fn not_found() -> Response {
    simple_response(StatusCode::NOT_FOUND, "not found")
}

/// Plain-text `405` retained for the cluster-internal listener.
async fn method_not_allowed() -> Response {
    simple_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
}

async fn public_not_found() -> Response {
    public_error_response(StatusCode::NOT_FOUND, "not_found")
}

async fn public_method_not_allowed() -> Response {
    public_error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed")
}

async fn healthz(State(state): State<HttpServerState>) -> Response {
    // Provider-correlated loss stays live because restart would discard its
    // warm cache without fixing the provider. Internal unrecoverable loss fails
    // liveness after the worker's autonomous repair path had a chance to run.
    if state
        .renderer_supervisor
        .as_ref()
        .is_none_or(|supervisor| supervisor.is_livable())
    {
        simple_response(StatusCode::OK, "ok")
    } else {
        simple_response(StatusCode::SERVICE_UNAVAILABLE, "renderer unrecoverable")
    }
}

async fn readyz(State(state): State<HttpServerState>) -> Response {
    let ready = state
        .drain
        .as_ref()
        .is_none_or(|drain| !drain.is_draining());
    let ready = ready
        && state
            .renderer_supervisor
            .as_ref()
            .is_none_or(|supervisor| supervisor.is_ready());
    let ready = ready
        && match &state.membership {
            Some(membership) => membership.is_ready().await,
            None => true,
        };
    if ready {
        simple_response(StatusCode::OK, "ready")
    } else {
        simple_response(StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

async fn metricsz(State(state): State<HttpServerState>) -> Response {
    let Some(metrics) = state.metrics.as_ref() else {
        return simple_response(StatusCode::NOT_FOUND, "metrics disabled");
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
        .body(Body::from(metrics.render_prometheus().await))
        .unwrap_or_else(|_| {
            simple_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "metrics response build failed",
            )
        })
}

async fn forwardz(State(state): State<HttpServerState>, request: Request<Body>) -> Response {
    let Some(internal_forward) = state.internal_forward.as_ref() else {
        return simple_response(StatusCode::NOT_FOUND, "internal forward disabled");
    };
    let Some(admission) = internal_forward.try_admit() else {
        return crate::http::internal::overloaded_response();
    };
    let (parts, request_body) = request.into_parts();
    let body = match tokio::time::timeout(
        INTERNAL_FORWARD_BODY_TIMEOUT,
        to_bytes(request_body, MAX_INTERNAL_FORWARD_BODY_BYTES),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => return simple_response(StatusCode::PAYLOAD_TOO_LARGE, "body too large"),
        Err(_) => return simple_response(StatusCode::REQUEST_TIMEOUT, "request body timed out"),
    };
    internal_forward
        .handle_admitted(&parts.headers, body, admission)
        .await
}

/// Fallback for the dynamic render space. Unlike the fixed operational routes,
/// this sees every method, so it keeps its own `GET` guard, the URI-length
/// limit, and preview detection.
async fn public_render(
    State(state): State<HttpServerState>,
    Extension(request_id): Extension<RequestId>,
    method: Method,
    uri: Uri,
) -> Response {
    if method != Method::GET {
        return public_error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
    }
    if uri
        .path_and_query()
        .is_some_and(|path_and_query| path_and_query.as_str().len() > MAX_PUBLIC_PATH_BYTES)
    {
        return public_error_response(StatusCode::URI_TOO_LONG, "uri_too_long");
    }
    let Some(ingress) = state.ingress else {
        return public_error_response(StatusCode::NOT_FOUND, "not_found");
    };
    into_axum_response(
        ingress
            .handle_public_path_with_request_id(
                uri.path(),
                uri.query(),
                Some(request_id),
                Instant::now(),
            )
            .await,
    )
}

fn public_error_response(status: StatusCode, code: &'static str) -> Response {
    into_axum_response(IngressResponse::json(status.as_u16(), code, ""))
}

fn into_axum_response(response: IngressResponse) -> Response {
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut builder = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, response.content_type);
    for (name, value) in response.headers {
        builder = builder.header(name, value);
    }
    builder.body(Body::from(response.body)).unwrap_or_else(|_| {
        simple_response(StatusCode::INTERNAL_SERVER_ERROR, "response build failed")
    })
}

fn simple_response(status: StatusCode, body: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(body))
        .unwrap_or_else(|_| Response::new(Body::from(body)))
}

// Test-only dispatch drivers. Rather than re-implementing the route tables,
// these build the real production router (`combined_router` / `public_router` /
// `internal_router`) and drive the request through it via
// `tower::ServiceExt::oneshot`, so the tests exercise the actual routing.
#[cfg(test)]
async fn handle(
    State(state): State<HttpServerState>,
    method: Method,
    uri: Uri,
    request: Request<Body>,
) -> Response {
    route_through(combined_router(state), method, uri, request).await
}

#[cfg(test)]
async fn handle_public(
    State(state): State<HttpServerState>,
    method: Method,
    uri: Uri,
    request: Request<Body>,
) -> Response {
    route_through(public_router(state), method, uri, request).await
}

#[cfg(test)]
async fn handle_internal(
    State(state): State<HttpServerState>,
    method: Method,
    uri: Uri,
    request: Request<Body>,
) -> Response {
    route_through(internal_router(state), method, uri, request).await
}

/// Route a request through a real production router: carry over the passed
/// request's headers and body, override its method and URI, then `oneshot` it
/// through the router. The router's error type is `Infallible`, so the result
/// always resolves to a `Response`.
#[cfg(test)]
async fn route_through(
    router: Router,
    method: Method,
    uri: Uri,
    request: Request<Body>,
) -> Response {
    use tower::ServiceExt;
    let (mut parts, body) = request.into_parts();
    parts.method = method;
    parts.uri = uri;
    router
        .oneshot(Request::from_parts(parts, body))
        .await
        .expect("router is infallible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn empty_state() -> HttpServerState {
        HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: None,
        }
    }

    fn assert_cache_control(response: &Response, expected: &str) {
        assert_eq!(
            response
                .headers()
                .get(CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some(expected),
            "status {}",
            response.status()
        );
    }

    async fn assert_public_error(
        response: Response,
        expected_status: StatusCode,
        expected_code: &str,
        expected_request_id: &str,
    ) {
        assert_eq!(response.status(), expected_status);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert_eq!(
            response.headers().get(REQUEST_ID_HEADER).unwrap(),
            expected_request_id
        );
        assert_eq!(
            response.headers().get_all(REQUEST_ID_HEADER).iter().count(),
            1,
            "response must carry exactly one request ID"
        );
        assert_cache_control(&response, PRIVATE_NO_STORE_CACHE_CONTROL);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"], expected_code);
        assert_eq!(body["detail"], "");
    }

    async fn completed_tile_response() -> Response {
        into_axum_response(IngressResponse::image(
            200,
            "image/png",
            vec![1, 2, 3].into(),
            crate::http::response::PublicResponsePolicy::Tile,
        ))
    }

    #[test]
    fn axum_response_preserves_status_content_type_and_headers() {
        let response = into_axum_response(IngressResponse {
            status: 503,
            content_type: "application/json",
            headers: vec![("Retry-After", "1".to_string())],
            body: br#"{"error":"queue_full","detail":""}"#.to_vec().into(),
        });

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert_eq!(response.headers().get("Retry-After").unwrap(), "1");
    }

    #[test]
    fn internal_method_not_allowed_response_is_plain_text() {
        let response = simple_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    fn classify_endpoint_is_listener_aware_and_bounded() {
        use ListenerScope::{Internal, Public, Standalone};
        use RequestEndpoint::*;
        let ok = StatusCode::OK;
        for (scope, path, status, expected) in [
            (Public, PUBLIC_LIVENESS_PATH, ok, Health),
            (Public, PUBLIC_READINESS_PATH, ok, Ready),
            // Exact internal endpoint names are refusals on the public listener.
            (
                Public,
                INTERNAL_LIVENESS_PATH,
                StatusCode::NOT_FOUND,
                NotFound,
            ),
            (
                Public,
                INTERNAL_READINESS_PATH,
                StatusCode::NOT_FOUND,
                NotFound,
            ),
            (
                Public,
                INTERNAL_METRICS_PATH,
                StatusCode::NOT_FOUND,
                NotFound,
            ),
            (
                Public,
                "/_internal/forward",
                StatusCode::NOT_FOUND,
                NotFound,
            ),
            (Public, "/metrics", StatusCode::NOT_FOUND, NotFound),
            (Public, "/carto/voyager/0/0/0@2x.png", ok, Render),
            (Internal, INTERNAL_LIVENESS_PATH, ok, Health),
            (Internal, INTERNAL_READINESS_PATH, ok, Ready),
            (Internal, INTERNAL_METRICS_PATH, ok, Metrics),
            (
                Internal,
                "/_internal/forward",
                StatusCode::NOT_FOUND,
                InternalForward,
            ),
            (
                Internal,
                PUBLIC_LIVENESS_PATH,
                StatusCode::NOT_FOUND,
                NotFound,
            ),
            (Standalone, INTERNAL_LIVENESS_PATH, ok, Health),
            (Standalone, INTERNAL_READINESS_PATH, ok, Ready),
            (Standalone, INTERNAL_METRICS_PATH, ok, Metrics),
            (
                Standalone,
                "/_internal/forward",
                StatusCode::NOT_FOUND,
                InternalForward,
            ),
            (
                Standalone,
                "/_internal/anything",
                StatusCode::BAD_REQUEST,
                NotFound,
            ),
            (
                Standalone,
                "/unknown-style/0/0/0.png",
                StatusCode::NOT_FOUND,
                NotFound,
            ),
        ] {
            assert_eq!(
                classify_endpoint(scope, path, status),
                expected,
                "scope {scope:?} path {path:?} status {status}"
            );
        }
    }

    #[tokio::test]
    async fn health_and_ready_endpoints_are_plain_text() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: None,
        };
        let request = Request::builder().body(Body::empty()).unwrap();

        let health = handle(
            State(state.clone()),
            Method::GET,
            "/_internal/healthz".parse().unwrap(),
            request,
        )
        .await;
        assert_eq!(health.status(), StatusCode::OK);

        let request = Request::builder().body(Body::empty()).unwrap();
        let ready = handle(
            State(state),
            Method::GET,
            "/_internal/readyz".parse().unwrap(),
            request,
        )
        .await;
        assert_eq!(ready.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_endpoint_reports_not_ready_while_draining() {
        let drain = crate::drain::DrainController::new();
        let state = HttpServerState {
            ingress: None,
            drain: Some(drain.clone()),
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: None,
        };

        let request = Request::builder().body(Body::empty()).unwrap();
        let ready = handle(
            State(state.clone()),
            Method::GET,
            "/_internal/readyz".parse().unwrap(),
            request,
        )
        .await;
        assert_eq!(ready.status(), StatusCode::OK);

        drain.begin_draining();

        let request = Request::builder().body(Body::empty()).unwrap();
        let ready = handle(
            State(state),
            Method::GET,
            "/_internal/readyz".parse().unwrap(),
            request,
        )
        .await;
        assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn renderer_unavailability_gates_readiness_and_liveness() {
        let supervisor = crate::renderer::actor::RendererActorSupervisor::new(2);
        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: Some(supervisor.clone()),
        };

        // `slot_available=false` is set only after replacement was refused or
        // failed. Even with another healthy slot, readiness drains traffic
        // immediately; the much slower liveness threshold may later restart
        // the permanently reduced process.
        let ready = handle(
            State(state.clone()),
            Method::GET,
            "/readyz".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE, "/readyz");
        let live = handle(
            State(state.clone()),
            Method::GET,
            "/livez".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(
            live.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "/livez once a renderer slot cannot be replaced"
        );
    }

    #[tokio::test]
    async fn active_provider_retry_keeps_degraded_cache_endpoint_ready_and_live() {
        let provider = mmpf_mln_filesource::ProviderHealthTracker::new();
        let supervisor = crate::renderer::actor::RendererActorSupervisor::with_provider_health(
            2,
            provider.clone(),
        );
        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);
        let retry = provider.begin_retry();
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: Some(supervisor.clone()),
        };

        for path in ["/readyz", "/livez"] {
            let response = handle(
                State(state.clone()),
                Method::GET,
                path.parse().unwrap(),
                Request::builder().body(Body::empty()).unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
        }
        assert!(
            supervisor.can_start_render(),
            "readiness preserves cache reachability, and the remaining healthy slot still renders while externally degraded"
        );

        drop(retry);
        let live = handle(
            State(state),
            Method::GET,
            "/livez".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(live.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    async fn mark_public_content(request: Request<Body>, next: Next) -> Response {
        let mut response = next.run(request).await;
        response.headers_mut().insert(
            "x-public-content-layer",
            axum::http::HeaderValue::from_static("applied"),
        );
        response
    }

    #[tokio::test]
    async fn standalone_public_content_layer_excludes_operational_and_internal_routes() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: None,
        };
        let public_content =
            public_content_routes().layer(middleware::from_fn(mark_public_content));
        let router = combined_router_with_public_content(state, public_content);

        let content = route_through(
            router.clone(),
            Method::GET,
            "/carto/voyager/0/0/0.png".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(
            content.headers().get("x-public-content-layer").unwrap(),
            "applied"
        );

        for path in [
            PUBLIC_LIVENESS_PATH,
            INTERNAL_LIVENESS_PATH,
            INTERNAL_METRICS_PATH,
        ] {
            let response = route_through(
                router.clone(),
                Method::GET,
                path.parse().unwrap(),
                Request::builder().body(Body::empty()).unwrap(),
            )
            .await;
            assert!(
                response.headers().get("x-public-content-layer").is_none(),
                "public content layer reached operational path {path}"
            );
        }
    }

    #[tokio::test]
    async fn single_router_routes_public_and_internal_paths() {
        let options =
            crate::options::test_options("http://style-api.test/styles/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let ingress = runtime.http_ingress(Duration::from_secs(2));
        let metrics = Some(HttpMetrics::new(
            runtime.node(),
            None,
            ingress.drain_controller(),
            runtime.renderer_supervisor(),
        ));
        let state = HttpServerState {
            drain: ingress.drain_controller(),
            ingress: Some(ingress),
            membership: None,
            internal_forward: Some(crate::http::internal::InternalForwardEndpoint::with_drain(
                runtime.node(),
                runtime.drain_controller(),
            )),
            metrics,
            renderer_supervisor: Some(runtime.renderer_supervisor()),
        };

        let public = handle(
            State(state.clone()),
            Method::GET,
            "/carto/voyager/static/not-an-overlay/auto/256x256.png"
                .parse()
                .unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(public.status(), StatusCode::BAD_REQUEST);

        let internal = handle(
            State(state),
            Method::POST,
            "/_internal/forward".parse().unwrap(),
            Request::builder()
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from("not json"))
                .unwrap(),
        )
        .await;
        assert_eq!(internal.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn metrics_endpoint_reports_worker_queue_depths() {
        let options =
            crate::options::test_options("http://style-api.test/styles/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: Some(HttpMetrics::new(
                runtime.node(),
                None,
                None,
                runtime.renderer_supervisor(),
            )),
            renderer_supervisor: Some(runtime.renderer_supervisor()),
        };

        let response = handle(
            State(state),
            Method::GET,
            "/_internal/metrics".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("metrics body");
        let body = std::str::from_utf8(&body).expect("utf8 metrics");
        assert!(body.contains("# TYPE biei_queue_depth gauge"));
        assert!(!body.contains("style_id="));
        assert!(body.contains("biei_worker_loaded"));
        assert!(body.contains("biei_native_render_permits_inuse"));
        assert!(body.contains("biei_drain_state"));
        assert!(body.contains("biei_renderer_slots"));
        assert!(body.contains("biei_renderer_orphan_threads"));
        assert!(body.contains("biei_renderer_health"));
        assert!(body.contains("biei_renderer_replacements_total"));
        assert!(body.contains("# TYPE biei_tasks_completed_total counter"));
        assert!(body.contains(r#"scope="ingress"} 0"#));
    }

    #[tokio::test]
    async fn http_request_tally_counts_responses_before_core_tasks() {
        let options =
            crate::options::test_options("http://style-api.test/styles/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let metrics = HttpMetrics::new(runtime.node(), None, None, runtime.renderer_supervisor());
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: Some(metrics),
            renderer_supervisor: Some(runtime.renderer_supervisor()),
        };

        // A liveness probe never creates a core task, yet must be tallied.
        let health = handle(
            State(state.clone()),
            Method::GET,
            PUBLIC_LIVENESS_PATH.parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(health.status(), StatusCode::OK);

        // A subsequent scrape observes the earlier probe (the scrape itself is
        // recorded only after its own response, so it never appears in its body).
        let scrape = handle(
            State(state),
            Method::GET,
            "/_internal/metrics".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        let body = to_bytes(scrape.into_body(), 1024 * 1024).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(
            body.contains(r#"biei_http_requests_total{endpoint="health",status="200"} 1"#),
            "missing health request tally in:\n{body}"
        );
        assert!(
            !body.contains(r#"endpoint="metrics""#),
            "scrape counted itself"
        );
    }

    #[tokio::test]
    async fn production_routers_keep_request_metrics_listener_aware() {
        let options =
            crate::options::test_options("http://style-api.test/styles/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let supervisor = runtime.renderer_supervisor();

        let public_metrics = HttpMetrics::new(runtime.node(), None, None, supervisor.clone());
        let public_state = HttpServerState {
            metrics: Some(public_metrics.clone()),
            renderer_supervisor: Some(supervisor.clone()),
            ..empty_state()
        };
        for path in [
            INTERNAL_LIVENESS_PATH,
            INTERNAL_READINESS_PATH,
            INTERNAL_METRICS_PATH,
            "/_internal/forward",
        ] {
            let response = handle_public(
                State(public_state.clone()),
                Method::GET,
                path.parse().unwrap(),
                Request::builder().body(Body::empty()).unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
        let public = public_metrics.render_prometheus().await;
        assert!(
            public.contains(r#"biei_http_requests_total{endpoint="not_found",status="404"} 4"#)
        );
        for endpoint in ["health", "ready", "metrics", "internal_forward"] {
            assert!(
                !public.contains(&format!(
                    r#"biei_http_requests_total{{endpoint="{endpoint}""#
                )),
                "public refusal polluted {endpoint} metrics:\n{public}"
            );
        }

        for (scope, metrics) in [
            (
                ListenerScope::Internal,
                HttpMetrics::new(runtime.node(), None, None, supervisor.clone()),
            ),
            (
                ListenerScope::Standalone,
                HttpMetrics::new(runtime.node(), None, None, supervisor.clone()),
            ),
        ] {
            let state = HttpServerState {
                metrics: Some(metrics.clone()),
                renderer_supervisor: Some(supervisor.clone()),
                ..empty_state()
            };
            for (method, path, status) in [
                (Method::GET, INTERNAL_LIVENESS_PATH, StatusCode::OK),
                (Method::GET, INTERNAL_READINESS_PATH, StatusCode::OK),
                (Method::GET, INTERNAL_METRICS_PATH, StatusCode::OK),
                (Method::POST, "/_internal/forward", StatusCode::NOT_FOUND),
            ] {
                let response = match scope {
                    ListenerScope::Internal => {
                        handle_internal(
                            State(state.clone()),
                            method,
                            path.parse().unwrap(),
                            Request::builder().body(Body::empty()).unwrap(),
                        )
                        .await
                    }
                    ListenerScope::Standalone => {
                        handle(
                            State(state.clone()),
                            method,
                            path.parse().unwrap(),
                            Request::builder().body(Body::empty()).unwrap(),
                        )
                        .await
                    }
                    ListenerScope::Public => unreachable!(),
                };
                assert_eq!(response.status(), status, "{scope:?} {path}");
            }

            let rendered = metrics.render_prometheus().await;
            for expected in [
                r#"biei_http_requests_total{endpoint="health",status="200"} 1"#,
                r#"biei_http_requests_total{endpoint="ready",status="200"} 1"#,
                r#"biei_http_requests_total{endpoint="metrics",status="200"} 1"#,
                r#"biei_http_requests_total{endpoint="internal_forward",status="404"} 1"#,
            ] {
                assert!(
                    rendered.contains(expected),
                    "missing {expected} for {scope:?}:\n{rendered}"
                );
            }
        }
    }

    #[tokio::test]
    async fn public_boundary_stamps_early_errors_and_operational_success() {
        let state = empty_state();
        let oversized_uri = format!("/{}", "x".repeat(MAX_PUBLIC_PATH_BYTES + 1))
            .parse()
            .expect("oversized URI");

        for (method, uri, status, code) in [
            (
                Method::POST,
                "/carto/voyager/0/0/0.png".parse().unwrap(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
            ),
            (
                Method::GET,
                oversized_uri,
                StatusCode::URI_TOO_LONG,
                "uri_too_long",
            ),
            (
                Method::GET,
                "/metrics".parse().unwrap(),
                StatusCode::NOT_FOUND,
                "not_found",
            ),
            (
                Method::POST,
                PUBLIC_LIVENESS_PATH.parse().unwrap(),
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
            ),
        ] {
            let response = handle_public(
                State(state.clone()),
                method,
                uri,
                Request::builder()
                    .header(REQUEST_ID_HEADER, "req-early")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
            assert_public_error(response, status, code, "req-early").await;
        }

        let health = handle_public(
            State(state.clone()),
            Method::GET,
            PUBLIC_LIVENESS_PATH.parse().unwrap(),
            Request::builder()
                .header(REQUEST_ID_HEADER, "req-health")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(
            health.headers().get(REQUEST_ID_HEADER).unwrap(),
            "req-health"
        );
        assert_eq!(
            health.headers().get_all(REQUEST_ID_HEADER).iter().count(),
            1
        );

        let generated = handle_public(
            State(state),
            Method::GET,
            "/metrics".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        let generated_id = generated
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("generated request ID");
        assert!(RequestId::from_candidate(generated_id).is_some());
        assert_eq!(
            generated
                .headers()
                .get_all(REQUEST_ID_HEADER)
                .iter()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn public_ingress_echoes_supplied_request_id() {
        let options =
            crate::options::test_options("http://style-api.test/styles/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let ingress = runtime.http_ingress(Duration::from_secs(2));
        let state = HttpServerState {
            ingress: Some(ingress),
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: Some(runtime.renderer_supervisor()),
        };

        let response = handle(
            State(state),
            Method::GET,
            "/bad".parse().unwrap(),
            Request::builder()
                .header(REQUEST_ID_HEADER, "req-123")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get(REQUEST_ID_HEADER).unwrap(),
            "req-123"
        );
        assert_eq!(
            response.headers().get_all(REQUEST_ID_HEADER).iter().count(),
            1
        );
    }

    #[tokio::test]
    async fn cluster_public_router_rejects_reserved_and_noncanonical_aliases_before_admission() {
        let options =
            crate::options::test_options("http://style-api.test/styles/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let ingress = runtime.http_ingress(Duration::from_secs(2));
        runtime.drain_controller().begin_draining();
        let state = HttpServerState {
            drain: ingress.drain_controller(),
            ingress: Some(ingress),
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: Some(runtime.renderer_supervisor()),
        };

        for path in [
            "/_internal",
            "/_internal/",
            "/_internal/metrics",
            "/metrics",
            "/metrics/",
        ] {
            let response = handle_public(
                State(state.clone()),
                Method::GET,
                path.parse().expect("reserved URI"),
                Request::builder()
                    .header(REQUEST_ID_HEADER, "req-refusal")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
            assert_public_error(response, StatusCode::NOT_FOUND, "not_found", "req-refusal").await;
        }

        // These would be valid render paths after trim/filter normalization.
        // A 400, rather than the draining ingress's 503, proves that raw path
        // validation rejects them before admission or render/provider work.
        for path in [
            "//_internal/0/0/0.png",
            "/carto//voyager/0/0/0.png",
            "/carto/voyager/0/0/0.png/",
        ] {
            let response = handle_public(
                State(state.clone()),
                Method::GET,
                path.parse().expect("noncanonical URI"),
                Request::builder().body(Body::empty()).unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        }

        // Canonical paths, including arbitrary-depth style IDs, still parse and
        // reach the deliberately draining admission gate.
        for path in [
            "/carto/voyager/0/0/0.png",
            "/org/team/carto/voyager/0/0/0.png",
            "/org/team/carto/voyager/static/none/0,0,1/1x1.png",
            "/org/team/carto/voyager/preview",
        ] {
            let response = handle_public(
                State(state.clone()),
                Method::GET,
                path.parse().expect("canonical URI"),
                Request::builder().body(Body::empty()).unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
        }
    }

    #[tokio::test]
    async fn public_and_standalone_non_success_responses_are_private_no_store() {
        let options =
            crate::options::test_options("http://style-api.test/styles/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let ingress = runtime.http_ingress(Duration::from_secs(2));
        runtime.drain_controller().begin_draining();
        let state = HttpServerState {
            drain: ingress.drain_controller(),
            ingress: Some(ingress),
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: Some(runtime.renderer_supervisor()),
        };
        let oversized_uri = format!("/{}", "x".repeat(MAX_PUBLIC_PATH_BYTES + 1))
            .parse()
            .expect("oversized URI");

        for (method, uri, expected) in [
            (
                Method::GET,
                "/bad".parse().unwrap(),
                StatusCode::BAD_REQUEST,
            ),
            (
                Method::GET,
                "/metrics".parse().unwrap(),
                StatusCode::NOT_FOUND,
            ),
            (
                Method::POST,
                PUBLIC_LIVENESS_PATH.parse().unwrap(),
                StatusCode::METHOD_NOT_ALLOWED,
            ),
            (Method::GET, oversized_uri, StatusCode::URI_TOO_LONG),
            (
                Method::GET,
                PUBLIC_READINESS_PATH.parse().unwrap(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            let response = handle_public(
                State(state.clone()),
                method,
                uri,
                Request::builder().body(Body::empty()).unwrap(),
            )
            .await;
            assert_eq!(response.status(), expected);
            assert_cache_control(&response, PRIVATE_NO_STORE_CACHE_CONTROL);
        }

        let standalone = handle(
            State(state),
            Method::GET,
            "/bad".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(standalone.status(), StatusCode::BAD_REQUEST);
        assert_cache_control(&standalone, PRIVATE_NO_STORE_CACHE_CONTROL);
    }

    #[tokio::test]
    async fn public_cache_boundary_preserves_completed_tile_policy() {
        let public_content: Router<HttpServerState> =
            Router::new().fallback(completed_tile_response);
        let router = public_router_with_public_content(empty_state(), public_content);

        let response = route_through(
            router,
            Method::GET,
            "/org/team/carto/voyager/0/0/0.png".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_cache_control(&response, "public, max-age=3600");
    }

    #[tokio::test]
    async fn public_path_limit_rejects_oversized_paths_before_ingress() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: None,
        };
        let long_path = format!("/{}", "x".repeat(MAX_PUBLIC_PATH_BYTES + 1));

        let response = handle(
            State(state),
            Method::GET,
            long_path.parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::URI_TOO_LONG);
    }

    #[tokio::test]
    async fn public_path_limit_accepts_exact_8192_bytes() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: None,
        };
        let exact_path = format!("/{}", "x".repeat(MAX_PUBLIC_PATH_BYTES - 1));

        let response = handle_public(
            State(state),
            Method::GET,
            exact_path.parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;

        assert_ne!(response.status(), StatusCode::URI_TOO_LONG);
    }

    #[tokio::test]
    async fn public_uri_limit_includes_query_string() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: None,
        };
        let uri: Uri = format!("/style/static/none/0,0,1/1x1?addlayer={}", "x".repeat(8192))
            .parse()
            .expect("valid oversized URI");

        let response = handle_public(
            State(state),
            Method::GET,
            uri,
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::URI_TOO_LONG);
    }

    #[tokio::test(start_paused = true)]
    async fn forced_shutdown_grace_starts_after_the_signal() {
        let (tx, signal) = shutdown_channel();
        let task = tokio::spawn(shutdown_grace_elapsed(signal));

        tokio::time::advance(HTTP_SHUTDOWN_GRACE + Duration::from_secs(1)).await;
        assert!(!task.is_finished());

        tx.send(true).expect("send shutdown");
        tokio::task::yield_now().await;
        tokio::time::advance(HTTP_SHUTDOWN_GRACE).await;
        tokio::task::yield_now().await;
        assert!(task.is_finished());
    }

    #[tokio::test]
    async fn public_listener_hides_internal_endpoints() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: None,
        };

        for path in [
            "/metrics",
            "/_internal/healthz",
            "/_internal/metrics",
            "/_internal/forward",
        ] {
            let response = handle_public(
                State(state.clone()),
                Method::GET,
                path.parse().unwrap(),
                Request::builder().body(Body::empty()).unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }

        let livez = handle_public(
            State(state),
            Method::GET,
            "/livez".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(livez.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn internal_listener_does_not_serve_public_render_paths() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
            renderer_supervisor: None,
        };

        let response = handle_internal(
            State(state),
            Method::GET,
            "/carto/voyager-gl-style/0/0/0.png".parse().unwrap(),
            Request::builder()
                .header(REQUEST_ID_HEADER, "internal-request")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(response.headers().get(REQUEST_ID_HEADER).is_none());
    }
}
