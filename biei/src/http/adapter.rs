//! Axum adapter for production HTTP ingress.
//!
//! URL parsing and response classification stay in `http::ingress`; this module
//! only binds a socket and converts that small internal response shape into an
//! HTTP response.

use std::net::SocketAddr;

use anyhow::Context;
use axum::Router;
use axum::body::Body;
use axum::body::to_bytes;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, Method, Request, StatusCode, Uri};
use axum::response::Response;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::gossip::GossipBus;
use crate::http::REQUEST_ID_HEADER;
use crate::http::ingress::HttpIngress;
use crate::http::response::IngressResponse;
use crate::metrics::{RuntimeGauges, WorkerGaugeSample};
use crate::types::RequestId;

const MAX_INTERNAL_FORWARD_BODY_BYTES: usize = 10 * 1024 * 1024;
const MAX_PUBLIC_PATH_BYTES: usize = 8192;

#[derive(Clone)]
pub struct ShutdownSignal {
    rx: watch::Receiver<bool>,
}

pub fn shutdown_channel() -> (watch::Sender<bool>, ShutdownSignal) {
    let (tx, rx) = watch::channel(false);
    (tx, ShutdownSignal { rx })
}

impl ShutdownSignal {
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
    membership: Option<crate::membership::Membership>,
    internal_forward: Option<crate::http::internal::InternalForwardEndpoint>,
    metrics: Option<HttpMetrics>,
}

#[derive(Clone)]
pub struct HttpMetrics {
    node: crate::node::Node,
    membership: Option<crate::membership::Membership>,
    drain: Option<crate::drain::DrainController>,
}

impl HttpMetrics {
    pub fn new(
        node: crate::node::Node,
        membership: Option<crate::membership::Membership>,
        drain: Option<crate::drain::DrainController>,
    ) -> Self {
        Self {
            node,
            membership,
            drain,
        }
    }

    async fn render_prometheus(&self) -> String {
        let node_id = self.node.id();
        // Extract the runtime gauge samples here (this layer knows the worker /
        // profile types); the gauge schema + rendering lives in `metrics`.
        let workers = self
            .node
            .worker_snapshot()
            .iter()
            .map(|worker| {
                let profile = worker.loaded_profile.as_ref();
                WorkerGaugeSample {
                    worker: worker.id.to_string(),
                    style_id: profile
                        .map(|p| p.style.id.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    render_mode: profile
                        .map(|p| p.render_mode.as_gossip_value())
                        .unwrap_or("none"),
                    scale: profile.map(|p| p.scale.as_gossip_value()).unwrap_or("none"),
                    queue_depth: worker.queue_depth as i64,
                    loaded: worker.loaded_profile.is_some(),
                }
            })
            .collect();
        let membership_live = match &self.membership {
            Some(membership) => Some(membership.view().await.members.len() as i64),
            None => None,
        };
        let runtime = RuntimeGauges {
            node_id: node_id.as_str().to_string(),
            workers,
            membership_live,
            cpu_permits_inuse: self.node.cpu_permits_inuse() as i64,
            draining: self.drain.as_ref().is_some_and(|drain| drain.is_draining()),
        };
        self.node.metrics().render_prometheus_with_runtime(&runtime)
    }
}

// Single-node / local: one listener serving the combined router (public render
// plus health/metrics/forward). Not fronted by a Gateway, so no port split.
pub async fn serve_with_shutdown(
    ingress: HttpIngress,
    bind: SocketAddr,
    shutdown: Option<ShutdownSignal>,
) -> anyhow::Result<()> {
    let drain = ingress.drain_controller();
    let metrics = Some(HttpMetrics::new(ingress.node(), None, drain.clone()));
    serve_with_state(
        HttpServerState {
            drain,
            ingress: Some(ingress),
            membership: None,
            internal_forward: None,
            metrics,
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
pub async fn serve_with_shutdown_and_membership_and_internal_forward(
    ingress: HttpIngress,
    public_bind: SocketAddr,
    internal_bind: SocketAddr,
    shutdown: Option<ShutdownSignal>,
    membership: Option<crate::membership::Membership>,
    internal_forward: Option<crate::http::internal::InternalForwardEndpoint>,
) -> anyhow::Result<()> {
    let drain = ingress.drain_controller();
    let metrics = Some(HttpMetrics::new(
        ingress.node(),
        membership.clone(),
        drain.clone(),
    ));
    serve_with_state(
        HttpServerState {
            drain,
            ingress: Some(ingress),
            membership,
            internal_forward,
            metrics,
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
        let server = axum::serve(listener, Router::new().fallback(handle).with_state(state));
        if let Some(signal) = shutdown {
            server
                .with_graceful_shutdown(signal.wait())
                .await
                .context("serve HTTP listener")?;
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
    let public = Router::new()
        .fallback(handle_public)
        .with_state(state.clone());
    let internal = Router::new().fallback(handle_internal).with_state(state);
    let public_server = axum::serve(public_listener, public);
    let internal_server = axum::serve(internal_listener, internal);

    // try_join! so that if one listener errors unexpectedly, the error surfaces
    // immediately and the other listener is dropped, rather than blocking on it.
    if let Some(signal) = shutdown {
        // Fan the single shutdown signal out to both graceful-shutdown futures.
        let internal_signal = signal.clone();
        tokio::try_join!(
            async {
                public_server
                    .with_graceful_shutdown(signal.wait())
                    .await
                    .context("serve public HTTP listener")
            },
            async {
                internal_server
                    .with_graceful_shutdown(internal_signal.wait())
                    .await
                    .context("serve internal listener")
            },
        )?;
    } else {
        tokio::try_join!(
            async { public_server.await.context("serve public HTTP listener") },
            async { internal_server.await.context("serve internal listener") },
        )?;
    }
    Ok(())
}

/// Combined dispatcher (single-node / tests): serves every endpoint on one port.
async fn handle(
    State(state): State<HttpServerState>,
    method: Method,
    uri: Uri,
    request: Request<Body>,
) -> Response {
    match uri.path() {
        "/livez" | "/_internal/healthz" => health_ok(&method),
        "/readyz" | "/_internal/readyz" => readyz(&method, &state).await,
        "/_internal/metrics" => metricsz(&method, &state).await,
        "/_internal/forward" => forwardz(method, &state, request).await,
        _ => public_render(state, method, &uri, request).await,
    }
}

/// Gateway-fronted public listener: render/preview plus top-level health probes.
/// `/_internal/*` (metrics, peer forwarding) is served only on the internal
/// port; a stray `/metrics` is likewise refused here.
async fn handle_public(
    State(state): State<HttpServerState>,
    method: Method,
    uri: Uri,
    request: Request<Body>,
) -> Response {
    match uri.path() {
        "/livez" => health_ok(&method),
        "/readyz" => readyz(&method, &state).await,
        path if path == "/metrics" || path.starts_with("/_internal/") => {
            simple_response(StatusCode::NOT_FOUND, "not found")
        }
        _ => public_render(state, method, &uri, request).await,
    }
}

/// Cluster-internal listener: metrics, peer forwarding and health. Never
/// exposed through the Gateway; serves no public render paths. Everything is
/// namespaced under `/_internal/*` (matching the sibling `ishikari` service) —
/// top-level `/livez` `/readyz` live only on the public port.
async fn handle_internal(
    State(state): State<HttpServerState>,
    method: Method,
    uri: Uri,
    request: Request<Body>,
) -> Response {
    match uri.path() {
        "/_internal/healthz" => health_ok(&method),
        "/_internal/readyz" => readyz(&method, &state).await,
        "/_internal/metrics" => metricsz(&method, &state).await,
        "/_internal/forward" => forwardz(method, &state, request).await,
        _ => simple_response(StatusCode::NOT_FOUND, "not found"),
    }
}

fn health_ok(method: &Method) -> Response {
    if method != Method::GET {
        return simple_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    simple_response(StatusCode::OK, "ok")
}

async fn readyz(method: &Method, state: &HttpServerState) -> Response {
    if method != Method::GET {
        return simple_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    let ready = state
        .drain
        .as_ref()
        .is_none_or(|drain| !drain.is_draining());
    let ready = ready
        && match &state.membership {
            Some(membership) => membership.is_gossip_ready().await,
            None => true,
        };
    if ready {
        simple_response(StatusCode::OK, "ready")
    } else {
        simple_response(StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

async fn metricsz(method: &Method, state: &HttpServerState) -> Response {
    if method != Method::GET {
        return simple_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
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

async fn forwardz(method: Method, state: &HttpServerState, request: Request<Body>) -> Response {
    if method != Method::POST {
        return simple_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    let Some(internal_forward) = state.internal_forward.as_ref() else {
        return simple_response(StatusCode::NOT_FOUND, "internal forward disabled");
    };
    let headers = request.headers().clone();
    let body = match to_bytes(request.into_body(), MAX_INTERNAL_FORWARD_BODY_BYTES).await {
        Ok(body) => body,
        Err(_) => return simple_response(StatusCode::PAYLOAD_TOO_LARGE, "body too large"),
    };
    internal_forward.handle(&headers, body).await
}

async fn public_render(
    state: HttpServerState,
    method: Method,
    uri: &Uri,
    request: Request<Body>,
) -> Response {
    if method != Method::GET {
        return simple_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if uri.path().len() > MAX_PUBLIC_PATH_BYTES {
        return simple_response(StatusCode::URI_TOO_LONG, "path too long");
    }
    let Some(ingress) = state.ingress else {
        return simple_response(StatusCode::NOT_FOUND, "not found");
    };
    let request_id = request_id_from_headers(request.headers());
    if is_preview_path(uri.path()) {
        return into_axum_response(ingress.serve_preview(uri.path(), request_id).await);
    }
    into_axum_response(
        ingress
            .handle_path_with_request_id(uri.path(), uri.query(), request_id, Instant::now())
            .await,
    )
}

/// `/{user}/{style}/preview` または `/{style_id}/preview` だけを対象にする。
/// 一般の tile / static 描画 path とは「最終 segment が literal `preview`」
/// で衝突しない構造になっているのを利用する。
fn is_preview_path(path: &str) -> bool {
    let segments: Vec<_> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    matches!(segments.len(), 2 | 3) && segments.last().copied() == Some("preview")
}

fn request_id_from_headers(headers: &HeaderMap) -> Option<RequestId> {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(RequestId::from_string)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
    fn method_not_allowed_response_is_plain_text() {
        let response = simple_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    fn is_preview_path_matches_preview_suffix_only() {
        // Two-segment style + preview
        assert!(is_preview_path("/carto/voyager-gl-style/preview"));
        assert!(is_preview_path("/foo/bar/preview/"));
        // Single-segment style + preview
        assert!(is_preview_path("/voyager-gl-style/preview"));
        // Tile path — last segment is not "preview"
        assert!(!is_preview_path("/carto/voyager/0/0/0@2x.png"));
        // preview not last segment
        assert!(!is_preview_path("/foo/preview/bar"));
        // Too few segments(style id 部分なし)
        assert!(!is_preview_path("/preview"));
        // Too many segments(style id が 2 を超える)
        assert!(!is_preview_path("/foo/bar/baz/preview"));
        // Empty / root
        assert!(!is_preview_path("/"));
        assert!(!is_preview_path(""));
    }

    #[tokio::test]
    async fn health_and_ready_endpoints_are_plain_text() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
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
    async fn single_router_routes_public_and_internal_paths() {
        let options = crate::options::Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cores",
            "1",
        ])
        .expect("options parse");
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let ingress = runtime.http_ingress(Duration::from_secs(2));
        let metrics = Some(HttpMetrics::new(
            runtime.node(),
            None,
            ingress.drain_controller(),
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
        let options = crate::options::Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cores",
            "1",
        ])
        .expect("options parse");
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: Some(HttpMetrics::new(runtime.node(), None, None)),
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
        assert!(body.contains("biei_worker_loaded"));
        assert!(body.contains("biei_cpu_permits_inuse"));
        assert!(body.contains("biei_drain_state"));
        assert!(body.contains("# TYPE biei_tasks_completed_total counter"));
        assert!(body.contains(r#"scope="ingress"} 0"#));
    }

    #[tokio::test]
    async fn public_ingress_echoes_supplied_request_id() {
        let options = crate::options::Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cores",
            "1",
        ])
        .expect("options parse");
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let ingress = runtime.http_ingress(Duration::from_secs(2));
        let state = HttpServerState {
            ingress: Some(ingress),
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
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
    }

    #[tokio::test]
    async fn public_path_limit_rejects_oversized_paths_before_ingress() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
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
    async fn public_listener_hides_internal_endpoints() {
        let state = HttpServerState {
            ingress: None,
            drain: None,
            membership: None,
            internal_forward: None,
            metrics: None,
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
        };

        let response = handle_internal(
            State(state),
            Method::GET,
            "/carto/voyager-gl-style/0/0/0.png".parse().unwrap(),
            Request::builder().body(Body::empty()).unwrap(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
