//! Internal peer-to-peer HTTP forwarding.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use axum::body::{Body, Bytes};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use bytes::BytesMut;
use futures_util::{StreamExt, stream};
use mmpf_http::content_type::media_type_eq;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::drain::DrainController;
use crate::http::{REQUEST_ID_HEADER, request_id_from_headers};
use biei_core::internal_transport::{ForwardError, InternalTransport};
use biei_core::node::Node;
#[cfg(test)]
use biei_core::types::RequestId;
use biei_core::types::{FailureKind, NodeId, RejectionReason, RenderOutput};
use biei_core::wire::{
    ForwardRequest, ForwardResponse, OutcomeHeader, OutcomeResult, decode_response_bytes,
    encode_response_header,
};

const JSON_CONTENT_TYPE: &str = "application/json";
const BIEI_RESPONSE_CONTENT_TYPE: &str = "application/x-biei-forward-response";
const MAX_FORWARD_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_FORWARD_RESPONSE_BODY_BYTES: usize = 48 * 1024 * 1024;

#[async_trait]
pub(crate) trait PeerResolver: Send + Sync {
    async fn advertise_addr_of(&self, node_id: &NodeId) -> Option<SocketAddr>;
}

#[async_trait]
impl PeerResolver for crate::membership::Membership {
    async fn advertise_addr_of(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.advertise_addr_of(node_id).await
    }
}

#[derive(Clone)]
pub(crate) struct InternalForwardEndpoint {
    node: Node,
    drain: Option<DrainController>,
    admission: Arc<Semaphore>,
}

impl InternalForwardEndpoint {
    #[cfg(test)]
    pub(crate) fn with_drain(node: Node, drain: DrainController) -> Self {
        Self::with_drain_and_limit(node, drain, 1)
    }

    pub(crate) fn with_drain_and_limit(node: Node, drain: DrainController, limit: usize) -> Self {
        Self {
            node,
            drain: Some(drain),
            admission: Arc::new(Semaphore::new(limit.max(1))),
        }
    }

    #[cfg(test)]
    pub(crate) async fn handle(&self, headers: &HeaderMap, body: Bytes) -> Response {
        let Some(permit) = self.try_admit() else {
            return overloaded_response();
        };
        self.handle_admitted(headers, body, permit).await
    }

    pub(crate) fn try_admit(&self) -> Option<OwnedSemaphorePermit> {
        // Degraded shedding is decided in the node (after the cache key is
        // decoded), so forwarded hits stay reachable. This is the concurrency
        // gate only, matching the public path's ordering.
        Arc::clone(&self.admission).try_acquire_owned().ok()
    }

    pub(crate) async fn handle_admitted(
        &self,
        headers: &HeaderMap,
        body: Bytes,
        permit: OwnedSemaphorePermit,
    ) -> Response {
        if !is_json_content_type(headers) {
            return response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                b"unsupported media type".to_vec(),
                "text/plain",
            );
        }
        let drain_permit = match &self.drain {
            Some(drain) => match drain.try_acquire() {
                Some(permit) => Some(permit),
                None => {
                    return response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        b"draining".to_vec(),
                        "text/plain",
                    );
                }
            },
            None => None,
        };
        let mut forwarded = match serde_json::from_slice::<ForwardRequest>(&body) {
            Ok(forwarded) => forwarded,
            Err(err) => {
                return response(
                    StatusCode::BAD_REQUEST,
                    format!("decode forwarded request: {err}").into_bytes(),
                    "text/plain",
                );
            }
        };
        if let Some(request_id) = request_id_from_headers(headers) {
            forwarded.task.request_id = request_id;
        }
        // Serde success proves only syntax. A peer-forwarded request must pass
        // the same render-safety limits as public ingress before any native
        // work is queued, so a malformed or hostile peer cannot drive an
        // oversized allocation or an out-of-range camera into the renderer.
        if let Err(err) = crate::http::static_image::validate_render_request(
            &forwarded.task.request,
            forwarded.task.scale,
        ) {
            return response(
                StatusCode::BAD_REQUEST,
                format!("invalid forwarded render request: {err}").into_bytes(),
                "text/plain",
            );
        }
        let style_id = forwarded.task.style.id.clone();
        let node = self.node.clone();
        let outcome = match tokio::spawn(async move {
            // A peer may disconnect before a native render returns. Keep both
            // admission guards and outcome recording alive with the render.
            let _permit = permit;
            let _drain_permit = drain_permit;
            node.handle_forwarded(forwarded).await
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::error!(%error, "forwarded render task terminated unexpectedly");
                return response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    b"forwarded render task failed".to_vec(),
                    "text/plain",
                );
            }
        };
        let (outcome, output) = OutcomeHeader::from_task_outcome(outcome, style_id);
        response_from_outcome(outcome, output)
    }
}

pub(crate) fn overloaded_response() -> Response {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("retry-after", "1")
        .body(Body::from("internal forward busy"))
        .unwrap_or_else(|_| Response::new(Body::from("internal forward busy")))
}

pub(crate) struct HttpTransport {
    client: reqwest::Client,
    resolver: Arc<dyn PeerResolver>,
}

impl HttpTransport {
    pub(crate) fn new(resolver: Arc<dyn PeerResolver>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            // Peer forwarding is an in-cluster trust boundary. Never leak its
            // request metadata/image traffic to an environment-configured
            // HTTP proxy or let that proxy impersonate a peer response.
            .no_proxy()
            .build()
            .context("build HTTP forward client")?;
        Ok(Self { client, resolver })
    }
}

#[async_trait]
impl InternalTransport for HttpTransport {
    async fn send(
        &self,
        target: NodeId,
        mut fwd: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError> {
        let response_budget_ms = fwd.origin_response_budget_ms;
        if response_budget_ms == 0 {
            return Err(ForwardError::Fatal(
                "forward request has zero origin response budget".to_string(),
            ));
        }
        let timeout = Duration::from_millis(response_budget_ms as u64)
            .min(MAX_FORWARD_TIMEOUT)
            .max(Duration::from_millis(1));
        let send_started_at = Instant::now();
        let deadline = send_started_at + timeout;
        let addr = tokio::time::timeout_at(deadline, self.resolver.advertise_addr_of(&target))
            .await
            .map_err(|_| {
                ForwardError::Retryable(format!(
                    "peer address resolution exceeded origin response budget for {target}"
                ))
            })?
            .ok_or_else(|| {
                ForwardError::Retryable(format!("no advertise address for node {target}"))
            })?;
        // `remaining_budget_ms` is reconstructed from the receiver's arrival
        // time. If we serialized the original value after a slow successful
        // resolution, the receiver would grant that elapsed time again and an
        // uncancellable native render could outlive the origin request.
        let resolution_ms = duration_millis_ceil(send_started_at.elapsed());
        fwd.task.remaining_budget_ms = fwd.task.remaining_budget_ms.saturating_sub(resolution_ms);
        if fwd.task.remaining_budget_ms == 0 {
            return Err(ForwardError::Retryable(format!(
                "peer address resolution exhausted remote execution budget for {target}"
            )));
        }
        fwd.origin_response_budget_ms = fwd
            .origin_response_budget_ms
            .saturating_sub(resolution_ms)
            .max(1);
        let body = serde_json::to_vec(&fwd)
            .map_err(|err| ForwardError::Fatal(format!("encode forwarded request: {err}")))?;
        let expected_task_id = fwd.task.id;
        let expected_request_id = fwd.task.request_id.clone();
        let expected_style_id = fwd.task.style.id.clone();
        let expected_output_format = fwd.task.output_format;
        let expected_had_source =
            fwd.task.source.is_some() || fwd.task.request.has_addlayer_source();
        let url = format!("http://{addr}/_internal/forward");
        let response = self
            .client
            .post(url)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .header(REQUEST_ID_HEADER, fwd.task.request_id.as_str())
            .body(body)
            .timeout(
                deadline
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(1)),
            )
            .send()
            .await
            .map_err(|err| {
                if err.is_timeout() || err.is_connect() || err.is_request() {
                    ForwardError::Retryable(err.to_string())
                } else {
                    ForwardError::Fatal(err.to_string())
                }
            })?;

        let status = response.status();
        if !is_biei_response_content_type(response.headers()) {
            if retryable_peer_status(status) {
                return Err(ForwardError::Retryable(format!(
                    "peer returned retryable HTTP status {status}"
                )));
            }
            return Err(ForwardError::Fatal(format!(
                "peer returned {status} without {BIEI_RESPONSE_CONTENT_TYPE} response body"
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_FORWARD_RESPONSE_BODY_BYTES as u64)
        {
            return Err(ForwardError::Fatal(
                "peer forward response body exceeds size limit".to_string(),
            ));
        }
        let mut body = BytesMut::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk
                .map_err(|err| ForwardError::Retryable(format!("read response body: {err}")))?;
            if body.len().saturating_add(chunk.len()) > MAX_FORWARD_RESPONSE_BODY_BYTES {
                return Err(ForwardError::Fatal(
                    "peer forward response body exceeds size limit".to_string(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let (outcome, image_bytes) = decode_response_bytes(body.freeze())
            .map_err(|err| ForwardError::Fatal(format!("decode forward response body: {err}")))?;

        if outcome.task_id != expected_task_id
            || outcome.request_id != expected_request_id
            || outcome.style_id != expected_style_id
            || outcome.had_source != expected_had_source
        {
            return Err(ForwardError::Fatal(format!(
                "peer response identity mismatch for task {expected_task_id} request {} style {}",
                expected_request_id.as_str(),
                expected_style_id.as_str(),
            )));
        }

        if !status.is_success() {
            match &outcome.result {
                OutcomeResult::Rejected { .. } | OutcomeResult::Failed { .. } => {
                    return Ok(ForwardResponse {
                        outcome,
                        output: None,
                    });
                }
                OutcomeResult::Completed { .. } => {
                    return Err(ForwardError::Fatal(format!(
                        "peer returned completed outcome with non-success status {status}"
                    )));
                }
            }
        }

        let output = match outcome.completed_format() {
            Some(format) if format != expected_output_format => {
                return Err(ForwardError::Fatal(format!(
                    "peer response output format mismatch: expected {expected_output_format:?}, got {format:?}"
                )));
            }
            Some(_) if image_bytes.is_empty() => {
                return Err(ForwardError::Fatal(
                    "peer returned completed outcome with an empty image body".to_string(),
                ));
            }
            Some(format) => Some(RenderOutput {
                bytes: image_bytes,
                format,
            }),
            None => {
                return Err(ForwardError::Fatal(
                    "peer returned success without completed image format".to_string(),
                ));
            }
        };
        Ok(ForwardResponse { outcome, output })
    }
}

fn duration_millis_ceil(duration: Duration) -> u32 {
    let whole = duration.as_millis();
    let rounded = if duration.subsec_nanos().is_multiple_of(1_000_000) {
        whole
    } else {
        whole.saturating_add(1)
    };
    rounded.min(u32::MAX as u128) as u32
}

fn response(status: StatusCode, body: Vec<u8>, content_type: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

fn response_from_outcome(outcome: OutcomeHeader, output: Option<RenderOutput>) -> Response {
    let status = status_for_outcome(&outcome);
    let request_id = outcome.request_id.as_str().to_string();
    let header = match encode_response_header(&outcome) {
        Ok(header) => header,
        Err(err) => {
            return response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("encode forward response body: {err}").into_bytes(),
                "text/plain; charset=utf-8",
            );
        }
    };
    let image = output.map_or_else(Bytes::new, |output| output.bytes);
    let body = Body::from_stream(stream::iter([
        Ok::<Bytes, Infallible>(header),
        Ok::<Bytes, Infallible>(image),
    ]));
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, BIEI_RESPONSE_CONTENT_TYPE)
        .header(REQUEST_ID_HEADER, request_id)
        .body(body)
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

fn status_for_outcome(outcome: &OutcomeHeader) -> StatusCode {
    match &outcome.result {
        OutcomeResult::Completed { .. } => StatusCode::OK,
        OutcomeResult::Rejected { reason, .. } => match reason {
            RejectionReason::UnknownStyle => StatusCode::NOT_FOUND,
            RejectionReason::DeadlineTooClose | RejectionReason::DeadlineExceeded => {
                StatusCode::GATEWAY_TIMEOUT
            }
            RejectionReason::HopLimitExceeded => StatusCode::INTERNAL_SERVER_ERROR,
            RejectionReason::QueueFull
            | RejectionReason::NoCapacity
            | RejectionReason::RendererDegraded
            | RejectionReason::DrainTooSlow
            | RejectionReason::ForwardFailed => StatusCode::SERVICE_UNAVAILABLE,
        },
        OutcomeResult::Failed { kind, .. } => match kind {
            FailureKind::RenderTimeout | FailureKind::PreparationTimeout => {
                StatusCode::GATEWAY_TIMEOUT
            }
            FailureKind::StyleUnavailable | FailureKind::SourceUnavailable => {
                StatusCode::BAD_GATEWAY
            }
            FailureKind::RendererDead | FailureKind::StyleNotReady | FailureKind::Other => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        },
    }
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| media_type_eq(value, JSON_CONTENT_TYPE))
}

fn is_biei_response_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| media_type_eq(value, BIEI_RESPONSE_CONTENT_TYPE))
}

fn retryable_peer_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::Router;
    use axum::body::to_bytes;
    use axum::http::Request;
    use std::collections::HashMap;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, Instant};

    use crate::renderer::{BoxRenderer, Renderer, RendererOutput};
    use biei_core::config::{
        CostConfig, CostRange, GossipConfig, QueueLimits, ResolvedNodeConfig, RoutingConfig,
        Tier1Strategy,
    };
    use biei_core::gossip::GossipBus;
    use biei_core::node::{DispatcherEntropy, Node, NodeSpawn};
    use biei_core::style_catalog::StyleCatalog;
    use biei_core::types::{
        ClusterView, ImageFormat, InternalTask, LngLat, NodeKvs, NodeStateView, Padding,
        PathOverlay, PixelRatio, Positioning, RejectionReason, RenderOutput, RenderRequest,
        RendererError, RouteTier, Scale, SourceHash, StaticOverlay, StyleId, StyleRevision,
        TaskResult,
    };
    use biei_core::wire::{ForwardRequest, OutcomeHeader, OutcomeResult, WireTask};

    fn encode_response_body(
        header: &OutcomeHeader,
        image_bytes: &[u8],
    ) -> Result<Vec<u8>, biei_core::wire::WireError> {
        let prefix = encode_response_header(header)?;
        let mut body = Vec::with_capacity(prefix.len() + image_bytes.len());
        body.extend_from_slice(&prefix);
        body.extend_from_slice(image_bytes);
        Ok(body)
    }

    fn failed_outcome(kind: FailureKind) -> OutcomeHeader {
        OutcomeHeader {
            task_id: 1,
            request_id: RequestId::from_string("status-test"),
            style_id: StyleId("carto/voyager".to_string()),
            had_source: false,
            image_format: None,
            result: OutcomeResult::Failed {
                error: "test failure".to_string(),
                kind,
            },
        }
    }

    #[test]
    fn failed_outcome_statuses_distinguish_timeout_and_provider_failures() {
        assert_eq!(
            status_for_outcome(&failed_outcome(FailureKind::RenderTimeout)),
            StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            status_for_outcome(&failed_outcome(FailureKind::PreparationTimeout)),
            StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            status_for_outcome(&failed_outcome(FailureKind::StyleUnavailable)),
            StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            status_for_outcome(&failed_outcome(FailureKind::SourceUnavailable)),
            StatusCode::BAD_GATEWAY
        );
    }

    #[test]
    fn json_content_type_accepts_optional_parameters() {
        let mut headers = HeaderMap::new();

        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());
        assert!(is_json_content_type(&headers));

        headers.insert(
            CONTENT_TYPE,
            "Application/JSON; charset=utf-8".parse().unwrap(),
        );
        assert!(is_json_content_type(&headers));

        headers.insert(CONTENT_TYPE, "application/octet-stream".parse().unwrap());
        assert!(!is_json_content_type(&headers));
    }

    struct StaticResolver {
        addr: SocketAddr,
    }

    #[async_trait]
    impl PeerResolver for StaticResolver {
        async fn advertise_addr_of(&self, _node_id: &NodeId) -> Option<SocketAddr> {
            Some(self.addr)
        }
    }

    struct SlowResolver {
        addr: SocketAddr,
        delay: Duration,
    }

    #[async_trait]
    impl PeerResolver for SlowResolver {
        async fn advertise_addr_of(&self, _node_id: &NodeId) -> Option<SocketAddr> {
            tokio::time::sleep(self.delay).await;
            Some(self.addr)
        }
    }

    fn forward_request() -> ForwardRequest {
        ForwardRequest {
            task: WireTask {
                id: 1,
                request_id: RequestId::from_string("forward-test"),
                style: StyleRevision {
                    id: StyleId("carto/voyager".to_string()),
                    version: 1,
                },
                source: None,
                request: RenderRequest::Tile {
                    z: 0,
                    x: 0,
                    y: 0,
                    tile_size: 512,
                },
                scale: Scale::X2,
                output_format: ImageFormat::Png,
                remaining_budget_ms: 1_000,
                forwarding_hops: 1,
            },
            route_tier: RouteTier::Tier2HrwBl,
            drain_worker: Some(0),
            origin_response_budget_ms: 1_500,
        }
    }

    async fn spawn_status_server(status: StatusCode) -> SocketAddr {
        let app = Router::new().fallback(move || async move { status });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    async fn spawn_wrong_content_type_server() -> SocketAddr {
        let app = Router::new().fallback(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from("wrong content type"))
                .expect("response")
        });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    async fn spawn_framed_rejection_server() -> SocketAddr {
        let app = Router::new().fallback(move |request: Request<Body>| async move {
            assert_eq!(
                request.headers().get(CONTENT_TYPE).expect("content type"),
                JSON_CONTENT_TYPE
            );
            assert_eq!(
                request
                    .headers()
                    .get(REQUEST_ID_HEADER)
                    .expect("request id"),
                "forward-test"
            );
            let bytes = to_bytes(request.into_body(), 1024 * 1024)
                .await
                .expect("request body");
            let fwd: ForwardRequest = serde_json::from_slice(&bytes).expect("decode request");
            let outcome = OutcomeHeader {
                task_id: fwd.task.id,
                request_id: fwd.task.request_id,
                style_id: fwd.task.style.id,
                had_source: false,
                image_format: None,
                result: OutcomeResult::Rejected {
                    reason: RejectionReason::QueueFull,
                    deadline_stage: None,
                },
            };
            let body = encode_response_body(&outcome, &[]).expect("encode outcome");

            Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header(CONTENT_TYPE, BIEI_RESPONSE_CONTENT_TYPE)
                .body(Body::from(body))
                .expect("response")
        });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    async fn spawn_raw_image_server(mismatch_identity: bool, delay: Duration) -> SocketAddr {
        spawn_raw_image_server_with_options(mismatch_identity, delay, ImageFormat::Png, None).await
    }

    async fn spawn_raw_image_server_with_options(
        mismatch_identity: bool,
        delay: Duration,
        response_format: ImageFormat,
        maximum_remote_budget_ms: Option<u32>,
    ) -> SocketAddr {
        let app = Router::new().fallback(move |request: Request<Body>| async move {
            assert_eq!(
                request.headers().get(CONTENT_TYPE).expect("content type"),
                JSON_CONTENT_TYPE
            );
            assert_eq!(
                request
                    .headers()
                    .get(REQUEST_ID_HEADER)
                    .expect("request id"),
                "forward-test"
            );
            let bytes = to_bytes(request.into_body(), 1024 * 1024)
                .await
                .expect("request body");
            let fwd: ForwardRequest = serde_json::from_slice(&bytes).expect("decode request");
            if let Some(maximum) = maximum_remote_budget_ms {
                assert!(
                    fwd.task.remaining_budget_ms <= maximum,
                    "slow successful resolution must be removed from the remote execution budget: got {}ms, maximum {maximum}ms",
                    fwd.task.remaining_budget_ms,
                );
            }
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let outcome = OutcomeHeader {
                task_id: if mismatch_identity {
                    fwd.task.id.saturating_add(1)
                } else {
                    fwd.task.id
                },
                request_id: fwd.task.request_id,
                style_id: fwd.task.style.id,
                had_source: false,
                image_format: Some(response_format),
                result: OutcomeResult::Completed {
                    node_id: NodeId::from("peer-a"),
                    worker_id: Some(2),
                    route_tier: RouteTier::Tier2HrwBl,
                    render_started_ms: 1,
                    native_render_started_ms: 2,
                    native_render_completed_ms: 10,
                    completed_ms: 12,
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
                    render_observation: None,
                },
            };
            let body = encode_response_body(&outcome, &[137, 80, 78, 71]).expect("encode outcome");

            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, BIEI_RESPONSE_CONTENT_TYPE)
                .body(Body::from(body))
                .expect("response")
        });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    async fn spawn_peer_forward_server(node: Node) -> SocketAddr {
        let endpoint =
            InternalForwardEndpoint::with_drain(node, crate::drain::DrainController::new());
        let app = Router::new().fallback(move |request: Request<Body>| {
            let endpoint = endpoint.clone();
            async move {
                let headers = request.headers().clone();
                let body = to_bytes(request.into_body(), 1024 * 1024)
                    .await
                    .expect("request body");
                endpoint.handle(&headers, body).await
            }
        });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer server");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    #[derive(Clone)]
    struct StaticGossip {
        view: ClusterView,
    }

    #[async_trait]
    impl GossipBus for StaticGossip {
        async fn set(&self, _key: String, _value: String) {}

        async fn view(&self) -> ClusterView {
            self.view.clone()
        }
    }

    struct FakeRenderer {
        bytes: Vec<u8>,
        loaded: Option<biei_core::types::WorkerProfile>,
    }

    struct GatedRenderer {
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
        loaded: Option<biei_core::types::WorkerProfile>,
    }

    impl FakeRenderer {
        fn new(bytes: Vec<u8>) -> Self {
            Self {
                bytes,
                loaded: None,
            }
        }
    }

    #[async_trait]
    impl Renderer for FakeRenderer {
        async fn setup_profile(
            &mut self,
            task: &InternalTask,
            _prepared: Option<crate::renderer::PreparedProfile>,
        ) -> Result<(), RendererError> {
            self.loaded = Some(task.worker_profile());
            Ok(())
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
            if self.loaded.as_ref() != Some(&task.worker_profile()) {
                return Err(RendererError::StyleNotReady {
                    style_id: task.style.id.clone(),
                    version: task.style.version,
                });
            }
            Ok(RenderOutput {
                bytes: self.bytes.clone().into(),
                format: task.output_format,
            }
            .into())
        }
    }

    #[async_trait]
    impl Renderer for GatedRenderer {
        async fn setup_profile(
            &mut self,
            task: &InternalTask,
            _prepared: Option<crate::renderer::PreparedProfile>,
        ) -> Result<(), RendererError> {
            self.loaded = Some(task.worker_profile());
            Ok(())
        }

        async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
            Ok(())
        }

        async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
            self.started.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("test release semaphore remains open")
                .forget();
            Ok(RenderOutput {
                bytes: vec![1].into(),
                format: task.output_format,
            }
            .into())
        }
    }

    struct UnexpectedTransport;

    #[async_trait]
    impl InternalTransport for UnexpectedTransport {
        async fn send(
            &self,
            target: NodeId,
            _fwd: ForwardRequest,
        ) -> Result<ForwardResponse, ForwardError> {
            Err(ForwardError::Fatal(format!(
                "unexpected nested forward to {target}"
            )))
        }
    }

    fn test_catalog() -> Arc<StyleCatalog> {
        let catalog = Arc::new(StyleCatalog::new());
        catalog.set_url_template("https://styles.example.test/{style_id}/style.json");
        catalog
    }

    fn test_costs() -> CostConfig {
        CostConfig {
            style_setup_cost: CostRange::fixed(Duration::from_millis(1)),
            source_load_cost: CostRange::fixed(Duration::ZERO),
            render_cpu_cost: CostRange::fixed(Duration::from_millis(1)),
            render_resource_cost: CostRange::fixed(Duration::ZERO),
            first_render_resource_cost: CostRange::fixed(Duration::ZERO),
            hop_latency: Duration::ZERO,
            sla: Duration::from_secs(2),
        }
    }

    fn spawn_test_node(
        node_id: NodeId,
        renderers: Vec<BoxRenderer>,
        gossip: Arc<dyn GossipBus>,
        transport: Arc<dyn InternalTransport>,
        catalog: Arc<StyleCatalog>,
    ) -> Node {
        spawn_test_node_with_admission(
            node_id,
            renderers,
            gossip,
            transport,
            catalog,
            Arc::new(|| true),
        )
    }

    fn spawn_test_node_with_admission(
        node_id: NodeId,
        renderers: Vec<BoxRenderer>,
        gossip: Arc<dyn GossipBus>,
        transport: Arc<dyn InternalTransport>,
        catalog: Arc<StyleCatalog>,
        render_admission: biei_core::node::RenderAdmission,
    ) -> Node {
        Node::spawn(NodeSpawn {
            id: node_id,
            renderers,
            profile_preparer: Arc::new(crate::renderer::NoopProfilePreparer),
            gossip,
            transport,
            style_catalog: catalog,
            config: ResolvedNodeConfig {
                routing: RoutingConfig {
                    tier1_strategy: Tier1Strategy::PowerOfTwo,
                    tier3_enabled: true,
                    drain_max_queue: 1,
                },
                costs: test_costs(),
                gossip: GossipConfig {
                    publish_interval: Duration::from_secs(60),
                },
                queue_limits: QueueLimits { soft: 1, hard: 2 },
                render_permits: 1,
                native_render_permits: 1,
                source_cache_capacity: 1,
                render_output_cache_capacity_bytes: 0,
            },
            dispatcher_entropy: DispatcherEntropy::Deterministic { run_seed: 0 },
            render_admission,
        })
    }

    fn peer_only_view(peer_id: NodeId) -> ClusterView {
        let mut kvs = NodeKvs::new();
        kvs.insert(
            biei_core::types::RENDER_ADMISSION_GOSSIP_KEY.to_owned(),
            "true".to_owned(),
        );
        biei_core::types::encode_worker_kvs(&mut kvs, 0, None, 0);
        let peer_state = NodeStateView::from_kvs(peer_id.clone(), &kvs);
        ClusterView {
            members: vec![peer_id.clone()],
            states: HashMap::from([(peer_id, peer_state)]),
            generated_at: Instant::now(),
        }
    }

    fn incoming_task() -> InternalTask {
        let now = Instant::now();
        InternalTask {
            id: 99,
            request_id: RequestId::from_string("internal-test"),
            style: StyleRevision {
                id: StyleId("carto/voyager".to_string()),
                version: 1,
            },
            source: None,
            request: RenderRequest::Tile {
                z: 0,
                x: 0,
                y: 0,
                tile_size: 512,
            },
            pixel_ratio: PixelRatio::from(Scale::X2),
            output_format: ImageFormat::Png,
            arrived_at: now,
            deadline: now + Duration::from_secs(2),
            forwarding_hops: 0,
        }
    }

    #[tokio::test]
    async fn http_transport_roundtrips_framed_rejection_response() {
        let addr = spawn_framed_rejection_server().await;
        let transport = HttpTransport::new(Arc::new(StaticResolver { addr })).expect("transport");

        let response = transport
            .send(NodeId::from("peer-a"), forward_request())
            .await
            .expect("framed rejection roundtrip");

        assert!(matches!(
            response.rejected_reason(),
            Some(RejectionReason::QueueFull)
        ));
    }

    #[tokio::test]
    async fn http_transport_returns_raw_image_bytes_from_framed_body() {
        let addr = spawn_raw_image_server(false, Duration::ZERO).await;
        let transport = HttpTransport::new(Arc::new(StaticResolver { addr })).expect("transport");

        let response = transport
            .send(NodeId::from("peer-a"), forward_request())
            .await
            .expect("raw image roundtrip");

        let output = response.output.expect("completed output");
        assert_eq!(output.format, ImageFormat::Png);
        assert_eq!(output.bytes.as_ref(), &[137, 80, 78, 71]);
    }

    #[tokio::test]
    async fn http_transport_uses_origin_response_budget_not_remote_execution_budget() {
        let addr = spawn_raw_image_server(false, Duration::from_millis(50)).await;
        let transport = HttpTransport::new(Arc::new(StaticResolver { addr })).expect("transport");
        let mut request = forward_request();
        request.task.remaining_budget_ms = 20;
        request.origin_response_budget_ms = 200;

        let response = transport
            .send(NodeId::from("peer-a"), request)
            .await
            .expect("origin keeps waiting after the smaller remote budget");

        assert!(response.output.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn http_transport_budget_includes_peer_address_resolution() {
        let transport = HttpTransport::new(Arc::new(SlowResolver {
            addr: "127.0.0.1:9".parse().expect("test address"),
            delay: Duration::from_secs(10),
        }))
        .expect("transport");
        let mut request = forward_request();
        request.origin_response_budget_ms = 50;

        let send =
            tokio::spawn(async move { transport.send(NodeId::from("peer-a"), request).await });
        tokio::time::advance(Duration::from_millis(51)).await;
        let error = send
            .await
            .expect("send task")
            .expect_err("resolver must share the origin response budget");

        assert!(
            matches!(error, ForwardError::Retryable(message) if message.contains("address resolution"))
        );
    }

    #[tokio::test]
    async fn http_transport_subtracts_slow_successful_resolution_from_remote_budget() {
        let addr =
            spawn_raw_image_server_with_options(false, Duration::ZERO, ImageFormat::Png, Some(199))
                .await;
        let transport = HttpTransport::new(Arc::new(SlowResolver {
            addr,
            delay: Duration::from_millis(25),
        }))
        .expect("transport");
        let mut request = forward_request();
        request.task.remaining_budget_ms = 200;
        request.origin_response_budget_ms = 500;

        let response = transport
            .send(NodeId::from("peer-a"), request)
            .await
            .expect("slow successful resolution still leaves a remote budget");
        assert!(response.output.is_some());
    }

    #[tokio::test]
    async fn http_transport_rejects_mismatched_peer_response_identity() {
        let addr = spawn_raw_image_server(true, Duration::ZERO).await;
        let transport = HttpTransport::new(Arc::new(StaticResolver { addr })).expect("transport");

        let error = transport
            .send(NodeId::from("peer-a"), forward_request())
            .await
            .expect_err("a response for another task must not be cached or returned");

        assert!(
            matches!(error, ForwardError::Fatal(message) if message.contains("identity mismatch"))
        );
    }

    #[tokio::test]
    async fn http_transport_rejects_mismatched_peer_output_format() {
        let addr =
            spawn_raw_image_server_with_options(false, Duration::ZERO, ImageFormat::Webp, None)
                .await;
        let transport = HttpTransport::new(Arc::new(StaticResolver { addr })).expect("transport");

        let error = transport
            .send(NodeId::from("peer-a"), forward_request())
            .await
            .expect_err("a peer must not poison a PNG cache key with WebP bytes");

        assert!(
            matches!(error, ForwardError::Fatal(message) if message.contains("output format mismatch"))
        );
    }

    #[tokio::test]
    async fn http_transport_treats_unframed_service_unavailable_as_retryable() {
        let addr = spawn_status_server(StatusCode::SERVICE_UNAVAILABLE).await;
        let transport = HttpTransport::new(Arc::new(StaticResolver { addr })).expect("transport");

        let err = transport
            .send(NodeId::from("peer-a"), forward_request())
            .await
            .expect_err("unframed 503 should trigger peer failover");

        assert!(matches!(err, ForwardError::Retryable(_)));
    }

    #[tokio::test]
    async fn http_transport_treats_bad_request_as_fatal() {
        let addr = spawn_status_server(StatusCode::BAD_REQUEST).await;
        let transport = HttpTransport::new(Arc::new(StaticResolver { addr })).expect("transport");

        let err = transport
            .send(NodeId::from("peer-a"), forward_request())
            .await
            .expect_err("400 should be fatal error");

        assert!(matches!(err, ForwardError::Fatal(_)));
    }

    #[tokio::test]
    async fn http_transport_rejects_success_with_wrong_content_type_as_fatal() {
        let addr = spawn_wrong_content_type_server().await;
        let transport = HttpTransport::new(Arc::new(StaticResolver { addr })).expect("transport");

        let err = transport
            .send(NodeId::from("peer-a"), forward_request())
            .await
            .expect_err("200 with wrong content type should be fatal");

        assert!(matches!(err, ForwardError::Fatal(_)));
    }

    #[tokio::test]
    async fn node_forwards_to_peer_over_http_and_returns_render_output() {
        let peer_id = NodeId::from("peer-a");
        let catalog = test_catalog();
        let peer = spawn_test_node(
            peer_id.clone(),
            vec![Box::new(FakeRenderer::new(vec![9, 8, 7, 6])) as BoxRenderer],
            Arc::new(StaticGossip {
                view: peer_only_view(peer_id.clone()),
            }),
            Arc::new(UnexpectedTransport),
            catalog.clone(),
        );
        let peer_addr = spawn_peer_forward_server(peer).await;

        let entry_transport = Arc::new(
            HttpTransport::new(Arc::new(StaticResolver { addr: peer_addr })).expect("transport"),
        );
        let entry = spawn_test_node(
            NodeId::from("entry"),
            Vec::new(),
            Arc::new(StaticGossip {
                view: peer_only_view(peer_id),
            }),
            entry_transport,
            catalog,
        );

        let outcome = entry.handle_incoming(incoming_task()).await;
        let TaskResult::Completed { info, output } = outcome.result else {
            panic!("expected completed forwarded render, got {outcome:?}");
        };

        assert_eq!(info.node_id, NodeId::from("peer-a"));
        assert_eq!(info.route_tier, RouteTier::Tier2HrwBl);
        assert_eq!(output.format, ImageFormat::Png);
        assert_eq!(output.bytes.as_ref(), &[9, 8, 7, 6]);
    }

    #[tokio::test]
    async fn internal_forward_admission_is_independent_and_bounded() {
        let node_id = NodeId::from("peer-a");
        let node = spawn_test_node(
            node_id.clone(),
            vec![Box::new(FakeRenderer::new(vec![1])) as BoxRenderer],
            Arc::new(StaticGossip {
                view: peer_only_view(node_id),
            }),
            Arc::new(UnexpectedTransport),
            test_catalog(),
        );
        let endpoint = InternalForwardEndpoint::with_drain_and_limit(
            node,
            crate::drain::DrainController::new(),
            1,
        );

        let permit = endpoint.try_admit().expect("first request is admitted");
        assert!(endpoint.try_admit().is_none());
        drop(permit);
        assert!(endpoint.try_admit().is_some());
    }

    #[tokio::test]
    async fn internal_forward_rejects_nested_public_limit_bypass() {
        let node_id = NodeId::from("peer-a");
        let node = spawn_test_node(
            node_id.clone(),
            vec![Box::new(FakeRenderer::new(vec![1])) as BoxRenderer],
            Arc::new(StaticGossip {
                view: peer_only_view(node_id),
            }),
            Arc::new(UnexpectedTransport),
            test_catalog(),
        );
        let endpoint = InternalForwardEndpoint::with_drain_and_limit(
            node,
            crate::drain::DrainController::new(),
            1,
        );
        let mut request = forward_request();
        request.task.request = RenderRequest::StaticImage {
            positioning: Positioning::Center {
                lon: 0.0,
                lat: 0.0,
                zoom: 1.0,
                bearing: 0.0,
                pitch: 0.0,
            },
            width: 256,
            height: 256,
            overlays: vec![StaticOverlay::Path(PathOverlay {
                stroke_width: None,
                stroke_color: None,
                stroke_opacity: None,
                fill_color: None,
                fill_opacity: None,
                coordinates: vec![LngLat { lon: 0.0, lat: 0.0 }; 501],
            })],
            before_layer: None,
            padding: Padding::default(),
            addlayer: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            JSON_CONTENT_TYPE.parse().expect("content type"),
        );
        let body = Bytes::from(serde_json::to_vec(&request).expect("forward request"));

        let response = endpoint.handle(&headers, body).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn degraded_renderer_admits_forward_then_node_sheds_the_render() {
        let node_id = NodeId::from("peer-a");
        // Build the node with the production-shaped dynamic probe. It is full
        // initially, then changes after endpoint construction below.
        let supervisor = crate::renderer::actor::RendererActorSupervisor::new(1);
        let node = spawn_test_node_with_admission(
            node_id.clone(),
            vec![Box::new(FakeRenderer::new(vec![1])) as BoxRenderer],
            Arc::new(StaticGossip {
                view: peer_only_view(node_id),
            }),
            Arc::new(UnexpectedTransport),
            test_catalog(),
            supervisor.render_admission_probe(),
        );
        // Single slot: losing it leaves no capacity, so the node must shed the
        // render rather than run it on a still-healthy slot. (A multi-slot pod
        // would keep rendering on its remaining slots — see
        // `can_start_render`.)
        let endpoint = InternalForwardEndpoint::with_drain_and_limit(
            node,
            crate::drain::DrainController::new(),
            1,
        );

        // Degraded-render shedding is no longer decided at internal admission:
        // forwarded requests must reach the node so exact output-cache hits
        // stay reachable over the gossip path (which bypasses Kubernetes
        // Service readiness). `try_admit` reflects only the concurrency
        // semaphore now.
        let permit = endpoint
            .try_admit()
            .expect("degraded endpoint still admits forwards so cache hits stay reachable");

        // Simulate the renderer becoming degraded while axum is buffering the
        // request body, after the pre-body concurrency admission succeeded.
        // The decoded Node path must observe the current state rather than the
        // stale state from `try_admit`.
        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);

        // This forward is a cache miss, so the node sheds the would-be render
        // (a forward-retryable 503) rather than starting native work on a
        // degraded renderer. A FakeRenderer would return 200 if the shed had
        // not happened, so the status distinguishes shed from render.
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            JSON_CONTENT_TYPE.parse().expect("content type"),
        );
        let body = Bytes::from(serde_json::to_vec(&forward_request()).expect("forward request"));
        let response = endpoint.handle_admitted(&headers, body, permit).await;
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "degraded node sheds the forwarded render instead of starting native work"
        );
        let body = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("forward response body");
        let (outcome, image) = decode_response_bytes(body).expect("framed forward response");
        assert_eq!(
            outcome.rejected_reason(),
            Some(RejectionReason::RendererDegraded)
        );
        assert!(image.is_empty());
    }

    #[tokio::test]
    async fn peer_disconnect_does_not_cancel_render_or_release_drain_early() {
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let node_id = NodeId::from("peer-a");
        let node = spawn_test_node(
            node_id.clone(),
            vec![Box::new(GatedRenderer {
                started: started.clone(),
                release: release.clone(),
                loaded: None,
            }) as BoxRenderer],
            Arc::new(StaticGossip {
                view: peer_only_view(node_id),
            }),
            Arc::new(UnexpectedTransport),
            test_catalog(),
        );
        let metrics = node.metrics();
        let drain = crate::drain::DrainController::new();
        let endpoint = InternalForwardEndpoint::with_drain_and_limit(node, drain.clone(), 1);
        let permit = endpoint.try_admit().expect("forward is admitted");
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            JSON_CONTENT_TYPE.parse().expect("content type"),
        );
        let body = Bytes::from(serde_json::to_vec(&forward_request()).expect("forward request"));

        let caller =
            tokio::spawn(async move { endpoint.handle_admitted(&headers, body, permit).await });
        started
            .acquire()
            .await
            .expect("renderer start semaphore remains open")
            .forget();
        assert_eq!(drain.in_flight(), 1);

        caller.abort();
        let _ = caller.await;
        assert_eq!(
            drain.in_flight(),
            1,
            "dropping the HTTP response future must not hide native work"
        );

        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while drain.in_flight() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached render completes and releases drain admission");
        assert!(metrics.render_prometheus().contains(
            "biei_tasks_completed_total{route_tier=\"tier2_hrw_bl\",scope=\"forwarded\"} 1"
        ));
    }
}
