//! Internal peer-to-peer HTTP forwarding.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use axum::body::{Body, Bytes};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use crate::drain::DrainController;
use crate::http::REQUEST_ID_HEADER;
use crate::node::Node;
use crate::transport::{ForwardError, Transport};
use crate::types::{FailureKind, NodeId, RejectionReason, RenderOutput, RequestId};
use crate::wire::{
    ForwardRequest, ForwardResponse, OutcomeHeader, OutcomeResult, decode_response_body,
    encode_response_body,
};

const JSON_CONTENT_TYPE: &str = "application/json";
const BIEI_RESPONSE_CONTENT_TYPE: &str = "application/x-biei-forward-response";
const MAX_FORWARD_TIMEOUT: Duration = Duration::from_secs(30);

#[async_trait]
pub trait PeerResolver: Send + Sync {
    async fn advertise_addr_of(&self, node_id: &NodeId) -> Option<SocketAddr>;
}

#[async_trait]
impl PeerResolver for crate::membership::Membership {
    async fn advertise_addr_of(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.advertise_addr_of(node_id).await
    }
}

#[derive(Clone)]
pub struct InternalForwardEndpoint {
    node: Node,
    drain: Option<DrainController>,
}

impl InternalForwardEndpoint {
    pub fn with_drain(node: Node, drain: DrainController) -> Self {
        Self {
            node,
            drain: Some(drain),
        }
    }

    pub async fn handle(&self, headers: &HeaderMap, body: Bytes) -> Response {
        if !is_json_content_type(headers) {
            return response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                b"unsupported media type".to_vec(),
                "text/plain",
            );
        }
        let _drain_permit = match &self.drain {
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
        let style_id = forwarded.task.style.id.clone();
        let outcome = self.node.handle_forwarded(forwarded).await;
        let (outcome, output) = OutcomeHeader::from_task_outcome(outcome, style_id);
        response_from_outcome(outcome, output)
    }
}

pub struct HttpTransport {
    client: reqwest::Client,
    resolver: Arc<dyn PeerResolver>,
}

impl HttpTransport {
    pub fn new(resolver: Arc<dyn PeerResolver>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .build()
            .context("build HTTP forward client")?;
        Ok(Self { client, resolver })
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn send(
        &self,
        target: NodeId,
        fwd: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError> {
        let Some(addr) = self.resolver.advertise_addr_of(&target).await else {
            return Err(ForwardError::Retryable(format!(
                "no advertise address for node {target}"
            )));
        };
        let body = serde_json::to_vec(&fwd)
            .map_err(|err| ForwardError::Fatal(format!("encode forwarded request: {err}")))?;
        let timeout = Duration::from_millis(fwd.task.remaining_budget_ms as u64)
            .min(MAX_FORWARD_TIMEOUT)
            .max(Duration::from_millis(1));
        let url = format!("http://{addr}/_internal/forward");
        let response = self
            .client
            .post(url)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .header(REQUEST_ID_HEADER, fwd.task.request_id.as_str())
            .body(body)
            .timeout(timeout)
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
            return Err(ForwardError::Fatal(format!(
                "peer returned {status} without {BIEI_RESPONSE_CONTENT_TYPE} response body"
            )));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|err| ForwardError::Retryable(format!("read response body: {err}")))?;
        let (outcome, image_bytes) = decode_response_body(&bytes)
            .map_err(|err| ForwardError::Fatal(format!("decode forward response body: {err}")))?;

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
            Some(format) => Some(RenderOutput {
                bytes: bytes::Bytes::copy_from_slice(image_bytes),
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

fn response(status: StatusCode, body: Vec<u8>, content_type: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .body(Body::from(body.clone()))
        .unwrap_or_else(|_| Response::new(Body::from(body)))
}

fn response_from_outcome(outcome: OutcomeHeader, output: Option<RenderOutput>) -> Response {
    let status = status_for_outcome(&outcome);
    let request_id = outcome.request_id.as_str().to_string();
    let image_bytes = output
        .as_ref()
        .map_or(&[][..], |output| output.bytes.as_ref());
    let body = match encode_response_body(&outcome, image_bytes) {
        Ok(body) => body,
        Err(err) => {
            return response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("encode forward response body: {err}").into_bytes(),
                "text/plain; charset=utf-8",
            );
        }
    };
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, BIEI_RESPONSE_CONTENT_TYPE)
        .header(REQUEST_ID_HEADER, request_id)
        .body(Body::from(body.clone()))
        .unwrap_or_else(|_| Response::new(Body::from(body)))
}

fn request_id_from_headers(headers: &HeaderMap) -> Option<RequestId> {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(RequestId::from_string)
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
            | RejectionReason::DrainTooSlow
            | RejectionReason::ForwardFailed => StatusCode::SERVICE_UNAVAILABLE,
        },
        OutcomeResult::Failed { kind, .. } => match kind {
            FailureKind::RenderTimeout => StatusCode::GATEWAY_TIMEOUT,
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
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(JSON_CONTENT_TYPE))
}

fn is_biei_response_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case(BIEI_RESPONSE_CONTENT_TYPE)
        })
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

    use crate::activity::ProfileActivityTracker;
    use crate::config::{CostConfig, CostRange, GossipConfig, RoutingConfig, Tier1Strategy};
    use crate::gossip::GossipBus;
    use crate::node::{Node, NodeSpawn};
    use crate::renderer::{BoxRenderer, Renderer};
    use crate::style_catalog::StyleCatalog;
    use crate::types::{
        ClusterView, ImageFormat, InternalTask, NodeKvs, NodeStateView, PixelRatio,
        RejectionReason, RenderOutput, RenderRequest, RendererError, RouteTier, Scale, SourceHash,
        StyleId, StyleRevision, TaskResult,
    };
    use crate::wire::{ForwardRequest, OutcomeHeader, OutcomeResult, WireTask};

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
        let app = Router::new().fallback(|request: Request<Body>| async move {
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

    async fn spawn_raw_image_server() -> SocketAddr {
        let app = Router::new().fallback(|request: Request<Body>| async move {
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
                image_format: Some(ImageFormat::Png),
                result: OutcomeResult::Completed {
                    node_id: NodeId::from("peer-a"),
                    worker_id: Some(2),
                    route_tier: RouteTier::Tier2HrwBl,
                    render_started_ms: 1,
                    cpu_started_ms: 2,
                    cpu_completed_ms: 10,
                    completed_ms: 12,
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
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
        async fn set(&self, _node_id: NodeId, _key: String, _value: String) {}

        async fn view(&self) -> ClusterView {
            self.view.clone()
        }
    }

    struct FakeRenderer {
        bytes: Vec<u8>,
        loaded: Option<crate::types::WorkerProfile>,
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

        async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError> {
            if self.loaded.as_ref() != Some(&task.worker_profile()) {
                return Err(RendererError::StyleNotReady {
                    style_id: task.style.id.clone(),
                    version: task.style.version,
                });
            }
            Ok(RenderOutput {
                bytes: self.bytes.clone().into(),
                format: task.output_format,
            })
        }
    }

    struct UnexpectedTransport;

    #[async_trait]
    impl Transport for UnexpectedTransport {
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
            render_cost: CostRange::fixed(Duration::from_millis(1)),
            hop_latency: Duration::ZERO,
            sla: Duration::from_secs(2),
        }
    }

    fn spawn_test_node(
        node_id: NodeId,
        renderers: Vec<BoxRenderer>,
        gossip: Arc<dyn GossipBus>,
        transport: Arc<dyn Transport>,
        catalog: Arc<StyleCatalog>,
    ) -> Node {
        Node::spawn(NodeSpawn {
            id: node_id,
            renderers,
            profile_preparer: Arc::new(crate::renderer::NoopProfilePreparer),
            gossip,
            transport,
            style_catalog: catalog,
            activity: Arc::new(ProfileActivityTracker::new()),
            routing: RoutingConfig {
                tier1_strategy: Tier1Strategy::PowerOfTwo,
                tier3_enabled: true,
                drain_max_queue: 1,
            },
            costs: test_costs(),
            gossip_cfg: GossipConfig {
                publish_interval: Duration::from_secs(60),
            },
            bl_capacity: 1,
            queue_capacity: 2,
            render_permits: 1,
            cpu_render_permits: 1,
            source_cache_capacity: 1,
            render_output_cache_capacity_bytes: 0,
            dispatcher_seed: 0,
        })
    }

    fn peer_only_view(peer_id: NodeId) -> ClusterView {
        let mut kvs = NodeKvs::new();
        crate::types::encode_worker_kvs(&mut kvs, 0, None, 0);
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
        let addr = spawn_raw_image_server().await;
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
    async fn http_transport_treats_unframed_service_unavailable_as_fatal() {
        let addr = spawn_status_server(StatusCode::SERVICE_UNAVAILABLE).await;
        let transport = HttpTransport::new(Arc::new(StaticResolver { addr })).expect("transport");

        let err = transport
            .send(NodeId::from("peer-a"), forward_request())
            .await
            .expect_err("unframed 503 should be fatal");

        assert!(matches!(err, ForwardError::Fatal(_)));
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
}
