//! Small response shape and classification helpers used by HTTP ingress.

use bytes::Bytes;

use crate::http::REQUEST_ID_HEADER;
use crate::http::error::IngressError;
use crate::types::{FailureKind, RejectionReason, RequestId, TaskOutcome, TaskResult};

pub(crate) const JSON_CONTENT_TYPE: &str = "application/json";
pub(crate) const HTML_CONTENT_TYPE: &str = "text/html; charset=utf-8";

/// `Cache-Control` for successful renders. A render is deterministic for its URL,
/// so it is safe for shared caches (CDN/browser) to reuse — without this header a
/// CDN like Cloudflare treats the image as uncacheable (`cf-cache-status:
/// DYNAMIC`) and every request re-runs the expensive render. `public` allows
/// shared caches; `max-age` (1h) bounds staleness if the upstream style changes.
const RENDER_CACHE_CONTROL: &str = "public, max-age=3600";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressResponse {
    pub status: u16,
    pub content_type: &'static str,
    pub headers: Vec<(&'static str, String)>,
    pub body: Bytes,
}

impl IngressResponse {
    pub(crate) fn image(status: u16, content_type: &'static str, body: Bytes) -> Self {
        Self {
            status,
            content_type,
            headers: vec![("Cache-Control", RENDER_CACHE_CONTROL.to_string())],
            body,
        }
    }

    pub(crate) fn json(status: u16, code: &'static str, detail: impl AsRef<str>) -> Self {
        let body = serde_json::json!({
            "error": code,
            "detail": detail.as_ref(),
        });
        Self {
            status,
            content_type: JSON_CONTENT_TYPE,
            headers: Vec::new(),
            body: Bytes::from(body.to_string()),
        }
    }

    pub(crate) fn html(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: HTML_CONTENT_TYPE,
            headers: vec![("Cache-Control", "max-age=300".to_string())],
            body: Bytes::from(body),
        }
    }

    pub(crate) fn with_retry_after(mut self, seconds: &'static str) -> Self {
        self.headers.push(("Retry-After", seconds.to_string()));
        self
    }

    pub(crate) fn with_request_id(mut self, request_id: &RequestId) -> Self {
        self.headers
            .push((REQUEST_ID_HEADER, request_id.as_str().to_string()));
        self
    }
}

pub fn response_from_ingress_error(err: IngressError) -> IngressResponse {
    match err {
        IngressError::InvalidRequest(detail) => {
            IngressResponse::json(400, "invalid_request", detail)
        }
        IngressError::UnknownStyle(style_id) => {
            IngressResponse::json(404, "unknown_style", style_id.as_str())
        }
    }
}

pub fn response_from_outcome(outcome: TaskOutcome) -> IngressResponse {
    let request_id = outcome.request_id.clone();
    match outcome.result {
        TaskResult::Completed { output, .. } => {
            IngressResponse::image(200, output.format.content_type(), output.bytes)
                .with_request_id(&request_id)
        }
        TaskResult::Rejected { reason } => {
            response_from_rejection(reason).with_request_id(&request_id)
        }
        TaskResult::Failed { kind, .. } => {
            tracing::warn!(
                request_id = request_id.as_str(),
                failure_kind = ?kind,
                "render request failed"
            );
            response_from_failure(kind).with_request_id(&request_id)
        }
    }
}

pub(crate) fn response_from_rejection(reason: RejectionReason) -> IngressResponse {
    match reason {
        RejectionReason::UnknownStyle => IngressResponse::json(404, "unknown_style", ""),
        RejectionReason::QueueFull => {
            IngressResponse::json(503, "queue_full", "").with_retry_after("1")
        }
        RejectionReason::NoCapacity => {
            IngressResponse::json(503, "no_capacity", "").with_retry_after("5")
        }
        RejectionReason::DrainTooSlow => {
            IngressResponse::json(503, "drain_too_slow", "").with_retry_after("2")
        }
        RejectionReason::ForwardFailed => {
            IngressResponse::json(503, "forward_failed", "").with_retry_after("1")
        }
        RejectionReason::HopLimitExceeded => IngressResponse::json(500, "hop_limit", ""),
        RejectionReason::DeadlineTooClose | RejectionReason::DeadlineExceeded => {
            IngressResponse::json(504, "budget_exhausted", "").with_retry_after("1")
        }
    }
}

fn response_from_failure(kind: FailureKind) -> IngressResponse {
    match kind {
        FailureKind::RenderTimeout => {
            IngressResponse::json(504, "render_timeout", "").with_retry_after("1")
        }
        FailureKind::RendererDead => {
            IngressResponse::json(500, "renderer_dead", "").with_retry_after("1")
        }
        FailureKind::StyleUnavailable => IngressResponse::json(502, "style_unavailable", ""),
        FailureKind::StyleNotReady => IngressResponse::json(500, "style_not_ready", ""),
        FailureKind::SourceUnavailable => IngressResponse::json(502, "source_unavailable", ""),
        FailureKind::Other => IngressResponse::json(500, "render_failed", ""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        CompletedInfo, ImageFormat, NodeId, RenderOutput, RouteTier, StyleId, TaskId, TaskOutcome,
    };
    use tokio::time::Instant;

    fn completed_outcome() -> TaskOutcome {
        let now = Instant::now();
        TaskOutcome {
            task_id: 1 as TaskId,
            request_id: RequestId::from_string("rid"),
            arrived_at: now,
            had_source: false,
            deadline_stage: None,
            result: TaskResult::Completed {
                info: CompletedInfo {
                    node_id: NodeId::from_index(1),
                    worker_id: Some(1),
                    route_tier: RouteTier::Tier1WarmTracking,
                    started_at: now,
                    cpu_started_at: now,
                    cpu_completed_at: now,
                    completed_at: now,
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
                    render_observation: None,
                },
                output: RenderOutput {
                    bytes: vec![1, 2, 3].into(),
                    format: ImageFormat::Png,
                },
            },
        }
    }

    #[test]
    fn maps_completed_outcome_to_image_response() {
        let response = response_from_outcome(completed_outcome());

        assert_eq!(response.status, 200);
        assert_eq!(response.content_type, ImageFormat::Png.content_type());
        assert_eq!(response.body.as_ref(), &[1, 2, 3]);
        assert_eq!(
            response.headers,
            vec![
                ("Cache-Control", "public, max-age=3600".to_string()),
                (REQUEST_ID_HEADER, "rid".to_string())
            ]
        );
    }

    #[test]
    fn maps_retryable_rejection_to_retry_after() {
        let response = response_from_rejection(RejectionReason::QueueFull);

        assert_eq!(response.status, 503);
        assert_eq!(response.headers, vec![("Retry-After", "1".to_string())]);
    }

    #[test]
    fn maps_ingress_errors_to_json_responses() {
        let invalid =
            response_from_ingress_error(IngressError::InvalidRequest("bad \"format\"".to_string()));
        assert_eq!(invalid.status, 400);
        assert!(
            std::str::from_utf8(&invalid.body)
                .unwrap()
                .contains(r#"bad \"format\""#)
        );

        let unknown =
            response_from_ingress_error(IngressError::UnknownStyle(StyleId("missing".to_string())));
        assert_eq!(unknown.status, 404);
    }

    #[test]
    fn maps_renderer_failure_strings_to_http_response() {
        let now = Instant::now();
        let response = response_from_outcome(TaskOutcome {
            task_id: 1,
            request_id: RequestId::from_string("rid"),
            arrived_at: now,
            had_source: false,
            deadline_stage: None,
            result: TaskResult::Failed {
                error: "fetch failed for https://provider.test/style?token=secret".to_string(),
                kind: FailureKind::RenderTimeout,
            },
        });

        assert_eq!(response.status, 504);
        assert_eq!(
            response.headers,
            vec![
                ("Retry-After", "1".to_string()),
                (REQUEST_ID_HEADER, "rid".to_string())
            ]
        );
        let body = std::str::from_utf8(&response.body).expect("json body");
        assert!(body.contains("render_timeout"));
        assert!(!body.contains("provider.test"));
        assert!(!body.contains("secret"));
    }
}
