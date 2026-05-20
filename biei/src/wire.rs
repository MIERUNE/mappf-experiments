//! Wire-safe request/response shapes for inter-node forwarding.
//!
//! `InternalTask` intentionally carries process-local time (`Instant`).
//! Cross-node transport must go through these types so clock assumptions do
//! not leak onto the wire.

use std::error::Error;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::types::{
    CachePolicy, CompletedInfo, DeadlineStage, FailureKind, ImageFormat, InternalTask,
    RejectionReason, RenderOutput, RenderRequest, RequestId, RouteTier, Scale, SourceHash,
    SourceRef, StyleId, StyleRevision, TaskId, TaskOutcome, TaskResult, WorkerId,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireTask {
    pub id: TaskId,
    #[serde(default)]
    pub request_id: RequestId,
    pub style: StyleRevision,
    pub source: Option<WireSourceRef>,
    pub request: RenderRequest,
    pub scale: Scale,
    pub output_format: ImageFormat,
    /// Remaining task budget at send time. Receiver reconstructs its own
    /// local deadline as `Instant::now() + remaining_budget_ms`.
    pub remaining_budget_ms: u32,
    pub forwarding_hops: u8,
}

/// Envelope used when one node forwards a task to another. The task payload
/// is wire-safe: process-local `Instant` has already been converted to a
/// clock-skew-safe budget and stable style identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwardRequest {
    pub task: WireTask,
    pub route_tier: RouteTier,
    pub drain_worker: Option<WorkerId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireSourceRef {
    pub hash: SourceHash,
    pub policy: CachePolicy,
}

impl From<&SourceRef> for WireSourceRef {
    fn from(value: &SourceRef) -> Self {
        Self {
            hash: value.hash,
            policy: value.policy,
        }
    }
}

impl From<WireSourceRef> for SourceRef {
    fn from(value: WireSourceRef) -> Self {
        Self {
            hash: value.hash,
            policy: value.policy,
        }
    }
}

impl InternalTask {
    pub fn to_wire(&self, now: Instant) -> WireTask {
        self.to_wire_with_hop_latency(now, Duration::ZERO)
    }

    pub fn to_wire_with_hop_latency(&self, now: Instant, hop_latency: Duration) -> WireTask {
        let budget = self
            .deadline
            .saturating_duration_since(now)
            .saturating_sub(hop_latency);
        WireTask {
            id: self.id,
            request_id: self.request_id.clone(),
            style: self.style.clone(),
            source: self.source.as_ref().map(WireSourceRef::from),
            request: self.request.clone(),
            scale: self.pixel_ratio.to_scale(),
            output_format: self.output_format,
            remaining_budget_ms: budget.as_millis().min(u32::MAX as u128) as u32,
            forwarding_hops: self.forwarding_hops,
        }
    }
}

impl WireTask {
    /// Reconstruct a process-local task after the caller has validated the
    /// wire style revision against its catalog.
    pub fn into_internal(self, now: Instant) -> InternalTask {
        InternalTask {
            id: self.id,
            request_id: self.request_id,
            style: self.style,
            source: self.source.map(SourceRef::from),
            request: self.request,
            pixel_ratio: self.scale.into(),
            output_format: self.output_format,
            arrived_at: now,
            deadline: now + Duration::from_millis(self.remaining_budget_ms as u64),
            forwarding_hops: self.forwarding_hops,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ForwardResponse {
    pub outcome: OutcomeHeader,
    pub output: Option<RenderOutput>,
}

/// JSON metadata carried at the front of an internal forward response body.
/// Large image bytes follow this metadata as raw bytes in the same body frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeHeader {
    pub task_id: TaskId,
    #[serde(default)]
    pub request_id: RequestId,
    pub style_id: StyleId,
    #[serde(default)]
    pub had_source: bool,
    #[serde(default)]
    pub image_format: Option<ImageFormat>,
    #[serde(flatten)]
    pub result: OutcomeResult,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeResult {
    Completed {
        node_id: crate::types::NodeId,
        #[serde(default)]
        worker_id: Option<WorkerId>,
        route_tier: RouteTier,
        render_started_ms: u64,
        cpu_started_ms: u64,
        cpu_completed_ms: u64,
        completed_ms: u64,
        style_swap: bool,
        cold_start: bool,
        source_loaded: bool,
        admitted_at_overflow: bool,
    },
    Rejected {
        reason: RejectionReason,
        #[serde(default)]
        deadline_stage: Option<DeadlineStage>,
    },
    Failed {
        error: String,
        kind: FailureKind,
    },
}

#[derive(Debug)]
pub enum WireError {
    Encode(serde_json::Error),
    Decode(serde_json::Error),
    Truncated,
    TooLarge,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(err) => write!(f, "encode forward response metadata: {err}"),
            Self::Decode(err) => write!(f, "decode forward response metadata: {err}"),
            Self::Truncated => f.write_str("truncated forward response body"),
            Self::TooLarge => f.write_str("forward response metadata exceeds u32 length"),
        }
    }
}

impl Error for WireError {}

pub fn encode_response_body(
    header: &OutcomeHeader,
    image_bytes: &[u8],
) -> Result<Vec<u8>, WireError> {
    let json = serde_json::to_vec(header).map_err(WireError::Encode)?;
    let json_len: u32 = json.len().try_into().map_err(|_| WireError::TooLarge)?;
    let mut out = Vec::with_capacity(4 + json.len() + image_bytes.len());
    out.extend_from_slice(&json_len.to_be_bytes());
    out.extend_from_slice(&json);
    out.extend_from_slice(image_bytes);
    Ok(out)
}

pub fn decode_response_body(body: &[u8]) -> Result<(OutcomeHeader, &[u8]), WireError> {
    if body.len() < 4 {
        return Err(WireError::Truncated);
    }
    let json_len = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let payload_start = 4usize;
    let payload_end = payload_start
        .checked_add(json_len)
        .ok_or(WireError::Truncated)?;
    if body.len() < payload_end {
        return Err(WireError::Truncated);
    }
    let header =
        serde_json::from_slice(&body[payload_start..payload_end]).map_err(WireError::Decode)?;
    Ok((header, &body[payload_end..]))
}

impl ForwardResponse {
    pub fn from_task_outcome(outcome: TaskOutcome, style_id: StyleId) -> Self {
        let (outcome, output) = OutcomeHeader::from_task_outcome(outcome, style_id);
        Self { outcome, output }
    }

    /// Convert a wire response back into the current in-process outcome.
    pub fn into_task_outcome(self, arrived_at: Instant) -> TaskOutcome {
        let ForwardResponse { outcome, output } = self;
        outcome.into_task_outcome(arrived_at, output)
    }

    pub fn rejected_reason(&self) -> Option<RejectionReason> {
        self.outcome.rejected_reason()
    }
}

impl OutcomeHeader {
    pub fn from_task_outcome(
        outcome: TaskOutcome,
        style_id: StyleId,
    ) -> (Self, Option<RenderOutput>) {
        let TaskOutcome {
            task_id,
            request_id,
            arrived_at,
            had_source,
            deadline_stage,
            result,
        } = outcome;

        let mut output = None;
        let mut image_format = None;
        let result = match result {
            TaskResult::Completed {
                info,
                output: render_output,
            } => {
                image_format = Some(render_output.format);
                output = Some(render_output);
                OutcomeResult::Completed {
                    node_id: info.node_id,
                    worker_id: info.worker_id,
                    route_tier: info.route_tier,
                    render_started_ms: millis_since(info.started_at, arrived_at),
                    cpu_started_ms: millis_since(info.cpu_started_at, arrived_at),
                    cpu_completed_ms: millis_since(info.cpu_completed_at, arrived_at),
                    completed_ms: millis_since(info.completed_at, arrived_at),
                    style_swap: info.style_swap,
                    cold_start: info.cold_start,
                    source_loaded: info.source_loaded,
                    admitted_at_overflow: info.admitted_at_overflow,
                }
            }
            TaskResult::Rejected { reason } => OutcomeResult::Rejected {
                reason,
                deadline_stage,
            },
            TaskResult::Failed { error, kind } => OutcomeResult::Failed { error, kind },
        };

        (
            Self {
                task_id,
                request_id,
                style_id,
                had_source,
                image_format,
                result,
            },
            output,
        )
    }

    pub fn into_task_outcome(
        self,
        arrived_at: Instant,
        output: Option<RenderOutput>,
    ) -> TaskOutcome {
        let OutcomeHeader {
            task_id,
            request_id,
            had_source,
            result,
            ..
        } = self;
        let mut deadline_stage = None;
        let result = match result {
            OutcomeResult::Completed {
                node_id,
                worker_id,
                route_tier,
                render_started_ms,
                cpu_started_ms,
                cpu_completed_ms,
                completed_ms,
                style_swap,
                cold_start,
                source_loaded,
                admitted_at_overflow,
            } => {
                let started_at = arrived_at + Duration::from_millis(render_started_ms);
                let cpu_started_at = arrived_at + Duration::from_millis(cpu_started_ms);
                let cpu_completed_at = arrived_at + Duration::from_millis(cpu_completed_ms);
                let completed_at = arrived_at + Duration::from_millis(completed_ms);
                if let Some(output) = output {
                    TaskResult::Completed {
                        info: CompletedInfo {
                            node_id,
                            worker_id,
                            route_tier,
                            started_at,
                            cpu_started_at,
                            cpu_completed_at,
                            completed_at,
                            style_swap,
                            cold_start,
                            source_loaded,
                            admitted_at_overflow,
                        },
                        output,
                    }
                } else {
                    TaskResult::Failed {
                        error: "completed response missing render output".to_string(),
                        kind: FailureKind::Other,
                    }
                }
            }
            OutcomeResult::Rejected {
                reason,
                deadline_stage: stage,
            } => {
                deadline_stage = stage;
                TaskResult::Rejected { reason }
            }
            OutcomeResult::Failed { error, kind } => TaskResult::Failed { error, kind },
        };

        TaskOutcome {
            task_id,
            request_id,
            arrived_at,
            had_source,
            deadline_stage,
            result,
        }
    }

    pub fn rejected_reason(&self) -> Option<RejectionReason> {
        match &self.result {
            OutcomeResult::Rejected { reason, .. } => Some(*reason),
            _ => None,
        }
    }

    pub fn completed_format(&self) -> Option<ImageFormat> {
        match &self.result {
            OutcomeResult::Completed { .. } => self.image_format,
            _ => None,
        }
    }
}

fn millis_since(instant: Instant, base: Instant) -> u64 {
    instant
        .saturating_duration_since(base)
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PixelRatio, RenderRequest};

    fn style() -> StyleRevision {
        StyleRevision {
            id: StyleId("style-1".to_string()),
            version: 7,
        }
    }

    #[test]
    fn internal_task_roundtrips_through_wire() {
        let now = Instant::now();
        let task = InternalTask {
            id: 42,
            request_id: RequestId::from_string("wire-test"),
            style: style(),
            source: Some(SourceRef {
                hash: 123,
                policy: crate::types::CachePolicy::Cacheable,
            }),
            request: RenderRequest::Tile {
                z: 1,
                x: 2,
                y: 3,
                tile_size: 256,
            },
            pixel_ratio: PixelRatio::from(Scale::X2),
            output_format: ImageFormat::Webp,
            arrived_at: now,
            deadline: now + Duration::from_secs(3),
            forwarding_hops: 1,
        };

        let wire = task.to_wire(now);
        assert_eq!(wire.style, style());
        assert_eq!(wire.scale, Scale::X2);
        assert!(wire.remaining_budget_ms <= 3000);

        let rebuilt = wire.into_internal(now);
        assert_eq!(rebuilt.id, 42);
        assert_eq!(rebuilt.style, style());
        assert_eq!(rebuilt.pixel_ratio.to_scale(), Scale::X2);
        assert_eq!(rebuilt.output_format, ImageFormat::Webp);
    }

    #[test]
    fn wire_budget_subtracts_forward_hop_latency() {
        let now = Instant::now();
        let task = InternalTask {
            id: 42,
            request_id: RequestId::from_string("wire-hop-test"),
            style: style(),
            source: None,
            request: RenderRequest::Tile {
                z: 1,
                x: 2,
                y: 3,
                tile_size: 256,
            },
            pixel_ratio: PixelRatio::from(Scale::X1),
            output_format: ImageFormat::Png,
            arrived_at: now,
            deadline: now + Duration::from_secs(3),
            forwarding_hops: 1,
        };

        let wire = task.to_wire_with_hop_latency(now, Duration::from_millis(125));
        assert_eq!(wire.remaining_budget_ms, 2875);
    }

    #[test]
    fn forward_response_reconstructs_peer_times_from_arrival_offsets() {
        let now = Instant::now();
        let response = ForwardResponse {
            outcome: OutcomeHeader {
                task_id: 7,
                request_id: RequestId::from_string("response-test"),
                style_id: style().id,
                had_source: false,
                image_format: Some(ImageFormat::Png),
                result: OutcomeResult::Completed {
                    node_id: crate::types::NodeId::from_index(2),
                    worker_id: Some(3),
                    route_tier: RouteTier::Tier2HrwBl,
                    render_started_ms: 25,
                    cpu_started_ms: 40,
                    cpu_completed_ms: 70,
                    completed_ms: 100,
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
                },
            },
            output: Some(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: ImageFormat::Png,
            }),
        };

        let outcome = response.into_task_outcome(now);
        let TaskResult::Completed { info, output } = outcome.result else {
            panic!("expected completed outcome");
        };
        assert_eq!(info.started_at, now + Duration::from_millis(25));
        assert_eq!(info.cpu_started_at, now + Duration::from_millis(40));
        assert_eq!(info.cpu_completed_at, now + Duration::from_millis(70));
        assert_eq!(info.completed_at, now + Duration::from_millis(100));
        assert_eq!(output.format, ImageFormat::Png);
    }

    #[test]
    fn forward_response_from_task_outcome_carries_render_output() {
        let now = Instant::now();
        let outcome = TaskOutcome {
            task_id: 9,
            request_id: RequestId::from_string("outcome-test"),
            arrived_at: now,
            had_source: false,
            deadline_stage: None,
            result: TaskResult::Completed {
                info: CompletedInfo {
                    node_id: crate::types::NodeId::from_index(2),
                    worker_id: Some(3),
                    route_tier: RouteTier::Tier2HrwBl,
                    started_at: now,
                    cpu_started_at: now + Duration::from_millis(2),
                    cpu_completed_at: now + Duration::from_millis(8),
                    completed_at: now + Duration::from_millis(10),
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
                },
                output: RenderOutput {
                    bytes: vec![1, 2, 3].into(),
                    format: ImageFormat::Webp,
                },
            },
        };

        let response = ForwardResponse::from_task_outcome(outcome, style().id);

        assert_eq!(
            response.output,
            Some(RenderOutput {
                bytes: vec![1, 2, 3].into(),
                format: ImageFormat::Webp,
            })
        );
        assert_eq!(response.outcome.completed_format(), Some(ImageFormat::Webp));
    }

    #[test]
    fn forward_response_preserves_failed_result() {
        let now = Instant::now();
        let response = ForwardResponse {
            outcome: OutcomeHeader {
                task_id: 8,
                request_id: RequestId::from_string("failed-test"),
                style_id: style().id,
                had_source: true,
                image_format: None,
                result: OutcomeResult::Failed {
                    error: "renderer actor dead".to_string(),
                    kind: FailureKind::RendererDead,
                },
            },
            output: None,
        };

        let outcome = response.into_task_outcome(now);
        let TaskResult::Failed { error, .. } = outcome.result else {
            panic!("expected failed outcome");
        };
        assert_eq!(error, "renderer actor dead");
        assert!(outcome.had_source);
    }

    #[test]
    fn outcome_header_roundtrips_completed_rejected_and_failed() {
        let completed = OutcomeHeader {
            task_id: 1,
            request_id: RequestId::from_string("completed-test"),
            style_id: style().id,
            had_source: false,
            image_format: Some(ImageFormat::Png),
            result: OutcomeResult::Completed {
                node_id: crate::types::NodeId::from_index(2),
                worker_id: Some(3),
                route_tier: RouteTier::Tier1WarmTracking,
                render_started_ms: 1,
                cpu_started_ms: 2,
                cpu_completed_ms: 8,
                completed_ms: 11,
                style_swap: false,
                cold_start: false,
                source_loaded: false,
                admitted_at_overflow: false,
            },
        };
        let json = serde_json::to_string(&completed).expect("completed header JSON");
        assert!(json.contains("\"completed\""));
        let decoded: OutcomeHeader = serde_json::from_str(&json).expect("completed decodes");
        assert_eq!(decoded.completed_format(), Some(ImageFormat::Png));

        let rejected = OutcomeHeader {
            task_id: 2,
            request_id: RequestId::from_string("rejected-test"),
            style_id: style().id,
            had_source: true,
            image_format: None,
            result: OutcomeResult::Rejected {
                reason: RejectionReason::QueueFull,
                deadline_stage: None,
            },
        };
        let json = serde_json::to_string(&rejected).expect("rejected header JSON");
        let decoded: OutcomeHeader = serde_json::from_str(&json).expect("rejected decodes");
        assert_eq!(decoded.rejected_reason(), Some(RejectionReason::QueueFull));
        assert!(decoded.had_source);

        let failed = OutcomeHeader {
            task_id: 3,
            request_id: RequestId::from_string("failed-test"),
            style_id: style().id,
            had_source: false,
            image_format: None,
            result: OutcomeResult::Failed {
                error: "renderer failed".to_string(),
                kind: FailureKind::Other,
            },
        };
        let json = serde_json::to_string(&failed).expect("failed header JSON");
        let decoded: OutcomeHeader = serde_json::from_str(&json).expect("failed decodes");
        assert!(matches!(decoded.result, OutcomeResult::Failed { .. }));
    }

    #[test]
    fn outcome_header_ignores_unknown_fields() {
        let json = r#"{
            "task_id": 1,
            "style_id": "style-1",
            "had_source": false,
            "image_format": null,
            "future_field": "ignored",
            "rejected": {
                "reason": "QueueFull",
                "future_nested_field": 42
            }
        }"#;

        let decoded: OutcomeHeader = serde_json::from_str(json).expect("unknown fields ignored");
        assert_eq!(decoded.rejected_reason(), Some(RejectionReason::QueueFull));
    }

    #[test]
    fn response_body_frame_roundtrips_metadata_and_image_bytes() {
        let header = OutcomeHeader {
            task_id: 1,
            request_id: RequestId::from_string("frame-test"),
            style_id: style().id,
            had_source: true,
            image_format: Some(ImageFormat::Webp),
            result: OutcomeResult::Completed {
                node_id: crate::types::NodeId::from_index(2),
                worker_id: Some(3),
                route_tier: RouteTier::Tier1WarmTracking,
                render_started_ms: 1,
                cpu_started_ms: 2,
                cpu_completed_ms: 8,
                completed_ms: 11,
                style_swap: false,
                cold_start: false,
                source_loaded: true,
                admitted_at_overflow: false,
            },
        };
        let image = [1, 2, 3, 4, 5];

        let body = encode_response_body(&header, &image).expect("encode body");
        let (decoded, decoded_image) = decode_response_body(&body).expect("decode body");

        assert_eq!(decoded, header);
        assert_eq!(decoded_image, image);
    }

    #[test]
    fn response_body_frame_roundtrips_rejected_and_failed_without_image() {
        let rejected = OutcomeHeader {
            task_id: 2,
            request_id: RequestId::from_string("rejected-frame-test"),
            style_id: style().id,
            had_source: false,
            image_format: None,
            result: OutcomeResult::Rejected {
                reason: RejectionReason::NoCapacity,
                deadline_stage: None,
            },
        };
        let body = encode_response_body(&rejected, &[]).expect("encode rejected");
        let (decoded, image) = decode_response_body(&body).expect("decode rejected");
        assert_eq!(decoded.rejected_reason(), Some(RejectionReason::NoCapacity));
        assert!(image.is_empty());

        let failed = OutcomeHeader {
            task_id: 3,
            request_id: RequestId::from_string("failed-frame-test"),
            style_id: style().id,
            had_source: false,
            image_format: None,
            result: OutcomeResult::Failed {
                error: "renderer failed".to_string(),
                kind: FailureKind::Other,
            },
        };
        let body = encode_response_body(&failed, &[]).expect("encode failed");
        let (decoded, image) = decode_response_body(&body).expect("decode failed");
        assert!(matches!(decoded.result, OutcomeResult::Failed { .. }));
        assert!(image.is_empty());
    }

    #[test]
    fn response_body_frame_reports_malformed_bodies() {
        assert!(matches!(
            decode_response_body(&[0, 0, 0]),
            Err(WireError::Truncated)
        ));
        assert!(matches!(
            decode_response_body(&[0, 0, 0, 10, b'{']),
            Err(WireError::Truncated)
        ));
        assert!(matches!(
            decode_response_body(&[0, 0, 0, 1, b'{']),
            Err(WireError::Decode(_))
        ));
    }
}
