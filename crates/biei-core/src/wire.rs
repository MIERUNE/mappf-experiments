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
    RejectionReason, RenderAuthorization, RenderMode, RenderObservation, RenderOutput,
    RenderRequest, RequestId, RouteTier, Scale, SourceHash, SourceRef, StyleId, StyleRevision,
    TaskId, TaskOutcome, TaskResult, WorkerId,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireTask {
    pub id: TaskId,
    pub request_id: RequestId,
    #[serde(deserialize_with = "crate::types::deserialize_required_option")]
    pub authorization: Option<RenderAuthorization>,
    pub style: StyleRevision,
    #[serde(deserialize_with = "crate::types::deserialize_required_option")]
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
    #[serde(deserialize_with = "crate::types::deserialize_required_option")]
    pub drain_worker: Option<WorkerId>,
    /// Origin-local budget for receiving the complete HTTP response. This is
    /// intentionally distinct from `task.remaining_budget_ms`, which is the
    /// smaller remote execution budget after reserving transport latency.
    pub origin_response_budget_ms: u32,
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
    #[cfg(test)]
    pub(crate) fn to_wire(&self, now: Instant) -> WireTask {
        self.to_wire_with_hop_latency(now, Duration::ZERO)
    }

    fn to_wire_with_hop_latency(&self, now: Instant, hop_latency: Duration) -> WireTask {
        let budget = self
            .deadline
            .saturating_duration_since(now)
            .saturating_sub(hop_latency);
        WireTask {
            id: self.id,
            request_id: self.request_id.clone(),
            authorization: self.authorization.clone(),
            style: self.style.clone(),
            source: self.source.as_ref().map(WireSourceRef::from),
            request: self.request.clone(),
            scale: self.pixel_ratio.to_scale(),
            output_format: self.output_format,
            remaining_budget_ms: budget.as_millis().min(u32::MAX as u128) as u32,
            forwarding_hops: self.forwarding_hops,
        }
    }

    pub fn to_forward_wire(&self, now: Instant, hop_latency: Duration) -> WireTask {
        // Reserve the round trip, not just the outbound hop. A native render
        // cannot be cancelled, so a remote render that consumed its whole
        // budget would finish just as the origin's deadline arrives — and its
        // response still needs a return hop the origin will no longer wait
        // for, wasting an uncancellable render. Subtracting both hops keeps a
        // completed remote render's response arriving at/under the origin
        // deadline when the hop estimate holds, and halves the overshoot when
        // the actual hop exceeds it.
        let round_trip = hop_latency.saturating_mul(2);
        let mut wire = self.to_wire_with_hop_latency(now, round_trip);
        wire.forwarding_hops = self.forwarding_hops.saturating_add(1);
        wire
    }
}

impl WireTask {
    /// Reconstruct a process-local task after the caller has validated the
    /// wire style revision against its catalog.
    pub fn into_internal(self, now: Instant) -> InternalTask {
        InternalTask {
            id: self.id,
            request_id: self.request_id,
            authorization: self.authorization,
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
    pub request_id: RequestId,
    pub style_id: StyleId,
    pub had_source: bool,
    #[serde(deserialize_with = "crate::types::deserialize_required_option")]
    pub image_format: Option<ImageFormat>,
    #[serde(flatten)]
    pub result: OutcomeResult,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeResult {
    Completed {
        node_id: crate::types::NodeId,
        #[serde(deserialize_with = "crate::types::deserialize_required_option")]
        worker_id: Option<WorkerId>,
        route_tier: RouteTier,
        /// Peer-local elapsed times measured from the peer's reconstructed
        /// arrival. The origin preserves their differences but anchors the
        /// timeline at its actual response receipt time.
        render_started_ms: u64,
        native_render_started_ms: u64,
        native_render_completed_ms: u64,
        completed_ms: u64,
        style_swap: bool,
        cold_start: bool,
        source_loaded: bool,
        admitted_at_overflow: bool,
        #[serde(deserialize_with = "crate::types::deserialize_required_option")]
        render_observation: Option<WireRenderObservation>,
    },
    Rejected {
        reason: RejectionReason,
        #[serde(deserialize_with = "crate::types::deserialize_required_option")]
        deadline_stage: Option<DeadlineStage>,
    },
    Failed {
        error: String,
        kind: FailureKind,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireRenderObservation {
    pub render_mode: RenderMode,
    pub scale: Scale,
    pub output_format: ImageFormat,
    pub width: u16,
    pub height: u16,
    #[serde(deserialize_with = "crate::types::deserialize_required_option")]
    pub style_setup_ms: Option<u64>,
    #[serde(deserialize_with = "crate::types::deserialize_required_option")]
    pub source_setup_ms: Option<u64>,
}

impl From<RenderObservation> for WireRenderObservation {
    fn from(observation: RenderObservation) -> Self {
        Self {
            render_mode: observation.render_mode,
            scale: observation.scale,
            output_format: observation.output_format,
            width: observation.width,
            height: observation.height,
            style_setup_ms: observation.style_setup_duration.map(duration_millis),
            source_setup_ms: observation.source_setup_duration.map(duration_millis),
        }
    }
}

impl From<WireRenderObservation> for RenderObservation {
    fn from(observation: WireRenderObservation) -> Self {
        Self {
            render_mode: observation.render_mode,
            scale: observation.scale,
            output_format: observation.output_format,
            width: observation.width,
            height: observation.height,
            style_setup_duration: observation.style_setup_ms.map(Duration::from_millis),
            source_setup_duration: observation.source_setup_ms.map(Duration::from_millis),
        }
    }
}

#[derive(Debug)]
pub enum WireError {
    Encode(serde_json::Error),
    Decode(serde_json::Error),
    InvalidCompletedTimeline,
    Truncated,
    TooLarge,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(err) => write!(f, "encode forward response metadata: {err}"),
            Self::Decode(err) => write!(f, "decode forward response metadata: {err}"),
            Self::InvalidCompletedTimeline => {
                f.write_str("forward response contains an invalid completed timeline")
            }
            Self::Truncated => f.write_str("truncated forward response body"),
            Self::TooLarge => f.write_str("forward response metadata exceeds u32 length"),
        }
    }
}

impl Error for WireError {}

#[cfg(test)]
fn encode_response_body(header: &OutcomeHeader, image_bytes: &[u8]) -> Result<Vec<u8>, WireError> {
    let json = serde_json::to_vec(header).map_err(WireError::Encode)?;
    let json_len: u32 = json.len().try_into().map_err(|_| WireError::TooLarge)?;
    let mut out = Vec::with_capacity(4 + json.len() + image_bytes.len());
    out.extend_from_slice(&json_len.to_be_bytes());
    out.extend_from_slice(&json);
    out.extend_from_slice(image_bytes);
    Ok(out)
}

/// Encode only the length-prefixed metadata portion of a forward response.
/// The HTTP adapter can chain this with an existing image `Bytes` value
/// without copying the image into a second contiguous allocation.
pub fn encode_response_header(header: &OutcomeHeader) -> Result<bytes::Bytes, WireError> {
    let json = serde_json::to_vec(header).map_err(WireError::Encode)?;
    let json_len: u32 = json.len().try_into().map_err(|_| WireError::TooLarge)?;
    let mut out = Vec::with_capacity(4 + json.len());
    out.extend_from_slice(&json_len.to_be_bytes());
    out.extend_from_slice(&json);
    Ok(out.into())
}

fn decode_response_prefix(body: &[u8]) -> Result<(OutcomeHeader, usize), WireError> {
    if body.len() < 4 {
        return Err(WireError::Truncated);
    }
    let json_len = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let payload_end = 4usize.checked_add(json_len).ok_or(WireError::Truncated)?;
    if body.len() < payload_end {
        return Err(WireError::Truncated);
    }
    let header: OutcomeHeader =
        serde_json::from_slice(&body[4..payload_end]).map_err(WireError::Decode)?;
    if let OutcomeResult::Completed {
        render_started_ms,
        native_render_started_ms,
        native_render_completed_ms,
        completed_ms,
        ..
    } = &header.result
        && !(render_started_ms <= native_render_started_ms
            && native_render_started_ms <= native_render_completed_ms
            && native_render_completed_ms <= completed_ms)
    {
        return Err(WireError::InvalidCompletedTimeline);
    }
    Ok((header, payload_end))
}

#[cfg(test)]
fn decode_response_body(body: &[u8]) -> Result<(OutcomeHeader, &[u8]), WireError> {
    let (header, payload_end) = decode_response_prefix(body)?;
    Ok((header, &body[payload_end..]))
}

/// Decode an owned HTTP body while retaining the image as a zero-copy slice.
pub fn decode_response_bytes(
    body: bytes::Bytes,
) -> Result<(OutcomeHeader, bytes::Bytes), WireError> {
    let (header, payload_end) = decode_response_prefix(&body)?;
    Ok((header, body.slice(payload_end..)))
}

impl ForwardResponse {
    pub fn from_task_outcome(outcome: TaskOutcome, style_id: StyleId) -> Self {
        let (outcome, output) = OutcomeHeader::from_task_outcome(outcome, style_id);
        Self { outcome, output }
    }

    /// Convert a wire response back into the current in-process outcome.
    pub fn into_task_outcome(self, arrived_at: Instant) -> TaskOutcome {
        // The peer's offsets use its own monotonic clock and therefore only
        // describe durations within the peer. Anchor them at the time the full
        // response reached this process so end-to-end latency also includes
        // request and response transport.
        let received_at = Instant::now();
        let ForwardResponse { outcome, output } = self;
        outcome.into_task_outcome(arrived_at, received_at, output)
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
                    native_render_started_ms: millis_since(
                        info.native_render_started_at,
                        arrived_at,
                    ),
                    native_render_completed_ms: millis_since(
                        info.native_render_completed_at,
                        arrived_at,
                    ),
                    completed_ms: millis_since(info.completed_at, arrived_at),
                    style_swap: info.style_swap,
                    cold_start: info.cold_start,
                    source_loaded: info.source_loaded,
                    admitted_at_overflow: info.admitted_at_overflow,
                    render_observation: info.render_observation.map(Into::into),
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
        received_at: Instant,
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
                native_render_started_ms,
                native_render_completed_ms,
                completed_ms,
                style_swap,
                cold_start,
                source_loaded,
                admitted_at_overflow,
                render_observation,
            } => {
                let completed_at = received_at.max(arrived_at);
                let peer_duration = Duration::from_millis(completed_ms);
                let peer_arrived_at = completed_at
                    .checked_sub(peer_duration)
                    .unwrap_or(arrived_at)
                    .max(arrived_at);
                let peer_duration = completed_at.duration_since(peer_arrived_at);
                let peer_offset = |offset_ms| {
                    peer_arrived_at + Duration::from_millis(offset_ms).min(peer_duration)
                };
                let started_at = peer_offset(render_started_ms);
                let native_render_started_at = peer_offset(native_render_started_ms);
                let native_render_completed_at = peer_offset(native_render_completed_ms);
                if let Some(output) = output {
                    TaskResult::Completed {
                        info: CompletedInfo {
                            node_id,
                            worker_id,
                            route_tier,
                            started_at,
                            native_render_started_at,
                            native_render_completed_at,
                            completed_at,
                            style_swap,
                            cold_start,
                            source_loaded,
                            admitted_at_overflow,
                            render_observation: render_observation.map(Into::into),
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
    duration_millis(instant.saturating_duration_since(base))
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AddLayer, Padding, PixelRatio, Positioning, RenderRequest};

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
            authorization: Some(crate::types::RenderAuthorization {
                readable_namespaces: crate::types::NamespaceSet::try_new(vec![
                    "terrain".to_string(),
                    "basemap".to_string(),
                ])
                .unwrap(),
                cache_partition: crate::types::CredentialCachePartition::from_digest([7; 32]),
                provider_bearer_token: crate::types::ProviderBearerToken::try_new(
                    "public.wire-secret".to_string(),
                )
                .unwrap(),
            }),
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
        let mut missing_authorization = serde_json::to_value(&wire).unwrap();
        missing_authorization
            .as_object_mut()
            .unwrap()
            .remove("authorization");
        assert!(
            serde_json::from_value::<WireTask>(missing_authorization).is_err(),
            "the internal contract must not silently treat an old peer as unauthenticated"
        );
        let mut missing_partition = serde_json::to_value(&wire).unwrap();
        missing_partition["authorization"]
            .as_object_mut()
            .unwrap()
            .remove("cache_partition");
        assert!(
            serde_json::from_value::<WireTask>(missing_partition).is_err(),
            "a protected task from a peer must include its cache partition"
        );
        let mut missing_provider_token = serde_json::to_value(&wire).unwrap();
        missing_provider_token["authorization"]
            .as_object_mut()
            .unwrap()
            .remove("provider_bearer_token");
        assert!(
            serde_json::from_value::<WireTask>(missing_provider_token).is_err(),
            "a protected task from a peer must include its provider credential"
        );

        let rebuilt = wire.into_internal(now);
        assert_eq!(rebuilt.id, 42);
        assert_eq!(
            rebuilt
                .authorization
                .as_ref()
                .unwrap()
                .readable_namespaces
                .as_slice(),
            &["basemap".to_string(), "terrain".to_string()]
        );
        assert_eq!(
            rebuilt.authorization.as_ref().unwrap().cache_partition,
            crate::types::CredentialCachePartition::from_digest([7; 32])
        );
        assert_eq!(
            rebuilt
                .authorization
                .as_ref()
                .unwrap()
                .provider_bearer_token
                .as_str(),
            "public.wire-secret"
        );
        assert!(
            !format!("{rebuilt:?}").contains("wire-secret"),
            "wire credentials must stay redacted from Debug output"
        );
        assert_eq!(rebuilt.style, style());
        assert_eq!(rebuilt.pixel_ratio.to_scale(), Scale::X2);
        assert_eq!(rebuilt.output_format, ImageFormat::Webp);
    }

    #[test]
    fn nested_wire_values_require_explicit_current_fields() {
        let request = RenderRequest::StaticImage {
            positioning: Positioning::Center {
                lon: 0.0,
                lat: 0.0,
                zoom: 1.0,
                bearing: 0.0,
                pitch: 0.0,
            },
            width: 256,
            height: 256,
            overlays: Vec::new(),
            before_layer: None,
            padding: Padding::default(),
            addlayer: None,
        };
        let encoded = serde_json::to_value(request).expect("request JSON");
        for missing in ["before_layer", "padding", "addlayer"] {
            let mut incomplete = encoded.clone();
            incomplete
                .get_mut("StaticImage")
                .and_then(serde_json::Value::as_object_mut)
                .expect("static request object")
                .remove(missing);
            assert!(
                serde_json::from_value::<RenderRequest>(incomplete).is_err(),
                "missing {missing} must be rejected"
            );
        }

        let mut addlayer = serde_json::to_value(AddLayer {
            json: "{}".to_owned(),
            hash: 0,
            source: None,
        })
        .expect("addlayer JSON");
        addlayer
            .as_object_mut()
            .expect("addlayer object")
            .remove("source");
        assert!(serde_json::from_value::<AddLayer>(addlayer).is_err());
    }

    #[test]
    fn forward_wire_increments_hop_and_subtracts_hop_latency() {
        let now = Instant::now();
        let task = InternalTask {
            id: 42,
            request_id: RequestId::from_string("wire-hop-test"),
            authorization: None,
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

        // Forward reserves the round trip (2 x 125ms), not just the outbound
        // hop, so a completed remote render's response can still return before
        // the origin's deadline: 3000 - 250 = 2750.
        let wire = task.to_forward_wire(now, Duration::from_millis(125));
        assert_eq!(wire.remaining_budget_ms, 2750);
        assert_eq!(wire.forwarding_hops, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn forwarded_render_does_not_outlive_origin_deadline_at_estimated_latency() {
        // When the actual hop matches the estimate, a remote render must not
        // run past the point where its response can still reach the origin
        // before the origin's deadline — otherwise an uncancellable render
        // keeps burning cluster capacity after the origin gave up.
        let hop = Duration::from_millis(60);
        let origin_send = Instant::now();
        let origin_deadline = origin_send + Duration::from_secs(2);
        let task = InternalTask {
            id: 7,
            request_id: RequestId::from_string("forward-overshoot"),
            authorization: None,
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
            arrived_at: origin_send,
            deadline: origin_deadline,
            forwarding_hops: 0,
        };

        let wire = task.to_forward_wire(origin_send, hop);
        // Remote receives one actual hop later and rebuilds its local deadline.
        let remote_receipt = origin_send + hop;
        let remote = wire.into_internal(remote_receipt);
        // Worst case: the remote render finishes exactly at its deadline; its
        // response then needs one more hop to return.
        let response_back_at_origin = remote.deadline + hop;
        assert!(
            response_back_at_origin <= origin_deadline,
            "remote render + return hop must not exceed the origin deadline when the hop estimate holds"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn forward_response_latency_includes_transport_time() {
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
                    native_render_started_ms: 40,
                    native_render_completed_ms: 70,
                    completed_ms: 100,
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
                    render_observation: None,
                },
            },
            output: Some(RenderOutput {
                bytes: bytes::Bytes::new(),
                format: ImageFormat::Png,
            }),
        };

        tokio::time::advance(Duration::from_millis(150)).await;
        let outcome = response.into_task_outcome(now);
        let TaskResult::Completed { info, output } = outcome.result else {
            panic!("expected completed outcome");
        };
        // The peer took 100 ms from its own arrival to completion. The full
        // origin-side request took 150 ms, so the remaining 50 ms is transport
        // time. Anchor the peer offsets at receipt while preserving its stage
        // durations.
        assert_eq!(info.started_at, now + Duration::from_millis(75));
        assert_eq!(
            info.native_render_started_at,
            now + Duration::from_millis(90)
        );
        assert_eq!(
            info.native_render_completed_at,
            now + Duration::from_millis(120)
        );
        assert_eq!(info.completed_at, now + Duration::from_millis(150));
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
                    native_render_started_at: now + Duration::from_millis(2),
                    native_render_completed_at: now + Duration::from_millis(8),
                    completed_at: now + Duration::from_millis(10),
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
                    render_observation: None,
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
                native_render_started_ms: 2,
                native_render_completed_ms: 8,
                completed_ms: 11,
                style_swap: false,
                cold_start: false,
                source_loaded: false,
                admitted_at_overflow: false,
                render_observation: Some(WireRenderObservation {
                    render_mode: RenderMode::Static,
                    scale: Scale::X2,
                    output_format: ImageFormat::Png,
                    width: 640,
                    height: 360,
                    style_setup_ms: Some(42),
                    source_setup_ms: Some(7),
                }),
            },
        };
        let json = serde_json::to_string(&completed).expect("completed header JSON");
        assert!(json.contains("\"completed\""));
        let decoded: OutcomeHeader = serde_json::from_str(&json).expect("completed decodes");
        assert_eq!(decoded.completed_format(), Some(ImageFormat::Png));
        assert_eq!(decoded, completed);

        let rejected = OutcomeHeader {
            task_id: 2,
            request_id: RequestId::from_string("rejected-test"),
            style_id: style().id,
            had_source: true,
            image_format: None,
            result: OutcomeResult::Rejected {
                reason: RejectionReason::RendererDegraded,
                deadline_stage: None,
            },
        };
        let json = serde_json::to_string(&rejected).expect("rejected header JSON");
        assert!(json.contains(r#""reason":"RendererDegraded""#));
        let decoded: OutcomeHeader = serde_json::from_str(&json).expect("rejected decodes");
        assert_eq!(
            decoded.rejected_reason(),
            Some(RejectionReason::RendererDegraded)
        );
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
            "request_id": "unknown-field-test",
            "style_id": "style-1",
            "had_source": false,
            "image_format": null,
            "future_field": "ignored",
            "rejected": {
                "reason": "QueueFull",
                "deadline_stage": null,
                "future_nested_field": 42
            }
        }"#;

        let decoded: OutcomeHeader = serde_json::from_str(json).expect("unknown fields ignored");
        assert_eq!(decoded.rejected_reason(), Some(RejectionReason::QueueFull));
    }

    #[test]
    fn unknown_rejection_reason_is_rejected() {
        let json = r#"{
            "task_id": 1,
            "request_id": "unknown-rejection-test",
            "style_id": "style-1",
            "had_source": false,
            "image_format": null,
            "rejected": {
                "reason": "FutureRejection",
                "deadline_stage": null
            }
        }"#;

        assert!(serde_json::from_str::<OutcomeHeader>(json).is_err());
    }

    #[test]
    fn failed_without_kind_is_rejected() {
        let json = r#"{
            "task_id": 1,
            "request_id": "missing-kind-test",
            "style_id": "style-1",
            "had_source": false,
            "image_format": null,
            "failed": {
                "error": "renderer failed"
            }
        }"#;

        assert!(serde_json::from_str::<OutcomeHeader>(json).is_err());
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
                native_render_started_ms: 2,
                native_render_completed_ms: 8,
                completed_ms: 11,
                style_swap: false,
                cold_start: false,
                source_loaded: true,
                admitted_at_overflow: false,
                render_observation: None,
            },
        };
        let image = [1, 2, 3, 4, 5];

        let body = encode_response_body(&header, &image).expect("encode body");
        let (decoded, decoded_image) = decode_response_body(&body).expect("decode body");

        assert_eq!(decoded, header);
        assert_eq!(decoded_image, image);
    }

    #[test]
    fn owned_response_body_keeps_image_as_a_bytes_slice() {
        let header = OutcomeHeader {
            task_id: 1,
            request_id: RequestId::from_string("owned-frame-test"),
            style_id: style().id,
            had_source: false,
            image_format: None,
            result: OutcomeResult::Rejected {
                reason: RejectionReason::QueueFull,
                deadline_stage: None,
            },
        };
        let body: bytes::Bytes = encode_response_body(&header, b"image")
            .expect("encode body")
            .into();
        let (decoded, image) = decode_response_bytes(body).expect("decode owned body");

        assert_eq!(decoded, header);
        assert_eq!(image.as_ref(), b"image");
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
    fn response_body_frame_rejects_reversed_completed_timing() {
        let header = OutcomeHeader {
            task_id: 4,
            request_id: RequestId::from_string("reversed-timing-test"),
            style_id: style().id,
            had_source: false,
            image_format: Some(ImageFormat::Png),
            result: OutcomeResult::Completed {
                node_id: crate::types::NodeId::from_index(2),
                worker_id: Some(3),
                route_tier: RouteTier::Tier2HrwBl,
                render_started_ms: 1,
                native_render_started_ms: 10,
                native_render_completed_ms: 2,
                completed_ms: 12,
                style_swap: false,
                cold_start: false,
                source_loaded: false,
                admitted_at_overflow: false,
                render_observation: None,
            },
        };
        let body = encode_response_body(&header, &[1]).expect("encode body");

        assert!(matches!(
            decode_response_body(&body),
            Err(WireError::InvalidCompletedTimeline)
        ));
        assert!(matches!(
            decode_response_bytes(body.into()),
            Err(WireError::InvalidCompletedTimeline)
        ));
    }

    #[test]
    fn response_body_frame_reports_malformed_bodies() {
        for body in [&[0, 0, 0][..], &[0, 0, 0, 10, b'{']] {
            assert!(matches!(
                decode_response_body(body),
                Err(WireError::Truncated)
            ));
            assert!(matches!(
                decode_response_bytes(bytes::Bytes::copy_from_slice(body)),
                Err(WireError::Truncated)
            ));
        }

        let invalid_json = [0, 0, 0, 1, b'{'];
        assert!(matches!(
            decode_response_body(&invalid_json),
            Err(WireError::Decode(_))
        ));
        assert!(matches!(
            decode_response_bytes(bytes::Bytes::copy_from_slice(&invalid_json)),
            Err(WireError::Decode(_))
        ));
    }
}
