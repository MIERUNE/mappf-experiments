//! Domain types: `InternalTask`, `TaskOutcome`, `NodeStateView`, `Decision`,
//! style identity types (`StyleId` / `StyleRevision` / `WorkerProfile`), and
//! the per-key KV encoding shared across gossip backends.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RouteTier {
    RenderCacheHit,
    Tier1WarmTracking,
    Tier2HrwBl,
    Tier3DrainSwap,
    Tier4Overflow,
}

pub type WorkerId = u32;
pub type SourceHash = u64;
pub type TaskId = u64;

/// End-to-end request correlation ID. Public HTTP ingress accepts
/// `X-Request-Id` and generates one when it is absent; internal forward keeps
/// the same value so logs can be joined across hops without requiring OTel.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub struct RequestId(String);

impl RequestId {
    pub fn new_random() -> Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let bytes: [u8; 16] = rand::random();
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        Self(out)
    }

    pub fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new_random()
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Cluster-wide node identity. Production should feed this from the chitchat
/// node id / pod identity directly; simulator uses stable `node-{index}`
/// values. This is intentionally not compacted to `u32`, because collisions
/// would corrupt HRW routing and gossip state.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn from_index(index: usize) -> Self {
        Self(format!("node-{index}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Cluster-wide stable style identifier(version 無視)。static image URL の
/// `{username}/{style_id}` 部分。HRW input / metrics label / wire 上の
/// style identity はすべてこの型。
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub struct StyleId(pub String);

impl StyleId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Cluster-wide stable style identifier with **version**. Same ID but
/// different version = different `StyleRevision`, treated as cold for warm
/// judgment so that style definition updates trigger natural reload across
/// the cluster.
///
/// gossip wire format: `"{style_id}@{version}"` (empty string = unloaded worker).
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub struct StyleRevision {
    pub id: StyleId,
    pub version: u64,
}

impl StyleRevision {
    pub fn to_gossip_value(&self) -> String {
        format!("{}@{}", self.id.0, self.version)
    }

    /// Parse `"{key}@{version}"`. Returns `None` for malformed input.
    pub fn parse_gossip_value(s: &str) -> Option<Self> {
        let (id, v) = s.rsplit_once('@')?;
        if id.is_empty() {
            return None;
        }
        Some(StyleRevision {
            id: StyleId(id.to_string()),
            version: v.parse().ok()?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RenderMode {
    Static,
    Tile,
}

impl RenderMode {
    pub fn as_gossip_value(self) -> &'static str {
        match self {
            RenderMode::Static => "static",
            RenderMode::Tile => "tile",
        }
    }

    pub fn parse_gossip_value(value: &str) -> Option<Self> {
        match value {
            "static" => Some(RenderMode::Static),
            "tile" => Some(RenderMode::Tile),
            _ => None,
        }
    }
}

/// The `@2x` (scale) URL parameter. Wire / API boundary uses this enum
/// for type safety. Renderer boundary converts to `PixelRatio(f32)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Scale {
    X1,
    X2,
}

impl Scale {
    pub fn as_gossip_value(self) -> &'static str {
        match self {
            Scale::X1 => "1x",
            Scale::X2 => "2x",
        }
    }

    pub fn parse_gossip_value(value: &str) -> Option<Self> {
        match value {
            "1x" => Some(Scale::X1),
            "2x" => Some(Scale::X2),
            _ => None,
        }
    }
}

/// Internal newtype around maplibre's `pixel_ratio: f32`. Created from
/// `Scale` at ingress, passed to maplibre at render time as `as_f32()`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PixelRatio(f32);

impl PixelRatio {
    pub const X1: PixelRatio = PixelRatio(1.0);

    pub fn as_f32(self) -> f32 {
        self.0
    }

    pub fn to_scale(self) -> Scale {
        if self.0 >= 1.5 { Scale::X2 } else { Scale::X1 }
    }
}

impl From<Scale> for PixelRatio {
    fn from(s: Scale) -> Self {
        PixelRatio(match s {
            Scale::X1 => 1.0,
            Scale::X2 => 2.0,
        })
    }
}

/// Output image encoding format. Decided at ingress(URL extension /
/// Accept header)and carried through to the renderer actor for encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ImageFormat {
    Png,
    Webp,
    Jpeg,
}

impl ImageFormat {
    pub fn content_type(self) -> &'static str {
        match self {
            ImageFormat::Png => "image/png",
            ImageFormat::Webp => "image/webp",
            ImageFormat::Jpeg => "image/jpeg",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CachePolicy {
    /// Keep the source in the worker's LRU cache (default — shared / refresh).
    Cacheable,
    /// Use once, do not pollute the cache. For user-specific overlays etc.
    OneShot,
}

/// A reference to a source datum a task needs. The static image API
/// allows at most one `addlayer` per request, so each task carries at most
/// one additional source beyond the base style's intrinsic sources.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceRef {
    pub hash: SourceHash,
    pub policy: CachePolicy,
}

/// How a `StaticImage` request locates the rendered viewport. Mirrors the
/// three forms in the static image URL grammar
/// (`{lon},{lat},{zoom},{bearing},{pitch}` | `{bbox}` | `auto`).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Positioning {
    Center {
        lon: f64,
        lat: f64,
        zoom: f64,
        bearing: f32,
        pitch: f32,
    },
    Bbox {
        min_lon: f64,
        min_lat: f64,
        max_lon: f64,
        max_lat: f64,
    },
    /// Auto-fit the camera to the union of all overlay geometries. Requires
    /// at least one overlay with a fittable geometry; an empty overlay
    /// list is rejected at ingress.
    Auto,
}

/// Viewport padding in logical pixels for bounds-fitting positioning
/// modes (`Bbox` and `Auto`). Ignored for `Center`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Padding {
    pub top: u16,
    pub right: u16,
    pub bottom: u16,
    pub left: u16,
}

impl Padding {
    /// All-sides uniform padding.
    pub const fn all(value: u16) -> Self {
        Self {
            top: value,
            right: value,
            bottom: value,
            left: value,
        }
    }
}

/// A request-local source definition introduced by an `addlayer` source
/// object. The user-facing `source.url` value is treated as a tileset id
/// and resolved by biei before MapLibre sees the source.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AddLayerSource {
    /// The tileset id supplied in `source.url`.
    pub tileset_id: String,
    /// Pre-validated style-spec source JSON after biei policy rewriting.
    /// The renderer passes this to `AnySource::from_json_str` using a
    /// request-local source id.
    pub json: String,
}

impl AddLayerSource {
    pub fn stable_source_id(&self) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.tileset_id.hash(&mut hasher);
        self.json.hash(&mut hasher);
        format!("__biei_addlayer_source_{:016x}", hasher.finish())
    }
}

/// A request-local style layer injected via the `addlayer` URL query
/// parameter. biei carries the layer JSON as a pre-validated string so
/// the renderer can hand it directly to `AnyLayer::from_json_str` after
/// rewriting `id` and, when present, `source`.
///
/// `addlayer` is added below the overlay slot band and, when
/// `before_layer={X}` is set, sits in the same band as the overlays —
/// i.e. just below `X`. A string `source` references an existing base-style
/// source. An object `source` is resolved by biei into a request-local
/// temporary source.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AddLayer {
    /// Pre-validated style-spec layer JSON. The renderer hands this to
    /// `AnyLayer::from_json_str` after rewriting the user-supplied `id`
    /// to the biei-internal namespace.
    pub json: String,
    /// Stable hash of the user-supplied layer JSON. Used as part of the
    /// render cache key so two requests with different addlayer JSON do
    /// not collide on a single cache entry.
    pub hash: u64,
    /// Optional source definition carried by this addlayer.
    #[serde(default)]
    pub source: Option<AddLayerSource>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct LngLat {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PathOverlay {
    pub stroke_width: Option<f32>,
    pub stroke_color: Option<String>,
    pub stroke_opacity: Option<f32>,
    pub fill_color: Option<String>,
    pub fill_opacity: Option<f32>,
    pub coordinates: Vec<LngLat>,
}

/// A `geojson(...)` overlay carries an already-parsed FeatureCollection. Per-
/// feature simplestyle properties (`stroke`, `fill`, `marker-color`, etc.) are
/// read by the rendering layer via DDS expressions, so all features share the
/// same Fill / Line / Circle layer set per overlay regardless of styling
/// cardinality.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GeoJsonOverlay {
    pub feature_collection: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum PinSize {
    Small,
    Medium,
    Large,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PinOverlay {
    pub size: PinSize,
    pub label: Option<String>,
    pub color: String,
    pub coordinate: LngLat,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StaticOverlay {
    Path(PathOverlay),
    GeoJson(GeoJsonOverlay),
    Pin(PinOverlay),
}

/// The render shape a task is requesting. `Tile` is a single XYZ tile;
/// `StaticImage` is a static-image-style arbitrary viewport.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RenderRequest {
    Tile {
        z: u8,
        x: u32,
        y: u32,
        /// Edge length in CSS pixels (256 or 512 in standard MapLibre).
        tile_size: u16,
    },
    StaticImage {
        positioning: Positioning,
        width: u16,
        height: u16,
        overlays: Vec<StaticOverlay>,
        /// `before_layer={X}` URL query parameter: render all overlays just
        /// below the base-style layer named X (static-image-API-compatible). When
        /// `addlayer` is also present, the addlayer is placed in the same
        /// band, immediately below the overlays. `None` = the biei-added
        /// band sits on top of the entire base style.
        #[serde(default)]
        before_layer: Option<String>,
        /// `padding={...}` URL query parameter: viewport insets applied to
        /// bounds-fitting positioning (`Bbox` and `Auto`). Ignored for
        /// `Center`. Default is zero padding on all sides.
        #[serde(default)]
        padding: Padding,
        /// `addlayer={...}` URL query parameter: at most one request-local
        /// style layer injected by the caller. Sits below the overlay slot
        /// band; when `before_layer` is set, both addlayer and overlays go
        /// under the named base-style layer.
        #[serde(default)]
        addlayer: Option<AddLayer>,
    },
}

impl RenderRequest {
    pub fn render_mode(&self) -> RenderMode {
        match self {
            RenderRequest::Tile { .. } => RenderMode::Tile,
            RenderRequest::StaticImage { .. } => RenderMode::Static,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub struct WorkerProfile {
    pub style: StyleRevision,
    pub render_mode: RenderMode,
    pub scale: Scale,
}

/// Process-local view of a task in flight. **Not wire-safe** — holds
/// `Instant`, which is meaningless outside this process. Wire serialization
/// goes through `wire::WireTask`.
#[derive(Clone, Debug)]
pub struct InternalTask {
    pub id: TaskId,
    pub request_id: RequestId,
    /// Cluster-wide stable style identifier(ID + version). HRW routing and
    /// warm judgment use this together with request mode and scale via
    /// `worker_profile()`.
    pub style: StyleRevision,
    /// Optional addlayer source (= the one inline source the request brings
    /// beyond what the base style provides). `None` for tasks that render
    /// the base style alone.
    pub source: Option<SourceRef>,
    pub request: RenderRequest,
    /// MapLibre / maplibre-native internal name for the device-pixel
    /// multiplier (1.0 default, 2.0 for `@2x` URLs).
    pub pixel_ratio: PixelRatio,
    /// Output format decided at ingress (URL extension / Accept). The
    /// renderer encodes the final bytes in this format.
    pub output_format: ImageFormat,
    /// Wall-clock time this node received the task. Local `Instant` —
    /// SLA latency 計測の起点. Forward 受信時は新たに `Instant::now()`.
    pub arrived_at: Instant,
    /// Deadline for this task on this node (local `Instant`). Wire boundary
    /// carries `remaining_budget_ms` (clock-skew-free) instead.
    pub deadline: Instant,
    pub forwarding_hops: u8,
}

impl InternalTask {
    pub fn worker_profile(&self) -> WorkerProfile {
        WorkerProfile {
            style: self.style.clone(),
            render_mode: self.request.render_mode(),
            scale: self.pixel_ratio.to_scale(),
        }
    }
}

/// Per-node KV namespace as carried in `ClusterView` between gossip backends.
pub type NodeKvs = BTreeMap<String, String>;

/// Reconstructed worker info as the dispatcher sees it (decoded from gossip
/// KVs).
#[derive(Clone, Debug)]
pub struct WorkerView {
    pub id: WorkerId,
    /// `None` for fresh / unloaded workers. `Some(profile)` when the worker
    /// has loaded a style revision for a specific render mode and scale.
    /// Warm 判定は profile 完全一致(style revision + mode + scale)。
    pub loaded_profile: Option<WorkerProfile>,
    pub queue_depth: usize,
}

/// Dispatcher-facing view of a node, decoded from per-key gossip values.
/// Aggregates like warm profile counts and `has_capacity` are derived on demand
/// from `workers` rather than transmitted separately, so a partial gossip
/// view simply yields fewer workers.
#[derive(Clone, Debug)]
pub struct NodeStateView {
    pub id: NodeId,
    pub workers: Vec<WorkerView>,
}

impl NodeStateView {
    /// Decode a `NodeStateView` from a stream of `(key, value)` borrows.
    /// Workers missing one of the expected keys are skipped (partial gossip
    /// propagation). Taking an iterator avoids materialising an intermediate
    /// `BTreeMap` + per-key `String` clones on the chitchat read path.
    ///
    /// gossip value format:
    ///   - empty string → `loaded_profile = None` (fresh worker)
    ///   - `"{style_id}@{version}"` → `Some(StyleRevision { id, version })`
    ///   - `worker.{wid}.mode` is `static` / `tile`
    ///   - `worker.{wid}.scale` is `1x` / `2x`
    ///   - malformed → worker is skipped (treated as partial state)
    pub fn from_kvs<I, K, V>(id: NodeId, kvs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        #[derive(Default)]
        struct Builder {
            loaded_revision: Option<Option<StyleRevision>>,
            render_mode: Option<RenderMode>,
            scale: Option<Scale>,
            queue_depth: Option<usize>,
        }
        let mut by_worker: BTreeMap<WorkerId, Builder> = BTreeMap::new();
        for (key, value) in kvs {
            let key = key.as_ref();
            let value = value.as_ref();
            let Some(rest) = key.strip_prefix("worker.") else {
                continue;
            };
            let Some((wid_s, field)) = rest.split_once('.') else {
                continue;
            };
            let Ok(wid) = wid_s.parse::<WorkerId>() else {
                continue;
            };
            let entry = by_worker.entry(wid).or_default();
            match field {
                "style" => {
                    if value.is_empty() {
                        entry.loaded_revision = Some(None);
                    } else if let Some(rev) = StyleRevision::parse_gossip_value(value) {
                        entry.loaded_revision = Some(Some(rev));
                    }
                    // else: malformed revision (non-empty, unparseable) →
                    // leave entry.loaded_revision unset so the worker is
                    // skipped as partial state, matching the
                    // partial-key-missing semantics elsewhere.
                }
                "mode" if !value.is_empty() => {
                    entry.render_mode = RenderMode::parse_gossip_value(value);
                }
                "scale" if !value.is_empty() => {
                    entry.scale = Scale::parse_gossip_value(value);
                }
                "queue" => {
                    entry.queue_depth = value.parse().ok();
                }
                _ => {}
            }
        }
        let workers: Vec<WorkerView> = by_worker
            .into_iter()
            .filter_map(|(wid, b)| {
                let loaded_profile = match b.loaded_revision? {
                    None => None,
                    Some(style) => Some(WorkerProfile {
                        style,
                        render_mode: b.render_mode?,
                        scale: b.scale?,
                    }),
                };
                Some(WorkerView {
                    id: wid,
                    loaded_profile,
                    queue_depth: b.queue_depth?,
                })
            })
            .collect();
        Self { id, workers }
    }

    /// At least one worker has soft-limit headroom for SLA-oriented routing.
    pub fn has_capacity(&self, bl_capacity_per_worker: usize) -> bool {
        self.workers
            .iter()
            .any(|w| w.queue_depth < bl_capacity_per_worker)
    }

    pub fn has_admission_capacity(&self, queue_capacity_per_worker: usize) -> bool {
        self.workers
            .iter()
            .any(|w| w.queue_depth < queue_capacity_per_worker)
    }
}

/// Cluster-wide view assembled by a `GossipBus`. `members` is static; `states`
/// holds the decoded per-node view for nodes whose KVs have been propagated.
#[derive(Clone, Debug)]
pub struct ClusterView {
    pub members: Vec<NodeId>,
    pub states: HashMap<NodeId, NodeStateView>,
    pub generated_at: Instant,
}

/// Final result of handling a task — what a caller awaits.
#[derive(Debug, Clone)]
pub struct TaskOutcome {
    pub task_id: TaskId,
    pub request_id: RequestId,
    pub arrived_at: Instant,
    /// Whether the task carried an addlayer source.
    pub had_source: bool,
    /// Stage where a deadline rejection happened, when known.
    pub deadline_stage: Option<DeadlineStage>,
    pub result: TaskResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeadlineStage {
    AcquireRenderPermit,
    StyleSwap,
    EnsureSource,
    AcquireCpuPermit,
    Render,
}

#[derive(Debug, Clone)]
pub enum TaskResult {
    Completed {
        info: CompletedInfo,
        output: RenderOutput,
    },
    Rejected {
        reason: RejectionReason,
    },
    Failed {
        error: String,
        kind: FailureKind,
    },
}

/// Typed classification of a render failure, so the HTTP layer maps it to a
/// status code by variant instead of matching on the error message string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureKind {
    /// The render exceeded its deadline.
    RenderTimeout,
    /// The renderer actor died / is unavailable.
    RendererDead,
    /// The style could not be fetched/loaded from the provider.
    StyleUnavailable,
    /// The style is known but not yet warm in the renderer.
    StyleNotReady,
    /// An addlayer/source fetch failed.
    SourceUnavailable,
    /// Any other render failure.
    Other,
}

impl FailureKind {
    /// Classifies a [`RendererError`] into a transport-stable [`FailureKind`].
    pub fn from_renderer_error(error: &RendererError) -> Self {
        match error {
            RendererError::Timeout => Self::RenderTimeout,
            RendererError::ActorDead => Self::RendererDead,
            RendererError::StyleLoadFailed { .. } => Self::StyleUnavailable,
            RendererError::StyleNotReady { .. } => Self::StyleNotReady,
            RendererError::SourceFetchFailed { .. } => Self::SourceUnavailable,
            RendererError::RenderFailed(_) => Self::Other,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompletedInfo {
    pub node_id: NodeId,
    /// Worker that produced the output. Cache hits bypass worker execution,
    /// so this is absent for `RouteTier::RenderCacheHit`.
    pub worker_id: Option<WorkerId>,
    pub route_tier: RouteTier,
    pub started_at: Instant,
    /// Time spent holding the CPU/GPU-heavy render-stage permit.
    pub cpu_started_at: Instant,
    pub cpu_completed_at: Instant,
    pub completed_at: Instant,
    pub style_swap: bool,
    pub cold_start: bool,
    /// True if the task's addlayer source was a cache miss and had to be
    /// loaded. False if it hit the cache or the task had no source.
    pub source_loaded: bool,
    /// True if the task was admitted when the chosen worker's queue had
    /// already reached the soft queue limit (BL). Indicates the pool is
    /// leaning on the overflow band between soft and hard limits rather than
    /// refusing the request.
    pub admitted_at_overflow: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RendererError {
    StyleLoadFailed { style_id: StyleId, source: String },
    StyleNotReady { style_id: StyleId, version: u64 },
    SourceFetchFailed { hash: SourceHash, source: String },
    RenderFailed(String),
    Timeout,
    ActorDead,
}

impl std::fmt::Display for RendererError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RendererError::StyleLoadFailed { style_id, source } => {
                write!(f, "style load failed for {}: {}", style_id.as_str(), source)
            }
            RendererError::StyleNotReady { style_id, version } => {
                write!(f, "style not ready for {}@{}", style_id.as_str(), version)
            }
            RendererError::SourceFetchFailed { hash, source } => {
                write!(f, "source fetch failed for {hash}: {source}")
            }
            RendererError::RenderFailed(source) => write!(f, "render failed: {source}"),
            RendererError::Timeout => write!(f, "render timeout"),
            RendererError::ActorDead => write!(f, "renderer actor dead"),
        }
    }
}

impl std::error::Error for RendererError {}

/// Rendered image bytes. Simulator responses use an empty byte vector; real
/// production renderers fill this with encoded PNG/WebP bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderOutput {
    pub bytes: Bytes,
    pub format: ImageFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RejectionReason {
    QueueFull,
    NoCapacity,
    DrainTooSlow,
    UnknownStyle,
    HopLimitExceeded,
    ForwardFailed,
    DeadlineTooClose,
    DeadlineExceeded,
}

#[derive(Debug)]
pub enum ProcessError {
    // Boxed: `InternalTask` grew with overlays / `before_layer` and dwarfs the
    // unit-sized `QueueDisconnected` variant — `Box` keeps the enum's stack
    // size proportional to the smaller variant, which matters because this
    // type is returned by value from every pool dispatch.
    QueueFull(Box<InternalTask>),
    QueueDisconnected,
}

impl RejectionReason {
    pub fn is_retryable_at_forward(self) -> bool {
        match self {
            RejectionReason::QueueFull
            | RejectionReason::NoCapacity
            | RejectionReason::DrainTooSlow => true,
            RejectionReason::UnknownStyle
            | RejectionReason::HopLimitExceeded
            | RejectionReason::ForwardFailed
            | RejectionReason::DeadlineTooClose
            | RejectionReason::DeadlineExceeded => false,
        }
    }
}

#[derive(Debug)]
pub enum Decision {
    Local {
        route_tier: RouteTier,
        worker_hint: Option<WorkerId>,
    },
    Forward {
        route_tier: RouteTier,
        candidates: Vec<ForwardCandidate>,
    },
    Reject {
        reason: RejectionReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardCandidate {
    pub node_id: NodeId,
    pub drain_worker: Option<WorkerId>,
}

// ---------------------------------------------------------------------------
// Encoder helpers shared between gossip backends.
// ---------------------------------------------------------------------------

/// Encode a single worker's (loaded_profile, queue) into KV pairs.
/// `loaded_profile = None` (fresh worker) is emitted as empty strings;
/// `Some(profile)` emits style revision, render mode, and scale separately.
pub fn encode_worker_kvs(
    out: &mut NodeKvs,
    worker_id: WorkerId,
    loaded_profile: Option<&WorkerProfile>,
    queue_depth: usize,
) {
    let (style_value, mode_value, scale_value) = match loaded_profile {
        Some(profile) => (
            profile.style.to_gossip_value(),
            profile.render_mode.as_gossip_value().to_string(),
            profile.scale.as_gossip_value().to_string(),
        ),
        None => (String::new(), String::new(), String::new()),
    };
    out.insert(format!("worker.{}.style", worker_id), style_value);
    out.insert(format!("worker.{}.mode", worker_id), mode_value);
    out.insert(format!("worker.{}.scale", worker_id), scale_value);
    out.insert(
        format!("worker.{}.queue", worker_id),
        queue_depth.to_string(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kvs<const N: usize>(pairs: [(&str, &str); N]) -> NodeKvs {
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn rev(key: &str, v: u64) -> StyleRevision {
        StyleRevision {
            id: StyleId(key.to_string()),
            version: v,
        }
    }

    fn profile(key: &str, v: u64, render_mode: RenderMode, scale: Scale) -> WorkerProfile {
        WorkerProfile {
            style: rev(key, v),
            render_mode,
            scale,
        }
    }

    #[test]
    fn from_kvs_includes_workers_with_complete_profile_keys() {
        let m = kvs([
            ("worker.0.style", "style-3@0"),
            ("worker.0.mode", "tile"),
            ("worker.0.scale", "2x"),
            ("worker.0.queue", "2"),
        ]);
        let view = NodeStateView::from_kvs(NodeId::from_index(7), &m);
        assert_eq!(view.id, NodeId::from_index(7));
        assert_eq!(view.workers.len(), 1);
        assert_eq!(view.workers[0].id, 0);
        assert_eq!(
            view.workers[0].loaded_profile,
            Some(profile("style-3", 0, RenderMode::Tile, Scale::X2))
        );
        assert_eq!(view.workers[0].queue_depth, 2);
    }

    #[test]
    fn from_kvs_treats_empty_style_value_as_fresh_worker() {
        // Worker has reported state but no current style — the worker IS
        // visible, just with loaded_revision=None.
        let m = kvs([("worker.0.style", ""), ("worker.0.queue", "0")]);
        let view = NodeStateView::from_kvs(NodeId::from_index(0), &m);
        assert_eq!(view.workers.len(), 1);
        assert_eq!(view.workers[0].loaded_profile, None);
        assert_eq!(view.workers[0].queue_depth, 0);
    }

    #[test]
    fn from_kvs_skips_workers_missing_the_style_field() {
        let m = kvs([("worker.0.queue", "5")]);
        let view = NodeStateView::from_kvs(NodeId::from_index(0), &m);
        assert!(view.workers.is_empty());
    }

    #[test]
    fn from_kvs_skips_workers_missing_the_queue_key() {
        let m = kvs([("worker.0.style", "style-1@0")]);
        let view = NodeStateView::from_kvs(NodeId::from_index(0), &m);
        assert!(view.workers.is_empty());
    }

    #[test]
    fn from_kvs_decodes_many_workers_independently() {
        let m = kvs([
            ("worker.0.style", "style-1@0"),
            ("worker.0.mode", "static"),
            ("worker.0.scale", "2x"),
            ("worker.0.queue", "0"),
            ("worker.1.style", ""),
            ("worker.1.queue", "3"),
            ("worker.2.queue", "7"),
            ("misc.something", "ignored"),
        ]);
        let view = NodeStateView::from_kvs(NodeId::from_index(0), &m);
        assert_eq!(view.workers.len(), 2);
        assert_eq!(view.workers[0].id, 0);
        assert_eq!(
            view.workers[0].loaded_profile,
            Some(profile("style-1", 0, RenderMode::Static, Scale::X2))
        );
        assert_eq!(view.workers[1].id, 1);
        assert_eq!(view.workers[1].loaded_profile, None);
        assert_eq!(view.workers[1].queue_depth, 3);
    }

    #[test]
    fn from_kvs_skips_malformed_revision() {
        // value is non-empty but doesn't match "{key}@{version}".
        let m = kvs([("worker.0.style", "garbage"), ("worker.0.queue", "0")]);
        let view = NodeStateView::from_kvs(NodeId::from_index(0), &m);
        assert!(view.workers.is_empty());
    }

    #[test]
    fn from_kvs_skips_loaded_workers_missing_profile_fields() {
        let m = kvs([("worker.0.style", "style-1@0"), ("worker.0.queue", "0")]);
        let view = NodeStateView::from_kvs(NodeId::from_index(0), &m);
        assert!(view.workers.is_empty());
    }

    #[test]
    fn revision_handles_keys_containing_at_sign() {
        // `rsplit_once('@')` splits at the last '@', so keys with '@'
        // in them (e.g. namespaced "alice@org/streets") round-trip.
        let r = rev("alice@org/streets", 7);
        assert_eq!(r.to_gossip_value(), "alice@org/streets@7");
        let parsed = StyleRevision::parse_gossip_value("alice@org/streets@7");
        assert_eq!(parsed, Some(r));
    }

    #[test]
    fn has_capacity_against_bl_limit() {
        let m = kvs([
            ("worker.0.style", "style-1@0"),
            ("worker.0.mode", "tile"),
            ("worker.0.scale", "2x"),
            ("worker.0.queue", "2"),
            ("worker.1.style", "style-1@0"),
            ("worker.1.mode", "tile"),
            ("worker.1.scale", "2x"),
            ("worker.1.queue", "2"),
        ]);
        let view = NodeStateView::from_kvs(NodeId::from_index(0), &m);
        assert!(!view.has_capacity(2));
        assert!(view.has_capacity(3));
    }

    #[test]
    fn encode_then_decode_roundtrips() {
        let mut m = NodeKvs::new();
        let p = profile("style-42", 1, RenderMode::Static, Scale::X2);
        encode_worker_kvs(&mut m, 0, Some(&p), 3);
        encode_worker_kvs(&mut m, 1, None, 0);
        let view = NodeStateView::from_kvs(NodeId::from_index(0), &m);
        assert_eq!(view.workers.len(), 2);
        assert_eq!(view.workers[0].loaded_profile, Some(p));
        assert_eq!(view.workers[0].queue_depth, 3);
        assert_eq!(view.workers[1].loaded_profile, None);
        assert_eq!(view.workers[1].queue_depth, 0);
    }

    #[test]
    fn pixel_ratio_scale_roundtrip() {
        assert_eq!(PixelRatio::from(Scale::X1).as_f32(), 1.0);
        assert_eq!(PixelRatio::from(Scale::X2).as_f32(), 2.0);
        assert_eq!(PixelRatio::from(Scale::X1).to_scale(), Scale::X1);
        assert_eq!(PixelRatio::from(Scale::X2).to_scale(), Scale::X2);
    }
}
