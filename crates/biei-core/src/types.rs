//! Domain types: `InternalTask`, `TaskOutcome`, `NodeStateView`, `Decision`,
//! style identity types (`StyleId` / `StyleRevision` / `WorkerProfile`), and
//! the per-key KV encoding shared across gossip backends.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
pub use mmpf_http::request_id::RequestId;
use serde::{Deserialize, Deserializer, Serialize};
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

const MAX_AUTHORIZATION_NAMESPACES: usize = 1024;
const MAX_AUTHORIZATION_NAMESPACE_BYTES: usize = 256;
const MAX_PROVIDER_BEARER_TOKEN_BYTES: usize = 4096;

/// Opaque, one-way partition for caches whose bytes may contain a delivery
/// credential (for example, an Ishikari-rewritten style URL). The verifier
/// binds it to the policy revision. This is safe to carry on the trusted render
/// wire; it is not the credential, registry verifier digest, or principal.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CredentialCachePartition([u8; 32]);

impl CredentialCachePartition {
    pub fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }
}

impl std::fmt::Debug for CredentialCachePartition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CredentialCachePartition([redacted])")
    }
}

/// The verified caller credential forwarded to the configured delivery
/// provider for a protected render.
///
/// This value is deliberately separate from cache identity and has a redacted
/// `Debug` implementation. It may cross only Biei's trusted peer transport and
/// must be attached only to the explicitly configured provider origin.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct ProviderBearerToken(String);

impl ProviderBearerToken {
    pub fn try_new(value: String) -> Result<Self, &'static str> {
        if value.is_empty()
            || value.len() > MAX_PROVIDER_BEARER_TOKEN_BYTES
            || value
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        {
            return Err(
                "provider bearer token must be non-empty, bounded, and contain no whitespace",
            );
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ProviderBearerToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProviderBearerToken([redacted])")
    }
}

impl<'de> Deserialize<'de> for ProviderBearerToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// A bounded, normalized set of delivery namespaces.
///
/// Biei carries this authorization result across its trusted internal render
/// transport so every node can apply the same output-cache admission rule
/// without forwarding registry state or re-reading object storage. It contains
/// no credential or principal identifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceSet(Arc<[String]>);

impl NamespaceSet {
    pub fn try_new(mut namespaces: Vec<String>) -> Result<Self, &'static str> {
        if namespaces.is_empty() || namespaces.len() > MAX_AUTHORIZATION_NAMESPACES {
            return Err("namespace set must be non-empty and bounded");
        }
        if namespaces.iter().any(|namespace| {
            namespace.is_empty()
                || namespace.len() > MAX_AUTHORIZATION_NAMESPACE_BYTES
                || namespace.chars().any(char::is_control)
        }) {
            return Err("namespace must be non-empty, bounded, and contain no control characters");
        }
        namespaces.sort_unstable();
        namespaces.dedup();
        if namespaces
            .binary_search_by(|value| value.as_str().cmp("*"))
            .is_ok()
        {
            namespaces.clear();
            namespaces.push("*".to_string());
        }
        Ok(Self(namespaces.into()))
    }

    /// Accepts already normalized shared storage from a verifier boundary.
    /// Validation is repeated because this type is also part of the internal
    /// wire contract, but no labels or backing allocation are cloned.
    pub fn try_from_shared(namespaces: Arc<[String]>) -> Result<Self, &'static str> {
        if namespaces.is_empty() || namespaces.len() > MAX_AUTHORIZATION_NAMESPACES {
            return Err("namespace set must be non-empty and bounded");
        }
        if namespaces.iter().any(|namespace| {
            namespace.is_empty()
                || namespace.len() > MAX_AUTHORIZATION_NAMESPACE_BYTES
                || namespace.chars().any(char::is_control)
        }) {
            return Err("namespace must be non-empty, bounded, and contain no control characters");
        }
        if namespaces
            .windows(2)
            .any(|pair| pair[0].as_str() >= pair[1].as_str())
            || (namespaces.iter().any(|namespace| namespace == "*")
                && !(namespaces.len() == 1 && namespaces[0] == "*"))
        {
            return Err("shared namespace set must be sorted, unique, and canonical");
        }
        Ok(Self(namespaces))
    }

    pub fn as_slice(&self) -> &[String] {
        self.0.as_ref()
    }

    /// Returns true when this caller grant satisfies every required namespace.
    pub fn allows(&self, required: &Self) -> bool {
        if self.0.first().is_some_and(|namespace| namespace == "*") {
            return true;
        }
        required
            .0
            .iter()
            .all(|namespace| namespace != "*" && self.0.binary_search(namespace).is_ok())
    }

    pub(crate) fn estimated_size_bytes(&self) -> usize {
        self.0
            .iter()
            .map(|namespace| std::mem::size_of::<String>() + namespace.len())
            .sum()
    }
}

impl<'de> Deserialize<'de> for NamespaceSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let namespaces = Vec::<String>::deserialize(deserializer)?;
        Self::try_new(namespaces).map_err(serde::de::Error::custom)
    }
}

impl Serialize for NamespaceSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderAuthorization {
    pub readable_namespaces: NamespaceSet,
    pub cache_partition: CredentialCachePartition,
    pub provider_bearer_token: ProviderBearerToken,
}

/// Deserialize an optional wire value while still requiring the field itself
/// to be present. Serde otherwise treats a missing `Option<T>` as `None`, which
/// would silently accept an older internal contract that never sent the field.
pub(crate) fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
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

    /// The authorization namespace: the first `/`-separated segment.
    ///
    /// Biei style ids are arbitrary-depth (`{namespace}/…/{id}`), so the coarsest
    /// scope a future authorizer can grant is the leading segment; finer scopes
    /// are longer prefixes of [`as_str`](Self::as_str). Returns the whole id when
    /// it has no `/`. See `specs/auth-sketch.md` §8.3.
    pub fn namespace(&self) -> &str {
        self.0.split_once('/').map_or(self.0.as_str(), |(ns, _)| ns)
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
    /// Stable hash of the user-supplied layer JSON. Used for lightweight
    /// affinity/debug identity only; correctness-sensitive render and source
    /// caches compare the bounded canonical content and do not trust this
    /// 64-bit value to be collision-free.
    pub hash: u64,
    /// Optional source definition carried by this addlayer.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub source: Option<AddLayerSource>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct LngLat {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PathOverlay {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub stroke_width: Option<f32>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub stroke_color: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub stroke_opacity: Option<f32>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub fill_color: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
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
    #[serde(deserialize_with = "deserialize_required_option")]
    pub label: Option<String>,
    pub color: String,
    pub coordinate: LngLat,
}

/// Maximum number of static overlays accepted by ingress and represented by
/// the renderer's persistent overlay slot pool.
pub const MAX_STATIC_OVERLAYS: usize = 64;

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
        #[serde(deserialize_with = "deserialize_required_option")]
        before_layer: Option<String>,
        /// `padding={...}` URL query parameter: viewport insets applied to
        /// bounds-fitting positioning (`Bbox` and `Auto`). Ignored for
        /// `Center`. Default is zero padding on all sides.
        padding: Padding,
        /// `addlayer={...}` URL query parameter: at most one request-local
        /// style layer injected by the caller. Sits below the overlay slot
        /// band; when `before_layer` is set, both addlayer and overlays go
        /// under the named base-style layer.
        #[serde(deserialize_with = "deserialize_required_option")]
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

    pub fn has_addlayer_source(&self) -> bool {
        matches!(
            self,
            RenderRequest::StaticImage {
                addlayer: Some(AddLayer {
                    source: Some(_),
                    ..
                }),
                ..
            }
        )
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub struct WorkerProfile {
    pub style: StyleRevision,
    pub render_mode: RenderMode,
    pub scale: Scale,
}

/// Runtime-independent task fields produced by an ingress adapter or workload
/// generator. Starting the task applies the shared SLA/deadline policy.
#[derive(Clone, Debug)]
pub struct TaskSpec {
    pub id: TaskId,
    pub request_id: RequestId,
    pub style: StyleRevision,
    pub source: Option<SourceRef>,
    pub request: RenderRequest,
    pub pixel_ratio: PixelRatio,
    pub output_format: ImageFormat,
}

impl TaskSpec {
    pub fn start(self, arrived_at: Instant, budget: std::time::Duration) -> InternalTask {
        InternalTask {
            id: self.id,
            request_id: self.request_id,
            authorization: None,
            style: self.style,
            source: self.source,
            request: self.request,
            pixel_ratio: self.pixel_ratio,
            output_format: self.output_format,
            arrived_at,
            deadline: arrived_at + budget,
            forwarding_hops: 0,
        }
    }
}

/// Process-local view of a task in flight. **Not wire-safe** — holds
/// `Instant`, which is meaningless outside this process. Wire serialization
/// goes through `wire::WireTask`.
#[derive(Clone, Debug)]
pub struct InternalTask {
    pub id: TaskId,
    pub request_id: RequestId,
    /// Freshly verified caller grants and the redacted provider credential for
    /// protected delivery, or `None` when delivery auth is explicitly disabled
    /// for this route. The credential may cross only the trusted render wire;
    /// it never enters cache identity, gossip, outcomes, or logs.
    pub authorization: Option<RenderAuthorization>,
    /// Cluster-wide stable style identifier(ID + version). HRW routing and
    /// warm judgment use this together with request mode and scale via
    /// `worker_profile()`.
    pub style: StyleRevision,
    /// Optional modeled source reference used by the shared worker/simulator
    /// path. Production addlayer sources live in `request` and are resolved by
    /// the profile preparer before rendering.
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

    pub fn has_source(&self) -> bool {
        self.source.is_some() || self.request.has_addlayer_source()
    }
}

/// Per-node KV namespace as carried in `ClusterView` between gossip backends.
pub type NodeKvs = BTreeMap<String, String>;

/// Node-level admission state published alongside per-worker gossip.
///
/// The key is required for render routing. Missing, malformed, and explicit
/// `false` values all fail closed. A non-accepting node may still serve exact
/// output-cache hits, but must not be selected for new native renders.
pub const RENDER_ADMISSION_GOSSIP_KEY: &str = "renderer.accepting";

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
    pub accepts_new_renders: bool,
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
        // Admission state is required. A partial gossip snapshot becomes
        // routable only after the explicit current value arrives.
        let mut accepts_new_renders = false;
        for (key, value) in kvs {
            let key = key.as_ref();
            let value = value.as_ref();
            if key == RENDER_ADMISSION_GOSSIP_KEY {
                accepts_new_renders = value == "true";
                continue;
            }
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
        Self {
            id,
            accepts_new_renders,
            workers,
        }
    }

    /// At least one worker has soft-limit headroom for SLA-oriented routing.
    pub fn has_capacity(&self, bl_capacity_per_worker: usize) -> bool {
        self.accepts_new_renders
            && self
                .workers
                .iter()
                .any(|w| w.queue_depth < bl_capacity_per_worker)
    }

    pub fn has_admission_capacity(&self, queue_capacity_per_worker: usize) -> bool {
        self.accepts_new_renders
            && self
                .workers
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
    /// Whether the task carried either a modeled source or an addlayer source.
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
    AcquireNativeRenderPermit,
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
///
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
    /// The caller deadline elapsed during profile *preparation* (style/source
    /// fetch), before any native render started. Kept distinct from
    /// `RenderTimeout` so it never feeds the render-timeout censoring histogram
    /// or is mistaken for a native-render timeout in calibration.
    PreparationTimeout,
    /// Any other render failure.
    Other,
}

impl FailureKind {
    /// Every variant, for zero-initializing bounded metric series.
    pub const ALL: [FailureKind; 7] = [
        FailureKind::RenderTimeout,
        FailureKind::RendererDead,
        FailureKind::StyleUnavailable,
        FailureKind::StyleNotReady,
        FailureKind::SourceUnavailable,
        FailureKind::PreparationTimeout,
        FailureKind::Other,
    ];

    /// Stable, bounded-cardinality metric label for this failure kind.
    pub fn as_label(self) -> &'static str {
        match self {
            FailureKind::RenderTimeout => "render_timeout",
            FailureKind::RendererDead => "renderer_dead",
            FailureKind::StyleUnavailable => "style_unavailable",
            FailureKind::StyleNotReady => "style_not_ready",
            FailureKind::SourceUnavailable => "source_unavailable",
            FailureKind::PreparationTimeout => "preparation_timeout",
            FailureKind::Other => "other",
        }
    }

    /// Classifies a [`RendererError`] into a transport-stable [`FailureKind`].
    pub fn from_renderer_error(error: &RendererError) -> Self {
        match error {
            RendererError::Timeout => Self::RenderTimeout,
            RendererError::ActorDead => Self::RendererDead,
            RendererError::StyleLoadFailed { .. } => Self::StyleUnavailable,
            // Setup failures are renderer-local, not a bad style document, so
            // they classify as a generic failure and never negative-cache the
            // revision (see `Node::process_local_task`).
            RendererError::SetupFailed { .. } => Self::Other,
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
    pub native_render_started_at: Instant,
    pub native_render_completed_at: Instant,
    pub completed_at: Instant,
    pub style_swap: bool,
    pub cold_start: bool,
    /// True if a modeled or addlayer source was a cache miss and had to be
    /// loaded. False if it hit the cache or the task had no source.
    pub source_loaded: bool,
    /// True if the task was admitted when the chosen worker's queue had
    /// already reached the soft queue limit (BL). Indicates the pool is
    /// leaning on the overflow band between soft and hard limits rather than
    /// refusing the request.
    pub admitted_at_overflow: bool,
    /// Bounded-cardinality render metadata and stage durations used to
    /// calibrate the simulator from production metrics. Cache hits have no
    /// render observation because they bypass the worker entirely.
    pub render_observation: Option<RenderObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderObservation {
    pub render_mode: RenderMode,
    pub scale: Scale,
    pub output_format: ImageFormat,
    /// Logical output dimensions. `scale` carries the device-pixel multiplier.
    pub width: u16,
    pub height: u16,
    /// Present only when this request changed the worker profile.
    pub style_setup_duration: Option<Duration>,
    /// Present only when a modeled or addlayer source cache miss required
    /// setup.
    pub source_setup_duration: Option<Duration>,
}

impl RenderObservation {
    pub fn from_task(
        task: &InternalTask,
        style_setup_duration: Option<Duration>,
        source_setup_duration: Option<Duration>,
    ) -> Self {
        let (width, height) = match &task.request {
            RenderRequest::Tile { tile_size, .. } => (*tile_size, *tile_size),
            RenderRequest::StaticImage { width, height, .. } => (*width, *height),
        };
        Self {
            render_mode: task.request.render_mode(),
            scale: task.pixel_ratio.to_scale(),
            output_format: task.output_format,
            width,
            height,
            style_setup_duration,
            source_setup_duration,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileContent {
    Style(StyleId),
    Source(SourceHash),
}

/// Failure while fetching or validating data before a task enters a renderer
/// slot. This remains local to the preparation boundary and is converted to the
/// existing wire-stable `FailureKind` or deadline rejection by `Node`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfilePreparationError {
    StyleUnavailable {
        style_id: StyleId,
        source: String,
    },
    SourceUnavailable {
        hash: SourceHash,
        source: String,
    },
    CallerDeadlineExceeded,
    InfrastructureFailure {
        source: String,
    },
    InvalidPreparedContent {
        content: ProfileContent,
        source: String,
    },
}

impl ProfilePreparationError {
    pub fn style_unavailable(style_id: &StyleId, source: impl Into<String>) -> Self {
        Self::StyleUnavailable {
            style_id: style_id.clone(),
            source: source.into(),
        }
    }

    pub fn invalid_style(style_id: &StyleId, source: impl Into<String>) -> Self {
        Self::InvalidPreparedContent {
            content: ProfileContent::Style(style_id.clone()),
            source: source.into(),
        }
    }

    pub fn infrastructure(source: impl Into<String>) -> Self {
        Self::InfrastructureFailure {
            source: source.into(),
        }
    }

    /// Re-label style-context errors produced by shared JSON helpers at the
    /// addlayer boundary, while preserving caller deadlines and infrastructure
    /// failures unchanged.
    pub fn into_source(self, hash: SourceHash) -> Self {
        match self {
            Self::StyleUnavailable { source, .. } => Self::SourceUnavailable { hash, source },
            Self::InvalidPreparedContent { source, .. } => Self::InvalidPreparedContent {
                content: ProfileContent::Source(hash),
                source,
            },
            other => other,
        }
    }

    pub fn failure_kind(&self) -> FailureKind {
        match self {
            Self::StyleUnavailable { .. } => FailureKind::StyleUnavailable,
            Self::SourceUnavailable { .. } => FailureKind::SourceUnavailable,
            Self::CallerDeadlineExceeded => FailureKind::PreparationTimeout,
            Self::InfrastructureFailure { .. } => FailureKind::Other,
            Self::InvalidPreparedContent { content, .. } => match content {
                ProfileContent::Style(_) => FailureKind::StyleUnavailable,
                ProfileContent::Source(_) => FailureKind::SourceUnavailable,
            },
        }
    }
}

impl std::fmt::Display for ProfilePreparationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StyleUnavailable { style_id, source }
            | Self::InvalidPreparedContent {
                content: ProfileContent::Style(style_id),
                source,
            } => write!(f, "style load failed for {}: {source}", style_id.as_str()),
            Self::SourceUnavailable { hash, source }
            | Self::InvalidPreparedContent {
                content: ProfileContent::Source(hash),
                source,
            } => write!(f, "source fetch failed for {hash}: {source}"),
            Self::CallerDeadlineExceeded => write!(f, "profile preparation deadline exceeded"),
            Self::InfrastructureFailure { source } => {
                write!(f, "profile preparation infrastructure failure: {source}")
            }
        }
    }
}

impl std::error::Error for ProfilePreparationError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RendererError {
    StyleLoadFailed {
        style_id: StyleId,
        source: String,
    },
    /// Renderer-state setup failed *after* a valid style document loaded — e.g.
    /// overlay-slot installation collided, or a scale/slot-specific build step
    /// failed. This is profile/renderer-local, NOT a property of the style
    /// document, so it must not negative-cache the whole `StyleRevision` (a
    /// static-only slot failure must not poison tile rendering for that style).
    /// It still invalidates the worker's loaded state so the next render rebuilds.
    SetupFailed {
        style_id: StyleId,
        source: String,
    },
    StyleNotReady {
        style_id: StyleId,
        version: u64,
    },
    SourceFetchFailed {
        hash: SourceHash,
        source: String,
    },
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
            RendererError::SetupFailed { style_id, source } => {
                write!(
                    f,
                    "renderer setup failed for {}: {}",
                    style_id.as_str(),
                    source
                )
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
/// production renderers fill this with encoded PNG/WebP/JPEG bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderOutput {
    pub bytes: Bytes,
    pub format: ImageFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RejectionReason {
    QueueFull,
    NoCapacity,
    /// A render was shed because local renderer admission was closed.
    RendererDegraded,
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
    /// Node health changed after cache/dispatch/profile preparation but before
    /// worker admission. Preserve the task for normal peer failover.
    RenderAdmissionClosed(Box<InternalTask>),
    QueueDisconnected,
}

impl RejectionReason {
    pub(crate) fn is_retryable_at_forward(self) -> bool {
        match self {
            RejectionReason::QueueFull
            | RejectionReason::NoCapacity
            | RejectionReason::RendererDegraded
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
        /// Remaining HRW candidates to try if local admission races with a
        /// stale cluster view and the selected worker queue is unavailable.
        fallback_candidates: Vec<ForwardCandidate>,
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

    #[test]
    fn setup_failure_does_not_classify_as_style_unavailable() {
        let style_id = StyleId("carto/voyager".to_string());
        // A document-load failure is `StyleUnavailable` — it negative-caches the
        // revision (see `Node::process_local_task`).
        assert_eq!(
            FailureKind::from_renderer_error(&RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: "bad style json".to_string(),
            }),
            FailureKind::StyleUnavailable,
        );
        // A renderer-setup failure (e.g. overlay-slot collision) is renderer-local
        // and must NOT be `StyleUnavailable`, so it cannot poison the revision for
        // tile or other-profile rendering.
        assert_eq!(
            FailureKind::from_renderer_error(&RendererError::SetupFailed {
                style_id,
                source: "overlay slot collision".to_string(),
            }),
            FailureKind::Other,
        );
    }

    #[test]
    fn style_id_namespace_is_the_leading_segment() {
        assert_eq!(
            StyleId("analysis/hrnowc/sample".to_string()).namespace(),
            "analysis"
        );
        assert_eq!(
            StyleId("carto/gl/voyager-gl-style".to_string()).namespace(),
            "carto"
        );
        // A flat id (no `/`) is its own namespace.
        assert_eq!(StyleId("voyager".to_string()).namespace(), "voyager");
    }

    #[test]
    fn namespace_sets_normalize_and_apply_subset_authorization() {
        let broad = NamespaceSet::try_new(vec![
            "terrain".to_string(),
            "basemap".to_string(),
            "basemap".to_string(),
        ])
        .unwrap();
        let basemap = NamespaceSet::try_new(vec!["basemap".to_string()]).unwrap();
        let labels = NamespaceSet::try_new(vec!["labels".to_string()]).unwrap();
        let wildcard = NamespaceSet::try_new(vec!["*".to_string()]).unwrap();

        assert_eq!(
            broad.as_slice(),
            &["basemap".to_string(), "terrain".to_string()]
        );
        assert!(broad.allows(&basemap));
        assert!(!basemap.allows(&broad));
        assert!(!broad.allows(&labels));
        assert!(wildcard.allows(&broad));
        assert!(!broad.allows(&wildcard));

        let shared: Arc<[String]> = vec!["basemap".to_string(), "terrain".to_string()].into();
        let shared_set = NamespaceSet::try_from_shared(Arc::clone(&shared)).unwrap();
        assert!(Arc::ptr_eq(&shared, &shared_set.0));
    }

    #[test]
    fn namespace_set_wire_shape_rejects_missing_empty_and_oversized_values() {
        assert!(serde_json::from_str::<NamespaceSet>(r#"[]"#).is_err());
        assert!(serde_json::from_str::<NamespaceSet>(r#"[""]"#).is_err());
        let too_many = serde_json::to_string(&vec!["a"; MAX_AUTHORIZATION_NAMESPACES + 1]).unwrap();
        assert!(serde_json::from_str::<NamespaceSet>(&too_many).is_err());
    }

    #[test]
    fn credential_cache_partition_debug_is_redacted() {
        let partition = CredentialCachePartition::from_digest([7; 32]);
        let debug = format!("{partition:?}");
        assert_eq!(debug, "CredentialCachePartition([redacted])");
        assert!(!debug.contains('7'));
    }

    #[test]
    fn provider_bearer_token_is_bounded_and_debug_redacted() {
        let token = ProviderBearerToken::try_new("public.secret-value".to_string()).unwrap();
        assert_eq!(token.as_str(), "public.secret-value");
        assert_eq!(format!("{token:?}"), "ProviderBearerToken([redacted])");
        assert!(ProviderBearerToken::try_new(String::new()).is_err());
        assert!(ProviderBearerToken::try_new("public.bad token".to_string()).is_err());
        assert!(
            ProviderBearerToken::try_new("x".repeat(MAX_PROVIDER_BEARER_TOKEN_BYTES + 1)).is_err()
        );
        assert!(serde_json::from_str::<ProviderBearerToken>(r#""public.bad\ntoken""#).is_err());
    }

    #[test]
    fn preparation_errors_have_bounded_terminal_classifications() {
        let style_id = StyleId("style".to_string());
        assert_eq!(
            ProfilePreparationError::CallerDeadlineExceeded.failure_kind(),
            FailureKind::PreparationTimeout,
        );
        assert_eq!(
            ProfilePreparationError::style_unavailable(&style_id, "provider 503").failure_kind(),
            FailureKind::StyleUnavailable,
        );
        assert_eq!(
            ProfilePreparationError::invalid_style(&style_id, "invalid JSON")
                .into_source(1)
                .failure_kind(),
            FailureKind::SourceUnavailable,
        );
        assert_eq!(
            ProfilePreparationError::infrastructure("semaphore closed").failure_kind(),
            FailureKind::Other,
        );
        assert_eq!(
            FailureKind::from_renderer_error(&RendererError::Timeout),
            FailureKind::RenderTimeout,
        );
    }

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
    fn task_spec_applies_budget_and_initial_hop_state() {
        let arrived_at = Instant::now();
        let budget = std::time::Duration::from_secs(7);
        let task = TaskSpec {
            id: 42,
            request_id: RequestId::from_string("task-spec-test"),
            style: rev("style", 3),
            source: None,
            request: RenderRequest::Tile {
                z: 1,
                x: 0,
                y: 1,
                tile_size: 512,
            },
            pixel_ratio: PixelRatio::from(Scale::X2),
            output_format: ImageFormat::Png,
        }
        .start(arrived_at, budget);

        assert_eq!(task.arrived_at, arrived_at);
        assert_eq!(task.deadline, arrived_at + budget);
        assert_eq!(task.forwarding_hops, 0);
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
    fn from_kvs_requires_explicit_render_admission() {
        let missing = NodeStateView::from_kvs(
            NodeId::from_index(0),
            kvs([("worker.0.style", ""), ("worker.0.queue", "0")]),
        );
        assert!(!missing.accepts_new_renders);

        let degraded = NodeStateView::from_kvs(
            NodeId::from_index(0),
            kvs([
                (RENDER_ADMISSION_GOSSIP_KEY, "false"),
                ("worker.0.style", ""),
                ("worker.0.queue", "0"),
            ]),
        );
        assert!(!degraded.accepts_new_renders);
        assert!(!degraded.has_capacity(1));
        assert!(!degraded.has_admission_capacity(1));

        let malformed = NodeStateView::from_kvs(
            NodeId::from_index(0),
            kvs([(RENDER_ADMISSION_GOSSIP_KEY, "maybe")]),
        );
        assert!(
            !malformed.accepts_new_renders,
            "an advertised but malformed state must fail closed"
        );
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
            (RENDER_ADMISSION_GOSSIP_KEY, "true"),
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
            (RENDER_ADMISSION_GOSSIP_KEY, "true"),
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
