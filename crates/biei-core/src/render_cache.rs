//! Node-local rendered image cache.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moka::sync::Cache;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::types::{
    CachePolicy, CompletedInfo, GeoJsonOverlay, ImageFormat, InternalTask, LngLat, NodeId,
    PathOverlay, PinOverlay, PinSize, Positioning, RenderOutput, RenderRequest, RouteTier, Scale,
    SourceHash, StaticOverlay, TaskOutcome, TaskResult,
};
use mmpf_common::sync::lock_unpoisoned;

// Rendered output freshness is independent from the style revision: base
// tiles and other referenced resources may change at stable URLs. Keep the
// cache useful for burst coalescing without serving such output indefinitely.
const RENDER_OUTPUT_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const RENDER_FLIGHT_SHARDS: usize = 16;
const _: () = assert!(RENDER_FLIGHT_SHARDS.is_power_of_two());
type RenderFlightMap = Mutex<HashMap<Arc<RenderCacheKey>, watch::Sender<u64>>>;

#[derive(Clone, Default)]
pub(crate) struct RenderOutputCache {
    inner: Option<Arc<RenderOutputCacheInner>>,
}

struct RenderOutputCacheInner {
    cache: Cache<Arc<RenderCacheKey>, Arc<RenderOutput>>,
    in_flight: Box<[RenderFlightMap]>,
}

impl RenderOutputCacheInner {
    fn flight_shard(&self, key: &RenderCacheKey) -> &RenderFlightMap {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        &self.in_flight[(hasher.finish() as usize) & (RENDER_FLIGHT_SHARDS - 1)]
    }
}

pub(crate) enum RenderCacheLookup {
    Disabled,
    Hit(RenderOutput),
    Leader(RenderFlightLeader),
    Wait(RenderFlightFollower),
}

/// Retains the canonical key while waiting so a follower can recheck the cache
/// or become the next leader without rebuilding an overlay-heavy key.
pub(crate) struct RenderFlightFollower {
    inner: Arc<RenderOutputCacheInner>,
    key: Arc<RenderCacheKey>,
    changed: watch::Receiver<u64>,
}

/// Owns one cache key's render flight. Dropping the guard always wakes
/// followers, including when rendering fails or the leader future is aborted.
pub(crate) struct RenderFlightLeader {
    inner: Arc<RenderOutputCacheInner>,
    key: Option<Arc<RenderCacheKey>>,
}

impl RenderOutputCache {
    pub(crate) fn new(max_capacity_bytes: u64) -> Self {
        Self::with_ttl(max_capacity_bytes, RENDER_OUTPUT_CACHE_TTL)
    }

    fn with_ttl(max_capacity_bytes: u64, ttl: Duration) -> Self {
        if max_capacity_bytes == 0 {
            return Self { inner: None };
        }
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .time_to_live(ttl)
            .weigher(|key: &Arc<RenderCacheKey>, output: &Arc<RenderOutput>| {
                key.estimated_size_bytes()
                    .saturating_add(output.bytes.len())
                    .clamp(1, u32::MAX as usize) as u32
            })
            .build();
        Self {
            inner: Some(Arc::new(RenderOutputCacheInner {
                cache,
                in_flight: (0..RENDER_FLIGHT_SHARDS)
                    .map(|_| Mutex::new(HashMap::new()))
                    .collect(),
            })),
        }
    }

    pub(crate) fn is_enabled_for(&self, task: &InternalTask) -> bool {
        self.inner.is_some()
            && task
                .source
                .as_ref()
                .is_none_or(|s| s.policy == CachePolicy::Cacheable)
    }

    /// Returns a cache hit, joins an existing render, or elects this caller as
    /// the sole renderer for the request key.
    pub(crate) fn lookup_or_join(&self, task: &InternalTask) -> RenderCacheLookup {
        if !self.is_enabled_for(task) {
            return RenderCacheLookup::Disabled;
        }
        let Some(inner) = &self.inner else {
            return RenderCacheLookup::Disabled;
        };
        Self::lookup_or_join_key(Arc::clone(inner), Arc::new(RenderCacheKey::from_task(task)))
    }

    fn lookup_or_join_key(
        inner: Arc<RenderOutputCacheInner>,
        key: Arc<RenderCacheKey>,
    ) -> RenderCacheLookup {
        if let Some(output) = inner.cache.get(&key) {
            return RenderCacheLookup::Hit((*output).clone());
        }

        let mut in_flight = lock_unpoisoned(inner.flight_shard(&key));
        // Close the race with a leader that inserted between the first cache
        // lookup and acquiring the flight lock.
        if let Some(output) = inner.cache.get(&key) {
            return RenderCacheLookup::Hit((*output).clone());
        }
        if let Some((key, changed)) = in_flight.get_key_value(&key) {
            let follower = RenderFlightFollower {
                inner: Arc::clone(&inner),
                key: Arc::clone(key),
                changed: changed.subscribe(),
            };
            drop(in_flight);
            return RenderCacheLookup::Wait(follower);
        }

        let (changed, _) = watch::channel(0);
        in_flight.insert(Arc::clone(&key), changed);
        drop(in_flight);
        RenderCacheLookup::Leader(RenderFlightLeader {
            inner,
            key: Some(key),
        })
    }
}

impl RenderFlightFollower {
    pub(crate) async fn changed(&mut self) {
        let _ = self.changed.changed().await;
    }

    pub(crate) fn recheck(self) -> RenderCacheLookup {
        let Self { inner, key, .. } = self;
        RenderOutputCache::lookup_or_join_key(inner, key)
    }
}

impl RenderFlightLeader {
    pub(crate) fn insert_from_outcome(&self, outcome: &TaskOutcome) -> bool {
        let TaskResult::Completed { output, .. } = &outcome.result else {
            return false;
        };
        let Some(key) = &self.key else {
            return false;
        };
        self.inner
            .cache
            .insert(Arc::clone(key), Arc::new(output.clone()));
        true
    }
}

impl Drop for RenderFlightLeader {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        let changed = lock_unpoisoned(self.inner.flight_shard(&key)).remove(&key);
        if let Some(changed) = changed {
            changed.send_modify(|version| *version = version.wrapping_add(1));
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RenderCacheKey {
    style_id: String,
    style_version: u64,
    request: RenderRequestKey,
    scale: Scale,
    output_format: ImageFormat,
    source_hash: Option<SourceHash>,
}

impl RenderCacheKey {
    fn from_task(task: &InternalTask) -> Self {
        Self {
            style_id: task.style.id.as_str().to_owned(),
            style_version: task.style.version,
            request: RenderRequestKey::from(&task.request),
            scale: task.pixel_ratio.to_scale(),
            output_format: task.output_format,
            source_hash: task.source.as_ref().map(|s| s.hash),
        }
    }

    fn estimated_size_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.style_id.len())
            .saturating_add(self.request.heap_size_bytes())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum RenderRequestKey {
    Tile {
        z: u8,
        x: u32,
        y: u32,
        tile_size: u16,
    },
    StaticImage {
        positioning: PositioningKey,
        width: u16,
        height: u16,
        overlays: Vec<StaticOverlayKey>,
        before_layer: Option<String>,
        padding: crate::types::Padding,
        /// Canonical identity of the request's `addlayer` — its effective
        /// layer JSON plus any request-local source content — or `None` when
        /// absent. Carrying the bounded JSON itself rather than a 64-bit hash
        /// of it keeps two requests that differ only in their addlayer from
        /// colliding on one render cache entry without depending on hash
        /// uniqueness (a collision would otherwise return the wrong image).
        addlayer: Option<Arc<str>>,
    },
}

/// Collision-free render-cache identity for an addlayer: its effective layer
/// JSON plus any request-local source content. Each component is length-framed
/// (`<byte-len>:<bytes>`), which is unambiguous for *any* content — the encoding
/// stays injective (distinct component tuples → distinct strings, including a
/// different component count) without assuming a separator byte is absent from
/// canonical JSON or validated ids. All components are length-bounded at
/// ingress. Never depend on a forbidden-byte separator for cache-correctness
/// identity, which a future component change could silently break.
fn addlayer_identity(addlayer: &crate::types::AddLayer) -> Arc<str> {
    use std::fmt::Write as _;
    let mut identity = String::new();
    let mut frame = |component: &str| {
        let _ = write!(identity, "{}:{}", component.len(), component);
    };
    frame(&addlayer.json);
    if let Some(source) = &addlayer.source {
        frame(&source.tileset_id);
        frame(&source.json);
    }
    Arc::from(identity.as_str())
}

impl RenderRequestKey {
    fn heap_size_bytes(&self) -> usize {
        match self {
            Self::Tile { .. } => 0,
            Self::StaticImage {
                overlays,
                before_layer,
                addlayer,
                ..
            } => overlays
                .len()
                .saturating_mul(std::mem::size_of::<StaticOverlayKey>())
                .saturating_add(
                    overlays
                        .iter()
                        .map(StaticOverlayKey::heap_size_bytes)
                        .sum::<usize>(),
                )
                .saturating_add(before_layer.as_ref().map_or(0, String::len))
                .saturating_add(addlayer.as_ref().map_or(0, |identity| identity.len())),
        }
    }
}

impl From<&RenderRequest> for RenderRequestKey {
    fn from(request: &RenderRequest) -> Self {
        match request {
            RenderRequest::Tile { z, x, y, tile_size } => Self::Tile {
                z: *z,
                x: *x,
                y: *y,
                tile_size: *tile_size,
            },
            RenderRequest::StaticImage {
                positioning,
                width,
                height,
                overlays,
                before_layer,
                padding,
                addlayer,
            } => Self::StaticImage {
                positioning: PositioningKey::from(positioning),
                width: *width,
                height: *height,
                overlays: overlays.iter().map(StaticOverlayKey::from).collect(),
                before_layer: before_layer.clone(),
                padding: *padding,
                addlayer: addlayer.as_ref().map(addlayer_identity),
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum StaticOverlayKey {
    Path(PathOverlayKey),
    GeoJson(GeoJsonOverlayKey),
    Pin(PinOverlayKey),
}

impl StaticOverlayKey {
    fn heap_size_bytes(&self) -> usize {
        match self {
            Self::Path(path) => path
                .coordinates
                .len()
                .saturating_mul(std::mem::size_of::<LngLatKey>())
                .saturating_add(path.stroke_color.as_ref().map_or(0, String::len))
                .saturating_add(path.fill_color.as_ref().map_or(0, String::len)),
            Self::GeoJson(geojson) => geojson.feature_collection.len(),
            Self::Pin(pin) => pin
                .label
                .as_ref()
                .map_or(0, String::len)
                .saturating_add(pin.color.len()),
        }
    }
}

impl From<&StaticOverlay> for StaticOverlayKey {
    fn from(overlay: &StaticOverlay) -> Self {
        match overlay {
            StaticOverlay::Path(path) => Self::Path(PathOverlayKey::from(path)),
            StaticOverlay::GeoJson(geojson) => Self::GeoJson(GeoJsonOverlayKey::from(geojson)),
            StaticOverlay::Pin(pin) => Self::Pin(PinOverlayKey::from(pin)),
        }
    }
}

/// `serde_json::Value` itself is not `Hash`, so we hash the JSON byte form
/// instead. This is **lexical** equality on the serialized form — two GeoJSON
/// payloads that are semantically equivalent but differ in whitespace,
/// number formatting, or object-key order will land in different cache
/// entries. Acceptable here because the cache is keyed on what the client
/// actually sent, and dropping a few near-miss hits is cheaper than running a
/// canonical-form normalizer on every request.
#[derive(Debug, PartialEq, Eq, Hash)]
struct GeoJsonOverlayKey {
    feature_collection: String,
}

impl From<&GeoJsonOverlay> for GeoJsonOverlayKey {
    fn from(overlay: &GeoJsonOverlay) -> Self {
        Self {
            feature_collection: overlay.feature_collection.to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PathOverlayKey {
    stroke_width: Option<u32>,
    stroke_color: Option<String>,
    stroke_opacity: Option<u32>,
    fill_color: Option<String>,
    fill_opacity: Option<u32>,
    coordinates: Vec<LngLatKey>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PinOverlayKey {
    size: PinSize,
    label: Option<String>,
    color: String,
    coordinate: LngLatKey,
}

impl From<&PinOverlay> for PinOverlayKey {
    fn from(pin: &PinOverlay) -> Self {
        Self {
            size: pin.size,
            label: pin.label.clone(),
            color: pin.color.clone(),
            coordinate: LngLatKey::from(&pin.coordinate),
        }
    }
}

impl From<&PathOverlay> for PathOverlayKey {
    fn from(path: &PathOverlay) -> Self {
        Self {
            stroke_width: path.stroke_width.map(f32::to_bits),
            stroke_color: path.stroke_color.clone(),
            stroke_opacity: path.stroke_opacity.map(f32::to_bits),
            fill_color: path.fill_color.clone(),
            fill_opacity: path.fill_opacity.map(f32::to_bits),
            coordinates: path.coordinates.iter().map(LngLatKey::from).collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct LngLatKey {
    lon: u64,
    lat: u64,
}

impl From<&LngLat> for LngLatKey {
    fn from(point: &LngLat) -> Self {
        Self {
            lon: point.lon.to_bits(),
            lat: point.lat.to_bits(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum PositioningKey {
    Center {
        lon: u64,
        lat: u64,
        zoom: u64,
        bearing: u32,
        pitch: u32,
    },
    Bbox {
        min_lon: u64,
        min_lat: u64,
        max_lon: u64,
        max_lat: u64,
    },
    Auto,
}

impl From<&Positioning> for PositioningKey {
    fn from(positioning: &Positioning) -> Self {
        match positioning {
            Positioning::Center {
                lon,
                lat,
                zoom,
                bearing,
                pitch,
            } => Self::Center {
                lon: lon.to_bits(),
                lat: lat.to_bits(),
                zoom: zoom.to_bits(),
                bearing: bearing.to_bits(),
                pitch: pitch.to_bits(),
            },
            Positioning::Bbox {
                min_lon,
                min_lat,
                max_lon,
                max_lat,
            } => Self::Bbox {
                min_lon: min_lon.to_bits(),
                min_lat: min_lat.to_bits(),
                max_lon: max_lon.to_bits(),
                max_lat: max_lat.to_bits(),
            },
            Positioning::Auto => Self::Auto,
        }
    }
}

pub(crate) fn cache_hit_outcome(
    node_id: NodeId,
    task: &InternalTask,
    output: RenderOutput,
) -> TaskOutcome {
    let now = Instant::now();
    TaskOutcome {
        task_id: task.id,
        request_id: task.request_id.clone(),
        arrived_at: task.arrived_at,
        had_source: task.has_source(),
        deadline_stage: None,
        result: TaskResult::Completed {
            info: CompletedInfo {
                node_id,
                worker_id: None,
                route_tier: RouteTier::RenderCacheHit,
                started_at: task.arrived_at,
                native_render_started_at: now,
                native_render_completed_at: now,
                completed_at: now,
                style_swap: false,
                cold_start: false,
                source_loaded: false,
                admitted_at_overflow: false,
                render_observation: None,
            },
            output,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        AddLayer, AddLayerSource, GeoJsonOverlay, LngLat, NodeId, PathOverlay, PinOverlay,
        PixelRatio, RequestId, SourceRef, StaticOverlay, StyleId, StyleRevision, TaskResult,
    };
    use std::hash::BuildHasherDefault;
    use std::time::Duration;

    fn task(y: u32, source: Option<SourceRef>) -> InternalTask {
        let now = Instant::now();
        InternalTask {
            id: 1,
            request_id: RequestId::from_string("cache-test"),
            style: StyleRevision {
                id: StyleId("style".to_string()),
                version: 1,
            },
            source,
            request: RenderRequest::Tile {
                z: 0,
                x: 0,
                y,
                tile_size: 512,
            },
            pixel_ratio: PixelRatio::X1,
            output_format: ImageFormat::Png,
            arrived_at: now,
            deadline: now + Duration::from_secs(1),
            forwarding_hops: 0,
        }
    }

    fn rich_static_task(feature_name: &str) -> InternalTask {
        let mut task = task(0, None);
        task.request = RenderRequest::StaticImage {
            positioning: Positioning::Auto,
            width: 1024,
            height: 768,
            overlays: vec![
                StaticOverlay::Path(PathOverlay {
                    stroke_width: Some(5.0),
                    stroke_color: Some("f44".to_string()),
                    stroke_opacity: Some(0.8),
                    fill_color: Some("00ff00".to_string()),
                    fill_opacity: Some(0.25),
                    coordinates: vec![
                        LngLat {
                            lon: 139.0,
                            lat: 35.0,
                        },
                        LngLat {
                            lon: 140.0,
                            lat: 36.0,
                        },
                    ],
                }),
                StaticOverlay::GeoJson(GeoJsonOverlay {
                    feature_collection: serde_json::json!({
                        "type": "FeatureCollection",
                        "features": [{
                            "type": "Feature",
                            "properties": { "name": feature_name },
                            "geometry": {
                                "type": "Point",
                                "coordinates": [139.5, 35.5]
                            }
                        }]
                    }),
                }),
                StaticOverlay::Pin(PinOverlay {
                    size: PinSize::Large,
                    label: Some("A".to_string()),
                    color: "336699".to_string(),
                    coordinate: LngLat {
                        lon: 139.5,
                        lat: 35.5,
                    },
                }),
            ],
            before_layer: Some("labels".to_string()),
            padding: crate::types::Padding {
                top: 10,
                right: 20,
                bottom: 30,
                left: 40,
            },
            addlayer: Some(AddLayer {
                json: r#"{"id":"route","type":"line","source":"route-source"}"#.to_string(),
                hash: 42,
                source: Some(AddLayerSource {
                    tileset_id: "routes".to_string(),
                    json: r#"{"type":"vector","url":"mapbox://routes"}"#.to_string(),
                }),
            }),
        };
        task
    }

    #[derive(Default)]
    struct CollisionHasher;

    impl Hasher for CollisionHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    fn output(bytes: &[u8], format: ImageFormat) -> RenderOutput {
        RenderOutput {
            bytes: bytes.to_vec().into(),
            format,
        }
    }

    fn completed_outcome(task: &InternalTask, output: RenderOutput) -> TaskOutcome {
        let now = Instant::now();
        TaskOutcome {
            task_id: task.id,
            request_id: task.request_id.clone(),
            arrived_at: task.arrived_at,
            had_source: task.has_source(),
            deadline_stage: None,
            result: TaskResult::Completed {
                info: CompletedInfo {
                    node_id: NodeId::from_index(0),
                    worker_id: Some(0),
                    route_tier: RouteTier::Tier1WarmTracking,
                    started_at: task.arrived_at,
                    native_render_started_at: now,
                    native_render_completed_at: now,
                    completed_at: now,
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
                    render_observation: None,
                },
                output,
            },
        }
    }

    #[test]
    fn cache_key_separates_request_identity() {
        let a = RenderCacheKey::from_task(&task(0, None));
        let b = RenderCacheKey::from_task(&task(1, None));

        assert_ne!(a, b);
    }

    #[test]
    fn rich_static_keys_use_full_equality_even_when_hashes_collide() {
        let key_a = Arc::new(RenderCacheKey::from_task(&rich_static_task("alpha")));
        let key_b = Arc::new(RenderCacheKey::from_task(&rich_static_task("beta")));
        let mut keys =
            HashMap::<Arc<RenderCacheKey>, (), BuildHasherDefault<CollisionHasher>>::default();

        keys.insert(Arc::clone(&key_a), ());
        keys.insert(Arc::clone(&key_b), ());

        assert_ne!(key_a, key_b);
        assert_eq!(
            keys.len(),
            2,
            "content equality must resolve hash collisions"
        );
    }

    #[test]
    fn cache_key_separates_scale_format_positioning_and_style_version() {
        let base = task(0, None);
        let mut scaled = base.clone();
        scaled.pixel_ratio = PixelRatio::from(Scale::X2);
        let mut webp = base.clone();
        webp.output_format = ImageFormat::Webp;
        let mut newer_style = base.clone();
        newer_style.style.version += 1;
        let mut static_image = base.clone();
        static_image.request = RenderRequest::StaticImage {
            positioning: Positioning::Center {
                lon: 139.0,
                lat: 35.0,
                zoom: 10.0,
                bearing: 0.0,
                pitch: 0.0,
            },
            width: 512,
            height: 512,
            overlays: Vec::new(),
            before_layer: None,
            padding: crate::types::Padding::default(),
            addlayer: None,
        };

        let base_key = RenderCacheKey::from_task(&base);
        assert_ne!(base_key, RenderCacheKey::from_task(&scaled));
        assert_ne!(base_key, RenderCacheKey::from_task(&webp));
        assert_ne!(base_key, RenderCacheKey::from_task(&newer_style));
        assert_ne!(base_key, RenderCacheKey::from_task(&static_image));
    }

    #[test]
    fn cache_key_separates_static_overlays() {
        let mut base = task(0, None);
        base.request = RenderRequest::StaticImage {
            positioning: Positioning::Center {
                lon: 139.0,
                lat: 35.0,
                zoom: 10.0,
                bearing: 0.0,
                pitch: 0.0,
            },
            width: 512,
            height: 512,
            overlays: Vec::new(),
            before_layer: None,
            padding: crate::types::Padding::default(),
            addlayer: None,
        };
        let mut with_path = base.clone();
        with_path.request = RenderRequest::StaticImage {
            positioning: Positioning::Center {
                lon: 139.0,
                lat: 35.0,
                zoom: 10.0,
                bearing: 0.0,
                pitch: 0.0,
            },
            width: 512,
            height: 512,
            overlays: vec![StaticOverlay::Path(PathOverlay {
                stroke_width: Some(5.0),
                stroke_color: Some("f44".to_string()),
                stroke_opacity: Some(1.0),
                fill_color: None,
                fill_opacity: None,
                coordinates: vec![
                    LngLat {
                        lon: 139.0,
                        lat: 35.0,
                    },
                    LngLat {
                        lon: 140.0,
                        lat: 36.0,
                    },
                ],
            })],
            before_layer: None,
            padding: crate::types::Padding::default(),
            addlayer: None,
        };

        let base_key = RenderCacheKey::from_task(&base);
        let path_key = RenderCacheKey::from_task(&with_path);
        assert_ne!(base_key, path_key);
        assert!(path_key.estimated_size_bytes() > base_key.estimated_size_bytes());
    }

    #[test]
    fn cache_key_separates_static_by_addlayer_identity() {
        let mut base = task(0, None);
        base.request = RenderRequest::StaticImage {
            positioning: Positioning::Center {
                lon: 139.0,
                lat: 35.0,
                zoom: 10.0,
                bearing: 0.0,
                pitch: 0.0,
            },
            width: 512,
            height: 512,
            overlays: Vec::new(),
            before_layer: None,
            padding: crate::types::Padding::default(),
            addlayer: None,
        };
        // Same request with an addlayer attached.
        let mut with_addlayer = base.clone();
        with_addlayer.request = RenderRequest::StaticImage {
            positioning: Positioning::Center {
                lon: 139.0,
                lat: 35.0,
                zoom: 10.0,
                bearing: 0.0,
                pitch: 0.0,
            },
            width: 512,
            height: 512,
            overlays: Vec::new(),
            before_layer: None,
            padding: crate::types::Padding::default(),
            addlayer: Some(crate::types::AddLayer {
                json: r#"{"id":"x","type":"fill","source":"s"}"#.to_string(),
                hash: 123,
                source: None,
            }),
        };
        // A second addlayer whose JSON differs but whose `hash` field is
        // identical: the render cache key must still separate them, because it
        // keys on the JSON identity rather than the 64-bit hash. This is the
        // hash-collision case that would otherwise return the wrong image.
        let mut with_other_addlayer = with_addlayer.clone();
        if let RenderRequest::StaticImage { addlayer, .. } = &mut with_other_addlayer.request
            && let Some(a) = addlayer
        {
            a.json = r#"{"id":"x","type":"line","source":"s"}"#.to_string();
            // Same colliding hash value on purpose.
        }

        assert_ne!(
            RenderCacheKey::from_task(&base),
            RenderCacheKey::from_task(&with_addlayer)
        );
        assert_ne!(
            RenderCacheKey::from_task(&with_addlayer),
            RenderCacheKey::from_task(&with_other_addlayer),
            "identical hash but different addlayer JSON must not share a cache entry"
        );
    }

    #[test]
    fn cache_key_separates_static_before_layer_and_padding() {
        let mut base = task(0, None);
        base.request = RenderRequest::StaticImage {
            positioning: Positioning::Bbox {
                min_lon: 139.0,
                min_lat: 35.0,
                max_lon: 140.0,
                max_lat: 36.0,
            },
            width: 512,
            height: 512,
            overlays: Vec::new(),
            before_layer: None,
            padding: crate::types::Padding::default(),
            addlayer: None,
        };

        let mut before_layer = base.clone();
        if let RenderRequest::StaticImage { before_layer, .. } = &mut before_layer.request {
            *before_layer = Some("labels".to_string());
        }

        let mut padded = base.clone();
        if let RenderRequest::StaticImage { padding, .. } = &mut padded.request {
            *padding = crate::types::Padding {
                top: 10,
                right: 20,
                bottom: 30,
                left: 40,
            };
        }

        let base_key = RenderCacheKey::from_task(&base);
        assert_ne!(base_key, RenderCacheKey::from_task(&before_layer));
        assert_ne!(base_key, RenderCacheKey::from_task(&padded));
    }

    #[test]
    fn rich_static_key_ownership_is_shared_across_flight_and_cache() {
        let cache = RenderOutputCache::new(1024 * 1024);
        let task = rich_static_task("shared");
        let leader = match cache.lookup_or_join(&task) {
            RenderCacheLookup::Leader(leader) => leader,
            _ => panic!("first cache miss should elect a leader"),
        };
        let leader_key = Arc::clone(leader.key.as_ref().expect("leader should own its key"));
        let inner = cache.inner.as_ref().expect("cache should be enabled");

        {
            let in_flight = lock_unpoisoned(inner.flight_shard(&leader_key));
            let (flight_key, _) = in_flight
                .get_key_value(&leader_key)
                .expect("leader key should be registered in the flight map");
            assert!(Arc::ptr_eq(&leader_key, flight_key));
        }

        let follower = match cache.lookup_or_join(&task) {
            RenderCacheLookup::Wait(follower) => follower,
            _ => panic!("matching cache miss should follow the existing flight"),
        };
        assert!(Arc::ptr_eq(&leader_key, &follower.key));

        assert!(leader.insert_from_outcome(&completed_outcome(
            &task,
            output(&[1, 2, 3], ImageFormat::Png),
        )));
        let (cached_key, _) = inner.cache.iter().next().expect("output should be cached");
        assert!(Arc::ptr_eq(&leader_key, cached_key.as_ref()));

        drop(leader);
        assert!(matches!(follower.recheck(), RenderCacheLookup::Hit(_)));
    }

    #[test]
    fn insert_and_get_roundtrip_render_output() {
        let cache = RenderOutputCache::new(1024);
        let t = task(0, None);
        let rendered = output(&[1, 2, 3], ImageFormat::Png);
        let outcome = completed_outcome(&t, rendered.clone());

        let leader = match cache.lookup_or_join(&t) {
            RenderCacheLookup::Leader(leader) => leader,
            _ => panic!("cache miss should elect a leader"),
        };
        assert!(leader.insert_from_outcome(&outcome));
        drop(leader);
        match cache.lookup_or_join(&t) {
            RenderCacheLookup::Hit(output) => assert_eq!(output, rendered),
            _ => panic!("inserted output should be a cache hit"),
        }
    }

    #[test]
    fn rendered_output_expires_even_when_the_key_is_stable() {
        let cache = RenderOutputCache::with_ttl(1024, Duration::from_millis(10));
        let t = task(0, None);
        let leader = match cache.lookup_or_join(&t) {
            RenderCacheLookup::Leader(leader) => leader,
            _ => panic!("cache miss should elect a leader"),
        };
        assert!(
            leader
                .insert_from_outcome(&completed_outcome(&t, output(&[1, 2, 3], ImageFormat::Png),))
        );
        drop(leader);

        std::thread::sleep(Duration::from_millis(30));
        assert!(matches!(
            cache.lookup_or_join(&t),
            RenderCacheLookup::Leader(_)
        ));
    }

    #[tokio::test]
    async fn concurrent_lookup_joins_leader_and_observes_inserted_output() {
        let cache = RenderOutputCache::new(1024);
        let t = task(0, None);
        let leader = match cache.lookup_or_join(&t) {
            RenderCacheLookup::Leader(leader) => leader,
            _ => panic!("first cache miss should lead the render"),
        };
        let mut follower = match cache.lookup_or_join(&t) {
            RenderCacheLookup::Wait(follower) => follower,
            _ => panic!("concurrent cache miss should wait for the leader"),
        };
        let rendered = output(&[1, 2, 3], ImageFormat::Png);

        assert!(leader.insert_from_outcome(&completed_outcome(&t, rendered.clone())));
        drop(leader);
        tokio::time::timeout(Duration::from_secs(1), follower.changed())
            .await
            .expect("leader completion should wake followers");

        match follower.recheck() {
            RenderCacheLookup::Hit(output) => assert_eq!(output, rendered),
            _ => panic!("follower should observe the leader's cached output"),
        }
    }

    #[tokio::test]
    async fn dropping_failed_leader_wakes_follower_for_new_election() {
        let cache = RenderOutputCache::new(1024);
        let t = task(0, None);
        let leader = match cache.lookup_or_join(&t) {
            RenderCacheLookup::Leader(leader) => leader,
            _ => panic!("first cache miss should lead the render"),
        };
        let mut follower = match cache.lookup_or_join(&t) {
            RenderCacheLookup::Wait(follower) => follower,
            _ => panic!("concurrent cache miss should wait for the leader"),
        };

        drop(leader);
        tokio::time::timeout(Duration::from_secs(1), follower.changed())
            .await
            .expect("failed leader should wake followers");

        assert!(matches!(follower.recheck(), RenderCacheLookup::Leader(_)));
    }

    #[test]
    fn disabled_cache_never_hits_or_inserts() {
        let cache = RenderOutputCache::new(0);
        let t = task(0, None);

        assert!(!cache.is_enabled_for(&t));
        assert!(matches!(
            cache.lookup_or_join(&t),
            RenderCacheLookup::Disabled
        ));
    }

    #[test]
    fn cache_hit_outcome_has_no_worker_and_records_request_latency() {
        let t = task(0, None);
        let outcome = cache_hit_outcome(NodeId::from_index(0), &t, output(&[1], ImageFormat::Png));
        let TaskResult::Completed { info, .. } = outcome.result else {
            panic!("expected completed cache hit");
        };

        assert_eq!(info.worker_id, None);
        assert_eq!(info.route_tier, RouteTier::RenderCacheHit);
        assert_eq!(info.started_at, t.arrived_at);
        assert!(info.completed_at >= t.arrived_at);
    }

    #[test]
    fn one_shot_sources_are_not_cacheable() {
        let cache = RenderOutputCache::new(1024);
        let t = task(
            0,
            Some(SourceRef {
                hash: 42,
                policy: CachePolicy::OneShot,
            }),
        );

        assert!(!cache.is_enabled_for(&t));
    }
}
