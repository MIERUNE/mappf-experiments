//! Node-local rendered image cache.

use std::sync::Arc;

use moka::sync::Cache;
use tokio::time::Instant;

use crate::types::{
    CachePolicy, CompletedInfo, GeoJsonOverlay, ImageFormat, InternalTask, LngLat, NodeId,
    PathOverlay, PinOverlay, PinSize, Positioning, RenderOutput, RenderRequest, RouteTier, Scale,
    SourceHash, StaticOverlay, TaskOutcome, TaskResult,
};

#[derive(Clone, Default)]
pub struct RenderOutputCache {
    inner: Option<Cache<RenderCacheKey, Arc<RenderOutput>>>,
}

impl RenderOutputCache {
    pub fn new(max_capacity_bytes: u64) -> Self {
        if max_capacity_bytes == 0 {
            return Self { inner: None };
        }
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .weigher(|_key: &RenderCacheKey, output: &Arc<RenderOutput>| {
                output.bytes.len().clamp(1, u32::MAX as usize) as u32
            })
            .build();
        Self { inner: Some(cache) }
    }

    pub fn is_enabled_for(&self, task: &InternalTask) -> bool {
        self.inner.is_some()
            && task
                .source
                .as_ref()
                .is_none_or(|s| s.policy == CachePolicy::Cacheable)
    }

    pub fn get(&self, task: &InternalTask) -> Option<RenderOutput> {
        let cache = self.inner.as_ref()?;
        cache
            .get(&RenderCacheKey::from_task(task))
            .map(|output| (*output).clone())
    }

    pub fn insert_from_outcome(&self, task: &InternalTask, outcome: &TaskOutcome) -> bool {
        if !self.is_enabled_for(task) {
            return false;
        }
        let Some(cache) = &self.inner else {
            return false;
        };
        let TaskResult::Completed { output, .. } = &outcome.result else {
            return false;
        };
        cache.insert(RenderCacheKey::from_task(task), Arc::new(output.clone()));
        true
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
            request: RenderRequestKey::from(task.request.clone()),
            scale: task.pixel_ratio.to_scale(),
            output_format: task.output_format,
            source_hash: task.source.as_ref().map(|s| s.hash),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
        /// Hash of the request's `addlayer` JSON, or `None` when no
        /// `addlayer` was provided. Including the hash here is what keeps
        /// two requests that differ only in their addlayer JSON from
        /// colliding on the same render cache entry.
        addlayer_hash: Option<u64>,
    },
}

impl From<RenderRequest> for RenderRequestKey {
    fn from(request: RenderRequest) -> Self {
        match request {
            RenderRequest::Tile { z, x, y, tile_size } => Self::Tile { z, x, y, tile_size },
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
                width,
                height,
                overlays: overlays.into_iter().map(StaticOverlayKey::from).collect(),
                before_layer,
                padding,
                addlayer_hash: addlayer.map(|a| a.hash),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum StaticOverlayKey {
    Path(PathOverlayKey),
    GeoJson(GeoJsonOverlayKey),
    Pin(PinOverlayKey),
}

impl From<StaticOverlay> for StaticOverlayKey {
    fn from(overlay: StaticOverlay) -> Self {
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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GeoJsonOverlayKey {
    feature_collection: String,
}

impl From<GeoJsonOverlay> for GeoJsonOverlayKey {
    fn from(overlay: GeoJsonOverlay) -> Self {
        Self {
            feature_collection: overlay.feature_collection.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PathOverlayKey {
    stroke_width: Option<u32>,
    stroke_color: Option<String>,
    stroke_opacity: Option<u32>,
    fill_color: Option<String>,
    fill_opacity: Option<u32>,
    coordinates: Vec<LngLatKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PinOverlayKey {
    size: PinSize,
    label: Option<String>,
    color: String,
    coordinate: LngLatKey,
}

impl From<PinOverlay> for PinOverlayKey {
    fn from(pin: PinOverlay) -> Self {
        Self {
            size: pin.size,
            label: pin.label,
            color: pin.color,
            coordinate: LngLatKey::from(pin.coordinate),
        }
    }
}

impl From<PathOverlay> for PathOverlayKey {
    fn from(path: PathOverlay) -> Self {
        Self {
            stroke_width: path.stroke_width.map(f32::to_bits),
            stroke_color: path.stroke_color,
            stroke_opacity: path.stroke_opacity.map(f32::to_bits),
            fill_color: path.fill_color,
            fill_opacity: path.fill_opacity.map(f32::to_bits),
            coordinates: path.coordinates.into_iter().map(LngLatKey::from).collect(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct LngLatKey {
    lon: u64,
    lat: u64,
}

impl From<LngLat> for LngLatKey {
    fn from(point: LngLat) -> Self {
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

impl From<Positioning> for PositioningKey {
    fn from(positioning: Positioning) -> Self {
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

pub fn cache_hit_outcome(
    node_id: NodeId,
    task: &InternalTask,
    output: RenderOutput,
) -> TaskOutcome {
    let now = Instant::now();
    TaskOutcome {
        task_id: task.id,
        request_id: task.request_id.clone(),
        arrived_at: task.arrived_at,
        had_source: task.source.is_some(),
        deadline_stage: None,
        result: TaskResult::Completed {
            info: CompletedInfo {
                node_id,
                worker_id: None,
                route_tier: RouteTier::RenderCacheHit,
                started_at: task.arrived_at,
                cpu_started_at: now,
                cpu_completed_at: now,
                completed_at: now,
                style_swap: false,
                cold_start: false,
                source_loaded: false,
                admitted_at_overflow: false,
            },
            output,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        LngLat, NodeId, PathOverlay, PixelRatio, RequestId, SourceRef, StaticOverlay, StyleId,
        StyleRevision, TaskResult,
    };
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
            had_source: task.source.is_some(),
            deadline_stage: None,
            result: TaskResult::Completed {
                info: CompletedInfo {
                    node_id: NodeId::from_index(0),
                    worker_id: Some(0),
                    route_tier: RouteTier::Tier1WarmTracking,
                    started_at: task.arrived_at,
                    cpu_started_at: now,
                    cpu_completed_at: now,
                    completed_at: now,
                    style_swap: false,
                    cold_start: false,
                    source_loaded: false,
                    admitted_at_overflow: false,
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

        assert_ne!(
            RenderCacheKey::from_task(&base),
            RenderCacheKey::from_task(&with_path)
        );
    }

    #[test]
    fn cache_key_separates_static_by_addlayer_hash() {
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
        // A second request with a different addlayer hash.
        let mut with_other_addlayer = with_addlayer.clone();
        if let RenderRequest::StaticImage { addlayer, .. } = &mut with_other_addlayer.request
            && let Some(a) = addlayer
        {
            a.hash = 456;
        }

        assert_ne!(
            RenderCacheKey::from_task(&base),
            RenderCacheKey::from_task(&with_addlayer)
        );
        assert_ne!(
            RenderCacheKey::from_task(&with_addlayer),
            RenderCacheKey::from_task(&with_other_addlayer)
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
    fn insert_and_get_roundtrip_render_output() {
        let cache = RenderOutputCache::new(1024);
        let t = task(0, None);
        let rendered = output(&[1, 2, 3], ImageFormat::Png);
        let outcome = completed_outcome(&t, rendered.clone());

        assert!(cache.insert_from_outcome(&t, &outcome));
        assert_eq!(cache.get(&t), Some(rendered));
    }

    #[test]
    fn disabled_cache_never_hits_or_inserts() {
        let cache = RenderOutputCache::new(0);
        let t = task(0, None);
        let outcome = completed_outcome(&t, output(&[1, 2, 3], ImageFormat::Png));

        assert!(!cache.is_enabled_for(&t));
        assert!(!cache.insert_from_outcome(&t, &outcome));
        assert_eq!(cache.get(&t), None);
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
