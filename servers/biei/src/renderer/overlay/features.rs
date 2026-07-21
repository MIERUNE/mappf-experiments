use biei_core::types::{GeoJsonOverlay, LngLat, PathOverlay, PinOverlay, StaticOverlay};

use super::pin::{pin_icon_offset_y, pin_image_id};
use super::{
    OVERLAY_IDX_PROPERTY, PIN_IMAGE_PROPERTY, PIN_KIND_PROPERTY, PIN_KIND_VALUE,
    PIN_OFFSET_PROPERTY, parse_geojson_str,
};

/// Build and parse the indexed union FeatureCollection used by both the
/// shared overlay source and `camera_for_geojson` for auto positioning.
/// `_overlay_idx` only changes Feature properties, so the same parsed
/// collection is suitable for camera fitting and source installation.
pub(crate) fn build_overlay_geojson(
    overlays: &[StaticOverlay],
) -> Result<maplibre_native::GeoJson, maplibre_native::StyleError> {
    let fc_json = build_union_feature_collection(overlays).to_string();
    parse_geojson_str(&fc_json)
}

pub(super) fn build_union_feature_collection(overlays: &[StaticOverlay]) -> serde_json::Value {
    serde_json::json!({
        "type": "FeatureCollection",
        "features": build_union_features(overlays),
    })
}

/// Build the union of all overlays' Features, with an `_overlay_idx`
/// property injected into each Feature so per-slot layer filters can
/// scope to one render slot. Consecutive stroke-only paths share a slot;
/// fill-capable paths and GeoJSON overlays remain one slot each.
pub(super) fn build_union_features(overlays: &[StaticOverlay]) -> Vec<serde_json::Value> {
    let mut all = Vec::new();
    for (slot_idx, group) in overlay_slot_groups(overlays).enumerate() {
        for overlay in group {
            let mut features = overlay_to_features(overlay);
            inject_overlay_idx(&mut features, slot_idx);
            all.append(&mut features);
        }
    }
    all
}

pub(super) fn overlay_slot_count(overlays: &[StaticOverlay]) -> usize {
    overlay_slot_groups(overlays).count()
}

/// Groups overlays according to the native layer-slot invariant: one slot for
/// each consecutive stroke-only path run, and one slot for every other overlay.
fn overlay_slot_groups(overlays: &[StaticOverlay]) -> impl Iterator<Item = &[StaticOverlay]> {
    let mut remaining = overlays;
    std::iter::from_fn(move || {
        if remaining.is_empty() {
            return None;
        }
        let group_len = stroke_only_path_run_len(remaining).max(1);
        let (group, rest) = remaining.split_at(group_len);
        remaining = rest;
        Some(group)
    })
}

fn stroke_only_path_run_len(overlays: &[StaticOverlay]) -> usize {
    overlays
        .iter()
        .take_while(|overlay| matches!(overlay, StaticOverlay::Path(path) if !path_has_fill(path)))
        .count()
}

fn path_has_fill(path: &PathOverlay) -> bool {
    path.fill_color.is_some()
}

/// Stamp `_overlay_idx` onto each Feature's `properties` object. If
/// properties is missing or non-object (some GeoJSON uses `null`), we
/// reset it to a fresh object before inserting.
pub(super) fn inject_overlay_idx(features: &mut [serde_json::Value], idx: usize) {
    for feature in features {
        let needs_init = !matches!(
            feature.get("properties"),
            Some(serde_json::Value::Object(_))
        );
        if needs_init {
            feature["properties"] = serde_json::json!({});
        }
        if let Some(props) = feature
            .get_mut("properties")
            .and_then(|v| v.as_object_mut())
        {
            props.insert(OVERLAY_IDX_PROPERTY.to_string(), idx.into());
        }
    }
}

fn overlay_to_features(overlay: &StaticOverlay) -> Vec<serde_json::Value> {
    match overlay {
        StaticOverlay::Path(path) => path_features(path),
        StaticOverlay::GeoJson(g) => geojson_features(g),
        StaticOverlay::Pin(pin) => pin_features(pin),
    }
}

pub(super) fn path_features(path: &PathOverlay) -> Vec<serde_json::Value> {
    let mut features = Vec::new();
    features.push(serde_json::json!({
        "type": "Feature",
        "properties": path_stroke_properties(path),
        "geometry": {
            "type": "LineString",
            "coordinates": coordinates_json(&path.coordinates),
        }
    }));
    if path.fill_color.is_some() && path.coordinates.len() >= 3 {
        let mut ring = path.coordinates.clone();
        if let (Some(first), Some(last)) = (ring.first().copied(), ring.last().copied())
            && first != last
        {
            ring.push(first);
        }
        features.push(serde_json::json!({
            "type": "Feature",
            "properties": path_fill_properties(path),
            "geometry": {
                "type": "Polygon",
                "coordinates": [coordinates_json(&ring)],
            }
        }));
    }
    features
}

pub(super) fn geojson_features(overlay: &GeoJsonOverlay) -> Vec<serde_json::Value> {
    let fc = &overlay.feature_collection;
    let type_str = fc.get("type").and_then(serde_json::Value::as_str);
    match type_str {
        Some("FeatureCollection") => fc
            .get("features")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default(),
        Some("Feature") => vec![fc.clone()],
        _ => Vec::new(),
    }
}

pub(super) fn pin_features(pin: &PinOverlay) -> Vec<serde_json::Value> {
    let mut props = serde_json::Map::new();
    props.insert(PIN_KIND_PROPERTY.to_string(), PIN_KIND_VALUE.into());
    props.insert(PIN_IMAGE_PROPERTY.to_string(), pin_image_id(pin).into());
    props.insert(
        PIN_OFFSET_PROPERTY.to_string(),
        serde_json::json!([0.0, pin_icon_offset_y(pin.size)]),
    );
    vec![serde_json::json!({
        "type": "Feature",
        "properties": props,
        "geometry": {
            "type": "Point",
            "coordinates": [pin.coordinate.lon, pin.coordinate.lat],
        }
    })]
}
pub(super) fn coordinates_json(coordinates: &[LngLat]) -> Vec<serde_json::Value> {
    coordinates
        .iter()
        .map(|p| serde_json::json!([p.lon, p.lat]))
        .collect()
}

pub(super) fn path_stroke_properties(path: &PathOverlay) -> serde_json::Value {
    let mut props = serde_json::Map::new();
    if let Some(color) = &path.stroke_color {
        props.insert("stroke".to_string(), css_color(color).into());
    }
    if let Some(width) = path.stroke_width {
        props.insert("stroke-width".to_string(), width.into());
    }
    if let Some(opacity) = path.stroke_opacity {
        props.insert("stroke-opacity".to_string(), opacity.into());
    }
    serde_json::Value::Object(props)
}

pub(super) fn path_fill_properties(path: &PathOverlay) -> serde_json::Value {
    let mut props = serde_json::Map::new();
    if let Some(color) = &path.fill_color {
        props.insert("fill".to_string(), css_color(color).into());
    }
    if let Some(opacity) = path.fill_opacity {
        props.insert("fill-opacity".to_string(), opacity.into());
    }
    serde_json::Value::Object(props)
}

/// simplestyle expects CSS-style hex colors. The overlay parser stores
/// them as bare hex (`f44` / `00ffcc`), so prepend `#` for `["to-color",
/// ...]`.
pub(super) fn css_color(hex: &str) -> String {
    format!("#{hex}")
}
