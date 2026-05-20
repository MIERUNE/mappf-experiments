//! Static overlay rendering support for the MapLibre backend.
//!
//! This module owns the shared GeoJsonSource and the per-slot layer triples
//! used by static image overlays. The actor keeps the renderer
//! thread-affine; this file keeps per-request overlay mutation separate
//! from the actor loop and backend setup logic.
//!
//! ## Topology
//!
//! - **One shared source** (`biei-overlays-src`) holds the union of all
//!   overlays' Features. biei injects an `_overlay_idx` property into each
//!   Feature so that per-slot layers can scope to a single overlay's
//!   subset.
//! - **Per-slot layer sets** (Fill/Line/Circle/Symbol) filter by both
//!   `["geometry-type"]` and `["==", ["get", "__biei_overlay_idx"], i]`. URL
//!   order is preserved because layers for slot i are added above layers
//!   for slot i-1 in the style stack.
//! - Consecutive stroke-only `path(...)` overlays are coalesced into one
//!   slot. Fill-capable paths and GeoJSON overlays keep their own slots so
//!   cross-geometry z-order cannot change.
//! - Layers are lazily allocated up to `MAX_OVERLAYS = 64`. The source is
//!   created once at pool init and reused for every request.

use crate::http::overlay::MAX_OVERLAYS;
use crate::types::{GeoJsonOverlay, LngLat, PathOverlay, PinOverlay, StaticOverlay};

mod pin;

pub(crate) use pin::pin_auto_padding_inset;
use pin::{pin_icon_offset_y, pin_image_id, pin_image_ids, register_pin_images};

const OVERLAY_SOURCE_ID: &str = "biei-overlays-src";
/// Property name biei injects into each Feature in the shared source so
/// that the per-slot layer filter `["==", ["get", "__biei_overlay_idx"], i]`
/// can pick out the right subset. Double-underscore prefix marks it as
/// internal and reduces the chance of collision with a user-supplied
/// property of the same name.
const OVERLAY_IDX_PROPERTY: &str = "__biei_overlay_idx";
const PIN_KIND_PROPERTY: &str = "__biei_marker_kind";
const PIN_IMAGE_PROPERTY: &str = "__biei_marker_image";
const PIN_OFFSET_PROPERTY: &str = "__biei_marker_offset";
const PIN_KIND_VALUE: &str = "pin";
const EMPTY_FEATURE_COLLECTION: &str = r#"{"type":"FeatureCollection","features":[]}"#;

/// Lazy, high-water-mark pool for overlay layers. The shared source is
/// created once at pool construction; layer triples are added on demand
/// and kept across requests. Grows up to `MAX_OVERLAYS` (mirroring the
/// ingress hard limit, section 7.3.1) and never shrinks.
#[derive(Debug)]
pub(crate) struct OverlaySlotPool {
    /// Number of slot layer triples (fill/line/circle) currently in the
    /// style. Slot indices run `0..allocated`; per-layer filters refer to
    /// these indices via `_overlay_idx`.
    allocated: usize,
    /// Mirrors the ingress overlay-count cap. Defense in depth: even if a
    /// caller bypasses ingress, the pool refuses to grow past this.
    max_size: usize,
    /// Pre-parsed empty FeatureCollection, used to reset the shared source
    /// when transitioning from N>0 to N=0. Cached at pool construction so
    /// the constant `EMPTY_FEATURE_COLLECTION` JSON is parsed only once
    /// per renderer lifetime.
    empty_fc: maplibre_native::GeoJson,
    /// All allocated layers are positioned BEFORE this base-style layer
    /// when set, or at default top when `None`. Tracked so that
    /// consecutive requests with the same `before_layer` skip the
    /// remove + add_layer_before sweep entirely.
    ///
    /// Only meaningful when `needs_reset == false`. After a
    /// `move_all_layers` failure the layers can be split across positions,
    /// so this alone can't be trusted to decide whether a move is needed.
    current_before: Option<String>,
    /// Set when a previous `move_all_layers` failure left layers spread
    /// across positions. `assign_slots` re-moves unconditionally when this
    /// is set; the flag is cleared on a successful move.
    needs_reset: bool,
    /// True when the shared source currently holds non-empty data from a
    /// prior request. Lets us skip `set_geojson(empty)` on consecutive
    /// zero-overlay requests.
    has_data: bool,
}

pub(crate) fn render_static_with_overlays(
    renderer: &mut maplibre_native::ImageRenderer<maplibre_native::Static>,
    slots: &mut OverlaySlotPool,
    camera: &maplibre_native::CameraUpdate,
    overlays: &[StaticOverlay],
    before_layer: Option<&str>,
) -> Result<maplibre_native::Image, maplibre_native::RenderingError> {
    // `style()` borrows the renderer mutably, so wrap the slot-update phase
    // in its own scope. The borrow has to end before `render_static`.
    let registered_images = {
        let mut style = renderer.style();
        if let Err(err) = assign_slots(&mut style, slots, overlays, before_layer) {
            return Err(maplibre_native::RenderingError::Native(err.to_string()));
        }
        pin_image_ids(overlays)
    };
    let result = renderer.render_static(camera);
    if !registered_images.is_empty() {
        let mut style = renderer.style();
        for image_id in registered_images {
            style.remove_image(image_id);
        }
    }
    result
}

/// Install the shared overlay source on a freshly loaded style and return
/// a fresh pool with no layers allocated yet. Each renderer's first
/// overlay-bearing request will grow the layer set via `ensure_capacity`.
pub(crate) fn populate_static_slots(
    renderer: &mut maplibre_native::ImageRenderer<maplibre_native::Static>,
) -> Result<OverlaySlotPool, maplibre_native::StyleError> {
    let empty_fc = parse_geojson_str(EMPTY_FEATURE_COLLECTION)?;
    {
        let mut style = renderer.style();
        let mut source = maplibre_native::GeoJsonSource::new(OVERLAY_SOURCE_ID);
        source.set_geojson(&empty_fc);
        style.add_source(source)?;
    }
    Ok(OverlaySlotPool {
        allocated: 0,
        max_size: MAX_OVERLAYS,
        empty_fc,
        current_before: None,
        needs_reset: false,
        has_data: false,
    })
}

impl OverlaySlotPool {
    /// Grow the pool so it has at least `needed` layer triples (capped at
    /// `max_size`). New layers are added at the current overlay band
    /// position (`current_before` when set, default top otherwise) so they
    /// sit at the right Z-order relative to existing slots without a
    /// follow-up move when `before_layer` is unchanged.
    fn ensure_capacity(
        &mut self,
        style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
        needed: usize,
    ) -> Result<(), maplibre_native::StyleError> {
        let target = needed.min(self.max_size);
        if self.allocated >= target {
            return Ok(());
        }
        while self.allocated < target {
            let idx = self.allocated;
            // Transactional: if any of the 3 add_layer calls fails, unwind
            // the layers we did add for this slot so the next call doesn't
            // hit a stale `biei-overlay-{idx}-fill` collision.
            match install_slot_layers(style, idx, self.current_before.as_deref()) {
                Ok(()) => self.allocated += 1,
                Err(err) => {
                    rollback_slot_layers(style, idx);
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}

/// Add slot `idx`'s layers (fill / line / circle / symbol) to the style at the
/// current overlay band. Layers are added in Fill → Line → Circle → Symbol
/// order so the simplestyle within-overlay Z convention
/// (Fill < Line < Circle < Symbol)
/// holds within a slot, and so each new slot's layers land above the
/// previous slot's (when added at `before` or default top, later additions
/// end up closer to `before` / on top).
fn install_slot_layers(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    idx: usize,
    before: Option<&str>,
) -> Result<(), maplibre_native::StyleError> {
    for json in [
        slot_fill_layer_json(idx),
        slot_line_layer_json(idx),
        slot_circle_layer_json(idx),
        slot_symbol_layer_json(idx),
    ] {
        let layer = maplibre_native::AnyLayer::from_json_str(&json)?;
        match before {
            Some(b) => {
                style.add_layer_before(layer, b)?;
            }
            None => {
                style.add_layer(layer)?;
            }
        }
    }
    Ok(())
}

/// Best-effort cleanup of any layers that may have been added for slot
/// `idx` before a failed install. `remove_layer` is a no-op for ids that
/// were never added.
fn rollback_slot_layers(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    idx: usize,
) {
    for id in slot_layer_ids(idx) {
        let _ = style.remove_layer(&id);
    }
}

fn assign_slots(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    slots: &mut OverlaySlotPool,
    overlays: &[StaticOverlay],
    before_layer: Option<&str>,
) -> Result<(), maplibre_native::StyleError> {
    // Defense in depth: ingress already caps overlays at MAX_OVERLAYS, but
    // we refuse extras rather than silently truncate.
    if overlays.len() > slots.max_size {
        return Err(maplibre_native::StyleError::Native(format!(
            "overlay count {} exceeds pool max {}",
            overlays.len(),
            slots.max_size
        )));
    }

    let needed_slots = overlay_slot_count(overlays);
    slots.ensure_capacity(style, needed_slots)?;

    if overlays.is_empty() {
        // Nothing to render. Layers stay where they are — empty filters
        // produce no draw calls, so position doesn't matter for output.
        // Reset the source data only if it isn't already empty.
        if slots.has_data {
            set_source_geojson(style, &slots.empty_fc)?;
            slots.has_data = false;
        }
        return Ok(());
    }

    // We need a move when either:
    //   - the previous position is degenerate (needs_reset), or
    //   - the target position differs from the current one.
    // Growth alone never needs a move: `ensure_capacity` adds new layers
    // at `current_before`, so when `current_before == before_layer` the
    // new layers are already at the right position; when they differ,
    // `position_changed` triggers the move below.
    if slots.needs_reset || slots.current_before.as_deref() != before_layer {
        move_all_layers(style, slots, before_layer)?;
    }

    let features = build_union_features(overlays);
    let fc_json =
        serde_json::json!({"type": "FeatureCollection", "features": features}).to_string();
    let fc = parse_geojson_str(&fc_json)?;
    set_source_geojson(style, &fc)?;
    register_pin_images(style, overlays)?;
    slots.has_data = true;
    Ok(())
}

fn set_source_geojson(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    fc: &maplibre_native::GeoJson,
) -> Result<(), maplibre_native::StyleError> {
    let Some(maplibre_native::SourceRefMut::GeoJson(mut source)) =
        style.source_mut(OVERLAY_SOURCE_ID)
    else {
        return Err(maplibre_native::StyleError::Native(format!(
            "overlay source `{OVERLAY_SOURCE_ID}` is missing or not a GeoJsonSource"
        )));
    };
    source.set_geojson(fc);
    Ok(())
}

/// Reposition every allocated overlay layer to the new `before` (or
/// default top) in a single sweep. Uses `remove_layer` (returns the
/// original `AnyLayer`, preserving compiled paint/layout expressions) and
/// `add_layer*` to keep layer objects intact.
///
/// Phase 1 detaches all layers up front so Phase 2's re-inserts cannot
/// collide on layer id. Phase 2 reinserts in URL order, so later
/// additions (= higher slot index) end up closer to `before` (= higher
/// Z), preserving the slot-major Z-order.
///
/// **`before_layer` not in style**: maplibre-native-rs documents that
/// `add_layer_before(layer, missing_id)` appends the layer to the top of
/// the style rather than returning an error. We rely on that fallback —
/// overlays land at default top when `before_layer` is unknown — instead
/// of validating up front. mbgl exposes no `has_layer(id)` predicate, so
/// strict validation would require an upstream addition.
///
/// **Atomicity**: on exit (Ok or Err) every allocated layer is back in the
/// style. On Err the failing `add_layer*` will have consumed its Layer
/// object (mbgl drops it), so we reconstruct that layer from its JSON
/// template and add it at default top; any not-yet-reinserted layers are
/// added at default top to avoid leaks. `current_before` is cleared and
/// `needs_reset` is set so the next call re-moves regardless of which
/// `before_layer` is requested.
fn move_all_layers(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    slots: &mut OverlaySlotPool,
    before: Option<&str>,
) -> Result<(), maplibre_native::StyleError> {
    // Phase 1: detach all layers up front, in slot-major then
    // Fill/Line/Circle/Symbol order. Phase 2 re-adds in the same order so URL
    // order is preserved (each subsequent add lands closer to `before`).
    let mut taken: Vec<((usize, usize), maplibre_native::AnyLayer)> =
        Vec::with_capacity(slots.allocated * 4);
    for slot_idx in 0..slots.allocated {
        for (within_idx, id) in slot_layer_ids(slot_idx).iter().enumerate() {
            if let Some(layer) = style.remove_layer(id) {
                taken.push(((slot_idx, within_idx), layer));
            }
        }
    }

    // Phase 2: reinsert with degenerate-state recovery on any failure.
    let mut iter = taken.into_iter();
    while let Some(((slot_idx, within_idx), layer)) = iter.next() {
        let result = match before {
            Some(b) => style.add_layer_before(layer, b),
            None => style.add_layer(layer),
        };
        if let Err(err) = result {
            recover_failed_move_all(style, (slot_idx, within_idx), iter);
            slots.current_before = None;
            slots.needs_reset = true;
            return Err(err);
        }
    }

    slots.current_before = before.map(str::to_string);
    slots.needs_reset = false;
    Ok(())
}

/// Best-effort recovery from a mid-move `add_layer*` failure: rebuild the
/// consumed layer from its JSON template (slot index + within-slot index)
/// and add at default top, then add any not-yet-reinserted Layer objects
/// at default top. After this, every layer is back in the style at a
/// degenerate position; the caller sets `needs_reset = true` so the next
/// move recomputes from a clean baseline.
fn recover_failed_move_all(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    consumed: (usize, usize),
    remaining: impl Iterator<Item = ((usize, usize), maplibre_native::AnyLayer)>,
) {
    let (slot_idx, within_idx) = consumed;
    if let Some(json) = slot_layer_json(slot_idx, within_idx)
        && let Ok(reconstructed) = maplibre_native::AnyLayer::from_json_str(&json)
    {
        let _ = style.add_layer(reconstructed);
    }
    for (_, layer) in remaining {
        let _ = style.add_layer(layer);
    }
}

/// Return the JSON template for the (`slot_idx`, `within_idx`) layer where
/// `within_idx` is 0 = fill, 1 = line, 2 = circle, 3 = symbol. Returns `None` for
/// out-of-range `within_idx` (programmer bug — the recovery path treats
/// `None` as "give up on reconstructing this layer").
fn slot_layer_json(slot_idx: usize, within_idx: usize) -> Option<String> {
    match within_idx {
        0 => Some(slot_fill_layer_json(slot_idx)),
        1 => Some(slot_line_layer_json(slot_idx)),
        2 => Some(slot_circle_layer_json(slot_idx)),
        3 => Some(slot_symbol_layer_json(slot_idx)),
        _ => None,
    }
}

fn parse_geojson_str(json: &str) -> Result<maplibre_native::GeoJson, maplibre_native::StyleError> {
    json.parse::<maplibre_native::GeoJson>()
        .map_err(|err| maplibre_native::StyleError::Native(err.to_string()))
}

/// Build a parsed GeoJson holding the union of all overlays' geometries,
/// for callers that need an `mln::GeoJson` outside the assign_slots write
/// path (notably `camera_for_geojson` used by `Positioning::Auto`). No
/// `_overlay_idx` is injected here since the camera fit only consults
/// geometries.
pub(crate) fn build_overlay_geojson(
    overlays: &[StaticOverlay],
) -> Result<maplibre_native::GeoJson, maplibre_native::StyleError> {
    let features: Vec<serde_json::Value> = overlays.iter().flat_map(overlay_to_features).collect();
    let fc_json =
        serde_json::json!({"type": "FeatureCollection", "features": features}).to_string();
    parse_geojson_str(&fc_json)
}

/// Build the union of all overlays' Features, with an `_overlay_idx`
/// property injected into each Feature so per-slot layer filters can
/// scope to one render slot. Consecutive stroke-only paths share a slot;
/// fill-capable paths and GeoJSON overlays remain one slot each.
fn build_union_features(overlays: &[StaticOverlay]) -> Vec<serde_json::Value> {
    let mut all = Vec::new();
    let mut slot_idx = 0usize;
    let mut idx = 0usize;
    while idx < overlays.len() {
        let run_len = stroke_only_path_run_len(&overlays[idx..]);
        if run_len > 0 {
            for overlay in &overlays[idx..idx + run_len] {
                let mut feats = overlay_to_features(overlay);
                inject_overlay_idx(&mut feats, slot_idx);
                all.append(&mut feats);
            }
            idx += run_len;
            slot_idx += 1;
            continue;
        }

        let mut feats = overlay_to_features(&overlays[idx]);
        inject_overlay_idx(&mut feats, slot_idx);
        all.append(&mut feats);
        idx += 1;
        slot_idx += 1;
    }
    all
}

fn overlay_slot_count(overlays: &[StaticOverlay]) -> usize {
    let mut count = 0usize;
    let mut idx = 0usize;
    while idx < overlays.len() {
        let run_len = stroke_only_path_run_len(&overlays[idx..]);
        idx += run_len.max(1);
        count += 1;
    }
    count
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
fn inject_overlay_idx(features: &mut [serde_json::Value], idx: usize) {
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

fn path_features(path: &PathOverlay) -> Vec<serde_json::Value> {
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

fn geojson_features(overlay: &GeoJsonOverlay) -> Vec<serde_json::Value> {
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

fn pin_features(pin: &PinOverlay) -> Vec<serde_json::Value> {
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

fn slot_layer_ids(idx: usize) -> [String; 4] {
    [
        slot_fill_layer_id(idx),
        slot_line_layer_id(idx),
        slot_circle_layer_id(idx),
        slot_symbol_layer_id(idx),
    ]
}

fn slot_fill_layer_id(idx: usize) -> String {
    format!("biei-overlay-{idx}-fill")
}

fn slot_line_layer_id(idx: usize) -> String {
    format!("biei-overlay-{idx}-line")
}

fn slot_circle_layer_id(idx: usize) -> String {
    format!("biei-overlay-{idx}-circle")
}

fn slot_symbol_layer_id(idx: usize) -> String {
    format!("biei-overlay-{idx}-symbol")
}

fn slot_fill_layer_json(idx: usize) -> String {
    serde_json::json!({
        "id": slot_fill_layer_id(idx),
        "type": "fill",
        "source": OVERLAY_SOURCE_ID,
        "filter": ["all",
            ["==", ["geometry-type"], "Polygon"],
            ["==", ["get", OVERLAY_IDX_PROPERTY], idx]
        ],
        "paint": {
            "fill-color":   ["coalesce", ["to-color", ["get", "fill"]], "#555555"],
            "fill-opacity": ["coalesce", ["number",   ["get", "fill-opacity"]], 0.6],
        }
    })
    .to_string()
}

fn slot_line_layer_json(idx: usize) -> String {
    serde_json::json!({
        "id": slot_line_layer_id(idx),
        "type": "line",
        "source": OVERLAY_SOURCE_ID,
        "filter": ["all",
            ["match", ["geometry-type"], ["LineString", "Polygon"], true, false],
            ["==", ["get", OVERLAY_IDX_PROPERTY], idx]
        ],
        "paint": {
            "line-color":   ["coalesce", ["to-color", ["get", "stroke"]], "#555555"],
            "line-width":   ["coalesce", ["number",   ["get", "stroke-width"]], 2],
            "line-opacity": ["coalesce", ["number",   ["get", "stroke-opacity"]], 1.0],
        },
        "layout": {
            "line-cap":  "round",
            "line-join": "miter",
            "line-miter-limit": 0,
        }
    })
    .to_string()
}

fn slot_circle_layer_json(idx: usize) -> String {
    serde_json::json!({
        "id": slot_circle_layer_id(idx),
        "type": "circle",
        "source": OVERLAY_SOURCE_ID,
        "filter": ["all",
            ["==", ["geometry-type"], "Point"],
            ["==", ["get", OVERLAY_IDX_PROPERTY], idx],
            ["!=", ["get", PIN_KIND_PROPERTY], PIN_KIND_VALUE]
        ],
        "paint": {
            "circle-color":   ["coalesce", ["to-color", ["get", "marker-color"]], "#7e7e7e"],
            "circle-radius":  ["match", ["get", "marker-size"],
                                "small", 5,
                                "large", 10,
                                7],
            "circle-stroke-color": "#ffffff",
            "circle-stroke-width": 1,
            "circle-opacity": ["coalesce", ["number", ["get", "marker-opacity"]], 1.0],
        }
    })
    .to_string()
}

fn slot_symbol_layer_json(idx: usize) -> String {
    serde_json::json!({
        "id": slot_symbol_layer_id(idx),
        "type": "symbol",
        "source": OVERLAY_SOURCE_ID,
        "filter": ["all",
            ["==", ["geometry-type"], "Point"],
            ["==", ["get", OVERLAY_IDX_PROPERTY], idx],
            ["==", ["get", PIN_KIND_PROPERTY], PIN_KIND_VALUE]
        ],
        "layout": {
            "icon-image": ["get", PIN_IMAGE_PROPERTY],
            "icon-anchor": "bottom",
            "icon-offset": ["get", PIN_OFFSET_PROPERTY],
            "icon-allow-overlap": true,
            "icon-ignore-placement": true,
        }
    })
    .to_string()
}

fn coordinates_json(coordinates: &[LngLat]) -> Vec<serde_json::Value> {
    coordinates
        .iter()
        .map(|p| serde_json::json!([p.lon, p.lat]))
        .collect()
}

fn path_stroke_properties(path: &PathOverlay) -> serde_json::Value {
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

fn path_fill_properties(path: &PathOverlay) -> serde_json::Value {
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
fn css_color(hex: &str) -> String {
    format!("#{hex}")
}

#[cfg(test)]
mod tests {
    use super::pin::{label_color_for_pin, render_pin_image};
    use super::*;
    use crate::types::PinSize;

    fn pt(lon: f64, lat: f64) -> LngLat {
        LngLat { lon, lat }
    }

    fn bare_path(coordinates: Vec<LngLat>) -> PathOverlay {
        PathOverlay {
            stroke_width: None,
            stroke_color: None,
            stroke_opacity: None,
            fill_color: None,
            fill_opacity: None,
            coordinates,
        }
    }

    fn pin(label: Option<&str>) -> PinOverlay {
        PinOverlay {
            size: PinSize::Small,
            label: label.map(str::to_string),
            color: "9ed4bd".to_string(),
            coordinate: pt(139.0, 35.0),
        }
    }

    #[test]
    fn css_color_prepends_hash() {
        assert_eq!(css_color("f44"), "#f44");
        assert_eq!(css_color("00ffcc"), "#00ffcc");
    }

    #[test]
    fn path_stroke_properties_omit_unset_fields() {
        let v = path_stroke_properties(&bare_path(vec![]));
        assert!(v.as_object().expect("object").is_empty());
    }

    #[test]
    fn path_stroke_properties_include_set_fields_with_hash_prefixed_color() {
        let path = PathOverlay {
            stroke_width: Some(3.0),
            stroke_color: Some("f44".to_string()),
            stroke_opacity: Some(0.5),
            fill_color: None,
            fill_opacity: None,
            coordinates: vec![],
        };
        let v = path_stroke_properties(&path);
        assert_eq!(v.get("stroke").and_then(|v| v.as_str()), Some("#f44"));
        assert_eq!(v.get("stroke-width").and_then(|v| v.as_f64()), Some(3.0));
        assert_eq!(v.get("stroke-opacity").and_then(|v| v.as_f64()), Some(0.5));
    }

    #[test]
    fn path_fill_properties_omit_unset_fields() {
        let v = path_fill_properties(&bare_path(vec![]));
        assert!(v.as_object().expect("object").is_empty());
    }

    #[test]
    fn path_fill_properties_include_set_fields_with_hash_prefixed_color() {
        let path = PathOverlay {
            stroke_width: None,
            stroke_color: None,
            stroke_opacity: None,
            fill_color: Some("00ffcc".to_string()),
            // 0.5 is exactly representable as f32, so the f32→f64 round-
            // trip is lossless and we can compare with assert_eq!.
            fill_opacity: Some(0.5),
            coordinates: vec![],
        };
        let v = path_fill_properties(&path);
        assert_eq!(v.get("fill").and_then(|v| v.as_str()), Some("#00ffcc"));
        assert_eq!(v.get("fill-opacity").and_then(|v| v.as_f64()), Some(0.5));
    }

    #[test]
    fn coordinates_json_preserves_input_order() {
        let v = coordinates_json(&[pt(0.0, 0.0), pt(1.0, 2.0), pt(-3.0, 4.0)]);
        assert_eq!(v.len(), 3);
        assert_eq!(v[0], serde_json::json!([0.0, 0.0]));
        assert_eq!(v[1], serde_json::json!([1.0, 2.0]));
        assert_eq!(v[2], serde_json::json!([-3.0, 4.0]));
    }

    #[test]
    fn path_features_emits_only_linestring_when_no_fill() {
        let path = PathOverlay {
            stroke_width: Some(5.0),
            stroke_color: Some("f44".to_string()),
            stroke_opacity: None,
            fill_color: None,
            fill_opacity: None,
            coordinates: vec![pt(0.0, 0.0), pt(1.0, 1.0)],
        };
        let features = path_features(&path);
        assert_eq!(features.len(), 1);
        assert_eq!(features[0]["geometry"]["type"], "LineString");
        assert_eq!(features[0]["properties"]["stroke"], "#f44");
    }

    #[test]
    fn path_features_emits_polygon_when_fill_and_three_or_more_coords() {
        let path = PathOverlay {
            stroke_width: None,
            stroke_color: None,
            stroke_opacity: None,
            fill_color: Some("0c8".to_string()),
            fill_opacity: Some(0.5),
            coordinates: vec![pt(0.0, 0.0), pt(1.0, 0.0), pt(0.5, 1.0)],
        };
        let features = path_features(&path);
        assert_eq!(features.len(), 2);
        assert_eq!(features[1]["geometry"]["type"], "Polygon");
        assert_eq!(features[1]["properties"]["fill"], "#0c8");
        // Polygon outer ring must close to form valid GeoJSON.
        let ring = features[1]["geometry"]["coordinates"][0]
            .as_array()
            .expect("polygon outer ring");
        assert_eq!(ring.first(), ring.last(), "polygon ring should be closed");
    }

    #[test]
    fn path_features_keeps_already_closed_ring_intact() {
        let path = PathOverlay {
            stroke_width: None,
            stroke_color: None,
            stroke_opacity: None,
            fill_color: Some("0c8".to_string()),
            fill_opacity: None,
            // First == last already.
            coordinates: vec![pt(0.0, 0.0), pt(1.0, 0.0), pt(0.5, 1.0), pt(0.0, 0.0)],
        };
        let features = path_features(&path);
        let ring = features[1]["geometry"]["coordinates"][0]
            .as_array()
            .expect("polygon outer ring");
        assert_eq!(ring.len(), 4, "no spurious closing vertex appended");
    }

    #[test]
    fn path_features_skips_polygon_when_fewer_than_three_coords() {
        let path = PathOverlay {
            stroke_width: None,
            stroke_color: None,
            stroke_opacity: None,
            fill_color: Some("0c8".to_string()),
            fill_opacity: None,
            coordinates: vec![pt(0.0, 0.0), pt(1.0, 1.0)],
        };
        let features = path_features(&path);
        // fill_color set but only 2 coords: only LineString, no Polygon.
        assert_eq!(features.len(), 1);
        assert_eq!(features[0]["geometry"]["type"], "LineString");
    }

    #[test]
    fn geojson_features_extracts_from_feature_collection() {
        let overlay = GeoJsonOverlay {
            feature_collection: serde_json::json!({
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [0, 0]}},
                    {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [1, 1]}}
                ]
            }),
        };
        let features = geojson_features(&overlay);
        assert_eq!(features.len(), 2);
    }

    #[test]
    fn geojson_features_wraps_single_feature() {
        let overlay = GeoJsonOverlay {
            feature_collection: serde_json::json!({
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "Point", "coordinates": [0, 0]}
            }),
        };
        let features = geojson_features(&overlay);
        assert_eq!(features.len(), 1);
        assert_eq!(features[0]["type"], "Feature");
    }

    #[test]
    fn pin_features_emit_symbol_marker_point() {
        let features = pin_features(&pin(Some("a")));

        assert_eq!(features.len(), 1);
        assert_eq!(features[0]["geometry"]["type"], "Point");
        assert_eq!(features[0]["properties"][PIN_KIND_PROPERTY], PIN_KIND_VALUE);
        assert_eq!(
            features[0]["properties"][PIN_IMAGE_PROPERTY],
            "biei-pin-s-a-9ed4bd-x2"
        );
        assert_eq!(
            features[0]["properties"][PIN_OFFSET_PROPERTY],
            serde_json::json!([0.0, pin_icon_offset_y(PinSize::Small)])
        );
    }

    #[test]
    fn inject_overlay_idx_stamps_index_on_existing_properties_object() {
        let mut features = vec![serde_json::json!({
            "type": "Feature",
            "properties": {"stroke": "#f44"},
            "geometry": {"type": "Point", "coordinates": [0, 0]}
        })];
        inject_overlay_idx(&mut features, 3);
        assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 3);
        // Pre-existing properties are preserved.
        assert_eq!(features[0]["properties"]["stroke"], "#f44");
    }

    #[test]
    fn inject_overlay_idx_creates_properties_when_missing_or_null() {
        let mut features = vec![
            // Missing properties.
            serde_json::json!({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0, 0]}
            }),
            // Null properties (valid GeoJSON).
            serde_json::json!({
                "type": "Feature",
                "properties": null,
                "geometry": {"type": "Point", "coordinates": [0, 0]}
            }),
        ];
        inject_overlay_idx(&mut features, 7);
        for f in &features {
            assert_eq!(f["properties"]["__biei_overlay_idx"], 7);
        }
    }

    #[test]
    fn build_union_features_concatenates_with_overlay_idx_per_feature() {
        let path = PathOverlay {
            stroke_width: None,
            stroke_color: Some("f44".to_string()),
            stroke_opacity: None,
            fill_color: None,
            fill_opacity: None,
            coordinates: vec![pt(0.0, 0.0), pt(1.0, 1.0)],
        };
        let geojson = GeoJsonOverlay {
            feature_collection: serde_json::json!({
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [2, 2]}},
                    {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [3, 3]}}
                ]
            }),
        };
        let overlays = vec![StaticOverlay::Path(path), StaticOverlay::GeoJson(geojson)];
        let features = build_union_features(&overlays);

        // Path emits 1 feature (LineString, no fill), GeoJSON emits 2.
        assert_eq!(features.len(), 3);
        assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 0);
        assert_eq!(features[0]["geometry"]["type"], "LineString");
        assert_eq!(features[1]["properties"]["__biei_overlay_idx"], 1);
        assert_eq!(features[2]["properties"]["__biei_overlay_idx"], 1);
    }

    #[test]
    fn consecutive_stroke_only_paths_share_one_slot() {
        let a = StaticOverlay::Path(bare_path(vec![pt(0.0, 0.0), pt(1.0, 1.0)]));
        let b = StaticOverlay::Path(bare_path(vec![pt(2.0, 2.0), pt(3.0, 3.0)]));
        let features = build_union_features(&[a, b]);

        assert_eq!(
            overlay_slot_count(&[
                StaticOverlay::Path(bare_path(vec![pt(0.0, 0.0), pt(1.0, 1.0)])),
                StaticOverlay::Path(bare_path(vec![pt(2.0, 2.0), pt(3.0, 3.0)])),
            ]),
            1
        );
        assert_eq!(features.len(), 2);
        assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 0);
        assert_eq!(features[1]["properties"]["__biei_overlay_idx"], 0);
    }

    #[test]
    fn fill_paths_keep_slot_boundaries_to_preserve_z_order() {
        let stroke_only = StaticOverlay::Path(bare_path(vec![pt(0.0, 0.0), pt(1.0, 1.0)]));
        let filled = StaticOverlay::Path(PathOverlay {
            stroke_width: None,
            stroke_color: None,
            stroke_opacity: None,
            fill_color: Some("0c8".to_string()),
            fill_opacity: None,
            coordinates: vec![pt(0.0, 0.0), pt(1.0, 0.0), pt(0.5, 1.0)],
        });
        let later_stroke = StaticOverlay::Path(bare_path(vec![pt(2.0, 2.0), pt(3.0, 3.0)]));
        let overlays = vec![stroke_only, filled, later_stroke];
        let features = build_union_features(&overlays);

        assert_eq!(overlay_slot_count(&overlays), 3);
        assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 0);
        assert_eq!(features[1]["properties"]["__biei_overlay_idx"], 1);
        assert_eq!(features[2]["properties"]["__biei_overlay_idx"], 1);
        assert_eq!(features[3]["properties"]["__biei_overlay_idx"], 2);
    }

    #[test]
    fn pin_breaks_path_run_and_keeps_own_slot() {
        let overlays = vec![
            StaticOverlay::Path(bare_path(vec![pt(0.0, 0.0), pt(1.0, 1.0)])),
            StaticOverlay::Pin(pin(None)),
            StaticOverlay::Path(bare_path(vec![pt(2.0, 2.0), pt(3.0, 3.0)])),
        ];
        let features = build_union_features(&overlays);

        assert_eq!(overlay_slot_count(&overlays), 3);
        assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 0);
        assert_eq!(features[1]["properties"]["__biei_overlay_idx"], 1);
        assert_eq!(features[1]["properties"][PIN_KIND_PROPERTY], PIN_KIND_VALUE);
        assert_eq!(features[2]["properties"]["__biei_overlay_idx"], 2);
    }

    #[test]
    fn slot_layer_ids_follow_indexed_naming() {
        let ids = slot_layer_ids(5);
        assert_eq!(ids[0], "biei-overlay-5-fill");
        assert_eq!(ids[1], "biei-overlay-5-line");
        assert_eq!(ids[2], "biei-overlay-5-circle");
        assert_eq!(ids[3], "biei-overlay-5-symbol");
    }

    #[test]
    fn slot_layer_jsons_parse_and_target_shared_source() {
        for (kind, json) in [
            ("fill", slot_fill_layer_json(7)),
            ("line", slot_line_layer_json(7)),
            ("circle", slot_circle_layer_json(7)),
            ("symbol", slot_symbol_layer_json(7)),
        ] {
            let v: serde_json::Value = serde_json::from_str(&json)
                .unwrap_or_else(|_| panic!("{kind} layer JSON must parse"));
            assert_eq!(v["type"].as_str(), Some(kind));
            // Every slot's layer references the SINGLE shared source.
            assert_eq!(v["source"].as_str(), Some(OVERLAY_SOURCE_ID));
            assert!(
                v["paint"].is_object() || v["layout"].is_object(),
                "{kind} layer needs paint or layout props"
            );
            assert_eq!(
                v["id"].as_str(),
                Some(format!("biei-overlay-7-{kind}").as_str())
            );
        }
    }

    #[test]
    fn slot_fill_layer_filter_scopes_to_polygon_and_overlay_idx() {
        let v: serde_json::Value = serde_json::from_str(&slot_fill_layer_json(2)).unwrap();
        // ["all", ["==", ["geometry-type"], "Polygon"], ["==", ["get", "__biei_overlay_idx"], 2]]
        let filter = &v["filter"];
        assert_eq!(filter[0], "all");
        assert_eq!(
            filter[1],
            serde_json::json!(["==", ["geometry-type"], "Polygon"])
        );
        assert_eq!(
            filter[2],
            serde_json::json!(["==", ["get", "__biei_overlay_idx"], 2])
        );
    }

    #[test]
    fn slot_line_layer_filter_accepts_linestring_and_polygon_for_overlay_idx() {
        let v: serde_json::Value = serde_json::from_str(&slot_line_layer_json(0)).unwrap();
        let filter = &v["filter"];
        assert_eq!(filter[0], "all");
        // geometry-type clause accepts LineString OR Polygon (the latter for
        // polygon stroke).
        assert_eq!(filter[1][0], "match");
        assert_eq!(filter[1][1][0], "geometry-type");
        assert_eq!(filter[1][2], serde_json::json!(["LineString", "Polygon"]));
        assert_eq!(
            filter[2],
            serde_json::json!(["==", ["get", "__biei_overlay_idx"], 0])
        );
        // DDS line-color reads `stroke`.
        let line_color = &v["paint"]["line-color"];
        assert_eq!(line_color[0], "coalesce");
        assert_eq!(line_color[1][0], "to-color");
        assert_eq!(line_color[1][1][1], "stroke");
    }

    #[test]
    fn slot_circle_layer_filter_scopes_to_point_and_overlay_idx() {
        let v: serde_json::Value = serde_json::from_str(&slot_circle_layer_json(4)).unwrap();
        let filter = &v["filter"];
        assert_eq!(filter[0], "all");
        assert_eq!(
            filter[1],
            serde_json::json!(["==", ["geometry-type"], "Point"])
        );
        assert_eq!(
            filter[2],
            serde_json::json!(["==", ["get", "__biei_overlay_idx"], 4])
        );
        assert_eq!(
            filter[3],
            serde_json::json!(["!=", ["get", PIN_KIND_PROPERTY], PIN_KIND_VALUE])
        );
        // DDS circle-color reads `marker-color`.
        let circle_color = &v["paint"]["circle-color"];
        assert_eq!(circle_color[0], "coalesce");
        assert_eq!(circle_color[1][1][1], "marker-color");
    }

    #[test]
    fn slot_layer_json_picks_the_right_template() {
        let fill = slot_layer_json(2, 0).expect("0 -> fill");
        let line = slot_layer_json(2, 1).expect("1 -> line");
        let circle = slot_layer_json(2, 2).expect("2 -> circle");
        let symbol = slot_layer_json(2, 3).expect("3 -> symbol");
        assert!(fill.contains("biei-overlay-2-fill"));
        assert!(line.contains("biei-overlay-2-line"));
        assert!(circle.contains("biei-overlay-2-circle"));
        assert!(symbol.contains("biei-overlay-2-symbol"));
        let fv: serde_json::Value = serde_json::from_str(&fill).unwrap();
        let lv: serde_json::Value = serde_json::from_str(&line).unwrap();
        let cv: serde_json::Value = serde_json::from_str(&circle).unwrap();
        let sv: serde_json::Value = serde_json::from_str(&symbol).unwrap();
        assert_eq!(fv["type"], "fill");
        assert_eq!(lv["type"], "line");
        assert_eq!(cv["type"], "circle");
        assert_eq!(sv["type"], "symbol");
        assert!(slot_layer_json(2, 4).is_none());
    }

    #[test]
    fn slot_symbol_layer_targets_pin_features() {
        let v: serde_json::Value = serde_json::from_str(&slot_symbol_layer_json(4)).unwrap();
        assert_eq!(v["type"], "symbol");
        assert_eq!(
            v["filter"],
            serde_json::json!([
                "all",
                ["==", ["geometry-type"], "Point"],
                ["==", ["get", OVERLAY_IDX_PROPERTY], 4],
                ["==", ["get", PIN_KIND_PROPERTY], PIN_KIND_VALUE]
            ])
        );
        assert_eq!(
            v["layout"]["icon-image"],
            serde_json::json!(["get", PIN_IMAGE_PROPERTY])
        );
        assert_eq!(
            v["layout"]["icon-offset"],
            serde_json::json!(["get", PIN_OFFSET_PROPERTY])
        );
    }

    #[test]
    fn render_pin_image_produces_rgba_bitmap() {
        let image = render_pin_image(&pin(None)).expect("pin image renders");
        assert_eq!(image.width(), 48);
        assert_eq!(image.height(), 56);
    }

    #[test]
    fn render_pin_image_draws_antialiased_label() {
        let mut pin = pin(Some("s"));
        pin.color = "4682b4".to_string();
        let image = render_pin_image(&pin).expect("pin image renders");
        let whiteish_pixels = image
            .to_rgba8()
            .pixels()
            .filter(|pixel| pixel[0] > 220 && pixel[1] > 220 && pixel[2] > 220 && pixel[3] > 0)
            .count();
        assert!(whiteish_pixels > 20);
    }

    #[test]
    fn render_pin_image_uses_dark_label_on_light_fill() {
        let mut pin = pin(Some("s"));
        pin.color = "ffff66".to_string();
        let image = render_pin_image(&pin).expect("pin image renders");
        let dark_pixels = image
            .to_rgba8()
            .pixels()
            .filter(|pixel| pixel[0] < 40 && pixel[1] < 40 && pixel[2] < 40 && pixel[3] > 0)
            .count();
        assert!(dark_pixels > 20);
    }

    #[test]
    fn label_color_matches_reference_luminance_threshold() {
        assert_eq!(label_color_for_pin((158, 158, 158)), [255, 255, 255]);
        assert_eq!(label_color_for_pin((161, 161, 161)), [0, 0, 0]);
        assert_eq!(label_color_for_pin((255, 117, 117)), [255, 255, 255]);
        assert_eq!(label_color_for_pin((255, 122, 122)), [0, 0, 0]);
        assert_eq!(label_color_for_pin((143, 147, 255)), [255, 255, 255]);
        assert_eq!(label_color_for_pin((150, 153, 255)), [0, 0, 0]);
    }
}
