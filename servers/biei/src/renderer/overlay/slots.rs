use biei_core::types::{MAX_STATIC_OVERLAYS, StaticOverlay};

use super::features::{build_overlay_geojson, overlay_slot_count};
use super::layers::{
    slot_circle_layer_json, slot_fill_layer_json, slot_layer_ids, slot_line_layer_json,
    slot_symbol_layer_json,
};
use super::pin::{pin_image_ids, register_pin_images};
use super::{OVERLAY_SOURCE_ID, parse_geojson_str};

const EMPTY_FEATURE_COLLECTION: &str = r#"{"type":"FeatureCollection","features":[]}"#;

/// Lazy, high-water-mark pool for overlay layers. The shared source is
/// created once at pool construction; layer sets are added on demand
/// and kept across requests. Grows up to `MAX_STATIC_OVERLAYS` (mirroring
/// the ingress hard limit, section 7.3.1) and never shrinks.
#[derive(Debug)]
pub(crate) struct OverlaySlotPool {
    /// Number of slot layer sets (fill/line/circle/symbol) currently in the
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
    /// Set when recovery from a `move_all_layers` failure could NOT restore
    /// every allocated layer, so the style is missing overlay layers that a
    /// re-move cannot recreate. Unlike `needs_reset` (positional only), this
    /// requires the backend to rebuild the whole static renderer before the
    /// next render, otherwise a later request would silently return an image
    /// missing an overlay class.
    needs_rebuild: bool,
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
    prepared_geojson: Option<&maplibre_native::GeoJson>,
    before_layer: Option<&str>,
) -> Result<maplibre_native::Image, maplibre_native::RenderingError> {
    // `style()` borrows the renderer mutably, so wrap the slot-update phase
    // in its own scope. The borrow has to end before `render_static`.
    let registered_images = {
        let mut style = renderer.style();
        if let Err(err) = assign_slots(&mut style, slots, overlays, prepared_geojson, before_layer)
        {
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
        max_size: MAX_STATIC_OVERLAYS,
        empty_fc,
        current_before: None,
        needs_reset: false,
        needs_rebuild: false,
        has_data: false,
    })
}

impl OverlaySlotPool {
    /// Whether the pool's layer topology is known-corrupt (a recovery could not
    /// restore every allocated layer) and the backend must rebuild the static
    /// renderer before the next render rather than reuse this pool.
    pub(crate) fn needs_rebuild(&self) -> bool {
        self.needs_rebuild
    }
}

impl OverlaySlotPool {
    /// Grow the pool so it has at least `needed` layer sets (capped at
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
    prepared_geojson: Option<&maplibre_native::GeoJson>,
    before_layer: Option<&str>,
) -> Result<(), maplibre_native::StyleError> {
    // Defense in depth: ingress already caps overlays at MAX_STATIC_OVERLAYS,
    // but we refuse extras rather than silently truncate.
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

    if let Some(fc) = prepared_geojson {
        set_source_geojson(style, fc)?;
    } else {
        let fc = build_overlay_geojson(overlays)?;
        set_source_geojson(style, &fc)?;
    }
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
/// **Atomicity**: on Err the failing `add_layer*` will have consumed its Layer
/// object (mbgl drops it), so we reconstruct that layer from its JSON template
/// and add it at default top; any not-yet-reinserted layers are added at default
/// top to avoid leaks. `current_before` is cleared and `needs_reset` is set so
/// the next call re-moves regardless of which `before_layer` is requested. If
/// recovery itself cannot restore every layer, `needs_rebuild` is set so the
/// backend rebuilds the renderer rather than trusting a re-move to recreate a
/// now-missing layer.
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
            let restored = recover_failed_move_all(style, (slot_idx, within_idx), iter);
            slots.current_before = None;
            slots.needs_reset = true;
            // If recovery could not put every layer back, a re-move cannot
            // recreate the missing layer; the renderer must be rebuilt before
            // the next render so a later request can't return an image missing
            // an overlay class.
            if !restored {
                slots.needs_rebuild = true;
            }
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
/// at default top. After this, every layer is (ideally) back in the style at
/// a degenerate position; the caller sets `needs_reset = true` so the next
/// move recomputes from a clean baseline.
///
/// Returns `true` only when every layer was restored. A `false` return means a
/// layer is now missing (reconstruction or re-`add_layer` failed), which a
/// re-move cannot repair — the caller must rebuild the renderer instead.
#[must_use]
fn recover_failed_move_all(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    consumed: (usize, usize),
    remaining: impl Iterator<Item = ((usize, usize), maplibre_native::AnyLayer)>,
) -> bool {
    let (slot_idx, within_idx) = consumed;
    let mut all_restored = true;
    match slot_layer_json(slot_idx, within_idx) {
        Some(json) => match maplibre_native::AnyLayer::from_json_str(&json) {
            Ok(reconstructed) => all_restored &= style.add_layer(reconstructed).is_ok(),
            Err(_) => all_restored = false,
        },
        None => all_restored = false,
    }
    for (_, layer) in remaining {
        all_restored &= style.add_layer(layer).is_ok();
    }
    all_restored
}

/// Return the JSON template for the (`slot_idx`, `within_idx`) layer where
/// `within_idx` is 0 = fill, 1 = line, 2 = circle, 3 = symbol. Returns `None` for
/// out-of-range `within_idx` (programmer bug — the recovery path treats
/// `None` as "give up on reconstructing this layer").
pub(super) fn slot_layer_json(slot_idx: usize, within_idx: usize) -> Option<String> {
    match within_idx {
        0 => Some(slot_fill_layer_json(slot_idx)),
        1 => Some(slot_line_layer_json(slot_idx)),
        2 => Some(slot_circle_layer_json(slot_idx)),
        3 => Some(slot_symbol_layer_json(slot_idx)),
        _ => None,
    }
}
