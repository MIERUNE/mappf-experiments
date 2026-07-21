//! Request-local `addlayer` installation for static renders.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::renderer::overlay::{OverlaySlotPool, render_static_with_overlays};
use biei_core::types::{AddLayer, AddLayerSource, StaticOverlay, TaskId};

/// Wrap `render_static_with_overlays` with optional request-local
/// `addlayer` install / remove. The addlayer is inserted before overlay
/// slots reposition themselves, so the slot pool's later
/// `assign_slots`/`move_all_layers` finds it in the style and the
/// overlays end up above it within the same Z band.
///
/// On any exit path (success or failure) the addlayer layer is removed
/// before returning, since its id is derived from `task_id` and reusing it
/// on the next request would collide with the lingering installation.
/// Request-local sources use stable ids and are kept in a small worker-local
/// cache; without a referencing layer they do not draw anything.
#[allow(clippy::too_many_arguments)]
pub(super) fn render_static_with_overlays_and_addlayer(
    renderer: &mut maplibre_native::ImageRenderer<maplibre_native::Static>,
    slots: &mut OverlaySlotPool,
    addlayer_sources: &mut AddLayerSourceCache,
    camera: &maplibre_native::CameraUpdate,
    overlays: &[StaticOverlay],
    prepared_overlay_geojson: Option<&maplibre_native::GeoJson>,
    before_layer: Option<&str>,
    addlayer: Option<&AddLayer>,
    task_id: TaskId,
) -> Result<(maplibre_native::Image, Option<Duration>), maplibre_native::RenderingError> {
    let installed_addlayer = if let Some(layer) = addlayer {
        let mut style = renderer.style();
        match install_addlayer(&mut style, addlayer_sources, layer, before_layer, task_id) {
            Ok(installed) => Some(installed),
            Err(e) => return Err(maplibre_native::RenderingError::Native(e.to_string())),
        }
    } else {
        None
    };
    let result = render_static_with_overlays(
        renderer,
        slots,
        camera,
        overlays,
        prepared_overlay_geojson,
        before_layer,
    );
    if let Some(installed) = &installed_addlayer {
        let mut style = renderer.style();
        remove_addlayer(&mut style, installed);
    }
    let source_setup_duration = installed_addlayer
        .as_ref()
        .and_then(|installed| installed.source_setup_duration);
    result.map(|image| (image, source_setup_duration))
}

struct InstalledAddLayer {
    layer_id: String,
    source_setup_duration: Option<Duration>,
}

const ADDLAYER_SOURCE_CACHE_CAPACITY: usize = 64;

pub(super) struct AddLayerSourceCache {
    entries: HashMap<String, AddLayerSource>,
    lru: VecDeque<String>,
    capacity: usize,
}

impl AddLayerSourceCache {
    pub(super) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            capacity: ADDLAYER_SOURCE_CACHE_CAPACITY,
        }
    }

    fn ensure(
        &mut self,
        style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
        source: &AddLayerSource,
    ) -> Result<(String, bool), maplibre_native::StyleError> {
        if let Some(id) = self
            .entries
            .iter()
            .find_map(|(id, cached)| (cached == source).then(|| id.clone()))
        {
            self.touch(&id);
            return Ok((id, false));
        }
        while self.entries.len() >= self.capacity
            && let Some(evicted) = self.lru.pop_front()
        {
            self.entries.remove(&evicted);
            style.remove_source(&evicted);
        }
        let base_id = source.stable_source_id();
        let id = self.vacant_id(&base_id);
        let any_source = maplibre_native::AnySource::from_json_str(&id, &source.json)?;
        style.add_source(any_source)?;
        self.entries.insert(id.clone(), source.clone());
        self.lru.push_back(id.clone());
        Ok((id, true))
    }

    fn remove_if_present(
        &mut self,
        style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
        id: &str,
    ) {
        if self.entries.remove(id).is_none() {
            return;
        }
        self.lru.retain(|cached| cached != id);
        style.remove_source(id);
    }

    fn touch(&mut self, id: &str) {
        self.lru.retain(|cached| cached != id);
        self.lru.push_back(id.to_string());
    }

    fn vacant_id(&self, base_id: &str) -> String {
        if !self.entries.contains_key(base_id) {
            return base_id.to_owned();
        }
        (1_u32..)
            .map(|suffix| format!("{base_id}_{suffix}"))
            .find(|candidate| !self.entries.contains_key(candidate))
            .expect("bounded addlayer source cache always has a vacant suffix")
    }
}

/// Install the request-local `addlayer` onto the active style and return
/// the biei-internal layer id that needs to be removed after rendering.
/// The user-supplied `id` from the addlayer JSON is rewritten to a
/// `__biei_addlayer_{task_id}` namespace to keep biei-managed layers
/// distinct from arbitrary user-supplied ids — mbgl's style throws on
/// duplicate ids, so this also prevents accidental collision with the
/// base style or with previously-installed biei slots.
///
/// Placement: addlayer sits at the bottom of the biei-managed band. When
/// `before_layer={X}` is set, the layer is inserted before X (matching
/// `before_layer`'s semantics for overlays). Otherwise it lands at the
/// top of the base style, where the overlay slot pool will later add its
/// own layers above it.
fn install_addlayer(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    addlayer_sources: &mut AddLayerSourceCache,
    addlayer: &AddLayer,
    before_layer: Option<&str>,
    task_id: TaskId,
) -> Result<InstalledAddLayer, maplibre_native::StyleError> {
    let internal_id = format!("__biei_addlayer_{task_id}");
    let mut newly_added_source_id = None;
    let mut source_setup_duration = None;
    let source_id = match &addlayer.source {
        Some(source) => {
            let setup_started_at = Instant::now();
            let (id, newly_added) = addlayer_sources.ensure(style, source)?;
            if newly_added {
                newly_added_source_id = Some(id.clone());
                source_setup_duration = Some(setup_started_at.elapsed());
            }
            Some(id)
        }
        None => None,
    };
    let rewritten =
        match rewrite_addlayer_id_and_source(&addlayer.json, &internal_id, source_id.as_deref()) {
            Ok(rewritten) => rewritten,
            Err(err) => {
                if let Some(source_id) = &newly_added_source_id {
                    addlayer_sources.remove_if_present(style, source_id);
                }
                return Err(err);
            }
        };
    let layer = match maplibre_native::AnyLayer::from_json_str(&rewritten) {
        Ok(layer) => layer,
        Err(err) => {
            if let Some(source_id) = &newly_added_source_id {
                addlayer_sources.remove_if_present(style, source_id);
            }
            return Err(err);
        }
    };
    let added_layer = match before_layer {
        Some(b) => style.add_layer_before(layer, b),
        None => style.add_layer(layer),
    };
    if let Err(err) = added_layer {
        if let Some(source_id) = &newly_added_source_id {
            addlayer_sources.remove_if_present(style, source_id);
        }
        return Err(err);
    }
    Ok(InstalledAddLayer {
        layer_id: internal_id,
        source_setup_duration,
    })
}

/// Drop a previously-installed addlayer layer. Best-effort: a failed remove
/// would leave the layer in the style for the next request, where the same
/// `task_id`-derived id would collide. Stable addlayer sources are intentionally
/// left cached; without this layer they are unreferenced and invisible.
fn remove_addlayer(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    installed: &InstalledAddLayer,
) {
    let _ = style.remove_layer(&installed.layer_id);
}

/// Rewrite the `id` field of a style-spec layer JSON to `new_id`. The
/// input has already been validated at ingress, so we expect a JSON
/// object; we still return `Err` instead of panicking on a misshape so
/// that the `Result` plumbing handles any drift from validation.
fn rewrite_addlayer_id_and_source(
    json: &str,
    new_id: &str,
    new_source_id: Option<&str>,
) -> Result<String, maplibre_native::StyleError> {
    let mut value: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| maplibre_native::StyleError::Native(format!("addlayer JSON: {e}")))?;
    let obj = value.as_object_mut().ok_or_else(|| {
        maplibre_native::StyleError::Native("addlayer JSON must be an object".to_string())
    })?;
    obj.insert(
        "id".to_string(),
        serde_json::Value::String(new_id.to_string()),
    );
    if let Some(source_id) = new_source_id {
        obj.insert(
            "source".to_string(),
            serde_json::Value::String(source_id.to_string()),
        );
    }
    serde_json::to_string(&value)
        .map_err(|e| maplibre_native::StyleError::Native(format!("addlayer reserialize: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_addlayer_source_id_depends_on_tileset_and_json() {
        let source = AddLayerSource {
            tileset_id: "rain".to_string(),
            json: r#"{"type":"vector","tiles":["https://example.test/{z}/{x}/{y}.pbf"]}"#
                .to_string(),
        };
        let same = AddLayerSource {
            tileset_id: "rain".to_string(),
            json: source.json.clone(),
        };
        let different_tileset = AddLayerSource {
            tileset_id: "snow".to_string(),
            json: source.json.clone(),
        };
        let different_json = AddLayerSource {
            tileset_id: "rain".to_string(),
            json: r#"{"type":"vector","tiles":["https://other.example.test/{z}/{x}/{y}.pbf"]}"#
                .to_string(),
        };

        assert_eq!(source.stable_source_id(), same.stable_source_id());
        assert_ne!(
            source.stable_source_id(),
            different_tileset.stable_source_id()
        );
        assert_ne!(source.stable_source_id(), different_json.stable_source_id());
    }
}
