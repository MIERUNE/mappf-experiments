//! Static overlay rendering support for the MapLibre backend.
//!
//! `slots` owns persistent MapLibre source/layer state, `features` converts
//! domain overlays into GeoJSON, `layers` defines the fixed style templates,
//! and `pin` owns generated marker images and font handling.

mod features;
mod layers;
mod pin;
mod slots;

pub(crate) use features::build_overlay_geojson;
pub(crate) use pin::{configure_pin_label_font_path, pin_auto_padding_inset};
pub(crate) use slots::{OverlaySlotPool, populate_static_slots, render_static_with_overlays};

pub(super) const OVERLAY_SOURCE_ID: &str = "biei-overlays-src";
pub(super) const OVERLAY_IDX_PROPERTY: &str = "__biei_overlay_idx";
pub(super) const PIN_KIND_PROPERTY: &str = "__biei_marker_kind";
pub(super) const PIN_IMAGE_PROPERTY: &str = "__biei_marker_image";
pub(super) const PIN_OFFSET_PROPERTY: &str = "__biei_marker_offset";
pub(super) const PIN_KIND_VALUE: &str = "pin";

pub(super) fn parse_geojson_str(
    json: &str,
) -> Result<maplibre_native::GeoJson, maplibre_native::StyleError> {
    json.parse::<maplibre_native::GeoJson>()
        .map_err(|err| maplibre_native::StyleError::Native(err.to_string()))
}

#[cfg(test)]
use biei_core::types::{GeoJsonOverlay, LngLat, PathOverlay, PinOverlay, StaticOverlay};
#[cfg(test)]
use features::*;
#[cfg(test)]
use layers::*;
#[cfg(test)]
use slots::slot_layer_json;

#[cfg(test)]
mod tests;
