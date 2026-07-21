//! Static overlay URL parsing and validation.
//!
//! The facade splits a request's overlay list and dispatches each segment to
//! the path, GeoJSON, or pin parser. Format-specific validation stays in the
//! corresponding child module.

mod error;
mod geojson;
mod path;
mod pin;

#[cfg(test)]
use biei_core::types::{LngLat, PinSize};
use biei_core::types::{MAX_STATIC_OVERLAYS, StaticOverlay};

pub(crate) use error::OverlayParseError;
pub(crate) use geojson::parse_geojson_overlay;
#[cfg(test)]
use path::decode_polyline;
pub(crate) use path::parse_path_overlay;
pub(crate) use pin::parse_pin_overlay;

pub(crate) const MAX_PATH_POINTS: usize = 500;
pub(crate) const MAX_GEOJSON_FEATURES: usize = 500;
pub(crate) const MAX_GEOJSON_COORDINATES: usize = 5_000;
pub(super) const MIN_LAT: f64 = -90.0;
pub(super) const MAX_LAT: f64 = 90.0;
pub(super) const MIN_LON: f64 = -180.0;
pub(super) const MAX_LON: f64 = 180.0;

pub(crate) fn validate_static_overlay(overlay: &StaticOverlay) -> Result<(), OverlayParseError> {
    match overlay {
        StaticOverlay::Path(path) => path::validate_path_overlay(path),
        StaticOverlay::GeoJson(geojson) => geojson::validate_geojson(&geojson.feature_collection),
        StaticOverlay::Pin(pin) => pin::validate_pin_overlay(pin),
    }
}

pub(crate) fn parse_static_overlays(
    overlay: &str,
) -> Result<Vec<StaticOverlay>, OverlayParseError> {
    if overlay == "none" {
        return Ok(Vec::new());
    }
    let parts = split_overlays(overlay)?;
    if parts.len() > MAX_STATIC_OVERLAYS {
        return Err(OverlayParseError::TooManyOverlays);
    }
    parts
        .into_iter()
        .map(|part| {
            if part.starts_with("geojson(") {
                parse_geojson_overlay(part).map(StaticOverlay::GeoJson)
            } else if part.starts_with("pin-") {
                parse_pin_overlay(part).map(StaticOverlay::Pin)
            } else {
                parse_path_overlay(part).map(StaticOverlay::Path)
            }
        })
        .collect()
}

fn split_overlays(overlay: &str) -> Result<Vec<&str>, OverlayParseError> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (idx, ch) in overlay.char_indices() {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or(OverlayParseError::InvalidPathSyntax)?;
            }
            ',' if depth == 0 => {
                parts.push(&overlay[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(OverlayParseError::InvalidPathSyntax);
    }
    parts.push(&overlay[start..]);
    Ok(parts)
}

#[cfg(test)]
mod tests;
