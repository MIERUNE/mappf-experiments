use biei_core::types::GeoJsonOverlay;

use super::error::OverlayParseError;
use super::path::percent_decode;
use super::{MAX_GEOJSON_COORDINATES, MAX_GEOJSON_FEATURES, MAX_LAT, MAX_LON, MIN_LAT, MIN_LON};

/// Parse a `geojson(<percent-encoded JSON>)` overlay segment.
/// is required to be a GeoJSON Feature or FeatureCollection; everything else is
/// rejected at the ingress to satisfy §7.5 (no arbitrary network fetch, no
/// unbounded geometry). Feature count and total coordinate count are capped.
pub(crate) fn parse_geojson_overlay(overlay: &str) -> Result<GeoJsonOverlay, OverlayParseError> {
    let Some(body) = overlay.strip_prefix("geojson(") else {
        return Err(OverlayParseError::InvalidGeoJsonSyntax);
    };
    let Some(body) = body.strip_suffix(')') else {
        return Err(OverlayParseError::InvalidGeoJsonSyntax);
    };
    let decoded = percent_decode(body)?;
    let value: serde_json::Value =
        serde_json::from_str(&decoded).map_err(|_| OverlayParseError::InvalidGeoJsonSyntax)?;
    validate_geojson(&value)?;
    Ok(GeoJsonOverlay {
        feature_collection: value,
    })
}

pub(super) fn validate_geojson(value: &serde_json::Value) -> Result<(), OverlayParseError> {
    let obj = value
        .as_object()
        .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
    let type_str = obj
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
    let mut total_coords = 0usize;
    match type_str {
        "Feature" => validate_feature(value, &mut total_coords)?,
        "FeatureCollection" => {
            let features = obj
                .get("features")
                .and_then(serde_json::Value::as_array)
                .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
            if features.len() > MAX_GEOJSON_FEATURES {
                return Err(OverlayParseError::TooManyFeatures);
            }
            for f in features {
                validate_feature(f, &mut total_coords)?;
            }
        }
        _ => return Err(OverlayParseError::UnsupportedGeoJsonType),
    }
    Ok(())
}

fn validate_feature(
    value: &serde_json::Value,
    total_coords: &mut usize,
) -> Result<(), OverlayParseError> {
    let obj = value
        .as_object()
        .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
    if obj.get("type").and_then(serde_json::Value::as_str) != Some("Feature") {
        return Err(OverlayParseError::InvalidGeoJsonSyntax);
    }
    let Some(geom) = obj.get("geometry") else {
        return Ok(());
    };
    if geom.is_null() {
        return Ok(());
    }
    let geom_obj = geom
        .as_object()
        .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
    let g_type = geom_obj
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
    // Whitelist the six geometry types simplestyle covers. `GeometryCollection`
    // is rejected explicitly because its nested-geometries shape would skip
    // the `coordinates` array entirely and bypass our coordinate-count cap
    // (§7.5). Unknown geometry types are also rejected at ingress so they
    // never reach the renderer's mbgl validation path.
    if !matches!(
        g_type,
        "Point" | "LineString" | "Polygon" | "MultiPoint" | "MultiLineString" | "MultiPolygon"
    ) {
        return Err(OverlayParseError::UnsupportedGeoJsonType);
    }
    let coords = geom_obj
        .get("coordinates")
        .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
    *total_coords = total_coords.saturating_add(count_coordinates(g_type, coords));
    if *total_coords > MAX_GEOJSON_COORDINATES {
        return Err(OverlayParseError::TooManyCoordinates);
    }
    // Don't trust the renderer to silently clamp bad inputs: every leaf
    // position must be a finite numeric `[lon, lat]` within Web Mercator
    // bounds. Defense-in-depth on top of the count check above.
    validate_coordinate_values(g_type, coords)?;
    Ok(())
}

fn validate_coordinate_values(
    g_type: &str,
    coords: &serde_json::Value,
) -> Result<(), OverlayParseError> {
    match g_type {
        "Point" => validate_position(coords),
        "LineString" | "MultiPoint" => coords
            .as_array()
            .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?
            .iter()
            .try_for_each(validate_position),
        "Polygon" | "MultiLineString" => coords
            .as_array()
            .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?
            .iter()
            .try_for_each(|ring| {
                ring.as_array()
                    .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?
                    .iter()
                    .try_for_each(validate_position)
            }),
        "MultiPolygon" => coords
            .as_array()
            .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?
            .iter()
            .try_for_each(|poly| {
                poly.as_array()
                    .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?
                    .iter()
                    .try_for_each(|ring| {
                        ring.as_array()
                            .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?
                            .iter()
                            .try_for_each(validate_position)
                    })
            }),
        // `validate_feature` whitelists the geometry types reachable here, so
        // this arm is defense-in-depth only.
        _ => Ok(()),
    }
}

fn validate_position(pos: &serde_json::Value) -> Result<(), OverlayParseError> {
    let arr = pos
        .as_array()
        .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
    if arr.len() < 2 {
        return Err(OverlayParseError::InvalidGeoJsonSyntax);
    }
    let lon = arr[0]
        .as_f64()
        .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
    let lat = arr[1]
        .as_f64()
        .ok_or(OverlayParseError::InvalidGeoJsonSyntax)?;
    if !lon.is_finite() || !lat.is_finite() {
        return Err(OverlayParseError::CoordinateOutOfRange);
    }
    if !(MIN_LON..=MAX_LON).contains(&lon) || !(MIN_LAT..=MAX_LAT).contains(&lat) {
        return Err(OverlayParseError::CoordinateOutOfRange);
    }
    Ok(())
}

fn count_coordinates(g_type: &str, coords: &serde_json::Value) -> usize {
    match g_type {
        "Point" => 1,
        "LineString" | "MultiPoint" => coords.as_array().map(Vec::len).unwrap_or(0),
        "Polygon" | "MultiLineString" => coords
            .as_array()
            .map(|rings| {
                rings
                    .iter()
                    .map(|r| r.as_array().map(Vec::len).unwrap_or(0))
                    .sum()
            })
            .unwrap_or(0),
        "MultiPolygon" => coords
            .as_array()
            .map(|polys| {
                polys
                    .iter()
                    .filter_map(serde_json::Value::as_array)
                    .flat_map(|rings| {
                        rings
                            .iter()
                            .map(|r| r.as_array().map(Vec::len).unwrap_or(0))
                    })
                    .sum()
            })
            .unwrap_or(0),
        // `validate_feature` whitelists the geometry types reachable here, so
        // this arm is defense-in-depth only.
        _ => 0,
    }
}
