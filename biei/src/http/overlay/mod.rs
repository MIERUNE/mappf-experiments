/// Hard cap on `,`-separated overlay items per static image request. The
/// renderer-side slot pool grows lazily up to this same number (§7.3.1), so
/// raising this requires a memory/CPU review of permanent style state per
/// renderer. Sized generously for realistic use cases.
pub const MAX_OVERLAYS: usize = 64;
pub(crate) const MAX_PATH_POINTS: usize = 500;
/// Hard cap on `geojson({FeatureCollection})` overlays. §7.5.1 demands a bound
/// at the parser layer so that GeoJSON-driven cardinality attacks (many small
/// features, or one feature with millions of coordinates) cannot reach the
/// renderer. Tuned generously for legitimate use; raise only with a matching
/// review of memory/CPU impact of `add_geojson_overlay`.
pub(crate) const MAX_GEOJSON_FEATURES: usize = 500;
pub(crate) const MAX_GEOJSON_COORDINATES: usize = 5_000;
const MIN_LAT: f64 = -90.0;
const MAX_LAT: f64 = 90.0;
const MIN_LON: f64 = -180.0;
const MAX_LON: f64 = 180.0;

use crate::types::{GeoJsonOverlay, LngLat, PathOverlay, PinOverlay, PinSize, StaticOverlay};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PolylineError {
    Empty,
    InvalidPathSyntax,
    InvalidStrokeWidth,
    InvalidColor,
    InvalidOpacity,
    InvalidPercentEncoding,
    InvalidByte { byte: u8, index: usize },
    Truncated,
    CoordinateOverflow,
    CoordinateOutOfRange,
    TooManyPoints,
    TooManyOverlays,
    InvalidGeoJsonSyntax,
    UnsupportedGeoJsonType,
    TooManyFeatures,
    TooManyCoordinates,
    InvalidPinSyntax,
    InvalidPinSize,
    InvalidPinLabel,
}

impl std::fmt::Display for PolylineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "polyline must not be empty"),
            Self::InvalidPathSyntax => write!(f, "path overlay must be path-...(<polyline>)"),
            Self::InvalidStrokeWidth => write!(f, "path stroke width must be positive"),
            Self::InvalidColor => write!(f, "path color must be a 3- or 6-digit hex color"),
            Self::InvalidOpacity => write!(f, "path opacity must be between 0 and 1"),
            Self::InvalidPercentEncoding => write!(f, "invalid percent-encoded polyline"),
            Self::InvalidByte { byte, index } => {
                write!(f, "invalid polyline byte {byte} at index {index}")
            }
            Self::Truncated => write!(f, "polyline ended in the middle of a coordinate"),
            Self::CoordinateOverflow => write!(f, "polyline coordinate overflow"),
            Self::CoordinateOutOfRange => write!(f, "polyline coordinate is out of range"),
            Self::TooManyPoints => write!(f, "path overlay has too many points"),
            Self::TooManyOverlays => {
                write!(f, "request has more than {MAX_OVERLAYS} overlays")
            }
            Self::InvalidGeoJsonSyntax => write!(f, "geojson overlay payload is not valid JSON"),
            Self::UnsupportedGeoJsonType => {
                write!(f, "geojson overlay must be a Feature or FeatureCollection")
            }
            Self::TooManyFeatures => write!(
                f,
                "geojson overlay has more than {MAX_GEOJSON_FEATURES} features"
            ),
            Self::TooManyCoordinates => write!(
                f,
                "geojson overlay has more than {MAX_GEOJSON_COORDINATES} coordinates"
            ),
            Self::InvalidPinSyntax => {
                write!(
                    f,
                    "pin overlay must be pin-{{s|m|l}}[-label]+color(lon,lat)"
                )
            }
            Self::InvalidPinSize => write!(f, "pin size must be s, m, or l"),
            Self::InvalidPinLabel => write!(f, "pin label must be 1 ASCII alphanumeric character"),
        }
    }
}

impl std::error::Error for PolylineError {}

pub(crate) fn parse_static_overlays(overlay: &str) -> Result<Vec<StaticOverlay>, PolylineError> {
    if overlay == "none" {
        return Ok(Vec::new());
    }
    let parts: Vec<&str> = split_overlays(overlay)?;
    if parts.len() > MAX_OVERLAYS {
        return Err(PolylineError::TooManyOverlays);
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

/// Parse a `geojson(<percent-encoded JSON>)` overlay segment. The inner JSON
/// is required to be a GeoJSON Feature or FeatureCollection; everything else is
/// rejected at the ingress to satisfy §7.5.1 (no arbitrary network fetch, no
/// unbounded geometry). Feature count and total coordinate count are capped.
pub(crate) fn parse_geojson_overlay(overlay: &str) -> Result<GeoJsonOverlay, PolylineError> {
    let Some(body) = overlay.strip_prefix("geojson(") else {
        return Err(PolylineError::InvalidGeoJsonSyntax);
    };
    let Some(body) = body.strip_suffix(')') else {
        return Err(PolylineError::InvalidGeoJsonSyntax);
    };
    let decoded = percent_decode(body)?;
    let value: serde_json::Value =
        serde_json::from_str(&decoded).map_err(|_| PolylineError::InvalidGeoJsonSyntax)?;
    validate_geojson(&value)?;
    Ok(GeoJsonOverlay {
        feature_collection: value,
    })
}

fn validate_geojson(value: &serde_json::Value) -> Result<(), PolylineError> {
    let obj = value
        .as_object()
        .ok_or(PolylineError::InvalidGeoJsonSyntax)?;
    let type_str = obj
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or(PolylineError::InvalidGeoJsonSyntax)?;
    let mut total_coords = 0usize;
    match type_str {
        "Feature" => validate_feature(value, &mut total_coords)?,
        "FeatureCollection" => {
            let features = obj
                .get("features")
                .and_then(serde_json::Value::as_array)
                .ok_or(PolylineError::InvalidGeoJsonSyntax)?;
            if features.len() > MAX_GEOJSON_FEATURES {
                return Err(PolylineError::TooManyFeatures);
            }
            for f in features {
                validate_feature(f, &mut total_coords)?;
            }
        }
        _ => return Err(PolylineError::UnsupportedGeoJsonType),
    }
    Ok(())
}

fn validate_feature(
    value: &serde_json::Value,
    total_coords: &mut usize,
) -> Result<(), PolylineError> {
    let obj = value
        .as_object()
        .ok_or(PolylineError::InvalidGeoJsonSyntax)?;
    if obj.get("type").and_then(serde_json::Value::as_str) != Some("Feature") {
        return Err(PolylineError::InvalidGeoJsonSyntax);
    }
    let Some(geom) = obj.get("geometry") else {
        return Ok(());
    };
    if geom.is_null() {
        return Ok(());
    }
    let geom_obj = geom
        .as_object()
        .ok_or(PolylineError::InvalidGeoJsonSyntax)?;
    let g_type = geom_obj
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or(PolylineError::InvalidGeoJsonSyntax)?;
    // Whitelist the six geometry types simplestyle covers. `GeometryCollection`
    // is rejected explicitly because its nested-geometries shape would skip
    // the `coordinates` array entirely and bypass our coordinate-count cap
    // (§7.5.1). Unknown geometry types are also rejected at ingress so they
    // never reach the renderer's mbgl validation path.
    if !matches!(
        g_type,
        "Point" | "LineString" | "Polygon" | "MultiPoint" | "MultiLineString" | "MultiPolygon"
    ) {
        return Err(PolylineError::UnsupportedGeoJsonType);
    }
    let coords = geom_obj
        .get("coordinates")
        .ok_or(PolylineError::InvalidGeoJsonSyntax)?;
    *total_coords = total_coords.saturating_add(count_coordinates(g_type, coords));
    if *total_coords > MAX_GEOJSON_COORDINATES {
        return Err(PolylineError::TooManyCoordinates);
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
) -> Result<(), PolylineError> {
    match g_type {
        "Point" => validate_position(coords),
        "LineString" | "MultiPoint" => coords
            .as_array()
            .ok_or(PolylineError::InvalidGeoJsonSyntax)?
            .iter()
            .try_for_each(validate_position),
        "Polygon" | "MultiLineString" => coords
            .as_array()
            .ok_or(PolylineError::InvalidGeoJsonSyntax)?
            .iter()
            .try_for_each(|ring| {
                ring.as_array()
                    .ok_or(PolylineError::InvalidGeoJsonSyntax)?
                    .iter()
                    .try_for_each(validate_position)
            }),
        "MultiPolygon" => coords
            .as_array()
            .ok_or(PolylineError::InvalidGeoJsonSyntax)?
            .iter()
            .try_for_each(|poly| {
                poly.as_array()
                    .ok_or(PolylineError::InvalidGeoJsonSyntax)?
                    .iter()
                    .try_for_each(|ring| {
                        ring.as_array()
                            .ok_or(PolylineError::InvalidGeoJsonSyntax)?
                            .iter()
                            .try_for_each(validate_position)
                    })
            }),
        // `validate_feature` whitelists the geometry types reachable here, so
        // this arm is defense-in-depth only.
        _ => Ok(()),
    }
}

fn validate_position(pos: &serde_json::Value) -> Result<(), PolylineError> {
    let arr = pos.as_array().ok_or(PolylineError::InvalidGeoJsonSyntax)?;
    if arr.len() < 2 {
        return Err(PolylineError::InvalidGeoJsonSyntax);
    }
    let lon = arr[0].as_f64().ok_or(PolylineError::InvalidGeoJsonSyntax)?;
    let lat = arr[1].as_f64().ok_or(PolylineError::InvalidGeoJsonSyntax)?;
    if !lon.is_finite() || !lat.is_finite() {
        return Err(PolylineError::CoordinateOutOfRange);
    }
    if !(MIN_LON..=MAX_LON).contains(&lon) || !(MIN_LAT..=MAX_LAT).contains(&lat) {
        return Err(PolylineError::CoordinateOutOfRange);
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

pub(crate) fn parse_path_overlay(overlay: &str) -> Result<PathOverlay, PolylineError> {
    let Some(path_body) = overlay.strip_prefix("path-") else {
        return Err(PolylineError::InvalidPathSyntax);
    };
    let Some((style, encoded)) = path_body.split_once('(') else {
        return Err(PolylineError::InvalidPathSyntax);
    };
    let Some(encoded) = encoded.strip_suffix(')') else {
        return Err(PolylineError::InvalidPathSyntax);
    };
    let (stroke_width, stroke_color, stroke_opacity, fill_color, fill_opacity) =
        parse_path_style(style)?;
    let decoded = percent_decode(encoded)?;
    let coordinates = decode_polyline(&decoded)?;
    Ok(PathOverlay {
        stroke_width,
        stroke_color,
        stroke_opacity,
        fill_color,
        fill_opacity,
        coordinates,
    })
}

pub(crate) fn parse_pin_overlay(overlay: &str) -> Result<PinOverlay, PolylineError> {
    let Some(body) = overlay.strip_prefix("pin-") else {
        return Err(PolylineError::InvalidPinSyntax);
    };
    let Some((style, coord)) = body.split_once('(') else {
        return Err(PolylineError::InvalidPinSyntax);
    };
    let Some(coord) = coord.strip_suffix(')') else {
        return Err(PolylineError::InvalidPinSyntax);
    };
    let Some((head, color)) = style.split_once('+') else {
        return Err(PolylineError::InvalidPinSyntax);
    };
    let (size, label) = parse_pin_head(head)?;
    let color = parse_optional_color(Some(color))?.ok_or(PolylineError::InvalidColor)?;
    let coordinate = parse_lng_lat(coord)?;
    Ok(PinOverlay {
        size,
        label,
        color,
        coordinate,
    })
}

fn parse_pin_head(value: &str) -> Result<(PinSize, Option<String>), PolylineError> {
    let (size, label) = value
        .split_once('-')
        .map_or((value, None), |(size, label)| (size, Some(label)));
    let size = match size {
        "s" => PinSize::Small,
        "m" => PinSize::Medium,
        "l" => PinSize::Large,
        _ => return Err(PolylineError::InvalidPinSize),
    };
    let label = match label {
        Some(label)
            if label.len() == 1 && label.bytes().all(|byte| byte.is_ascii_alphanumeric()) =>
        {
            Some(label.to_ascii_lowercase())
        }
        Some(_) => return Err(PolylineError::InvalidPinLabel),
        None => None,
    };
    Ok((size, label))
}

fn parse_lng_lat(value: &str) -> Result<LngLat, PolylineError> {
    let (lon, lat) = value
        .split_once(',')
        .ok_or(PolylineError::InvalidPinSyntax)?;
    let lon = lon
        .parse::<f64>()
        .map_err(|_| PolylineError::InvalidPinSyntax)?;
    let lat = lat
        .parse::<f64>()
        .map_err(|_| PolylineError::InvalidPinSyntax)?;
    let point = LngLat { lon, lat };
    validate_coordinate(point)?;
    Ok(point)
}

fn split_overlays(overlay: &str) -> Result<Vec<&str>, PolylineError> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (idx, ch) in overlay.char_indices() {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or(PolylineError::InvalidPathSyntax)?;
            }
            ',' if depth == 0 => {
                parts.push(&overlay[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(PolylineError::InvalidPathSyntax);
    }
    parts.push(&overlay[start..]);
    Ok(parts)
}

type PathStyle = (
    Option<f32>,
    Option<String>,
    Option<f32>,
    Option<String>,
    Option<f32>,
);

fn parse_path_style(style: &str) -> Result<PathStyle, PolylineError> {
    let (stroke_width, paint) = split_first_optional_pair(style, '+');
    let Some(paint) = paint else {
        return Ok((
            parse_optional_stroke_width(stroke_width)?,
            None,
            None,
            None,
            None,
        ));
    };
    let (stroke, fill) = split_optional_pair(paint, '+')?;
    let (stroke_color, stroke_opacity) = split_required_pair(stroke, '-')?;
    let (fill_color, fill_opacity) = match fill {
        Some(fill) => {
            let (fill_color, fill_opacity) = split_required_pair(fill, '-')?;
            (Some(fill_color), fill_opacity)
        }
        None => (None, None),
    };

    Ok((
        parse_optional_stroke_width(stroke_width)?,
        parse_optional_color(Some(stroke_color))?,
        parse_optional_opacity(stroke_opacity)?,
        parse_optional_color(fill_color)?,
        parse_optional_opacity(fill_opacity)?,
    ))
}

fn split_first_optional_pair(value: &str, delimiter: char) -> (&str, Option<&str>) {
    value
        .split_once(delimiter)
        .map_or((value, None), |(first, second)| {
            (first, (!second.is_empty()).then_some(second))
        })
}

fn split_optional_pair(
    value: &str,
    delimiter: char,
) -> Result<(&str, Option<&str>), PolylineError> {
    let mut parts = value.split(delimiter);
    let first = parts.next().unwrap_or("");
    let second = parts.next();
    if parts.next().is_some() {
        return Err(PolylineError::InvalidPathSyntax);
    }
    Ok((first, second.filter(|value| !value.is_empty())))
}

fn split_required_pair(
    value: &str,
    delimiter: char,
) -> Result<(&str, Option<&str>), PolylineError> {
    let (first, second) = split_optional_pair(value, delimiter)?;
    if first.is_empty() {
        return Err(PolylineError::InvalidPathSyntax);
    }
    Ok((first, second))
}

fn parse_optional_stroke_width(value: &str) -> Result<Option<f32>, PolylineError> {
    if value.is_empty() {
        return Ok(None);
    }
    let width = value
        .parse::<f32>()
        .map_err(|_| PolylineError::InvalidStrokeWidth)?;
    if width.is_finite() && width > 0.0 {
        Ok(Some(width))
    } else {
        Err(PolylineError::InvalidStrokeWidth)
    }
}

fn parse_optional_color(value: Option<&str>) -> Result<Option<String>, PolylineError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if matches!(value.len(), 3 | 6) && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(Some(value.to_ascii_lowercase()))
    } else {
        Err(PolylineError::InvalidColor)
    }
}

fn parse_optional_opacity(value: Option<&str>) -> Result<Option<f32>, PolylineError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let opacity = value
        .parse::<f32>()
        .map_err(|_| PolylineError::InvalidOpacity)?;
    if opacity.is_finite() && (0.0..=1.0).contains(&opacity) {
        Ok(Some(opacity))
    } else {
        Err(PolylineError::InvalidOpacity)
    }
}

pub(crate) fn decode_polyline(encoded: &str) -> Result<Vec<LngLat>, PolylineError> {
    if encoded.is_empty() {
        return Err(PolylineError::Empty);
    }

    let bytes = encoded.as_bytes();
    let mut index = 0;
    let mut lat = 0i64;
    let mut lon = 0i64;
    let mut points = Vec::new();

    while index < bytes.len() {
        let delta_lat = decode_delta(bytes, &mut index)?;
        let delta_lon = decode_delta(bytes, &mut index)?;
        lat = lat
            .checked_add(delta_lat)
            .ok_or(PolylineError::CoordinateOverflow)?;
        lon = lon
            .checked_add(delta_lon)
            .ok_or(PolylineError::CoordinateOverflow)?;
        if points.len() >= MAX_PATH_POINTS {
            return Err(PolylineError::TooManyPoints);
        }
        let point = LngLat {
            lon: lon as f64 / 100_000.0,
            lat: lat as f64 / 100_000.0,
        };
        validate_coordinate(point)?;
        points.push(point);
    }

    Ok(points)
}

fn validate_coordinate(point: LngLat) -> Result<(), PolylineError> {
    if (MIN_LON..=MAX_LON).contains(&point.lon) && (MIN_LAT..=MAX_LAT).contains(&point.lat) {
        Ok(())
    } else {
        Err(PolylineError::CoordinateOutOfRange)
    }
}

fn percent_decode(value: &str) -> Result<String, PolylineError> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hi = *bytes
                .get(index + 1)
                .ok_or(PolylineError::InvalidPercentEncoding)?;
            let lo = *bytes
                .get(index + 2)
                .ok_or(PolylineError::InvalidPercentEncoding)?;
            let byte = from_hex(hi)
                .and_then(|hi| from_hex(lo).map(|lo| (hi << 4) | lo))
                .ok_or(PolylineError::InvalidPercentEncoding)?;
            out.push(byte);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }

    String::from_utf8(out).map_err(|_| PolylineError::InvalidPercentEncoding)
}

fn from_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn decode_delta(bytes: &[u8], index: &mut usize) -> Result<i64, PolylineError> {
    let mut result = 0i64;
    let mut shift = 0;

    loop {
        let Some(&byte) = bytes.get(*index) else {
            return Err(PolylineError::Truncated);
        };
        let current_index = *index;
        *index += 1;

        let value = byte.checked_sub(63).ok_or(PolylineError::InvalidByte {
            byte,
            index: current_index,
        })?;
        if value > 0x3f {
            return Err(PolylineError::InvalidByte {
                byte,
                index: current_index,
            });
        }
        if shift >= 60 {
            return Err(PolylineError::CoordinateOverflow);
        }

        result |= i64::from(value & 0x1f) << shift;
        shift += 5;

        if value < 0x20 {
            break;
        }
    }

    Ok(if result & 1 == 1 {
        !(result >> 1)
    } else {
        result >> 1
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 0.000_001,
            "expected {expected}, got {actual}"
        );
    }

    fn encode_delta(delta: i64) -> String {
        let mut value = if delta < 0 {
            (!(delta << 1)) as u64
        } else {
            (delta << 1) as u64
        };
        let mut encoded = String::new();
        while value >= 0x20 {
            encoded.push(char::from(((0x20 | (value & 0x1f)) + 63) as u8));
            value >>= 5;
        }
        encoded.push(char::from((value + 63) as u8));
        encoded
    }

    fn encode_point(lat_e5: i64, lon_e5: i64) -> String {
        format!("{}{}", encode_delta(lat_e5), encode_delta(lon_e5))
    }

    #[test]
    fn decodes_reference_polyline() {
        let points = decode_polyline(r"_p~iF~ps|U_ulLnnqC_mqNvxq`@").expect("polyline decodes");

        assert_eq!(points.len(), 3);
        assert_close(points[0].lat, 38.5);
        assert_close(points[0].lon, -120.2);
        assert_close(points[1].lat, 40.7);
        assert_close(points[1].lon, -120.95);
        assert_close(points[2].lat, 43.252);
        assert_close(points[2].lon, -126.453);
    }

    #[test]
    fn extracts_polyline_from_path_overlay() {
        let points = parse_path_overlay(r"path-5+f44-0.5(_p~iF~ps%7CU)")
            .map(|overlay| overlay.coordinates)
            .expect("path overlay polyline decodes");

        assert_eq!(points.len(), 1);
        assert_close(points[0].lat, 38.5);
        assert_close(points[0].lon, -120.2);
    }

    #[test]
    fn parses_path_overlay_style_parameters() {
        let overlay =
            parse_path_overlay(r"path-5+F44-0.5+00ffcc-0.25(_p~iF~ps%7CU)").expect("path parses");

        assert_eq!(overlay.stroke_width, Some(5.0));
        assert_eq!(overlay.stroke_color.as_deref(), Some("f44"));
        assert_eq!(overlay.stroke_opacity, Some(0.5));
        assert_eq!(overlay.fill_color.as_deref(), Some("00ffcc"));
        assert_eq!(overlay.fill_opacity, Some(0.25));
        assert_eq!(overlay.coordinates.len(), 1);
    }

    #[test]
    fn parses_pin_overlay_without_label() {
        let pin = parse_pin_overlay("pin-m+9ed4bd(139.0,35.0)").expect("pin parses");

        assert_eq!(pin.size, PinSize::Medium);
        assert_eq!(pin.label, None);
        assert_eq!(pin.color, "9ed4bd");
        assert_close(pin.coordinate.lon, 139.0);
        assert_close(pin.coordinate.lat, 35.0);
    }

    #[test]
    fn parses_pin_overlay_with_label() {
        let pin = parse_pin_overlay("pin-s-a+9ED4BD(139,35)").expect("pin parses");

        assert_eq!(pin.size, PinSize::Small);
        assert_eq!(pin.label.as_deref(), Some("a"));
        assert_eq!(pin.color, "9ed4bd");
    }

    #[test]
    fn rejects_invalid_pin_overlay() {
        assert_eq!(
            parse_pin_overlay("pin-x+9ed4bd(139,35)").map(|_| ()),
            Err(PolylineError::InvalidPinSize)
        );
        assert_eq!(
            parse_pin_overlay("pin-s-aa+9ed4bd(139,35)").map(|_| ()),
            Err(PolylineError::InvalidPinLabel)
        );
        assert_eq!(
            parse_pin_overlay("pin-s+ff(139,35)").map(|_| ()),
            Err(PolylineError::InvalidColor)
        );
        assert_eq!(
            parse_pin_overlay("pin-s+9ed4bd(200,35)").map(|_| ()),
            Err(PolylineError::CoordinateOutOfRange)
        );
    }

    #[test]
    fn parses_path_overlay_with_only_width() {
        let overlay = parse_path_overlay(r"path-2(_p~iF~ps%7CU)").expect("path parses");

        assert_eq!(overlay.stroke_width, Some(2.0));
        assert_eq!(overlay.stroke_color, None);
        assert_eq!(overlay.stroke_opacity, None);
        assert_eq!(overlay.fill_color, None);
        assert_eq!(overlay.fill_opacity, None);
    }

    #[test]
    fn rejects_invalid_path_style_parameters() {
        assert_eq!(
            parse_path_overlay(r"path-0+f44(_p~iF~ps%7CU)").map(|_| ()),
            Err(PolylineError::InvalidStrokeWidth)
        );
        assert_eq!(
            parse_path_overlay(r"path-5+ff(_p~iF~ps%7CU)").map(|_| ()),
            Err(PolylineError::InvalidColor)
        );
        assert_eq!(
            parse_path_overlay(r"path-5+f44-1.2(_p~iF~ps%7CU)").map(|_| ()),
            Err(PolylineError::InvalidOpacity)
        );
    }

    #[test]
    fn rejects_bad_percent_encoding() {
        assert_eq!(
            parse_path_overlay("path-5+f44(%7X)").map(|overlay| overlay.coordinates),
            Err(PolylineError::InvalidPercentEncoding)
        );
    }

    #[test]
    fn rejects_malformed_path_overlay() {
        assert_eq!(
            parse_path_overlay("path-5+f44").map(|overlay| overlay.coordinates),
            Err(PolylineError::InvalidPathSyntax)
        );
    }

    #[test]
    fn decodes_negative_delta() {
        let points = decode_polyline("??@B").expect("polyline decodes");

        assert_eq!(
            points,
            vec![
                LngLat { lon: 0.0, lat: 0.0 },
                LngLat {
                    lon: -0.00002,
                    lat: -0.00001
                },
            ]
        );
    }

    #[test]
    fn rejects_empty_polyline() {
        assert_eq!(decode_polyline(""), Err(PolylineError::Empty));
    }

    #[test]
    fn rejects_too_many_path_points() {
        let encoded = "??".repeat(MAX_PATH_POINTS + 1);

        assert_eq!(decode_polyline(&encoded), Err(PolylineError::TooManyPoints));
    }

    #[test]
    fn rejects_coordinates_out_of_range() {
        let encoded = encode_point(9_100_000, 0);

        assert_eq!(
            decode_polyline(&encoded),
            Err(PolylineError::CoordinateOutOfRange)
        );
    }

    #[test]
    fn rejects_invalid_byte() {
        assert_eq!(
            decode_polyline("\n"),
            Err(PolylineError::InvalidByte {
                byte: b'\n',
                index: 0
            })
        );
    }

    #[test]
    fn rejects_truncated_coordinate_pair() {
        assert_eq!(decode_polyline("?"), Err(PolylineError::Truncated));
    }

    #[test]
    fn rejects_unterminated_varint() {
        assert_eq!(decode_polyline("_____"), Err(PolylineError::Truncated));
    }

    /// Percent-encode every non-alphanumeric byte so the JSON survives the
    /// URL-level `,`-split that `parse_static_overlays` uses to detach
    /// comma-separated overlays. Mirrors what a real client must do.
    fn pct_encode(s: &str) -> String {
        s.bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() {
                    (b as char).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect()
    }

    #[test]
    fn parses_geojson_feature_collection_overlay() {
        // r##…##: the JSON contains `"#ff0000"`, and `"#` would otherwise end
        // a `r#"…"#` raw string early.
        let json = r##"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"stroke":"#ff0000"},"geometry":{"type":"LineString","coordinates":[[0,0],[1,1]]}}]}"##;
        let overlay = format!("geojson({})", pct_encode(json));

        let parsed = parse_geojson_overlay(&overlay).expect("geojson parses");

        // Round-trips to the same JSON value (modulo whitespace).
        assert_eq!(
            parsed.feature_collection,
            serde_json::from_str::<serde_json::Value>(json).unwrap()
        );
    }

    #[test]
    fn parses_geojson_single_feature_overlay() {
        let json = r#"{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[139,35]}}"#;
        let overlay = format!("geojson({})", pct_encode(json));

        let parsed = parse_geojson_overlay(&overlay).expect("single feature parses");

        assert_eq!(
            parsed
                .feature_collection
                .get("type")
                .and_then(|v| v.as_str()),
            Some("Feature"),
        );
    }

    #[test]
    fn rejects_geojson_with_unsupported_root_type() {
        // Bare geometry is GeoJSON-valid but not accepted by the static images
        // overlay grammar (must be Feature or FeatureCollection).
        let json = r#"{"type":"Point","coordinates":[0,0]}"#;
        let overlay = format!("geojson({})", pct_encode(json));

        assert_eq!(
            parse_geojson_overlay(&overlay),
            Err(PolylineError::UnsupportedGeoJsonType)
        );
    }

    #[test]
    fn rejects_geojson_with_invalid_json() {
        let overlay = format!("geojson({})", pct_encode("{not json"));

        assert_eq!(
            parse_geojson_overlay(&overlay),
            Err(PolylineError::InvalidGeoJsonSyntax)
        );
    }

    #[test]
    fn rejects_geojson_with_too_many_features() {
        let feature =
            r#"{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[0,0]}}"#;
        let features: Vec<_> = std::iter::repeat_n(feature, MAX_GEOJSON_FEATURES + 1).collect();
        let json = format!(
            r#"{{"type":"FeatureCollection","features":[{}]}}"#,
            features.join(",")
        );
        let overlay = format!("geojson({})", pct_encode(&json));

        assert_eq!(
            parse_geojson_overlay(&overlay),
            Err(PolylineError::TooManyFeatures)
        );
    }

    #[test]
    fn rejects_geojson_with_too_many_coordinates() {
        // One LineString with > MAX_GEOJSON_COORDINATES points.
        let coords: Vec<_> = (0..=MAX_GEOJSON_COORDINATES)
            .map(|i| format!("[{i},0]"))
            .collect();
        let json = format!(
            r#"{{"type":"Feature","properties":{{}},"geometry":{{"type":"LineString","coordinates":[{}]}}}}"#,
            coords.join(",")
        );
        let overlay = format!("geojson({})", pct_encode(&json));

        assert_eq!(
            parse_geojson_overlay(&overlay),
            Err(PolylineError::TooManyCoordinates)
        );
    }

    #[test]
    fn parse_static_overlays_routes_path_geojson_and_pin() {
        // Path and geojson sit side by side in a comma-separated list. The
        // geojson payload is percent-encoded so its inner commas survive the
        // `,`-split.
        let path = r"path-5+f44(_p~iF~ps%7CU)";
        let gj_json =
            r#"{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[1,2]}}"#;
        let overlay = format!(
            "{path},pin-s-a+9ed4bd(139,35),geojson({})",
            pct_encode(gj_json)
        );

        let parsed = parse_static_overlays(&overlay).expect("mixed overlays parse");

        assert!(matches!(parsed[0], StaticOverlay::Path(_)));
        assert!(matches!(parsed[1], StaticOverlay::Pin(_)));
        assert!(matches!(parsed[2], StaticOverlay::GeoJson(_)));
    }

    #[test]
    fn rejects_geojson_with_non_numeric_coordinates() {
        let json = r#"{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":["a","b"]}}"#;
        let overlay = format!("geojson({})", pct_encode(json));

        assert_eq!(
            parse_geojson_overlay(&overlay),
            Err(PolylineError::InvalidGeoJsonSyntax)
        );
    }

    #[test]
    fn rejects_geojson_with_out_of_range_coordinate() {
        let json = r#"{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[200,0]}}"#;
        let overlay = format!("geojson({})", pct_encode(json));

        assert_eq!(
            parse_geojson_overlay(&overlay),
            Err(PolylineError::CoordinateOutOfRange)
        );
    }

    #[test]
    fn rejects_geojson_with_nan_coordinate() {
        // JSON has no NaN literal — but Polygon with empty position triggers
        // the same "non-finite / malformed position" branch.
        let json = r#"{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[0]]]}}"#;
        let overlay = format!("geojson({})", pct_encode(json));

        assert_eq!(
            parse_geojson_overlay(&overlay),
            Err(PolylineError::InvalidGeoJsonSyntax)
        );
    }

    #[test]
    fn rejects_unknown_geometry_type() {
        // Made-up `Circle` geometry — not in the simplestyle whitelist. Must
        // be rejected at ingress so renderer never sees it.
        let json = r#"{"type":"Feature","properties":{},"geometry":{"type":"Circle","coordinates":[0,0]}}"#;
        let overlay = format!("geojson({})", pct_encode(json));

        assert_eq!(
            parse_geojson_overlay(&overlay),
            Err(PolylineError::UnsupportedGeoJsonType)
        );
    }

    #[test]
    fn rejects_geometry_collection() {
        // GeometryCollection's nested geometries would bypass the per-feature
        // coordinate-count cap; we reject it outright.
        let json = r#"{"type":"Feature","properties":{},"geometry":{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[0,0]}]}}"#;
        let overlay = format!("geojson({})", pct_encode(json));

        assert_eq!(
            parse_geojson_overlay(&overlay),
            Err(PolylineError::UnsupportedGeoJsonType)
        );
    }

    #[test]
    fn parse_static_overlays_accepts_up_to_max_overlays() {
        let path = r"path-5+f44(_p~iF~ps%7CU)";
        // exactly MAX_OVERLAYS items
        let url: String = std::iter::repeat_n(path, MAX_OVERLAYS)
            .collect::<Vec<_>>()
            .join(",");

        let parsed = parse_static_overlays(&url).expect("MAX_OVERLAYS exactly should parse");
        assert_eq!(parsed.len(), MAX_OVERLAYS);
    }

    #[test]
    fn parse_static_overlays_rejects_more_than_max_overlays() {
        let path = r"path-5+f44(_p~iF~ps%7CU)";
        let url: String = std::iter::repeat_n(path, MAX_OVERLAYS + 1)
            .collect::<Vec<_>>()
            .join(",");

        assert_eq!(
            parse_static_overlays(&url),
            Err(PolylineError::TooManyOverlays)
        );
    }
}
