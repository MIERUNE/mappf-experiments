use biei_core::types::{LngLat, PathOverlay};

use super::error::OverlayParseError;
use super::{MAX_LAT, MAX_LON, MAX_PATH_POINTS, MIN_LAT, MIN_LON};

pub(crate) fn parse_path_overlay(overlay: &str) -> Result<PathOverlay, OverlayParseError> {
    let Some(path_body) = overlay.strip_prefix("path-") else {
        return Err(OverlayParseError::InvalidPathSyntax);
    };
    let Some((style, encoded)) = path_body.split_once('(') else {
        return Err(OverlayParseError::InvalidPathSyntax);
    };
    let Some(encoded) = encoded.strip_suffix(')') else {
        return Err(OverlayParseError::InvalidPathSyntax);
    };
    let (stroke_width, stroke_color, stroke_opacity, fill_color, fill_opacity) =
        parse_path_style(style)?;
    let decoded = percent_decode(encoded)?;
    let coordinates = decode_polyline(&decoded)?;
    let overlay = PathOverlay {
        stroke_width,
        stroke_color,
        stroke_opacity,
        fill_color,
        fill_opacity,
        coordinates,
    };
    validate_path_overlay(&overlay)?;
    Ok(overlay)
}

pub(super) fn validate_path_overlay(overlay: &PathOverlay) -> Result<(), OverlayParseError> {
    if overlay.coordinates.is_empty() {
        return Err(OverlayParseError::Empty);
    }
    if overlay.coordinates.len() > MAX_PATH_POINTS {
        return Err(OverlayParseError::TooManyPoints);
    }
    if overlay
        .stroke_width
        .is_some_and(|width| !width.is_finite() || width <= 0.0)
    {
        return Err(OverlayParseError::InvalidStrokeWidth);
    }
    parse_optional_color(overlay.stroke_color.as_deref())?;
    parse_optional_color(overlay.fill_color.as_deref())?;
    if let Some(opacity) = overlay.stroke_opacity {
        validate_opacity(opacity)?;
    }
    if let Some(opacity) = overlay.fill_opacity {
        validate_opacity(opacity)?;
    }
    overlay
        .coordinates
        .iter()
        .copied()
        .try_for_each(validate_coordinate)
}

type PathStyle = (
    Option<f32>,
    Option<String>,
    Option<f32>,
    Option<String>,
    Option<f32>,
);

fn parse_path_style(style: &str) -> Result<PathStyle, OverlayParseError> {
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
) -> Result<(&str, Option<&str>), OverlayParseError> {
    let mut parts = value.split(delimiter);
    let first = parts.next().unwrap_or("");
    let second = parts.next();
    if parts.next().is_some() {
        return Err(OverlayParseError::InvalidPathSyntax);
    }
    Ok((first, second.filter(|value| !value.is_empty())))
}

fn split_required_pair(
    value: &str,
    delimiter: char,
) -> Result<(&str, Option<&str>), OverlayParseError> {
    let (first, second) = split_optional_pair(value, delimiter)?;
    if first.is_empty() {
        return Err(OverlayParseError::InvalidPathSyntax);
    }
    Ok((first, second))
}

fn parse_optional_stroke_width(value: &str) -> Result<Option<f32>, OverlayParseError> {
    if value.is_empty() {
        return Ok(None);
    }
    let width = value
        .parse::<f32>()
        .map_err(|_| OverlayParseError::InvalidStrokeWidth)?;
    if width.is_finite() && width > 0.0 {
        Ok(Some(width))
    } else {
        Err(OverlayParseError::InvalidStrokeWidth)
    }
}

pub(super) fn parse_optional_color(
    value: Option<&str>,
) -> Result<Option<String>, OverlayParseError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if matches!(value.len(), 3 | 6) && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(Some(value.to_ascii_lowercase()))
    } else {
        Err(OverlayParseError::InvalidColor)
    }
}

fn parse_optional_opacity(value: Option<&str>) -> Result<Option<f32>, OverlayParseError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let opacity = value
        .parse::<f32>()
        .map_err(|_| OverlayParseError::InvalidOpacity)?;
    validate_opacity(opacity)?;
    Ok(Some(opacity))
}

fn validate_opacity(opacity: f32) -> Result<(), OverlayParseError> {
    if opacity.is_finite() && (0.0..=1.0).contains(&opacity) {
        Ok(())
    } else {
        Err(OverlayParseError::InvalidOpacity)
    }
}

pub(crate) fn decode_polyline(encoded: &str) -> Result<Vec<LngLat>, OverlayParseError> {
    if encoded.is_empty() {
        return Err(OverlayParseError::Empty);
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
            .ok_or(OverlayParseError::CoordinateOverflow)?;
        lon = lon
            .checked_add(delta_lon)
            .ok_or(OverlayParseError::CoordinateOverflow)?;
        if points.len() >= MAX_PATH_POINTS {
            return Err(OverlayParseError::TooManyPoints);
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

pub(super) fn validate_coordinate(point: LngLat) -> Result<(), OverlayParseError> {
    if (MIN_LON..=MAX_LON).contains(&point.lon) && (MIN_LAT..=MAX_LAT).contains(&point.lat) {
        Ok(())
    } else {
        Err(OverlayParseError::CoordinateOutOfRange)
    }
}

pub(super) fn percent_decode(value: &str) -> Result<String, OverlayParseError> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;

    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hi = *bytes
                .get(index + 1)
                .ok_or(OverlayParseError::InvalidPercentEncoding)?;
            let lo = *bytes
                .get(index + 2)
                .ok_or(OverlayParseError::InvalidPercentEncoding)?;
            let byte = from_hex(hi)
                .and_then(|hi| from_hex(lo).map(|lo| (hi << 4) | lo))
                .ok_or(OverlayParseError::InvalidPercentEncoding)?;
            out.push(byte);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }

    String::from_utf8(out).map_err(|_| OverlayParseError::InvalidPercentEncoding)
}

fn from_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn decode_delta(bytes: &[u8], index: &mut usize) -> Result<i64, OverlayParseError> {
    let mut result = 0i64;
    let mut shift = 0;

    loop {
        let Some(&byte) = bytes.get(*index) else {
            return Err(OverlayParseError::Truncated);
        };
        let current_index = *index;
        *index += 1;

        let value = byte.checked_sub(63).ok_or(OverlayParseError::InvalidByte {
            byte,
            index: current_index,
        })?;
        if value > 0x3f {
            return Err(OverlayParseError::InvalidByte {
                byte,
                index: current_index,
            });
        }
        if shift >= 60 {
            return Err(OverlayParseError::CoordinateOverflow);
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
