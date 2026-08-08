use biei_core::types::{LngLat, PinOverlay, PinSize};

use super::error::OverlayParseError;
use super::path::{parse_optional_color, validate_coordinate};

pub(crate) fn parse_pin_overlay(overlay: &str) -> Result<PinOverlay, OverlayParseError> {
    let Some(body) = overlay.strip_prefix("pin-") else {
        return Err(OverlayParseError::InvalidPinSyntax);
    };
    let Some((style, coord)) = body.split_once('(') else {
        return Err(OverlayParseError::InvalidPinSyntax);
    };
    let Some(coord) = coord.strip_suffix(')') else {
        return Err(OverlayParseError::InvalidPinSyntax);
    };
    let Some((head, color)) = style.split_once('+') else {
        return Err(OverlayParseError::InvalidPinSyntax);
    };
    let (size, label) = parse_pin_head(head)?;
    let color = parse_optional_color(Some(color))?.ok_or(OverlayParseError::InvalidColor)?;
    let coordinate = parse_lng_lat(coord)?;
    let overlay = PinOverlay {
        size,
        label,
        color,
        coordinate,
    };
    validate_pin_overlay(&overlay)?;
    Ok(overlay)
}

pub(super) fn validate_pin_overlay(overlay: &PinOverlay) -> Result<(), OverlayParseError> {
    if overlay
        .label
        .as_ref()
        .is_some_and(|label| normalize_pin_label(label).is_none())
    {
        return Err(OverlayParseError::InvalidPinLabel);
    }
    parse_optional_color(Some(&overlay.color))?;
    validate_coordinate(overlay.coordinate)
}

fn parse_pin_head(value: &str) -> Result<(PinSize, Option<String>), OverlayParseError> {
    let (size, label) = value
        .split_once('-')
        .map_or((value, None), |(size, label)| (size, Some(label)));
    let size = match size {
        "s" => PinSize::Small,
        "m" => PinSize::Medium,
        "l" => PinSize::Large,
        _ => return Err(OverlayParseError::InvalidPinSize),
    };
    let label = match label {
        Some(label) => Some(normalize_pin_label(label).ok_or(OverlayParseError::InvalidPinLabel)?),
        None => None,
    };
    Ok((size, label))
}

/// Normalize the generated-pin label subset accepted at ingress.
///
/// Letters are case-insensitive at ingress and stored lowercase because they
/// are rendered uppercase. Numeric labels use their canonical decimal spelling
/// in the documented inclusive range 0..=99; leading-zero spellings would only
/// create duplicate cache/image identities and are rejected.
fn normalize_pin_label(value: &str) -> Option<String> {
    match value.as_bytes() {
        [byte] if byte.is_ascii_alphabetic() => Some(value.to_ascii_lowercase()),
        [byte] if byte.is_ascii_digit() => Some(value.to_string()),
        [b'1'..=b'9', second] if second.is_ascii_digit() => Some(value.to_string()),
        _ => None,
    }
}

fn parse_lng_lat(value: &str) -> Result<LngLat, OverlayParseError> {
    let (lon, lat) = value
        .split_once(',')
        .ok_or(OverlayParseError::InvalidPinSyntax)?;
    let lon = lon
        .parse::<f64>()
        .map_err(|_| OverlayParseError::InvalidPinSyntax)?;
    let lat = lat
        .parse::<f64>()
        .map_err(|_| OverlayParseError::InvalidPinSyntax)?;
    let point = LngLat { lon, lat };
    validate_coordinate(point)?;
    Ok(point)
}
