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
    if overlay.label.as_ref().is_some_and(|label| {
        label.len() != 1 || !label.bytes().all(|byte| byte.is_ascii_alphanumeric())
    }) {
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
        Some(label)
            if label.len() == 1 && label.bytes().all(|byte| byte.is_ascii_alphanumeric()) =>
        {
            Some(label.to_ascii_lowercase())
        }
        Some(_) => return Err(OverlayParseError::InvalidPinLabel),
        None => None,
    };
    Ok((size, label))
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
