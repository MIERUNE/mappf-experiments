//! Static camera helpers for bbox/auto positioning.

use crate::renderer::overlay::pin_auto_padding_inset;
use biei_core::types::{Padding, StaticOverlay};

/// Convert URL-pixel padding (biei) to mbgl `EdgeInsets` (logical pixels,
/// f64). The two share the same coordinate system for static rendering,
/// so this is a straight cast.
pub(super) fn padding_to_edge_insets(p: Padding) -> maplibre_native::EdgeInsets {
    maplibre_native::EdgeInsets {
        top: f64::from(p.top),
        right: f64::from(p.right),
        bottom: f64::from(p.bottom),
        left: f64::from(p.left),
    }
}

pub(super) fn auto_padding_for_overlays(
    mut padding: Padding,
    overlays: &[StaticOverlay],
    width: u16,
    height: u16,
) -> Padding {
    let Some(bounds) = overlay_bounds(overlays) else {
        return padding;
    };
    let pin_inset = overlays.iter().fold(Padding::default(), |acc, overlay| {
        let StaticOverlay::Pin(pin) = overlay else {
            return acc;
        };
        let inset = pin_auto_padding_inset(pin.size);
        let deficit = pin_padding_deficit(padding, bounds, pin.coordinate, inset, width, height);
        Padding {
            top: acc.top.max(deficit.top),
            right: acc.right.max(deficit.right),
            bottom: acc.bottom.max(deficit.bottom),
            left: acc.left.max(deficit.left),
        }
    });
    padding.top = padding.top.saturating_add(pin_inset.top);
    padding.right = padding.right.saturating_add(pin_inset.right);
    padding.bottom = padding.bottom.saturating_add(pin_inset.bottom);
    padding.left = padding.left.saturating_add(pin_inset.left);
    padding
}

fn pin_padding_deficit(
    padding: Padding,
    bounds: OverlayBounds,
    point: biei_core::types::LngLat,
    inset: Padding,
    width: u16,
    height: u16,
) -> Padding {
    let clearance = projected_pin_clearance(padding, bounds, point, width, height);
    Padding {
        top: missing_padding(inset.top, clearance.top),
        right: missing_padding(inset.right, clearance.right),
        bottom: missing_padding(inset.bottom, clearance.bottom),
        left: missing_padding(inset.left, clearance.left),
    }
}

#[derive(Clone, Copy, Debug)]
struct EdgeClearance {
    top: f64,
    right: f64,
    bottom: f64,
    left: f64,
}

fn projected_pin_clearance(
    padding: Padding,
    bounds: OverlayBounds,
    point: biei_core::types::LngLat,
    width: u16,
    height: u16,
) -> EdgeClearance {
    let inner_width = f64::from(
        u32::from(width)
            .saturating_sub(u32::from(padding.left) + u32::from(padding.right))
            .max(1),
    );
    let inner_height = f64::from(
        u32::from(height)
            .saturating_sub(u32::from(padding.top) + u32::from(padding.bottom))
            .max(1),
    );

    let min_x = mercator_x(bounds.min_lon);
    let max_x = mercator_x(bounds.max_lon);
    let point_x = mercator_x(point.lon);
    let x_span = (max_x - min_x).max(0.0);
    let (min_y, max_y, point_y) = mercator_coordinates(bounds, point);
    let y_span = (max_y - min_y).max(0.0);
    let scale = projected_fit_scale(x_span, y_span, inner_width, inner_height);
    let horizontal_slack = ((inner_width - x_span * scale) / 2.0).max(0.0);
    let vertical_slack = ((inner_height - y_span * scale) / 2.0).max(0.0);

    EdgeClearance {
        top: f64::from(padding.top) + vertical_slack + ((max_y - point_y).max(0.0) * scale),
        right: f64::from(padding.right) + horizontal_slack + ((max_x - point_x).max(0.0) * scale),
        bottom: f64::from(padding.bottom) + vertical_slack + ((point_y - min_y).max(0.0) * scale),
        left: f64::from(padding.left) + horizontal_slack + ((point_x - min_x).max(0.0) * scale),
    }
}

fn projected_fit_scale(x_span: f64, y_span: f64, inner_width: f64, inner_height: f64) -> f64 {
    match (x_span > f64::EPSILON, y_span > f64::EPSILON) {
        (true, true) => (inner_width / x_span).min(inner_height / y_span),
        (true, false) => inner_width / x_span,
        (false, true) => inner_height / y_span,
        (false, false) => 1.0,
    }
}

fn mercator_coordinates(bounds: OverlayBounds, point: biei_core::types::LngLat) -> (f64, f64, f64) {
    (
        mercator_y(bounds.min_lat),
        mercator_y(bounds.max_lat),
        mercator_y(point.lat),
    )
}

fn mercator_x(lon: f64) -> f64 {
    let lon = if lon.is_finite() { lon } else { 0.0 };
    lon.to_radians()
}

fn missing_padding(required: u16, available: f64) -> u16 {
    let available = available.max(0.0).floor() as u16;
    required.saturating_sub(available)
}

fn mercator_y(lat: f64) -> f64 {
    let lat = if lat.is_finite() { lat } else { 0.0 };
    let lat = lat.clamp(-85.051_128_78, 85.051_128_78).to_radians();
    (std::f64::consts::FRAC_PI_4 + lat / 2.0).tan().ln()
}

#[derive(Clone, Copy, Debug)]
struct OverlayBounds {
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
}

impl OverlayBounds {
    fn new(point: biei_core::types::LngLat) -> Self {
        Self {
            min_lon: point.lon,
            min_lat: point.lat,
            max_lon: point.lon,
            max_lat: point.lat,
        }
    }

    fn include(&mut self, point: biei_core::types::LngLat) {
        self.min_lon = self.min_lon.min(point.lon);
        self.min_lat = self.min_lat.min(point.lat);
        self.max_lon = self.max_lon.max(point.lon);
        self.max_lat = self.max_lat.max(point.lat);
    }
}

fn overlay_bounds(overlays: &[StaticOverlay]) -> Option<OverlayBounds> {
    let mut bounds = None;
    for overlay in overlays {
        match overlay {
            StaticOverlay::Path(path) => {
                for point in &path.coordinates {
                    include_bounds(&mut bounds, *point);
                }
            }
            StaticOverlay::Pin(pin) => include_bounds(&mut bounds, pin.coordinate),
            StaticOverlay::GeoJson(geojson) => {
                collect_geojson_bounds(&geojson.feature_collection, &mut bounds);
            }
        }
    }
    bounds
}

fn include_bounds(bounds: &mut Option<OverlayBounds>, point: biei_core::types::LngLat) {
    match bounds {
        Some(bounds) => bounds.include(point),
        None => *bounds = Some(OverlayBounds::new(point)),
    }
}

fn collect_geojson_bounds(value: &serde_json::Value, bounds: &mut Option<OverlayBounds>) {
    match value.get("type").and_then(serde_json::Value::as_str) {
        Some("FeatureCollection") => {
            if let Some(features) = value.get("features").and_then(serde_json::Value::as_array) {
                for feature in features {
                    collect_geojson_bounds(feature, bounds);
                }
            }
        }
        Some("Feature") => {
            if let Some(geometry) = value.get("geometry") {
                collect_geojson_bounds(geometry, bounds);
            }
        }
        Some("Point")
        | Some("MultiPoint")
        | Some("LineString")
        | Some("MultiLineString")
        | Some("Polygon")
        | Some("MultiPolygon") => collect_geojson_coordinates(value.get("coordinates"), bounds),
        _ => {}
    }
}

fn collect_geojson_coordinates(
    value: Option<&serde_json::Value>,
    bounds: &mut Option<OverlayBounds>,
) {
    let Some(value) = value else {
        return;
    };
    let Some(array) = value.as_array() else {
        return;
    };
    if array.len() >= 2
        && array[0].is_number()
        && array[1].is_number()
        && let (Some(lon), Some(lat)) = (array[0].as_f64(), array[1].as_f64())
    {
        include_bounds(bounds, biei_core::types::LngLat { lon, lat });
        return;
    }
    for item in array {
        collect_geojson_coordinates(Some(item), bounds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use biei_core::types::{LngLat, PinOverlay, PinSize};

    fn pin(size: PinSize) -> StaticOverlay {
        StaticOverlay::Pin(PinOverlay {
            size,
            label: None,
            color: "4c78a8".to_string(),
            coordinate: LngLat {
                lon: 139.767,
                lat: 35.681,
            },
        })
    }

    #[test]
    fn auto_padding_adds_pin_top_inset() {
        let base = Padding {
            top: 20,
            right: 26,
            bottom: 20,
            left: 26,
        };
        let overlays = vec![
            StaticOverlay::Path(biei_core::types::PathOverlay {
                stroke_width: None,
                stroke_color: None,
                stroke_opacity: None,
                fill_color: None,
                fill_opacity: None,
                coordinates: vec![
                    LngLat {
                        lon: 139.767,
                        lat: 35.0,
                    },
                    LngLat {
                        lon: 139.767,
                        lat: 35.681,
                    },
                ],
            }),
            pin(PinSize::Large),
        ];

        assert_eq!(
            auto_padding_for_overlays(base, &overlays, 300, 190),
            Padding {
                top: 46,
                right: 26,
                bottom: 20,
                left: 26,
            }
        );
    }

    #[test]
    fn auto_padding_ignores_non_pin_overlays() {
        let base = Padding::all(10);
        assert_eq!(auto_padding_for_overlays(base, &[], 300, 190), base);
    }

    #[test]
    fn auto_padding_only_counts_pins_on_bounds_edges() {
        let base = Padding::all(10);
        let overlays = vec![
            pin(PinSize::Small),
            StaticOverlay::Path(biei_core::types::PathOverlay {
                stroke_width: None,
                stroke_color: None,
                stroke_opacity: None,
                fill_color: None,
                fill_opacity: None,
                coordinates: vec![
                    LngLat {
                        lon: 138.0,
                        lat: 36.0,
                    },
                    LngLat {
                        lon: 140.0,
                        lat: 36.0,
                    },
                ],
            }),
        ];

        assert_eq!(
            auto_padding_for_overlays(base, &overlays, 300, 190),
            Padding {
                top: 10,
                right: 10,
                bottom: 10,
                left: 10,
            }
        );
    }

    #[test]
    fn auto_padding_counts_pins_near_bounds_edges() {
        let base = Padding::all(10);
        let overlays = vec![
            StaticOverlay::GeoJson(biei_core::types::GeoJsonOverlay {
                feature_collection: serde_json::json!({
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-122.4111, 37.770025],
                            [-122.372037, 37.738775],
                            [-122.309537, 37.762213],
                            [-122.270475, 37.801275],
                            [-122.293912, 37.863775],
                            [-122.340787, 37.895025],
                            [-122.395475, 37.84815],
                            [-122.4111, 37.770025]
                        ]]
                    },
                    "properties": {}
                }),
            }),
            StaticOverlay::Pin(PinOverlay {
                size: PinSize::Small,
                label: None,
                color: "4682b4".to_string(),
                coordinate: LngLat {
                    lon: -122.4486,
                    lat: 37.8269,
                },
            }),
            StaticOverlay::Pin(PinOverlay {
                size: PinSize::Small,
                label: None,
                color: "4682b4".to_string(),
                coordinate: LngLat {
                    lon: -122.54,
                    lat: 36.7761,
                },
            }),
        ];

        let padding = auto_padding_for_overlays(base, &overlays, 300, 190);
        assert!(
            padding.top > base.top,
            "pin near the north edge needs extra top padding"
        );
        assert_eq!(padding.right, base.right);
        assert_eq!(padding.left, base.left);
    }
}
