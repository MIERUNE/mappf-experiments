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
        Err(OverlayParseError::InvalidPinSize)
    );
    assert_eq!(
        parse_pin_overlay("pin-s-aa+9ed4bd(139,35)").map(|_| ()),
        Err(OverlayParseError::InvalidPinLabel)
    );
    assert_eq!(
        parse_pin_overlay("pin-s+ff(139,35)").map(|_| ()),
        Err(OverlayParseError::InvalidColor)
    );
    assert_eq!(
        parse_pin_overlay("pin-s+9ed4bd(200,35)").map(|_| ()),
        Err(OverlayParseError::CoordinateOutOfRange)
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
        Err(OverlayParseError::InvalidStrokeWidth)
    );
    assert_eq!(
        parse_path_overlay(r"path-5+ff(_p~iF~ps%7CU)").map(|_| ()),
        Err(OverlayParseError::InvalidColor)
    );
    assert_eq!(
        parse_path_overlay(r"path-5+f44-1.2(_p~iF~ps%7CU)").map(|_| ()),
        Err(OverlayParseError::InvalidOpacity)
    );
}

#[test]
fn rejects_bad_percent_encoding() {
    assert_eq!(
        parse_path_overlay("path-5+f44(%7X)").map(|overlay| overlay.coordinates),
        Err(OverlayParseError::InvalidPercentEncoding)
    );
}

#[test]
fn rejects_malformed_path_overlay() {
    assert_eq!(
        parse_path_overlay("path-5+f44").map(|overlay| overlay.coordinates),
        Err(OverlayParseError::InvalidPathSyntax)
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
    assert_eq!(decode_polyline(""), Err(OverlayParseError::Empty));
}

#[test]
fn rejects_too_many_path_points() {
    let encoded = "??".repeat(MAX_PATH_POINTS + 1);

    assert_eq!(
        decode_polyline(&encoded),
        Err(OverlayParseError::TooManyPoints)
    );
}

#[test]
fn rejects_coordinates_out_of_range() {
    let encoded = encode_point(9_100_000, 0);

    assert_eq!(
        decode_polyline(&encoded),
        Err(OverlayParseError::CoordinateOutOfRange)
    );
}

#[test]
fn rejects_invalid_byte() {
    assert_eq!(
        decode_polyline("\n"),
        Err(OverlayParseError::InvalidByte {
            byte: b'\n',
            index: 0
        })
    );
}

#[test]
fn rejects_truncated_coordinate_pair() {
    assert_eq!(decode_polyline("?"), Err(OverlayParseError::Truncated));
}

#[test]
fn rejects_unterminated_varint() {
    assert_eq!(decode_polyline("_____"), Err(OverlayParseError::Truncated));
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
    let json =
        r#"{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[139,35]}}"#;
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
        Err(OverlayParseError::UnsupportedGeoJsonType)
    );
}

#[test]
fn rejects_geojson_with_invalid_json() {
    let overlay = format!("geojson({})", pct_encode("{not json"));

    assert_eq!(
        parse_geojson_overlay(&overlay),
        Err(OverlayParseError::InvalidGeoJsonSyntax)
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
        Err(OverlayParseError::TooManyFeatures)
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
        Err(OverlayParseError::TooManyCoordinates)
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
    let json =
        r#"{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":["a","b"]}}"#;
    let overlay = format!("geojson({})", pct_encode(json));

    assert_eq!(
        parse_geojson_overlay(&overlay),
        Err(OverlayParseError::InvalidGeoJsonSyntax)
    );
}

#[test]
fn rejects_geojson_with_out_of_range_coordinate() {
    let json =
        r#"{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[200,0]}}"#;
    let overlay = format!("geojson({})", pct_encode(json));

    assert_eq!(
        parse_geojson_overlay(&overlay),
        Err(OverlayParseError::CoordinateOutOfRange)
    );
}

#[test]
fn rejects_geojson_with_nan_coordinate() {
    // JSON has no NaN literal — but Polygon with empty position triggers
    // the same "non-finite / malformed position" branch.
    let json =
        r#"{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[0]]]}}"#;
    let overlay = format!("geojson({})", pct_encode(json));

    assert_eq!(
        parse_geojson_overlay(&overlay),
        Err(OverlayParseError::InvalidGeoJsonSyntax)
    );
}

#[test]
fn rejects_unknown_geometry_type() {
    // Made-up `Circle` geometry — not in the simplestyle whitelist. Must
    // be rejected at ingress so renderer never sees it.
    let json =
        r#"{"type":"Feature","properties":{},"geometry":{"type":"Circle","coordinates":[0,0]}}"#;
    let overlay = format!("geojson({})", pct_encode(json));

    assert_eq!(
        parse_geojson_overlay(&overlay),
        Err(OverlayParseError::UnsupportedGeoJsonType)
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
        Err(OverlayParseError::UnsupportedGeoJsonType)
    );
}

#[test]
fn parse_static_overlays_accepts_up_to_max_overlays() {
    let path = r"path-5+f44(_p~iF~ps%7CU)";
    // exactly MAX_STATIC_OVERLAYS items
    let url: String = std::iter::repeat_n(path, MAX_STATIC_OVERLAYS)
        .collect::<Vec<_>>()
        .join(",");

    let parsed = parse_static_overlays(&url).expect("MAX_STATIC_OVERLAYS exactly should parse");
    assert_eq!(parsed.len(), MAX_STATIC_OVERLAYS);
}

#[test]
fn parse_static_overlays_rejects_more_than_max_overlays() {
    let path = r"path-5+f44(_p~iF~ps%7CU)";
    let url: String = std::iter::repeat_n(path, MAX_STATIC_OVERLAYS + 1)
        .collect::<Vec<_>>()
        .join(",");

    assert_eq!(
        parse_static_overlays(&url),
        Err(OverlayParseError::TooManyOverlays)
    );
}
