use super::pin::pin_icon_offset_y;
use super::*;
use biei_core::types::PinSize;

fn pt(lon: f64, lat: f64) -> LngLat {
    LngLat { lon, lat }
}

fn bare_path(coordinates: Vec<LngLat>) -> PathOverlay {
    PathOverlay {
        stroke_width: None,
        stroke_color: None,
        stroke_opacity: None,
        fill_color: None,
        fill_opacity: None,
        coordinates,
    }
}

fn pin(label: Option<&str>) -> PinOverlay {
    PinOverlay {
        size: PinSize::Small,
        label: label.map(str::to_string),
        color: "9ed4bd".to_string(),
        coordinate: pt(139.0, 35.0),
    }
}

#[test]
fn css_color_prepends_hash() {
    assert_eq!(css_color("f44"), "#f44");
    assert_eq!(css_color("00ffcc"), "#00ffcc");
}

#[test]
fn path_stroke_properties_omit_unset_fields() {
    let v = path_stroke_properties(&bare_path(vec![]));
    assert!(v.as_object().expect("object").is_empty());
}

#[test]
fn path_stroke_properties_include_set_fields_with_hash_prefixed_color() {
    let path = PathOverlay {
        stroke_width: Some(3.0),
        stroke_color: Some("f44".to_string()),
        stroke_opacity: Some(0.5),
        fill_color: None,
        fill_opacity: None,
        coordinates: vec![],
    };
    let v = path_stroke_properties(&path);
    assert_eq!(v.get("stroke").and_then(|v| v.as_str()), Some("#f44"));
    assert_eq!(v.get("stroke-width").and_then(|v| v.as_f64()), Some(3.0));
    assert_eq!(v.get("stroke-opacity").and_then(|v| v.as_f64()), Some(0.5));
}

#[test]
fn path_fill_properties_omit_unset_fields() {
    let v = path_fill_properties(&bare_path(vec![]));
    assert!(v.as_object().expect("object").is_empty());
}

#[test]
fn path_fill_properties_include_set_fields_with_hash_prefixed_color() {
    let path = PathOverlay {
        stroke_width: None,
        stroke_color: None,
        stroke_opacity: None,
        fill_color: Some("00ffcc".to_string()),
        // 0.5 is exactly representable as f32, so the f32→f64 round-
        // trip is lossless and we can compare with assert_eq!.
        fill_opacity: Some(0.5),
        coordinates: vec![],
    };
    let v = path_fill_properties(&path);
    assert_eq!(v.get("fill").and_then(|v| v.as_str()), Some("#00ffcc"));
    assert_eq!(v.get("fill-opacity").and_then(|v| v.as_f64()), Some(0.5));
}

#[test]
fn coordinates_json_preserves_input_order() {
    let v = coordinates_json(&[pt(0.0, 0.0), pt(1.0, 2.0), pt(-3.0, 4.0)]);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0], serde_json::json!([0.0, 0.0]));
    assert_eq!(v[1], serde_json::json!([1.0, 2.0]));
    assert_eq!(v[2], serde_json::json!([-3.0, 4.0]));
}

#[test]
fn path_features_emits_only_linestring_when_no_fill() {
    let path = PathOverlay {
        stroke_width: Some(5.0),
        stroke_color: Some("f44".to_string()),
        stroke_opacity: None,
        fill_color: None,
        fill_opacity: None,
        coordinates: vec![pt(0.0, 0.0), pt(1.0, 1.0)],
    };
    let features = path_features(&path);
    assert_eq!(features.len(), 1);
    assert_eq!(features[0]["geometry"]["type"], "LineString");
    assert_eq!(features[0]["properties"]["stroke"], "#f44");
}

#[test]
fn path_features_emits_polygon_when_fill_and_three_or_more_coords() {
    let path = PathOverlay {
        stroke_width: None,
        stroke_color: None,
        stroke_opacity: None,
        fill_color: Some("0c8".to_string()),
        fill_opacity: Some(0.5),
        coordinates: vec![pt(0.0, 0.0), pt(1.0, 0.0), pt(0.5, 1.0)],
    };
    let features = path_features(&path);
    assert_eq!(features.len(), 2);
    assert_eq!(features[1]["geometry"]["type"], "Polygon");
    assert_eq!(features[1]["properties"]["fill"], "#0c8");
    // Polygon outer ring must close to form valid GeoJSON.
    let ring = features[1]["geometry"]["coordinates"][0]
        .as_array()
        .expect("polygon outer ring");
    assert_eq!(ring.first(), ring.last(), "polygon ring should be closed");
}

#[test]
fn path_features_keeps_already_closed_ring_intact() {
    let path = PathOverlay {
        stroke_width: None,
        stroke_color: None,
        stroke_opacity: None,
        fill_color: Some("0c8".to_string()),
        fill_opacity: None,
        // First == last already.
        coordinates: vec![pt(0.0, 0.0), pt(1.0, 0.0), pt(0.5, 1.0), pt(0.0, 0.0)],
    };
    let features = path_features(&path);
    let ring = features[1]["geometry"]["coordinates"][0]
        .as_array()
        .expect("polygon outer ring");
    assert_eq!(ring.len(), 4, "no spurious closing vertex appended");
}

#[test]
fn path_features_skips_polygon_when_fewer_than_three_coords() {
    let path = PathOverlay {
        stroke_width: None,
        stroke_color: None,
        stroke_opacity: None,
        fill_color: Some("0c8".to_string()),
        fill_opacity: None,
        coordinates: vec![pt(0.0, 0.0), pt(1.0, 1.0)],
    };
    let features = path_features(&path);
    // fill_color set but only 2 coords: only LineString, no Polygon.
    assert_eq!(features.len(), 1);
    assert_eq!(features[0]["geometry"]["type"], "LineString");
}

#[test]
fn geojson_features_extracts_from_feature_collection() {
    let overlay = GeoJsonOverlay {
        feature_collection: serde_json::json!({
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [0, 0]}},
                {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [1, 1]}}
            ]
        }),
    };
    let features = geojson_features(&overlay);
    assert_eq!(features.len(), 2);
}

#[test]
fn geojson_features_wraps_single_feature() {
    let overlay = GeoJsonOverlay {
        feature_collection: serde_json::json!({
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Point", "coordinates": [0, 0]}
        }),
    };
    let features = geojson_features(&overlay);
    assert_eq!(features.len(), 1);
    assert_eq!(features[0]["type"], "Feature");
}

#[test]
fn pin_features_emit_symbol_marker_point() {
    let features = pin_features(&pin(Some("a")));

    assert_eq!(features.len(), 1);
    assert_eq!(features[0]["geometry"]["type"], "Point");
    assert_eq!(features[0]["properties"][PIN_KIND_PROPERTY], PIN_KIND_VALUE);
    assert_eq!(
        features[0]["properties"][PIN_IMAGE_PROPERTY],
        "biei-pin-s-a-9ed4bd-x2"
    );
    assert_eq!(
        features[0]["properties"][PIN_OFFSET_PROPERTY],
        serde_json::json!([0.0, pin_icon_offset_y(PinSize::Small)])
    );
}

#[test]
fn inject_overlay_idx_stamps_index_on_existing_properties_object() {
    let mut features = vec![serde_json::json!({
        "type": "Feature",
        "properties": {"stroke": "#f44"},
        "geometry": {"type": "Point", "coordinates": [0, 0]}
    })];
    inject_overlay_idx(&mut features, 3);
    assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 3);
    // Pre-existing properties are preserved.
    assert_eq!(features[0]["properties"]["stroke"], "#f44");
}

#[test]
fn inject_overlay_idx_creates_properties_when_missing_or_null() {
    let mut features = vec![
        // Missing properties.
        serde_json::json!({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]}
        }),
        // Null properties (valid GeoJSON).
        serde_json::json!({
            "type": "Feature",
            "properties": null,
            "geometry": {"type": "Point", "coordinates": [0, 0]}
        }),
    ];
    inject_overlay_idx(&mut features, 7);
    for f in &features {
        assert_eq!(f["properties"]["__biei_overlay_idx"], 7);
    }
}

#[test]
fn build_union_features_concatenates_with_overlay_idx_per_feature() {
    let path = PathOverlay {
        stroke_width: None,
        stroke_color: Some("f44".to_string()),
        stroke_opacity: None,
        fill_color: None,
        fill_opacity: None,
        coordinates: vec![pt(0.0, 0.0), pt(1.0, 1.0)],
    };
    let geojson = GeoJsonOverlay {
        feature_collection: serde_json::json!({
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [2, 2]}},
                {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [3, 3]}}
            ]
        }),
    };
    let overlays = vec![StaticOverlay::Path(path), StaticOverlay::GeoJson(geojson)];
    let features = build_union_features(&overlays);

    // Path emits 1 feature (LineString, no fill), GeoJSON emits 2.
    assert_eq!(features.len(), 3);
    assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 0);
    assert_eq!(features[0]["geometry"]["type"], "LineString");
    assert_eq!(features[1]["properties"]["__biei_overlay_idx"], 1);
    assert_eq!(features[2]["properties"]["__biei_overlay_idx"], 1);
}

#[test]
fn indexed_union_feature_collection_preserves_geometry_and_properties() {
    let geometry = serde_json::json!({
        "type": "Polygon",
        "coordinates": [[
            [142.0, 43.0],
            [142.1, 43.0],
            [142.1, 43.1],
            [142.0, 43.0]
        ]]
    });
    let overlay = GeoJsonOverlay {
        feature_collection: serde_json::json!({
            "type": "Feature",
            "properties": {
                "name": "Shikisai Hill",
                "fill": "#9ed4bd"
            },
            "geometry": geometry.clone()
        }),
    };

    let collection = build_union_feature_collection(&[StaticOverlay::GeoJson(overlay)]);
    let features = collection["features"].as_array().expect("features array");

    assert_eq!(collection["type"], "FeatureCollection");
    assert_eq!(features.len(), 1);
    assert_eq!(features[0]["geometry"], geometry);
    assert_eq!(features[0]["properties"]["name"], "Shikisai Hill");
    assert_eq!(features[0]["properties"]["fill"], "#9ed4bd");
    assert_eq!(features[0]["properties"][OVERLAY_IDX_PROPERTY], 0);
}

#[test]
fn indexed_union_feature_collection_serializes_and_parses_for_native() {
    let overlays = [StaticOverlay::Path(PathOverlay {
        stroke_width: Some(2.0),
        stroke_color: Some("f44".to_string()),
        stroke_opacity: Some(0.75),
        fill_color: None,
        fill_opacity: None,
        coordinates: vec![pt(142.0, 43.0), pt(142.1, 43.1)],
    })];

    build_overlay_geojson(&overlays).expect("indexed union should parse as native GeoJSON");

    let collection = build_union_feature_collection(&overlays);
    assert_eq!(
        collection["features"][0]["geometry"]["coordinates"],
        serde_json::json!([[142.0, 43.0], [142.1, 43.1]])
    );
    assert_eq!(collection["features"][0]["properties"]["stroke"], "#f44");
    assert_eq!(collection["features"][0]["properties"]["stroke-width"], 2.0);
    assert_eq!(
        collection["features"][0]["properties"][OVERLAY_IDX_PROPERTY],
        0
    );
}

#[test]
fn consecutive_stroke_only_paths_share_one_slot() {
    let a = StaticOverlay::Path(bare_path(vec![pt(0.0, 0.0), pt(1.0, 1.0)]));
    let b = StaticOverlay::Path(bare_path(vec![pt(2.0, 2.0), pt(3.0, 3.0)]));
    let features = build_union_features(&[a, b]);

    assert_eq!(
        overlay_slot_count(&[
            StaticOverlay::Path(bare_path(vec![pt(0.0, 0.0), pt(1.0, 1.0)])),
            StaticOverlay::Path(bare_path(vec![pt(2.0, 2.0), pt(3.0, 3.0)])),
        ]),
        1
    );
    assert_eq!(features.len(), 2);
    assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 0);
    assert_eq!(features[1]["properties"]["__biei_overlay_idx"], 0);
}

#[test]
fn fill_paths_keep_slot_boundaries_to_preserve_z_order() {
    let stroke_only = StaticOverlay::Path(bare_path(vec![pt(0.0, 0.0), pt(1.0, 1.0)]));
    let filled = StaticOverlay::Path(PathOverlay {
        stroke_width: None,
        stroke_color: None,
        stroke_opacity: None,
        fill_color: Some("0c8".to_string()),
        fill_opacity: None,
        coordinates: vec![pt(0.0, 0.0), pt(1.0, 0.0), pt(0.5, 1.0)],
    });
    let later_stroke = StaticOverlay::Path(bare_path(vec![pt(2.0, 2.0), pt(3.0, 3.0)]));
    let overlays = vec![stroke_only, filled, later_stroke];
    let features = build_union_features(&overlays);

    assert_eq!(overlay_slot_count(&overlays), 3);
    assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 0);
    assert_eq!(features[1]["properties"]["__biei_overlay_idx"], 1);
    assert_eq!(features[2]["properties"]["__biei_overlay_idx"], 1);
    assert_eq!(features[3]["properties"]["__biei_overlay_idx"], 2);
}

#[test]
fn pin_breaks_path_run_and_keeps_own_slot() {
    let overlays = vec![
        StaticOverlay::Path(bare_path(vec![pt(0.0, 0.0), pt(1.0, 1.0)])),
        StaticOverlay::Pin(pin(None)),
        StaticOverlay::Path(bare_path(vec![pt(2.0, 2.0), pt(3.0, 3.0)])),
    ];
    let features = build_union_features(&overlays);

    assert_eq!(overlay_slot_count(&overlays), 3);
    assert_eq!(features[0]["properties"]["__biei_overlay_idx"], 0);
    assert_eq!(features[1]["properties"]["__biei_overlay_idx"], 1);
    assert_eq!(features[1]["properties"][PIN_KIND_PROPERTY], PIN_KIND_VALUE);
    assert_eq!(features[2]["properties"]["__biei_overlay_idx"], 2);
}

#[test]
fn slot_layer_ids_follow_indexed_naming() {
    let ids = slot_layer_ids(5);
    assert_eq!(ids[0], "biei-overlay-5-fill");
    assert_eq!(ids[1], "biei-overlay-5-line");
    assert_eq!(ids[2], "biei-overlay-5-circle");
    assert_eq!(ids[3], "biei-overlay-5-symbol");
}

#[test]
fn slot_layer_jsons_parse_and_target_shared_source() {
    for (kind, json) in [
        ("fill", slot_fill_layer_json(7)),
        ("line", slot_line_layer_json(7)),
        ("circle", slot_circle_layer_json(7)),
        ("symbol", slot_symbol_layer_json(7)),
    ] {
        let v: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|_| panic!("{kind} layer JSON must parse"));
        assert_eq!(v["type"].as_str(), Some(kind));
        // Every slot's layer references the SINGLE shared source.
        assert_eq!(v["source"].as_str(), Some(OVERLAY_SOURCE_ID));
        assert!(
            v["paint"].is_object() || v["layout"].is_object(),
            "{kind} layer needs paint or layout props"
        );
        assert_eq!(
            v["id"].as_str(),
            Some(format!("biei-overlay-7-{kind}").as_str())
        );
    }
}

#[test]
fn slot_fill_layer_filter_scopes_to_polygon_and_overlay_idx() {
    let v: serde_json::Value = serde_json::from_str(&slot_fill_layer_json(2)).unwrap();
    // ["all", ["==", ["geometry-type"], "Polygon"], ["==", ["get", "__biei_overlay_idx"], 2]]
    let filter = &v["filter"];
    assert_eq!(filter[0], "all");
    assert_eq!(
        filter[1],
        serde_json::json!(["==", ["geometry-type"], "Polygon"])
    );
    assert_eq!(
        filter[2],
        serde_json::json!(["==", ["get", "__biei_overlay_idx"], 2])
    );
}

#[test]
fn slot_line_layer_filter_accepts_linestring_and_polygon_for_overlay_idx() {
    let v: serde_json::Value = serde_json::from_str(&slot_line_layer_json(0)).unwrap();
    let filter = &v["filter"];
    assert_eq!(filter[0], "all");
    // geometry-type clause accepts LineString OR Polygon (the latter for
    // polygon stroke).
    assert_eq!(filter[1][0], "match");
    assert_eq!(filter[1][1][0], "geometry-type");
    assert_eq!(filter[1][2], serde_json::json!(["LineString", "Polygon"]));
    assert_eq!(
        filter[2],
        serde_json::json!(["==", ["get", "__biei_overlay_idx"], 0])
    );
    // DDS line-color reads `stroke`.
    let line_color = &v["paint"]["line-color"];
    assert_eq!(line_color[0], "coalesce");
    assert_eq!(line_color[1][0], "to-color");
    assert_eq!(line_color[1][1][1], "stroke");
}

#[test]
fn slot_circle_layer_filter_scopes_to_point_and_overlay_idx() {
    let v: serde_json::Value = serde_json::from_str(&slot_circle_layer_json(4)).unwrap();
    let filter = &v["filter"];
    assert_eq!(filter[0], "all");
    assert_eq!(
        filter[1],
        serde_json::json!(["==", ["geometry-type"], "Point"])
    );
    assert_eq!(
        filter[2],
        serde_json::json!(["==", ["get", "__biei_overlay_idx"], 4])
    );
    assert_eq!(
        filter[3],
        serde_json::json!(["!=", ["get", PIN_KIND_PROPERTY], PIN_KIND_VALUE])
    );
    // DDS circle-color reads `marker-color`.
    let circle_color = &v["paint"]["circle-color"];
    assert_eq!(circle_color[0], "coalesce");
    assert_eq!(circle_color[1][1][1], "marker-color");
}

#[test]
fn slot_layer_json_picks_the_right_template() {
    let fill = slot_layer_json(2, 0).expect("0 -> fill");
    let line = slot_layer_json(2, 1).expect("1 -> line");
    let circle = slot_layer_json(2, 2).expect("2 -> circle");
    let symbol = slot_layer_json(2, 3).expect("3 -> symbol");
    assert!(fill.contains("biei-overlay-2-fill"));
    assert!(line.contains("biei-overlay-2-line"));
    assert!(circle.contains("biei-overlay-2-circle"));
    assert!(symbol.contains("biei-overlay-2-symbol"));
    let fv: serde_json::Value = serde_json::from_str(&fill).unwrap();
    let lv: serde_json::Value = serde_json::from_str(&line).unwrap();
    let cv: serde_json::Value = serde_json::from_str(&circle).unwrap();
    let sv: serde_json::Value = serde_json::from_str(&symbol).unwrap();
    assert_eq!(fv["type"], "fill");
    assert_eq!(lv["type"], "line");
    assert_eq!(cv["type"], "circle");
    assert_eq!(sv["type"], "symbol");
    assert!(slot_layer_json(2, 4).is_none());
}

#[test]
fn slot_symbol_layer_targets_pin_features() {
    let v: serde_json::Value = serde_json::from_str(&slot_symbol_layer_json(4)).unwrap();
    assert_eq!(v["type"], "symbol");
    assert_eq!(
        v["filter"],
        serde_json::json!([
            "all",
            ["==", ["geometry-type"], "Point"],
            ["==", ["get", OVERLAY_IDX_PROPERTY], 4],
            ["==", ["get", PIN_KIND_PROPERTY], PIN_KIND_VALUE]
        ])
    );
    assert_eq!(
        v["layout"]["icon-image"],
        serde_json::json!(["get", PIN_IMAGE_PROPERTY])
    );
    assert_eq!(
        v["layout"]["icon-offset"],
        serde_json::json!(["get", PIN_OFFSET_PROPERTY])
    );
}
