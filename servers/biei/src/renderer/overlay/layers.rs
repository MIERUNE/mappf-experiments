use super::{
    OVERLAY_IDX_PROPERTY, OVERLAY_SOURCE_ID, PIN_IMAGE_PROPERTY, PIN_KIND_PROPERTY, PIN_KIND_VALUE,
    PIN_OFFSET_PROPERTY,
};

pub(super) fn slot_layer_ids(idx: usize) -> [String; 4] {
    [
        slot_fill_layer_id(idx),
        slot_line_layer_id(idx),
        slot_circle_layer_id(idx),
        slot_symbol_layer_id(idx),
    ]
}

fn slot_fill_layer_id(idx: usize) -> String {
    format!("biei-overlay-{idx}-fill")
}

fn slot_line_layer_id(idx: usize) -> String {
    format!("biei-overlay-{idx}-line")
}

fn slot_circle_layer_id(idx: usize) -> String {
    format!("biei-overlay-{idx}-circle")
}

fn slot_symbol_layer_id(idx: usize) -> String {
    format!("biei-overlay-{idx}-symbol")
}

pub(super) fn slot_fill_layer_json(idx: usize) -> String {
    serde_json::json!({
        "id": slot_fill_layer_id(idx),
        "type": "fill",
        "source": OVERLAY_SOURCE_ID,
        "filter": ["all",
            ["==", ["geometry-type"], "Polygon"],
            ["==", ["get", OVERLAY_IDX_PROPERTY], idx]
        ],
        "paint": {
            "fill-color":   ["coalesce", ["to-color", ["get", "fill"]], "#555555"],
            "fill-opacity": ["coalesce", ["number",   ["get", "fill-opacity"]], 0.6],
        }
    })
    .to_string()
}

pub(super) fn slot_line_layer_json(idx: usize) -> String {
    serde_json::json!({
        "id": slot_line_layer_id(idx),
        "type": "line",
        "source": OVERLAY_SOURCE_ID,
        "filter": ["all",
            ["match", ["geometry-type"], ["LineString", "Polygon"], true, false],
            ["==", ["get", OVERLAY_IDX_PROPERTY], idx]
        ],
        "paint": {
            "line-color":   ["coalesce", ["to-color", ["get", "stroke"]], "#555555"],
            "line-width":   ["coalesce", ["number",   ["get", "stroke-width"]], 2],
            "line-opacity": ["coalesce", ["number",   ["get", "stroke-opacity"]], 1.0],
        },
        "layout": {
            "line-cap":  "round",
            "line-join": "miter",
            "line-miter-limit": 0,
        }
    })
    .to_string()
}

pub(super) fn slot_circle_layer_json(idx: usize) -> String {
    serde_json::json!({
        "id": slot_circle_layer_id(idx),
        "type": "circle",
        "source": OVERLAY_SOURCE_ID,
        "filter": ["all",
            ["==", ["geometry-type"], "Point"],
            ["==", ["get", OVERLAY_IDX_PROPERTY], idx],
            ["!=", ["get", PIN_KIND_PROPERTY], PIN_KIND_VALUE]
        ],
        "paint": {
            "circle-color":   ["coalesce", ["to-color", ["get", "marker-color"]], "#7e7e7e"],
            "circle-radius":  ["match", ["get", "marker-size"],
                                "small", 5,
                                "large", 10,
                                7],
            "circle-stroke-color": "#ffffff",
            "circle-stroke-width": 1,
            "circle-opacity": ["coalesce", ["number", ["get", "marker-opacity"]], 1.0],
        }
    })
    .to_string()
}

pub(super) fn slot_symbol_layer_json(idx: usize) -> String {
    serde_json::json!({
        "id": slot_symbol_layer_id(idx),
        "type": "symbol",
        "source": OVERLAY_SOURCE_ID,
        "filter": ["all",
            ["==", ["geometry-type"], "Point"],
            ["==", ["get", OVERLAY_IDX_PROPERTY], idx],
            ["==", ["get", PIN_KIND_PROPERTY], PIN_KIND_VALUE]
        ],
        "layout": {
            "icon-image": ["get", PIN_IMAGE_PROPERTY],
            "icon-anchor": "bottom",
            "icon-offset": ["get", PIN_OFFSET_PROPERTY],
            "icon-allow-overlap": true,
            "icon-ignore-placement": true,
        }
    })
    .to_string()
}
