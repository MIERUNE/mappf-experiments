//! Preview HTML and MapLibre style generation for a tileset.

use std::hash::Hasher;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::debug;
use twox_hash::XxHash64;

use crate::server::{
    AppState, HttpError, apply_origin_vary, auth::PropagatedAccessToken, cache, get_origin,
};
use ishikari_core::{interned::TilesetId, pmtiles::TileType, storage::TilesetInfo};

use super::error::tileset_error_response;

// 6.x (prerelease) bundles @maplibre/mlt >= 1.1.9, whose MLT decoder handles
// the non-nullable struct fields ishikari emits; 5.x ships the broken 1.1.8.
// 6.x is ESM-only (no UMD global), so preview.html loads it as a module.
const MAPLIBRE_GL_VERSION: &str = "6.0.0-17";
const PREVIEW_HTML_TEMPLATE: &str = include_str!("preview.html");

#[derive(Deserialize)]
pub(crate) struct PreviewQuery {
    encoding: Option<String>,
}

#[derive(Clone, Copy)]
enum DemEncoding {
    Terrarium,
    TerrainRgb,
}

impl DemEncoding {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "terrarium" => Some(Self::Terrarium),
            "terrainrgb" => Some(Self::TerrainRgb),
            _ => None,
        }
    }

    fn maplibre_encoding(self) -> &'static str {
        match self {
            Self::Terrarium => "terrarium",
            Self::TerrainRgb => "mapbox",
        }
    }
}

/// Serves the HTML preview shell for a flat tileset key.
pub(crate) async fn preview_handler(
    State(state): State<AppState>,
    Path(tileset_id): Path<String>,
    Query(query): Query<PreviewQuery>,
    token: Option<Extension<PropagatedAccessToken>>,
) -> Result<([(header::HeaderName, &'static str); 1], Html<String>), HttpError> {
    serve_preview(state, tileset_id, query, token.map(|value| value.0)).await
}

/// Serves the HTML preview shell for a `{namespace}/{tileset_id}` key.
pub(crate) async fn namespaced_preview_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id)): Path<(String, String)>,
    Query(query): Query<PreviewQuery>,
    token: Option<Extension<PropagatedAccessToken>>,
) -> Result<([(header::HeaderName, &'static str); 1], Html<String>), HttpError> {
    serve_preview(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        query,
        token.map(|value| value.0),
    )
    .await
}

/// Renders the preview HTML for an already-joined tileset key.
async fn serve_preview(
    state: AppState,
    tileset_id: String,
    query: PreviewQuery,
    token: Option<PropagatedAccessToken>,
) -> Result<([(header::HeaderName, &'static str); 1], Html<String>), HttpError> {
    let tileset_id = TilesetId::try_from(tileset_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let info = state
        .resource_resolver
        .load_tileset_info(tileset_id.clone())
        .await
        .map_err(|e| tileset_error_response(&e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tileset not found".to_string()))?;
    let is_mapterhorn = state
        .mapterhorn()
        .is_some_and(|resolver| resolver.matches(&tileset_id));
    let html = preview_html(
        &tileset_id,
        query.encoding.as_deref(),
        info.header.tile_type,
        is_mapterhorn,
        token.as_ref(),
    );
    debug!(
        endpoint = "preview",
        tileset_id = %tileset_id,
        served_bytes = html.len(),
        "served external response"
    );
    Ok(([(header::CACHE_CONTROL, cache::PREVIEW)], Html(html)))
}

/// Serves the generated preview style for a flat tileset key.
pub(crate) async fn preview_style_handler(
    State(state): State<AppState>,
    Path(tileset_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<PreviewQuery>,
    token: Option<Extension<PropagatedAccessToken>>,
) -> Result<Response, HttpError> {
    serve_preview_style(
        state,
        tileset_id,
        headers,
        query,
        token.map(|value| value.0),
    )
    .await
}

/// Serves the generated preview style for a `{namespace}/{tileset_id}` key.
pub(crate) async fn namespaced_preview_style_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<PreviewQuery>,
    token: Option<Extension<PropagatedAccessToken>>,
) -> Result<Response, HttpError> {
    serve_preview_style(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        headers,
        query,
        token.map(|value| value.0),
    )
    .await
}

/// Builds the preview style for an already-joined tileset key.
async fn serve_preview_style(
    state: AppState,
    tileset_id: String,
    headers: HeaderMap,
    query: PreviewQuery,
    token: Option<PropagatedAccessToken>,
) -> Result<Response, HttpError> {
    let tileset_id = TilesetId::try_from(tileset_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let base_url = get_origin(&headers);
    let info = state
        .resource_resolver
        .load_tileset_info(tileset_id.clone())
        .await
        .map_err(|e| tileset_error_response(&e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tileset not found".to_string()))?;
    let maxzoom_override = state
        .mapterhorn()
        .filter(|resolver| resolver.matches(&tileset_id))
        .map(|resolver| resolver.maxzoom());
    let is_mapterhorn = maxzoom_override.is_some();
    let mut style = preview_style(
        &tileset_id,
        &base_url,
        &info,
        query.encoding.as_deref(),
        maxzoom_override,
        is_mapterhorn,
    );
    if let Some(token) = token.as_ref() {
        propagate_generated_urls(&mut style, &base_url, token);
    }
    debug!(
        endpoint = "preview_style",
        tileset_id = %tileset_id,
        "served external response"
    );
    let mut response = ([(header::CACHE_CONTROL, cache::PREVIEW)], Json(style)).into_response();
    apply_origin_vary(response.headers_mut());
    Ok(response)
}

/// Renders the shared MapLibre preview page for any style URL.
pub(crate) fn render_preview_html(
    title: &str,
    style_url: &str,
    terrain_control: &str,
    format_toggle: bool,
    terrain_products: bool,
) -> String {
    PREVIEW_HTML_TEMPLATE
        .replace("__TITLE__", title)
        .replace("__STYLE_URL__", style_url)
        .replace("__MAPLIBRE_GL_VERSION__", MAPLIBRE_GL_VERSION)
        .replace("__TERRAIN_CONTROL__", terrain_control)
        .replace(
            "__FORMAT_TOGGLE__",
            if format_toggle { "true" } else { "false" },
        )
        .replace(
            "__TERRAIN_PRODUCTS__",
            if terrain_products { "true" } else { "false" },
        )
}

fn preview_html(
    tileset_id: &TilesetId,
    encoding: Option<&str>,
    tile_type: TileType,
    is_mapterhorn: bool,
    token: Option<&PropagatedAccessToken>,
) -> String {
    let is_vector = !matches!(
        tile_type,
        TileType::Png | TileType::Jpeg | TileType::Webp | TileType::Avif
    );
    let title = format!("tileset {tileset_id}");
    if is_vector {
        // Vector preview defaults to MVT and offers an MVT/MLT toggle.
        let fmt = if encoding == Some("mlt") {
            "mlt"
        } else {
            "mvt"
        };
        let mut style_url = format!("/tilesets/{tileset_id}/preview.json?encoding={fmt}");
        if let Some(token) = token {
            style_url = token.append_to(&style_url);
        }
        return render_preview_html(&title, &style_url, "", true, false);
    }
    if is_mapterhorn {
        let mut style_url = format!("/tilesets/{tileset_id}/preview.json");
        if let Some(token) = token {
            style_url = token.append_to(&style_url);
        }
        return render_preview_html(&title, &style_url, "", false, true);
    }
    // Raster / raster-dem: `encoding` selects the DEM hillshade scheme, no toggle.
    let mut style_url = match encoding {
        Some(enc) => format!("/tilesets/{tileset_id}/preview.json?encoding={enc}"),
        None => format!("/tilesets/{tileset_id}/preview.json"),
    };
    if let Some(token) = token {
        style_url = token.append_to(&style_url);
    }
    let terrain_control = if encoding.and_then(DemEncoding::from_str).is_some() {
        r#"map.addControl(new maplibregl.TerrainControl({ source: "dem", exaggeration: 1.0 }), "top-right");"#
    } else {
        ""
    };
    render_preview_html(&title, &style_url, terrain_control, false, false)
}

fn propagate_generated_urls(value: &mut Value, base_url: &str, token: &PropagatedAccessToken) {
    match value {
        Value::Array(values) => {
            for value in values {
                propagate_generated_urls(value, base_url, token);
            }
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                propagate_generated_urls(value, base_url, token);
            }
        }
        Value::String(url)
            if url
                .strip_prefix(base_url)
                .is_some_and(|path| path.starts_with("/tilesets/")) =>
        {
            *url = token.append_to(url);
        }
        _ => {}
    }
}

fn preview_style(
    tileset_id: &TilesetId,
    base_url: &str,
    info: &TilesetInfo,
    encoding: Option<&str>,
    maxzoom_override: Option<u8>,
    is_mapterhorn: bool,
) -> Value {
    if is_mapterhorn {
        return preview_style_mapterhorn(
            tileset_id,
            base_url,
            info,
            maxzoom_override.unwrap_or(info.header.max_zoom),
        );
    }
    match info.header.tile_type {
        TileType::Png | TileType::Jpeg | TileType::Webp | TileType::Avif => {
            if let Some(dem) = encoding.and_then(DemEncoding::from_str) {
                preview_style_dem(tileset_id, base_url, info, dem, maxzoom_override)
            } else {
                preview_style_raster(tileset_id, base_url, info, maxzoom_override)
            }
        }
        _ => preview_style_vector(tileset_id, base_url, info, encoding, maxzoom_override),
    }
}

fn preview_style_mapterhorn(
    tileset_id: &TilesetId,
    base_url: &str,
    info: &TilesetInfo,
    maxzoom: u8,
) -> Value {
    let shade_opacity = |shadow| {
        let mut expression = vec![json!("match"), json!(["get", "level"])];
        for (level, opacity) in super::terrain::hillshade_opacity_stops(shadow) {
            expression.push(json!(level));
            expression.push(json!(opacity));
        }
        expression.push(json!(0));
        Value::Array(expression)
    };
    let shadow_opacity = shade_opacity(true);
    let highlight_opacity = shade_opacity(false);
    let terrain_tiles = format!("{base_url}/tilesets/{tileset_id}/{{z}}/{{x}}/{{y}}.webp");
    let derived_tiles = |product: &str| {
        format!("{base_url}/tilesets/{tileset_id}/derived/{product}/{{z}}/{{x}}/{{y}}.mvt")
    };
    // Neutral shade rasters (image tiles, no `.mvt`): the byte carries the
    // signed tone code, recolored client-side by the color-relief ramp below.
    let shade_tiles = |product: &str| {
        format!("{base_url}/tilesets/{tileset_id}/derived/{product}/{{z}}/{{x}}/{{y}}")
    };
    // The shade byte is a grayscale (R=G=B) value; decode it through the
    // standard Terrarium unpack (color-relief's custom encoding does not
    // evaluate correctly in the GPU shader). Because all channels are equal,
    // Terrarium's high-byte sensitivity is harmless even for lossy codecs: a
    // small byte error maps to a small, sub-level elevation error.
    let scale = super::terrain::hillshade_shade_code_scale();
    let terrarium_elevation = |code: f64| -> f64 {
        let byte = (128.0 + code * scale).round().clamp(0.0, 255.0);
        byte * 256.0 + byte + byte / 256.0 - 32768.0
    };
    let raster_dem_shade = |product: &str| {
        json!({
            "type": "raster-dem",
            "tiles": [shade_tiles(product)],
            "minzoom": info.header.min_zoom,
            "maxzoom": maxzoom,
            "tileSize": 512,
            "encoding": "terrarium"
        })
    };
    // Reproduce the vector fills' appearance from a single ramp over the signed
    // tone code: shadow color fading in toward negative codes, transparent at
    // neutral, highlight (white) toward positive. Opacities are the same CIE
    // L*-derived stops the vector style uses.
    let color_relief_color = {
        // Stops are placed at each level's Terrarium-decoded elevation, so the
        // ramp reproduces the vector fills' CIE L*-derived opacities.
        let mut stops: Vec<(f64, String)> = Vec::new();
        for (level, opacity) in super::terrain::hillshade_opacity_stops(true) {
            stops.push((
                terrarium_elevation(-f64::from(level)),
                format!("rgba(37, 48, 51, {opacity})"),
            ));
        }
        stops.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut expression = vec![
            json!("interpolate"),
            json!(["linear"]),
            json!(["elevation"]),
        ];
        for (elevation, color) in &stops {
            expression.push(json!(elevation));
            expression.push(json!(color));
        }
        expression.push(json!(terrarium_elevation(0.0)));
        expression.push(json!("rgba(201, 204, 202, 0)"));
        for (level, opacity) in super::terrain::hillshade_opacity_stops(false) {
            expression.push(json!(terrarium_elevation(f64::from(level))));
            expression.push(json!(format!("rgba(255, 255, 255, {opacity})")));
        }
        Value::Array(expression)
    };
    let color_relief_layer = |id: &str, source: &str| {
        json!({
            "id": id,
            "type": "color-relief",
            "source": source,
            "layout": { "visibility": "none" },
            "paint": { "color-relief-opacity": 1, "color-relief-color": color_relief_color }
        })
    };
    json!({
        "version": 8,
        "name": format!("preview - {tileset_id}"),
        "glyphs": "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
        "center": [info.header.center_longitude, info.header.center_latitude],
        "zoom": info.header.center_zoom,
        "sources": {
            "raw-terrain": {
                "type": "raster",
                "tiles": [terrain_tiles],
                "minzoom": info.header.min_zoom,
                "maxzoom": maxzoom,
                "tileSize": 512
            },
            "terrain-dem": {
                "type": "raster-dem",
                "tiles": [terrain_tiles],
                "minzoom": info.header.min_zoom,
                "maxzoom": maxzoom,
                "tileSize": 512,
                "encoding": "terrarium"
            },
            "vector-hillshade": {
                "type": "vector",
                "tiles": [derived_tiles("hillshade")],
                "minzoom": info.header.min_zoom,
                "maxzoom": maxzoom
            },
            "shade-webp-lossless": raster_dem_shade("hillshade-raster"),
            "shade-webp-lossy": raster_dem_shade("hillshade-webp-lossy"),
            "shade-jpeg": raster_dem_shade("hillshade-jpeg"),
            "isolines": {
                "type": "vector",
                "tiles": [derived_tiles("contours")],
                "minzoom": info.header.min_zoom,
                "maxzoom": maxzoom
            }
        },
        "layers": [
            {
                "id": "terrain-background",
                "type": "background",
                "paint": { "background-color": "#c9ccca" }
            },
            {
                "id": "raw-raster",
                "type": "raster",
                "source": "raw-terrain",
                "layout": { "visibility": "none" },
                "paint": { "raster-resampling": "linear" }
            },
            {
                "id": "raster-hillshade",
                "type": "hillshade",
                "source": "terrain-dem",
                "paint": {
                    "hillshade-illumination-direction": 315,
                    "hillshade-illumination-anchor": "map",
                    "hillshade-method": "standard",
                    "hillshade-exaggeration": 0.5,
                    "hillshade-shadow-color": "#253033",
                    "hillshade-highlight-color": "#ffffff",
                    "hillshade-accent-color": "#657074"
                }
            },
            {
                "id": "vector-hillshade-shadow",
                "type": "fill",
                "source": "vector-hillshade",
                "source-layer": "hillshade",
                "filter": ["==", ["get", "class"], "shadow"],
                "layout": { "visibility": "none" },
                "paint": {
                    "fill-color": "#253033",
                    "fill-outline-color": "rgba(0, 0, 0, 0)",
                    "fill-opacity": shadow_opacity,
                    "fill-antialias": true
                }
            },
            {
                "id": "vector-hillshade-highlight",
                "type": "fill",
                "source": "vector-hillshade",
                "source-layer": "hillshade",
                "filter": ["==", ["get", "class"], "highlight"],
                "layout": { "visibility": "none" },
                "paint": {
                    "fill-color": "#ffffff",
                    "fill-outline-color": "rgba(0, 0, 0, 0)",
                    "fill-opacity": highlight_opacity,
                    "fill-antialias": true
                }
            },
            color_relief_layer("shade-webp-lossless", "shade-webp-lossless"),
            color_relief_layer("shade-webp-lossy", "shade-webp-lossy"),
            color_relief_layer("shade-jpeg", "shade-jpeg"),
            {
                "id": "isolines",
                "type": "line",
                "source": "isolines",
                "source-layer": "contours",
                "layout": { "visibility": "none" },
                "paint": {
                    "line-color": "#9a5236",
                    "line-opacity": 0.85,
                    "line-width": ["match", ["get", "level"], 1, 1.35, 0.7]
                }
            },
            {
                "id": "isoline-labels",
                "type": "symbol",
                "source": "isolines",
                "source-layer": "contours",
                "filter": ["==", ["get", "level"], 1],
                "layout": {
                    "visibility": "none",
                    "symbol-placement": "line",
                    "symbol-spacing": 120,
                    "text-field": ["concat", ["to-string", ["get", "ele"]], " m"],
                    "text-font": ["Noto Sans Regular"],
                    // Smaller text at low zoom (rings are tiny) and a generous
                    // max-angle let index labels place on small, tightly curved
                    // contours; at 11px/45deg low-zoom rings got no labels.
                    "text-size": ["interpolate", ["linear"], ["zoom"], 8, 9, 13, 11],
                    "text-max-angle": 70,
                    "text-padding": 4,
                    "text-keep-upright": true
                },
                "paint": {
                    "text-color": "#6f3a24",
                    "text-halo-color": "rgba(238, 233, 228, 0.9)",
                    "text-halo-width": 1.25,
                    "text-halo-blur": 0.5
                }
            }
        ]
    })
}

fn preview_style_dem(
    tileset_id: &TilesetId,
    base_url: &str,
    info: &TilesetInfo,
    encoding: DemEncoding,
    maxzoom_override: Option<u8>,
) -> Value {
    json!({
        "version": 8,
        "name": format!("preview - {tileset_id}"),
        "center": [info.header.center_longitude, info.header.center_latitude],
        "zoom": info.header.center_zoom,
        "sources": {
            "dem": {
                "type": "raster-dem",
                "tiles": [format!("{base_url}/tilesets/{tileset_id}/{{z}}/{{x}}/{{y}}")],
                "minzoom": info.header.min_zoom,
                "maxzoom": maxzoom_override.unwrap_or(info.header.max_zoom),
                "tileSize": 256,
                "encoding": encoding.maplibre_encoding()
            }
        },
        "layers": [
            {
                "id": "background",
                "type": "background",
                "paint": { "background-color": "white" }
            },
            {
                "id": "hillshade",
                "type": "hillshade",
                "source": "dem",
                "paint": {
                    "hillshade-shadow-color": "#5a331f"
                }
            }
        ]
    })
}

fn preview_style_raster(
    tileset_id: &TilesetId,
    base_url: &str,
    info: &TilesetInfo,
    maxzoom_override: Option<u8>,
) -> Value {
    json!({
        "version": 8,
        "name": format!("preview - {tileset_id}"),
        "center": [info.header.center_longitude, info.header.center_latitude],
        "zoom": info.header.center_zoom,
        "sources": {
            "preview": {
                "type": "raster",
                "tiles": [format!("{base_url}/tilesets/{tileset_id}/{{z}}/{{x}}/{{y}}")],
                "minzoom": info.header.min_zoom,
                "maxzoom": maxzoom_override.unwrap_or(info.header.max_zoom),
                "tileSize": 256
            }
        },
        "layers": [
            {
                "id": "raster",
                "type": "raster",
                "source": "preview"
            }
        ]
    })
}

/// A MapLibre `case` expression selecting `hover` when the feature-state `hover`
/// flag is set, otherwise `base`. Used by the preview layers' hover styling.
fn hover_case(hover: impl Into<Value>, base: impl Into<Value>) -> Value {
    json!([
        "case",
        ["boolean", ["feature-state", "hover"], false],
        hover.into(),
        base.into()
    ])
}

fn preview_style_vector(
    tileset_id: &TilesetId,
    base_url: &str,
    info: &TilesetInfo,
    encoding: Option<&str>,
    maxzoom_override: Option<u8>,
) -> Value {
    let vector_layers = info.metadata.vector_layers();
    let mut layers = vec![json!({
        "id": "background",
        "type": "background",
        "paint": { "background-color": "#777" }
    })];

    layers.reserve(vector_layers.len() * 5);

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_fill_color(layer_id);
        let hover_color = layer_hover_fill_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-fill"),
            "type": "fill",
            "source": "preview",
            "source-layer": layer_id,
            "filter": ["==", ["geometry-type"], "Polygon"],
            "paint": {
                "fill-color": hover_case(hover_color.clone(), color),
                "fill-opacity": 0.62,
                "fill-outline-color": hover_case(hover_color, "rgba(0, 0, 0, 0)")
            }
        }));
    }

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_color(layer_id);
        let hover_color = layer_hover_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-line"),
            "type": "line",
            "source": "preview",
            "source-layer": layer_id,
            "filter": ["==", ["geometry-type"], "LineString"],
            "paint": {
                "line-color": hover_case(hover_color, color),
                "line-width": hover_case(2, 1)
            }
        }));
    }

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_circle_color(layer_id);
        let hover_color = layer_hover_circle_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-circle"),
            "type": "circle",
            "source": "preview",
            "source-layer": layer_id,
            "filter": ["==", ["geometry-type"], "Point"],
            "paint": {
                "circle-color": hover_case(hover_color, color),
                "circle-radius": hover_case(5.5, 3.0),
                "circle-opacity": 0.8,
                "circle-stroke-width": 0.0
            }
        }));
    }

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-label"),
            "type": "symbol",
            "source": "preview",
            "source-layer": layer_id,
            "filter": [
                "all",
                ["==", ["geometry-type"], "Point"],
                ["has", "name"]
            ],
            "layout": {
                "text-field": ["get", "name"],
                "text-size": 11,
                "text-offset": [0, 1.1],
                "text-anchor": "top"
            },
            "paint": {
                "text-color": color,
                "text-halo-color": "rgba(255,255,255,0.85)",
                "text-halo-width": 1.2
            }
        }));
    }

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-line-label"),
            "type": "symbol",
            "source": "preview",
            "source-layer": layer_id,
            "filter": [
                "all",
                ["==", ["geometry-type"], "LineString"],
                ["has", "name"]
            ],
            "layout": {
                "symbol-placement": "line",
                "text-field": ["get", "name"],
                "text-size": 11
            },
            "paint": {
                "text-color": color,
                "text-halo-color": "rgba(255,255,255,0.82)",
                "text-halo-width": 1.2
            }
        }));
    }

    // MLT when requested: ishikari transcodes MVT->MLT for `.mlt`, decoded via
    // `encoding: mlt` (needs @maplibre/mlt >= 1.1.9, bundled in gl-js >= 6).
    // Default MVT. The preview page exposes an MVT/MLT toggle that swaps this.
    let wants_mlt = encoding == Some("mlt");
    let tile_suffix = if wants_mlt { ".mlt" } else { "" };
    let mut source = json!({
        "type": "vector",
        "tiles": [format!("{base_url}/tilesets/{tileset_id}/{{z}}/{{x}}/{{y}}{tile_suffix}")],
        "minzoom": info.header.min_zoom,
        "maxzoom": maxzoom_override.unwrap_or(info.header.max_zoom)
    });
    if wants_mlt {
        source["encoding"] = Value::String("mlt".to_string());
    }

    json!({
        "version": 8,
        "name": format!("preview - {tileset_id}"),
        "glyphs": "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
        "center": [info.header.center_longitude, info.header.center_latitude],
        "zoom": info.header.center_zoom,
        "sources": { "preview": source },
        "layers": layers
    })
}

/// Assigns a stable hue to preview layers by name, with overrides for known categories.
fn layer_hue(layer_id: &str) -> f64 {
    if let Some(hue) = layer_hue_override(layer_id) {
        return hue;
    }

    let mut hasher = XxHash64::with_seed(0x2c4a68f3);
    hasher.write(layer_id.as_bytes());
    (hasher.finish() % 360) as f64
}

/// Uses a darker fill palette so polygons sit behind lines and points.
fn layer_fill_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.40, 0.24)
}

/// Uses a brighter variant of the fill palette for hovered polygons.
fn layer_hover_fill_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.50, 0.29)
}

/// Uses a brighter stroke palette for lines, points, and labels.
fn layer_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.56, 0.55)
}

/// Uses a brighter variant of the stroke palette for hovered lines.
fn layer_hover_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.74, 0.67)
}

/// Returns a higher-saturation, lower-lightness accent color for point features.
fn layer_circle_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.82, 0.48)
}

/// Returns a brighter point color for hovered point features.
fn layer_hover_circle_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.94, 0.64)
}

/// Overrides hue assignment for well-known layer names.
fn layer_hue_override(layer_id: &str) -> Option<f64> {
    match layer_id {
        "water" | "waterway" => Some(210.0),
        _ => None,
    }
}

/// Formats an HSL color string for the generated style.
fn hsl(hue: f64, saturation: f64, lightness: f64) -> String {
    format!(
        "hsl({hue:.0} {:.0}% {:.0}%)",
        saturation * 100.0,
        lightness * 100.0
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{BufMut, BytesMut};
    use serde_json::json;

    use super::{preview_html, preview_style};
    use ishikari_core::{
        interned::TilesetId,
        pmtiles::{Header, Metadata, TileType},
        storage::TilesetInfo,
    };

    fn header_with_tile_type(tile_type: u8) -> Header {
        let mut bytes = BytesMut::with_capacity(127);
        bytes.extend_from_slice(b"PMTiles");
        bytes.put_u8(3); // version
        for _ in 0..11 {
            bytes.put_u64_le(0);
        }
        bytes.put_u8(1); // clustered
        bytes.put_u8(1); // internal compression: none
        bytes.put_u8(2); // tile compression: gzip
        bytes.put_u8(tile_type);
        bytes.put_u8(0); // min zoom
        bytes.put_u8(12); // max zoom
        bytes.put_i32_le(-1800000000); // min lon
        bytes.put_i32_le(-850000000); // min lat
        bytes.put_i32_le(1800000000); // max lon
        bytes.put_i32_le(850000000); // max lat
        bytes.put_u8(0); // center zoom
        bytes.put_i32_le(0); // center lon
        bytes.put_i32_le(0); // center lat

        Header::parse(bytes.freeze()).expect("header parses")
    }

    fn info(tile_type: u8) -> TilesetInfo {
        TilesetInfo {
            header: header_with_tile_type(tile_type),
            metadata: Arc::new(Metadata::default()),
        }
    }

    #[test]
    fn preview_style_applies_maxzoom_override_to_dem_sources() {
        let tileset_id =
            TilesetId::try_from("mapterhorn/planet".to_string()).expect("valid tileset id");
        let style = preview_style(
            &tileset_id,
            "https://ishikari.example",
            &info(4),
            Some("terrarium"),
            Some(16),
            false,
        );

        assert_eq!(style["sources"]["dem"]["maxzoom"], 16);
    }

    #[test]
    fn preview_style_applies_maxzoom_override_to_vector_sources() {
        let tileset_id = TilesetId::try_from("demo/omt".to_string()).expect("valid tileset id");
        let style = preview_style(
            &tileset_id,
            "https://ishikari.example",
            &info(1),
            None,
            Some(16),
            false,
        );

        assert_eq!(style["sources"]["preview"]["maxzoom"], 16);
    }

    #[test]
    fn mapterhorn_preview_exposes_all_terrain_products() {
        let tileset_id =
            TilesetId::try_from("mapterhorn/planet".to_string()).expect("valid tileset id");
        let style = preview_style(
            &tileset_id,
            "https://ishikari.example",
            &info(4),
            None,
            Some(17),
            true,
        );

        assert_eq!(style["sources"]["raw-terrain"]["tileSize"], 512);
        assert_eq!(style["sources"]["terrain-dem"]["encoding"], "terrarium");
        assert_eq!(style["sources"]["vector-hillshade"]["maxzoom"], 17);
        assert_eq!(
            style["sources"]["isolines"]["tiles"][0],
            "https://ishikari.example/tilesets/mapterhorn/planet/derived/contours/{z}/{x}/{y}.mvt"
        );
        let layer_ids = style["layers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|layer| layer["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(layer_ids.contains(&"raw-raster"));
        assert!(layer_ids.contains(&"raster-hillshade"));
        assert!(layer_ids.contains(&"vector-hillshade-shadow"));
        assert!(layer_ids.contains(&"vector-hillshade-highlight"));
        assert!(layer_ids.contains(&"isolines"));
        assert!(layer_ids.contains(&"isoline-labels"));
        let layer = |id| {
            style["layers"]
                .as_array()
                .unwrap()
                .iter()
                .find(|layer| layer["id"] == id)
                .unwrap()
        };
        assert_eq!(layer("raw-raster")["layout"]["visibility"], "none");
        assert!(layer("raster-hillshade")["layout"].is_null());
        assert_eq!(
            layer("isoline-labels")["layout"]["text-field"],
            json!(["concat", ["to-string", ["get", "ele"]], " m"])
        );
        assert_eq!(
            layer("isoline-labels")["filter"],
            json!(["==", ["get", "level"], 1])
        );
        assert_eq!(layer("isoline-labels")["layout"]["symbol-spacing"], 120);
        let mut level_counts = Vec::new();
        for layer_id in ["vector-hillshade-shadow", "vector-hillshade-highlight"] {
            let layer = layer(layer_id);
            assert_eq!(layer["paint"]["fill-outline-color"], "rgba(0, 0, 0, 0)");
            assert_eq!(layer["paint"]["fill-antialias"], true);
            let opacity = layer["paint"]["fill-opacity"].as_array().unwrap();
            level_counts.push((opacity.len() - 3) / 2);
        }
        let expected_levels = super::super::terrain::hillshade_opacity_stops(true).len()
            + super::super::terrain::hillshade_opacity_stops(false).len();
        assert_eq!(level_counts.iter().sum::<usize>(), expected_levels);
        assert!(level_counts[0] > level_counts[1]);
        assert_eq!(
            layer("raster-hillshade")["paint"]["hillshade-illumination-anchor"],
            "map"
        );
        assert_eq!(
            layer("raster-hillshade")["paint"]["hillshade-exaggeration"],
            0.5
        );
    }

    #[test]
    fn terrain_controls_are_enabled_only_for_mapterhorn_preview() {
        let tileset_id = TilesetId::try_new("mapterhorn/planet").unwrap();
        let mapterhorn = preview_html(&tileset_id, None, TileType::Webp, true, None);
        let ordinary = preview_html(&tileset_id, None, TileType::Webp, false, None);

        assert!(mapterhorn.contains("const TERRAIN_PRODUCTS = true"));
        assert!(mapterhorn.contains("Vector hillshade"));
        assert!(mapterhorn.contains("params.set(\"hillshade\", terrainMode)"));
        let hillshade = mapterhorn.find("[\"hillshade\", \"Hillshade\"]").unwrap();
        let raw = mapterhorn.find("[\"raw\", \"Raw raster\"]").unwrap();
        let vector = mapterhorn
            .find("[\"vector\", \"Vector hillshade\"]")
            .unwrap();
        assert!(hillshade < vector && vector < raw);
        assert!(ordinary.contains("const TERRAIN_PRODUCTS = false"));
        for preview in [&mapterhorn, &ordinary] {
            assert!(preview.contains("Tile boundaries"));
            assert!(preview.contains("map.showTileBoundaries = tileBoundaryInput.checked"));
        }
        assert!(mapterhorn.contains("tileBoundaryInput.checked = true"));
    }
}
