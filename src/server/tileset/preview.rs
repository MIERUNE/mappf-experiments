//! Preview HTML and MapLibre style generation for a tileset.

use std::hash::Hasher;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::Html,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::debug;
use twox_hash::XxHash64;

use crate::{
    interned::TilesetId,
    pmtiles::TileType,
    server::{AppState, HttpError, cache, get_origin},
    storage::TilesetInfo,
};

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
) -> Result<([(header::HeaderName, &'static str); 1], Html<String>), HttpError> {
    serve_preview(state, tileset_id, query).await
}

/// Serves the HTML preview shell for a `{namespace}/{tileset_id}` key.
pub(crate) async fn namespaced_preview_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id)): Path<(String, String)>,
    Query(query): Query<PreviewQuery>,
) -> Result<([(header::HeaderName, &'static str); 1], Html<String>), HttpError> {
    serve_preview(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        query,
    )
    .await
}

/// Renders the preview HTML for an already-joined tileset key.
async fn serve_preview(
    state: AppState,
    tileset_id: String,
    query: PreviewQuery,
) -> Result<([(header::HeaderName, &'static str); 1], Html<String>), HttpError> {
    let tileset_id = TilesetId::try_from(tileset_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let info = state
        .resource_resolver
        .load_tileset_info(tileset_id.clone())
        .await
        .map_err(|e| tileset_error_response(&e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tileset not found".to_string()))?;
    let html = preview_html(
        &tileset_id,
        query.encoding.as_deref(),
        info.header.tile_type,
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
) -> Result<([(header::HeaderName, &'static str); 1], Json<Value>), HttpError> {
    serve_preview_style(state, tileset_id, headers, query).await
}

/// Serves the generated preview style for a `{namespace}/{tileset_id}` key.
pub(crate) async fn namespaced_preview_style_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<PreviewQuery>,
) -> Result<([(header::HeaderName, &'static str); 1], Json<Value>), HttpError> {
    serve_preview_style(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        headers,
        query,
    )
    .await
}

/// Builds the preview style for an already-joined tileset key.
async fn serve_preview_style(
    state: AppState,
    tileset_id: String,
    headers: HeaderMap,
    query: PreviewQuery,
) -> Result<([(header::HeaderName, &'static str); 1], Json<Value>), HttpError> {
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
    let style = preview_style(
        &tileset_id,
        &base_url,
        &info,
        query.encoding.as_deref(),
        maxzoom_override,
    );
    debug!(
        endpoint = "preview_style",
        tileset_id = %tileset_id,
        "served external response"
    );
    Ok(([(header::CACHE_CONTROL, cache::PREVIEW)], Json(style)))
}

/// Renders the shared MapLibre preview page for any style URL.
pub(crate) fn render_preview_html(
    title: &str,
    style_url: &str,
    terrain_control: &str,
    format_toggle: bool,
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
}

fn preview_html(tileset_id: &TilesetId, encoding: Option<&str>, tile_type: TileType) -> String {
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
        let style_url = format!("/tilesets/{tileset_id}/preview.json?encoding={fmt}");
        return render_preview_html(&title, &style_url, "", true);
    }
    // Raster / raster-dem: `encoding` selects the DEM hillshade scheme, no toggle.
    let style_url = match encoding {
        Some(enc) => format!("/tilesets/{tileset_id}/preview.json?encoding={enc}"),
        None => format!("/tilesets/{tileset_id}/preview.json"),
    };
    let terrain_control = if encoding.and_then(DemEncoding::from_str).is_some() {
        r#"map.addControl(new maplibregl.TerrainControl({ source: "dem", exaggeration: 1.0 }), "top-right");"#
    } else {
        ""
    };
    render_preview_html(&title, &style_url, terrain_control, false)
}

fn preview_style(
    tileset_id: &TilesetId,
    base_url: &str,
    info: &TilesetInfo,
    encoding: Option<&str>,
    maxzoom_override: Option<u8>,
) -> Value {
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
                "fill-color": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    hover_color,
                    color
                ],
                "fill-opacity": 0.62,
                "fill-outline-color": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    hover_color,
                    "rgba(0, 0, 0, 0)"
                ]
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
                "line-color": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    hover_color,
                    color
                ],
                "line-width": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    2,
                    1
                ]
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
                "circle-color": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    hover_color,
                    color
                ],
                "circle-radius": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    5.5,
                    3.0
                ],
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

    use super::preview_style;
    use crate::{
        interned::TilesetId,
        pmtiles::{Header, Metadata},
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
        );

        assert_eq!(style["sources"]["preview"]["maxzoom"], 16);
    }
}
