//! On-demand vector terrain products derived from Mapterhorn Terrarium tiles.
//!
//! Source tiles always enter through the normal composite resolver and
//! `ResourceResolver::route_tile`, so detail-archive selection, HRW ownership,
//! tile/chunk caches, object-store range batching, and negative caches are
//! shared with ordinary Mapterhorn serving.

pub(crate) use mmpf_terrain::dem;
use mmpf_terrain::{contours, hillshade};

use std::io::Write;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use flate2::{Compression as GzLevel, write::GzEncoder};
use serde::Deserialize;
use serde_json::json;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::{
    interned::TilesetId,
    pmtiles::{MLT_CONTENT_TYPE, TileCoord, TileData, TileId, TileType},
    server::{
        AppState, HttpError, apply_origin_vary, bytes_response, cache, conditional::Validators,
        get_origin, provider::path_percent_encode,
    },
};

use super::{
    error::tileset_error_response,
    mlt::{RequestedTileFormat, mlt_response_bytes, negotiate_format},
    tile::{resolve_archive, tile_data_response},
};

pub(super) fn hillshade_opacity_stops(shadow: bool) -> Vec<(u8, f64)> {
    hillshade::opacity_stops(shadow)
}

/// Bytes-per-tone-code of the neutral shade raster, so the preview's
/// `color-relief` custom encoding recovers the signed code.
pub(super) fn hillshade_shade_code_scale() -> f64 {
    hillshade::SHADE_CODE_SCALE
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum DerivedProduct {
    Contours,
    Hillshade,
    /// Experimental: the hillshade shade field as a quantized WebP raster
    /// instead of vector polygons, for the raster-vs-vector size/quality
    /// Pareto comparison. Fixed palette/sun.
    HillshadeRaster,
    /// Experimental: continuous shade as lossy WebP (neutral grayscale, colored
    /// by a style-side color-relief ramp).
    HillshadeWebpLossy,
    /// Experimental: continuous (un-quantized) shade as lossy JPEG — the size
    /// floor for fixed-palette delivery, with no tone banding.
    HillshadeJpeg,
}

impl DerivedProduct {
    fn parse(value: &str) -> Result<Self, HttpError> {
        match value {
            "contours" => Ok(Self::Contours),
            "hillshade" => Ok(Self::Hillshade),
            "hillshade-raster" => Ok(Self::HillshadeRaster),
            "hillshade-webp-lossy" => Ok(Self::HillshadeWebpLossy),
            "hillshade-jpeg" => Ok(Self::HillshadeJpeg),
            _ => Err((StatusCode::NOT_FOUND, "derived product not found".into())),
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Contours => "contours",
            Self::Hillshade => "hillshade",
            Self::HillshadeRaster => "hillshade-raster",
            Self::HillshadeWebpLossy => "hillshade-webp-lossy",
            Self::HillshadeJpeg => "hillshade-jpeg",
        }
    }

    fn is_raster(self) -> bool {
        matches!(
            self,
            Self::HillshadeRaster | Self::HillshadeWebpLossy | Self::HillshadeJpeg
        )
    }

    fn layer(self) -> &'static str {
        self.path()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DerivedTileKey {
    tileset_id: TilesetId,
    product: DerivedProduct,
    tile_id: u64,
}

#[cfg(test)]
impl DerivedTileKey {
    pub(crate) fn for_test() -> Self {
        Self {
            tileset_id: TilesetId::new_unchecked("terrain"),
            product: DerivedProduct::Hillshade,
            tile_id: 0,
        }
    }
}

/// Cached result of a derived-tile generation. `Absent` records an
/// authoritative "no DEM here" so a no-data region is served as a cacheable
/// 404 without re-running the fetch/generate pipeline; it carries a short
/// negative TTL in the cache. Transient errors are never cached (they surface
/// as `Err` and moka's `try_get_with` does not store them).
#[derive(Clone)]
pub(crate) enum DerivedOutcome {
    Tile(TileData),
    Absent,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DerivedTileJsonQuery {
    encoding: Option<String>,
}

struct DerivedTileRequest {
    tileset_id: TilesetId,
    product: DerivedProduct,
    z: u8,
    x: u32,
    y: u32,
    tile_id: u64,
    format: RequestedTileFormat,
}

pub(crate) async fn derived_tilejson_handler(
    State(state): State<AppState>,
    Path((tileset_id, product)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<DerivedTileJsonQuery>,
) -> Result<Response, HttpError> {
    serve_tilejson(state, tileset_id, product, headers, query).await
}

pub(crate) async fn namespaced_derived_tilejson_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id, product)): Path<(String, String, String)>,
    headers: HeaderMap,
    Query(query): Query<DerivedTileJsonQuery>,
) -> Result<Response, HttpError> {
    serve_tilejson(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        product,
        headers,
        query,
    )
    .await
}

async fn serve_tilejson(
    state: AppState,
    tileset_id: String,
    product: String,
    headers: HeaderMap,
    query: DerivedTileJsonQuery,
) -> Result<Response, HttpError> {
    let tileset_id = validated_mapterhorn(&state, tileset_id)?;
    let product = DerivedProduct::parse(&product)?;
    let info = state
        .resource_resolver
        .load_tileset_info(tileset_id.clone())
        .await
        .map_err(|error| tileset_error_response(&error))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tileset not found".to_string()))?;
    let wants_mlt = query
        .encoding
        .as_deref()
        .is_some_and(|encoding| encoding.eq_ignore_ascii_case("mlt"));
    let suffix = if wants_mlt { ".mlt" } else { ".mvt" };
    let base_url = get_origin(&headers);
    let maxzoom = state
        .mapterhorn()
        .expect("validated_mapterhorn checked configuration")
        .maxzoom();
    let fields = match product {
        DerivedProduct::Contours => json!({ "ele": "Number", "level": "Number" }),
        DerivedProduct::Hillshade => json!({ "class": "String", "level": "Number" }),
        // Raster product has no vector layer; the TileJSON is not used for it.
        DerivedProduct::HillshadeRaster
        | DerivedProduct::HillshadeWebpLossy
        | DerivedProduct::HillshadeJpeg => json!({}),
    };
    let document = json!({
        "tilejson": "3.0.0",
        "tiles": [format!(
            "{base_url}/tilesets/{tileset_id}/derived/{}/{{z}}/{{x}}/{{y}}{suffix}",
            product.path(),
        )],
        "vector_layers": [{
            "id": product.layer(),
            "fields": fields,
            "minzoom": info.header.min_zoom,
            "maxzoom": maxzoom
        }],
        "attribution": info.metadata.attribution.clone(),
        "bounds": [
            info.header.min_longitude,
            info.header.min_latitude,
            info.header.max_longitude,
            info.header.max_latitude
        ],
        "center": [
            info.header.center_longitude,
            info.header.center_latitude,
            info.header.center_zoom
        ],
        "minzoom": info.header.min_zoom,
        "maxzoom": maxzoom,
        "name": format!("{tileset_id} {}", product.path()),
        "format": "pbf",
        "encoding": if wants_mlt { "mlt" } else { "mvt" }
    });
    // Origin-derived like the base TileJSON: validate by a strong ETag over the
    // exact bytes served so conditional requests can 304.
    let body = serde_json::to_vec(&document).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("derived tilejson serialization failed: {error}"),
        )
    })?;
    let validators = Validators::for_derived_body(&body);
    if validators.not_modified(&headers) {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(cache::TILEJSON),
        );
        validators.apply(response.headers_mut());
        apply_origin_vary(response.headers_mut());
        return Ok(response);
    }
    let mut response = bytes_response(body, "application/json", Some(cache::TILEJSON));
    validators.apply(response.headers_mut());
    apply_origin_vary(response.headers_mut());
    Ok(response)
}

pub(crate) async fn derived_tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, product, z, x, y_raw)): Path<(String, String, u8, u32, String)>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    serve_derived_tile(state, tileset_id, product, z, x, y_raw, headers).await
}

pub(crate) async fn namespaced_derived_tile_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id, product, z, x, y_raw)): Path<(
        String,
        String,
        String,
        u8,
        u32,
        String,
    )>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    serve_derived_tile(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        product,
        z,
        x,
        y_raw,
        headers,
    )
    .await
}

async fn serve_derived_tile(
    state: AppState,
    tileset_id: String,
    product: String,
    z: u8,
    x: u32,
    y_raw: String,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    let request = parse_derived_tile_request(&state, tileset_id, product, z, x, &y_raw, &headers)?;
    let routing_id = derived_resource_id(&request.tileset_id, request.product);
    let y_path = match request.format {
        RequestedTileFormat::AsStored => request.y.to_string(),
        RequestedTileFormat::Mlt => format!("{}.mlt", request.y),
    };
    let internal_path = format!(
        "/_internal/derived/{}/{}/{}/{}/{y_path}",
        path_percent_encode(request.tileset_id.as_ref()),
        request.product.path(),
        request.z,
        request.x,
    );
    let routed = match state
        .resource_resolver
        .route_derived_resource(&routing_id, request.tile_id, &internal_path)
        .await
    {
        Ok(Some(wire)) => match decode_derived_wire(wire, request.product, request.format) {
            Ok(outcome) => Some(outcome),
            Err(error) => {
                // A future/older incompatible peer must not break serving
                // during a rolling update. Generate locally as the fail-safe.
                warn!(
                    tileset_id = %request.tileset_id,
                    product = request.product.path(),
                    z = request.z,
                    x = request.x,
                    y = request.y,
                    error,
                    "invalid derived peer response; falling back local"
                );
                None
            }
        },
        Ok(None) => None,
        Err(error) => {
            warn!(
                tileset_id = %request.tileset_id,
                product = request.product.path(),
                z = request.z,
                x = request.x,
                y = request.y,
                error = %error,
                "derived peer routing failed; falling back local"
            );
            None
        }
    };
    let outcome = match routed {
        Some(outcome) => outcome,
        None => local_derived_output(&state, &request).await?,
    };

    let generated = match outcome {
        DerivedOutcome::Tile(tile) => tile,
        DerivedOutcome::Absent => {
            return Ok(absent_derived_response(state.derived_negative_ttl()));
        }
    };
    state.metrics.add_egress_bytes(generated.bytes.len() as u64);
    let response = tile_data_response(generated);
    debug!(
        endpoint = "derived_tile",
        tileset_id = %request.tileset_id,
        product = request.product.path(),
        z = request.z,
        x = request.x,
        y = request.y,
        "served generated terrain tile"
    );
    Ok(response)
}

/// Serves the owner-only internal derived endpoint. It never performs peer
/// routing, which prevents forwarding loops and makes this node the failover
/// generation target selected by the caller's HRW candidate walk.
pub(crate) async fn internal_derived_tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, product, z, x, y_raw)): Path<(String, String, u8, u32, String)>,
) -> Result<Response, HttpError> {
    let request =
        parse_derived_tile_request(&state, tileset_id, product, z, x, &y_raw, &HeaderMap::new())?;
    let outcome = local_derived_output(&state, &request).await?;
    let wire = encode_derived_wire(&outcome).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cannot encode derived peer response: {error}"),
        )
    })?;
    state.metrics.add_internal_bytes(wire.len() as u64);
    Ok(bytes_response(wire, "application/octet-stream", None))
}

async fn local_derived_output(
    state: &AppState,
    request: &DerivedTileRequest,
) -> Result<DerivedOutcome, HttpError> {
    let key = DerivedTileKey {
        tileset_id: request.tileset_id.clone(),
        product: request.product,
        tile_id: request.tile_id,
    };
    let outcome = state
        .derived_tile_cache()
        .try_get_with(
            key,
            generate_tile(
                state.clone(),
                request.tileset_id.clone(),
                request.product,
                request.z,
                request.x,
                request.y,
            ),
        )
        .await
        .map_err(|error| (*error).clone())?;
    let generated = match outcome {
        DerivedOutcome::Absent => return Ok(DerivedOutcome::Absent),
        DerivedOutcome::Tile(tile) => tile,
    };
    match request.format {
        RequestedTileFormat::AsStored => Ok(DerivedOutcome::Tile(generated)),
        RequestedTileFormat::Mlt => {
            let cache_id = derived_resource_id(&request.tileset_id, request.product);
            let (bytes, content_encoding, _) =
                mlt_response_bytes(state, &cache_id, request.tile_id, generated).await?;
            Ok(DerivedOutcome::Tile(TileData {
                bytes,
                content_type: MLT_CONTENT_TYPE,
                content_encoding,
            }))
        }
    }
}

fn parse_derived_tile_request(
    state: &AppState,
    tileset_id: String,
    product: String,
    z: u8,
    x: u32,
    y_raw: &str,
    headers: &HeaderMap,
) -> Result<DerivedTileRequest, HttpError> {
    let tileset_id = validated_mapterhorn(state, tileset_id)?;
    let product = DerivedProduct::parse(&product)?;
    let (y, format) = negotiate_format(y_raw, headers);
    let y = y
        .parse::<u32>()
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("invalid tile y: {y}")))?;
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    Ok(DerivedTileRequest {
        tileset_id,
        product,
        z,
        x,
        y,
        tile_id,
        format: normalized_format(product, format),
    })
}

fn normalized_format(product: DerivedProduct, format: RequestedTileFormat) -> RequestedTileFormat {
    if product.is_raster() {
        RequestedTileFormat::AsStored
    } else {
        format
    }
}

/// Internal namespace shared by HRW placement and the MLT cache. `:` cannot
/// occur in validated public ids, so this cannot collide with stored tilesets.
fn derived_resource_id(tileset_id: &TilesetId, product: DerivedProduct) -> TilesetId {
    TilesetId::new_unchecked(&format!("derived:{}:{tileset_id}", product.path()))
}

const DERIVED_WIRE_MAGIC_V1: &[u8; 8] = b"ISKRDRV1";
const DERIVED_WIRE_MAGIC_V2: &[u8; 8] = b"ISKRDRV2";
const DERIVED_WIRE_ABSENT: u8 = 0;
const DERIVED_WIRE_TILE: u8 = 1;
const DERIVED_WIRE_CONTENT_MVT: u8 = 1;
const DERIVED_WIRE_CONTENT_MLT: u8 = 2;
const DERIVED_WIRE_CONTENT_PNG: u8 = 3;
const DERIVED_WIRE_CONTENT_JPEG: u8 = 4;
const DERIVED_WIRE_CONTENT_WEBP: u8 = 5;
const DERIVED_WIRE_CONTENT_AVIF: u8 = 6;
const DERIVED_WIRE_CONTENT_OCTET_STREAM: u8 = 7;
const DERIVED_WIRE_ENCODING_NONE: u8 = 0;
const DERIVED_WIRE_ENCODING_GZIP: u8 = 1;
const DERIVED_WIRE_ENCODING_BROTLI: u8 = 2;
const DERIVED_WIRE_ENCODING_ZSTD: u8 = 3;

fn encode_derived_wire(outcome: &DerivedOutcome) -> Result<Bytes, &'static str> {
    let wire = match outcome {
        DerivedOutcome::Tile(tile) => {
            let mut wire = Vec::with_capacity(DERIVED_WIRE_MAGIC_V2.len() + 3 + tile.bytes.len());
            wire.extend_from_slice(DERIVED_WIRE_MAGIC_V2);
            wire.push(DERIVED_WIRE_TILE);
            wire.push(derived_content_type_code(tile.content_type)?);
            wire.push(derived_content_encoding_code(tile.content_encoding)?);
            wire.extend_from_slice(&tile.bytes);
            wire
        }
        DerivedOutcome::Absent => {
            let mut wire = Vec::with_capacity(DERIVED_WIRE_MAGIC_V2.len() + 1);
            wire.extend_from_slice(DERIVED_WIRE_MAGIC_V2);
            wire.push(DERIVED_WIRE_ABSENT);
            wire
        }
    };
    Ok(Bytes::from(wire))
}

fn decode_derived_wire(
    wire: Bytes,
    product: DerivedProduct,
    format: RequestedTileFormat,
) -> Result<DerivedOutcome, &'static str> {
    if wire.len() < DERIVED_WIRE_MAGIC_V2.len() + 1 {
        return Err("invalid derived wire magic");
    }
    let magic = &wire[..DERIVED_WIRE_MAGIC_V2.len()];
    if magic == DERIVED_WIRE_MAGIC_V2 {
        return decode_derived_wire_v2(wire, product, format);
    }
    if magic == DERIVED_WIRE_MAGIC_V1 {
        return decode_derived_wire_v1(wire, product, format);
    }
    Err("invalid derived wire magic")
}

fn decode_derived_wire_v2(
    wire: Bytes,
    product: DerivedProduct,
    format: RequestedTileFormat,
) -> Result<DerivedOutcome, &'static str> {
    let status_offset = DERIVED_WIRE_MAGIC_V2.len();
    match wire[status_offset] {
        DERIVED_WIRE_ABSENT if wire.len() == status_offset + 1 => Ok(DerivedOutcome::Absent),
        DERIVED_WIRE_ABSENT => Err("absent derived wire response has a payload"),
        DERIVED_WIRE_TILE if wire.len() >= status_offset + 3 => {
            let tile = TileData {
                bytes: wire.slice(status_offset + 3..),
                content_type: derived_content_type(wire[status_offset + 1])?,
                content_encoding: derived_content_encoding(wire[status_offset + 2])?,
            };
            validate_derived_tile_data(product, format, tile).map(DerivedOutcome::Tile)
        }
        DERIVED_WIRE_TILE => Err("derived tile wire response is truncated"),
        _ => Err("invalid derived wire status"),
    }
}

/// Accepts v1 responses during rolling upgrades. New peers always emit v2,
/// which carries content metadata instead of reconstructing it implicitly.
fn decode_derived_wire_v1(
    wire: Bytes,
    product: DerivedProduct,
    format: RequestedTileFormat,
) -> Result<DerivedOutcome, &'static str> {
    if &wire[..DERIVED_WIRE_MAGIC_V1.len()] != DERIVED_WIRE_MAGIC_V1 {
        return Err("invalid derived wire magic");
    }
    let payload = wire.slice(DERIVED_WIRE_MAGIC_V1.len() + 1..);
    match wire[DERIVED_WIRE_MAGIC_V1.len()] {
        DERIVED_WIRE_ABSENT if payload.is_empty() => Ok(DerivedOutcome::Absent),
        DERIVED_WIRE_ABSENT => Err("absent derived wire response has a payload"),
        DERIVED_WIRE_TILE => Ok(DerivedOutcome::Tile(legacy_derived_tile_data(
            product, format, payload,
        ))),
        _ => Err("invalid derived wire status"),
    }
}

fn legacy_derived_tile_data(
    product: DerivedProduct,
    format: RequestedTileFormat,
    bytes: Bytes,
) -> TileData {
    match format {
        RequestedTileFormat::Mlt => TileData {
            bytes,
            content_type: MLT_CONTENT_TYPE,
            content_encoding: Some("gzip"),
        },
        RequestedTileFormat::AsStored if product.is_raster() => TileData {
            bytes,
            content_type: match product {
                DerivedProduct::HillshadeJpeg => TileType::Jpeg.content_type(),
                _ => TileType::Webp.content_type(),
            },
            content_encoding: None,
        },
        RequestedTileFormat::AsStored => TileData {
            bytes,
            content_type: TileType::Mvt.content_type(),
            content_encoding: Some("gzip"),
        },
    }
}

fn validate_derived_tile_data(
    product: DerivedProduct,
    format: RequestedTileFormat,
    tile: TileData,
) -> Result<TileData, &'static str> {
    let expected_content_type = match format {
        RequestedTileFormat::Mlt => MLT_CONTENT_TYPE,
        RequestedTileFormat::AsStored if product == DerivedProduct::HillshadeJpeg => {
            TileType::Jpeg.content_type()
        }
        RequestedTileFormat::AsStored if product.is_raster() => TileType::Webp.content_type(),
        RequestedTileFormat::AsStored => TileType::Mvt.content_type(),
    };
    if tile.content_type != expected_content_type {
        return Err("derived wire content type does not match request");
    }
    // Encoding is transport metadata carried authoritatively by wire v2, not a
    // property of the requested representation. Native MLT may legitimately be
    // uncompressed, gzip, Brotli, or Zstandard; the wire decoder already rejects
    // every encoding outside that allowlist.
    Ok(tile)
}

fn derived_content_type_code(content_type: &str) -> Result<u8, &'static str> {
    match content_type {
        value if value == TileType::Mvt.content_type() => Ok(DERIVED_WIRE_CONTENT_MVT),
        MLT_CONTENT_TYPE => Ok(DERIVED_WIRE_CONTENT_MLT),
        value if value == TileType::Png.content_type() => Ok(DERIVED_WIRE_CONTENT_PNG),
        value if value == TileType::Jpeg.content_type() => Ok(DERIVED_WIRE_CONTENT_JPEG),
        value if value == TileType::Webp.content_type() => Ok(DERIVED_WIRE_CONTENT_WEBP),
        value if value == TileType::Avif.content_type() => Ok(DERIVED_WIRE_CONTENT_AVIF),
        value if value == TileType::Unknown.content_type() => Ok(DERIVED_WIRE_CONTENT_OCTET_STREAM),
        _ => Err("unsupported derived wire content type"),
    }
}

fn derived_content_type(code: u8) -> Result<&'static str, &'static str> {
    match code {
        DERIVED_WIRE_CONTENT_MVT => Ok(TileType::Mvt.content_type()),
        DERIVED_WIRE_CONTENT_MLT => Ok(MLT_CONTENT_TYPE),
        DERIVED_WIRE_CONTENT_PNG => Ok(TileType::Png.content_type()),
        DERIVED_WIRE_CONTENT_JPEG => Ok(TileType::Jpeg.content_type()),
        DERIVED_WIRE_CONTENT_WEBP => Ok(TileType::Webp.content_type()),
        DERIVED_WIRE_CONTENT_AVIF => Ok(TileType::Avif.content_type()),
        DERIVED_WIRE_CONTENT_OCTET_STREAM => Ok(TileType::Unknown.content_type()),
        _ => Err("unsupported derived wire content type"),
    }
}

fn derived_content_encoding_code(encoding: Option<&str>) -> Result<u8, &'static str> {
    match encoding {
        None => Ok(DERIVED_WIRE_ENCODING_NONE),
        Some("gzip") => Ok(DERIVED_WIRE_ENCODING_GZIP),
        Some("br") => Ok(DERIVED_WIRE_ENCODING_BROTLI),
        Some("zstd") => Ok(DERIVED_WIRE_ENCODING_ZSTD),
        Some(_) => Err("unsupported derived wire content encoding"),
    }
}

fn derived_content_encoding(code: u8) -> Result<Option<&'static str>, &'static str> {
    match code {
        DERIVED_WIRE_ENCODING_NONE => Ok(None),
        DERIVED_WIRE_ENCODING_GZIP => Ok(Some("gzip")),
        DERIVED_WIRE_ENCODING_BROTLI => Ok(Some("br")),
        DERIVED_WIRE_ENCODING_ZSTD => Ok(Some("zstd")),
        _ => Err("unsupported derived wire content encoding"),
    }
}

fn validated_mapterhorn(state: &AppState, value: String) -> Result<TilesetId, HttpError> {
    let tileset_id =
        TilesetId::try_from(value).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    match state.mapterhorn() {
        Some(resolver) if resolver.matches(&tileset_id) => Ok(tileset_id),
        _ => Err((
            StatusCode::NOT_FOUND,
            "derived terrain products require the configured Mapterhorn tileset".into(),
        )),
    }
}

async fn generate_tile(
    state: AppState,
    tileset_id: TilesetId,
    product: DerivedProduct,
    z: u8,
    x: u32,
    y: u32,
) -> Result<DerivedOutcome, HttpError> {
    let fetch_started = std::time::Instant::now();
    let tiles = fetch_neighborhood(&state, tileset_id.clone(), z, x, y).await?;
    let fetch_elapsed = fetch_started.elapsed();

    // An absent center DEM is authoritative and stable — there is no terrain to
    // derive here. Return `Absent` so the caller caches it and serves a
    // cacheable 404, instead of re-running the 3x3 fetch on every request.
    if tiles[CENTER_INDEX].is_none() {
        return Ok(DerivedOutcome::Absent);
    }
    let present_sources = tiles.iter().filter(|tile| tile.is_some()).count() as u32;

    // Admit CPU work only around the actual generation — never across the
    // neighborhood fetch above — so slow object-store/peer I/O cannot hold a
    // CPU-concurrency slot while doing no CPU work. Admission sheds with 503
    // under extreme overload rather than growing the queue without bound.
    let generation_permit = state.admit_cpu_work("terrain_generate").await?;
    let metrics = state.metrics.clone();
    tokio::task::spawn_blocking(move || {
        // Keep the permit inside the blocking task. Dropping the HTTP future
        // cannot cancel spawn_blocking, so releasing it earlier would let
        // disconnected clients exceed the configured CPU concurrency.
        let _generation_permit = generation_permit;
        let cpu_started = std::time::Instant::now();
        let neighborhood = dem::DemNeighborhood::from_tiles(tiles).map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("assemble Mapterhorn DEM: {error:#}"),
            )
        })?;
        let payload = match product {
            DerivedProduct::Contours => contours::generate(&neighborhood, z),
            DerivedProduct::Hillshade => hillshade::generate(&neighborhood, z, y),
            DerivedProduct::HillshadeRaster => hillshade::generate_raster(&neighborhood, z, y),
            DerivedProduct::HillshadeWebpLossy => {
                hillshade::generate_raster_webp_lossy(&neighborhood, z, y, 80)
            }
            DerivedProduct::HillshadeJpeg => {
                hillshade::generate_raster_jpeg(&neighborhood, z, y, 85)
            }
        }
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("generate {}: {error:#}", product.path()),
            )
        })?;
        // Vector products gzip well and declare it; the raster WebP is already
        // compressed, so it is served as-is with its image content type.
        let (bytes, content_type, content_encoding) = if product.is_raster() {
            let content_type = match product {
                DerivedProduct::HillshadeJpeg => TileType::Jpeg.content_type(),
                _ => TileType::Webp.content_type(),
            };
            (Bytes::from(payload.clone()), content_type, None)
        } else {
            (
                Bytes::from(gzip(&payload)?),
                TileType::Mvt.content_type(),
                Some("gzip"),
            )
        };
        let generate_elapsed = cpu_started.elapsed();
        metrics.record_terrain_generation(
            product.path(),
            fetch_elapsed,
            generate_elapsed,
            present_sources as usize,
            bytes.len(),
        );
        // Splits the cold-tile cost so slow serving is attributable: source
        // acquisition (fetch + WebP decode, single-flighted per source) vs
        // local product generation CPU.
        debug!(
            tileset_id = %tileset_id,
            product = product.path(),
            z,
            x,
            y,
            source_ms = fetch_elapsed.as_millis() as u64,
            present_sources,
            generate_ms = generate_elapsed.as_millis() as u64,
            payload_bytes = payload.len(),
            "generated terrain tile"
        );
        Ok(DerivedOutcome::Tile(TileData {
            bytes,
            content_type,
            content_encoding,
        }))
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("terrain generation task failed: {error}"),
        )
    })?
}

/// Row-major index of the center tile within the 3x3 neighborhood.
const CENTER_INDEX: usize = 4;

/// Fetches and decodes the 3x3 DEM neighborhood around a tile, returning each
/// decoded source (or `None` where a source is absent). Every source is loaded
/// through [`load_decoded_dem`], which single-flights the fetch + WebP decode
/// per source tile across concurrent derived requests (sibling products and
/// adjacent derived tiles share six of nine sources).
///
/// Only the center is required. A missing *non-center* source — absent, or a
/// transient fetch error — degrades to an edge fallback rather than failing the
/// whole tile. A transient error on the *center* propagates as `Err`, so it is
/// never cached as a permanent absence.
async fn fetch_neighborhood(
    state: &AppState,
    tileset_id: TilesetId,
    z: u8,
    x: u32,
    y: u32,
) -> Result<[Option<std::sync::Arc<dem::DemTile>>; 9], HttpError> {
    let world = 1_i64 << z;
    let mut tasks = JoinSet::new();
    let mut tiles: [Option<std::sync::Arc<dem::DemTile>>; 9] = std::array::from_fn(|_| None);
    for dy in -1_i64..=1 {
        for dx in -1_i64..=1 {
            let index = ((dy + 1) * 3 + dx + 1) as usize;
            let neighbor_y = i64::from(y) + dy;
            if !(0..world).contains(&neighbor_y) {
                continue;
            }
            let neighbor_x = (i64::from(x) + dx).rem_euclid(world) as u32;
            let state = state.clone();
            let tileset_id = tileset_id.clone();
            tasks.spawn(async move {
                let result =
                    load_decoded_dem(&state, tileset_id, z, neighbor_x, neighbor_y as u32).await;
                (index, result)
            });
        }
    }

    while let Some(task) = tasks.join_next().await {
        let (index, result) = task.map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("DEM fetch task failed: {error}"),
            )
        })?;
        match result {
            Ok(tile) => tiles[index] = tile,
            Err(error) => {
                if index != CENTER_INDEX {
                    debug!(
                        z,
                        x,
                        y,
                        index,
                        error = %error.1,
                        "neighbor DEM source failed; using edge fallback"
                    );
                }
                tiles[index] = tolerate_neighbor_failure(index, error)?;
            }
        }
    }
    Ok(tiles)
}

fn tolerate_neighbor_failure<T>(index: usize, error: HttpError) -> Result<Option<T>, HttpError> {
    if index == CENTER_INDEX {
        Err(error)
    } else {
        Ok(None)
    }
}

/// Loads and decodes a single source DEM tile, single-flighting the fetch +
/// WebP decode per source tile id so concurrent derived requests sharing a
/// source only do it once. Absent sources are cached as `None` (bounded by the
/// DEM cache's negative TTL); transient errors are not cached.
async fn load_decoded_dem(
    state: &AppState,
    tileset_id: TilesetId,
    z: u8,
    x: u32,
    y: u32,
) -> Result<Option<std::sync::Arc<dem::DemTile>>, HttpError> {
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    let cache = state.dem_tile_cache().clone();
    let state = state.clone();
    cache
        .try_get_with((tileset_id.clone(), tile_id), async move {
            let Some(raw) = fetch_source_tile(&state, tileset_id, z, x, y).await? else {
                return Ok::<Option<std::sync::Arc<dem::DemTile>>, HttpError>(None);
            };
            if raw.content_encoding.is_some() {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "compressed Mapterhorn image payload is not supported: {:?}",
                        raw.content_encoding
                    ),
                ));
            }
            // Fetch first, then admit CPU work only for WebP decoding. This uses
            // the same bounded CPU pool (and shed) as product generation without
            // ever holding a slot across object-store or peer I/O.
            let decode_permit = state.admit_cpu_work("dem_decode").await?;
            let decoded = tokio::task::spawn_blocking(move || {
                let _decode_permit = decode_permit;
                dem::decode_terrarium(raw.bytes.as_ref())
            })
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("DEM decode task failed: {error}"),
                )
            })?
            .map_err(|error| {
                (
                    StatusCode::BAD_GATEWAY,
                    format!("decode Mapterhorn DEM: {error:#}"),
                )
            })?;
            Ok(Some(std::sync::Arc::new(decoded)))
        })
        .await
        .map_err(|error: std::sync::Arc<HttpError>| (*error).clone())
}

async fn fetch_source_tile(
    state: &AppState,
    tileset_id: TilesetId,
    z: u8,
    x: u32,
    y: u32,
) -> Result<Option<TileData>, HttpError> {
    let Some(archive) = resolve_archive(state, tileset_id, z, x, y).await? else {
        return Ok(None);
    };
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    let (tile, source) = state
        .resource_resolver
        .route_tile(archive, tile_id)
        .await
        .map_err(|error| tileset_error_response(&error))?;
    for outcome in state.resource_resolver.cache_outcomes(source) {
        state.metrics.record_tile_cache(outcome);
    }
    Ok(tile)
}

/// Builds a cacheable `404` for a derived tile whose center DEM is absent. The
/// short `max-age` (the derived negative TTL) lets the CDN and clients absorb
/// repeat requests for no-data regions, while still surfacing a later-provisioned
/// detail archive once the entry expires.
fn absent_derived_response(negative_ttl: std::time::Duration) -> Response {
    let mut response = (StatusCode::NOT_FOUND, "derived tile not available\n").into_response();
    if let Ok(value) =
        header::HeaderValue::from_str(&format!("public, max-age={}", negative_ttl.as_secs()))
    {
        response.headers_mut().insert(header::CACHE_CONTROL, value);
    }
    response
}

fn gzip(data: &[u8]) -> Result<Vec<u8>, HttpError> {
    let mut encoder = GzEncoder::new(Vec::new(), GzLevel::default());
    encoder.write_all(data).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("gzip generated tile: {error}"),
        )
    })?;
    encoder.finish().map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("gzip generated tile: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header;

    #[test]
    fn product_names_are_explicit() {
        assert_eq!(
            DerivedProduct::parse("contours").unwrap().path(),
            "contours"
        );
        assert_eq!(
            DerivedProduct::parse("hillshade").unwrap().path(),
            "hillshade"
        );
        assert!(DerivedProduct::parse("terrain").is_err());
    }

    #[test]
    fn derived_resource_ids_separate_products() {
        let source = TilesetId::new_unchecked("mapterhorn/planet");
        assert_ne!(
            derived_resource_id(&source, DerivedProduct::Contours),
            derived_resource_id(&source, DerivedProduct::Hillshade)
        );
    }

    #[test]
    fn derived_wire_round_trips_tile_metadata_and_absence() {
        let source = DerivedOutcome::Tile(TileData {
            bytes: Bytes::from_static(b"compressed tile"),
            content_type: TileType::Mvt.content_type(),
            content_encoding: Some("gzip"),
        });
        let decoded = decode_derived_wire(
            encode_derived_wire(&source).unwrap(),
            DerivedProduct::Hillshade,
            RequestedTileFormat::AsStored,
        )
        .unwrap();
        let DerivedOutcome::Tile(decoded) = decoded else {
            panic!("expected tile")
        };
        assert_eq!(decoded.bytes, Bytes::from_static(b"compressed tile"));
        assert_eq!(decoded.content_type, TileType::Mvt.content_type());
        assert_eq!(decoded.content_encoding, Some("gzip"));

        assert!(matches!(
            decode_derived_wire(
                encode_derived_wire(&DerivedOutcome::Absent).unwrap(),
                DerivedProduct::Hillshade,
                RequestedTileFormat::AsStored,
            )
            .unwrap(),
            DerivedOutcome::Absent
        ));
    }

    #[test]
    fn legacy_derived_wire_reconstructs_requested_representation() {
        let payload = Bytes::from_static(b"payload");
        let mut legacy_wire = DERIVED_WIRE_MAGIC_V1.to_vec();
        legacy_wire.push(DERIVED_WIRE_TILE);
        legacy_wire.extend_from_slice(&payload);
        let decoded = decode_derived_wire(
            Bytes::from(legacy_wire),
            DerivedProduct::Hillshade,
            RequestedTileFormat::Mlt,
        )
        .unwrap();
        let DerivedOutcome::Tile(decoded) = decoded else {
            panic!("expected legacy tile")
        };
        assert_eq!(decoded.bytes, payload);
        assert_eq!(decoded.content_type, MLT_CONTENT_TYPE);
        assert_eq!(decoded.content_encoding, Some("gzip"));

        let mlt = legacy_derived_tile_data(
            DerivedProduct::Hillshade,
            RequestedTileFormat::Mlt,
            payload.clone(),
        );
        assert_eq!(mlt.content_type, MLT_CONTENT_TYPE);
        assert_eq!(mlt.content_encoding, Some("gzip"));

        let raster = legacy_derived_tile_data(
            DerivedProduct::HillshadeRaster,
            RequestedTileFormat::AsStored,
            payload.clone(),
        );
        assert_eq!(raster.content_type, TileType::Webp.content_type());
        assert_eq!(raster.content_encoding, None);

        let jpeg = legacy_derived_tile_data(
            DerivedProduct::HillshadeJpeg,
            RequestedTileFormat::AsStored,
            payload,
        );
        assert_eq!(jpeg.content_type, TileType::Jpeg.content_type());
        assert_eq!(jpeg.content_encoding, None);
    }

    #[test]
    fn derived_wire_rejects_incompatible_or_malformed_responses() {
        assert!(
            decode_derived_wire(
                Bytes::from_static(b"old peer response"),
                DerivedProduct::Hillshade,
                RequestedTileFormat::AsStored,
            )
            .is_err()
        );

        let mut malformed_absent = encode_derived_wire(&DerivedOutcome::Absent)
            .unwrap()
            .to_vec();
        malformed_absent.push(1);
        assert!(
            decode_derived_wire(
                Bytes::from(malformed_absent),
                DerivedProduct::Hillshade,
                RequestedTileFormat::AsStored,
            )
            .is_err()
        );

        let incompatible = DerivedOutcome::Tile(TileData {
            bytes: Bytes::from_static(b"webp"),
            content_type: TileType::Webp.content_type(),
            content_encoding: None,
        });
        assert!(matches!(
            decode_derived_wire(
                encode_derived_wire(&incompatible).unwrap(),
                DerivedProduct::Hillshade,
                RequestedTileFormat::AsStored,
            ),
            Err("derived wire content type does not match request")
        ));
    }

    #[test]
    fn derived_wire_preserves_native_mlt_encoding() {
        for content_encoding in [None, Some("br"), Some("zstd")] {
            let source = DerivedOutcome::Tile(TileData {
                bytes: Bytes::from_static(b"native mlt"),
                content_type: MLT_CONTENT_TYPE,
                content_encoding,
            });
            let decoded = decode_derived_wire(
                encode_derived_wire(&source).unwrap(),
                DerivedProduct::Hillshade,
                RequestedTileFormat::Mlt,
            )
            .unwrap();
            let DerivedOutcome::Tile(decoded) = decoded else {
                panic!("expected tile")
            };
            assert_eq!(decoded.content_type, MLT_CONTENT_TYPE);
            assert_eq!(decoded.content_encoding, content_encoding);
        }
    }

    #[test]
    fn absent_response_is_short_lived_and_cacheable() {
        let response = absent_derived_response(std::time::Duration::from_secs(60));

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "public, max-age=60"
        );
    }

    #[test]
    fn only_center_source_errors_abort_generation() {
        let error = (StatusCode::BAD_GATEWAY, "source failed".to_string());

        assert_eq!(
            tolerate_neighbor_failure::<()>(0, error.clone()).unwrap(),
            None
        );
        assert_eq!(
            tolerate_neighbor_failure::<()>(CENTER_INDEX, error.clone()).unwrap_err(),
            error
        );
    }
}
