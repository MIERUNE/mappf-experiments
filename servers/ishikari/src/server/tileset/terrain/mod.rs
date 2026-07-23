//! On-demand vector terrain products derived from Mapterhorn Terrarium tiles.
//!
//! Source tiles always enter through the normal composite resolver and
//! `ResourceResolver::route_tile`, so detail-archive selection, HRW ownership,
//! tile/chunk caches, object-store range batching, and negative caches are
//! shared with ordinary Mapterhorn serving.

mod generation;
mod product;
mod tilejson;

pub(crate) use generation::{DerivedOutcome, DerivedTileKey};
pub(crate) use mmpf_terrain::dem;
use mmpf_terrain::hillshade;
pub(crate) use tilejson::DerivedTileJsonQuery;

use axum::{
    Extension,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use tracing::{debug, warn};

use crate::provider::path_percent_encode;
use crate::server::{
    AppState, HttpError, auth::PropagatedAccessToken, bytes_response, cache, derived_json_response,
    get_origin,
};
use ishikari_core::pmtiles::{MLT_CONTENT_TYPE, TileData, TileType};

use self::{
    generation::generate_tile,
    product::{
        DerivedProduct, DerivedTileRequest, derived_resource_key, parse_derived_tile_request,
        validated_mapterhorn,
    },
    tilejson::derived_tilejson,
};
use super::{
    error::tileset_error_response,
    mlt::{RequestedTileFormat, TranscodeCachePolicy, mlt_response_bytes},
    tile::tile_data_response,
};

pub(super) fn hillshade_opacity_stops(shadow: bool) -> Vec<(u8, f64)> {
    hillshade::opacity_stops(shadow)
}

/// Bytes-per-tone-code of the neutral shade raster, so the preview's
/// `color-relief` custom encoding recovers the signed code.
pub(super) fn hillshade_shade_code_scale() -> f64 {
    hillshade::SHADE_CODE_SCALE
}

pub(crate) async fn derived_tilejson_handler(
    State(state): State<AppState>,
    Path((tileset_id, product)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<DerivedTileJsonQuery>,
    token: Option<Extension<PropagatedAccessToken>>,
) -> Result<Response, HttpError> {
    serve_tilejson(
        state,
        tileset_id,
        product,
        headers,
        query,
        token.map(|value| value.0),
    )
    .await
}

pub(crate) async fn namespaced_derived_tilejson_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id, product)): Path<(String, String, String)>,
    headers: HeaderMap,
    Query(query): Query<DerivedTileJsonQuery>,
    token: Option<Extension<PropagatedAccessToken>>,
) -> Result<Response, HttpError> {
    serve_tilejson(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        product,
        headers,
        query,
        token.map(|value| value.0),
    )
    .await
}

async fn serve_tilejson(
    state: AppState,
    tileset_id: String,
    product: String,
    headers: HeaderMap,
    query: DerivedTileJsonQuery,
    token: Option<PropagatedAccessToken>,
) -> Result<Response, HttpError> {
    let tileset_id = validated_mapterhorn(&state, tileset_id)?;
    let product = DerivedProduct::parse(&product)?;
    let info = state
        .resource_resolver
        .load_tileset_info(tileset_id.clone())
        .await
        .map_err(|error| tileset_error_response(&error))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tileset not found".to_string()))?;
    let maxzoom = state
        .mapterhorn()
        .expect("validated_mapterhorn checked configuration")
        .maxzoom();
    let document = derived_tilejson(
        &tileset_id,
        product,
        &get_origin(&headers),
        &info,
        maxzoom,
        query.wants_mlt(),
        token.as_ref(),
    );
    // Origin-derived like the base TileJSON: validate by a strong ETag over the
    // exact bytes served so conditional requests can 304.
    let body = serde_json::to_vec(&document).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("derived tilejson serialization failed: {error}"),
        )
    })?;
    Ok(derived_json_response(body, &headers, cache::TILEJSON))
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
    let routing_key = derived_resource_key(&request.tileset_id, request.product);
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
        .route_derived_resource(&routing_key, request.tile_id, &internal_path)
        .await
    {
        Ok(Some(wire)) => match decode_derived_wire(wire, request.product, request.format) {
            Ok(outcome) => Some(outcome),
            Err(error) => {
                // A malformed same-epoch peer response must not break serving.
                // Generate locally as the fail-safe.
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
        DerivedOutcome::Tile(tile) | DerivedOutcome::Degraded(tile) => tile,
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
    let key = DerivedTileKey::new(request.tileset_id.clone(), request.product, request.tile_id);
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
    let (generated, degraded) = match outcome {
        DerivedOutcome::Absent => return Ok(DerivedOutcome::Absent),
        DerivedOutcome::Tile(tile) => (tile, false),
        DerivedOutcome::Degraded(tile) => (tile, true),
    };
    match request.format {
        RequestedTileFormat::AsStored => Ok(if degraded {
            DerivedOutcome::Degraded(generated)
        } else {
            DerivedOutcome::Tile(generated)
        }),
        RequestedTileFormat::Mlt => {
            let cache_key = derived_resource_key(&request.tileset_id, request.product);
            let cache_policy = if degraded {
                TranscodeCachePolicy::Bypass
            } else {
                TranscodeCachePolicy::Retain
            };
            let (bytes, content_encoding, _) =
                mlt_response_bytes(state, &cache_key, request.tile_id, generated, cache_policy)
                    .await?;
            let tile = TileData {
                bytes,
                content_type: MLT_CONTENT_TYPE,
                content_encoding,
            };
            Ok(if degraded {
                DerivedOutcome::Degraded(tile)
            } else {
                DerivedOutcome::Tile(tile)
            })
        }
    }
}

const DERIVED_WIRE_MAGIC: &[u8; 8] = b"ISKRDRV2";
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
        DerivedOutcome::Tile(tile) | DerivedOutcome::Degraded(tile) => {
            let mut wire = Vec::with_capacity(DERIVED_WIRE_MAGIC.len() + 3 + tile.bytes.len());
            wire.extend_from_slice(DERIVED_WIRE_MAGIC);
            wire.push(DERIVED_WIRE_TILE);
            wire.push(derived_content_type_code(tile.content_type)?);
            wire.push(derived_content_encoding_code(tile.content_encoding)?);
            wire.extend_from_slice(&tile.bytes);
            wire
        }
        DerivedOutcome::Absent => {
            let mut wire = Vec::with_capacity(DERIVED_WIRE_MAGIC.len() + 1);
            wire.extend_from_slice(DERIVED_WIRE_MAGIC);
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
    if wire.len() < DERIVED_WIRE_MAGIC.len() + 1 {
        return Err("invalid derived wire magic");
    }
    let magic = &wire[..DERIVED_WIRE_MAGIC.len()];
    if magic != DERIVED_WIRE_MAGIC {
        return Err("invalid derived wire magic");
    }
    decode_derived_wire_v2(wire, product, format)
}

fn decode_derived_wire_v2(
    wire: Bytes,
    product: DerivedProduct,
    format: RequestedTileFormat,
) -> Result<DerivedOutcome, &'static str> {
    let status_offset = DERIVED_WIRE_MAGIC.len();
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header;

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
}
