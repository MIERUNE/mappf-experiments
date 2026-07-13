//! On-demand vector terrain products derived from Mapterhorn Terrarium tiles.
//!
//! Source tiles always enter through the normal composite resolver and
//! `ResourceResolver::route_tile`, so detail-archive selection, HRW ownership,
//! tile/chunk caches, object-store range batching, and negative caches are
//! shared with ordinary Mapterhorn serving.

mod contours;
pub(crate) mod dem;
mod hillshade;
mod topology;

use std::io::Write;

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use bytes::Bytes;
use flate2::{Compression as GzLevel, write::GzEncoder};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinSet;
use tracing::debug;

use crate::{
    interned::TilesetId,
    pmtiles::{MLT_CONTENT_TYPE, TileCoord, TileData, TileId, TileType},
    server::{AppState, HttpError, cache, get_origin},
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

#[derive(Debug, Deserialize)]
pub(crate) struct DerivedTileJsonQuery {
    encoding: Option<String>,
}

pub(crate) async fn derived_tilejson_handler(
    State(state): State<AppState>,
    Path((tileset_id, product)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<DerivedTileJsonQuery>,
) -> Result<([(header::HeaderName, &'static str); 1], Json<Value>), HttpError> {
    serve_tilejson(state, tileset_id, product, headers, query).await
}

pub(crate) async fn namespaced_derived_tilejson_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id, product)): Path<(String, String, String)>,
    headers: HeaderMap,
    Query(query): Query<DerivedTileJsonQuery>,
) -> Result<([(header::HeaderName, &'static str); 1], Json<Value>), HttpError> {
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
) -> Result<([(header::HeaderName, &'static str); 1], Json<Value>), HttpError> {
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
    Ok(([(header::CACHE_CONTROL, cache::TILEJSON)], Json(document)))
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
    let tileset_id = validated_mapterhorn(&state, tileset_id)?;
    let product = DerivedProduct::parse(&product)?;
    let (y, format) = negotiate_format(&y_raw, &headers);
    let y = y
        .parse::<u32>()
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("invalid tile y: {y}")))?;
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    let key = DerivedTileKey {
        tileset_id: tileset_id.clone(),
        product,
        tile_id,
    };
    let generated = state
        .derived_tile_cache()
        .try_get_with(
            key,
            generate_tile(state.clone(), tileset_id.clone(), product, z, x, y),
        )
        .await
        .map_err(|error| (*error).clone())?;

    // The raster product is a WebP image; MLT transcoding only applies to the
    // vector products, so it is always served as stored.
    let format = if product.is_raster() {
        RequestedTileFormat::AsStored
    } else {
        format
    };
    let response = match format {
        RequestedTileFormat::AsStored => {
            state.metrics.add_egress_bytes(generated.bytes.len() as u64);
            tile_data_response(generated)
        }
        RequestedTileFormat::Mlt => {
            let cache_id = mlt_cache_id(&tileset_id, product);
            let (bytes, content_encoding, _) =
                mlt_response_bytes(&state, &cache_id, tile_id, generated)?;
            state.metrics.add_egress_bytes(bytes.len() as u64);
            tile_data_response(TileData {
                bytes,
                content_type: MLT_CONTENT_TYPE,
                content_encoding,
            })
        }
    };
    debug!(
        endpoint = "derived_tile",
        tileset_id = %tileset_id,
        product = product.path(),
        z,
        x,
        y,
        "served generated terrain tile"
    );
    Ok(response)
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
) -> Result<TileData, HttpError> {
    let generation_permit = state
        .terrain_generation_semaphore()
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "terrain generation is shutting down".to_string(),
            )
        })?;
    let fetch_started = std::time::Instant::now();
    let sources = fetch_neighborhood(&state, tileset_id.clone(), z, x, y).await?;
    let fetch_elapsed = fetch_started.elapsed();
    let dem_cache = state.dem_tile_cache().clone();
    tokio::task::spawn_blocking(move || {
        // Keep the permit inside the blocking task. Dropping the HTTP future
        // cannot cancel spawn_blocking, so releasing it earlier would let
        // disconnected clients exceed the configured CPU concurrency.
        let _generation_permit = generation_permit;
        let cpu_started = std::time::Instant::now();
        let mut decoded_tiles = 0_u32;
        let mut tiles: [Option<std::sync::Arc<dem::DemTile>>; 9] = std::array::from_fn(|_| None);
        for (slot, source) in tiles.iter_mut().zip(sources) {
            *slot = match source {
                None => None,
                Some(NeighborSource::Cached(tile)) => Some(tile),
                Some(NeighborSource::Raw(cache_key, data)) => {
                    let tile =
                        std::sync::Arc::new(dem::decode_terrarium(data).map_err(|error| {
                            (
                                StatusCode::BAD_GATEWAY,
                                format!("decode Mapterhorn DEM: {error:#}"),
                            )
                        })?);
                    decoded_tiles += 1;
                    dem_cache.insert(cache_key, tile.clone());
                    Some(tile)
                }
            };
        }
        let neighborhood = dem::DemNeighborhood::from_tiles(tiles).map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("assemble Mapterhorn DEM: {error:#}"),
            )
        })?;
        let decode_elapsed = cpu_started.elapsed();
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
        // Splits the cold-tile cost so slow serving is attributable: neighbor
        // fetch (object store / peers) vs local CPU (WebP decode, generation).
        debug!(
            tileset_id = %tileset_id,
            product = product.path(),
            z,
            x,
            y,
            fetch_ms = fetch_elapsed.as_millis() as u64,
            decode_ms = decode_elapsed.as_millis() as u64,
            decoded_tiles,
            generate_ms = cpu_started.elapsed().saturating_sub(decode_elapsed).as_millis() as u64,
            payload_bytes = payload.len(),
            "generated terrain tile"
        );
        Ok(TileData {
            bytes,
            content_type,
            content_encoding,
        })
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("terrain generation task failed: {error}"),
        )
    })?
}

/// One neighborhood slot: either a DEM already decoded by an earlier request
/// (this or the sibling product, or an adjacent derived tile — neighbors share
/// six of nine sources) or raw fetched bytes to decode and cache.
enum NeighborSource {
    Cached(std::sync::Arc<dem::DemTile>),
    Raw((TilesetId, u64), TileData),
}

async fn fetch_neighborhood(
    state: &AppState,
    tileset_id: TilesetId,
    z: u8,
    x: u32,
    y: u32,
) -> Result<[Option<NeighborSource>; 9], HttpError> {
    let world = 1_i64 << z;
    let mut tasks = JoinSet::new();
    let mut tiles: [Option<NeighborSource>; 9] = std::array::from_fn(|_| None);
    for dy in -1_i64..=1 {
        for dx in -1_i64..=1 {
            let index = ((dy + 1) * 3 + dx + 1) as usize;
            let neighbor_y = i64::from(y) + dy;
            if !(0..world).contains(&neighbor_y) {
                continue;
            }
            let neighbor_x = (i64::from(x) + dx).rem_euclid(world) as u32;
            let neighbor_id = TileId::from(
                TileCoord::new(z, neighbor_x, neighbor_y as u32)
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
            )
            .value();
            let cache_key = (tileset_id.clone(), neighbor_id);
            if let Some(cached) = state.dem_tile_cache().get(&cache_key) {
                tiles[index] = Some(NeighborSource::Cached(cached));
                continue;
            }
            let state = state.clone();
            let tileset_id = tileset_id.clone();
            tasks.spawn(async move {
                let tile =
                    fetch_source_tile(&state, tileset_id, z, neighbor_x, neighbor_y as u32).await;
                (index, cache_key, tile)
            });
        }
    }

    while let Some(task) = tasks.join_next().await {
        let (index, cache_key, tile) = task.map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("DEM fetch task failed: {error}"),
            )
        })?;
        tiles[index] = tile?.map(|data| NeighborSource::Raw(cache_key, data));
    }
    if tiles[4].is_none() {
        return Err((StatusCode::NOT_FOUND, "DEM tile not found".into()));
    }
    Ok(tiles)
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
    for outcome in source.cache_outcomes() {
        state.metrics.record_tile_cache(outcome);
    }
    Ok(tile)
}

/// Synthetic MLT-cache namespace for generated tiles. `:` can never appear in
/// a validated tileset id, so these keys cannot collide with stored tilesets in
/// the shared MLT cache, and they stay readable in logs and debugging.
fn mlt_cache_id(tileset_id: &TilesetId, product: DerivedProduct) -> TilesetId {
    TilesetId::new_unchecked(&format!("derived:{}:{tileset_id}", product.path()))
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
    fn mlt_cache_ids_separate_products() {
        let source = TilesetId::new_unchecked("mapterhorn/planet");
        assert_ne!(
            mlt_cache_id(&source, DerivedProduct::Contours),
            mlt_cache_id(&source, DerivedProduct::Hillshade)
        );
    }
}
