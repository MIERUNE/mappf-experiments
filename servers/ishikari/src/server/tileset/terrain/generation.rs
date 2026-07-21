use std::{io::Write, sync::Arc};

use axum::http::StatusCode;
use bytes::Bytes;
use flate2::{Compression as GzLevel, write::GzEncoder};
use ishikari_core::{
    interned::TilesetId,
    pmtiles::{TileCoord, TileData, TileId, TileType},
};
use mmpf_terrain::{contours, hillshade};
use tokio::task::JoinSet;
use tracing::debug;

use crate::server::{AppState, HttpError};

use super::{
    super::{error::tileset_error_response, tile::resolve_archive},
    dem,
    product::DerivedProduct,
};

/// Mapterhorn publishes 512px Terrarium tiles. Keep this source contract
/// separate from mmpf-terrain's more permissive generic decoder ceiling so a
/// malformed archive cannot multiply Ishikari's decoded working set.
const MAPTERHORN_DEM_TILE_DIMENSION: u32 = 512;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DerivedTileKey {
    tileset_id: TilesetId,
    product: DerivedProduct,
    tile_id: u64,
}

impl DerivedTileKey {
    pub(super) fn new(tileset_id: TilesetId, product: DerivedProduct, tile_id: u64) -> Self {
        Self {
            tileset_id,
            product,
            tile_id,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(
            TilesetId::try_new("terrain").expect("valid test tileset id"),
            DerivedProduct::Hillshade,
            0,
        )
    }
}

/// Cached result of a derived-tile generation. `Absent` records an
/// authoritative "no DEM here". `Degraded` records a usable tile generated
/// with an edge fallback after a transient neighbor failure. Both carry a
/// short TTL; complete tiles remain warm without an expiry.
#[derive(Clone)]
pub(crate) enum DerivedOutcome {
    Tile(TileData),
    Degraded(TileData),
    Absent,
}

pub(super) async fn generate_tile(
    state: AppState,
    tileset_id: TilesetId,
    product: DerivedProduct,
    z: u8,
    x: u32,
    y: u32,
) -> Result<DerivedOutcome, HttpError> {
    let fetch_started = std::time::Instant::now();
    let fetched = fetch_neighborhood(&state, tileset_id.clone(), z, x, y).await?;
    let fetch_elapsed = fetch_started.elapsed();

    // An absent center DEM is authoritative and stable — there is no terrain to
    // derive here. Return `Absent` so the caller caches it and serves a
    // cacheable 404, instead of re-running the 3x3 fetch on every request.
    if fetched.tiles[CENTER_INDEX].is_none() {
        return Ok(DerivedOutcome::Absent);
    }
    let present_sources = fetched.tiles.iter().filter(|tile| tile.is_some()).count() as u32;
    let degraded = fetched.degraded;
    let tiles = fetched.tiles;

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
        let (tile, payload_len) = tile_from_generated_payload(product, payload)?;
        let generate_elapsed = cpu_started.elapsed();
        metrics.record_terrain_generation(
            product.path(),
            fetch_elapsed,
            generate_elapsed,
            present_sources as usize,
            tile.bytes.len(),
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
            payload_bytes = payload_len,
            "generated terrain tile"
        );
        if degraded {
            Ok(DerivedOutcome::Degraded(tile))
        } else {
            Ok(DerivedOutcome::Tile(tile))
        }
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("terrain generation task failed: {error}"),
        )
    })?
}

/// Converts generated terrain bytes into their served representation.
fn tile_from_generated_payload(
    product: DerivedProduct,
    payload: Vec<u8>,
) -> Result<(TileData, usize), HttpError> {
    let payload_len = payload.len();
    // Vector products gzip well and declare it; raster products are already
    // compressed, so transfer their allocation directly into `Bytes`.
    let tile = if product.is_raster() {
        let content_type = match product {
            DerivedProduct::HillshadeJpeg => TileType::Jpeg.content_type(),
            _ => TileType::Webp.content_type(),
        };
        TileData {
            bytes: Bytes::from(payload),
            content_type,
            content_encoding: None,
        }
    } else {
        TileData {
            bytes: Bytes::from(gzip(&payload)?),
            content_type: TileType::Mvt.content_type(),
            content_encoding: Some("gzip"),
        }
    };
    Ok((tile, payload_len))
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
struct FetchedNeighborhood {
    tiles: [Option<Arc<dem::DemTile>>; 9],
    degraded: bool,
}

async fn fetch_neighborhood(
    state: &AppState,
    tileset_id: TilesetId,
    z: u8,
    x: u32,
    y: u32,
) -> Result<FetchedNeighborhood, HttpError> {
    let world = 1_i64 << z;
    let mut tasks = JoinSet::new();
    let mut tiles: [Option<Arc<dem::DemTile>>; 9] = std::array::from_fn(|_| None);
    let mut degraded = false;
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
                let result = Box::pin(load_decoded_dem(
                    &state,
                    tileset_id,
                    z,
                    neighbor_x,
                    neighbor_y as u32,
                ))
                .await;
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
                let (tile, transient_failure) = tolerate_neighbor_failure(index, error)?;
                tiles[index] = tile;
                degraded |= transient_failure;
            }
        }
    }
    Ok(FetchedNeighborhood { tiles, degraded })
}

fn tolerate_neighbor_failure<T>(
    index: usize,
    error: HttpError,
) -> Result<(Option<T>, bool), HttpError> {
    if index == CENTER_INDEX {
        Err(error)
    } else {
        Ok((None, true))
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
) -> Result<Option<Arc<dem::DemTile>>, HttpError> {
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    let cache = state.dem_tile_cache().clone();
    let state = state.clone();
    cache
        .try_get_with(
            (tileset_id.clone(), tile_id),
            Box::pin(async move {
                let Some(raw) = fetch_source_tile(&state, tileset_id, tile_id, z, x, y).await?
                else {
                    return Ok::<Option<Arc<dem::DemTile>>, HttpError>(None);
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
                    dem::decode_terrarium_with_dimension_limit(
                        raw.bytes.as_ref(),
                        MAPTERHORN_DEM_TILE_DIMENSION,
                    )
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
                Ok(Some(Arc::new(decoded)))
            }),
        )
        .await
        .map_err(|error: Arc<HttpError>| (*error).clone())
}

async fn fetch_source_tile(
    state: &AppState,
    tileset_id: TilesetId,
    tile_id: u64,
    z: u8,
    x: u32,
    y: u32,
) -> Result<Option<TileData>, HttpError> {
    let Some(archive) = resolve_archive(state, tileset_id, z, x, y).await? else {
        return Ok(None);
    };
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
    use std::io::Read;

    use flate2::read::GzDecoder;

    use super::*;

    #[test]
    fn raster_payload_moves_its_allocation_into_bytes() {
        let payload = vec![1, 2, 3, 4];
        let payload_ptr = payload.as_ptr();

        let (tile, payload_len) =
            tile_from_generated_payload(DerivedProduct::HillshadeRaster, payload).unwrap();

        assert_eq!(tile.bytes.as_ptr(), payload_ptr);
        assert_eq!(tile.bytes.as_ref(), [1, 2, 3, 4]);
        assert_eq!(payload_len, 4);
        assert_eq!(tile.content_type, TileType::Webp.content_type());
        assert_eq!(tile.content_encoding, None);
    }

    #[test]
    fn vector_payload_remains_gzip_encoded() {
        let payload = b"vector tile".to_vec();

        let (tile, payload_len) =
            tile_from_generated_payload(DerivedProduct::Contours, payload.clone()).unwrap();
        let mut decoded = Vec::new();
        GzDecoder::new(tile.bytes.as_ref())
            .read_to_end(&mut decoded)
            .unwrap();

        assert_eq!(decoded, payload);
        assert_eq!(payload_len, payload.len());
        assert_eq!(tile.content_type, TileType::Mvt.content_type());
        assert_eq!(tile.content_encoding, Some("gzip"));
    }

    #[test]
    fn only_center_source_errors_abort_generation() {
        let error = (StatusCode::BAD_GATEWAY, "source failed".to_string());

        assert_eq!(
            tolerate_neighbor_failure::<()>(0, error.clone()).unwrap(),
            (None, true)
        );
        assert_eq!(
            tolerate_neighbor_failure::<()>(CENTER_INDEX, error.clone()).unwrap_err(),
            error
        );
    }
}
