//! Axum handlers for tile-serving endpoints.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{self},
    },
    response::{IntoResponse, Response},
};
use tracing::debug;

use crate::{
    interned::TilesetId,
    pmtiles::{MLT_CONTENT_TYPE, TileCoord, TileData, TileId},
    server::{AppState, HttpError, cache},
};

use super::error::tileset_error_response;
use super::mapterhorn::Resolved;
use super::mlt::{RequestedTileFormat, mlt_response_bytes, negotiate_format};

/// Parses the numeric tile `y` (after extension stripping).
fn parse_y(y: &str) -> Result<u32, HttpError> {
    y.parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("invalid tile y: {y}")))
}

/// Serves the external z/x/y tile endpoint for a flat tileset key.
pub(crate) async fn tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, z, x, y_raw)): Path<(String, u8, u32, String)>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    let (y, format) = negotiate_format(&y_raw, &headers);
    serve_tile(state, tileset_id, z, x, parse_y(&y)?, format).await
}

/// Serves the external z/x/y tile endpoint for a `{namespace}/{tileset_id}` key.
pub(crate) async fn namespaced_tile_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id, z, x, y_raw)): Path<(String, String, u8, u32, String)>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    let (y, format) = negotiate_format(&y_raw, &headers);
    serve_tile(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        z,
        x,
        parse_y(&y)?,
        format,
    )
    .await
}

/// Resolves and serves a tile for an already-joined tileset key, either as
/// stored in PMTiles or as MLT per the negotiated `format`.
async fn serve_tile(
    state: AppState,
    tileset_id: String,
    z: u8,
    x: u32,
    y: u32,
    format: RequestedTileFormat,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = TilesetId::try_from(tileset_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    // Mapterhorn composite resolution rewrites z>12 onto the detail archive
    // (or 404s when that region has no detail data); other tilesets pass through.
    let Some(tileset_id) = resolve_archive(&state, tileset_id, z, x, y).await? else {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    };
    let (tile, source) = state
        .resource_resolver
        .route_tile(tileset_id.clone(), tile_id)
        .await
        .map_err(|e| tileset_error_response(&e))?;
    state.metrics.record_tile_served(source.served_label());
    for outcome in source.cache_outcomes() {
        state.metrics.record_tile_cache(outcome);
    }
    let Some(tile) = tile else {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    };
    let response = match format {
        RequestedTileFormat::AsStored => {
            state.metrics.add_egress_bytes(tile.bytes.len() as u64);
            debug!(
                endpoint = "tile",
                format = "as_stored",
                content_type = tile.content_type,
                source = source.served_label(),
                served_bytes = tile.bytes.len(),
                "served external response"
            );
            TilesetResponse::from(tile)
                .with_cache_control(cache::TILE)
                .into_response()
        }
        RequestedTileFormat::Mlt => {
            let (bytes, content_encoding, served_format) =
                mlt_response_bytes(&state, &tileset_id, tile_id, tile)?;
            state.metrics.add_egress_bytes(bytes.len() as u64);
            debug!(
                endpoint = "tile",
                format = served_format,
                source = source.served_label(),
                served_bytes = bytes.len(),
                "served external response"
            );
            TilesetResponse {
                bytes,
                content_type: MLT_CONTENT_TYPE,
                content_encoding,
                cache_control: Some(cache::TILE),
            }
            .into_response()
        }
    };
    Ok(response)
}

/// Resolves the physical PMTiles archive to read for a request, applying
/// Mapterhorn composite rules. Returns the archive's tileset id to serve, or
/// `None` to respond 404 (a z>12 detail region with no detail archive). Tiles
/// that aren't the composite tileset pass straight through.
async fn resolve_archive(
    state: &AppState,
    tileset_id: TilesetId,
    z: u8,
    x: u32,
    y: u32,
) -> Result<Option<TilesetId>, HttpError> {
    let Some(mapterhorn) = state.mapterhorn() else {
        return Ok(Some(tileset_id));
    };
    if !mapterhorn.matches(&tileset_id) {
        return Ok(Some(tileset_id));
    }
    // The presence probe (a header read) is single-flighted and cached inside
    // the resolver, so concurrent z13+ requests for a cold detail archive share
    // one object-store lookup and absent regions aren't re-probed.
    let resolver = state.resource_resolver.clone();
    let resolved = mapterhorn
        .resolve(z, x, y, move |detail| async move {
            match resolver.load_tileset_info(detail).await {
                Ok(Some(_)) => Ok(true),
                Ok(None) => Ok(false),
                Err(error) => Err(error),
            }
        })
        .await;
    match resolved {
        Ok(Resolved::Base(base)) => {
            state.metrics.record_mapterhorn("base");
            Ok(Some(base))
        }
        Ok(Resolved::Detail(detail)) => {
            state.metrics.record_mapterhorn("detail");
            Ok(Some(detail))
        }
        Ok(Resolved::Absent) => {
            state.metrics.record_mapterhorn("detail_negative");
            Ok(None)
        }
        Err(error) => {
            state.metrics.record_mapterhorn("detail_error");
            Err(tileset_error_response(&error))
        }
    }
}

/// Serves the internal tile endpoint used for node-to-node forwarding.
pub(crate) async fn internal_tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, tile_id)): Path<(String, u64)>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = TilesetId::try_from(tileset_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    state
        .resource_resolver
        .load_tile_by_id(tileset_id, tile_id)
        .await
        .map_err(|e| tileset_error_response(&e))?
        .map(|tile| {
            state.metrics.add_internal_bytes(tile.bytes.len() as u64);
            debug!(
                endpoint = "internal_tile",
                served_bytes = tile.bytes.len(),
                "served internal response"
            );
            TilesetResponse::from(tile).into_response()
        })
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))
}

struct TilesetResponse {
    bytes: bytes::Bytes,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
    cache_control: Option<&'static str>,
}

impl From<TileData> for TilesetResponse {
    /// Converts tile bytes plus headers into an HTTP response wrapper.
    ///
    /// No `Cache-Control` is set by default; node-to-node forwarding responses
    /// stay uncached. External responses opt in via [`Self::with_cache_control`].
    fn from(tile: TileData) -> Self {
        Self {
            bytes: tile.bytes,
            content_type: tile.content_type,
            content_encoding: tile.content_encoding,
            cache_control: None,
        }
    }
}

impl TilesetResponse {
    /// Attaches a public `Cache-Control` value to the response.
    fn with_cache_control(mut self, value: &'static str) -> Self {
        self.cache_control = Some(value);
        self
    }
}

impl IntoResponse for TilesetResponse {
    /// Finalizes the wrapped tile into an HTTP response.
    fn into_response(self) -> Response {
        let mut response = Response::new(Body::from(self.bytes));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(self.content_type),
        );
        if let Some(content_encoding) = self.content_encoding {
            response.headers_mut().insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_static(content_encoding),
            );
        }
        if let Some(cache_control) = self.cache_control {
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static(cache_control),
            );
        }
        response
    }
}
