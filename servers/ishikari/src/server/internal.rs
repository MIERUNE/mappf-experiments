//! Internal PMTiles forwarding endpoints shared across cluster nodes.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode},
    response::Response,
};
use bytes::BufMut;
use serde::Deserialize;
use tracing::debug;

use crate::server::{AppState, HttpError, bytes_response};
use ishikari_core::storage::{ARCHIVE_GENERATION_HEADER, LeafBytesError};

use super::tileset::{parse_tileset_id, tileset_error_response};

#[derive(Deserialize)]
pub(crate) struct BootstrapQuery {
    #[serde(default)]
    metadata: bool,
}

/// Serves PMTiles bootstrap bytes for peer cache reuse, optionally including metadata.
pub(crate) async fn internal_bootstrap_handler(
    State(state): State<AppState>,
    Path(tileset_id): Path<String>,
    Query(query): Query<BootstrapQuery>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = parse_tileset_id(tileset_id)?;
    let include_metadata = query.metadata;
    let transfer = state
        .resource_resolver
        .load_bootstrap_bytes(tileset_id.clone(), include_metadata)
        .await
        .map_err(|e| tileset_error_response(&e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    let generation = transfer.generation.to_wire();

    let body_bytes = if let Some(metadata) = transfer.metadata {
        let bootstrap_len = transfer.bootstrap.len() as u64;
        let mut buf = bytes::BytesMut::with_capacity(8 + transfer.bootstrap.len() + metadata.len());
        buf.put_u64_le(bootstrap_len);
        buf.extend_from_slice(&transfer.bootstrap);
        buf.extend_from_slice(&metadata);
        buf.freeze()
    } else {
        transfer.bootstrap
    };

    state.metrics.add_internal_bytes(body_bytes.len() as u64);
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            endpoint = "internal_bootstrap",
            tileset_id = %tileset_id,
            include_metadata = include_metadata,
            served_bytes = body_bytes.len(),
            "served internal response"
        );
    }
    let mut response = bytes_response(body_bytes, "application/octet-stream", None);
    insert_generation_header(&mut response, &generation)?;
    Ok(response)
}

/// Serves raw PMTiles leaf bytes for peer cache reuse.
pub(crate) async fn internal_leaf_handler(
    State(state): State<AppState>,
    Path((tileset_id, offset, length)): Path<(String, u64, usize)>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = parse_tileset_id(tileset_id)?;
    let leaf = state
        .resource_resolver
        .load_leaf_bytes(tileset_id.clone(), offset, length)
        .await
        .map_err(|error| leaf_error_response(&error))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    state.metrics.add_internal_bytes(leaf.value.len() as u64);
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            endpoint = "internal_leaf",
            tileset_id = %tileset_id,
            served_bytes = leaf.value.len(),
            "served internal response"
        );
    }
    let mut response = bytes_response(leaf.value, "application/octet-stream", None);
    insert_generation_header(&mut response, &leaf.generation.to_wire())?;
    Ok(response)
}

fn insert_generation_header(
    response: &mut Response<Body>,
    generation: &str,
) -> Result<(), HttpError> {
    response.headers_mut().insert(
        ARCHIVE_GENERATION_HEADER,
        HeaderValue::from_str(generation).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid archive generation".to_string(),
            )
        })?,
    );
    Ok(())
}

fn leaf_error_response(error: &LeafBytesError) -> HttpError {
    match error {
        LeafBytesError::InvalidRange => (StatusCode::BAD_REQUEST, "invalid leaf range".to_string()),
        LeafBytesError::Tileset(error) => tileset_error_response(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ishikari_core::storage::TilesetError;

    #[test]
    fn invalid_leaf_ranges_remain_bad_requests() {
        assert_eq!(
            leaf_error_response(&LeafBytesError::InvalidRange),
            (StatusCode::BAD_REQUEST, "invalid leaf range".to_string())
        );
    }

    #[test]
    fn leaf_storage_errors_retain_tileset_error_mapping() {
        assert_eq!(
            leaf_error_response(&LeafBytesError::Tileset(TilesetError::Timeout(
                "timed out".to_string()
            ))),
            (
                StatusCode::GATEWAY_TIMEOUT,
                "upstream tileset request timed out".to_string()
            )
        );
    }
}
