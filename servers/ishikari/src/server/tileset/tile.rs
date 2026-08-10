//! Axum handlers for tile-serving endpoints.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{self},
    },
    response::{IntoResponse, Response},
};
use tracing::debug;

use crate::server::{AppState, HttpError, cache};
use ishikari_core::{
    interned::{ResourceRoutingKey, TilesetId},
    pmtiles::{MLT_CONTENT_TYPE, TileData, TileId},
    storage::{
        ARCHIVE_GENERATION_HEADER, ArchivePresence, InternalTileSource, TILE_SOURCE_HEADER,
        TileSource,
    },
};

use super::error::tileset_error_response;
use super::mapterhorn::Resolved;
use super::mlt::{
    Representation, RequestedTileFormat, is_mlt_tile, mlt_response_bytes, negotiate_format,
};
use super::{TileRepresentationQuery, parse_tile_coord, parse_tileset_id};

/// Parses the numeric tile `y` (after extension stripping).
fn parse_y(y: &str) -> Result<u32, HttpError> {
    super::parse_tile_coordinate("y", y)
}

/// Serves the external z/x/y tile endpoint for a flat tileset key.
#[cfg_attr(
    feature = "unstable-schemas",
    utoipa::path(
        get,
        path = "/tilesets/{tileset_id}/{z}/{x}/{y}",
        tag = "delivery",
        params(
            ("tileset_id" = String, Path, description = "Flat tileset key"),
            ("z" = u8, Path, description = "Zoom level"),
            ("x" = u32, Path, description = "Tile column"),
            (
                "y" = String,
                Path,
                description = "Tile row, optionally suffixed to select a representation (`.mlt`, `.mvt`, `.pbf`)"
            )
        ),
        responses(
            (
                status = 200,
                description = "Stored or negotiated tile payload",
                content(
                    (crate::schemas::BinaryPayload = "application/vnd.mapbox-vector-tile"),
                    (crate::schemas::BinaryPayload = "application/vnd.maplibre-tile"),
                    (crate::schemas::BinaryPayload = "image/png"),
                    (crate::schemas::BinaryPayload = "image/jpeg"),
                    (crate::schemas::BinaryPayload = "image/webp"),
                    (crate::schemas::BinaryPayload = "image/avif"),
                    (crate::schemas::BinaryPayload = "application/octet-stream")
                )
            ),
            (
                status = 204,
                description = "The archive holds a zero-byte entry for this tile"
            ),
            (status = 404, description = "Unknown tileset or absent tile"),
            (status = 406, description = "Stored content encoding is not acceptable")
        )
    )
)]
pub(crate) async fn tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, z_raw, x_raw, y_raw)): Path<(String, String, String, String)>,
    Query(query): Query<TileRepresentationQuery>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    query.reject_encoding()?;
    let chosen = negotiate_format(&y_raw, &headers);
    serve_tile(
        state,
        tileset_id,
        super::parse_tile_coordinate("z", &z_raw)?,
        super::parse_tile_coordinate("x", &x_raw)?,
        parse_y(chosen.y)?,
        chosen.representation,
        &headers,
    )
    .await
}

/// Serves the external z/x/y tile endpoint for a `{namespace}/{tileset_id}` key.
#[cfg_attr(
    feature = "unstable-schemas",
    utoipa::path(
        get,
        path = "/tilesets/{namespace}/{tileset_id}/{z}/{x}/{y}",
        tag = "delivery",
        params(
            ("namespace" = String, Path, description = "Tileset namespace"),
            ("tileset_id" = String, Path, description = "Namespace-local tileset id"),
            ("z" = u8, Path, description = "Zoom level"),
            ("x" = u32, Path, description = "Tile column"),
            (
                "y" = String,
                Path,
                description = "Tile row, optionally suffixed to select a representation (`.mlt`, `.mvt`, `.pbf`)"
            )
        ),
        responses(
            (
                status = 200,
                description = "Stored or negotiated tile payload",
                content(
                    (crate::schemas::BinaryPayload = "application/vnd.mapbox-vector-tile"),
                    (crate::schemas::BinaryPayload = "application/vnd.maplibre-tile"),
                    (crate::schemas::BinaryPayload = "image/png"),
                    (crate::schemas::BinaryPayload = "image/jpeg"),
                    (crate::schemas::BinaryPayload = "image/webp"),
                    (crate::schemas::BinaryPayload = "image/avif"),
                    (crate::schemas::BinaryPayload = "application/octet-stream")
                )
            ),
            (
                status = 204,
                description = "The archive holds a zero-byte entry for this tile"
            ),
            (status = 404, description = "Unknown tileset or absent tile"),
            (status = 406, description = "Stored content encoding is not acceptable")
        )
    )
)]
pub(crate) async fn namespaced_tile_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id, z_raw, x_raw, y_raw)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<TileRepresentationQuery>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    query.reject_encoding()?;
    let chosen = negotiate_format(&y_raw, &headers);
    serve_tile(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        super::parse_tile_coordinate("z", &z_raw)?,
        super::parse_tile_coordinate("x", &x_raw)?,
        parse_y(chosen.y)?,
        chosen.representation,
        &headers,
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
    representation: Representation,
    headers: &HeaderMap,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = parse_tileset_id(tileset_id)?;
    let tile_id = TileId::from(parse_tile_coord(z, x, y)?).value();
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
    // Cache outcomes describe the resolution that just happened, so they are
    // recorded whatever the response turns out to be. `tiles_served` counts
    // delivered tiles, so it waits until a response actually exists: an absent
    // tile, an unacceptable encoding, or a failed transcode is not a serve.
    for outcome in state.resource_resolver.cache_outcomes(source) {
        state.metrics.record_tile_cache(outcome);
    }
    let Some(tile) = tile else {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    };
    let generation = tile.generation;
    let tile = tile.data;
    // A stored entry with no bytes cannot be served as a representation: the
    // archive's compression applies to every entry, so it would go out as an
    // empty body labelled `Content-Encoding: gzip`, which no client can decode.
    // Answer `204` instead — the archive positively says this tile is empty, so
    // there is nothing to encode, transcode, or validate.
    //
    // Deliberately narrow. A conventionally empty tile is a *compressed* empty
    // payload, so its bytes are non-empty and it is served normally; detecting
    // that would mean decompressing every tile. An *absent* tile keeps its
    // `404`, because `204` is cacheable and would pin "no tile here" in shared
    // caches far past the deliberately short negative TTL.
    if tile.bytes.is_empty() {
        state.metrics.record_tile_served(source.report_label());
        return Ok(empty_tile_response());
    }
    let Representation {
        format,
        negotiated_on_accept,
    } = representation;
    let response = match format {
        RequestedTileFormat::AsStored => {
            ensure_content_encoding_acceptable(headers, tile.content_encoding)?;
            state.metrics.add_egress_bytes(tile.bytes.len() as u64);
            debug!(
                endpoint = "tile",
                format = "as_stored",
                content_type = tile.content_type,
                source = source.report_label(),
                served_bytes = tile.bytes.len(),
                "served external response"
            );
            TilesetResponse::from(tile)
                .with_cache_control(cache::TILE)
                .negotiated_on_accept(negotiated_on_accept)
                .into_response()
        }
        RequestedTileFormat::Mlt => {
            // Transcoded MLT is always gzip. Reject an incompatible request
            // before entering the CPU-work queue; native MLT retains its
            // archive encoding.
            let expected_encoding = if is_mlt_tile(&tile) {
                tile.content_encoding
            } else {
                Some("gzip")
            };
            ensure_content_encoding_acceptable(headers, expected_encoding)?;
            let routing_key = ResourceRoutingKey::from(&tileset_id);
            let (bytes, content_encoding, served_format) = mlt_response_bytes(
                &state,
                &routing_key,
                &generation,
                tile_id,
                tile,
                super::mlt::TranscodeCachePolicy::Retain,
            )
            .await?;
            debug_assert_eq!(content_encoding, expected_encoding);
            state.metrics.add_egress_bytes(bytes.len() as u64);
            debug!(
                endpoint = "tile",
                format = served_format,
                source = source.report_label(),
                served_bytes = bytes.len(),
                "served external response"
            );
            TilesetResponse {
                bytes,
                content_type: MLT_CONTENT_TYPE,
                content_encoding,
                cache_control: Some(cache::TILE),
                negotiated_on_accept,
            }
            .into_response()
        }
    };
    state.metrics.record_tile_served(source.report_label());
    Ok(response)
}

/// Builds the `204` served for a zero-byte stored entry.
///
/// It carries the ordinary tile cache policy because an archive generation's
/// empty entry is a stable positive fact, and no `Vary`, `Content-Type`, or
/// `Content-Encoding`, because a `204` has no representation for a request
/// header to select or a client to decode.
fn empty_tile_response() -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(cache::TILE));
    response
}

/// Resolves the physical PMTiles archive to read for a request, applying
/// Mapterhorn composite rules. Returns the archive's tileset id to serve, or
/// `None` to respond 404 (a z>12 detail region with no detail archive). Tiles
/// that aren't the composite tileset pass straight through.
pub(super) async fn resolve_archive(
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
            match resolver.archive_presence(detail).await {
                Ok(ArchivePresence::Present) => Ok(true),
                Ok(ArchivePresence::Absent) => Ok(false),
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

/// Builds the standard public tile payload response, including transport
/// encoding and cache policy. Shared by stored and generated tile products.
pub(super) fn tile_data_response(
    tile: TileData,
    headers: &HeaderMap,
) -> Result<Response, HttpError> {
    ensure_content_encoding_acceptable(headers, tile.content_encoding)?;
    Ok(TilesetResponse::from(tile)
        .with_cache_control(cache::TILE)
        .into_response())
}

/// Serves the internal tile endpoint used for node-to-node forwarding.
pub(crate) async fn internal_tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, tile_id)): Path<(String, u64)>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = parse_tileset_id(tileset_id)?;
    let (tile, source) = state
        .resource_resolver
        .load_tile_by_id_with_source(tileset_id, tile_id)
        .await
        .map_err(|e| tileset_error_response(&e))?;
    let source = match source {
        TileSource::SelfTileCache | TileSource::SelfChunkCache => InternalTileSource::Cache,
        TileSource::SelfBackend => InternalTileSource::Backend,
        _ => return Err((StatusCode::NOT_FOUND, "not found".to_string())),
    };
    tile.map(|tile| {
        let generation = tile.generation.to_wire();
        state
            .metrics
            .add_internal_bytes(tile.data.bytes.len() as u64);
        debug!(
            endpoint = "internal_tile",
            served_bytes = tile.data.bytes.len(),
            "served internal response"
        );
        let mut response = TilesetResponse::from(tile.data).into_response();
        response.headers_mut().insert(
            TILE_SOURCE_HEADER,
            HeaderValue::from_static(source.as_str()),
        );
        response.headers_mut().insert(
            ARCHIVE_GENERATION_HEADER,
            HeaderValue::from_str(&generation).expect("archive generation is a valid header value"),
        );
        response
    })
    .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))
}

struct TilesetResponse {
    bytes: bytes::Bytes,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
    cache_control: Option<&'static str>,
    /// Set only when `Accept` chose the media type, so `Vary` lists it only
    /// where it can change the response. Defaults to false: a suffixed URL, a
    /// raster product, and an internal forward are all fixed representations.
    negotiated_on_accept: bool,
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
            negotiated_on_accept: false,
        }
    }
}

impl TilesetResponse {
    /// Attaches a public `Cache-Control` value to the response.
    fn with_cache_control(mut self, value: &'static str) -> Self {
        self.cache_control = Some(value);
        self
    }

    /// Records that `Accept` chose this representation, so `Vary` must list it.
    fn negotiated_on_accept(mut self, negotiated: bool) -> Self {
        self.negotiated_on_accept = negotiated;
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
            // `Accept-Encoding` always participates: the stored transport
            // representation is chosen from it, and every CDN normalizes and keys
            // on that header natively.
            //
            // `Accept` is listed only where it actually selects the media type,
            // which is the suffix-less URL. Advertising it on a suffixed or raster
            // URL would invite a cache to key on a header that cannot change the
            // response, letting arbitrary `Accept` values multiply variants of one
            // immutable tile.
            let vary = if self.negotiated_on_accept {
                "Accept, Accept-Encoding"
            } else {
                "Accept-Encoding"
            };
            response
                .headers_mut()
                .insert(header::VARY, HeaderValue::from_static(vary));
        }
        response
    }
}

/// Checks whether the representation Ishikari already has can be sent without
/// changing its transport encoding.
///
/// The common path remains zero-copy: a missing `Accept-Encoding` accepts any
/// coding, and a matching coding is served as stored. Ishikari deliberately
/// does not decompress or cross-compress tiles on this path; when the client
/// excludes the only available representation, it returns `406`.
pub(super) fn ensure_content_encoding_acceptable(
    headers: &HeaderMap,
    content_encoding: Option<&str>,
) -> Result<(), HttpError> {
    if content_encoding_is_acceptable(headers, content_encoding) {
        Ok(())
    } else {
        Err((
            StatusCode::NOT_ACCEPTABLE,
            "no acceptable tile content encoding is available".to_string(),
        ))
    }
}

fn content_encoding_is_acceptable(headers: &HeaderMap, content_encoding: Option<&str>) -> bool {
    if !headers.contains_key(header::ACCEPT_ENCODING) {
        return true;
    }

    let requested = content_encoding.unwrap_or("identity");
    let mut exact_quality: Option<f32> = None;
    let mut wildcard_quality: Option<f32> = None;

    for value in headers.get_all(header::ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for item in value.split(',') {
            let mut parts = item.split(';');
            let Some(coding) = parts
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let mut quality = 1.0_f32;
            for parameter in parts {
                let Some((name, value)) = parameter.split_once('=') else {
                    continue;
                };
                if name.trim().eq_ignore_ascii_case("q") {
                    quality = match value.trim().parse::<f32>() {
                        Ok(value) if value.is_finite() && (0.0..=1.0).contains(&value) => value,
                        _ => 0.0,
                    };
                }
            }

            if coding.eq_ignore_ascii_case(requested) {
                exact_quality = Some(exact_quality.map_or(quality, |current| current.max(quality)));
            } else if coding == "*" {
                wildcard_quality =
                    Some(wildcard_quality.map_or(quality, |current| current.max(quality)));
            }
        }
    }

    if let Some(quality) = exact_quality {
        return quality > 0.0;
    }
    if requested.eq_ignore_ascii_case("identity") {
        // Identity is acceptable by default. It is excluded only by an
        // explicit identity entry above, or by `*;q=0` when identity is absent.
        return wildcard_quality != Some(0.0);
    }
    wildcard_quality.is_some_and(|quality| quality > 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tile() -> TileData {
        TileData {
            bytes: bytes::Bytes::from_static(b"tile"),
            content_type: "application/x-protobuf",
            content_encoding: None,
        }
    }

    #[test]
    fn a_zero_byte_stored_entry_is_answered_with_204() {
        let response = super::empty_tile_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        // An empty body must not claim an encoding or a media type: labelling it
        // `Content-Encoding: gzip` is exactly the defect this replaces.
        assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
        assert!(response.headers().get(header::CONTENT_TYPE).is_none());
        // Nothing varies when there is no representation to select.
        assert!(response.headers().get(header::VARY).is_none());
        // The archive is immutable, so an empty entry is ordinarily cacheable.
        assert_eq!(response.headers()[header::CACHE_CONTROL], cache::TILE);
    }

    #[test]
    fn a_fixed_representation_varies_only_on_accept_encoding() {
        // Derived products and suffixed URLs pin their representation, so
        // advertising `Accept` would let a cache key on a header that cannot
        // change the response.
        let response = tile_data_response(tile(), &HeaderMap::new()).unwrap();
        assert_eq!(response.headers()[header::CACHE_CONTROL], cache::TILE);
        assert_eq!(response.headers()[header::VARY], "Accept-Encoding");
    }

    #[test]
    fn an_accept_negotiated_representation_also_varies_on_accept() {
        let response = TilesetResponse::from(tile())
            .with_cache_control(cache::TILE)
            .negotiated_on_accept(true)
            .into_response();
        assert_eq!(response.headers()[header::VARY], "Accept, Accept-Encoding");
    }

    #[test]
    fn internal_tile_responses_do_not_advertise_negotiation() {
        let response = TilesetResponse::from(tile()).into_response();
        assert!(response.headers().get(header::CACHE_CONTROL).is_none());
        assert!(response.headers().get(header::VARY).is_none());
    }

    fn accept_encoding(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT_ENCODING, HeaderValue::from_static(value));
        headers
    }

    #[test]
    fn absent_accept_encoding_accepts_stored_codings() {
        let headers = HeaderMap::new();
        assert!(content_encoding_is_acceptable(&headers, Some("gzip")));
        assert!(content_encoding_is_acceptable(&headers, None));
    }

    #[test]
    fn exact_or_wildcard_coding_accepts_the_stored_representation() {
        assert!(content_encoding_is_acceptable(
            &accept_encoding("br, gzip;q=0.5"),
            Some("gzip")
        ));
        assert!(content_encoding_is_acceptable(
            &accept_encoding("*;q=0.5"),
            Some("zstd")
        ));
    }

    #[test]
    fn repeated_accept_encoding_fields_are_combined() {
        let mut headers = accept_encoding("br;q=1");
        headers.append(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("gzip;q=0.5"),
        );
        assert!(content_encoding_is_acceptable(&headers, Some("gzip")));
    }

    #[test]
    fn exact_exclusion_overrides_a_permissive_wildcard() {
        assert!(!content_encoding_is_acceptable(
            &accept_encoding("gzip;q=0, *;q=1"),
            Some("gzip")
        ));
    }

    #[test]
    fn identity_is_acceptable_by_default_but_can_be_excluded() {
        assert!(content_encoding_is_acceptable(
            &accept_encoding("gzip"),
            None
        ));
        assert!(!content_encoding_is_acceptable(
            &accept_encoding("identity;q=0"),
            None
        ));
        assert!(!content_encoding_is_acceptable(
            &accept_encoding("*;q=0"),
            None
        ));
        assert!(content_encoding_is_acceptable(
            &accept_encoding("*;q=0, identity;q=1"),
            None
        ));
    }

    #[test]
    fn empty_accept_encoding_requests_identity_only() {
        let headers = accept_encoding("");
        assert!(content_encoding_is_acceptable(&headers, None));
        assert!(!content_encoding_is_acceptable(&headers, Some("gzip")));
    }

    #[test]
    fn rejected_stored_encoding_returns_not_acceptable() {
        let error = ensure_content_encoding_acceptable(
            &accept_encoding("br;q=1, gzip;q=0, identity;q=0"),
            Some("gzip"),
        )
        .unwrap_err();
        assert_eq!(error.0, StatusCode::NOT_ACCEPTABLE);
    }
}
