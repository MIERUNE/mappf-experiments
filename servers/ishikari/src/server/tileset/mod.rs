//! HTTP handlers and response helpers for tileset endpoints.

use axum::http::StatusCode;
use serde::Deserialize;

use ishikari_core::{interned::TilesetId, pmtiles::TileCoord};

use super::HttpError;

mod error;
pub(crate) mod mapterhorn;
mod mlt;
// Crate-visible so `schemas` can name the OpenAPI companion types utoipa
// generates beside each handler; those are not covered by the re-exports below.
pub(crate) mod preview;
pub(crate) mod terrain;
pub(crate) mod tile;
pub(crate) mod tilejson;

pub(crate) use error::tileset_error_response;
pub(crate) use preview::{
    namespaced_preview_handler, namespaced_preview_style_handler, preview_handler,
    preview_style_handler, render_preview_html,
};
pub(crate) use terrain::{
    derived_tile_handler, derived_tilejson_handler, internal_derived_tile_handler,
    namespaced_derived_tile_handler, namespaced_derived_tilejson_handler,
};
pub(crate) use tile::{internal_tile_handler, namespaced_tile_handler, tile_handler};
pub(crate) use tilejson::{namespaced_tilejson_handler, tilejson_handler};

/// Parse one tile coordinate, accepting only its canonical decimal spelling.
///
/// `u8`/`u32`'s `FromStr` accepts a leading `+` and unlimited leading zeros, so
/// `2`, `+2` and `0000002` all name the same tile. The internal cache keys on the
/// parsed integers and so collapses them, but every spelling is a separate entry
/// in each cache in front of this service, under the hour-long `max-age` a tile
/// carries — an unbounded family of URLs returning byte-identical payloads, able
/// to fill a shared cache with entries that can never be hit again and that
/// displace tiles someone actually wants. Only the shortest form is accepted, the
/// same stance the router already takes on non-canonical path aliases.
pub(crate) fn parse_tile_coordinate<T: std::str::FromStr>(
    label: &str,
    raw: &str,
) -> Result<T, HttpError> {
    let canonical = !raw.is_empty()
        && raw.bytes().all(|byte| byte.is_ascii_digit())
        && (raw == "0" || !raw.starts_with('0'));
    if !canonical {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("tile {label} must be a decimal integer without a sign or leading zeros"),
        ));
    }
    raw.parse().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            format!("tile {label} is out of range: {raw}"),
        )
    })
}

/// Parses a raw tileset id into its typed form, mapping validation failure to
/// the standard bad-request tuple used by every tileset handler.
pub(crate) fn parse_tileset_id(raw: String) -> Result<TilesetId, HttpError> {
    TilesetId::try_from(raw).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))
}

/// Parses a validated coordinate triple, mapping construction failure to the
/// standard bad-request tuple.
pub(crate) fn parse_tile_coord(z: u8, x: u32, y: u32) -> Result<TileCoord, HttpError> {
    TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))
}

/// Every encoding a caller may name in `?encoding=`, in canonical spelling:
/// `mvt`/`mlt` select the vector representation, `terrarium`/`terrainrgb` the DEM
/// scheme a raster preview decodes with.
const REQUESTABLE_ENCODINGS: [&str; 4] = ["mvt", "mlt", "terrarium", "terrainrgb"];

/// Map `?encoding=` onto that fixed vocabulary, or refuse the request.
///
/// Returning `&'static str` is the point rather than an implementation detail:
/// callers interpolate the result into TileJSON tile URLs and into the preview
/// shell, where it lands inside a JavaScript string literal that the template
/// substitutes without escaping. Yielding a borrowed constant makes it
/// structurally impossible for a caller's bytes to reach either place.
///
/// An unrecognized value is a `400`. It used to fall through to MVT, so
/// `?encoding=mtl` served MVT tile URLs while the caller believed it had asked
/// for MLT, and every distinct spelling minted its own separately cacheable URL
/// for identical content.
pub(crate) fn canonical_encoding(raw: Option<&str>) -> Result<Option<&'static str>, HttpError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    REQUESTABLE_ENCODINGS
        .into_iter()
        .find(|candidate| candidate.eq_ignore_ascii_case(raw))
        .map(Some)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!(
                    "encoding must be one of {}",
                    REQUESTABLE_ENCODINGS.join(", ")
                ),
            )
        })
}

/// Query fields that are meaningful on TileJSON but invalid on tile payloads.
///
/// Unknown fields remain allowed for cache-busting and delivery credentials.
#[derive(Deserialize)]
pub(crate) struct TileRepresentationQuery {
    encoding: Option<String>,
}

impl TileRepresentationQuery {
    fn reject_encoding(&self) -> Result<(), HttpError> {
        if self.encoding.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "tile encoding must be selected with a path suffix such as .mlt".to_string(),
            ));
        }
        Ok(())
    }
}

/// Joins a namespaced route's `(namespace, tileset_id)` path segments into the
/// flat `namespace/tileset_id` key the `serve_*` helpers expect. One home for
/// the join convention shared by the namespaced tile/tilejson/preview handlers.
fn join_tileset_key(namespace: &str, tileset_id: &str) -> String {
    format!("{namespace}/{tileset_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tile_coordinate_is_accepted_only_in_its_shortest_decimal_form() {
        assert_eq!(parse_tile_coordinate::<u32>("x", "0").unwrap(), 0);
        assert_eq!(parse_tile_coordinate::<u32>("x", "2").unwrap(), 2);
        assert_eq!(parse_tile_coordinate::<u32>("x", "1024").unwrap(), 1024);

        // Each of these is `2` to Rust's `FromStr`, and each was a separate
        // cacheable URL for byte-identical tile bytes.
        for alias in ["+2", "02", "0002", &"0".repeat(60), "2 ", " 2", "2.0", ""] {
            let error = parse_tile_coordinate::<u32>("x", alias)
                .expect_err(&format!("{alias:?} was accepted as a coordinate"));
            assert_eq!(error.0, StatusCode::BAD_REQUEST);
        }
        // A leading `-` is refused by the canonical-form check, so the message
        // names the form rather than leaking that `u32` cannot be negative.
        let (status, message) = parse_tile_coordinate::<u32>("x", "-1").unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            message.contains("without a sign or leading zeros"),
            "{message}"
        );
        // Canonical but too large is a range error, not a form error.
        let (status, message) = parse_tile_coordinate::<u8>("z", "256").unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(message.contains("out of range"), "{message}");
    }

    #[test]
    fn a_requested_encoding_is_narrowed_to_a_borrowed_constant_or_refused() {
        assert_eq!(canonical_encoding(None).unwrap(), None);
        for (raw, expected) in [
            ("mvt", "mvt"),
            ("mlt", "mlt"),
            ("MLT", "mlt"),
            ("Terrarium", "terrarium"),
            ("terrainrgb", "terrainrgb"),
        ] {
            assert_eq!(canonical_encoding(Some(raw)).unwrap(), Some(expected));
        }
        // `mtl` used to fall through to MVT, so the caller was served MVT tile
        // URLs while believing it had asked for MLT.
        for raw in ["mtl", "bogus", "", "mlt ", "\"+alert(1)+\""] {
            let (status, message) = canonical_encoding(Some(raw))
                .expect_err(&format!("{raw:?} was accepted as an encoding"));
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(
                !message.contains(raw) || raw.is_empty(),
                "the refusal echoed caller bytes: {message}"
            );
        }
    }
}
