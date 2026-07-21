//! MLT (MapLibre Tile) serving: request-format negotiation, on-the-fly
//! MVT → MLT transcoding, and the response bytes the tile handler emits. The
//! tile handler owns routing/orchestration; everything MLT-specific lives here.
//!
//! Uses the reference `mlt-core` encoder — the same crate Martin and the `mlt`
//! CLI use. Input is a single *decompressed* MVT tile; output is raw MLT bytes
//! (the caller applies transport compression). Encoder defaults match the
//! reference toolchain (Morton sort attempt, FSST + FastPFOR + shared dict on).
//!
//! ## Integer-property narrowing
//!
//! MVT encodes every integer attribute as a 64-bit varint, and `mlt-core`'s MVT
//! importer types them all as `I64`/`U64` columns regardless of their actual
//! range. The `@maplibre/mlt` decoder bundled in maplibre-gl materializes any
//! 64-bit column as a JS `BigInt`, and maplibre-gl's web-worker transfer cannot
//! serialize a `BigInt` ("Do not know how to serialize a BigInt"), so the whole
//! tile fails to render. This bites OMT-derived tiles hard: e.g. the `building`
//! layer's `render_height`/`render_min_height` (values like 26, 8, 0 — the
//! fill-extrusion height/base) come through as `BigInt` and crash 3D building
//! rendering from ~z15.
//!
//! We sidestep it by narrowing each `I64`/`U64` property column to the smallest
//! type that holds every value before encoding: `I32`/`U32` when in range, else
//! `F64` when within `2^53` (JS-safe). Those decode to plain JS `Number`. The
//! id column is unaffected — `mlt-core` already emits it via the dedicated id
//! path, which maplibre decodes as a `Number` for ids below `2^53`.
//!
//! TODO: remove this narrowing workaround once the MapLibre GL / `@maplibre/mlt`
//! decode path can render 64-bit property columns without producing worker
//! transfer failures or unusable `BigInt` paint/layout values.

use std::io::{Read, Write};

use anyhow::{Context, Result};
use axum::http::{HeaderMap, StatusCode, header};
use bytes::Bytes;
use flate2::{Compression as GzLevel, read::GzDecoder, write::GzEncoder};
use mlt_core::encoder::EncoderConfig;
use mlt_core::{PropKind, PropValue, TileLayer};

use crate::server::{AppState, HttpError};
use ishikari_core::interned::ResourceRoutingKey;
use ishikari_core::pmtiles::{MLT_CONTENT_TYPE, TileData, TileType};

/// Transcodes one decompressed MVT tile into raw MLT bytes. Concatenates the
/// per-layer MLT encodings the same way the reference `mlt` CLI does.
pub(crate) fn mvt_to_mlt(mvt: &[u8]) -> Result<Vec<u8>> {
    let cfg = EncoderConfig::default();
    let mut out = Vec::new();
    for layer in mlt_core::mvt::mvt_to_tile_layers(mvt).context("parse MVT layers")? {
        // Only rebuild layers that actually carry 64-bit integer columns;
        // everything else (the common case) encodes straight through.
        let layer = if has_wide_int_column(&layer) {
            narrow_int_columns(&layer).context("narrow MLT integer columns")?
        } else {
            layer
        };
        let mut encoded = layer.encode(cfg).context("encode MLT layer")?;
        out.append(&mut encoded);
    }
    Ok(out)
}

/// Largest unsigned integer exactly representable as an `f64` (`Number.MAX_SAFE_INTEGER + 1`).
const JS_SAFE_INT: u64 = 1 << 53;

/// Whether any property column in the layer is a 64-bit integer (so would
/// decode to a `BigInt` in the browser).
fn has_wide_int_column(layer: &TileLayer) -> bool {
    let Some(first) = layer.features().first() else {
        return false;
    };
    first
        .properties()
        .iter()
        .any(|p| matches!(PropKind::from(p), PropKind::I64 | PropKind::U64))
}

/// Picks the narrowest property kind that losslessly holds every value in the
/// column. Only `I64`/`U64` are narrowed; all other kinds pass through.
fn narrowed_kind<'a>(kind: PropKind, values: impl Iterator<Item = &'a PropValue>) -> PropKind {
    match kind {
        PropKind::I64 => {
            let (mut all_i32, mut all_safe) = (true, true);
            for v in values {
                if let PropValue::I64(Some(x)) = v {
                    all_i32 &= i32::try_from(*x).is_ok();
                    all_safe &= x.unsigned_abs() < JS_SAFE_INT;
                }
            }
            if all_i32 {
                PropKind::I32
            } else if all_safe {
                PropKind::F64
            } else {
                PropKind::I64
            }
        }
        PropKind::U64 => {
            let (mut all_u32, mut all_safe) = (true, true);
            for v in values {
                if let PropValue::U64(Some(x)) = v {
                    all_u32 &= u32::try_from(*x).is_ok();
                    all_safe &= *x < JS_SAFE_INT;
                }
            }
            if all_u32 {
                PropKind::U32
            } else if all_safe {
                PropKind::F64
            } else {
                PropKind::U64
            }
        }
        other => other,
    }
}

/// Converts one value to the narrowed target kind (preserving nulls). Values
/// only ever shrink within their numeric range, so the casts are lossless.
fn convert_value(value: &PropValue, target: PropKind) -> PropValue {
    match (target, value) {
        (PropKind::I32, PropValue::I64(o)) => PropValue::I32(o.map(|x| x as i32)),
        (PropKind::F64, PropValue::I64(o)) => PropValue::F64(o.map(|x| x as f64)),
        (PropKind::U32, PropValue::U64(o)) => PropValue::U32(o.map(|x| x as u32)),
        (PropKind::F64, PropValue::U64(o)) => PropValue::F64(o.map(|x| x as f64)),
        _ => value.clone(),
    }
}

/// Rebuilds a layer with its `I64`/`U64` property columns narrowed. `mlt-core`
/// validates that each value matches its column's declared kind and exposes no
/// in-place column retyping, so we reconstruct through the public builder.
fn narrow_int_columns(layer: &TileLayer) -> Result<TileLayer> {
    // TODO: this rebuild is a compatibility workaround, not part of the MLT
    // format contract. Prefer deleting it over extending it once clients handle
    // wide integer columns correctly.
    let names = layer.property_names();
    let features = layer.features();
    let ncols = names.len();

    let targets: Vec<PropKind> = (0..ncols)
        .map(|j| {
            let kind = PropKind::from(&features[0].properties()[j]);
            narrowed_kind(kind, features.iter().map(|f| &f.properties()[j]))
        })
        .collect();

    let mut builder =
        TileLayer::builder(layer.name(), layer.extent().get()).context("create layer builder")?;
    let keys = names
        .iter()
        .zip(&targets)
        .map(|(name, &kind)| builder.add_property(name.clone(), kind))
        .collect::<Result<Vec<_>, _>>()
        .context("declare narrowed columns")?;

    for feature in features {
        let mut fb = builder.feature(feature.geometry().clone());
        fb.id(feature.id());
        for (j, &key) in keys.iter().enumerate() {
            fb.property(key, convert_value(&feature.properties()[j], targets[j]))
                .context("set narrowed property")?;
        }
        fb.finish().context("finish feature")?;
    }
    Ok(builder.finish())
}

/// External tile representation requested by path extension or `Accept`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestedTileFormat {
    AsStored,
    Mlt,
}

/// Picks the response format and strips any extension off the `y` segment.
/// Priority: path extension (`.mlt` — the canonical, CDN-safe form) > `Accept:
/// application/vnd.maplibre-tile` (Martin-compatible). Defaults to the native
/// tile representation stored in PMTiles.
pub(crate) fn negotiate_format<'a>(
    y_raw: &'a str,
    headers: &HeaderMap,
) -> (&'a str, RequestedTileFormat) {
    let y = match y_raw.rsplit_once('.') {
        Some((y, "mlt")) => return (y, RequestedTileFormat::Mlt),
        // Raster terrain URLs are served as-stored.
        Some((y, "mvt" | "pbf" | "webp" | "jpg" | "jpeg")) => y,
        _ => y_raw,
    };
    let wants_mlt = headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .any(accept_value_allows_mlt);
    (
        y,
        if wants_mlt {
            RequestedTileFormat::Mlt
        } else {
            RequestedTileFormat::AsStored
        },
    )
}

fn accept_value_allows_mlt(value: &str) -> bool {
    let mut range_start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in value.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match byte {
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                if media_range_allows_mlt(&value[range_start..index]) {
                    return true;
                }
                range_start = index + 1;
            }
            _ => {}
        }
    }
    media_range_allows_mlt(&value[range_start..])
}

fn media_range_allows_mlt(range: &str) -> bool {
    let mut parts = range.split(';');
    if !parts
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(MLT_CONTENT_TYPE))
    {
        return false;
    }

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
    quality > 0.0
}

/// Returns MLT bytes for a tile. Native MLT PMTiles are served as-is; MVT
/// PMTiles are transcoded and gzip-compressed for transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TranscodeCachePolicy {
    Retain,
    Bypass,
}

pub(crate) async fn mlt_response_bytes(
    state: &AppState,
    routing_key: &ResourceRoutingKey,
    tile_id: u64,
    tile: TileData,
    cache_policy: TranscodeCachePolicy,
) -> Result<(Bytes, Option<&'static str>, &'static str), HttpError> {
    if is_mlt_tile(&tile) {
        // Native MLT PMTiles: serve as-is, no transcode.
        return Ok((tile.bytes, tile.content_encoding, "mlt_native"));
    }
    if tile.content_type != TileType::Mvt.content_type() {
        return Err((
            StatusCode::NOT_ACCEPTABLE,
            format!("cannot serve {} tile as MLT", tile.content_type),
        ));
    }
    let bytes = transcoded_mlt(state, routing_key, tile_id, tile, cache_policy).await?;
    Ok((bytes, Some("gzip"), "mlt_transcoded"))
}

/// Whether a fetched tile is already in MLT format (native MLT PMTiles).
pub(crate) fn is_mlt_tile(tile: &TileData) -> bool {
    tile.content_type == MLT_CONTENT_TYPE
}

/// Returns gzip-compressed MLT for a tile, transcoding from MVT on the first
/// request and caching the (transport-compressed) result per pod.
async fn transcoded_mlt(
    state: &AppState,
    routing_key: &ResourceRoutingKey,
    tile_id: u64,
    tile: TileData,
    cache_policy: TranscodeCachePolicy,
) -> Result<Bytes, HttpError> {
    if cache_policy == TranscodeCachePolicy::Bypass {
        return transcode_mlt_admitted(state.clone(), tile).await;
    }
    let key = (routing_key.clone(), tile_id);
    let cache = state.mlt_cache().clone();
    let state = state.clone();
    cache
        .try_get_with(key, transcode_mlt_admitted(state, tile))
        .await
        .map_err(|error: std::sync::Arc<HttpError>| (*error).clone())
}

async fn transcode_mlt_admitted(state: AppState, tile: TileData) -> Result<Bytes, HttpError> {
    // MLT encoding (FSST/FastPFOR plus gzip) is CPU-heavy. Keep it off Tokio
    // workers and under the shared CPU-work admission limit. A dropped request
    // cannot cancel blocking work, so the permit stays inside the closure.
    let permit = state.admit_cpu_work("mlt_transcode").await?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        transcode_mlt(tile)
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("MLT transcode task failed: {error}"),
        )
    })?
}

/// Upper bound on a single decompressed MVT tile. PMTiles can store tiles with
/// gzip, Brotli, or Zstd compression, so a corrupt or hostile archive could
/// otherwise expand a tiny tile into an arbitrarily large allocation before MLT
/// transcoding even runs. Real vector tiles are far below this; 32 MiB leaves
/// generous headroom while keeping per-request cost bounded under the CPU-work
/// gate.
const MAX_DECOMPRESSED_MVT_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug)]
enum MvtInputError {
    TooLarge { limit: usize },
    InvalidCompressed { encoding: &'static str },
    UnsupportedEncoding,
}

impl MvtInputError {
    fn into_http_error(self) -> HttpError {
        match self {
            Self::TooLarge { limit } => (
                StatusCode::BAD_GATEWAY,
                format!("MVT upstream payload exceeds {limit} decompressed bytes"),
            ),
            Self::InvalidCompressed { encoding } => (
                StatusCode::BAD_GATEWAY,
                format!("MVT upstream {encoding} payload is invalid"),
            ),
            Self::UnsupportedEncoding => (
                StatusCode::BAD_GATEWAY,
                "MVT upstream content encoding is unsupported".to_string(),
            ),
        }
    }
}

fn transcode_mlt(tile: TileData) -> Result<Bytes, HttpError> {
    // `mlt-core` needs the decompressed MVT; bound identity bytes and every
    // supported compression format before handing archive data to the parser.
    let raw_mvt = decode_mvt_input(
        tile.bytes,
        tile.content_encoding,
        MAX_DECOMPRESSED_MVT_BYTES,
    )
    .map_err(MvtInputError::into_http_error)?;
    let mlt = mvt_to_mlt(&raw_mvt).map_err(|_| {
        (
            StatusCode::BAD_GATEWAY,
            "MVT upstream payload cannot be transcoded".to_string(),
        )
    })?;
    Ok(Bytes::from(gzip(&mlt)?))
}

fn decode_mvt_input(
    data: Bytes,
    content_encoding: Option<&str>,
    max: usize,
) -> Result<Bytes, MvtInputError> {
    let decoded = match content_encoding {
        None | Some("identity") => {
            if data.len() > max {
                return Err(MvtInputError::TooLarge { limit: max });
            }
            return Ok(data);
        }
        Some("gzip") => decode_compressed(GzDecoder::new(data.as_ref()), max, "gzip")?,
        Some("br") => decode_compressed(
            brotli::Decompressor::new(data.as_ref(), 4096),
            max,
            "brotli",
        )?,
        Some("zstd") => {
            let decoder = zstd::stream::read::Decoder::new(data.as_ref())
                .map_err(|_| MvtInputError::InvalidCompressed { encoding: "zstd" })?;
            decode_compressed(decoder, max, "zstd")?
        }
        Some(_) => return Err(MvtInputError::UnsupportedEncoding),
    };
    Ok(Bytes::from(decoded))
}

/// Read at most one byte beyond `max` as an overflow sentinel. This bounds the
/// allocation even when the compressed stream advertises a hostile size.
fn decode_compressed(
    reader: impl Read,
    max: usize,
    encoding: &'static str,
) -> Result<Vec<u8>, MvtInputError> {
    let read_limit = max.saturating_add(1) as u64;
    let mut out = Vec::new();
    reader
        .take(read_limit)
        .read_to_end(&mut out)
        .map_err(|_| MvtInputError::InvalidCompressed { encoding })?;
    if out.len() > max {
        return Err(MvtInputError::TooLarge { limit: max });
    }
    Ok(out)
}

fn gzip(data: &[u8]) -> Result<Vec<u8>, HttpError> {
    let mut encoder = GzEncoder::new(Vec::new(), GzLevel::default());
    encoder
        .write_all(data)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("gzip: {error}")))?;
    encoder
        .finish()
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("gzip: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, header};
    use mlt_core::TileLayer;
    use mlt_core::geo_types::{Geometry, Point};

    fn gzip_test_data(data: &[u8]) -> Bytes {
        Bytes::from(gzip(data).unwrap())
    }

    fn brotli_test_data(data: &[u8]) -> Bytes {
        let mut compressed = Vec::new();
        brotli::CompressorReader::new(data, 4096, 5, 22)
            .read_to_end(&mut compressed)
            .expect("compress Brotli test data");
        Bytes::from(compressed)
    }

    fn zstd_test_data(data: &[u8]) -> Bytes {
        Bytes::from(zstd::stream::encode_all(data, 1).expect("compress Zstd test data"))
    }

    #[test]
    fn brotli_and_zstd_mvt_at_exact_limit_are_accepted() {
        let expected = Bytes::from(vec![0x5a; 1024]);
        for (encoding, compressed) in [
            ("br", brotli_test_data(&expected)),
            ("zstd", zstd_test_data(&expected)),
        ] {
            let decoded = decode_mvt_input(compressed, Some(encoding), expected.len())
                .expect("exact-limit compressed input should be accepted");
            assert_eq!(decoded, expected, "encoding {encoding}");
        }
    }

    #[test]
    fn brotli_and_zstd_mvt_at_limit_plus_one_are_rejected() {
        let data = vec![0; 1025];
        for (encoding, compressed) in [
            ("br", brotli_test_data(&data)),
            ("zstd", zstd_test_data(&data)),
        ] {
            let error = decode_mvt_input(compressed, Some(encoding), 1024)
                .expect_err("limit + 1 compressed expansion should be rejected");
            assert!(matches!(&error, MvtInputError::TooLarge { limit: 1024 }));
            assert_eq!(error.into_http_error().0, StatusCode::BAD_GATEWAY);
        }
    }

    #[test]
    fn gzip_mvt_at_exact_limit_is_accepted() {
        let expected = Bytes::from(vec![0x5a; 1024]);
        let decoded = decode_mvt_input(gzip_test_data(&expected), Some("gzip"), expected.len())
            .expect("exact-limit gzip input should be accepted");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn gzip_mvt_at_limit_plus_one_is_rejected() {
        let data = vec![0; 1025];
        let error = decode_mvt_input(gzip_test_data(&data), Some("gzip"), 1024)
            .expect_err("limit + 1 gzip expansion should be rejected");
        assert!(matches!(&error, MvtInputError::TooLarge { limit: 1024 }));
        assert_eq!(error.into_http_error().0, StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn truncated_gzip_mvt_is_rejected_as_upstream_payload_error() {
        let error = decode_mvt_input(Bytes::from_static(&[0x1f, 0x8b, 0x08]), Some("gzip"), 1024)
            .expect_err("truncated gzip should be rejected");
        assert!(matches!(
            &error,
            MvtInputError::InvalidCompressed { encoding: "gzip" }
        ));
        assert_eq!(error.into_http_error().0, StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn malformed_brotli_and_zstd_are_rejected_as_upstream_payload_errors() {
        for encoding in ["br", "zstd"] {
            let error = decode_mvt_input(
                Bytes::from_static(b"not a compressed stream"),
                Some(encoding),
                1024,
            )
            .expect_err("malformed compressed input should be rejected");
            assert_eq!(error.into_http_error().0, StatusCode::BAD_GATEWAY);
        }
    }

    #[test]
    fn unsupported_mvt_encoding_is_a_sanitized_upstream_error() {
        let error = decode_mvt_input(Bytes::new(), Some("compress"), 1024)
            .expect_err("unsupported encoding should be rejected");
        let (status, message) = error.into_http_error();
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(message, "MVT upstream content encoding is unsupported");
    }

    #[test]
    fn malformed_identity_mvt_is_a_sanitized_upstream_error() {
        let tile = TileData {
            bytes: Bytes::from_static(&[0xff]),
            content_type: "application/vnd.mapbox-vector-tile",
            content_encoding: None,
        };

        let (status, message) = transcode_mlt(tile).expect_err("malformed MVT should be rejected");

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(message, "MVT upstream payload cannot be transcoded");
    }

    #[test]
    fn identity_mvt_is_bounded_before_transcode() {
        let exact = Bytes::from(vec![0; 1024]);
        assert_eq!(
            decode_mvt_input(exact.clone(), None, exact.len()).unwrap(),
            exact
        );

        let error = decode_mvt_input(Bytes::from(vec![0; 1025]), None, 1024)
            .expect_err("limit + 1 identity input should be rejected");
        assert!(matches!(&error, MvtInputError::TooLarge { limit: 1024 }));
        assert_eq!(error.into_http_error().0, StatusCode::BAD_GATEWAY);
    }

    /// An empty MVT (zero-length protobuf message, no layers) transcodes to an
    /// empty MLT. Exercises the parse + encode path through `mlt-core`.
    #[test]
    fn empty_mvt_transcodes_to_empty_mlt() {
        assert!(mvt_to_mlt(&[]).unwrap().is_empty());
    }

    /// Builds a one-feature layer carrying small `I64`/`U64` property values and
    /// asserts narrowing collapses them to 32-bit columns (which decode as JS
    /// `Number`, not `BigInt`) while preserving the values.
    #[test]
    fn narrows_small_64bit_columns_to_32bit() {
        let mut b = TileLayer::builder("building", 4096).unwrap();
        let h = b.add_property("render_height", PropKind::I64).unwrap();
        let r = b.add_property("rank", PropKind::U64).unwrap();
        let mut f = b.feature(Geometry::Point(Point::new(1, 2)));
        f.id(Some(14_905_260_892)); // large OSM id: stays on the dedicated id path
        f.property(h, PropValue::I64(Some(26))).unwrap();
        f.property(r, PropValue::U64(Some(3))).unwrap();
        f.finish().unwrap();
        let layer = b.finish();

        assert!(has_wide_int_column(&layer));
        let narrowed = narrow_int_columns(&layer).unwrap();
        assert!(!has_wide_int_column(&narrowed));

        let props = narrowed.features()[0].properties();
        assert_eq!(props[0], PropValue::I32(Some(26)));
        assert_eq!(props[1], PropValue::U32(Some(3)));
        // The round-trip still encodes cleanly.
        assert!(
            !narrowed
                .encode(EncoderConfig::default())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn extension_selects_format_and_strips_y() {
        let h = HeaderMap::new();
        assert_eq!(
            negotiate_format("6451", &h),
            ("6451", RequestedTileFormat::AsStored)
        );
        assert_eq!(
            negotiate_format("6451.mvt", &h),
            ("6451", RequestedTileFormat::AsStored)
        );
        assert_eq!(
            negotiate_format("6451.pbf", &h),
            ("6451", RequestedTileFormat::AsStored)
        );
        // Raster terrain extensions are served as-stored.
        for suffix in ["webp", "jpg", "jpeg"] {
            assert_eq!(
                negotiate_format(&format!("6451.{suffix}"), &h),
                ("6451", RequestedTileFormat::AsStored)
            );
        }
        assert_eq!(
            negotiate_format("6451.mlt", &h),
            ("6451", RequestedTileFormat::Mlt)
        );

        let y_raw = String::from("6451.jpeg");
        let (y, _) = negotiate_format(&y_raw, &h);
        assert_eq!(y.as_ptr(), y_raw.as_ptr());
    }

    #[test]
    fn accept_header_selects_mlt_without_extension() {
        let plain = HeaderMap::new();
        assert_eq!(
            negotiate_format("6451", &plain).1,
            RequestedTileFormat::AsStored
        );

        let mut accept = HeaderMap::new();
        accept.insert(header::ACCEPT, HeaderValue::from_static(MLT_CONTENT_TYPE));
        assert_eq!(
            negotiate_format("6451", &accept).1,
            RequestedTileFormat::Mlt
        );
    }

    #[test]
    fn accept_negotiation_honors_exact_media_ranges_quality_and_all_fields() {
        for value in [
            "application/vnd.maplibre-tile;q=0",
            "application/vnd.maplibre-tile-extra",
            "text/plain; note=\"application/vnd.maplibre-tile\"",
            "application/vnd.maplibre-tile;q=invalid",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::ACCEPT, HeaderValue::from_str(value).unwrap());
            assert_eq!(
                negotiate_format("6451", &headers).1,
                RequestedTileFormat::AsStored,
                "{value} must not select MLT"
            );
        }

        let mut uppercase = HeaderMap::new();
        uppercase.insert(
            header::ACCEPT,
            HeaderValue::from_static("APPLICATION/VND.MAPLIBRE-TILE;Q=0.5"),
        );
        assert_eq!(
            negotiate_format("6451", &uppercase).1,
            RequestedTileFormat::Mlt
        );

        let mut repeated = HeaderMap::new();
        repeated.append(
            header::ACCEPT,
            HeaderValue::from_static("application/vnd.maplibre-tile;q=0"),
        );
        repeated.append(
            header::ACCEPT,
            HeaderValue::from_static("application/vnd.maplibre-tile;q=0.25"),
        );
        assert_eq!(
            negotiate_format("6451", &repeated).1,
            RequestedTileFormat::Mlt
        );
    }

    #[test]
    fn extension_wins_over_conflicting_accept() {
        // `.mlt` path is canonical even when Accept asks for protobuf.
        let mut accept_mvt = HeaderMap::new();
        accept_mvt.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/x-protobuf"),
        );
        assert_eq!(
            negotiate_format("6451.mlt", &accept_mvt).1,
            RequestedTileFormat::Mlt
        );
    }

    #[test]
    fn detects_native_mlt_tiles_by_content_type() {
        let mlt = TileData {
            bytes: Bytes::new(),
            content_type: MLT_CONTENT_TYPE,
            content_encoding: Some("gzip"),
        };
        assert!(is_mlt_tile(&mlt));

        let mvt = TileData {
            bytes: Bytes::new(),
            content_type: TileType::Mvt.content_type(),
            content_encoding: Some("gzip"),
        };
        assert!(!is_mlt_tile(&mvt));
    }
}
