use axum::http::{HeaderMap, StatusCode};
use ishikari_core::{
    interned::{ResourceRoutingKey, TilesetId},
    pmtiles::{TileCoord, TileId},
};

use crate::server::{AppState, HttpError};

use super::super::mlt::{RequestedTileFormat, negotiate_format};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum DerivedProduct {
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
    pub(super) fn parse(value: &str) -> Result<Self, HttpError> {
        match value {
            "contours" => Ok(Self::Contours),
            "hillshade" => Ok(Self::Hillshade),
            "hillshade-raster" => Ok(Self::HillshadeRaster),
            "hillshade-webp-lossy" => Ok(Self::HillshadeWebpLossy),
            "hillshade-jpeg" => Ok(Self::HillshadeJpeg),
            _ => Err((StatusCode::NOT_FOUND, "derived product not found".into())),
        }
    }

    pub(super) fn path(self) -> &'static str {
        match self {
            Self::Contours => "contours",
            Self::Hillshade => "hillshade",
            Self::HillshadeRaster => "hillshade-raster",
            Self::HillshadeWebpLossy => "hillshade-webp-lossy",
            Self::HillshadeJpeg => "hillshade-jpeg",
        }
    }

    pub(super) fn is_raster(self) -> bool {
        matches!(
            self,
            Self::HillshadeRaster | Self::HillshadeWebpLossy | Self::HillshadeJpeg
        )
    }

    pub(super) fn layer(self) -> &'static str {
        self.path()
    }
}

pub(super) struct DerivedTileRequest {
    pub(super) tileset_id: TilesetId,
    pub(super) product: DerivedProduct,
    pub(super) z: u8,
    pub(super) x: u32,
    pub(super) y: u32,
    pub(super) tile_id: u64,
    pub(super) format: RequestedTileFormat,
}

pub(super) fn parse_derived_tile_request(
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
pub(super) fn derived_resource_key(
    tileset_id: &TilesetId,
    product: DerivedProduct,
) -> ResourceRoutingKey {
    ResourceRoutingKey::for_derived_resource(product.path(), tileset_id)
        .expect("derived product names are valid routing-key segments")
}

pub(super) fn validated_mapterhorn(
    state: &AppState,
    value: String,
) -> Result<TilesetId, HttpError> {
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
    fn derived_resource_keys_separate_products() {
        let source = TilesetId::try_new("mapterhorn/planet").unwrap();
        assert_ne!(
            derived_resource_key(&source, DerivedProduct::Contours),
            derived_resource_key(&source, DerivedProduct::Hillshade)
        );
    }
}
