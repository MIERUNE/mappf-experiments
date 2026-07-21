use ishikari_core::{interned::TilesetId, storage::TilesetInfo};
use serde::Deserialize;
use serde_json::{Value, json};

use super::product::DerivedProduct;

#[derive(Debug, Deserialize)]
pub(crate) struct DerivedTileJsonQuery {
    encoding: Option<String>,
}

impl DerivedTileJsonQuery {
    pub(super) fn wants_mlt(&self) -> bool {
        self.encoding
            .as_deref()
            .is_some_and(|encoding| encoding.eq_ignore_ascii_case("mlt"))
    }
}

/// Converts source PMTiles metadata and derived-product rules into TileJSON.
pub(super) fn derived_tilejson(
    tileset_id: &TilesetId,
    product: DerivedProduct,
    base_url: &str,
    info: &TilesetInfo,
    maxzoom: u8,
    wants_mlt: bool,
) -> Value {
    let (suffix, format) = match product {
        DerivedProduct::Contours | DerivedProduct::Hillshade if wants_mlt => (".mlt", "pbf"),
        DerivedProduct::Contours | DerivedProduct::Hillshade => (".mvt", "pbf"),
        DerivedProduct::HillshadeRaster | DerivedProduct::HillshadeWebpLossy => (".webp", "webp"),
        DerivedProduct::HillshadeJpeg => (".jpg", "jpg"),
    };

    let mut document = json!({
        "tilejson": "3.0.0",
        "tiles": [format!(
            "{base_url}/tilesets/{tileset_id}/derived/{}/{{z}}/{{x}}/{{y}}{suffix}",
            product.path(),
        )],
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
        "format": format,
    });

    if !product.is_raster() {
        let fields = match product {
            DerivedProduct::Contours => json!({ "ele": "Number", "level": "Number" }),
            DerivedProduct::Hillshade => json!({ "class": "String", "level": "Number" }),
            DerivedProduct::HillshadeRaster
            | DerivedProduct::HillshadeWebpLossy
            | DerivedProduct::HillshadeJpeg => unreachable!("raster product handled above"),
        };
        let object = document
            .as_object_mut()
            .expect("derived TileJSON document is an object");
        object.insert(
            "vector_layers".to_string(),
            json!([{
                "id": product.layer(),
                "fields": fields,
                "minzoom": info.header.min_zoom,
                "maxzoom": maxzoom
            }]),
        );
        object.insert(
            "encoding".to_string(),
            json!(if wants_mlt { "mlt" } else { "mvt" }),
        );
    }

    document
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{BufMut, BytesMut};
    use ishikari_core::{
        interned::TilesetId,
        pmtiles::{Header, Metadata},
        storage::TilesetInfo,
    };

    use super::*;

    fn info() -> TilesetInfo {
        let mut bytes = BytesMut::with_capacity(127);
        bytes.extend_from_slice(b"PMTiles");
        bytes.put_u8(3);
        for _ in 0..11 {
            bytes.put_u64_le(0);
        }
        bytes.put_u8(1);
        bytes.put_u8(1);
        bytes.put_u8(1);
        bytes.put_u8(1);
        bytes.put_u8(0);
        bytes.put_u8(14);
        for _ in 0..4 {
            bytes.put_i32_le(0);
        }
        bytes.put_u8(0);
        bytes.put_i32_le(0);
        bytes.put_i32_le(0);
        TilesetInfo {
            header: Header::parse(bytes.freeze()).expect("test header parses"),
            metadata: Arc::new(Metadata::default()),
        }
    }

    #[test]
    fn vector_tilejson_retains_vector_contract_and_requested_encoding() {
        let tileset_id = TilesetId::try_new("terrain/planet").unwrap();
        let document = derived_tilejson(
            &tileset_id,
            DerivedProduct::Contours,
            "https://example.test",
            &info(),
            16,
            true,
        );

        assert_eq!(document["format"], "pbf");
        assert_eq!(document["encoding"], "mlt");
        assert!(document.get("vector_layers").is_some());
        assert_eq!(
            document["tiles"][0],
            "https://example.test/tilesets/terrain/planet/derived/contours/{z}/{x}/{y}.mlt"
        );
    }

    #[test]
    fn raster_tilejson_ignores_mlt_and_omits_vector_fields() {
        let tileset_id = TilesetId::try_new("terrain/planet").unwrap();
        for (product, suffix, format) in [
            (DerivedProduct::HillshadeRaster, ".webp", "webp"),
            (DerivedProduct::HillshadeWebpLossy, ".webp", "webp"),
            (DerivedProduct::HillshadeJpeg, ".jpg", "jpg"),
        ] {
            let document = derived_tilejson(
                &tileset_id,
                product,
                "https://example.test",
                &info(),
                16,
                true,
            );

            assert_eq!(document["format"], format);
            assert!(document.get("encoding").is_none());
            assert!(document.get("vector_layers").is_none());
            assert!(
                document["tiles"][0]
                    .as_str()
                    .expect("tile URL is a string")
                    .ends_with(&format!("/{{z}}/{{x}}/{{y}}{suffix}"))
            );
        }
    }
}
