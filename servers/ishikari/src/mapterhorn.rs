//! Validated Mapterhorn composite-tileset configuration.

use std::time::Duration;

use ishikari_core::interned::TilesetId;

/// Highest zoom served from the base archive; higher zooms use detail archives.
pub(crate) const BASE_MAX_ZOOM: u8 = 12;
/// Upper bound for an advertised max zoom (PMTiles tops out at 31; TileJSON
/// consumers never need more for terrain).
const MAX_ADVERTISED_ZOOM: u8 = 30;

/// Validated inputs needed to construct a Mapterhorn resolver.
pub(crate) struct MapterhornConfig {
    tileset: TilesetId,
    namespace: TilesetId,
    maxzoom: u8,
    negative_ttl: Duration,
}

impl MapterhornConfig {
    pub(crate) fn new(tileset: &str, maxzoom: u8, negative_ttl: Duration) -> Result<Self, String> {
        if !(BASE_MAX_ZOOM + 1..=MAX_ADVERTISED_ZOOM).contains(&maxzoom) {
            return Err(format!(
                "mapterhorn maxzoom must be {}..={} (got {maxzoom}): it advertises the detail \
                 archives' zoom, so <= {BASE_MAX_ZOOM} would never request detail tiles",
                BASE_MAX_ZOOM + 1,
                MAX_ADVERTISED_ZOOM
            ));
        }
        let tileset = TilesetId::try_new(tileset).map_err(|error| error.to_string())?;
        let namespace = tileset.namespace().unwrap_or_else(|| tileset.local_id());
        let namespace = TilesetId::try_new(namespace).map_err(|error| error.to_string())?;
        Ok(Self {
            tileset,
            namespace,
            maxzoom,
            negative_ttl,
        })
    }

    pub(crate) fn into_parts(self) -> (TilesetId, TilesetId, u8, Duration) {
        (
            self.tileset,
            self.namespace,
            self.maxzoom,
            self.negative_ttl,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::MapterhornConfig;

    #[test]
    fn maxzoom_must_be_a_detail_zoom() {
        assert!(MapterhornConfig::new("mapterhorn/planet", 12, Duration::from_secs(1)).is_err());
        assert!(MapterhornConfig::new("mapterhorn/planet", 13, Duration::from_secs(1)).is_ok());
        assert!(MapterhornConfig::new("mapterhorn/planet", 31, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn tileset_key_must_be_valid() {
        assert!(MapterhornConfig::new("", 16, Duration::from_secs(1)).is_err());
        assert!(MapterhornConfig::new("mapterhorn/planet", 16, Duration::from_secs(1)).is_ok());
    }
}
