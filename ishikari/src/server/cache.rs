//! Centralized `Cache-Control` policy for public provider responses.
//!
//! Public provider responses are CDN/browser-cacheable according to the
//! resource type below. Internal (`/_internal/*`) and node-to-node responses
//! intentionally carry no public caching headers.

/// Tile payloads. Keep browser reuse moderate, but let shared caches absorb
/// object-storage traffic for longer.
pub(crate) const TILE: &str = "public, max-age=3600, s-maxage=86400, stale-while-revalidate=604800";

/// TileJSON documents derived from PMTiles metadata.
pub(crate) const TILEJSON: &str =
    "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400";

/// MapLibre style JSON documents.
pub(crate) const STYLE: &str = "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400";

/// Development-facing preview HTML and generated preview style JSON.
pub(crate) const PREVIEW: &str = "public, max-age=300";

/// Glyphs and sprites. Safe to bump for versioned font/sprite assets.
pub(crate) const GLYPH_SPRITE: &str =
    "public, max-age=86400, s-maxage=604800, stale-while-revalidate=604800";
