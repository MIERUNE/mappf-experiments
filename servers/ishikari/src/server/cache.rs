//! Default `Cache-Control` policy for public provider responses.
//!
//! Generated resources use the policy below directly. Proxied style, glyph,
//! and sprite resources use it only when the upstream object has no explicit
//! policy. Internal (`/_internal/*`) responses carry provider policy in typed
//! metadata headers, not public caching headers.

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

/// Glyphs. A range is immutable by construction — `(fontstack, start-end)` fully
/// determines the bytes — so a long shared lifetime is safe.
pub(crate) const GLYPH: &str =
    "public, max-age=86400, s-maxage=604800, stale-while-revalidate=604800";

/// Sprites. Deliberately shorter than [`GLYPH`]: a sprite lives at a mutable
/// provider path, so republishing one under the same key would otherwise stay
/// stale in shared caches for a week. Raise this to the glyph lifetime only once
/// sprites are addressed by content hash (`specs/resource-layout-sketch.md` still
/// lists that as proposed).
pub(crate) const SPRITE: &str = "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400";
