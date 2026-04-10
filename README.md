# Ishikari

A distributed PMTiles cache proxy for efficient, low-cost, large-scale serving from object storage.

> [!WARNING]
> This is an experimental, proof-of-concept project. The behavior, API, and configuration are not stable.

Ishikari focuses on large-scale PMTiles serving workloads:

- **Backend request batching** - reduces object storage requests, traffic, and latency.
- **Distributed cache** - uses gossip membership, locality-aware routing, and caching tuned for Hilbert-sorted PMTiles archives.

LICENSE: MIT OR Apache-2.0


## Demo

```bash
# Serve from a local PMTiles file with an artificial backend delay.
mkdir data
pmtiles extract https://build.protomaps.com/20260206.pmtiles --bbox=122,24,155,46 data/japan.pmtiles
ISKR_ARTIFICIAL_BACKEND_DELAY_MS=50 bash demo.sh
open http://localhost:8080/tilesets/japan/preview
```

```bash
# Serve from a remote HTTP server (slow).
ISKR_TILESET_SOURCES=https://demo-bucket.protomaps.com/ bash demo.sh
open http://localhost:8080/tilesets/v4/preview
```

## Style, glyph, and sprite proxy

Ishikari can proxy MapLibre style JSON, glyph PBFs, and sprite assets from upstream templates:

```bash
ISKR_STYLE_TEMPLATES='carto=https://basemaps.cartocdn.com/{style_id}/style.json;default=https://styles.example/{style_id}/style.json' \
ISKR_GLYPH_URL_TEMPLATE='https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf' \
ISKR_SPRITE_TEMPLATES='carto=https://basemaps.cartocdn.com/{style_id}/sprite' \
cargo run -- --tileset-sources data
```

The style endpoint rewrites provider-relative `/{tileset_key}` sources to
Ishikari TileJSON URLs and points `glyphs` and `sprite` back to Ishikari.
Style, glyph, and sprite upstream fetches use bounded in-process caching and
single-flight coordination to absorb cold concurrent renders.

`ISKR_TILESET_SOURCES` (the PMTiles tile source) accepts the same `namespace=url;…;default=url`
form, so tilesets can be backed by multiple object-store roots. A namespaced key
is served from the matching root with the namespace stripped
(`regional/streets` → `{regional-root}/streets.pmtiles`); any other key falls to
the default root with its full path (`analysis/hrnowc` →
`{default-root}/analysis/hrnowc.pmtiles`). A single bare `ISKR_TILESET_SOURCES` stays the
default root.
