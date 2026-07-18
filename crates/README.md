# Library crates

- `biei-core`: renderer, scheduling, membership, and Biei HTTP runtime
- `ishikari-core`: PMTiles/resource serving, caching, membership, and routing
- `mmpf-terrain`: MIERUNE Map Platform terrain decoding and derived products

Executables live in `servers/` and `sims/`; dependency direction is always from
those binaries into these libraries. Cross-service primitives may be extracted
later, but similarly named policy is not treated as equivalent by default.
