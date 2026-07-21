# Library crates

- `biei-core`: render scheduling, node orchestration, cluster routing, and the cost model — the shared logic driven by both the Biei server and its simulator
- `ishikari-core`: PMTiles resource resolution, tile/chunk caching, HRW routing, and membership — shared by the Ishikari server and its simulator
- `mmpf-cluster`: shared one-node Chitchat lifecycle and state inspection
- `mmpf-common`: small service-independent configuration and runtime primitives
- `mmpf-http`: shared HTTP request-correlation, header syntax, and operational paths
- `mmpf-mln-filesource`: bounded HTTP, cache, retry, and SSRF-safe MapLibre Native FileSource
- `mmpf-pmtiles`: customizable PMTiles v3 decoding and single-archive range reader
- `mmpf-terrain`: MMPF terrain decoding and derived products

Executables live in `servers/` and `sims/`; dependency direction is always from
those binaries into these libraries. Server crates own the CLI, logging setup,
OS signal handling, and the HTTP serving / rendering runtime; the `-core`
libraries hold the shared domain logic (routing, caching, scheduling, node
orchestration) that both a service's server and its simulator drive.

Cross-service primitives belong in `mmpf-common` only when their meaning and behavior are intentionally shared; similarly named policy is not treated as equivalent by default.
