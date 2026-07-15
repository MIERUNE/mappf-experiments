# Ishikari TODO

## Positioning

Ishikari's primary purpose is efficient, low-cost delivery of PMTiles archives
stored in object storage. Its core product is PMTiles-backed TileJSON and tile
bytes over ordinary HTTP, with distributed cache locality and backend range-read
batching.

Style JSON, glyphs, sprites, preview pages, and renderer integration are
supporting provider features. Biei is the primary demo consumer, but Ishikari
must remain a standalone provider and must not grow renderer-specific routing or
worker concepts.

## Done (design decisions recorded)

### Composite PMTiles / Mapterhorn Detail Archives — IMPLEMENTED

Shipped as `server/tileset/mapterhorn.rs` (`MapterhornResolver`) + the
`resolve_archive` hook in the tile handler. The public contract is unchanged:
the composite tileset looks like an ordinary Ishikari tileset key.

Decisions as implemented (the original design sketch lives in git history;
where they differ, the implementation is the source of truth):

- Resolution: `z <= 12` (`BASE_MAX_ZOOM`) serves the base archive; `z > 12`
  rewrites onto the z6-ancestor detail archive key (`6-{x6}-{y6}`,
  `DETAIL_ANCESTOR_ZOOM = 6`). The rewrite happens before routing, so chunk
  cache / peer routing / drain / request IDs all apply unchanged — a detail
  archive is just another tileset key.
- **Fallback policy decided: no fallback.** A z>12 request whose detail
  archive is absent returns a plain 404 (`Resolved::Absent`); no server-side
  overzoom from the z12 parent. (Resolves the open question below.)
- **No manifest.** Instead of `download_urls.json`, detail-archive presence is
  probed on first use (a header read), single-flighted, and cached per z6 tile
  — present and absent alike. Transient object-store failures are not
  negative-cached (`detail_error` path).
- Config (env prefix `ISKR_`): `ISKR_MAPTERHORN_TILESET` (logical key, e.g.
  `mapterhorn/planet`; feature off when unset), `ISKR_MAPTERHORN_MAXZOOM`
  (required when the tileset is set; advertised detail max zoom), and the
  negative-cache TTL flag.
- Metrics: `base` / `detail` / `detail_negative` / `detail_error` serving
  paths (no `fallback` label — there is no fallback).

### MLT output — IMPLEMENTED (record of a feature the original TODO predates)

Ishikari can transcode stored MVT tiles to MLT (`server/tileset/mlt.rs`,
`mvt_to_mlt`) at serve time:

- Negotiation: `.mlt` path extension (canonical, CDN-safe) or
  `Accept: application/vnd.maplibre-tile` (Martin-compatible). Default remains
  the as-stored representation.
- Caching and execution: transcodes are single-flighted into a per-pod,
  byte-weighted moka cache (64 MiB). Encoding runs on the blocking pool behind
  the shared `ISKR_CPU_WORK_CONCURRENCY` limit, so MLT and derived-terrain work
  cannot collectively block Tokio workers or grow without bound. Transcodes are
  **not** shared across peers — only source bytes
  travel on the internal port. Revisit only if MLT traffic dominates.
- Preview: vector tilesets get an MVT/MLT toggle; the preview page pins a
  MapLibre GL JS 6.x prerelease because 5.x ships an MLT decoder too old for
  ishikari's output.
- Positioning note: transcoding spends CPU on the serving path, which is a
  deliberate, bounded exception to "ishikari ships stored bytes cheaply". The
  boundary stays: no rendering, no style-aware processing.

## Active Work

### Distributed Cache Evaluation

- Use `ishikari-sim` to compare the current entry-node L1 insertion policy with
  owner-only insertion across realistic node counts, cache capacities, request
  skew, and churn. Decide whether the tile cache is intentionally a replicated
  hot tier or part of the owned aggregate capacity before changing production
  behavior.

  Initial modeled result (2026-07-14): 10 nodes, 159,584 requests, and 64 MiB
  tile cache per node. With the normal 512 MiB chunk cache, both policies made
  1,526 backend fetches and read 1.93 GB; entry caching reduced peer requests
  from 143,788 to 122,533. With a deliberately constrained 1 MiB chunk cache,
  owner-only reduced backend fetches from 26,571 to 15,889 and backend bytes
  from 33.86 GB to 19.62 GB, at the cost of more peer traffic. This confirms the
  policy depends on the tile/chunk cache ratio. Keep entry caching as the
  production default until production-sized capacity and churn sweeps justify a
  change. The simulator exposes both through `--peer-tile-cache`.
- Measure whether routing all bootstrap and leaf-directory requests through the
  tileset's group-zero owner creates a material index hotspot. Add per-node
  internal index-fetch visibility first; shard leaf ownership by byte-offset key
  only if concentration is significant.
- Measure identical peer-request fan-in before adding entry-side tile
  single-flight. The backend chunk coordinator already prevents duplicated
  object-store reads, so the remaining benefit is internal HTTP/decode/copy
  reduction.
- Benchmark the fixed 10 ms chunk merge window against isolated and viewport
  workloads. Compare end-user latency, backend operation count, fetched bytes,
  and waiter fan-in; prefer an adaptive rule only if it improves the measured
  Pareto frontier.

### Derived Terrain Products (experimental)

Contour and hillshade generation is an optional, bounded extension to the core
PMTiles delivery path. Keep it measurable and avoid letting it complicate the
stored-tile fast path. The current product and algorithm contract is documented
in `isoline-and-hillshade-spec.md`.

- Build a repeatable Pareto benchmark over representative terrain fixtures and
  zooms. Compare vector MVT/MLT, quantized lossless WebP, and continuous lossy
  raster rendered through `color-relief` against MapLibre's raster hillshade.
  Record compressed bytes, feature/ring/vertex counts, generation and decode
  time, render time, SSIM or equivalent structural error, and perceptual color
  error (OKLab Delta E). Use the results to choose defaults rather than treating
  the current tone count or representation as final.
- Verify the raster `color-relief` path in both the supported MapLibre GL JS
  version and Biei's concrete MapLibre Native build, including transparent
  neutral stops, texture filtering, and overzoom behavior.
- Constrain shared-arc simplification before increasing its tolerance. Candidate
  replacements must not introduce intersections or self-intersections, reverse
  ring/face orientation, or collapse narrow shade faces. Add focused fixtures
  for close parallel bands and junction-heavy terrain.
- Consider request-coalesced 2x2/4x4 metatile generation only if benchmarks show
  a material geometry-fragmentation or CPU benefit. The current one-cell halo is
  sufficient for the one-cell speckle rule; always generating a 4x4 metatile for
  one requested tile would overcompute 15 outputs. A metatile implementation
  should batch nearby cold requests, build shared topology once, split the
  children, and populate the existing derived cache in one pass.
- Consider HRW ownership or peer sharing for generated outputs only if production
  metrics show significant cross-replica duplicate generation. Derived caches
  are intentionally pod-local today.

### Observability

- Add provider resource cache metrics by kind/outcome: `hit`, `miss`, `insert`, `negative`, and `singleflight_join`.
- Add request-duration histograms if production debugging needs latency breakdowns beyond Gateway/Cloud Monitoring.
- Add peer-fetch counters if peer routing or failover becomes hard to debug.
- Keep chunk-fetch metrics focused on object-store tuning: merge-window delay, pending chunks at dispatch, fetched byte size, fetched chunk count, waiter fan-in, and backend duration.

### Demo and Acceptance Checks

- Keep Gateway-routed smoke checks current for TileJSON, tile bytes, style JSON, glyphs, sprites, health, and internal-path non-exposure.
- Keep Biei as one consumer smoke, not as an Ishikari-specific API contract.
- Add cold/warm latency checks when performance tuning resumes.
- Add a local multi-node dev-cluster script only if single-process tests stop catching cluster regressions.
- Add focused HTTP contract tests for style namespace resolution, style rewrite, glyph validation, sprite suffix handling, MVT/MLT negotiation, and Gateway path exposure.

### Optional Hardening

- Add ETag / Last-Modified support for style, glyph, sprite, and TileJSON where upstream or object metadata is available.
- Add a style catalog admin/update endpoint only if dynamic style registration becomes necessary.
- Revisit tile negative-cache TTL if Ishikari supports mutable or unversioned PMTiles datasets.
- Evaluate framed internal APIs, per-hop/end-to-end timeout budgets, and OpenTelemetry only if measurements show the current HTTP + request-id + Prometheus model is insufficient.

## Open Questions

- How should style/version invalidation work before content-addressed IDs exist?
- Should Ishikari proxy external style assets, or require assets to be mirrored into the configured data backend?
- Which default cache TTLs are acceptable for mutable MIERUNE deployments?
- If fixed per-hop timeouts are insufficient, what internal end-to-end timeout budget should bound peer-forwarded fetches, and should it vary by resource kind?

Resolved:

- ~~Missing z13+ mapterhorn detail: overzoom or no fallback?~~ → **No fallback;
  404** (see "Done" above).
- ~~Should `/readyz` remain public-path compatible?~~ → **Yes, and decided
  structurally (2026-06):** the server runs two listeners. The public listener
  (`:8080`, Gateway-fronted) serves content plus top-level `/livez` `/readyz`
  (k8s convention; kubelet probes and the Gateway HealthCheckPolicy use these).
  All other operational endpoints (`/_internal/metrics`, `/_internal/cluster`,
  peer tile/pmtiles forwarding, health aliases) live only on the
  cluster-internal listener (`INTERNAL_PORT`, default `9090`), which is never
  exposed via a Service or the Gateway.

## Non-Goals and Guardrails

- Do not move Biei render routing into Ishikari.
- Do not make Ishikari aware of Biei worker slots, BL, render permits, or render output cache.
- Do not require Biei or other consumers to understand PMTiles archives directly.
- Do not create a shared cluster crate until repeated cross-project reuse proves it is worth the abstraction cost.
- Do not put attacker-controlled `style_id` or `tileset_id` values in metric labels.
- Keep `/_internal/*` and `/metrics` on the cluster-internal listener only (`INTERNAL_PORT`); never route the internal port through a Service, Gateway, or Ingress, and keep the public listener returning 404 for those paths.
- Keep the headless gossip Service gossip-only; do not publish public HTTP `8080` there.
- Keep PMTiles parsing in `pmtiles/`, byte access and peer routing in `storage/`, and style/glyph/sprite provider logic outside PMTiles archive parsing.

## Refactor Direction

Do not move files only for aesthetics. Move modules when a new responsibility
needs a clearer boundary.

Likely future splits:

- split `server` into `http` when response shaping, request IDs, metrics, and resource families make the current module too broad;
- split provider-resource fetch/cache/single-flight code if more metrics or resource kinds make `upstream.rs` hard to reason about;
- add traits for peer transport and membership only at the test seam needed for an in-process cluster harness.
