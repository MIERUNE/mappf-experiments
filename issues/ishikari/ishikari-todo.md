# Ishikari TODO

System positioning, non-goals, guardrails, and refactor direction are documented
in `../specs/ishikari-spec.md`.

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
- Use per-node `ishikari_internal_resource_requests_total` for owner-side load
  and `ishikari_peer_fetch_total` for sender-side attempts, filtered to
  `resource="bootstrap"|"leaf"`, to measure whether group-zero ownership
  creates a material hotspot. Shard leaf ownership by byte-offset key only if
  concentration is significant.

  Initial real-resolver result (2026-07-15): a 3-node, 26,018-request replay
  sent all 2 bootstrap and 117 leaf requests to one owner, as designed. Those
  119 index requests were only 1.1% of the 10,873 internal tile requests, so
  the measured concentration does not justify sharding leaf ownership. The
  simulator report now includes per-node inbound and outbound counts; repeat
  with multi-tileset production traces before reconsidering.
- Benchmark the configurable chunk merge window against isolated and viewport
  workloads, including the 0 ms no-delay baseline and 10 ms default. Compare
  end-user latency, backend operation count, fetched bytes, and waiter fan-in;
  prefer an adaptive rule only if it improves the measured Pareto frontier.

### Derived Terrain Products (experimental)

Contour and hillshade generation is an optional, bounded extension to the core
PMTiles delivery path. Keep it measurable and avoid letting it complicate the
stored-tile fast path. The current product and algorithm contract is documented
in `../specs/isoline-and-hillshade-spec.md`.

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

### Simulator

The implemented model and its fidelity boundaries are documented in
`../specs/simulator-spec.md`.


- Run controlled cold-cluster `replay-http` calibrations for direct-node and
  Gateway targets, compare their Prometheus deltas with the in-process simulator,
  and record the measured hit-rate/backend-GET error against the acceptance
  bounds in `../specs/simulator-spec.md`.
- Model terrain generation and the shared CPU-admission queue in Phase 2.

- Add gossip packet-loss or partition injection only after selecting measured
  failure inputs.

### Demo and Acceptance Checks

- Keep Gateway-routed smoke checks current for TileJSON, tile bytes, style JSON,
  glyphs, sprites, health, and internal-path non-exposure.
- Keep Biei as one consumer smoke, not as an Ishikari-specific API contract.
- Add cold/warm latency checks when performance tuning resumes.
- Add a local multi-node dev-cluster script only if single-process tests stop
  catching cluster regressions.
- Keep the router-level HTTP contract tests (`server/contract_tests.rs`) current.
  They cover stored MVT and negotiated MLT tile responses through a generated
  single-tile PMTiles fixture; namespaced styles; provider cache metadata (public
  `Cache-Control` / `Age`, default and upstream-derived, repeated field lines,
  compressed style bodies, and internal `x-ishikari-provider-*` headers); glyph
  and sprite defaults; client conditional requests including derived TileJSON
  ETags; conditional origin revalidation that extends stale provider entries on
  `304`; and public-router internal-path non-exposure over a real local HTTP
  upstream.

## Optional Hardening


- Add a style catalog admin/update endpoint only if dynamic style registration
  becomes necessary.
- Define an explicit cache-invalidation contract before supporting mutable or
  unversioned PMTiles archives; then revisit tile negative-cache TTLs.
- Evaluate framed internal APIs, per-hop/end-to-end timeout budgets, and
  OpenTelemetry only if measurements show the current HTTP + request-id +
  Prometheus model is insufficient.
- Measure dead-node state growth under Spot churn before shortening membership
  retention beyond its current failure-detection grace period.
- Persist a monotonic membership incarnation only if wall-clock rollback becomes
  an operational concern.

## Open Questions

- How should style/version invalidation work before content-addressed IDs exist?
- Should Ishikari proxy external style assets, or require assets to be mirrored into the configured data backend?
- Which default cache TTLs are acceptable for mutable MIERUNE deployments?
- If fixed per-hop timeouts are insufficient, what internal end-to-end timeout budget should bound peer-forwarded fetches, and should it vary by resource kind?
