# Ishikari Work Queue

This file contains only unresolved Ishikari-specific experiments and decisions. Durable contracts live in [`../specs/ishikari-spec.md`](../specs/ishikari-spec.md), and cross-cutting refactoring lives in [`../refactor.md`](../refactor.md). Delete completed items; git history is the archive.

## Active experiments

### Distributed cache evaluation

#### Entry-node L1 insertion policy

**Action:** Compare the current entry-node insertion policy with owner-only insertion across realistic node counts, cache capacities, request skew, and churn. Decide whether the tile cache is intentionally a replicated hot tier or part of owned aggregate capacity before changing production behavior.

**Evidence so far (2026-07-14):** In a 10-node, 159,584-request modeled run with 64 MiB of tile cache per node and the normal 512 MiB chunk cache, both policies made 1,526 backend fetches and read 1.93 GB; entry caching reduced peer requests from 143,788 to 122,533. With a deliberately constrained 1 MiB chunk cache, owner-only insertion reduced backend fetches from 26,571 to 15,889 and backend bytes from 33.86 GB to 19.62 GB, at the cost of more peer traffic.

Keep entry caching as the production default until production-sized capacity and churn sweeps justify a change. The simulator exposes both policies through `--peer-tile-cache`.

#### Group-zero index ownership

**Action:** Use per-node `ishikari_internal_resource_requests_total` for owner-side load and `ishikari_peer_fetch_total` for sender-side attempts, filtered to `resource="bootstrap"|"leaf"`, to determine whether group-zero ownership creates a material hotspot. Shard leaf ownership by byte-offset key only if concentration is significant.

**Evidence so far (2026-07-15):** A 3-node, 26,018-request real-resolver replay sent all 2 bootstrap and 117 leaf requests to one owner, as designed. Those 119 index requests were only 1.1% of the 10,873 internal tile requests, so this run does not justify sharding. The simulator report now includes per-node inbound and outbound counts; repeat with multi-tileset production traces before reconsidering.

#### Chunk merge window

**Action:** Benchmark the configurable merge window against isolated and viewport workloads, including the 0 ms no-delay baseline and 10 ms default. Compare end-user latency, backend operation count, fetched bytes, and waiter fan-in; prefer an adaptive rule only if it improves the measured Pareto frontier.

### Derived Terrain Products (experimental)

Contour and hillshade generation is an optional, bounded extension to the core
PMTiles delivery path. Keep it measurable and avoid letting it complicate the
stored-tile fast path. The current product and algorithm contract is documented
in `../specs/isoline-and-hillshade-spec.md`.

- Run the representation benchmark defined by the [evaluation criteria](../specs/isoline-and-hillshade-spec.md#evaluation-criteria) over representative fixtures and zooms, then use its byte, CPU, latency, complexity, and rendered-quality results to choose defaults.
- Verify the raster `color-relief` path in both the supported MapLibre GL JS
  version and Biei's concrete MapLibre Native build, including transparent
  neutral stops, texture filtering, and overzoom behavior.
- Constrain shared-arc simplification before increasing its tolerance. Candidate
  replacements must not introduce intersections or self-intersections, reverse
  ring/face orientation, or collapse narrow shade faces. Add focused fixtures
  for close parallel bands and junction-heavy terrain.
- Evaluate request-coalesced metatile generation only if that benchmark shows a material geometry-fragmentation or CPU benefit; preserve the specification's bounded-overcompute and shared-topology constraints.

### Simulator

The implemented model and its fidelity boundaries are documented in
[`../specs/ishikari-sim-spec.md`](../specs/ishikari-sim-spec.md).

- Run controlled cold-cluster `replay-http` calibrations for direct-node and
  Gateway targets, compare their Prometheus deltas with the in-process simulator,
  and record the measured hit-rate/backend-GET error against the acceptance
  bounds in [`../specs/ishikari-sim-spec.md`](../specs/ishikari-sim-spec.md).
- Make calibration inputs reproducible: document the source/version and
  acquisition steps for the population and PMTiles fixtures, and retain the
  small fitted latency-profile/provenance JSON used by published measurements.
- Model terrain generation and the shared CPU-admission queue in Phase 2.
- Add gossip packet-loss or partition injection only after selecting measured
  failure inputs.
- Confirm whether the production Gateway balances HTTP/2 traffic per request or per connection before changing the simulator's `entry_affinity` default.
- Add multi-tileset traces only when an experiment needs to exercise per-tileset coordinator and cache competition.
- Add a wall-clock interpretation of churn recovery only if a communication use case defines a defensible request-rate assumption.

Routine contract-test and smoke-test maintenance is not tracked as open work here; the tests and service README define their current coverage.

## Evidence-gated follow-ups

- Add a style catalog admin/update endpoint only if dynamic style registration
  becomes necessary.

- Evaluate framed internal APIs and per-hop/end-to-end timeout budgets if the
  current HTTP contract proves insufficient.

- Measure dead-node state growth under Spot churn before shortening membership
  retention beyond its current failure-detection grace period.
- Persist a monotonic membership incarnation only if wall-clock rollback becomes
  an operational concern.
- Add cold/warm latency checks when a named performance-tuning effort needs them.
- Add a local multi-node dev-cluster script only if single-process tests stop
  catching cluster regressions.

## Unresolved product decisions

- How should provider style/version invalidation work before content-addressed IDs exist?
- Should Ishikari proxy external style assets, or require assets to be mirrored into the configured data backend?
- If fixed per-hop timeouts prove insufficient, what end-to-end budget should bound peer-forwarded fetches, and should it vary by resource kind?
