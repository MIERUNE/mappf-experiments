# Ishikari Simulator Spec

Status: **current simulator contract and fidelity record.** Descriptive implementation status belongs here only when it defines the model being evaluated; unresolved implementation work is tracked in [`../issues/ishikari-todo.md`](../issues/ishikari-todo.md).

A specification and current-status record for a deterministic, trace-driven
simulator that answers quantitative
questions about Ishikari's distributed cache behavior — hit rates, backend
egress, node-churn recovery, chunk batching efficiency, and load skew — under a
workload that can be weighted by an operator-supplied population dataset.

The simulator's primary purpose is to approximate a real deployment **without
provisioning the corresponding nodes, memory, object-store traffic, or
wall-clock runtime**. Exact reproduction is impossible; the goal is to account
for every important resource and decision with the cheapest faithful model:

- reuse production policy and algorithms where their behavior matters;
- represent cache capacity and payload size as logical weights when real bytes
  are unnecessary;
- advance queueing, network delay, and user think time on a virtual clock;
- calibrate stochastic cost models from cloud measurements; and
- use real-cluster tests only to fit and validate the model, not as the normal
  way to answer capacity questions.

This follows the biei-sim design principle: production policy should stay in
the main crate and be consumed through narrow seams. Real-cache mode executes
the production resolver directly. Modeled-cache mode instead implements a
metadata-only request flow while reusing production HRW routing, PMTiles access
planning, chunk-range planning, Moka eviction policies, and byte weights. That
approximation is calibrated against real-cache runs; it must not be described
as identical production execution. Any behavior that is neither executed from
production code nor represented by a measured model is an explicit limitation,
not an implicit zero-cost operation.

---

## 1. Motivation — questions the simulator must answer

Each scenario in §6 maps back to one of these. All are cheap to answer with a
simulator ("sweep 100 configs in a minute") and expensive to answer with a real
cluster ("one config per load-test run").

- **Q1 — Group size / candidate count trade-off.** How do
  `ISKR_ROUTER_TILE_GROUP_SIZE` (default 512) and `ISKR_ROUTER_TOP_K`
  (default 3) shape hit rate, peer-forward rate, and per-node load skew?
  Note: candidates beyond the first only matter on failure/drain, so the
  expected answer for load spread is "barely" — quantify it.
- **Q2 — Churn miss tsunami.** When a node joins or leaves (spot preemption,
  rolling deploy), what fraction of cached groups gets re-routed, how deep is
  the hit-rate dip, and how many requests does recovery take? The demo runs on
  Spot nodes, so this is a real production question.
- **Q3 — Cache sizing curves.** Hit rate and backend egress as a function of
  `tile_cache_max_bytes` × `chunk_cache_max_bytes` × node count × workload
  concentration. Produces the memory-sizing rationale we currently don't have.
- **Q4 — Chunk batching × Hilbert locality.** How far do
  `chunk_size_bytes` / `max_fetch_chunks` (with `MAX_CHUNK_GAP = 1` gap
  prefetch) cut backend GET count under a viewport workload, and where does
  read amplification (bytes fetched / bytes used) start to hurt? Under Phase 2,
  also sweep `chunk_fetch_merge_window_ms`, `backend_fetch_concurrency`, and
  `backend_fetch_max_inflight`, then
  inspect end-user latency and backend admission queue p95 rather than assuming
  every planned range starts immediately. These admission controls affect the
  real-cache phase; the aggregate modeled cache does not currently simulate
  concurrent backend queues or overload shedding.
- **Q5 — Derived-tile generation queueing** (Phase 2 only). How does
  contour/hillshade generation (~100–500 ms/tile) behave under the
  shared `ISKR_CPU_WORK_CONCURRENCY` and `ISKR_CPU_WORK_MAX_INFLIGHT` admission
  limits with viewport request patterns? This is not modeled yet.

## 2. Approach — two phases

Execution phase and cache representation are independent concerns. `real`
cache mode is a small-scale reference oracle that exercises payload-bearing
production paths. `modeled` cache mode is the intended path for large node,
memory, and request-count sweeps: it preserves production placement, eviction,
weighting, and range-planning decisions while avoiding allocation of the
corresponding payload bytes.

**Phase 1A (this spec's default): deterministic trace-driven simulation.**
Requests are executed one at a time on a single-threaded Tokio runtime;
results are counted, not timed. This answers Q1–Q3: hit rates, egress, and
rebalance are functions of *request order*, not of latency.

**Phase 1B (Q4 only): deterministic viewport batches with paused Tokio time.**
All newly visible tiles from one viewport step are polled together, preserving
their deterministic `(dy, dx)` order. `tokio::time` starts paused so the real
configured fetch merge window (10 ms by default) advances without wall-clock
delay after every ready request has had a chance to enqueue its missing chunks.
This exercises the
production single-flight, gap merge, and `max_fetch_chunks` behavior in
real-cache mode without introducing a latency distribution. Modeled-cache mode
uses its metadata-only range planner instead.

**Phase 2: Tokio paused-time simulation** (biei-sim style) for questions where
latency and queueing matter. VUs run concurrently as closed users: one viewport
batch completes, then the VU sleeps for `1.2 +/- 0.5 s`. The runner uses the
real resolver, caches, peer transport, single-flight, merge window, and backend
fetch concurrency limit. Fixed backend and peer delays are controlled sweep
inputs; fitted lognormal backend profiles are implemented. Terrain generation
and shared CPU-admission queueing remain future work.

## 3. Workload model — population-driven viewport walk

The population-driven viewport model is implemented in Rust so one
deterministic trace can drive both simulator sweeps and real-cluster validation
runs.

### 3.1 Data

The generator reads a GeoJSON `FeatureCollection` of point features with a
`population` property and loads it into a cumulative-weight array
(`Vec<(lng, lat, cum_weight)>`). Zero/negative-population points are dropped.
The conventional default path is
`sims/ishikari-sim/data/census_2020_1km_population.geojson`, but the dataset is
not committed to this repository; populate that path or pass `--census`.
Historical runs used 2020 Japan census 1 km mesh centroids.

### 3.2 Session state machine (per simulated user)

The workload contract uses these parameters and defaults:

| Parameter | Default | Meaning |
|---|---|---|
| `min_zoom` / `max_zoom` | 4 / 15 | zoom sampling range |
| `focus_zoom` | 13 | Gaussian zoom-weight center |
| `zoom_sigma` | 1.8 | Gaussian zoom-weight sigma |
| `session_reset_probability` | 0.07 | per-step chance to teleport & re-roll zoom |
| `zoom_walk_probability` | 0.0 | per non-reset step chance to replace pan with `z±1` at the same center |
| `move_step_tiles` | 1.0 | pan step, in tiles at current zoom |
| `viewport` | 3×3 | tiles fetched around the center tile |
| `users` | scenario-defined | number of concurrent session walkers |

Per step:

1. With probability `session_reset_probability` (or on first step): sample a
   mesh centroid from the population CDF by binary search, jitter uniformly
   within the 1/80° mesh cell, sample zoom `z` from the Gaussian weights over
   `[min_zoom, max_zoom]`. Reset the previous-viewport set.
2. Otherwise, with probability `zoom_walk_probability`: keep longitude/latitude
   fixed and move exactly one level to `z-1` or `z+1`. Interior directions are
   equiprobable; bounds reflect toward the only valid adjacent level. Zoom
   replaces pan for that step and requires `min_zoom < max_zoom`.
3. Otherwise: random-walk the position in Web Mercator unit space by
   `±move_step_tiles / 2^z` per axis.
4. Emit the 3×3 viewport around the center tile (x wrapped, y clamped),
   **deduplicated against the previous step's viewport set** — only newly
   visible tiles become requests, exactly like a map client.

Zoom-walk event and direction decisions are deterministic, domain-separated
functions of `(seed, user, step)`. Non-reset steps always consume the baseline pan
RNG draws even when zooming, so enabling the knob does not shift later reset
locations, reset zooms, or the underlying pan stream. Parent/child requests are
therefore introduced without confounding unrelated randomness. The immediately
previous viewport remains the only deduplication set; a changed zoom naturally
emits the new level's viewport.

### 3.3 Interleaving and traces

- Users are stepped round-robin. Phase 1A executes each step's new tiles in
  deterministic `(dy, dx)` order; Phase 1B polls that ordered list as one
  viewport batch. Think time does not exist in either mode.
- The generator is a pure function of `(census file, params, seed)`. A run can
  optionally dump the request sequence as a trace file
  (`jsonl: {step, user, ordinal, tileset, z, x, y}`) so that:
  - the exact same trace can be replayed across config sweeps
    (variance-free A/B), and
  - the trace can be exported for replay against a **real** cluster through the
    versioned `replay-http` runner for cross-validation (§8).
- Entry-node choice: each request is assigned an entry node, modeling the
  Gateway LB. Knob `entry_affinity = per_request | per_session`
  (default `per_request` uniform; Google Cloud Application Load Balancing
  performs backend selection per request, while `per_session` is a sensitivity
  model for connection affinity). Entry selection is derived from
  `(seed, request index or user, node count)` and may be materialized as
  `entry_node` when exporting a trace for direct-to-node cluster replay. This
  matters because a peer-
  forwarded tile is inserted into the entry node's L1 *as well as* living on
  the owner — replication that reduces effective cluster-wide cache capacity.

## 4. System model (Phase 1)

### 4.1 What runs for real

In real-cache mode, the simulator instantiates **N real in-process node
instances** — each with its own production `ResourceResolver` (tile cache,
resource cache, `ChunkedStore` with chunk cache + fetch coordinator, PMTiles
`Reader`). It replaces internal HTTP and UDP with in-process transports and
uses a simulator-owned Ishikari membership adapter over `mmpf-cluster`'s
production Chitchat lifecycle and simulated transport. Everything that decides
*what* to fetch, cache, group, forward, or expose as a live peer uses production
algorithms and metadata contracts:

- `HrwRouter` (tile→group→candidates, XxHash64 HRW) — as is.
- `mmpf-pmtiles::ArchiveReader` owns tile-id validation and bounded directory
  traversal; Ishikari's `Reader` supplies peer-first bootstrap/leaf access,
  caching, and single-flight. Directory read amplification is therefore
  modeled through the same stack.
- `ChunkedStore` + `ChunkFetchCoordinator` (chunk cache, missing-chunk
  grouping with `MAX_CHUNK_GAP`, `max_fetch_chunks` cap, single-flight) — as is.
- `TileCache` / `ChunkCache` / `ResourceCache` (moka, byte-weighted, including
  negative entries) — as is.
- The full `route_tile` flow in `ResourceResolver` (L1 check → HRW → peer
  forward in score order → local fallback → cache insert on both sides) — as is.

Backing data is an operator-supplied real archive resolved from
`--tileset-sources` and the trace's tileset id (for example the default
`mierune/omt` resolves under `data/mierune/omt.pmtiles`). Archives are not
committed to this repository. Real-cache mode therefore preserves actual tile
sizes, directory layout, sparseness (real 404s / negative caching), and Hilbert
offsets rather than using a synthetic archive model.

For capacity and node-count sweeps, `cache_mode=modeled` first builds a catalog
for the trace's unique tiles by resolving the real PMTiles bootstrap and leaf
directories without reading tile payloads. Each modeled node then keeps tiny
metadata values in Moka while assigning them the production logical byte
weights. Tile caches retain TinyLFU behavior; chunk caches retain LRU behavior
and the production 1 GiB cap. HRW placement and production chunk-range planning
are reused. This decouples resident memory from configured logical capacity.
`cache_mode=real` remains the calibration path for request-level fidelity.

### 4.2 Production seams

The current implementation uses three narrow boundaries:

1. **Peer transport.** `PeerBackend` constructs typed `/_internal/*` paths and
   calls `InternalTransport`. Production uses the reqwest implementation; the
   simulator resolves the target node in-process and calls the same resolver
   operations wrapped by the internal HTTP endpoints. Transport failures and
   fixed peer latency are injected at this boundary.
2. **Peer directory.** `PeerBackend` reads candidates through `PeerDirectory`.
   The production server and real-cache simulator have separate Ishikari
   adapters over `mmpf-cluster`; every real-cache node owns an independent
   Chitchat instance using `SimulationTransport`. Modeled mode applies its
   cheaper active-node set immediately at event boundaries.
3. **Simulator support exports.** The `simulator-support` feature exposes the
   PMTiles access plan, chunk-range planner, routing primitives, and injected
   resolver constructors required by modeled and in-process runs. Normal
   Ishikari builds do not expose those simulator-only APIs.

No production `Clock` seam is needed. Phase 1A does not claim to model
cross-request merging. Phase 1B uses Tokio's paused clock to exercise the real
configured merge window without sleeping in wall-clock time. Production reads
it from `ISKR_CHUNK_FETCH_MERGE_WINDOW_MS`; simulator real-cache runs use
`ClusterConfig::chunk_fetch_merge_window_ms`. Zero removes the intentional wait
without disabling pending/inflight sharing or immediate bootstrap dispatch.

### 4.3 Per-request flow

For each trace entry `(user, z, x, y)` in Phase 1A, or viewport batch in Phase
1B:

1. Pick the entry node per §3.3 and call its `ResourceResolver::route_tile`.
2. Production code does the rest. A thin `SimNode::serve_tile` adapter records
   the returned `TileSource`, its `cache_outcomes`, successful response bytes,
   and peer-transfer bytes. This mirrors the metric calls normally made by the
   HTTP handlers; `ResourceResolver::route_tile` does not increment those
   counters itself.
3. Phase 1A awaits completion before the next request. Phase 1B joins the
   ordered viewport futures before advancing to the next user step.

## 5. Metrics & output

Per run, per node, harvested from each node's Prometheus registry
(`NodeMetrics`) plus sim-side counters:

- Tiles served by source (`self_cache` / `peer_cache` / `self_backend` /
  `peer_backend` / `miss`) — headline **cache hit rate**, **L1 hit rate**, and
  **peer-forward rate**.
- Chunk cache hit / miss / post-fetch-hit; **backend GET count** and
  **backend bytes** (egress proxy); **read amplification** = backend bytes /
  tile bytes served.
- Peer-transfer bytes (east-west traffic).
- Gossip messages/bytes and unavailable-peer attempts caused by stale views.
- Peer forwarding outcomes and backoff skips from the production
  `NodeMetrics`, so failover attempts avoided during convergence are visible.
- Identical in-flight peer fetch overlap by resource kind, so additional
  response coalescing is considered only when measured fan-in justifies it.
- Per-node inbound and outbound internal-resource counts, classified by the
  shared production path classifier, for tile/index/provider hotspot analysis.
- Per-sample membership convergence: converged/stale node views, missing and
  extra peer references, min/max peer count, and virtual elapsed time.
- Cache occupancy (`tile_cache_weighted_size`, `chunk_cache_weighted_size`).
- **Load skew**: per-node share of local loads; report max/mean and CV.
- Backend scheduler distributions: fetch duration/size/chunks, queue delay,
  pending chunks at dispatch, and waiters released per fetch group. Structured
  histogram snapshots are merged without parsing Prometheus text.

A single run outputs one versioned (`schema_version`) self-contained JSON
document containing execution mode, tagged trace source (`generated` with
workload config, or `replay` with input path), cluster config, aggregate metrics,
and per-node summaries. Generated request traces are written separately as
JSONL and can be replayed in-process with either serial or viewport-batch
execution.

Churn replay emits configurable counter snapshots (default every 1,000
requests), plus paired pre/post-event snapshots. Samples include active cache
occupancy, per-node request counters, virtual elapsed time, and each node's
agreement with the active membership set, so hit-rate, backend-fetch,
load-skew, gossip-convergence, and recovery curves can be derived.

The version-1 `sweep` runner accepts a versioned JSON specification, one replay
trace, entry-assignment seeds, a base cluster configuration, and Cartesian axes
for node count, candidate count, tile-group size, chunk size/range cap, tile and
chunk cache capacity, and entry-node peer caching. It builds one shared modeled
PMTiles catalog, executes each cell in a fresh modeled cluster, and flushes one
self-contained run document per JSONL line. Every line records the run index and
count, effective configuration, periodic samples, simulator version, and FNV-1a
spec/trace fingerprints. Paths are resolved relative to the spec. Duplicate
axis values, invalid cluster dimensions, unsupported schema versions, and grids
larger than `max_runs` are rejected before output is created.

Version 1 is intentionally replay-only, sequential, and modeled-cache-only.
Timed controls (`chunk_fetch_merge_window_ms`, backend latency/concurrency),
workload-generation axes, real-cache lifecycle, and sweep-level visualization
are not claimed by this runner. Modeled reports retain timed configuration for
provenance but do not treat it as an effective sweep axis.

## 6. Scenario catalog

Each scenario = a config grid × the workload of §3, run on the same seed(s).

- **S1 — Steady-state sizing (Q1, Q3).** First run a baseline plus one-factor
  sweeps, then run the full interactions only over the surviving Pareto
  candidates. Warm-up ends when bucketed hit rate and backend GET rate remain
  within a configured tolerance for several buckets (with a hard request cap);
  measurement then uses a fixed request count.
  Grid: `nodes ∈ {2,3,5,8}` × `tile_group_size ∈ {32,128,512,2048,8192}` ×
  `tile_cache ∈ {64M,256M,512M,1G}` × `chunk_cache ∈ {64M,256M,512M,1G}` ×
  `users ∈ {50, 500, 5000}` (workload concentration). Outputs: hit rate,
  egress, skew per cell. Decision informed: default group size, memory
  requests in the demo deployment.
- **S2 — Churn (Q2).** Warm cluster of N; at request index K: (a) add one
  empty node, (b) kill one node (spot preemption; its caches vanish),
  (c) rolling restart (serial leave→rejoin-empty for each node, drain
  semantics = node absent from peer list), (d) correlated majority loss
  (for example 10→3 at one viewport boundary). Measure: fraction of tile-groups
  re-routed (compare with analytic ~1/N), hit-rate dip depth, requests to
  recover within 1 pt of steady state, backend egress spike. Sweep
  `tile_group_size` — bigger groups mean fewer, bigger invalidations.
- **S3 — Chunk batching (Q4).** Modeled grid: `chunk_size_bytes ∈
  {256K,1M,4M}` × `max_fetch_chunks ∈ {1,4,8,16}` × zoom mix (`focus_zoom ∈
  {11,13,15}`). Real-cache/Phase 2 grid additionally varies
  `chunk_fetch_merge_window_ms ∈ {0,1,5,10,25}` and backend concurrency.
  Outputs: end-user latency, backend GETs per 1k tiles, read amplification,
  chunk-cache hit rate, backend queue p95, and waiter fan-in. Generate separate
  otherwise-identical traces for `zoom_walk_probability ∈ {0,0.05,0.1,0.2}` to
  see how cross-zoom requests break Hilbert locality; sweep version 1 is
  replay-only and intentionally does not vary workload-generation parameters.
- **S4 — Skew under population workload (Q1).** No failures: quantify how
  uneven HRW + tile groups + a Tokyo-heavy CDF make per-node load, and confirm
  `candidate_count` does *not* spread load (it's failover-only). Then repeat
  with one node marked failing (transport decorator) to see spillover onto
  candidate #2.
- **S5 — Cold start.** All caches empty, no churn: requests until hit rate
  plateaus. Informs deploy-time behavior and warm-up expectations.

## 7. Determinism

- Seeded `rand::rngs::StdRng`, one stream per user derived
  from `(seed, user_index)` so adding users doesn't perturb existing streams.
- A single-threaded Tokio current-thread runtime. Phase 1A is fully serialized.
  Phase 1B polls one deterministic ordered viewport batch at a time with Tokio
  time paused.
- Caveat: moka's TinyLFU maintenance is an internal detail we don't control;
  single-threaded access should be reproducible, but if bit-identical repeats
  fail, we accept "statistically stable" (fixed seed, N repeats, report
  spread) rather than swapping the production cache for a deterministic LRU —
  fidelity to moka's actual admission/eviction *is the point*.
- Phase 1A intentionally excludes cross-request coordinator merging. Q4
  results must come from Phase 1B. Bit-identical completion order is not a
  requirement for Phase 1B; fixed-seed repeats must produce statistically
  stable aggregate counters.

## 8. Validation against the real cluster

Before trusting sweep output, calibrate once. The `replay-http` subcommand reads
an existing trace and supports two explicit target contracts:

- repeated ordered `--node-url` values use each request's recorded `entry_node`
  as an exact index, preserving direct-node placement;
- one `--gateway-url` sends every request through the load balancer and ignores
  recorded entry assignments, measuring the real affinity distribution.

Serial mode preserves request order. `--viewport-batches` starts one validated
`(step,user)` batch concurrently and waits for it before the next. Requests send
`Cache-Control: no-cache`, follow no redirects, retry nothing, and fully consume
response bodies. The versioned report contains status/body totals, bounded
failure samples, client-observed latency percentiles, target configuration, and
a trace fingerprint.

Optional ordered `--metrics-url` endpoints are scraped immediately before and
after replay. Counter deltas are derived per node before aggregation and fail on
a reset or disappearing series. The comparable output covers normalized tile
sources, external/internal/backend bytes, positive L1 hits, peer attempts,
backend fetch outcomes/chunks, and chunk-cache/coordinator counters. Production
collapses all negative result sources into `miss`, so comparison must normalize
simulator `negative_cache`, `self_miss`, `peer_miss`, and `miss`. The production
positive L1 metric cannot distinguish every negative-cache case, and
client-observed Gateway latency is not equivalent to Axum's server histogram.
Metrics are process-wide, so calibration requires an otherwise idle deployment
and stable pod identities/restart counts outside the report.

Calibration procedure:

1. Run the local 3-node cluster with a fixed configuration and distinct public,
   internal, and gossip ports.
2. Replay an exported trace (§3.3) directly against the recorded entry nodes.
   Restart cold and replay it again through the Gateway.
3. Compare the replay report's Prometheus aggregate (and direct per-node values)
   with the in-process simulator report for the same trace and configuration.
4. Acceptance: Phase 1A hit rates within ~2 points; Phase 1B backend GET count
   within ~10%. Report direct-node and Gateway validation separately.

Calibration results are evidence, not part of this contract. A published result must retain the exact trace fingerprint, simulator configuration, code/version provenance, fixture sources, direct-node and Gateway reports, and any fitted latency profile. Superseded or non-reproducible historical numbers do not belong in this specification.

## 9. Implemented Model Surface

Layout: the repository is one Cargo workspace. Reusable production cache,
storage, and routing logic lives in `crates/ishikari-core`; CLI, HTTP, and
platform assembly live in `servers/ishikari`; simulator code lives in
`sims/ishikari-sim`. Generic PMTiles traversal and Chitchat lifecycle are reused
from `mmpf-pmtiles` and `mmpf-cluster`. The core crate exports its narrow
simulation seam only under `simulator-support`; normal server builds therefore
carry no simulator dependencies.

Implemented: the transport/directory seam, workload generator, Phase 1A
in-process cluster, structured metrics report, Phase 1B viewport runner,
validated in-process JSONL replay, scripted node add/remove replay, versioned
modeled-cache sweep orchestration, and external direct-node/Gateway HTTP replay
with optional Prometheus calibration deltas. Metadata-only per-node modeled
tile/chunk caches and PMTiles access catalogs are also implemented for large
logical-capacity sweeps. A
self-contained HTML report visualizer renders request-indexed churn charts,
interval rates, peer failover/backoff activity, cache occupancy, and active-node
load.
Structured production scheduler histograms and aggregate node request-load
skew are included in current JSON reports.

Remaining implementation work is tracked once in
`../issues/ishikari-todo.md`.

## 10. Phase 2 latency and queueing

`--phase2` replays each trace user in its own Tokio task. Tiles in one viewport
use concurrent batch semantics, and the next viewport starts
after batch completion plus deterministic think-time jitter. Because Tokio time
is paused, long virtual runs complete without wall-clock sleeps.

The first implementation models:

- fixed or fitted lognormal latency per object-storage range GET via the
  production fetcher seam, with a transfer-time term proportional to range size;
- fixed in-process peer latency;
- production request batching and single-flight, the per-tileset coordinator's
  32 in-flight-group cap, and the configurable process-wide per-node backend
  fetch semaphore (default 32);
- request timeout cancellation;
- overall/per-source p50, p90, p95, p99, max, throughput, and per-node peak
  in-flight requests.

Published Phase 2 measurements must include the trace fingerprint, full configuration, and latency-profile provenance. Real-cache mode includes Chitchat propagation and failure detection with a configurable virtual per-hop delay and reports backend admission and single-flight participation. The latency model does not include terrain CPU semaphore time, gossip packet loss or partitions, HTTP parsing, public Gateway/CDN latency, or kernel/network queues; those remain explicit fidelity boundaries and should be modeled only from measured inputs.

## 11. Current non-goals

- No stochastic gossip packet loss or partition model. Real-cache mode runs
  production chitchat with deterministic virtual hop latency; modeled mode
  intentionally changes membership instantaneously.
- No glyph/sprite/style provider traffic, no Mapterhorn composite resolution,
  no MLT transcoding, no terrain generation.
- No HTTP/serialization overhead; peer transfers are counted by tile byte
  length.
- No client cache model beyond viewport dedup. Browser HTTP caches are outside
  the simulator's current workload contract.

