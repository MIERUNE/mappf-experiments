# Ishikari

A distributed PMTiles cache proxy for efficient, low-cost, large-scale serving from object storage.

> [!WARNING]
> This is an experimental, proof-of-concept project. The behavior, API, and configuration are not stable.

Ishikari focuses on large-scale PMTiles serving workloads:

- **Backend request batching** - reduces object storage requests, traffic, and latency.
- **Distributed cache** - uses gossip membership, locality-aware routing, and caching tuned for Hilbert-sorted PMTiles archives.
- **Optional derived terrain products** - generates hillshade and contour tiles
  from raster DEM sources such as Mapterhorn while preserving the ordinary
  PMTiles delivery path for source data.

CPU-heavy DEM decode, terrain generation, and MLT transcoding share one bounded
worker budget. `ISKR_CPU_WORK_CONCURRENCY` defaults to the process's available
parallelism and can be set explicitly per deployment.

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

## Simulator

`ishikari-sim` generates deterministic population-weighted viewport traces and
estimates how a deployment behaves without allocating the equivalent cluster,
cache memory, object-store traffic, or wall-clock time. It reuses Ishikari's
production HRW, PMTiles range planning, request batching, and cache policy, then
combines them with logical byte capacity, virtual time, and cloud-calibrated
latency models:

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --cache-mode modeled \
  --tileset japan \
  --tileset-sources data \
  --nodes 3 \
  --users 50 \
  --steps 1000 \
  --viewport-batches \
  --output trace.jsonl \
  --report report.json
```

Without `--viewport-batches`, requests run serially for deterministic cache and
placement studies. With it, each viewport is polled concurrently under paused
Tokio time, exercising the production 10 ms chunk merge window without adding
wall-clock delay.

Replay the exact same trace against another cache or batching configuration:

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --viewport-batches \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 3 \
  --chunk-size-bytes 262144 \
  --max-fetch-chunks 8 \
  --report replay-report.json
```

The simulator can compare the production entry-node hot-cache policy with
owner-only positive tile caching using `--peer-tile-cache entry` (default) or
`--peer-tile-cache owner-only`. Both modes execute the production resolver;
the selected policy is recorded in the report as `cluster.cache_peer_tiles`.

Reports identify their trace source as `generated` or `replay` and include the
full cluster configuration and aggregate/per-node metrics.

Replay node additions and removals with a churn plan. Events are applied at
request boundaries in serial mode and at the next completed viewport boundary
with `--viewport-batches`; the report records both requested and actual request
indices:

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --cache-mode modeled \
  --viewport-batches \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 3 \
  --churn-plan ishikari-sim/data/churn-example.json \
  --churn-sample-every-requests 1000 \
  --report churn-report.json
```

New nodes join with empty tile and chunk caches. Removed nodes leave the ingress
set and in-process transport immediately; in real mode, stale chitchat views may
still select them briefly and exercise the production peer fallback path. Their
cumulative requests, backend bytes, and metrics remain in the final report with
`active: false`. Churn samples make cache-hit loss, peer redistribution, and
backend refetches visible over time.
Each event has `pre_event` and `post_event` samples at the same request index;
samples also include active cache occupancy and per-node request counters.
To make added nodes eligible for ingress, churn replay deterministically
reassigns requests over the current active set using `--entry-affinity`; it does
not reuse the trace's fixed node indices. In `real` cache mode every simulated
node runs Ishikari's production chitchat membership over an in-memory transport
and Tokio's virtual clock. Node-local peer views therefore converge after
churn, including the production failure detector and peer-list TTL. The
metadata-only `modeled` mode keeps membership changes instantaneous so large
node/capacity sweeps remain cheap.

Generate a self-contained visualization from any simulation report:

```bash
cargo run -p ishikari-sim -- visualize \
  churn-report.json \
  --output churn-report.html
```

Churn reports provide request-indexed trend charts with churn event markers,
interval cache/peer rates, backend fetch rate and transfer volume per 1,000
requests, active cache occupancy, and final node load.
The HTML embeds the report and has no server or external asset dependency.

Tile source labels distinguish both placement and backend involvement.
`self_cache` covers entry-node L1 hits and local resolutions completed entirely
from PMTiles/index and chunk caches. `peer_cache` is the equivalent response
from an HRW peer. `self_backend` and `peer_backend` mean that tile resolution
waited for at least one object-storage chunk fetch, including joining pending or
inflight work. `miss` includes positive lookup misses and negative-cache hits.
The reported `cache_hit_rate` is `(self_cache + peer_cache) / requests`, so it
includes positive L1 hits and PMTiles resolutions completed from chunk caches.
`l1_cache_hit_rate` remains available separately in the JSON report.
`Client egress` is the successful tile payload sent to end users; `Peer
transfer` is internal east-west traffic.

For a majority-loss scenario, start with 10 nodes and remove seven at the same
viewport boundary:

```bash
cargo run -p ishikari-sim --release -- \
  --simulate \
  --cache-mode modeled \
  --viewport-batches \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 10 \
  --churn-plan ishikari-sim/data/churn-majority-failure-example.json \
  --report majority-failure-report.json
```

This validates HRW redistribution and cold-cache recovery on the three
surviving nodes. Use `--cache-mode real` to include node-local chitchat
convergence, or `--cache-mode modeled` to isolate placement and logical cache
recovery with instantaneous membership. Gossip packet loss remains a separate
failure-injection model.
Use `churn-steady-state-example.json` for an event-free baseline with the same
dynamic ingress assignment; a regular replay preserves the trace's original
entry-node indices and is not comparable when changing the node count.
`churn-mixed-example.json` provides a longer-running deterministic sequence of
staggered additions, removals, temporary contraction, and removal of a node
that joined during the run.

For large cache-capacity and node-count sweeps, use metadata-only modeled
caches. The catalog reads PMTiles directories once, but tile and chunk cache
entries retain only logical byte weights rather than payloads:

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --cache-mode modeled \
  --viewport-batches \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 8 \
  --tile-cache-max-bytes 68719476736 \
  --chunk-cache-max-bytes 1073741824 \
  --report modeled-report.json
```

`real` remains the default reference mode and executes production resolvers
with real payload caches; it is useful for checking model fidelity on small
runs, not for representing production-scale memory. `modeled` is the scalable
capacity-study mode. It currently accepts one local PMTiles root and reuses
production HRW placement, Moka TinyLFU/LRU policy, byte weights, and chunk range
planning without retaining tile payloads. The production 1 GiB per-node
chunk-cache cap also applies in modeled mode.

For latency and queueing experiments, replay a trace with concurrent virtual
users under Tokio's paused clock. This runs the production resolver, caches,
single-flight, 10 ms merge window, and 32 concurrent range-fetch limit while
adding deterministic backend and peer latency. The repository includes a GCS
profile measured from the demo cluster in `asia-northeast1`:

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --phase2 \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 3 \
  --backend-latency-profile ishikari-sim/data/gcs-asia-northeast1-2026-07-13.json \
  --peer-latency-ms 1 \
  --report timed-report.json
```

The timed report includes throughput, request latency percentiles overall and
by source, timeouts, and peak in-flight requests per node. The common result
also reports backend fetch size/duration, batching queue delay, pending chunks,
group waiters, and node request-load skew (max/mean and coefficient of
variation). Each virtual user waits for its viewport batch, then sleeps for
`1200 +/- 500 ms` by default, matching the k6 closed-user model. The measured
profile uses a deterministic lognormal range-fetch latency plus a per-MiB
transfer term. Fixed controlled sweeps remain available through
`--artificial-backend-delay-ms`; sigma and the transfer slope can also be
supplied directly.
