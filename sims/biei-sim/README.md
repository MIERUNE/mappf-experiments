# biei-sim

Distributed renderer simulator specification.

## 1. Overview

The simulator evaluates request routing and renderer-slot management for a
distributed static-image and tile renderer. It runs in one process with Tokio
paused time. Rendering, network transfer, and resource loading are represented
by deterministic or sampled delays.

Production code lives in `biei-core`. Simulator-only adapters, workloads,
and reports live in `biei-sim`, which consumes the core crate's public traits
and types as a downstream crate.

The production meaning of `StyleRevision`, wire types, HTTP forwarding,
membership, MapLibre Native integration, and rendered output is defined in
[`../../specs/biei-spec.md`](../../specs/biei-spec.md). Concrete Rust
definitions are authoritative.

This is a current-state specification. Unqualified statements describe the
current simulator; future work is confined to the milestone and future-
experiment sections.

Current identity model:

- `StyleId` is a stable cluster-wide string; there is no process-local numeric
  style id.
- `WorkerProfile = StyleRevision + RenderMode + Scale` is the unit of warmness,
  HRW affinity, and eviction.
- Simulator workloads use 2x by default and assign a small subset of styles to
  tile mode so both renderer shapes participate without dominating scenarios.

The simulator models any profile mismatch with one `style_setup_cost`.
In-render work is split into CPU service, warm-profile resource wait, and the
first resource wait after profile setup. Resource waits retain native-render
residency but do not consume a modeled CPU core. The model does not yet expand
individual tile/glyph/sprite requests, cache state, provider capacity, or the
resource critical path into causal sub-operations. It can import
shape-conditioned wall distributions through the two-window calibration path,
but direct renderer-thread CPU/non-CPU attribution and validation against
production end-to-end latency remain pending.

## 2. Goals and Non-goals

### Goals

- Quantify warm tracking, HRW plus bounded loads, drain-and-swap, overflow, and
  rejection behavior.
- Measure how bounded-load capacity, warm slot count, execution permit count,
  gossip cadence, and workload shape affect latency, throughput, swaps, and
  cold starts.
- Compare Zipf, burst, sustained, style-shift, and addlayer-source workloads.
- Exercise the same request/response, profile, and routing contracts used by
  production.

### Non-goals

- Actual map rendering.
- Actual HTTP forwarding.
- Exact CPU/GPU contention simulation.
- Modeling native crashes or wedged renderer threads unless measured failure
  rates later make them relevant to capacity planning.
- Predictive prewarming.

## 3. Architecture

```text
WorkloadGenerator
       |
       v
Node::handle_incoming
       |
       +--> rendered-output cache model
       |
       v
Dispatcher -- gossip ClusterView
       |
       +--> WorkerPool --> RendererSlot 0..N-1
       |                    | shared execution permits
       |                    ` shared native-render residency permits
       |                         ` simulated CPU cores
       |
       `--> Transport --> peer Node::handle_forwarded

Completed outcomes --> simulator MetricsCollector --> Report / CSV / JSON / HTML
```

### Responsibilities

- `Simulation`: constructs the cluster and owns component lifetimes.
- `WorkloadGenerator`: generates tasks and spawns concurrent
  `handle_incoming` calls.
- `Node`: owns the dispatcher, worker pool, output cache, profile preparer,
  cluster-view cache, and forwarding policy.
- `Dispatcher`: chooses local execution, forwarding candidates, or rejection.
- `WorkerPool`: tracks local loaded profiles and queues, selects a renderer
  slot, and dispatches through bounded channels.
- `RendererSlot`: keeps one profile warm and processes one command at a time.
- execution permits: bound concurrent setup/render work independently from warm
  slot count.
- native-render permits: bound `renderStill` residency, including FileSource
  waits.
- CPU cores: bound only sampled CPU service after resource waiting.
- `GossipBus`: publishes per-worker KVs and produces cluster views.
- `Transport`: applies simulated hop latency and invokes the target node.
- `MetricsCollector`: finite-run simulator reporting, distinct from production
  Prometheus metrics.

The simulator uses the normal `StyleCatalog` template path. Template resolution
is computed per request and must not permanently insert arbitrary style ids.
Forwarded revisions pass the same `accepts_revision` check as production; there
is no simulator-only acceptance bypass.

## 4. Shared Domain Semantics

| Type | Simulator meaning |
|---|---|
| `RequestId` | end-to-end correlation preserved through forwarding |
| `NodeId` | stable cluster identity such as `node-0` |
| `StyleRevision` | stable style id plus version |
| `WorkerProfile` | revision, static/tile mode, and 1x/2x scale |
| `RenderRequest` | tile or static image request shape |
| `SourceRef` | optional addlayer source identity and cache policy |
| `InternalTask` | local task carrying Tokio `Instant` values |
| `WireTask` | clock-independent forwarded representation |
| `TaskOutcome` | completed, rejected, or failed result |
| `RouteTier` | cache hit or routing tier 1 through 4 |
| `ForwardRequest` | wire task plus route tier and drain hint |

`CompletedInfo.worker_id` is `None` for an output-cache hit and `Some(id)` for
actual worker execution.

### Gossip KVs

Worker state is encoded as per-key strings:

```text
worker.{id}.style = "<StyleId>@<version>" | ""
worker.{id}.mode  = "static" | "tile" | ""
worker.{id}.scale = "1x" | "2x" | ""
worker.{id}.queue = "<usize>"
```

The publisher computes a snapshot and sends only changed keys. A partially
propagated worker record is omitted from the decoded view rather than filled
with invented values.

`ClusterView` separates static membership from observed state. Dispatchers can
therefore compute the same HRW ordering during startup even before all worker
state has propagated.

### Internal-only state

- `WorkerCmd::Process` carries the task, optional prepared profile, route tier,
  overflow flag, and a oneshot response sender.
- `PoolState.loaded` is the eager local warm-profile index.
- `SourceCache` is a per-slot LRU for reusable addlayer source ids.

## 5. Trait Implementations

| Boundary | Simulator implementation |
|---|---|
| `ProfilePreparer` | `NoopProfilePreparer` |
| `Renderer` | `StubRenderer` using sampled sleeps |
| `GossipBus` | `ChitchatGossipBus` with simulated in-memory transport |
| `Transport` | `ChannelTransport` with hop-latency sleep |

`Transport` is request/response, not fire-and-forget. The simulator uses the
same `ForwardRequest` and `ForwardResponse` semantics as production, with empty
rendered bytes.

### Stub renderer

`StubRenderer` samples configured ranges for profile setup, source load, and
render. It returns an empty `RenderOutput` with the requested format. A profile
mismatch currently pays one setup cost regardless of whether production would
reload a style or rebuild the renderer shape.

### Chitchat simulator bus

The simulator runs real chitchat instances over an in-memory transport that
injects `hop_latency`. `view()` reads a deterministic member snapshot and
decodes worker KVs with the production decoder. Duplicate reads within one
paused-time epoch are cached. Churn events add or shut down real chitchat
handles and update the forwarding registry, rather than editing a synthetic
member count.

Chitchat uses Tokio time, so paused-time execution remains valid. Propagation
retains chitchat's multi-round convergence rather than using a custom fixed
delay model.

### Channel transport

`ChannelTransport` sleeps for the configured hop latency, resolves the target
through a weak in-process registry, and calls `handle_forwarded`. Weak entries
allow automatic cleanup when the harness drops a node.

## 6. Routing

### 6.1 Escalation ladder

1. Render-output cache hit.
2. Tier 1: local or cluster-visible warm profile.
3. Tier 2: HRW ownership under bounded loads, including optimistic capacity
   when state is missing.
4. Tier 3: drain an eligible slot and swap profile if its ETA fits the SLA.
5. Tier 4: local overflow admission if hard capacity permits.
6. Reject when no safe route remains.

`handle_forwarded` does not recursively perform a normal cluster-wide routing
cycle. It follows the route and drain hint selected by the entry node, checks
hop and deadline rules, then executes locally or rejects.

### 6.2 Tier 1: warm tracking

A slot is warm only when its complete `WorkerProfile` matches. Local warm
candidates use queue and optional source-affinity hints. Cluster-visible warm
candidates may be forwarded to according to load and deterministic ordering.

### 6.3 Tier 2: HRW plus bounded loads

HRW uses stable profile identity and node identity. The current implementation
uses an FNV-1a-derived profile hash mixed with splitmix64. Revision is excluded
from ownership reshuffling while remaining part of exact warmness.

For profile setup cost `S`, warm native residency `P_w = C + I_w`, CPU service
`C`, warm resource wait `I_w`, and SLA `L`, the soft queue bound is:

```text
BL = min(S / P_w, L / P_w - 1)
```

The hard queue bound is a configured multiple of BL. A member whose state has
not arrived is evaluated optimistically during bootstrap; otherwise every new
profile could herd onto the only node with visible state.

### 6.4 Tier 3: drain and swap

Choose slots whose current profile is over-allocated before singleton slots.
Prefer a candidate with the same renderer shape when that avoids a production
rebuild, then use queue depth and stable node/worker identity. Profile recency
is local-only state and is not currently used for cluster-wide eviction.

The candidate is acceptable only when estimated queue drain plus setup and
render work fits the task deadline. Warm work uses `P_w`; a profile change uses
`S + P_f`, where `P_f = C + I_f` includes first-render resource wait. The
current approximation still does not model execution/native/core permit waiting
as separate ETA terms.

### 6.5 Tier 4 and elastic expansion

When warm queues cross the comfort threshold, fresh local slots can join the
profile before hard saturation. Overflow admission is bounded and recorded.
It must never turn the worker queue into an unbounded latency buffer.

### 6.6 Forwarding

- At most one forwarding hop.
- Retry another candidate on retryable transport errors and retryable remote
  rejections.
- Do not extend the end-to-end deadline.
- Preserve request id, profile, source identity, and requested output format.

### 6.7 Render-output cache

The simulator keeps this disabled by default because outputs contain no useful
bytes and cache hits would distort routing experiments. Dedicated tests can
enable it to verify shared Node semantics and `RenderCacheHit` outcomes.

## 7. Worker and State Lifecycle

### 7.1 Worker loop

For each command:

1. Acquire an execution permit and compare the requested profile with current
   loaded state.
2. If needed, set up the profile and record a cold start/style swap.
3. Ensure a reusable or one-shot source according to policy.
4. Acquire a native-render residency permit.
5. Wait for warm/first-render resources without consuming a modeled CPU core.
6. Acquire a modeled CPU core, perform render/encode CPU service, and release
   the core and both permits.
7. Return a `TaskOutcome` through oneshot.
8. Update pool state and publish changed gossip KVs.

Deadline checks occur at each modeled stage. Channel closure and renderer
failure become typed failures rather than panics.

### 7.2 Source cache

Each slot has a bounded LRU. `Cacheable` sources are inserted and touched;
`OneShot` sources are loaded but never inserted or allowed to displace reusable
entries. Source identity is a soft affinity dimension separate from
`WorkerProfile`.

### 7.3 Activity tracking

Activity is an observation-based eviction tie-breaker, not a predictive demand
model. Entries are bounded or pruned because arbitrary profile ids must not
create monotonic memory growth.

### 7.4 Local slot selection

Selection considers exact warmth, source availability, queue depth, renderer
shape, and slot freshness in that order only where the configured policy makes
the trade-off explicit. Source affinity is an optimization hint; stale hint
state may cause an extra source load but never incorrect rendering.

## 8. Workloads

The workload generator supports:

- Poisson arrivals.
- Zipf-distributed style popularity.
- Bursts and sustained load.
- Style shifts and new-profile arrival.
- A configurable static/tile split at a fixed 2x scale.
- Optional addlayer source patterns.

Source patterns:

- shared reusable sources;
- periodically refreshed versions;
- one-shot sources;
- mixed realistic distributions.

At most one addlayer source is modeled per request, matching the current public
API policy.

Default scenarios are for comparisons, not production sizing. Absolute costs
remain provisional until calibrated with real MapLibre measurements.

## 9. Metrics and Reports

Each measured task records arrival, completion, route tier, rejection/failure,
cold start, style swap, source cache result, overflow admission, and timing
stages.

Reports include:

- completed/rejected/failed counts;
- throughput;
- p50, p90, p95, p99, maximum latency, and a fixed-bucket latency
  distribution;
- route-tier distribution;
- cold-start and style-swap rates;
- queue overflow and rejection reasons;
- source hit/miss rates;
- native-render busy time, average/peak in-flight work, and utilization against
  time-weighted residency-permit capacity. Schema v3 names these fields
  `native_render_*`; they are not OS CPU utilization.

Run one deterministic simulation and write both machine-readable and
self-contained visual reports:

```sh
cargo run -p biei-sim -- run \
  --report biei-sim-report.json \
  --html biei-sim-report.html
```

`biei-sim run --report <path>` writes a schema-versioned JSON report containing
the complete effective simulation configuration and final result. When a churn
plan is supplied, the report also contains observations before and after every
membership event and at a configurable request interval. Each observation
records active membership, per-node request outcomes, worker queue depth,
loaded worker count, route tiers, source hits, and peer-forward traffic.
Churn `at_request` and the sampling interval use a dedicated measured-request
clock that starts after warmup. Task IDs and `submitted_total` still cover the
entire workload, while `submitted_measured`, aggregate outcomes, latency,
tier, source, and swap counters all cover the same post-warmup population.
Each sample exposes cumulative terminal and outstanding work plus a
`completion_window` containing outcomes observed between adjacent boundaries.
Its p50, p99, and maximum latency therefore describe completion time, not the
topology under which each request was submitted. Physical forwarding fields
say whether attempts were *started* or successes were *observed* in that
window; a success may correspond to an attempt from the preceding window.

Requests are also grouped in `submission_cohorts` by the topology epoch stamped
at submission. A slow request remains in its original cohort even if it
completes after a membership event. Cohort `submitted`, `terminal_outcomes`,
and `outstanding` counts reconcile independently of the completion windows.
Events beyond the generated measured workload are retained as
`unapplied_events`; they warn at the CLI but never discard an otherwise
completed report.

`biei-sim visualize <report> --output <path>` embeds that JSON into a
self-contained HTML report. It requires no server or external JavaScript and
escapes script-terminating characters before embedding untrusted report data.

The simulator collector stores finite-run records in memory. This is deliberate
and is unrelated to the bounded-label Prometheus implementation in production.

### Production calibration profiles

Production emits the required bounded-cardinality histograms. M12a is
implemented by `biei-sim calibration export`: it evaluates time-bounded
`increase(...[window])` instant queries at one explicit end timestamp, sums away
pod/scrape-target labels, and retains only each metric's bounded semantic
labels. It converts Prometheus cumulative `le` buckets to ordered,
non-cumulative counts in schema v1
(`biei-production-calibration-profile`). The JSON also preserves every PromQL
query, collection window and selector, deployment revision, architecture,
operator-named hardware profile, and effective core/renderer/permit counts.

The exporter requires at least one usable histogram family, but no individual
family is mandatory. Empty families remain present and produce explicit
warnings, since a stable warm window may legitimately contain no render,
style, or source setup samples. Non-monotonic or malformed histograms fail
export. Bearer tokens are read only from an optional file and are never
serialized. Snapshot creation refuses to overwrite an existing path.

The M12b bridge is implemented by `biei-sim run --cost-profile
<traffic-snapshot> --cpu-profile <resource-warm-snapshot>`. The two-window form
derives setup and warm/first-render wall distributions from the
realistic traffic window, while the verified resource-warm reference supplies
the representative CPU+encode service-wall proxy. The reference must contain
non-empty upstream instrumentation, have at most 0.05 regular upstream attempts
per warm render, record `capture_concurrency=1` so its wall time does not already
contain scheduler contention, and match the traffic window's deployment
revision, hardware, architecture, core count, renderer slots, and permit layout.
Every sufficiently sampled traffic render shape must also have a matching warm
reference shape. The importer samples CPU from the exact reference shape and
reweights the routing-level global CPU range to the traffic mix; it rejects
cross-shape subtraction instead of interpreting format/size encoding cost as
resource I/O.

The importer applies each stage independently. A family with enough samples is
measured or derived; a missing, sparse, or unsafe family keeps its simulator
default without discarding useful evidence from other families. The run report
records structured stage coverage (`measured`, `derived`, or `default`), sample
counts, exact-shape and aggregate-fallback sampler counts,
collection/provenance metadata for both windows, and every
fallback/approximation note. The traffic profile's
core/slot/execution/native-permit provenance is applied to the run, while SLA
and hop latency remain scenario settings.

`biei-sim calibration exercise` provides a bounded low-volume measurement
window against one or more full tile/static URLs. It warms each URL first,
runs a fixed request count under bounded concurrency, consumes response bodies,
waits for a configurable scrape-settle interval before and after measurement,
and prints the exact Unix start/end timestamps for `calibration export`. Any
non-2xx response fails the exercise. It is not a general load generator and
intentionally does not require production-scale traffic.

```sh
cargo run -p biei-sim -- calibration exercise \
  --url 'http://localhost:8080/carto/gl/voyager-gl-style/0/0/0@2x.webp' \
  --url 'http://localhost:8080/carto/gl/voyager-gl-style/static/139.767,35.681,11,0,0/640x360@2x.webp' \
  --warmup-requests-per-url 2 --requests-per-url 100 --concurrency 4
```

The exercise prints the measured Unix-time window for the exporter. Its
default 30-second settle period before and after measurement assumes a
Prometheus scrape interval of at most 30 seconds; set
`--scrape-settle-seconds` to the deployment's scrape interval when it is
longer.

Routing continues to use workload-weighted global `CostRange`s because the
dispatcher needs one representative service-cost range. The stub renderer,
however, samples the imported histograms per request by mode, scale, format,
size, and warm/cold/swap state. Sparse exact shapes fall back to the aggregate
for the same state, then to the configured `CostRange`; style/source setup uses
the analogous shape-to-global fallback. Production still does not directly
measure the pure CPU/resource split inside `renderStill`, so that split remains
provisional. M12b is complete only after the resulting end-to-end distribution
is validated against production before any default is revisited.

Use the `scope="ingress"` series from `biei_render_duration_seconds` as renderer
service time and the setup-stage histograms for their corresponding simulator
costs. Do not sum `scope="forwarded"` into the ingress series: both describe the
same forwarded request at different nodes. Use
`biei_request_duration_seconds` only to compare simulated and observed
end-to-end behavior; feeding it back as service time would count production
queueing twice. A simulator run reads a checked-in or explicitly supplied
snapshot and never depends directly on a live Prometheus endpoint.

Example (the URL is a Prometheus API root, not biei's raw metrics endpoint):

```sh
cargo run -p biei-sim -- calibration export \
  --prometheus-url "$PROMETHEUS_URL" \
  --start-unix-seconds "$START" --end-unix-seconds "$END" \
  --match-label namespace=map-demo --match-label container=biei \
  --deployment-revision "$REVISION" \
  --architecture x86_64 --hardware-profile "$HARDWARE_PROFILE" \
  --cpu-cores-per-node 2 --renderer-slots-per-node 3 \
  --execution-permits-per-node 2 --native-render-permits-per-node 2 \
  --capture-concurrency 4 \
  --output "$PROFILE"
```

For Google Managed Service for Prometheus, use its project-scoped Prometheus
root and supply the OAuth token with `--bearer-token-file`.

## 10. Configuration

Configuration groups:

- cluster size and renderer slots per node;
- execution and native-render residency permits, plus actual CPU cores;
- queue soft/hard limits and bounded-load policy;
- gossip publish cadence and transport hop latency;
- profile setup, source load, render CPU, warm resource, and first-render
  resource cost ranges;
- workload duration, warmup, arrival rate, distribution, and source pattern;
- random seed and output/report options.

The current default is a comparison-sized run: 2 nodes, 16 renderer slots per
node, 15 initial styles, 2 tile-mode styles, 100 requests/s, 30 seconds, and a
2-second warmup. The recommended `run` subcommand can override node count,
style count, rate, duration, and warmup; larger experiments such as 50 nodes and
20 styles must opt in explicitly. All generated requests currently use 2x and a
fixed tile/static geometry per style, so the simulator models routing and
service time rather than geographic cache locality.

For example, a larger routing/churn experiment can start with:

```sh
cargo run -p biei-sim -- run --nodes 50 --styles 20 \
  --rate 5000 --duration-seconds 60 --warmup-seconds 5 \
  --report biei-sim-report.json --html biei-sim-report.html
```

The rate in that command is illustrative; production-calibrated service-time
profiles are required before interpreting utilization or capacity.

Production-only CLI settings do not belong here. Simulator cost sampling and
scenario controls do not belong in the production server.

Churn plans use ordered JSON events:

```json
{
  "events": [
    { "at_request": 500, "action": "add" },
    { "at_request": 1500, "action": "remove", "node_id": "node-0" }
  ]
}
```

An add event assigns the next stable `node-N` identity. Removing the final
active node is rejected. A remove event immediately stops new ingress and
forwarding to that node, then keeps it in a visible `draining` set until all
requests selected before the event complete. Queue depth and per-node counters
therefore remain observable during scale-down instead of disappearing at the
event boundary.

```sh
cargo run -p biei-sim -- run \
  --churn-plan sims/biei-sim/examples/churn-plan.json \
  --report churn-report.json
cargo run -p biei-sim -- visualize churn-report.json --output churn-report.html
```

## 11. Tokio Paused Time

Tests and runs use Tokio paused time so delay-heavy scenarios finish quickly
and reproducibly.

Rules:

- Use Tokio time throughout simulator adapters.
- Avoid blocking system calls.
- Give spawned tasks opportunities to run before advancing time.
- Treat random generation as explicitly seeded.
- Distinguish smoke duration from steady-state measurement duration.

Warm renderer slots, execution permits, native-render residency permits, and
CPU cores are intentionally separate. More slots increase warm coverage;
resource waits can overlap beyond core count, but CPU phases serialize on the
modeled cores. Permit sweeps are capacity experiments, not production sizing
evidence until imported distributions reproduce observed behavior.

### Measurement method

Short smoke runs validate startup and invariants only. Comparative sweeps use a
warmup period and a longer measurement window. Production sizing evidence
should use at least a 30-second run and warmup longer than the modeled cold-start
period, with repeated seeds rather than one sample.

## 12. Milestones

Completed:

- M1: single-node baseline.
- M2: profile swaps and warm tracking.
- M3: multi-node HRW plus bounded loads and one-hop forwarding.
- M4: Zipf, burst, and new-profile workloads.
- M5: drain-and-swap.
- M6: overflow, rejection, and deadline-aware drain ETA.
- M7: reusable, refreshed, one-shot, and mixed source models.
- M8: bounded-load, cluster-size, standby-slot, execution-permit, overflow,
  and style-shift sweeps.
- M9: real chitchat semantics over simulated transport.
- M10: dynamic node add/remove plans with pre-event, post-event, periodic, and
  final cluster observations.
- M11: schema-versioned JSON reports and self-contained HTML visualization.
- Structural M12 cost split: warm/first-render resource wait and CPU service
  are separate; resource wait does not consume a modeled core.
- M12a: time-bounded schema-v1 Prometheus profile export with disjoint bucket
  counts and deployment/hardware/permit provenance.
- M12b import: `--cost-profile` with `--cpu-profile` derives representative routing ranges and
  shape-conditioned empirical runtime samplers, applies measured node/permit
  provenance and partial stage evidence independently, and records structured
  coverage plus every approximation in the run report.
- Bounded calibration exercise runner for collecting representative stage
  samples without production-scale traffic.
- Resource-warm capture verification: the CPU-reference window must carry
  upstream instrumentation and stay below 0.05 regular-lane fetches per render.
  The realistic-traffic window is expected to contain provider I/O; its fetch
  ratio is recorded as context and its render walls are never promoted to CPU
  evidence. `state="warm"` alone means style-warm, not resource-warm.
- Two-window fusion (`--cpu-profile` + `--cost-profile`): a verified
  resource-warm reference window supplies a CPU+encode service-wall proxy and a
  realistic-traffic window supplies the walls that become resource waits.
  The CPU reference must record capture concurrency one; concurrent reference
  walls are rejected because replaying them under the simulator core semaphore
  would count CPU contention twice. Both windows must come from the same
  deployment revision, hardware, and node shape, and the CPU reference must
  cover every sufficiently sampled traffic render shape. Per-request CPU replay
  uses that exact reference shape; representative routing ranges are reweighted
  to the traffic mix. This remains an approximation
  without per-render fetch attribution or renderer-thread CPU time, which the
  current production metrics do not expose.
- Calibration import rejects windows containing render timeouts because their
  successful-render histograms are right-censored. Export also rejects `pod` or
  `instance` matchers: ingress accounting and forwarded FileSource work must be
  summed across the same deployment-wide pod set.

Pending calibration:

- Measure renderer-thread CPU and same-render non-CPU wall directly; retain
  two-window fusion as an independent validation cross-check.
- Validate the imported distributions against production end-to-end latency;
  do not feed end-to-end latency back as service time.
- Split style setup and renderer rebuild further only if those measurements
  materially change conclusions.
- Re-run baseline and permit sweeps after calibration.

## 13. Repository Layout

```text
crates/biei-core/src/              # production core and public traits
specs/biei-spec.md                 # production contracts
issues/mln-rs-wishlist.md
sims/biei-sim/
    |-- README.md                  # all simulator documentation
    `-- src/
        |-- main.rs
        |-- lib.rs
        |-- calibration.rs
        |-- calibration_runner.rs
        |-- calibrated_costs.rs
        |-- config.rs
        |-- churn.rs
        |-- harness.rs
        |-- workload.rs
        |-- metrics.rs
        |-- report.rs
        |-- visualization.rs
        |-- visualization.html
        |-- stub_renderer.rs
        |-- channel_transport.rs
        `-- chitchat_bus.rs
```

The workspace root owns the lockfile and shared dependency versions.

## 14. Production Counterparts

### Renderer

Production prepares style and TileJSON outside worker admission, then renders
with dedicated-thread MapLibre actors. Tile/glyph/sprite bytes use process-wide
Rust Network and Database FileSources. The simulator preserves the trait and
routing boundaries but models work with sleeps.

Bounded actor abandonment and replacement after a wedged native render is a
production recovery mechanism. Add a simulator failure/recovery model only if
measured wedge rates affect capacity planning.

### Gossip

Production membership uses each process's chitchat node state and UDP transport.
The simulator hosts multiple chitchat instances in one process. KV semantics
are shared, and churn plans exercise dynamic handle creation and shutdown.
Failure-detector timing remains chitchat's implementation rather than a custom
simulator model.

### Transport

Production uses an internal HTTP listener and a framed raw-image response. The
simulator uses in-process calls but preserves `WireTask`, route tier, drain hint,
retryable rejection, and one-hop semantics.

### Deployment

Containers, Kubernetes resources, structured logs, request-id correlation, and
Prometheus metrics are production concerns. The simulator remains a local
research and regression tool.

## 15. Future Experiments

Keep extensions reactive unless evidence demands prediction:

- Collapse observed concurrent source fetches.
- Evict an old source version after observing a new visible version.
- Evaluate independent source HRW only if style affinity is insufficient.
- Briefly cache repeatedly observed one-shot identities when that does not
  pollute reusable source state.
- Drive churn plans from an autoscaling controller fed by measured queue and
  rejection signals. Current plans replay explicit request-indexed decisions.
- Add EWMA demand estimates only if allocation-count and activity tie-breakers
  fail under real traffic.

Predictive prewarming and scheduled prefetch are outside the current model.

## Appendix A: Terms

| Term | Meaning |
|---|---|
| renderer slot | one worker capable of holding one active renderer/profile warm |
| execution permit | node-wide concurrent setup/render allowance |
| native-render permit | `renderStill` residency allowance, including resource waits |
| CPU core | simulated service capacity consumed only by render/encode CPU work |
| warm profile | exact `WorkerProfile` already loaded in a slot |
| cold start | first task after a profile setup or renderer rebuild |
| HRW | Highest Random Weight / rendezvous hashing |
| BL | bounded loads soft queue capacity |
| drain | finish queued work before repurposing a slot |
| elastic expansion | enlist a fresh slot before warm queues hit hard capacity |
| one-shot source | non-reusable request-specific source |
| render output cache | node-local encoded-image cache |

## Appendix B: Equations

```text
P_w                    = C + I_w
P_f                    = C + I_f
BL soft capacity       = min(S / P_w, L / P_w - 1)
required warm slots    = ceil(lambda_profile * P_w)
warm coverage          = sum(required slots) <= total renderer slots
native concurrency     = sum(lambda_profile * E[P_state]) <= native render permits
CPU throughput         = sum(lambda_profile * E[C]) <= total CPU cores
drain acceptance       = now + estimated drain/setup/render <= task deadline
```

Here `S` is setup cost, `C` is CPU service, `I_w`/`I_f` are warm/first-render
resource critical-path waits, `L` is the SLA, and `lambda` is the arrival rate.
The equations are comparative models until production distributions are
imported and validated.
