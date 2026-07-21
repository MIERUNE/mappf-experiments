# Refactoring Plan

This document records the remaining refactoring candidates identified after the Biei/Ishikari structural pass. The decision queue determines whether work should start; catalog length does not imply priority.

The goal is not mechanical symmetry or smaller files by itself. The goal is to:

- keep domain policy in `biei-core` / `ishikari-core`;
- keep production composition and HTTP concerns in the server crates;
- make production and simulation consume the same resolved policies;
- make invalid runtime states unrepresentable where practical;
- share code through `mmpf-*` only when there are at least two real consumers;
- avoid callback-heavy generic frameworks;
- preserve domain-required differences between Biei and Ishikari.

## Scope and constraints

- Do not change `internal_transport` implementation or behavior as part of the refactors below unless a task explicitly includes it.
- Preserve public HTTP, wire, metric, JSON, report, and CLI contracts unless a task explicitly documents a migration.
- Treat correctness and simulation-fidelity issues as higher priority than file splitting.
- Keep each change independently testable and reviewable.

## Security/performance review markers

These markers are review gates, not claims that every marked item is an exploitable vulnerability:

- **[SEC]** changes a trust, integrity, or supply-chain boundary.
- **[PERF]** materially changes latency, throughput, memory, I/O, or task cardinality.
- **[SEC↔PERF]** improves one dimension by spending budget in the other; its stated bound and
  overload/failure behavior are part of correctness.
- **[GUARDRAIL]** is a condition that must remain true while optimizing; an implementation that
  misses it should not merge even if benchmarks improve.

Combining **[SEC][PERF]** without the arrow means both dimensions matter but no inherent tradeoff
has been justified: preserve the security boundary and obtain the performance benefit inside it.
Availability under attacker- or tenant-controlled cardinality counts as security here. Pure
simulator fidelity and ordinary code cleanliness do not.

# Decision queue

This section is the source of truth for priority. Risk notes under individual items describe how to
implement them safely; they do not independently promote an item. Delete completed items rather than
keeping an archive of finished work here.

## Active priority

| Item | Why it is active | Next decision |
|---|---|---|
| **#41 [SEC↔PERF]** | Unbounded transient memory can exhaust the pod despite bounded cache weights. | Measure distinct-key peak RSS, then add only the byte admission demonstrated necessary. |
| **#108 [PERF] — IN PROGRESS (Codex)** | FileSource allocates strings for numeric/date HTTP headers that are parsed and immediately discarded. | Borrow header text for parsing and allocate only representation metadata that must be retained. |

## Evidence-gated catalog

| Gate | Items | Start only when |
|---|---|---|
| Named calibration or capacity experiment | **#7 [PERF][GUARDRAIL], #38, #63-#68, #88, #90, #92** | The experiment needs the fidelity or provenance change and defines how to validate it. |
| Profile, incident, or concrete capacity need | **#11 [SEC][PERF][GUARDRAIL], #30, #33 [SEC][PERF][GUARDRAIL], #106 [SEC][PERF][GUARDRAIL]** | Measured production evidence justifies contract, observability, or concurrency complexity. |
| Archive lifecycle decision | **#59 [SEC↔PERF]** | The deployment explicitly supports replacement at stable object keys; otherwise enforce immutable versioned keys instead. |

No other item is active. Simulator fidelity does not gain production security priority merely because
it can affect performance reports. For #30 and #33, identity and isolation are guardrails—not knobs
to relax for speed.

# Active P1 work

## 41. Bound aggregate Ishikari working memory

### Problem

The remaining RSS headroom is not protected by one aggregate admission boundary:

- distinct peer responses are individually bounded but have no process-wide byte reservation;
- PMTiles directory and metadata decompression is bounded per operation but not across concurrent keys;
- CPU-work admission counts jobs after MLT, style, or DEM/terrain inputs may already be loaded;
- the default count-only CPU queue can therefore retain much more input memory than the pod limit.

Relevant files:

- `crates/ishikari-core/src/pmtiles/reader.rs`
- `crates/ishikari-core/src/storage/chunked_store/`
- `servers/ishikari/src/internal_transport.rs`
- `servers/ishikari/src/server/cpu_work.rs`
- `servers/ishikari/src/server/tileset/mlt.rs`
- `servers/ishikari/src/server/tileset/terrain/generation.rs`
- `servers/ishikari/src/server/state.rs`

### Refactor

Introduce explicit byte reservations for peer bodies and decompression/CPU working sets, or move admission before loading inputs when a conservative reservation is possible.

Keep cache weight, transient working memory, runtime reserve, and safety margin as separate concepts; do not claim a precise RSS ceiling from Moka weights or double-counted `Bytes`/`Arc` allocations.

Ishikari already enforces Mapterhorn's published 512px source dimension before
RGB decode; `mmpf-terrain` keeps its wider 2048px generic safety ceiling behind
an explicit caller-selected limit. Preserve that source-specific boundary when
designing the remaining CPU working-set admission.

### Acceptance criteria

- Distinct peer responses cannot exceed a configured aggregate byte reservation.
- PMTiles decompression and queued CPU inputs consume bounded, cancellation-safe byte permits.
- A distinct-key, warm-cache peak-concurrency test stays below a documented cgroup threshold without OOM and releases all permits.

### Risk

High operational value and high implementation risk. Count-only admission is insufficient, but an overly conservative byte gate can reduce throughput or deadlock nested work. Acquire reservations in one documented order, release them on cancellation, and measure before tuning defaults.

**Mark: [SEC↔PERF] — P1.** Every accepted work item must have a finite memory claim, while overload sheds before retaining large inputs.

# Backlog catalog

The catalog is organized by domain for reference. The decision queue above controls execution order.

**Production and simulation fidelity**

## 7. Align Biei source handling and cache policy

### Problem

The source-cache configuration models different systems in production and simulation.

Production:

- ordinary tasks usually have no `TaskSpec.source`;
- `MapLibreRenderer::ensure_source` is effectively a no-op;
- addlayer caching happens in `AddLayerSourceCache` with a hard-coded capacity;
- worker affinity separately tracks addlayer source IDs with another hard-coded capacity.

Simulation:

- populates `TaskSpec.source`;
- executes `StubRenderer::ensure_source` with `source_load_cost`;
- uses the configured `source_cache_capacity`;
- does not follow production's `PreparedProfile.addlayer_source` affinity path.

Relevant files:

- `crates/biei-core/src/config.rs`
- `crates/biei-core/src/renderer/mod.rs`
- `crates/biei-core/src/worker.rs`
- `crates/biei-core/src/worker_pool.rs`
- `servers/biei/src/renderer/actor/addlayer.rs`
- `servers/biei/src/renderer/maplibre.rs`
- `sims/biei-sim/src/stub_renderer.rs`
- `sims/biei-sim/src/workload.rs`

### Refactor direction

Define one explicit source-work contract shared by production and simulation adapters:

- a stable source-affinity key;
- the cache capacity that actually governs source reuse;
- setup hit/miss observation;
- a clear execution stage and permit ownership.

Then:

- make configured source capacity control the actual production addlayer cache;
- stop making `WorkerPool` inspect the production-specific `PreparedProfile.addlayer_source` representation directly;
- let simulation use the same affinity/cache contract;
- remove or redefine the production no-op `Renderer::ensure_source` stage.

### Acceptance criteria

Add cross-adapter contract tests for:

- repeated source hit;
- capacity-one eviction;
- source-affinity worker choice;
- `source_loaded` outcome;
- source setup duration placement;
- equivalent production/simulation capacity semantics.

### Risk

High. This changes a core execution stage and should be implemented after the correctness fixes above.

**Mark: [PERF][GUARDRAIL].** The shared contract must retain a finite byte/entry capacity and
bounded affinity cardinality. Production/simulation equivalence is not permission to make the
production cache unbounded or to hold native-render permits across provider I/O.

---



**Ishikari domain types**

## 11. Replace the optional-field peer response bag

### Problem

`InternalFetchResponse` represents tile, provider, bootstrap, and leaf responses with one optional-field structure.

A peer tile response does not atomically carry complete tile representation metadata, so `ResourceResolver` may perform another header resolution after receiving valid bytes.

Relevant files:

- `crates/ishikari-core/src/storage/peer.rs`
- `crates/ishikari-core/src/storage/resolver.rs`

### Refactor

Use typed outcomes:

```rust
pub struct PeerTile {
    pub tile: TileData,
    pub source: InternalTileSource,
}
```

Bootstrap and leaf transfers can remain raw bytes or use their existing typed transfer structures.

### Acceptance criteria

- A successful peer tile does not require a second local header lookup.
- Invalid metadata combinations are unrepresentable.
- Tile content type and encoding survive peer resolution and simulation.
- Existing wire compatibility is preserved or explicitly versioned.

### Risk

Conditional. Implement only with evidence that the second header lookup is material or as part of a planned wire-version migration; otherwise the contract churn outweighs the benefit.

**Mark: [SEC][PERF].** Treat peer responses as untrusted structured input even on the internal
network: validate status, lengths, representation metadata, and cache policy before caching or
fallback. The performance win from avoiding a second lookup must not weaken wire validation.

---



**Singleflight and preparation**

## 30. Avoid fetching style material for an already-warm Biei worker

### Problem

`MapLibreProfilePreparer::prepare_profile` resolves style JSON before worker selection. Only later does execution determine whether the selected worker requires a profile swap. A warm renderer can therefore fail during provider outage because an otherwise unused style fetch/cache lookup failed.

Relevant files:

- `crates/biei-core/src/node.rs`
- `crates/biei-core/src/worker.rs`
- `crates/biei-core/src/worker_pool.rs`
- `servers/biei/src/renderer/maplibre/profile.rs`

### Refactor

Split preparation into:

- request-local material needed for every applicable request, such as addlayer resolution;
- swap-only style material fetched only when authoritative selected-worker state requires setup.

Use a reservation-backed execution plan or an equivalent race-safe boundary. Do not perform provider I/O while holding an inappropriate native-render permit.

### Acceptance criteria

- A warm worker renders after style-cache invalidation without contacting the provider.
- Cold starts and swaps still validate style material before native setup.
- Addlayer preparation remains available on warm renders.
- Profile-fetch metrics are recorded only when the fetch is actually required.

### Risk

High. Worker reservation and preparation ordering must remain race-safe.

**Mark: [SEC][PERF][GUARDRAIL].** Skipping provider I/O is valid only for an exact, immutable,
already-validated `StyleRevision` and compatible profile. An explicit purge/revocation must also
invalidate warm worker state; warmness must not authorize a different revision or profile.

---



## 33. Convert Ishikari duplicate-peer observation into peer singleflight

### Problem

`PeerBackend::fetch_from_peer` detects concurrent duplicate `(peer, path)` requests and records a metric, but every duplicate still performs a physical peer fetch and transfers the same response bytes.

Relevant file:

- `crates/ishikari-core/src/storage/peer.rs`

### Refactor

Use cancellation-safe singleflight keyed by peer identity and a typed internal request. Share completed bytes/metadata/errors with current followers, release the key on leader cancellation, and keep backoff updates tied to one physical attempt.

### Acceptance criteria

- N simultaneous identical peer requests produce one physical fetch.
- Different peers or typed requests do not share flights.
- Leader cancellation does not strand followers.
- Metrics distinguish physical attempts from joined requests.

### Risk

Medium. Shared error semantics and backoff accounting need explicit tests.

**Mark: [SEC][PERF][GUARDRAIL].** The flight key must include the complete typed representation
request and peer identity; never share credentials or request-scoped authorization. Bound
key/waiter/body retention, and do not let one peer's failure poison another peer's backoff domain.

---

**Simulator reporting**

## 38. Report effective Ishikari simulator configuration

### Problem

`ClusterConfig::validate` returns normalized `ResolverTuning`, but reports serialize the original raw `ClusterConfig`. A report may claim zero candidate count, tile-group size, fetch count, or backend concurrency while execution used one.

### Refactor

Resolve once into a serializable effective cluster configuration consumed by both real and modeled modes. Preserve requested values separately only when useful for diagnostics.

### Acceptance criteria

- Every reported execution-affecting value equals the value actually used.
- Requested versus effective normalization is explicit.
- Sweeps and ordinary reports use one effective vocabulary.

### Risk

Medium due to report-schema implications.



**Cache generation and freshness correctness**

## 59. Add archive-generation identity to Ishikari cache keys

### Problem

Ishikari tile, bootstrap, leaf, chunk, metadata, MLT, DEM, derived-output caches, and associated singleflight state are primarily keyed by logical tileset and location. Replacing an archive at a stable object key can mix bytes and derived values from different generations.

### Refactor

Introduce a narrow `ArchiveGeneration` derived from a trustworthy immutable version, object generation, or strong validator. Carry it through archive bootstrap and all archive-derived cache/flight keys. If the deployment contract instead requires immutable versioned paths, enforce and document that contract mechanically rather than relying on convention.

### Acceptance criteria

- One request cannot combine header, directory, tile, or derived data from different archive generations.
- Generation changes naturally isolate or invalidate every dependent cache and flight.
- The identity source is stable across nodes and is included in simulation/provenance where relevant.

### Risk

High when stable archive keys are overwriteable; medium implementation risk because identity crosses several cache layers.

**Mark: [SEC↔PERF].** This is a data-integrity boundary only when stable keys are mutable. Prefer
mechanically enforced immutable/versioned paths because they avoid per-request metadata I/O. If
replacement is supported, use a strong backend generation/version or validator—not timestamp,
length, or another collision-prone proxy—and include it before any cache lookup.

---

**Simulation methodology and provenance**

## 63. Generate true intra-interval Biei arrivals

### Problem

`sims/biei-sim/src/workload.rs::run_workload` samples a Poisson count per 1 ms tick and gives those requests the same timestamp. The process is Poisson in counts but packetized at millisecond boundaries, creating artificial microbursts and queue contention.

### Refactor

Either sample sorted uniform offsets within each interval, conditional on the interval count, or use a direct exponential inter-arrival clock. Preserve deterministic seeded replay and document timestamp precision.

### Acceptance criteria

- Multiple arrivals from one interval do not share a timestamp solely because of the generator.
- Aggregate rate and inter-arrival distribution pass deterministic statistical checks.
- Existing seed behavior is versioned if the generated trace changes.

### Risk

Medium. Results will legitimately change because the current workload shape is biased.

---

## 64. Make the Biei measurement window first-class

### Problem

`sims/biei-sim/src/workload.rs` and `sims/biei-sim/src/metrics.rs` derive throughput and utilization from the first measured arrival through the last successful completion. The denominator changes with queue drain, failures, and completion order, which makes scenarios difficult to compare.

### Refactor

Represent warmup, fixed measurement start/end, and post-window drain explicitly. Attribute arrivals to the measurement window, report completions and failures separately, and clip busy-time utilization to that window. Report drain duration as its own metric.

### Acceptance criteria

- Throughput and utilization use a declared fixed denominator.
- Work completing during drain is not silently counted as in-window capacity.
- Reports expose offered, admitted, completed, failed, and outstanding work at each boundary.

### Risk

High methodological value; existing benchmark numbers will not be directly comparable without a schema/version note.

---

## 65. Include Biei calibration profile content digests

### Problem

Calibrated reports identify profile paths and descriptive metadata but not the actual traffic and CPU-reference histogram contents. A file can change in place while producing an apparently equivalent report identity.

### Refactor

Compute stable cryptographic digests over canonical input bytes for every calibration profile. Include the digest algorithm and digest in report provenance; retain paths only as human-readable hints.

### Acceptance criteria

- Changing profile contents changes report identity even when the path is unchanged.
- Equivalent runs identify the exact traffic and CPU-reference artifacts used.
- Digest computation is streaming and bounded.

### Risk

Low.

**Mark: [SEC].** This protects experiment provenance, not a production request boundary. Use a
streaming cryptographic digest; the expected overhead is linear I/O with constant memory and should
not justify a weaker non-cryptographic identity.

---

## 66. Make `+Inf` calibration mass explicit

### Problem

`sims/biei-sim/src/calibrated_costs/histogram.rs` can map successful observations above the largest finite histogram bucket to that finite upper bound. This silently understates the tail and treats censored observations as exact.

### Refactor

Detect positive mass in the `+Inf` bucket that is not represented by a finite bucket. Reject such a profile by default or require an explicit, versioned tail model. Include the unmodeled mass in validation diagnostics and report provenance.

### Acceptance criteria

- Infinite-bucket mass is never silently assigned the largest finite latency.
- Calibration fails clearly or records the selected tail model.
- Tests cover no-tail, finite-tail, and malformed cumulative histograms.

### Risk

Medium. Some existing profiles may become invalid until regenerated with adequate buckets.

---

## 67. Unify Ishikari entry-assignment semantics

### Problem

Normal replay, churn, and sweep paths in `sims/ishikari-sim/src/workload.rs`, `churn.rs`, and `sweep.rs` do not use one clearly defined entry-node assignment rule. The same trace can therefore exercise different topology assumptions depending on the command.

### Refactor

Create one Ishikari-simulator assignment operation with explicit `preserve_recorded` and deterministic `reassign` modes. Define how unavailable recorded entries, node churn, seeds, and newly added nodes affect assignment. Reuse it in replay, churn, and sweeps without moving service routing into a generic simulator trait.

### Acceptance criteria

- Equivalent configuration produces identical entry assignments across all commands.
- Reports state assignment mode, seed, and fallback behavior.
- Recorded entry identities are never silently discarded.

### Risk

High experiment-comparability value; medium behavior-change risk.

---

## 68. Apply Ishikari churn events at their modeled boundary

### Problem

`sims/ishikari-sim/src/churn.rs` processes viewport batches as units. A request-indexed churn or sample event inside a batch can be applied only after the whole batch, shifting the modeled event and contaminating before/after measurements.

### Refactor

Split viewport batches at event and sampling boundaries, or introduce a distinct documented viewport clock and prohibit request-indexed events inside an indivisible batch. Prefer preserving batching only where it does not alter modeled chronology.

### Acceptance criteria

- Events occur at the declared request or viewport boundary.
- No request after a removal is processed under the pre-removal topology.
- Sample windows contain exactly the intended work.

### Risk

Medium. Correct splitting may change cache and concurrency behavior that previously depended on oversized batches.

---

**Simulation validity and deterministic replay**

## 88. Domain-separate simulator randomness

### Problem

Biei's workload RNG drives arrivals, style changes, style choice, source generation, and ingress-node selection. A renderer RNG likewise mixes setup, source, CPU, and wall-time sampling. Changing one scenario dimension consumes a different number of draws and changes supposedly unaffected traffic and cost samples.

Relevant files:

- `sims/biei-sim/src/workload.rs`
- `sims/biei-sim/src/stub_renderer.rs`

### Refactor

Derive deterministic streams or stable sample keys for each randomness domain. Service-cost samples should be keyed by stable task/stage/warm-state identity rather than mutable concurrent poll order where practical. Version the sampling scheme in provenance.

### Acceptance criteria

- Toggling source generation does not alter arrival times, style choices, or entry assignment.
- Routing changes do not change the cost sample for the same task and stage.
- Identical runs produce the same workload fingerprint and report apart from explicitly variable provenance.

### Risk

High comparative-validity value; medium behavior-change risk because seeded outputs will change.

---

## 90. Model CPU demand during Biei profile setup

### Problem

`sims/biei-sim/src/stub_renderer.rs::StubRenderer::setup_profile` models the entire setup as sleep and acquires no simulated CPU core. Production profile loading executes synchronous native style/renderer work on the actor thread. Style-heavy scenarios can therefore overlap setup without core contention and overstate cold-start capacity.

### Refactor

Represent profile setup as explicit CPU and non-CPU phases. Until direct attribution exists, require and record a conservative decomposition policy rather than silently treating setup as entirely non-CPU.

### Acceptance criteria

- Setup CPU portions serialize on a one-core model while non-CPU waits may overlap.
- Calibration provenance records the decomposition.
- A cold-start saturation fixture is compared with production measurements.

### Risk

High fidelity value; medium calibration-policy risk.

---

## 92. Verify Biei CPU-reference calibration isolation

### Problem

Biei calibration accepts operator-supplied `capture_concurrency = 1` as CPU-reference evidence, while export queries deployment-wide metrics and forbids pod/instance narrowing. The separate exercise and export commands have no machine-verifiable linkage, so ambient ingress can contaminate a nominally isolated window.

Relevant files:

- `sims/biei-sim/src/calibration.rs`
- `sims/biei-sim/src/calibrated_costs/derivation.rs`

### Refactor

Record expected ingress totals and outcomes, isolation mode, and observed deltas for CPU-reference windows. Reject CPU-reference use when ambient requests, failures, or timeouts prevent reconciliation. Do not add run identifiers to production metric labels.

### Acceptance criteria

- One extra ambient request invalidates a CPU-reference capture.
- Profiles contain expected and observed request totals and isolation evidence.
- Declared concurrency alone is insufficient proof.

### Risk

High calibration-correctness value; medium workflow risk.

---

## 106. Add distributed tracing only when aggregate observability is insufficient

### Problem

Prometheus aggregates and request-id logs cannot always reconstruct a render waterfall across Biei, the process-global FileSource, Ishikari, peer fetches, and object storage. Distributed tracing would add request-path work, new cardinality and data-exposure risks, and ambiguous parentage for single-flighted or background work.

### Refactor direction

Add optional, vendor-neutral OTLP tracing only after a named incident or measurement demonstrates that existing metrics and log correlation cannot answer the operational question. Preserve `X-Request-Id` for human correlation, propagate W3C `traceparent`/`tracestate`, represent shared single-flight work with span links, and allow background revalidation to outlive the initiating request.

### Acceptance criteria

- Exporters remain optional and disabled by default.
- Raw resource URLs, style ids, and other unbounded or attacker-controlled values are not span attributes.
- Tracing overhead is measured at representative tile and render QPS.
- Shared and detached work is represented without assigning it to an arbitrary request parent.

### Risk

Potentially useful for rare cross-service incidents, but low ROI without evidence. Instrumentation can consume CPU, allocate on hot paths, expose sensitive identifiers, and distort the latency being investigated.

**Mark: [SEC][PERF][GUARDRAIL].** Do not trade bounded labels, data minimization, or hot-path cost for speculative observability.

# Explicit non-goals

The following similarities should not be unified without new evidence:

- Biei and Ishikari drain controllers.
- Full membership adapters.
- A common Biei/Ishikari HRW implementation.
- A generic production/simulation cluster trait.
- Generic HTTP router construction.
- Generic cache wrappers across Biei, Ishikari, and FileSource caches.
- A generic versioned JSON artifact trait.
- A shared simulator report schema or visualization framework.
- A configurable cross-service identifier validator.
- Full metric-outcome visitors shared between production and simulation.
- Histogram and raw-sample quantile implementations under one abstraction.

# Validation expectations

For every slice:

```sh
cargo fmt --all -- --check
git diff --check
```

Run the narrowest relevant tests first, followed by affected package tests and strict Clippy:

```sh
cargo test -p <affected-package>
cargo clippy -p <affected-package> --all-targets -- \
  -D warnings \
  -D clippy::unchecked_time_subtraction \
  -D clippy::large_futures \
  -D clippy::large_stack_arrays \
  -D clippy::unused_async
```

For cross-crate policy or public contract changes, finish with:

```sh
cargo test --workspace --no-fail-fast
cargo clippy --workspace --all-targets -- \
  -D warnings \
  -D clippy::unchecked_time_subtraction \
  -D clippy::large_futures \
  -D clippy::large_stack_arrays \
  -D clippy::unused_async
```

These selected pedantic lints are blocking because their current baseline is clean and they guard panic, task-size, stack, and unnecessary async boundaries. Full `clippy::pedantic` remains an audit-only tool until its documentation, numeric-cast, and API-style backlog is reviewed rather than blanket-suppressed.

Do not use workspace `--all-features` for MapLibre builds on macOS: enabling the OpenGL backend there is unsupported. Use the platform-default feature set or the CI-specific feature matrix.
