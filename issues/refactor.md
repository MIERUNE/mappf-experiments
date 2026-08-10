# Refactoring Priorities

This is an execution queue, not an archive of every plausible improvement. The decision queue determines whether work may start; catalog length does not imply priority.

Completed items are deleted. Git history is the archive. Temporary agent-owned slices may appear while implementation is active, but they must be removed after validation and do not need durable catalog entries.

## Working principles

- Keep domain policy in `biei-core` and `ishikari-core`.
- Keep process composition, platform integration, and HTTP policy in server crates.
- Share code through `mmpf-*` only when there are at least two real consumers.
- Preserve production/simulation equivalence only where an experiment depends on it; do not build generic simulator frameworks.
- Preserve public HTTP, wire, metric, JSON, report, and CLI contracts unless a task explicitly includes a migration.
- Prefer measured removal of hot-path work over speculative caching, coalescing, or admission layers.
- Do not split files or unify services merely for symmetry.
- Keep each accepted change independently testable and reviewable.

## Review markers

- **[SEC]** changes a trust, integrity, or supply-chain boundary.
- **[PERF]** materially changes latency, throughput, memory, I/O, or task cardinality.
- **[SEC↔PERF]** spends one budget to protect the other; bounds and overload behavior are part of correctness.
- **[GUARDRAIL]** names a condition that an optimization must preserve.

Availability under attacker- or tenant-controlled cardinality counts as security. Simulator fidelity and ordinary code cleanliness do not gain security priority merely because they can affect a performance report.

# Decision queue

This section is the source of truth. Detailed catalog notes explain a candidate but do not authorize implementation.

| Class | Items | Policy |
| --- | --- | --- |
| **Active measurement** | **#41 [SEC↔PERF]** | Measure first. Add only the admission boundary demonstrated necessary. |
| **Active deployment validation** | **#109 [GUARDRAIL]** | Termination-test Biei's implemented shared shutdown deadline under in-flight work. |
| **Before the next affected simulator study** | **#38, #64, #65, #66, #67, #68, #88, #92** | Activate only when a named experiment depends on the affected report, chronology, provenance, or comparison. |
| **Named fidelity study only** | **#7 [PERF][GUARDRAIL], #63, #90** | Do not improve fidelity in the abstract; define the decision and validation data first. |
| **Production evidence required** | **#11 [SEC][PERF][GUARDRAIL], #30 [SEC][PERF][GUARDRAIL], #33 [SEC][PERF][GUARDRAIL], #106 [SEC][PERF][GUARDRAIL]** | Require a profile, incident, or concrete capacity/diagnostic failure. |
| **Before the next GKE capacity decision** | **#110 [PERF][GUARDRAIL]** | Make the effective CPU request and HPA denominator explicit before tuning cost or scaling. |
| **Before the next protocol epoch bump** | **#111 [GUARDRAIL]** | Validate rollout behavior when old and new gossip clusters intentionally cannot communicate. |
| **Before production hardening** | **#112 [SEC], #113 [SEC↔PERF]** | Apply least privilege to metrics collection and make the public edge abuse boundary auditable. |

Only #41 and the deployment validation for #109 are active.

**Bottom line:** the code audit found no hidden implementation project behind the deferred entries. #109's manifest and process-deadline corrections have landed; only a controlled deployment termination test remains. The other observations are lifecycle gates, not permission to redesign membership, autoscaling, or listeners immediately. An otherwise short active queue is the correct steady state, not a gap to fill.

# Active work

## 41. Measure and bound aggregate Ishikari working memory

**Status:** P1 measurement. A broad byte-admission implementation is **not** pre-approved.

### Why this remains active

Cache weights do not bound all transient RSS. Distinct peer bodies, PMTiles directory/metadata decompression, and queued MLT or terrain inputs can overlap outside one aggregate reservation. Count-only CPU admission can therefore retain materially different byte volumes for the same job count.

The post-deployment idle snapshot was not alarming, but idle RSS cannot validate distinct-key peak concurrency. Adding conservative permits everywhere without that measurement could reduce throughput or introduce nested-permit deadlocks.

### Next step

Run a reproducible warm-cache, distinct-key concurrency sweep at the deployed limits. Measure peak cgroup RSS separately for:

1. stored-tile and chunk reads;
2. peer response bodies;
3. bootstrap/leaf decompression;
4. MLT transcoding;
5. DEM decode and terrain generation.

Record cache weights, active request counts, input/output sizes, queue depth, and the runtime reserve. Attribute retained bytes before choosing a reservation point.

### Implement only if demonstrated

- Move admission before loading a large input when its size can be bounded conservatively; otherwise reserve bytes around the retained working set.
- Add one documented acquisition order and cancellation-safe release path.
- Keep cache weight, transient working memory, allocator/runtime reserve, and safety margin as separate concepts.
- Preserve Ishikari's explicit 512px Mapterhorn source limit; do not size service admission from the reusable decoder's wider generic ceiling.

### Acceptance

- The measurement is repeatable and identifies which working set threatens the 2 GiB pod limit.
- Any added gate protects that demonstrated budget without materially regressing representative throughput.
- Overload sheds before retaining the large input, and cancellation releases every permit.
- Do not claim a precise RSS ceiling from Moka weights or double-counted `Bytes`/`Arc` allocations.

**Opinion:** this is the only active code candidate with plausible near-term production value, but the measurement—not a new semaphore—is the current task.

## 109. Validate Biei's bounded shutdown under in-flight work

**Status:** implementation complete; P2 deployment validation remains.

### What landed

- The GKE overlay declares the real 25-second cap and uses a 3-second `preStop`.
- SIGTERM creates one 21-second monotonic application deadline, leaving one second for process/kubelet overhead.
- Drain publication, HTTP shutdown, drain coordination, worker join, and membership teardown are clipped by that deadline; workers preserve a final two-second membership reserve.
- Membership-owner waiting is bounded. Deadline expiry drops the owner, which still initiates Chitchat shutdown.
- Unit tests cover deadline propagation and clipping, and the rendered-manifest check asserts `preStop + application budget + reserve <= 25s`.

### Remaining validation

Run a controlled GKE termination with an in-flight render and peer membership activity. Confirm from pod events and logs that the pod publishes drain state, stops receiving new work, records clean join or bounded detach, attempts membership teardown, and exits without kubelet `SIGKILL` inside 25 seconds.

### Acceptance

- The controlled test exercises non-empty in-flight work rather than only an idle rollout.
- No new public work is admitted after drain begins.
- The pod exits inside 25 seconds without a platform force-kill.
- If a render exceeds its allotted worker window, logs show bounded detach rather than an aborted native render.

**Opinion:** keep this item only until the deployment test is recorded; no further lifecycle abstraction is justified.

# Deferred catalog

Deferred entries intentionally contain no implementation recipe. If a trigger occurs, rewrite the item from current code and evidence before starting work.

## Production contracts, deployment, and hot paths

### 110. Make Ishikari's effective GKE CPU request explicit

- **Trigger:** the next GKE cost, HPA, or capacity decision.
- **Outcome:** choose an explicit CPU request that Autopilot accepts unchanged so the manifest, billing request, and HPA utilization denominator describe the same capacity.
- **Guardrail:** measure the cost and scaling effect; do not copy the observed `308m` admission result into YAML as an unexplained magic value.
- **Opinion:** **next-touch, not standalone.** The current mutation is visible and stable, but capacity claims must use the effective value rather than the checked-in `200m`.

### 111. Validate rolling deployment across protocol epochs

- **Trigger:** before the next Biei or Ishikari gossip/internal-wire cluster-id bump.
- **Outcome:** a documented rollout test covers incompatible old/new clusters, bootstrap readiness, surge capacity, fail-open timing, and eventual two-peer convergence.
- **Guardrail:** never let incompatible epochs communicate merely to shorten rollout; isolation is the correctness boundary.
- **Opinion:** **required release procedure for epoch bumps, not a runtime refactor today.** The v1→v2 deployment behaved correctly but produced temporary readiness failures and required extra scheduling capacity.

### 112. Separate metrics collection from peer-handler reachability

- **Trigger:** before the GKE demo is treated as a hardened production deployment or shares a cluster with workloads outside one trusted administrative boundary.
- **Outcome:** place metrics on a dedicated listener/port or behind an authenticated metrics proxy so the GMP collector's NetworkPolicy permission cannot reach unauthenticated forwarding, provider, readiness, or other internal handlers on `9090`.
- **Guardrail:** keep all operational and peer routes off the public listener; do not add request-path auth or merge the services' router construction merely to split this port.
- **Opinion:** **worthwhile production hardening, not an immediate hot-path refactor.** Kubernetes NetworkPolicy cannot restrict HTTP paths, so the current shared port grants collectors more capability than scraping requires.

### 113. Make the public edge abuse boundary auditable

- **Trigger:** before either service is exposed as a production multi-tenant or Internet-facing deployment.
- **Outcome:** checked-in Gateway/CDN policy or an explicit external contract identifies tenant/IP request limits for expensive Biei entry/render routes and request/egress controls for Ishikari delivery, with observable rejection and usage signals.
- **Guardrail:** retain cheap bounded origin admission as defense in depth, but do not mistake a process-global semaphore, origin-local token bucket, authentication, or `Origin` binding for edge client isolation. CDN cache hits never reach origin controls.
- **Opinion:** **a production prerequisite, not an in-process rate-limiter project.** Prefer platform controls and alerts; add shared Rust machinery only if two real deployments demonstrate that need.

### 115. Accept a validated configuration file with a published schema

- **Trigger:** the first self-hosted deployment operated by someone other than us, or a deployment whose flag count makes reviewable configuration impractical.
- **Outcome:** one validated configuration document per service with a published schema usable for editor and CI validation, with defined precedence against the existing flags and environment variables.
- **Guardrail:** preserve the current CLI and environment contract; a file must not become a second source of truth that can silently disagree with an explicit flag. Validate the whole document and refuse to start on an invalid one rather than applying it partially. Secrets stay outside the file as references. Keep parsing in each server's composition root; do not build a shared configuration framework before two consumers prove the same shape.
- **Partial prototypes landed in both servers (2026-07); item stays open.** Each accepts a versioned TOML document, rejects unknown fields, applies explicit flags before file values, and emits a generated JSON Schema from the parsed type. Neither document can express a deployment on its own: listeners, routing and queue controls, source/style catalogs, and much of process sizing remain flag-only. Two exclusions are deliberate and constrain any completion: provider URL templates may embed a token, so admitting them would make the document a secret; and node identity and advertise addresses differ per replica, so a conventionally shared document must not carry them. Completing this item means defining a useful self-hosted configuration boundary, not simply widening the field list.
- **Selected stack (2026-07, does not authorize work):** `toml` for the document and `schemars` for the schema. TOML is already resolved in this workspace and needs no position on the post-`serde_yaml` fork question; adopt YAML instead only if operators must edit the file as a Kubernetes ConfigMap, and then prefer `serde-saphyr` at its 1.0 line over the unmaintained forks. Hand-write file/environment/flag precedence with `serde` and `clap`; do not adopt a layered-configuration crate. Keep schema dependencies behind the existing optional feature and emit artifacts from the dedicated CLI path, so served builds do not link schema generation.
- **Opinion:** **useful for a self-hosted deployment, but still gated on a real external operator.** Martin's schema-validated file plus discovery is a genuine operability advantage over flags; the risk here is precedence ambiguity, not parsing.

### 7. Align Biei source handling and cache policy

- **Trigger:** a named production/simulation experiment depends on equivalent add-layer source reuse and affinity semantics.
- **Outcome:** one explicit source-affinity/cache-observation contract consumed by production and simulation.
- **Guardrail:** bounded cache/affinity cardinality; no provider I/O while holding a native-render permit.
- **Opinion:** **probably avoid as a broad refactor.** Prefer the smallest adapter change needed by the experiment; do not create a generic source-work framework.

### 11. Replace the optional-field Ishikari peer response bag

- **Trigger:** measured duplicate header resolution is material, or a planned wire-version migration already requires response changes.
- **Outcome:** typed peer tile/provider/index outcomes with invalid metadata combinations unrepresentable.
- **Guardrail:** peer responses remain untrusted structured input; validate status, lengths, representation metadata, and cache policy before caching.
- **Opinion:** **defer.** Type purity alone does not justify wire and fallback churn.

### 30. Avoid fetching style material for an already-warm Biei worker

- **Trigger:** provider-outage or profile data shows meaningful warm-render failures or latency from unnecessary style resolution.
- **Outcome:** swap-only style material is fetched only after a race-safe worker decision; request-local add-layer preparation remains available.
- **Guardrail:** warmness applies only to the exact immutable `StyleRevision` and compatible profile; purge/revocation must invalidate warm state.
- **Opinion:** **potentially valuable but high risk.** Do not complicate reservation/preparation ordering without production evidence.

### 33. Add Ishikari peer-response singleflight

- **Trigger:** production metrics show material concurrent physical duplicates for the same typed `(peer, request)`.
- **Outcome:** one physical peer attempt with cancellation-safe followers and separate physical/joined metrics.
- **Guardrail:** complete typed key plus peer identity; bounded waiter/body retention; no cross-peer backoff poisoning or credential sharing.
- **Opinion:** **probably avoid.** Existing simulator evidence found very low peer-tile overlap; coordination cost is unjustified until production disproves it.

## Simulator and report validity

### 38. Report effective Ishikari simulator configuration

- **Trigger:** the next report or sweep uses normalized resolver values.
- **Outcome:** reports distinguish requested configuration from the effective values actually executed.
- **Guardrail:** real and modeled modes use the same effective vocabulary; schema changes are versioned.
- **Opinion:** **next-touch.** A report that can describe values different from execution should not be published as calibration evidence.

### 63. Generate true intra-interval Biei arrivals

- **Trigger:** a queueing study needs sub-millisecond arrival shape rather than per-millisecond Poisson counts.
- **Outcome:** deterministic exponential arrivals or sorted conditional offsets without artificial same-timestamp microbursts.
- **Guardrail:** preserve seeded replay and version the trace generator when outputs change.
- **Opinion:** **defer.** It is unnecessary for studies insensitive to sub-millisecond burst shape.

### 64. Make the Biei measurement window first-class

- **Trigger:** the next throughput/utilization comparison across scenarios.
- **Outcome:** explicit warmup, fixed measurement window, and post-window drain with offered/admitted/completed/failed/outstanding counts.
- **Guardrail:** do not count drain completion as in-window capacity; version incomparable report semantics.
- **Opinion:** **high-value next-touch.** This should precede any new capacity conclusion from Biei simulator reports.

### 65. Include Biei calibration profile content digests

- **Trigger:** the calibration/report schema is next changed or a calibrated result is published.
- **Outcome:** streaming cryptographic digests identify the exact traffic and CPU-reference artifacts, with paths retained only as hints.
- **Guardrail:** constant-memory hashing and an explicit digest algorithm.
- **Opinion:** **cheap next-touch improvement.** Do it with the next schema change rather than as a standalone project.

### 66. Make `+Inf` calibration mass explicit

- **Trigger:** an imported histogram contains successful observations beyond its largest finite bucket.
- **Outcome:** reject unrepresented tail mass or require an explicit, versioned tail model.
- **Guardrail:** never silently map censored `+Inf` mass to the largest finite latency.
- **Opinion:** **correctness requirement when triggered.** No urgency if current profiles prove zero unrepresented mass.

### 67. Unify Ishikari entry-assignment semantics

- **Trigger:** a study compares replay, churn, and sweep output or relies on recorded entry nodes.
- **Outcome:** explicit `preserve_recorded` and deterministic `reassign` modes with documented churn/fallback behavior.
- **Guardrail:** reports state mode, seed, and fallback; recorded identities are never silently discarded.
- **Opinion:** **next-touch before cross-command comparisons.** Avoid extracting a generic simulator routing trait.

### 68. Apply Ishikari churn events at their modeled boundary

- **Trigger:** a request-indexed churn or sample boundary can fall inside a viewport batch.
- **Outcome:** split batches at the boundary or define a viewport-clock contract that forbids such events.
- **Guardrail:** no post-removal request runs under the pre-removal topology; sample windows contain exactly their declared work.
- **Opinion:** **next-touch for churn claims.** Existing results should not imply request-level chronology if they use indivisible viewport batches.

### 88. Domain-separate simulator randomness

- **Trigger:** a sensitivity study expects one changed dimension to leave arrivals, style choice, entry assignment, or service-cost samples unchanged.
- **Outcome:** deterministic streams or stable sample keys per randomness domain, versioned in provenance.
- **Guardrail:** identical runs retain a stable workload fingerprint apart from explicitly variable provenance.
- **Opinion:** **valuable before serious sensitivity analysis, not before.** Seeded output changes are expected and must be versioned.

### 90. Model CPU demand during Biei profile setup

- **Trigger:** a style-heavy cold-start study has measured production evidence for CPU versus non-CPU setup time.
- **Outcome:** explicit setup phases that consume simulated CPU only for the measured CPU portion.
- **Guardrail:** do not invent a decomposition merely to make the simulator look more detailed.
- **Opinion:** **defer.** Uncalibrated extra fidelity is false precision.

### 92. Verify Biei CPU-reference calibration isolation

- **Trigger:** the next CPU-reference capture is used to publish or compare calibrated results.
- **Outcome:** expected ingress/outcomes and observed deltas prove that the capture window had no ambient contamination.
- **Guardrail:** declared concurrency alone is not evidence; do not add run ids to production metric labels.
- **Opinion:** **required before trusting new CPU-reference evidence.** This is workflow correctness, not optional polish.

## Observability

### 106. Add distributed tracing only when aggregate observability is insufficient

- **Trigger:** a named incident cannot be explained by Prometheus aggregates and request-id logs.
- **Outcome:** optional vendor-neutral OTLP tracing with W3C propagation, span links for shared work, and detached background revalidation.
- **Guardrail:** disabled by default; bounded attributes; no raw resource URLs, style ids, or attacker-controlled cardinality; measure hot-path overhead.
- **Opinion:** **probably avoid until an incident demands it.** Speculative tracing would add cost, allocation, and data-exposure risk to latency-sensitive paths.

# Explicit non-goals

Do not unify these without new evidence:

- Biei and Ishikari drain controllers or full membership adapters.
- Biei and Ishikari HRW implementations.
- Production and simulation behind one generic cluster trait.
- Generic HTTP router construction.
- Cache wrappers across Biei, Ishikari, and FileSource.
- Versioned JSON artifacts behind one generic trait.
- Simulator report schemas or visualization frameworks.
- Full metric-outcome visitors shared between production and simulation.

Also rejected on its own terms:

- **A resource catalog or capability-discovery endpoint on either service.** Built and removed in July 2026. A catalog earns its place only when it enumerates what a deployment serves, which is what Martin's `/catalog` does from its startup-resolved source manager. Neither service here can: Ishikari resolves tileset ids on demand against object-storage roots and never holds a list, and enumerating would need the prefix listing the design forbids. What remains without enumeration is the build's own capabilities and URL grammar — documentation served as JSON, which adds a route, an authorization decision, a cache policy, and a described API surface while giving a client no capability it did not already have from the docs. Per-tileset TileJSON and `style.json` already carry the addressing a map client actually consumes. Revisit only if a deployment gains an authoritative content index (see [`../specs/resource-layout-sketch.md`](../specs/resource-layout-sketch.md)); enumeration would then be the point, and the namespace-disclosure question under namespace-scoped grants would have to be answered first.
- Histogram and raw-sample quantile implementations under one abstraction.

# Validation expectations

For every accepted slice:

```sh
cargo fmt --all -- --check
git diff --check
cargo test -p <affected-package>
cargo clippy -p <affected-package> --all-targets -- \
  -D warnings \
  -D clippy::unchecked_time_subtraction \
  -D clippy::large_futures \
  -D clippy::large_stack_arrays \
  -D clippy::unused_async
```

For cross-crate policy or public-contract changes, finish with workspace tests and strict Clippy. Do not use workspace `--all-features` for MapLibre builds on macOS because the OpenGL backend is unsupported there.
