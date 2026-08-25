# Biei Decision Queue

No Biei-specific implementation item is currently active. This queue records bounded planned work and evidence-gated product or operational triggers; it is not an ordered roadmap. Durable behavior belongs in [`../specs/biei-spec.md`](../specs/biei-spec.md), cross-cutting work belongs in [`refactor.md`](refactor.md), and missing upstream bindings belong in [`mln-rs-wishlist.md`](mln-rs-wishlist.md). Delete resolved entries; git history is the archive.

## Mutable-style decisions

The implemented contract — content-derived revisions, activation, the refresh receiver, fence semantics, hint idempotency, and convergence — is in [`../specs/biei-spec.md`](../specs/biei-spec.md) §8.3 and §9.1. What remains open:

- Extend conditional revalidation to tileset JSON. Styles now revalidate with `If-None-Match` and reuse held bytes on `304`; the tileset fetch (`fetch_tileset_json_with_auth`, used by addlayer sources and base-source rewriting) still transfers the full body on every refresh even though Ishikari emits a derived `ETag` there too. Same shape as the style implementation.
- Style-layer stale-while-revalidate stays deliberately unimplemented: the background refresh has no requester to borrow `provider_bearer_token` from, so implementing it would breach the credential-partition contract; conditional `If-None-Match` revalidation already reduces the request-path cost to one bodyless round trip. Sub-resources honor SWR in the FileSource. The remaining availability gap is served-last-known-good on failed refresh (below), not SWR.
- Decide whether a failed refresh may serve the last-known-good revision, for how long, and how deletion differs from a transient provider failure.
- Decide whether a future revision also covers resolved dependency identity. It currently covers the normalized style representation only.
- Measure the number and memory cost of successfully observed dynamic style ids before imposing a catalog limit. `observed` is deliberately process-lifetime revision authority: LRU eviction could resurrect the bootstrap identity or reject current/previous peer work. If cardinality becomes material, add an explicit configurable admission/catalog limit before provider fetch rather than silently evicting revision state.

## Cold-render latency against the request deadline

- **Observed in the live demo (2026-08-08):** the first static render of `carto/positron-gl-style` after its caches aged out returned `504` at 5.06 s, against the 5-second default SLA; five immediate repeats returned `200` in 0.14–0.25 s, and the other five configured styles were 0.23–0.50 s throughout. The pod recorded one `render`/`504`, so the deadline was reached inside Biei rather than at the Gateway. A cold style therefore spends most of a request budget on profile and glyph I/O, and the first caller after any cache expiry can absorb a user-visible timeout while every later caller is served from warm state.
- **Controlled cold-stack measurement (2026-08-12):** fresh Biei and Ishikari processes rendered `mierune/jp_mierune_gray` plus the weather overlay at `600x500@2x` in 4.06 s on the deployed images. Native render residency was 3.16 s and profile preparation was 0.52 s. The render requested 152 glyph ranges (29.0 MiB decoded) and 12 tiles (2.7 MiB); glyph requests accumulated 67.5 s of overlapped request time, including 24.6 s waiting for Biei's resource permits, while Ishikari accumulated 41.9 s across the same cold glyph requests. On separate fresh Biei processes backed by already-warm public delivery, removing all 64 symbol layers reduced native render residency from 2.44 s to 1.39 s. Glyph and symbol work is therefore a material cold-path cost, but the current images did not reproduce the earlier 10-second outlier; do not attribute that tail to tile geometry or change the SLA from this one sample.
- **Concurrent cold-cache fault isolated (2026-08-17):** one isolated cold render completed consistently in about 3.4 s and all FileSource I/O finished within about 0.8 s. Two identical cold renders could instead leave one or both native renders waiting past 10 s even though normal I/O, body, and retry gauges were zero. The blocked run had glyph Network requests sleeping in `refresh_deferred_inflight`: another renderer populated the shared Database cache between their Database miss and Network request, and the Network leaf parked a render-blocking callback under the false assumption that this requester had received the Database body. The fault disappeared with the Rust resource cache disabled and with MapLibre Native's default loader. Deferred refresh is valid only when Database already delivered a usable body. A miss, or a stale body carried as `priorData` because native withheld it for revalidation, receives the newly cached fresh body immediately. Regression coverage as it actually stands: the race branch is exercised directly
against a real source and cache (`a_peer_cache_fill_is_served_to_every_requester_still_awaiting_bytes`),
and deleting the early return that serves the body makes it fail. There is **no**
concurrent cold-render probe in the suite, and none can be written today —
`ResourceRequest` is `#[non_exhaustive]` with a crate-private constructor upstream,
so `NetworkFileSource::request` cannot be driven from a test at all (tracked in
`issues/mln-rs-wishlist.md`). The two-identical-cold-renders reproduction remains a
manual procedure. Do not use this incident to justify a larger SLA or pre-warming.
- **Treat ordinary cold latency separately:** the remaining roughly 3–4 s cold cost is mostly native glyph decode, symbol layout, shader/raster work, and encoding after network completion. Consider predictive warming only if that latency is a product problem; it is no longer a mitigation for the resolved 10-second cache-callback fault.
- **Predictive warm handoff is planner-only.** `crates/biei-core/src/warm_plan.rs` accepts bounded recommendations pulled from other live nodes, reduces them to revision-independent `(style, mode, scale)` hints, resolves the current revision locally, and admits only the top two HRW owners. This intentionally treats peer advice as a replicated working-set hint: a primary teaches its likely successor early, while the receiver independently decides whether to spend work. No separate demand-history protocol is planned. The planner is not yet wired into runtime behaviour.
  - **Pull advice only while idle.** Add a versioned internal endpoint that returns the serving node's bounded, deduplicated loaded profiles. An idle node samples one live peer at a time in round-robin order with a short timeout; do not fan out to the whole cluster on every tick. The receiver unions only a small bounded window of replies, then recomputes HRW ownership and catalog revision locally. Advice is advisory, never a remote command.
  - **Warm only anonymous-access profiles in version one.** `WorkerView.loaded_profile` intentionally carries no credential or cache-partition identity. The executor must reconstruct current anonymous authorization locally and check local partition-aware renderer state before warming; credentials and partition identifiers never enter gossip.
  - **Run only from genuinely spare capacity.** Use a dedicated warm command that never waits in the normal worker queue, acquires drain admission, immediately reserves an otherwise-empty worker plus execution and native-render permits, and abandons the attempt if any prerequisite is unavailable. The shared FileSource still means an admitted warm can contend with a foreground request that arrives later; this is a bounded experimental cost to measure, not perfect isolation.
  - **Publish warmth only after success.** The normal pool predicts `loaded_profile` at dispatch time for routing. A warm command must instead keep the selected worker unavailable but unpublished until rendering succeeds, then publish the loaded profile; failure or timeout leaves it unloaded.
  - **Bound speculative work.** Permit at most one warm per node, add a global rate budget and provider-failure cooldown, and never evict an existing loaded profile in version one. HRW depth two bounds cluster replication, while the rate budget prevents repeated profile churn from becoming sustained background load.
  - **Keep deployment feedback visible.** Measure completions, useful completions followed by a foreground warm hit, skips by `WarmSkip`, foreground overlap, evictions, cold-latency change, and HPA replica changes. Ship behind a disabled-by-default flag; CPU-based HPA cannot distinguish warming from demand.
  - **Choose the warm camera experimentally.** A fixed mid-zoom camera is sufficient for the first canary, but the existing one-style sweep mainly measured shader coverage and says little about glyph coverage. Do not treat that viewport as a permanent platform default without production evidence.
- **Do not tune the readiness probe in response.** `failureThreshold: 1` with a 1-second timeout looked fragile against a CPU-saturating renderer, but `BIEI_CORES=2` under a 4-CPU limit leaves roughly two cores for the async runtime, so `/readyz` is not competing with render slots.

## Compatibility and product triggers

- **Flat tileset ids are globally shared by convention, not declaration.** Biei's
  addlayer authorization treats a namespace-less tileset id as a shared
  resource (like glyph ranges) because Abashiri's ownership model only covers
  namespaced tilesets. Reopen when Abashiri starts managing flat tilesets as
  owned resources; per-tileset grants stay out of scope — finer sharing is
  modeled by creating a namespace, never by adding resource ACLs to the
  registry snapshot.

- **Object-storage custom marker images (planned):** support managed marker images without accepting arbitrary request-supplied URLs. The request carries a bounded logical marker ID; deployment configuration resolves that ID through an object-storage root/template. Prefer a content-addressed immutable ID and object so decoded images and rendered outputs can be cached without an invalidation protocol. Before implementation:
  - choose the public overlay syntax and object layout; reproducing a third-party `url-*` overlay parameter is not a goal if it would expose an arbitrary fetch target;
  - decide whether Ishikari owns object-store access and serves the bounded asset to Biei (preferred, so Biei does not acquire content-store credentials) or whether a reusable provider abstraction justifies direct access;
  - reject path traversal and request-controlled schemes, authorities, query strings, or object-store options after template expansion;
  - define encoded-byte, decoded-dimension, total-pixel, format, and decode-time limits; do not enable SVG or other active/externally referencing formats;
  - reuse bounded cache and single-flight behavior, and include marker-fetch waiting in resource-I/O metrics and the render deadline;
  - include marker identity and rendering parameters in render-output cache keys while keeping coordinates and overlay ordering unchanged; and
  - test cold/warm fetches, duplicate marker reuse, missing/corrupt/oversized objects, scale behavior, auto-fit/anchor behavior, and mixed overlay z-order.
- **Text-layer pin labels:** add only for a concrete compatibility requirement. Current labels are intentionally rendered into request-local bitmaps.
- **Public ETag/304 handling:** add only if CDN or gateway validators are insufficient for measured traffic.
- **Standard throughput fixture:** choose a local, fast provider fixture before publishing reproducible throughput comparisons.

## Production security gates

- **Private-network resource authorities:** before accepting attacker-controlled style/resource URLs in a deployment that enables `BIEI_MLN_RESOURCE_PRIVATE_HOSTS`, replace host-only exceptions with exact allowed `(scheme, host, port)` authorities (or an equally narrow structured policy). The current exception permits every HTTP(S) port on an allowed private host; broad wildcard hosts expand that SSRF capability further.

## Operational evidence gates

- **Persistent FileSource cache:** require restart measurements showing enough benefit to justify disk state and invalidation policy.
- **Native subprocess isolation:** require evidence that process-level recovery is insufficient for observed MapLibre Native crashes.
- **Per-render FileSource context:** require a real diagnostic question that aggregate resource metrics, cancellation, and global timeouts cannot answer.
- **Per-peer gossip-age metric:** require an incident that existing membership and readiness signals cannot diagnose.
- **Cold style JSON parsing:** optimize only if setup profiles show material CPU or latency cost.
- **Orphan render memory accounting:** add byte-level orphan admission only if a slow-render or distinct-key measurement shows the count-bounded orphan pool can approach the pod memory limit. Orphans are bounded by count, not bytes (see biei-spec §8.2).
- **Production packaging:** add Helm or broader policy only if Biei moves beyond the current deployment-demo scope.

## The 504s are render-permit waiting, not preparation

Third and final correction of this symptom. The claim I carried for most of a
session — "profile preparation runs inside the response deadline, so it turns into
504s" — is not supported by the counters. Measured on the pod that produced the
failures:

    tasks_rejected_total{reason="deadline_exceeded"}                    5
      of which deadline_exceeded_total{stage="acquire_render_permit"}   3
    tasks_failed_by_kind_total{kind="render_timeout"}                   1   (9.95s)
    profile_prepare_duration_seconds{outcome="failure"}                 1   at 0.012s
    profile_prepare_duration_seconds{outcome="success"}   mean 1.3ms over 170

Preparation is fast in both the success and failure cases; the code already
separates `PreparationTimeout` from `RenderTimeout` for exactly this reason, and
production logged `RenderTimeout`. What actually consumes the deadline is queueing
for a render permit under capacity pressure, plus one genuinely slow native render.

This matters for the queue-multiplier question: raising `queue_capacity_multiplier`
3 -> 4 makes requests wait *longer* for the same permits, so it would increase
`deadline_exceeded_total{stage="acquire_render_permit"}` — a counter that is already
non-zero. Fewer `503`s would be bought with more `504`s, which are worse: no
`Retry-After`, and the queued work is discarded rather than never admitted.

Given the demo cluster accepts capacity shedding (see below), there is nothing to
fix here. If this workload were ever run for real, the lever is more renderer slots
or replicas, not a deeper queue, and the metric to watch is the permit-wait stage
rather than the 503 count.

## Accepted for the demo cluster: capacity shedding and Spot churn

Decided rather than deferred, so it is not re-investigated:

- `503 no_capacity` under a burst of distinct style profiles is the admission
  control working. Six renderer slots (3 per pod x 2 pods) cannot hold twelve warm
  profiles; shedding the excess with `Retry-After` is preferable to queueing it.
- Spot node reclaims periodically halve Biei capacity until a replacement is
  scheduled. Measured effect: one burst returned 12/12 `504` during a reclaim, and
  the identical burst on a stable cluster returned 12/12 `200` in 1.1-1.8s.
- The HPA scales on CPU at a 50% target and sits at ~1%, so it will not react to
  either. `maxReplicas: 6` is therefore unreachable under this workload.

None of these are fixed, on purpose: this is an experiment cluster, and the fixes
(more replicas, a shed-rate or queue-depth scaling signal, a deeper queue) all cost
money or latency for burst absorption the demo does not need.

The response deadline still bounds preparation, permit waiting, and rendering; a
capacity event may therefore produce `504` rather than an arbitrarily slow `200`.
That is the intended client contract. It no longer classifies the renderer as dead:
the slot is quarantined while the same native call continues, returns to service on
late success, and is replaced only after the separate hard-wedge deadline.

Measurement discipline, since ignoring the cause does not remove the noise: record
Biei pod readiness with every latency measurement. Three separate conclusions in one
session were confounded by not doing so, each of them plausible and wrong.
