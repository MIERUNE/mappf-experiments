# Biei Decision Queue

No Biei-specific implementation item is currently active. This queue records bounded planned work and evidence-gated product or operational triggers; it is not an ordered roadmap. Durable behavior belongs in [`../specs/biei-spec.md`](../specs/biei-spec.md), cross-cutting work belongs in [`refactor.md`](refactor.md), and missing upstream bindings belong in [`mln-rs-wishlist.md`](mln-rs-wishlist.md). Delete resolved entries; git history is the archive.

## Mutable-style decisions

The implemented contract — content-derived revisions, activation, the refresh receiver, fence semantics, hint idempotency, and convergence — is in [`../specs/biei-spec.md`](../specs/biei-spec.md) §8.3 and §9.1. What remains open:

- Add conditional revalidation. Ishikari emits a derived `ETag` over the exact rewritten bytes, so unchanged content should cost one `304` rather than a complete body transfer.
- Measure request-path revalidation latency. Move refresh into a bounded background stale-while-revalidate path only if production profiles justify the additional task and failure-state machinery.
- Decide whether a failed refresh may serve the last-known-good revision, for how long, and how deletion differs from a transient provider failure.
- Decide whether a future revision also covers resolved dependency identity. It currently covers the normalized style representation only.
- Measure the number and memory cost of successfully observed dynamic style ids before imposing a catalog limit. `observed` is deliberately process-lifetime revision authority: LRU eviction could resurrect the bootstrap identity or reject current/previous peer work. If cardinality becomes material, add an explicit configurable admission/catalog limit before provider fetch rather than silently evicting revision state.

## Cold-render latency against the request deadline

- **Observed in the live demo (2026-08-08):** the first static render of `carto/positron-gl-style` after its caches aged out returned `504` at 5.06 s, against the 5-second default SLA; five immediate repeats returned `200` in 0.14–0.25 s, and the other five configured styles were 0.23–0.50 s throughout. The pod recorded one `render`/`504`, so the deadline was reached inside Biei rather than at the Gateway. A cold style therefore spends most of a request budget on profile and glyph I/O, and the first caller after any cache expiry can absorb a user-visible timeout while every later caller is served from warm state.
- **Controlled cold-stack measurement (2026-08-12):** fresh Biei and Ishikari processes rendered `mierune/jp_mierune_gray` plus the weather overlay at `600x500@2x` in 4.06 s on the deployed images. Native render residency was 3.16 s and profile preparation was 0.52 s. The render requested 152 glyph ranges (29.0 MiB decoded) and 12 tiles (2.7 MiB); glyph requests accumulated 67.5 s of overlapped request time, including 24.6 s waiting for Biei's resource permits, while Ishikari accumulated 41.9 s across the same cold glyph requests. On separate fresh Biei processes backed by already-warm public delivery, removing all 64 symbol layers reduced native render residency from 2.44 s to 1.39 s. Glyph and symbol work is therefore a material cold-path cost, but the current images did not reproduce the earlier 10-second outlier; do not attribute that tail to tile geometry or change the SLA from this one sample.
- **Do not equate a request timeout with a dead renderer.** The current native call is not cancellable, but an overrun is not evidence that its actor is corrupt. Replace timeout-driven retirement with a quarantined-late-completion path: stop routing new work to the busy slot, return the client timeout, and make the same actor eligible again if the native call eventually returns successfully. Reserve actor replacement and orphan accounting for an actual actor exit, panic, failed recovery, or a separately bounded hard-wedge policy. This preserves the warm native cache and prevents one slow cold render from creating a `renderer_dead` tail for later requests.
- **Decide between three responses, with measurement first:** raise the default budget for a cold render only (a warm render needs nothing near 5 s); pre-warm configured styles at startup and after a refresh hint, which converts the timeout into background work but adds a startup cost proportional to catalog size; or accept it and document that the first request after expiry may fail. Attribute the 5 s first: `biei_render_duration_seconds` with the style-setup and profile-preparation calibration histograms already separate profile I/O from render+encode, so the split between glyph fetches and native setup is measurable without new instrumentation.
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

## Reproducible 504: a style's first render on a pod, while Ishikari is cold

Rolling Ishikari (Biei untouched) makes the next render for a style fail on the
response deadline before recovering:

    attempt 1  504  10.11s
    attempt 2  504  10.37s
    attempt 3  200   2.88s
    attempt 4  200   0.19s

The failing requests log the same pair, ~9.99s apart, with nothing in between:

    INFO  biei::renderer::maplibre::profile: style content changed;
          later requests use the new revision
          requested_version=1 observed_version=13085273775788304881
    WARN  biei::http::response: render request failed failure_kind=RenderTimeout

`requested_version=1` is *not* evidence of a revision reset. `style_content_version`
is a SHA-256 of the normalized style JSON with the high bit set, so it is stable
across an Ishikari restart, and `1` is the documented reserved sentinel for "no
content observed yet". Counting occurrences over ~8h of uptime: 2 on one pod, 0 on
the other, each a distinct style — consistent with the sentinel appearing on a
pod's first request for a style, which is expected, not a fault.

The fault is what it costs. That first preparation runs inside the render deadline,
so when Ishikari is also cold — exactly the case right after a rollout — it crosses
10s and the caller gets `504` instead of a slow `200`. Warm-Ishikari cold-Biei
renders measured 0.5-6.4s and all succeeded, so neither cold path alone is fatal;
the combination is.

The fix is deadline separation, not revision handling: preparation should degrade to
a slow render, not a response failure. Until then, every Ishikari rollout costs the
first request per (pod, style) a `504`.

Reproduce: `kubectl rollout restart deploy/ishikari -n map-demo`, wait for Ready,
then request a style that pod has not served yet.
