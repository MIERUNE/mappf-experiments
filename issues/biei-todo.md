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
- **Decide between three responses, with measurement first:** raise the default budget for a cold render only (a warm render needs nothing near 5 s); pre-warm configured styles at startup and after a refresh hint, which converts the timeout into background work but adds a startup cost proportional to catalog size; or accept it and document that the first request after expiry may fail. Attribute the 5 s first: `biei_render_duration_seconds` with the style-setup and profile-preparation calibration histograms already separate profile I/O from render+encode, so the split between glyph fetches and native setup is measurable without new instrumentation.
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
