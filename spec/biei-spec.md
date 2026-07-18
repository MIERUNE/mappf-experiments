# Distributed Map Renderer: Production Specification

This document records the production-specific contracts and design decisions for
biei, including routing, bounded loads, HRW, worker-pool behavior, HTTP,
membership, MapLibre Native integration, resource loading, and operations.
Simulator commands, models, calibration workflows, experiments, and reports are
documented only in [`../biei-sim/README.md`](../biei-sim/README.md).

Tile rendering, static center/bounds/auto rendering, overlays, `addlayer`, HTTP
forwarding, chitchat membership, Rust-backed Network and Database FileSources,
and the rendered-image cache are implemented. Section 14 and Section 18 list
the remaining work. Rust types and defaults in the code take precedence over
examples in this document.

This is a current-state specification, not an implementation history. Statements
without an explicit "planned", "blocked", or "open" qualifier describe behavior
present in the current workspace. It was reconciled against `maplibre_native`
0.8.7 and the workspace manifests; code and tests remain authoritative when
the document falls behind.

## 1. Scope

### Goals

- Run the routing, bounded-load, and worker-pool algorithms with real MapLibre
  Native rendering and real network forwarding.
- Keep dispatcher, worker pool, HRW, domain types, and trait contracts in the
  production crate.
- Expose a static-image-style HTTP API and a rasterized tile API.
- Support both a single-node server and an explicitly enabled distributed
  cluster.

### Non-goals

- Multi-region or geography-aware routing.
- Owning CDN behavior, authentication, authorization, or tenant rate limiting.
- Provider-specific URL schemes or service APIs.
- Hiding unbounded native execution behind an unbounded number of replacement
  threads.

## 2. Core Design

### 2.1 Shared core and trait boundaries

`Renderer`, `GossipBus`, and `Transport` are the replacement boundaries.
`Dispatcher`, `WorkerPool`, `Node`, HRW, and shared types are production code in
the `biei` crate.

| Boundary | Production implementation |
|---|---|
| renderer | MapLibre actor on a dedicated OS thread |
| gossip | chitchat membership adapter |
| transport | internal HTTP forwarding |

The old `production`/`sim` feature split and the value-level `Mode` enum are not
part of the design. Cluster mode is a runtime decision made with `--cluster`.

### 2.2 maplibre-native-rs is an evolvable dependency

Do not treat the current Rust binding as an immutable constraint. Prefer a
general-purpose upstream API over a biei-specific workaround when functionality
properly belongs at the binding boundary.

Rules:

- Keep `MapLibreRenderer` as a thin adaptation layer.
- Keep the MapLibre Native ResourceLoader waterfall and replace its Network and
  Database leaves through the process-global Rust FileSource API.
- Put general source/layer/style operations and controlled C++ exception
  handling in maplibre-native-rs.
- Treat render cancellation as a native-engine limitation, not a Rust binding
  omission.
- Revisit renderer-scoped FileSources only if a real multi-tenant isolation
  requirement appears.

Unlanded binding needs live in `mln-rs-wishlist.md`.

### 2.3 Provider independence

biei resolves stable style and tileset identifiers through configured catalogs.
It does not implement provider-specific URL schemes. A provider must expose
normal HTTP(S) style, TileJSON, tile, glyph, and sprite resources.

Reusable patterns such as chitchat membership, HRW peer selection, HTTP
forwarding, and retryable/fatal transport errors are implementation patterns,
not reasons to couple biei to another service.

### 2.4 Workspace and dependencies

MapLibre Native, axum, reqwest, serde, chitchat, and the production runtime are
unconditional dependencies of `biei`. The codebase intentionally has no feature
matrix for product capabilities. The sole biei feature, `gl-opengl`, selects the
Linux/headless OpenGL backend at build time; macOS development uses the native
default backend.

Use an immutable git revision only while a required binding change is awaiting
a crates.io release. Return to a version dependency after release. Local path
patches are acceptable for development only.

Primary checks:

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --tests
```

## 3. Repository Layout

Production is the mainline, so shared and production code belongs under
`biei/src/`, not under a `production/` or `real/` namespace. Submodules are
used only where a boundary has grown enough to benefit from them.

```text
biei/
|-- Cargo.toml
|-- spec/
|   `-- production-spec.md
|-- issues/
|   `-- mln-rs-wishlist.md
|-- biei/
|   `-- src/
|       |-- server.rs               # lifecycle and shutdown
|       |-- runtime.rs              # assembly
|       |-- membership.rs
|       |-- node.rs
|       |-- dispatcher.rs
|       |-- worker_pool.rs
|       |-- worker.rs
|       |-- style_catalog.rs
|       |-- http/                   # public and internal HTTP boundaries
|       `-- renderer/               # actor, overlays, and FileSources
```

## 4. Domain Contracts

The concrete definitions in `biei/src/types.rs` and `biei/src/wire.rs` are the
source of truth.

### 4.1 InternalTask and WireTask

`InternalTask` is process-local and contains local `Instant` values. `WireTask`
is the node-to-node representation and must never carry a process-local clock.

| Concern | InternalTask | WireTask |
|---|---|---|
| correlation | `RequestId` | same `RequestId` |
| style | `StyleRevision` | `StyleRevision` |
| request | `RenderRequest` | `RenderRequest` |
| scale | `PixelRatio` | `Scale` |
| output | `ImageFormat` | `ImageFormat` |
| budget | `arrived_at` and `deadline` | `remaining_budget_ms` |
| forwarding | `forwarding_hops` | `forwarding_hops` |

The sender encodes only relative budgets; the receiver creates a new local
deadline from its own clock. `WireTask.remaining_budget_ms` reserves the
estimated outbound and return hops and bounds remote execution. The surrounding
`ForwardRequest.origin_response_budget_ms` separately bounds the sender's full
peer transaction — address resolution plus HTTP connect/response/body — against
its original deadline; reusing the smaller remote budget here
would make the origin abandon a response before the remote deadline. These are
estimates, not synchronized cross-process timestamps.

### 4.2 Style identity and worker profiles

- `StyleId` is a stable cluster-wide string.
- `StyleRevision { id, version }` invalidates stale style state and cache keys.
- `WorkerProfile { style, render_mode, scale }` is the unit of warmness,
  eviction, and routing.
- HRW uses stable style identity plus renderer shape; revision changes reload
  the style but do not intentionally reshuffle ownership.

`StyleCatalog::resolve_latest` is the normal ingress path. Explicit definitions
are inserted only through trusted configuration or administration. Template
resolution is computed without permanently inserting attacker-controlled style
ids into an unbounded map.

### 4.3 Render requests and scale

`RenderRequest` supports rasterized tiles and static images. Static positioning
is one of center, bounds, or automatic fit. `Scale` is the wire-safe `1x`/`2x`
enum; `PixelRatio` is the renderer-facing value.

Map mode and pixel ratio are fixed when an `ImageRenderer` is built. A worker
therefore rebuilds its renderer when those profile dimensions change. Map size
changes use `set_map_size` and do not require a rebuild.

### 4.4 Outcomes and rendering errors

`TaskOutcome` is the internal result. `ForwardResponse` carries the outcome and
optional rendered output inside Rust. `OutcomeHeader` is the wire metadata.

`RendererError` distinguishes style loading, style readiness, source loading,
render failure, timeout, and actor death. Errors that invalidate native loaded
state use one shared predicate so worker and actor state cannot disagree.

`CompletedInfo.worker_id` is optional. A render-cache hit does not invent a
pseudo-worker id.

### 4.5 Deadlines

- Reject before admission when too little budget remains to do useful work.
- Check the deadline at each worker stage.
- A native render cannot be preempted. If it returns after the deadline, report
  timeout and retire that actor.
- Forward retries do not create a new end-to-end budget.

## 5. Trait Boundaries

| Trait | Responsibility |
|---|---|
| `ProfilePreparer` | Fetch and validate style/TileJSON before worker admission |
| `Renderer` | Set up a profile, ensure an optional source, and render |
| `Transport` | Send `ForwardRequest` and await a result |
| `GossipBus` | Publish worker KVs and build a cluster view |

Dynamic dispatch remains intentional. These traits are used as `dyn` objects,
so replacing `async_trait` with native async trait methods is not useful until
the object-safety and ownership design changes.

## 6. Entry Points

`biei/src/main.rs` runs the production server through the library entry point.
There is no conditional dual-entry main.

## 7. HTTP Ingress

### 7.1 Routes

The public API supports namespaced style identifiers and both static-image and
tile requests. Render routes accept a variable-length style path. Classification
is suffix-aware and validates a possible `z/x/y` tile suffix, rather than treating
an arbitrary segment named `static` as sufficient evidence of a static route.

Representative shapes:

```text
/{namespace}/{style_id}/preview
/{style_path...}/static/[{overlay}/]{position}/{width}x{height}{@2x}[.{format}]
/{style_path...}/{z}/{x}/{y}{@2x}[.{format}]
```

The format suffix may be omitted. PNG, WebP, and JPEG are supported according
to the current parser and encoder implementation. Static-only query parameters
must not be parsed on the tile route.

`StyleId` is the stable path-derived identity, including namespace when the
configured catalog uses one.

### 7.2 Static positioning

- Center: longitude, latitude, zoom, bearing, and pitch.
- Bounds: west, south, east, and north.
- Auto: fit the overlay geometry.

Bounds and auto use MapLibre Native camera helpers. With auto and no explicit
padding, each side starts from five percent of the corresponding image
dimension. Pin extents are included in fit calculations so the icon, not only
its anchor coordinate, remains visible. Auto without any overlay is invalid.

### 7.3 Overlays and addlayer

Supported request overlays include encoded paths, GeoJSON, generated pins, and
one `addlayer` object.

The fixed overlay renderer uses data-driven styling:

- One shared GeoJSON source per overlay slot.
- At most one Fill, Line, Circle, and Symbol layer per slot.
- Feature properties carry stroke, fill, opacity, width, marker image id, and
  simplestyle values.
- Layer JSON expressions read those properties through MapLibre Native's JSON
  converter; biei does not maintain its own expression AST.
- Consecutive compatible paths can share a slot while preserving z-order.
- `_overlay_idx` and geometry-type filters separate overlays and geometry
  classes without splitting sources by style value.
- A request uses only the layer types its content needs.

The overlay count, feature count, coordinate count, JSON depth, and payload size
are hard-bounded. The current overlay limit is 64.

Pins are generated as 2x bitmaps and registered with a pixel ratio of 2. Their
shape, shadow, label placement, and black/white label contrast are handled in
Rust. Provider-specific built-in icon names are not supported. URL marker
images remain optional future work.

`addlayer` accepts a policy-validated style layer JSON object. The JSON path via
`AnyLayer::from_json_str` lets MapLibre Native parse paint/layout expressions,
filters, visibility, `source-layer`, and supported layer types.

The source may be:

- A string referencing an existing source in the base style.
- A vector source object whose `url` value is a biei `tileset_id`.

Direct HTTP(S) URLs are rejected. The tileset catalog resolves the id to a
TileJSON URL, fetches it before worker admission, validates it, and rewrites the
source to a concrete `tiles` source. Stable source ids support worker-local LRU
reuse and soft source affinity. Source affinity is a hint, never correctness
state.

`before_layer` repositions the request overlay band. Missing-layer validation
is limited by the current binding's introspection API. `setfilter` for an
existing base-style layer remains blocked on the binding operation tracked in
`mln-rs-wishlist.md`.

### 7.4 Input and resource limits

| Limit | Value / rule |
|---|---|
| public URI path and query | 8192 bytes |
| style id | 512 bytes |
| static width | 1920 logical pixels |
| static height | 1280 logical pixels |
| scale | 1x or 2x |
| tile size | fixed at 512 logical pixels |
| tile zoom | 0 through 31 |
| static center zoom | 0 through 24 |
| static pitch | 0 through 85 degrees |
| path points | 500 per path |
| GeoJSON features | 500 |
| GeoJSON coordinates | 5,000 |
| overlay items | 64 |
| addlayer JSON | 4096 bytes, depth at most 16 |
| internal forward request body | 10 MiB |
| internal forward response frame | 48 MiB |

Coordinates, tile bounds, image dimensions, formats, path style fields, and
polyline point counts are validated before entering the renderer.

### 7.5 Backpressure and abuse resistance

Public ingress has a semaphore derived from renderer slots and queue capacity.
Internal forwarding has a separate semaphore acquired before its bounded body
read, so forwarded work is not counted twice and fan-in cannot create an
unbounded number of buffered bodies or profile waiters. Queue saturation
returns 503 with `Retry-After` before additional work is created.

The service assumes adversarial high-cardinality misses are possible. Defenses:

- Reject malformed or over-complex input before native conversion.
- Do not accept arbitrary network resource URLs.
- Use bounded positive, negative, and single-flight caches for style and
  TileJSON preparation.
- Honor explicit upstream freshness for bounded 404/410 caching. Without
  explicit freshness, fabricate a short negative lifetime only for missing
  tiles; required glyph/sprite/style/source/image misses are not cached.
  Do not negative-cache transient transport failures or server errors.
- Bound render output cache weight and lifetime.
- Keep attacker-controlled identifiers out of metric labels.
- Rely on an outer gateway for tenant/IP rate limiting, while retaining local
  hard limits for configuration failures at that layer.

### 7.6 Response caching

Successful render outputs are cached in a node-local weighted cache. The key
includes style revision, render request, scale, format, and additional source
identity, but excludes task id, request id, deadline, and forwarding hop count.
Entries have a five-minute TTL because referenced tiles and data may change at
stable URLs even when the style revision does not.

Both direct ingress and forwarded requests check the same cache before worker
admission. Concurrent misses for one key are single-flighted. Waiters retain
their own deadlines. Only completed reusable outputs are inserted; one-shot
sources, rejected work, and failed work are not cached.

Remote successful results are inserted on the entry node as well as the render
node. Cache hits report `RouteTier::RenderCacheHit`, no worker id, real ingress
latency, and no synthetic native-render residency sample.

Successful HTTP responses carry `Cache-Control: public, max-age=3600`.
Application-generated ETags and public `If-None-Match`/304 handling are not
implemented. CDN or gateway validators may still operate outside biei.

### 7.7 Status mapping

| Condition | Status |
|---|---:|
| completed | 200 |
| unknown style / preview style absent | 404 |
| invalid request | 400 |
| queue full / no capacity / forwarding unavailable | 503 |
| service draining | 503 with `Retry-After` |
| deadline or render timeout | 504 |
| style/source provider unavailable | 502 |
| actor dead or internal invariant failure | 500 |

Public responses never expose provider URLs, credentials, or internal error
chains. They include a stable error code and request id; detailed sanitized
diagnostics belong in structured logs.

## 8. MapLibre Native Integration

### 8.1 ImageRenderer model

`ImageRenderer` is the rendering primitive. biei fetches style JSON in Rust,
loads it with `load_style_from_json`, lets the ResourceLoader waterfall obtain
tiles/glyphs/sprites through Rust FileSources, receives RGBA output, and encodes
PNG/WebP/JPEG in Rust.

`ImageRenderer` is thread-affine. It is constructed, mutated, rendered, and
dropped on one dedicated actor thread.

### 8.2 Actor lifecycle

Each renderer slot owns one actor and at most one active renderer. Tokio and the
actor communicate through bounded channels and oneshot replies.

The actor:

1. Builds the native renderer on its own thread.
2. Loads already prepared style JSON.
3. Rebuilds for mode or pixel-ratio changes.
4. Uses `set_map_size` for size-only changes.
5. Applies request-local overlays and addlayer state.
6. Renders and encodes output.
7. Cleans request-local state and reports typed errors.

Native rendering cannot be cancelled. When a reply exceeds its deadline, biei
queues `Retire` to the old actor, detaches it as a bounded orphan, and starts a
replacement immediately. If the old render returns, it observes `Retire` and
exits. Orphan count is bounded by renderer-slot count. If the orphan budget is
exhausted for a worker, or spawning its replacement fails, that slot becomes
unavailable. Every idle worker runs a one-second repair tick, so a finished
retiring actor is joined and replaced without requiring another admitted task.
Repeated repair attempts do not repeatedly increment replacement-exhaustion
accounting.

Renderer health has three states:

- `full`: every configured slot is available;
- `external_degraded`: capacity is missing while a regular-priority Rust
  FileSource request shows external evidence — either an active transient-failure
  retry, or an upstream attempt that has stayed in network I/O past a short
  threshold (a render can time out and cost its slot before its first HTTP
  attempt fails, so requiring retry evidence would briefly and wrongly look
  internal; the threshold keeps fast, healthy traffic from counting);
- `internal_unrecoverable`: capacity is missing without any such external
  evidence.

A retry guard covers attempts and backoff and is released on success, final
failure, or cancellation; the slow-attempt guard is promoted only after
admission plus the network threshold and hands off to the retry guard.
Low-priority background refreshes do not count as render-failure evidence. The
evidence signal is process-global and cannot be proven to be the cause of a
specific lost slot (mbgl's `FileSource` carries no requester identity), but
elapsed time is deliberately *not* used to reclassify the loss: restarting
cannot repair a provider outage and would discard warm cache, so
`external_degraded` is not time-bounded and remains ready and live. The
slow-attempt threshold — evidence only after real network delay — is instead
what keeps normal fast traffic from masking an internal renderer loss.
Render admission is per-slot, not whole-pod: `can_start_render` is true whenever
at least one slot is available, so a single lost slot never stops the remaining
healthy slots (including renders that only touch already-cached resources).
A genuine systemic outage still self-limits — cold renders wedge their slots
until the per-worker orphan budget is exhausted and admission finally closes.
The cache-hit path runs first; a public miss may be dispatched to a healthy peer
and its result cached locally, while a forwarded-destination miss requires an
available local slot and is shed retryably otherwise.
Admission is checked again immediately before worker/native dispatch because
renderer health can change while a forwarded body is buffered, the cluster
view is loaded, or profile I/O runs. A local route that loses admission at this
last boundary uses its remaining peer candidates when available. After remote
candidates are exhausted — whether they rejected retryably or all failed at the
transport — a healthy local renderer is used for overflow (gated on render
admission first, so a degraded renderer does no wasted profile I/O).
`internal_unrecoverable` fails readiness and liveness; autonomous repair gets
the ordinary Kubernetes probe grace before restart. This is direct runtime
evidence, not inference from scraped Prometheus rates. Ordinary saturation and
successfully replaced orphaning remain `full`.

A native segfault still kills the process. Version 1 relies on pod/process
restart and cluster failover. Subprocess isolation is a possible future design,
not current scope.

### 8.3 Profile preparation

Style JSON and TileJSON are fetched before worker selection and before render
permits are acquired. This prevents an absent or slow profile from occupying a
renderer slot.

Preparation provides:

- Bounded body reads under the request deadline.
- UTF-8 and JSON validation.
- Revision-keyed positive cache.
- Short bounded negative cache for deterministic failures.
- Single-flight fetch coalescing.
- Sanitized diagnostics.

A successful JSON syntax check does not guarantee native semantic acceptance.
Native style-load failures are briefly remembered and invalidate the rejected
positive JSON entry. After the negative-cache window, a repaired resource at
the same URL and lazy-template revision is fetched again.

### 8.4 Rust FileSources

biei registers process-global Network and Database FileSources before creating
renderers. The MapLibre Native ResourceLoader waterfall remains intact.

Network behavior:

- reqwest-based HTTP(S) fetching.
- Separate semaphore lanes for regular render-blocking requests and
  low-priority background refreshes; online/offline usage remains an observed
  request attribute rather than another admission lane.
- Body-download permits default to `max(24, 4 * render_permits)` and regular
  admission defaults to `max(64, 2 * body_permits)`. Body permits are
  operator-visible because they trade resource-fetch parallelism against
  bounded response-buffer memory; regular admission remains an expert knob.
- Per-attempt connect/transfer timeout starts after admission; semaphore and
  single-flight waiting do not consume the network-attempt timeout.
- Bounded body buffers and per-resource-kind size limits.
- Conditional requests, ranges, 206, native bodyless 304 responses,
  cache-control semantics, ETag, Last-Modified, Age, and Date handling. A 304
  remains bodyless across the maplibre-native-rs 0.8.7 bridge and is
  materialized only for the shared Rust cache.
- A 304 without new freshness metadata reuses `no-cache` semantics when
  required; otherwise it receives a short bounded freshness window to avoid a
  revalidation request on every lookup.
- A cacheable 2xx with no explicit expiry (no `max-age`/`s-maxage`, no
  `Expires`, no inherited freshness) receives RFC 9111 §4.2.2 heuristic
  freshness — a fraction of the time since `Last-Modified`, clamped to a bounded
  window, or a short default when there is no `Last-Modified`. It is never
  treated as fresh forever, so a resource with no cache headers is revalidated
  (strictly, before being served) instead of being reused on every render and
  baked into the render-output cache indefinitely.
- Short retry/backoff for transport errors, 429, and 5xx.
- Bounded 404/410 negative cache. Its lifetime honors `s-maxage`, `max-age`,
  `Age`, `Date`, and `Expires`, capped at 15 seconds; `no-cache`, zero
  freshness, volatile storage, and explicit Network-only refresh bypass it.
  Only tiles get a fabricated fallback lifetime when the upstream sends no
  freshness metadata (an empty tile is a routine 404); required resources
  (glyphs, sprites, style, source, image) are negative-cached only when the
  upstream explicitly supplies freshness, so a transient provider 404 during a
  rolling deploy cannot fabricate a broken-render window that outlives the
  outage.
- Cross-renderer single-flight within each priority lane.
- Correct gzip/deflate handling without forwarding stale encoding metadata.
- Public-address-only SSRF policy by default, including DNS and redirect
  validation; explicitly configured private hosts are the only exception. Keep
  that allowlist to the narrowest exact hosts possible: broad private-domain
  wildcards can expose unrelated internal services to untrusted resource URLs.

Database behavior:

- Process-wide weighted Moka memory cache shared by all renderer actors.
- Capacity controlled by `BIEI_MLN_RESOURCE_CACHE_BYTES`.
- No persistent disk cache yet.
- Network responses are stored directly before crossing the bridge. This avoids
  an extra FFI round trip and lets a bodyless native 304 refresh the
  materialized shared-cache entry without recopying the image/resource body.
- A fresh Database response is delivered once. The paired low-priority Network
  request waits until the explicit expiry (and minimum update interval), capped
  at five minutes. If the shared entry is still fresh at that cap, the request
  completes with a bodyless 304; otherwise it performs conditional refresh. It
  must not return the same cached body through a second MLN callback or retain a
  Tokio task for an arbitrarily long freshness lifetime.

Resource metrics distinguish FileSource lifecycle time, admission wait, actual
upstream HTTP attempt count/latency, deferred refreshes, bytes, in-flight work,
the current deferred-refresh sleeper count, single-flight roles, and Database
hit/miss/revalidate/bypass operations. Kind, priority, usage, and outcome labels
are bounded enums.

`maplibre_native` 0.8.7 preserves all FileSource response fields across the C++
bridge. The direct Rust cache remains part of the design because it provides
process-wide memory bounds and revalidation control, not because of a bridge
limitation.

#### FileSource performance regression protocol

Replacing both native leaves is a deliberate optimization boundary and must be
treated as a continuing regression risk. Compare the default loader
(`--disable-mln-file-sources`) and the Rust loader in separate processes with
the same style, request corpus, renderer-slot count, concurrency, and empty or
warm cache state. At minimum inspect:

- cold completion time and warm rendered-output-cache-miss latency;
- `biei_mln_resource_cache_total` hit/miss/revalidate mix;
- `biei_mln_resource_upstream_attempts_total` and upstream-attempt latency;
- admission wait, single-flight leader/waiter ratio, and deferred refreshes;
- actor timeout, replacement, orphan, and renderer-availability metrics.

An unexpired Database hit must not cause an immediate upstream HTTP attempt or
a duplicate resource callback. A cold burst must coalesce identical requests,
must not consume network timeout while waiting for admission, and must not
degrade all renderer slots. Resource kind, URL range, and tile identity must be
part of the cache key; volatile resources and `no-store`/`private` responses
must not enter the shared positive cache.

### 8.5 Remaining efficiency limits

Response bytes and network single-flight are shared process-wide. Parsed style
state, glyph/sprite atlases, tessellation, and native CPU/GPU resources are
still per renderer. Measure before proposing native sharing APIs.

The cold style path parses JSON once in Rust for error classification and again
in MapLibre Native. This is accepted until profiling proves it material.

### 8.6 Permanent engine constraints

- Renderer thread affinity.
- Build-time-fixed map mode and pixel ratio.
- No safe in-flight render cancellation.
- GeoJSON normalization may drop non-rendering metadata and extra dimensions.
- No provider-specific style, tile, sprite, or icon service behavior.
- No built-in implementation of biei's URL grammar.
- Screen-space attribution/badge composition is outside normal geographic
  layers and would require post-processing.
- Memory-pressure feedback during native rendering is limited.

These constraints belong here, not in the Rust binding wishlist.

## 9. Distributed Forwarding

### 9.1 Routing and failover

`Dispatcher` returns local work, a prioritized list of forwarding candidates,
or rejection. A candidate includes node id and an optional drain-worker hint.

Forwarding rules:

- Increment `forwarding_hops` once when the forwarding decision is committed.
- Retrying another candidate does not increment it again.
- The current maximum is one forwarding hop.
- Retry transport failures and retryable remote rejections such as queue full,
  no capacity, or drain too slow.
- Warm-tracking and HRW routes retain a bounded remote fallback behind a local
  primary and use it when local admission races with stale queue or renderer
  health state.
- Do not retry deadline exhaustion, invalid input, unknown style, or hop-limit
  errors.
- Stop when the caller's original budget is exhausted.

### 9.2 Internal HTTP API

The cluster-internal listener exposes:

```text
POST /_internal/forward
```

The JSON request contains `ForwardRequest { task: WireTask, route_tier,
drain_worker, origin_response_budget_ms }`. `X-Request-Id` is propagated and
returned. The origin rejects a response unless its task id, request id, style id,
and source-presence bit match the request; mismatched image bytes are never
returned or inserted into the render cache. Peer HTTP uses a direct client and
does not inherit environment proxy settings; internal payloads must remain
inside the cluster trust boundary. Kubernetes readiness does not gate this
direct gossip-address path, so `/_internal/forward` independently rejects
an output-cache miss whenever local native admission is unavailable; exact
hits and an already-running same-key single-flight may still complete. An
unframed 408, 429, or
5xx response is a retryable transport result and advances to the next bounded
candidate; malformed success responses and non-retryable 4xx responses remain
fatal protocol errors.

The response content type is `application/x-biei-forward-response` and the body
is framed as:

```text
[4-byte big-endian JSON length]
[OutcomeHeader JSON]
[raw image bytes, completed responses only]
```

Malformed frames, content-type mismatch, inconsistent status/outcome pairs, or
missing image format are fatal transport errors. Request bodies have a bounded
read timeout. Response frames are capped at 48 MiB and decoded into zero-copy
image slices.

### 9.3 Evolution

JSON metadata tolerates additive optional/defaulted fields and unknown fields.
Semantic or type-breaking changes require a coordinated cluster upgrade or a
parallel `/_internal/forward/v2` path and new MIME type. Do not rely on mixed
wire-incompatible versions during a rolling update.

## 10. Membership and Lifecycle

### 10.1 Membership

Production membership uses chitchat. It owns node identity, live/draining
state, advertise address, worker KVs, readiness, and conversion to
`ClusterView`.

Published worker state includes profile, queue depth, and renderer shape. A
node-level `renderer.accepting` key separately reports whether the process may
start new native renders. Explicit `false` nodes remain live and addressable
for exact output-cache hits and already selected/stale forwards, but every new
routing tier excludes them. A missing key is treated as `true` for rolling
compatibility with older nodes; an advertised malformed value fails closed.
The HTTP advertise address is a single `host:port` value. Wildcard bind
addresses must not be advertised in cluster mode.

`Node` uses a short-lived `Arc<ClusterView>` snapshot cache with single-flight
refresh and stale-while-refresh behavior. Peer advertise-address snapshots use
the same single-refresher/stale-reader policy. Normal request hits avoid
chitchat locking, KV parsing, and O(N) snapshot cloning.

Marked-for-deletion state is retained for five minutes as a provisional balance
between rolling-deploy safety and state growth. Draining/dead nodes are excluded
from routing before deletion.

### 10.2 Health endpoints

Cluster mode uses separate public and internal listeners:

| Listener | Endpoint | Meaning |
|---|---|---|
| public | `/livez` | liveness; fails for renderer loss without active external-failure evidence |
| public | `/readyz` | readiness; false while draining, internally unrecoverable, or gossip-unready during bootstrap only |
| internal | `/_internal/healthz` | same liveness decision as `/livez` |
| internal | `/_internal/readyz` | same readiness decision as `/readyz` |
| internal | `/_internal/metrics` | Prometheus text exposition |
| internal | `/_internal/forward` | peer forwarding inside the network trust boundary |

`external_degraded` remains ready/live so the endpoint stays eligible for the
cache-hit and healthy-peer-routing paths, while local native render admission
requires an available slot (per-slot, not whole-pod `full`). The output-cache
lookup must therefore precede that admission gate. An
`internal_unrecoverable` renderer fails both probes. Health reachability and
permission to create native work are deliberately separate predicates. Gossip
also publishes the latter predicate so healthy entry nodes stop selecting a
degraded peer after the bounded publish/view-cache convergence delay; a stale
selection is still safe because the destination rechecks admission and returns
a retryable capacity rejection on a cache miss.

The public listener rejects `/_internal/*` and `/metrics`. In single-node mode,
one combined listener serves the public probes and `/_internal/*`; forwarding
itself remains disabled.

### 10.3 Startup and shutdown

Startup:

1. Parse and validate configuration.
2. Register process-global FileSources.
3. Start membership and renderer slots.
4. Start separate public and internal listeners in cluster mode, or one combined
   listener in single-node mode.
5. Become ready only after required cluster state is available.

Cluster bootstrap with DNS seeds requires discovery of not-yet-ready peers;
Kubernetes headless services should publish not-ready addresses. The peer
requirement is bootstrap-only: a seeded node waits for a first peer before
reporting gossip-ready, but once any peer has been seen (or a bootstrap grace
elapses) it stays ready even if gossip later partitions or every peer
disappears. Rendering and the warm cache need no quorum, so a healthy node must
not remove itself from the Service on peer loss — that would turn a gossip
partition or a single co-scheduled peer outage into a self-inflicted outage.

Shutdown:

1. Mark the node draining and publish that state.
2. Stop accepting new public work through axum graceful shutdown.
3. Allow existing HTTP connections and in-flight tasks to finish within the
   drain grace period.
   Slow internal body reads have their own timeout, and the HTTP server drops
   remaining active connections after a bounded shutdown grace.
   The main server lifecycle also awaits the drain coordinator: a client can
   disconnect and let hyper drop its handler while the separately spawned,
   non-cancellable render still owns a drain permit, so listener completion
   alone is not proof that local work finished.
4. Let runtime ownership drop workers, membership, and actor resources as the
   server exits.
5. Exit even if a bounded orphan native thread cannot be joined.

Endpoint propagation delay is an orchestration concern. A deployment may use a
preStop delay before SIGTERM; biei should not silently spend an undocumented
portion of the application drain budget on a platform-specific sleep.

## 11. Internal Security Boundary

The internal listener has no application bearer token. A shared bearer secret
would not provide peer identity, replay protection, integrity, or encryption.

The trust boundary is the network layer: Kubernetes namespace and
NetworkPolicy, VPC/firewall rules, or a service mesh. If authenticated peer
identity is required, use mTLS/SPIFFE or a mesh rather than adding a partial
application token scheme.

## 12. Observability

Production metrics use the `prometheus` crate and a private registry. Histograms
cover the default five-second SLA and extend to a ten-second tail:

```text
0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5,
0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0 seconds
```

Metric families include:

- completed, rejected, and failed tasks by fixed bounded labels;
- end-to-end and native-render residency histograms. The historical
  `cpu_render` metric name covers `renderStill` wall time, including FileSource
  waits, and must not be interpreted as CPU service time;
- calibration histograms for render+encode duration by bounded render shape,
  style setup, worker source setup (including addlayer), and pre-worker profile
  preparation;
- style swaps, cold starts, source cache outcomes, forwarding outcomes, and
  overflow admission;
- deadline stage;
- rendered-output cache outcomes and single-flight state;
- resource FileSource requests, bytes, latency, admission/body-permit wait,
  in-flight body work, active retry sequences, promoted slow attempts, and
  cache state. `upstream_attempt_duration` and provisional slow-attempt evidence
  count only time while HTTP send/body-chunk futures are pending; lane admission,
  body-permit wait, and retry backoff have separate accounting and cannot be
  treated as provider latency;
- queue depth, loaded workers, membership size, permit usage, drain state, and
  actor health state/replacement/orphan counts.

Never use style id, URL, request id, or other attacker-controlled values as
metric labels.

The calibration metric families are:

- `biei_render_duration_seconds{scope,render_mode,scale,format,size,state}`;
- `biei_render_timeout_lower_bound_seconds{scope}`;
- `biei_style_setup_duration_seconds{scope,render_mode,scale,state}`;
- `biei_source_setup_duration_seconds{scope,render_mode,scale}`;
- `biei_profile_prepare_duration_seconds{outcome}`.

`size` is a finite physical-edge bucket (`le_256px`, `le_512px`, `le_1024px`,
`le_2048px`, or `gt_2048px`), and `state` is `warm`, `swap`, or `cold`.
These dimensions are intentionally bounded; style identity and exact image
dimensions are not labels.
`scope=ingress` produces one sample per public request and is the calibration
view used across a cluster. `scope=forwarded` observes execution on a receiving
peer and must not be added to ingress samples for the same request.
The timeout family is censored lower-bound evidence rather than a render-time
distribution; consumers must not treat successful-render samples as an
uncensored distribution when timeouts occurred.

End-to-end request latency already includes queueing and must not be interpreted
as renderer service time. Offline export and import procedures live in the
documentation linked at the top of this specification.

`RequestId` is propagated through `InternalTask`, `WireTask`, internal HTTP, and
response headers. Tracing spans include it as a structured field, allowing
cross-hop log correlation without requiring OpenTelemetry. OTel remains an
optional future export path.

## 13. Configuration

### Operator-facing settings

- public bind address and internal listener port;
- internal advertised address and gossip bind address;
- explicit `--cluster` intent and gossip seeds;
- style and tileset URL templates/catalog entries;
- end-to-end SLA budget (five seconds by default);
- bounded hard queue multiplier over the fixed one-task-per-slot soft limit;
- core count, which conservatively derives one execution and one native-render
  residency permit per core until a calibrated deployment profile justifies
  oversubscription;
- per-renderer addlayer-source cache capacity;
- rendered-output and MapLibre resource-cache capacities;
- MapLibre resource body-download concurrency;
- explicitly allowed private resource hosts;
- fallback native ambient-cache path, used only when the Rust FileSources are
  disabled for diagnostics;
- logging filter through `RUST_LOG`.

Hidden `--debug-renderer-slots`, `--debug-render-permits`,
`--debug-cpu-render-permits`, and `--mln-regular-permits` overrides exist for
experiments. Drain grace, HTTP shutdown grace, retry policy, and the
low-priority FileSource lane are code-owned constants. A hidden
`--disable-mln-file-sources` escape hatch exists for comparison and recovery,
not as a normal deployment mode.

In cluster mode, wildcard advertise addresses are invalid. A seed node is
represented by `--cluster` with an empty seed list, not by inferring cluster
intent from unrelated options.

### Internal constants

Keep retry micro-policy and overlay layer layout in code unless operators have a
demonstrated need to tune them. Uncalibrated execution/native-render permit
defaults do not oversubscribe cores, and production uses a soft queue bound of
one task per renderer slot instead of deriving BL from heuristic CPU-only
costs. The hard queue multiplier is bounded to `1..=4`; it bridges short bursts
but is not a substitute for render capacity. Hidden overrides exist for
controlled measurements, not as sizing evidence.

## 14. Implementation Status

| Area | Status | Remaining work |
|---|---|---|
| shared domain and routing core | complete | production measurements may tune costs |
| two-crate workspace and unconditional product capabilities | complete | `gl-opengl` remains a platform backend build feature |
| tile and static center/bounds/auto rendering | complete | broader regression corpus |
| path, GeoJSON, pins, DDS overlays | complete for current scope | URL markers and optional text-layer pin labels |
| addlayer | v0 complete | broader layer policy and distributed source-affinity evidence |
| setfilter on existing base layer | blocked | maplibre-native-rs binding operation |
| output cache and same-node single-flight | complete | public ETag/304 is not implemented |
| chitchat membership and HTTP forwarding | complete | wire-compatibility discipline during upgrades |
| actor timeout replacement | complete | long-running overload soak tests |
| Rust Network/Database FileSources | complete in-memory version | optional persistent cache only if measured useful |
| observability | production histograms complete | production validation, direct CPU/resource attribution, gossip-age metric, and optional OTel |
| deployment demo | available | Helm/production policy only when needed |

When a shared type or trait changes, run workspace tests. Local implementation
TODOs are appropriate for small, code-adjacent optimizations with a clear
trigger. Architecture, security boundaries, and operational constraints belong
in this document.

## 15. Production Sizing

Production follows these capacity-safety principles:

- Bounded-load safety and queue overflow bands.
- Proactive expansion near the bounded-load comfort threshold.
- Allocation-aware drain-and-swap with singleton protection.
- Separate warm renderer slots from execution permits.
- HRW affinity by stable profile identity.
- One-hop forwarding.

Measure production style reload, renderer rebuild, first-resource load, render,
encode, queue, and admission-wait timings before changing capacity defaults.
The portable deployment example scales on standard CPU utilization only; an
I/O-bound deployment must add queue/admission-wait scaling because provider
latency can grow queues while CPU remains low.

## 16. External Providers

Code remains independent and integrates through HTTP and style/TileJSON
contracts. Provider-specific availability, caching, and latency are not biei
throughput benchmarks.

Use a local or same-cluster fast provider for renderer and routing benchmarks.
Use public remote styles only for compatibility and resilience smoke tests.

## 17. Build and Packaging

The workspace owns one lockfile. `biei` carries all production dependencies.

CI should run build, test, and clippy for the workspace. Production container
builds must use the MapLibre Native-compatible Linux runtime and reproducible
dependency versions. Precompiled native artifacts currently require the tested
Ubuntu ABI baseline; changing the runtime distribution requires an actual
render smoke test, not only a successful link.

`maplibre_native` 0.8.7 includes the `NDEBUG` alignment across the native ABI
boundary, routes asynchronous FileSource completion through
`Scheduler::bindOnce()`, and preserves complete FileSource responses. biei
builds the bridge at the normal release optimization level; Linux AArch64
container smoke tests remain required when upgrading the native dependency.

## 18. Open Questions

1. Is process-level native crash recovery sufficient, or does a future version
   require subprocess isolation?
2. Which local fast source should be the standard throughput fixture?
3. Can chitchat expose a reliable per-peer gossip-age metric?
4. Does real traffic justify propagating per-render request context into the
   global FileSource callback beyond cancellation and global timeout?
5. Does persistent resource caching materially improve restart behavior enough
   to justify SQLite or another disk cache?
6. Does cold style JSON double parsing matter in measured setup profiles?

## Appendix: Main Data Flow

```text
Public request
  -> HTTP parse and catalog resolution
  -> InternalTask with local deadline
  -> rendered-output cache / same-key single-flight
  -> Dispatcher
       -> local WorkerPool
       -> HTTP ForwardRequest with WireTask
       -> rejection
  -> ProfilePreparer before worker admission
  -> dedicated MapLibre actor
  -> RenderOutput
  -> cache insertion
  -> public response

Internal forwarding response body:
  [u32 BE metadata length][OutcomeHeader JSON][raw image bytes]
```
