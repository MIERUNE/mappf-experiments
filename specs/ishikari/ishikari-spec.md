# Ishikari Specification

Durable design contract for Ishikari: what it is, what it must not become, and
the invariants and module boundaries the implementation must uphold. Active work
items and open decisions live in `../issues/ishikari-todo.md`. Component-level
contracts are in `isoline-and-hillshade-spec.md` and `simulator-spec.md`.

## Positioning

Ishikari's primary purpose is efficient, low-cost delivery of PMTiles archives
stored in object storage. Its core product is PMTiles-backed TileJSON and tile
bytes over ordinary HTTP, with distributed cache locality and backend range-read
batching.

Style JSON, glyphs, sprites, preview pages, and renderer integration are
supporting provider features. Biei is the primary demo consumer, but Ishikari
must remain a standalone provider and must not grow renderer-specific routing or
worker concepts.

## Specification Evolution

These specifications capture the best-known design, not a performance ceiling
or a requirement to preserve a particular implementation technique. When a
different approach has well-founded evidence that it can outperform the
specified approach while preserving the public contract, correctness, safety,
and architectural guardrails, Ishikari should revise the specification and
adopt the better approach rather than treating the current technique as fixed.

Performance claims must be supported by reproducible measurements on
representative workloads. Evaluation must consider the relevant trade-offs
together, including latency, throughput, memory, backend requests and bytes,
network egress, and operating cost. Update the specification and regression
tests with the implementation; an optimization must not silently weaken an
existing contract merely because it wins one benchmark.

## Non-Goals and Guardrails

- Do not move Biei render routing into Ishikari.
- Do not make Ishikari aware of Biei worker slots, render permits, or render
  output caches.
- Do not require Biei or other consumers to understand PMTiles archives directly.
- Do not create a shared cluster crate until repeated cross-project reuse proves it is worth the abstraction cost.
- Do not put attacker-controlled `style_id` or `tileset_id` values in metric
  labels.
- Keep `/_internal/*`, including `/_internal/metrics`, on the cluster-internal
  listener (`ISKR_INTERNAL_HTTP_PORT`) only. Never route that port through a
  Service, Gateway, or Ingress, and keep the public listener returning 404 for
  those paths.
- Keep the headless gossip Service gossip-only; do not publish public HTTP
  `8080` there.
- Keep PMTiles parsing in `pmtiles/`, byte access and peer routing in `storage/`,
  and style/glyph/sprite provider logic outside PMTiles archive parsing.

## PMTiles Tile Delivery Contract

### Archive identity and HTTP behavior

- A tileset id identifies one PMTiles archive for the lifetime of cached data.
  Positive tiles, PMTiles directories, metadata, and backend chunks are treated
  as immutable and may remain cached until byte-capacity eviction. Replacing an
  archive under the same id is unsupported until an explicit invalidation
  contract exists; publish changed content under a versioned id instead.
- TileJSON is derived from the PMTiles header and metadata. Ordinary tile
  requests serve the archive's stored format and `Content-Encoding`. Explicit
  `.mlt` requests, or `Accept: application/vnd.maplibre-tile` where supported,
  may transcode stored MVT under the bounded CPU-work budget. Responses on a
  path that negotiates by `Accept` must emit `Vary: Accept`; the `.mlt` suffix
  remains the canonical cache-stable form.
- Public tile responses use `public, max-age=3600, s-maxage=86400,
  stale-while-revalidate=604800`. TileJSON uses `public, max-age=300,
  s-maxage=3600, stale-while-revalidate=86400`. These policies describe the
  immutable public representation and do not inherit object-storage metadata.
- Tile absence is cached internally for the configured short negative TTL;
  positive entries do not acquire that TTL. A negative hit must not be extended
  by reads, and transient backend or peer failures must not become authoritative
  absence. Public negative caching is opt-in per endpoint rather than implied by
  the positive tile policy.

### Distributed resolution and backend access

- Nodes use a stable HRW key per resource class so a converged membership view
  selects the same preferred owner. Peer unavailability may add fallback work
  or latency but must not change tile correctness; candidates are tried in score
  order before local object-storage fallback.
- The cache hierarchy has distinct jobs: tile bytes form a bounded near-entry
  and owner hot tier; PMTiles bootstrap/leaf data avoids repeated directory
  traversal; fixed-size backend chunks provide aggregate byte reuse around
  Hilbert-local tile requests. Cache placement may evolve only from measured
  latency, peer traffic, backend bytes, and effective aggregate capacity.
- Object-storage reads are aligned to chunks. Concurrent missing chunks may be
  merged into bounded range reads and consumers of pending or in-flight chunks
  share that work. The merge window is configurable per process through
  `ISKR_CHUNK_FETCH_MERGE_WINDOW_MS`, defaults to 10 ms, and accepts 0 as a
  no-intentional-wait baseline; bootstrap and capacity-release dispatches remain
  immediate. Correctness must not depend on a particular merge window, chunk
  size, or range cap; those are measured operating parameters.
- Memory, distinct admitted work, backend concurrency, peer requests, and
  CPU-heavy transformations remain bounded. Overload sheds work rather than
  creating an unbounded queue.

### Work coalescing principle

Coalesce equivalent work when the avoided cost and observed overlap justify the
coordination, cancellation, and tail-latency cost. Bootstrap, leaf, provider,
chunk, and derived-generation work currently meet that test. Entry-side peer
tile responses do not: a 50-VU, 3-node replay measured identical overlap in
only 0.23% of peer tile fetches. Re-evaluate from production measurements rather
than treating either coalescing or non-coalescing as a resource-wide rule.

## Provider Resource Caching

- Style, glyph, and sprite fetches honor upstream `Cache-Control` in both the
  pod-local shared cache and the public response. The normalized policy and
  current age must survive an internal peer hop. HTTP `Age` and `Date` contribute
  to the entry's initial age only when the upstream declares explicit freshness
  (`max-age`/`s-maxage`), so an already-old response consumes that declared
  lifetime rather than restarting it at Ishikari. When Ishikari applies a default
  policy (no upstream freshness), the clock starts at fetch time, so a transported
  `Age` must not shorten or evict the defaulted entry. Repeated `Cache-Control`
  field lines are combined before directive parsing.
- `no-store`, `no-cache`, and `private` responses do not enter Ishikari's shared
  provider cache. A successful uncacheable refresh invalidates any older stale
  entry for the same resource. Concurrent followers may reuse that successful
  representation ephemerally, without retaining it for later requests.
- `must-revalidate` and `proxy-revalidate` disable stale serving. Duplicate
  freshness directives use the most conservative parsed value so their order
  cannot extend freshness. Explicit freshness and stale windows are capped at
  seven days so a pathological upstream cannot pin shared-cache bytes
  indefinitely.
- A stale-while-revalidate leader sends the cached origin `ETag` (or
  `Last-Modified` when no ETag exists) as a conditional request. An origin `304`
  rebuilds the cache entry around the existing validated bytes, retaining
  representation metadata absent from the `304`, applying updated cache policy
  and validators, and restarting freshness without downloading the body.
  Object-store origins use the equivalent `GetOptions` preconditions. The
  `revalidated` provider-cache metric distinguishes this path from a full-body
  replacement; refresh errors leave the stale entry untouched.
- When an upstream supplies no cache policy, style responses use `STYLE` and
  glyph/sprite responses use `GLYPH_SPRITE` from `server/cache.rs`. Provider
  `404 Not Found` results are cached internally for 30 seconds with no stale
  window; transient failures are not negative-cached.
- Distinct provider URLs are protected by a process-wide fetch concurrency and
  admission bound in addition to per-key single-flight. A slow or hung upstream
  must not pin request tasks or body memory without a bound.
- The direct HTTP provider fetch does not follow redirects. Upstreams answer
  directly; chasing a redirect would let a compromised or open-redirecting
  upstream steer the fetch at cluster-internal or link-local addresses that the
  internal-listener isolation otherwise fences off.
- `Content-Encoding` is representation metadata and survives byte-identical
  glyph/sprite responses and peer hops. Compressed style JSON is decoded with a
  bounded output before validation and rewriting; invalid JSON never enters the
  provider cache.
- Validators pass through only for byte-identical bodies: glyphs and sprites
  emit the upstream `ETag`/`Last-Modified` and answer `If-None-Match`
  (weak comparison, precedence over `If-Modified-Since`) and second-granular
  `If-Modified-Since` with `304 Not Modified`. The `304` carries the same cache
  metadata (`Cache-Control`, `Age`, validators) as the `200` but omits
  representation metadata such as `Content-Encoding` (RFC 9110 §15.4.5).
  `If-None-Match: *` matches any existing representation even
  when no `ETag` is available. Derived representations — rewritten style JSON,
  TileJSON, and derived-product TileJSON — instead emit an Ishikari-computed
  strong `ETag` over the exact bytes served and no `Last-Modified`, and answer
  `If-None-Match` with a `304`. Ishikari never emits a validator that the
  upstream did not supply for a byte-identical body, and validators survive the
  internal peer hop alongside the cache policy.
- TileJSON, rewritten style JSON, and generated preview/derived documents embed
  the effective request origin. They emit `Vary: Origin, X-Forwarded-Proto`.
  Standards-compliant caches already include the request URI's scheme,
  authority, path, and query in their primary key; deployments must not place a
  shared cache in front of Ishikari that collapses different authorities or
  ignores query parameters.

### Provider wire compatibility

During a rolling upgrade from peers that predate provider cache metadata, a
peer response with no policy is exposed as `no-cache` rather than guessed to be
cacheable. This is a transition rule, not a permanent provider-cache design
goal; it can be removed when the supported rolling-upgrade floor guarantees
cache metadata on every peer.

## Refactor Direction

Do not move files only for aesthetics. Move modules when a new responsibility
needs a clearer boundary.

`server/provider_cache_policy.rs` owns upstream `Cache-Control` parsing,
normalization, and TTL selection. `server/provider_body.rs` owns bounded
representation decoding, JSON validation, and media-type checks.
`server/upstream.rs` owns bounded fetch, single-flight, cache-entry lifecycle,
and revalidation. Keep these policy/body/orchestration boundaries; split
transport further only when it evolves independently.

Likely future split:

- split `server` into `http` when response shaping, request IDs, metrics, and resource families make the current module too broad.
