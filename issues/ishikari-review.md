# Ishikari Architecture Review

This review records only findings that remain actionable in the current tree.
Resolved implementation history belongs in git history, not in the active
review. `ishikari-todo-spec.md` is the source of truth for planned work.

## Overall

Ishikari's strongest design remains the PMTiles chunk coordinator. All local
archive reads converge on the same byte-range path, where pending reads are
batched and in-flight chunks are shared. HRW routing then gives those chunk
caches stable locality as membership changes.

The stored-tile path is bounded and coherent. The main remaining questions are
not correctness blockers; they concern which cache artifacts should be owned by
one node, where cold-path coordination belongs, and whether optional derived
terrain work is important enough to distribute.

## Remaining Findings

### 1. Peer-fetched positive tiles populate the entry node's tile cache

`storage/resolver.rs::load_tile_from_peer` inserts a successful peer response
into the requesting node's L1 tile cache. Under a request-randomizing load
balancer, popular decoded tiles can therefore become replicated across pods,
while the underlying chunk cache remains HRW-sharded.

This may be desirable as a small near-client hot cache, but it reduces the
aggregate-capacity benefit of adding nodes if the tile cache is large. Do not
remove it based on theory alone: use `ishikari-sim` to compare the current
policy with owner-only insertion under representative cache sizes, churn, and
request skew. Then document the tile cache as either a deliberate replicated
hot tier or an owned tier.

### 2. Bootstrap and leaf-directory requests share one tileset owner

`storage/routing.rs::route_tileset` routes both bootstrap and leaf-directory
fetches using tile group zero. This keeps index state localized and simple, but
one node can become the index hotspot for a large or very popular archive.

Add per-node internal index-fetch metrics before changing ownership. If the
concentration is material, keep bootstrap/root ownership per tileset and route
leaf directories by a stable key derived from their byte offset. Any change
must preserve directory-cache locality and failover behavior.

### 3. Identical peer tile requests are not coalesced at the entry node

Concurrent misses for the same tile can produce multiple internal HTTP calls
from an entry node to the same owner. The owner still coalesces backend chunk
reads, so this does not multiply object-storage work, but it does add internal
request, decode, and response-copy overhead.

Only add entry-side single-flight if peer fan-in metrics show useful savings.
The key must include the resolved archive, tile id, requested representation,
and request semantics that affect the response; cancellation must not strand
followers.

### 4. The fixed merge window trades isolated latency for backend batching

`storage/chunked_store/coordinator.rs::FETCH_MERGE_WINDOW` is currently 10 ms.
An isolated cold non-bootstrap read can pay that delay even when no adjacent
request arrives, while viewport bursts benefit from the opportunity to merge
nearby Hilbert-local chunks.

This is a workload-dependent tradeoff. Use simulator and cloud measurements to
compare merge delay, fetched bytes, request fan-in, backend operations, and
end-user latency. Prefer a measured adaptive rule over simply reducing the
constant.

### 5. Generated terrain output is intentionally pod-local

DEM inputs use the normal resolver, HRW routing, chunk cache, and negative
cache. Generated contour, raster-hillshade, and vector-hillshade outputs are
single-flighted and byte-cached only within the serving pod. Multiple replicas
can therefore generate the same cold output independently.

This is acceptable while derived products remain experimental. Introduce HRW
ownership or peer sharing only if generation metrics show meaningful
cross-replica duplication; doing so would add a new distributed artifact and
failure mode to the core PMTiles provider.

### 6. Sibling hillshade products repeat post-DEM computation

Decoded DEM tiles are shared, but separate requests for vector hillshade,
quantized raster, lossless WebP, or lossy raster can each recompute the shade
field. The public preview makes this easy to trigger during comparisons.

A short-lived, byte-weighted cache of the continuous shade field could share
that work, but its memory cost is substantial. Add it only with measured
cross-product concurrency, or gate comparison-only products in deployments
that do not need them.

### 7. Distinct-id enumeration is only partially bounded

The short-TTL archive negative cache and per-key single-flight collapse repeated
and concurrent misses for the *same* missing tileset, and CPU-work admission
shedding bounds generation/decode/transcode. But a client enumerating many
*distinct* missing tileset ids or tiles still forces one uncached backend probe
each, and there is no global request-rate or concurrency ceiling in the process.

Treat blanket rate limiting as an edge/gateway responsibility where possible. If
an in-process backstop is wanted, add a global (or per-source) concurrency or
rate limit on cache-missing backend reads, sized well above normal viewport
load so only pathological enumeration is throttled.

## Lower-Priority Hardening

- Membership keeps dead-node state much longer than its failure-detection
  grace period. Measure state-set growth under Spot churn and expose live/dead
  counts before shortening retention.
- Membership generation ids are wall-clock-derived. A persisted monotonic
  incarnation would be stronger against clock rollback, but the operational
  risk is low relative to the cache-path work above.
- Mutable archives still need an explicit cache-invalidation contract. The
  current deployment model should continue to prefer immutable or versioned
  tileset ids.
- Reflected `Host` is charset-validated but not checked against an allowlist of
  expected public hostnames, so a shared cache fronting `/styles` without varying
  on `Host` could serve a poisoned style. Prefer keying the cache on `Host`, or
  add an expected-hostname allowlist if reflection must stay.

## Resolved in the Current Tree

The following former review findings are implemented and are no longer active
work:

- authoritative peer 404 responses stop resolution and populate the negative
  cache;
- terrain input fetch happens before the shared CPU permit, absent derived
  tiles are negatively cached, neighbor failures degrade to absent, and DEM
  decode is single-flighted;
- MLT transcoding runs on the blocking pool under the shared CPU-work limit, and
  all request-driven CPU work (terrain generation, DEM decode, MLT transcode) is
  admitted through a bounded gate that sheds with 503 under extreme overload
  instead of growing the wait queue and blocking backlog without limit;
- absent PMTiles archives (bootstrap / tileset-info misses) are negatively cached
  for a short (~1 s) TTL, so enumerating a missing tileset no longer re-probes
  object storage on every request, while a newly-provisioned archive still
  becomes visible within about a second;
- PMTiles header/version/directory bounds are validated and excessive directory
  depth is reported as an error; compressed directory and metadata payloads
  also have an explicit decompressed-size bound; DEM WebP decoding is
  dimension-bounded and residual zoom/glyph arithmetic uses checked operations;
- local readiness works with one clustered node and uses `DrainController` as
  the local drain source of truth;
- request ids are token-validated and internal/upstream error details are not
  returned in public response bodies; reflected response URLs honor only
  `http`/`https` forwarded schemes (a spoofed `X-Forwarded-Proto` falls back to
  the default) and tileset ids are length-bounded;
- provider resource single-flight cleans up cancelled leaders.

## Preserve

- The chunk coordinator's pending/in-flight separation, contiguous range
  merging, and backend concurrency bound.
- Deterministic HRW scoring with stable tie-breaking.
- Typed timeout/not-found/retryable error classification.
- `ObjectStoreRegistry` pooling by scheme and authority.
- Mapterhorn archive-presence single-flight and negative caching.
- Bounded metric label cardinality and public/internal listener separation.
- The rule that optional style and terrain features must not complicate the
  stored-PMTiles fast path.
