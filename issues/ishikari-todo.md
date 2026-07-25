# Ishikari Decision and Experiment Queue

No Ishikari-specific implementation item is active. Current production defaults remain in force until a named experiment or product decision supplies contrary evidence. Durable contracts live in [`../specs/ishikari-spec.md`](../specs/ishikari-spec.md), cross-cutting work lives in [`refactor.md`](refactor.md), and simulator fidelity boundaries live in [`../specs/ishikari-sim-spec.md`](../specs/ishikari-sim-spec.md). Delete resolved entries; git history is the archive.

## Distributed-cache decisions

### Entry-node L1 insertion

- **Current decision:** keep entry-node insertion as the production default.
- **Evidence:** a 10-node, 159,584-request modeled run with 64 MiB tile caches and normal 512 MiB chunk caches produced identical backend work under entry and owner-only insertion, while entry insertion reduced peer requests. Owner-only insertion won only with an intentionally constrained 1 MiB chunk cache.
- **Reopen when:** a production-sized capacity/churn sweep needs to decide whether L1 is a replicated hot tier or owned aggregate capacity.

### Group-zero index ownership

- **Current decision:** do not shard bootstrap or leaf ownership.
- **Evidence:** a 3-node, 26,018-request real-resolver replay concentrated all 119 index requests on one owner, but they were only 1.1% of 10,873 internal tile requests.
- **Reopen when:** a multi-tileset production trace shows material owner concentration. Use per-node `ishikari_internal_resource_requests_total` and `ishikari_peer_fetch_total` filtered to bootstrap/leaf resources.

### Chunk merge window

- **Current decision:** retain the configurable 10 ms default.
- **Reopen when:** a named tuning effort compares the 0 ms baseline and current default across end-user latency, backend operations and bytes, waiter fan-in, and the measured Pareto frontier.

## Tile representation selection

- **Current decision: a path suffix is authoritative.** `.mlt` serves MLT; `.mvt`, `.pbf`, and the raster suffixes serve the stored representation. `Accept` is consulted only on the suffixless URL, and `Vary` lists `Accept` only there. Both `application/vnd.maplibre-tile` and the earlier `application/vnd.maplibre-vector-tile` spelling select MLT, while responses use the canonical shorter type. `Accept-Encoding` continues to participate everywhere.
- **Why:** [`../specs/auth-sketch.md`](../specs/auth-sketch.md) §4 states that the CDN cache key is the full URL and that arbitrary `Vary` values cannot be relied on. A representation chosen by a request header is therefore either served wrongly from a shared cache or keyed on an attacker-controlled header that multiplies variants of one immutable tile. Putting the representation in the URL avoids both. Martin resolves the same ambiguity in the opposite direction — extensionless canonical URL, `Accept` only, `.{ext}` answered with a 301 — which suits a server that makes no CDN assumptions but costs a redirect per tile and leaves it emitting no `Vary` at all.
- **Migration note:** this changed observable behavior. A request combining a suffix with a conflicting `Accept` (for example `.mvt` with `Accept: application/vnd.maplibre-tile`) previously returned MLT and now returns the stored representation. Such a request was self-contradictory, so no supported client is expected to depend on it.
- **Reopen when:** a client needs a representation that cannot be expressed as a suffix, or measurement shows the suffixless URL carries enough traffic that its `Vary: Accept` fragmentation matters.

## Derived terrain decisions

The contract and evaluation dimensions live in [`../specs/isoline-and-hillshade-spec.md`](../specs/isoline-and-hillshade-spec.md).

- Run the representation benchmark over representative fixtures and zooms before changing format, tone, or simplification defaults.
- Verify raster `color-relief` behavior in the supported MapLibre GL JS and concrete Biei MapLibre Native versions before claiming those clients as supported.
- Increase shared-arc simplification tolerance only with fixtures proving no intersections, orientation reversal, or narrow-face collapse.
- Evaluate request-coalesced metatiles only if the representation benchmark shows a material geometry or CPU benefit; preserve bounded overcompute and shared-topology constraints.

## Simulator decisions

- Before publishing calibrated results, run cold-cluster direct-node and Gateway replays against the acceptance bounds in [`../specs/ishikari-sim-spec.md`](../specs/ishikari-sim-spec.md).
- Published measurements must retain fixture source/version, acquisition steps, trace fingerprint, and fitted latency-profile provenance.
- Model terrain generation and shared CPU admission only for a named Phase 2 study.
- Add gossip loss or partition injection only from measured failure inputs.
- Change `entry_affinity` only after confirming whether the production Gateway balances HTTP/2 traffic per request or per connection.
- Add multi-tileset traces only when an experiment needs per-tileset coordinator and cache competition.
- Report churn recovery in wall-clock terms only when a communication use case supplies a defensible request-rate assumption.

## Production correctness and publishing gates

- **Provider configuration revision:** before provider catalogs can roll independently across pods or become dynamically mutable, bind a bounded catalog/config revision to internal provider requests and reject or fall back locally on mismatch. HRW placement currently includes the caller's upstream URL, while the peer receives only logical identity and may resolve a different upstream under skew.
- **Immutable PMTiles publishing:** before production ingest permits writers to publish archives, provide a create-only procedure and prevent or detect overwrite of a live logical id. A revision label may be part of that opaque id, but it is not a separate Ishikari version field. Runtime caches intentionally assume a tileset id is immutable; silently replacing bytes under one id is unsupported, not eventual consistency.

## Product and operational contingencies

- Add a style-catalog admin/update endpoint only if dynamic registration becomes necessary.
- Define provider style/version invalidation before supporting mutable logical identifiers.
- Before adding a content-management publisher, define the style-scoped
  content-addressed sprite contract: the publisher writes the complete bundle,
  injects its SHA-256 `sprite_id` into the style, and publishes the lightweight
  style last. Ishikari must preserve that logical reference, while Biei must see
  a new `StyleRevision` or immutable style id.
- Decide whether external style assets must be mirrored or may be proxied before expanding provider behavior.
- Revisit framed internal APIs or end-to-end timeout budgets only if the current HTTP and fixed per-hop contracts prove insufficient.
- Shorten dead-node retention only after measuring state growth under Spot churn.
- Persist a monotonic membership incarnation only if wall-clock rollback becomes an operational concern.
