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

- **Current decision: a path suffix is authoritative.** `.mlt` serves MLT; `.mvt`, `.pbf`, and the raster suffixes serve the stored representation. `Accept` is consulted only on the suffixless URL, and `Vary` lists `Accept` only there. Both `application/vnd.maplibre-tile` and the earlier `application/vnd.maplibre-vector-tile` spelling select MLT, while responses use the canonical shorter type. `encoding=mlt` belongs to TileJSON; tile payload routes reject that query rather than silently serving MVT. `Accept-Encoding` continues to participate everywhere.
- **Why:** [`../specs/auth-sketch.md`](../specs/auth-sketch.md) §4 states that the CDN cache key is the full URL and that arbitrary `Vary` values cannot be relied on. A representation chosen by a request header is therefore either served wrongly from a shared cache or keyed on an attacker-controlled header that multiplies variants of one immutable tile. Putting the representation in the URL avoids both. Martin resolves the same ambiguity in the opposite direction — extensionless canonical URL, `Accept` only, `.{ext}` answered with a 301 — which suits a server that makes no CDN assumptions but costs a redirect per tile and leaves it emitting no `Vary` at all.
- **Migration note:** this changed observable behavior. A request combining a suffix with a conflicting `Accept` (for example `.mvt` with `Accept: application/vnd.maplibre-tile`) previously returned MLT and now returns the stored representation. Such a request was self-contradictory, so no supported client is expected to depend on it.
- **Reopen when:** a client needs a representation that cannot be expressed as a suffix, or measurement shows the suffixless URL carries enough traffic that its `Vary: Accept` fragmentation matters.

## Empty and absent tile status

- **Current decision:** a zero-byte stored entry is answered `204`; an _absent_ tile keeps `404`; a conventionally empty tile is served normally as `200`.
- **Why `204` for a zero-byte entry:** the archive's compression flag applies to every entry, so such a tile would otherwise go out as an empty body labelled `Content-Encoding: gzip`, which no client can decode. `204` states the positive fact that the archive holds nothing there, and needs no encoding, transcode, or validator.
- **Why `404` stays for an absent tile:** `204` is cacheable, so adopting it here would pin "no tile here" in shared caches for the full tile `s-maxage`. Ishikari instead gives every public delivery error `private, no-store`; the bounded origin-local negative cache still collapses repeated misses without allowing a request for not-yet-published content to delay its rollout. Martin can answer both cases identically because it sets no cache headers at all.
- **Why this does not extend to conventionally empty tiles:** Martin checks `tile.data.is_empty()` because it holds decoded tile data. Ishikari holds stored compressed bytes, and a compressed empty payload is ~20 non-zero bytes, so the equivalent check would require decompressing every tile — far more cost than the empty case could ever save. An earlier claim that `204` would avoid a wasted MLT transcode was wrong for the same reason: compressed empty tiles never reach that branch as empty.
- **Reopen when:** a measured archive shows enough conventionally empty tiles that transcoding them is material, and a cheap way to identify them without decompressing exists.

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
- **Shorten stable-id replacement convergence:** PMTiles reads pin every archive-derived cache/read to an object version or strong ETag and restart on a mid-read change, so replacement cannot mix generations. The logical bootstrap pointer now revalidates directly against object storage every five minutes by default; generation-keyed material remains warm when unchanged. Add an Abashiri-to-Ishikari refresh signal (with cluster fan-out) before promising a shorter publication SLA. That path must also account for generated terrain whose 3x3 neighborhood can span physical archives, and publication must pair activation with CDN purge or a versioned delivery URL.
- **Gate unclustered archives at publication, never at serving.** An arbitrarily ordered data section still serves correct bytes — every directory entry carries an explicit offset — so this is a cost property, and the cost scales with size instead of being uniform. At the 1 MiB default chunk size a 21 MB archive is ~21 chunks and pays nothing once warm, while a planet-scale archive pays a chunk-sized read per scattered tile: one to two orders of magnitude of read amplification against a near-zero hit rate. The gate therefore belongs at publication, keyed on chunk count versus chunk-cache capacity rather than on the bare flag; a serve-time refusal would `5xx` archives that work correctly today. `mmpf_pmtiles::{HeaderLayout, LayoutVerifier}` provide the two halves: header screening from the first 127 bytes, and verification of the claim itself. The `clustered` bit is only a producer assertion, and checking it requires walking the directory — affordable while writing an archive, not while serving one tile of it. Serving should read the flag it already parses and expose it per archive/generation as a metric plus a one-time log, with any hard refusal behind explicit configuration.
- **Evidence — the risk is confined to the archives where it does not matter.** Every large archive in the demo bucket is clustered and fully optimized, screened from its 127-byte header: `mierune/omt` (83 GB, z0-14) claims clustered with 275,725,970 addressed tiles collapsing to 51,265,350 entries by run-length encoding and 45,217,181 stored blobs by reuse — a 6.1x reduction; `mapterhorn/planet` (706 GB, z0-12) and the z13-16 detail archives likewise. Only the `grib2pmtiles` weather archives (5-60 MB) are unclustered, and there the whole archive is a few dozen chunks, so the cost is immaterial. A serve-time refusal would therefore have broken exactly the harmless case while protecting nothing.
- **Producer-side fix:** directory verification on the three local weather archives found roughly half of all entries jumping backwards (40/78, 218/427, 8/17) with the first backward tile at id 70 in every archive regardless of maxzoom. That signature points at writing the data section in per-zoom scan order rather than tile-id order; sorting by tile id before writing fixes it. Directories themselves are well formed, which is why serving is unaffected. Those archives also use neither reuse nor run-length encoding, which for precipitation rasters with large uniform areas is the larger loss — compare the 6.1x that run-length encoding and reuse achieve on `mierune/omt`.

## Product and operational contingencies

- Before claiming that an existing map SDK application can point at MMPF unchanged, capture a delivery behavior matrix for styles, TileJSON, tiles, glyphs, and sprites. Abashiri's management contract says nothing about delivery behavior. Keep Ishikari's current canonical routes unless a concrete client justifies aliases and their additional authorization/cache surface.
- Add a style-catalog admin/update endpoint only if dynamic registration becomes necessary.
- **Content-addressed sprites:** Ishikari currently proxies provider sprites at `/styles/{namespace}/{style_id}/sprite{@2x}.{json,png}` with no immutable bundle id. Before adding the content publisher, implement the proposed `/styles/{namespace}/{style_id}/sprites/{sprite_id}{@2x}.{json,png}` delivery contract. The publisher writes the complete bundle, injects its SHA-256 `sprite_id` into the style, and publishes the lightweight style last. Ishikari preserves that logical reference, while Biei observes the resulting content-derived `StyleRevision`.
- Decide whether external style assets must be mirrored or may be proxied before expanding provider behavior.
- Revisit framed internal APIs or end-to-end timeout budgets only if the current HTTP and fixed per-hop contracts prove insufficient.
- Shorten dead-node retention only after measuring state growth under Spot churn.
- Persist a monotonic membership incarnation only if wall-clock rollback becomes an operational concern.

## Weighted provider fetch permits (or a sprite lane)

One semaphore admits every provider resource, so the startup body reserve has to
charge each permit the largest cap — an 8 MiB sprite PNG. That makes the default
concurrency of 64 require a 512 MiB reserve
(`ISKR_PROVIDER_ACTIVE_BODY_BUDGET_BYTES`), even though the workload the value was
tuned for is glyph-dominated at 1 MiB per body. The reserve is therefore correct but
pessimistic by roughly 8x against real traffic.

Two ways out, both behaviour changes rather than accounting changes:

- Weight the permits by each resource's cap, so a glyph costs 1 and a sprite PNG 8.
  The reserve then tracks actual bytes, and 64 concurrent glyph fetches stop
  reserving memory that only 64 concurrent sprite fetches could ever use.
- Give sprites a separate, much lower-concurrency lane. Simpler, and sprite fetches
  are rare and not on the cold-render critical path, but it adds a second knob.

Until one of these lands, a deployment that lowers the reserve must lower the
concurrency with it, which costs cold-render latency for memory it was never going
to use.
