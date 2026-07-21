# Access-token protection for biei / ishikari — design sketch

Status: **exploratory design sketch; not adopted or implemented.** Captures the
July 2026 design discussion. Nothing here is an implementation contract, and
implementation should not begin without a separate decision. [`biei-spec.md`](biei-spec.md)
and [`ishikari-spec.md`](ishikari-spec.md) remain authoritative for what exists.

## 1. Requirements and constraints

- Protect the public endpoints of both biei and ishikari with access tokens.
- Request profile is map-shaped, not API-shaped: one page/session issues
  hundreds of tile / glyph / sprite requests, each cheap and latency-sensitive.
  biei `static` renders are the opposite: expensive, low-volume, poorly
  cacheable (arbitrary camera parameters).
- A CDN sits in front, but a *dumb* one: no signed-URL/cookie validation, no
  edge compute. The only lever is the cache-key configuration (full URL, plus
  optionally named request headers).
- The products may be sold as self-hosted middleware: customers already have
  their own auth (API gateways, OIDC/Keycloak, service mesh), so the auth
  boundary must be pluggable, with our own scheme as one implementation.
- Long-lived clients exist (car navigation): `style.json` is fetched once and
  tile URLs are then used for many hours; client firmware may be old and
  cannot be assumed to handle renewal.

## 2. Core decision: verify in-process, issue out-of-band

Token **verification** happens inside biei/ishikari: self-contained HMAC-signed
tokens, verified in ~µs with no I/O. Start with an implementation in the
owning server; extract shared verification primitives only after both servers
have real implementations with the same contract (§8).
No per-request hop to an external auth service — at tile QPS an extra network
round trip per request dominates latency and creates a single point of failure
in front of everything (the correlated-outage failure mode again).

The auth *service* still exists, but off the hot path: it **issues and
manages** tokens (mint, rotate keys, revoke) at per-session or per-epoch
frequency. Hops are spent per session, never per request.

Rejected for the request path: ext_authz-style callout services, managed API
gateways (per-request hop, SPOF, cost), IAP (Google identities, wrong fit for
map API keys), LB service extensions (a callout service by another name).

## 3. Token model: two tiers

### 3.1 Entry documents — strong auth, no downstream caching

`style.json`, TileJSON, preview HTML, and biei `static` renders are
authenticated with the real credential (customer API key or session token)
and served `Cache-Control: private/no-store`. These are low-volume, so strong
checks and zero downstream caching are affordable. Shared internal caches may
still hold an unpersonalized provider representation or render result when its
semantic cache identity is independent of the principal (§8.2). `static` is
also the expensive, abuse-attractive endpoint — it gets the strict tier by
design.

### 3.2 Tiles / glyphs / sprites — epoch capability (`cap`)

Map stacks have natural indirection: client → `style.json` → TileJSON → tile
URL templates. When ishikari serves an entry document (authenticated), it
embeds a **capability token** into the tile/glyph/sprite URL templates:

```
/t/{cap}/{z}/{x}/{y}.png
cap = truncated HMAC(master_secret, key_id, epoch_index)
```

- Verified statelessly at origin (pure computation; no hop).
- Cache-sharing unit is **(customer key, epoch)** — all visitors of one
  customer's site share one `cap`, so CDN hit rate within a site is the same
  as with no auth at all. Fragmentation is bounded by the number of keys, not
  users or sessions. (Per-session tokens in the cache key would make every
  session cold; that combination is dead on arrival with a dumb CDN.)
- Origin accepts the current and previous epoch (no thundering herd at
  rotation). CDN object TTL ≤ epoch length keeps "expired but still cached"
  bounded.

### 3.3 Accepted weaknesses

With a dumb CDN, a cached object is served to anyone presenting a matching
URL. The effective protection of cached content is therefore *capability
possession* with a lifetime of one epoch — acceptable for commodity base-map
tiles. **Escape hatch:** any tileset carrying private data is marked
`no-store` per scope and always takes the strong-auth path; this must be a
per-scope cache policy from day one.

## 4. CDN contract

- Cache key = **full URL (cap included) + the `Origin` request header**,
  configured explicitly in the CDN's cache-key settings. Do not rely on the
  `Vary` response header (many CDNs ignore arbitrary `Vary` values).
- **`Origin`, not `Referer`.** `Origin` is spec-guaranteed to be origin-form
  (scheme+host+port, never a path). A full-URL `Referer` in the cache key
  would (a) silently fragment the cache per page path, (b) leak end-user
  paths/query strings into CDN cache keys and logs, and (c) open an
  auth-passing cache-pollution DoS: `Referer: https://allowed.example/random-N`
  passes the origin check and caches unlimited valid 200 variants. `Origin`
  eliminates all three structurally.
- Guard anyway: if the keyed header value is not origin-form, serve the
  response with `no-store` (default; graceful for misconfigured clients, and
  it neuters cache pollution because such 200s are never stored). A strict
  per-key mode may 403 instead (`origin_not_origin_form`).
- 4xx responses are `no-store` (or minimal TTL) so a transient rejection
  cannot poison a variant.
- Absent `Origin`: browsers omit it on same-origin GETs and non-browser
  clients never send it. Treat `Origin: null` as absent. Per-key policy
  decides whether absent is acceptable (§5). A future authenticated preview
  must use an appropriate key tier for same-origin tile GETs; a referrer policy
  does not manufacture an `Origin` header.

## 5. Origin binding (anti-hotlink)

Key registry entries carry `allowed_origins` and an enforcement mode:

| Key tier | Policy |
|---|---|
| Web key | `Origin` required and must match `allowed_origins` |
| SDK / native key (car-nav apps etc.) | absent `Origin` accepted; other controls apply (§6) |

Because the origin value is part of the CDN cache key, the binding holds
**even on cache hits**: a hotlinking site's visitors present a different
`Origin`, miss the cache, reach origin, and are rejected — the legitimate
site's cached variants are unreachable to them. (This closes the classic
"referer checks don't survive CDN caching" hole.)

Honest scope: `Origin` is client-supplied and trivially forged by non-browser
scrapers. This control is **anti-hotlink, not anti-scraper**. Scraping is
controlled by rate limiting (§6), not by header checks.

## 6. Long-lived clients (car navigation)

Failure mode: clients fetch `style.json` once, so the `cap` is baked in for
the whole session; an epoch rotation mid-drive would 403 tiles while driving.
Old firmware cannot be assumed to renew.

Design response — stop defending freshness, bound *volume* instead:

- **Per-key epoch schedules.** `cap` derivation already includes `key_id`, so
  epoch length can vary per key: web keys ~1 day; SDK/nav keys ~30 days, with
  the previous epoch accepted (≥ one full epoch of residual validity). Long
  epochs also *improve* CDN hit rate for fleets sharing road corridors.
- **Per-cap rate limits at origin** (local per-pod token buckets — approximate
  global limits, no hop). Legitimate nav traffic is a few tiles/sec; scrapers
  are orders of magnitude above. This is the actual anti-abuse control; a
  leaked nav cap is worth "one vehicle's fetch rate for one epoch".
- **Error contract for renewal-capable clients**: 403 bodies distinguish
  `cap_expired` / `cap_invalid` / `origin_forbidden`. SDKs we control re-fetch
  TileJSON on `cap_expired` (one hop per epoch per device).
- **Soft-expiry option**: accept caps up to N epochs old while counting and
  flagging them — expiry as an anomaly signal rather than a hard gate — for
  fleets that cannot renew.
- **Bulk/offline packs are a separate channel.** If the real use case is
  region pre-download, serve it from a dedicated export endpoint with strong
  short-lived auth and no CDN, instead of letting devices crawl the tile API
  for hours.

## 7. Pluggable auth seam (middleware product)

What must be swappable is not "the sidecar" but the **authentication seam**.
Three layers:

```
Layer 1  AuthN (pluggable)   — who/what credential → normalized Principal
Layer 2  AuthZ (always in-app) — Principal.scopes vs parsed style/tileset id
                                 (needs the URL grammar; cannot be externalized)
Layer 3  CDN capability (optional module) — cap minting at entry documents,
         stateless verify; composes with any Layer 1
```

Layer 1 exposes a small server-local interface with a fixed menu of built-ins
(Rust; no dynamic loading). Do not commit to a trait object or shared crate
until a second implementation demonstrates that the boundary is genuinely
common:

| Implementation | Use |
|---|---|
| `None` | default; evaluation/dev, current behavior |
| `TrustedHeader` | *this is "swappable sidecar/gateway" support*: a fronting proxy (Envoy, Kong, oauth2-proxy, …) verifies and passes identity headers |
| `StaticApiKeys` / `HmacCap` | self-contained; our SaaS and small deployments |
| `Jwt { jwks_url }` | OIDC providers, verified locally with cached keys |
| `ForwardAuth { url }` | nginx `auth_request` / Traefik-compatible escape hatch; reintroduces the per-request hop **as the customer's explicit choice** |

`TrustedHeader` requires a documented trust anchor: the proxy strips inbound
identity headers at the edge, and proxy→app traffic is authenticated by a
shared-secret header, mTLS, or internal-only binding. The 403 error contract
(§6) is fixed across all implementations.

**Cluster-internal listener paths are exempt from public authentication** and
remain protected by network policy (or a stronger transport identity when one
is introduced). In-cluster biei→ishikari resource fetches are different: if
they use an authenticated public Ishikari route, Biei supplies a dedicated
service credential. Never forward the end user's credential across that hop.

## 8. Refactoring implications

This sketch is also a boundary review, but it is **not** a reason to add auth
scaffolding before an authenticator exists. Preparatory work must improve the
current system independently of this proposed design.

### 8.1 Existing boundaries to preserve

- Cluster deployments assemble separate public and cluster-internal routers and
  have contract tests that keep internal endpoints off the public listener.
  Biei standalone mode serves one listener but composes public content and
  operational/internal route sets separately. Apply authentication only to the
  public content router; do not recover the distinction later by inspecting path
  strings in one combined middleware.
- Biei's render-output cache is keyed from the parsed render request and style
  revision rather than request metadata. Keep credentials, principals, scopes,
  and capability tokens out of that key and out of `InternalTask`, peer wire
  messages, gossip, and simulator artifacts. A bounded, non-secret `RequestId`
  may cross task and peer-wire boundaries for end-to-end correlation, but it
  must never affect authorization, routing, cache identity, or representation
  selection.
- Ishikari uses validated `TilesetId` and `ResourceRoutingKey` values, Biei has
  a `StyleId`, and Ishikari provider routing uses a closed `ProviderRequest`
  that binds resource kind, logical identity, internal endpoint, and placement
  identity. Future authorization must compare scopes with parsed logical domain
  identifiers, not raw URL prefixes or provider URLs. Keep this request
  domain-specific rather than widening it into a generic routing framework.
- Resource URL diagnostics already remove userinfo, the complete query, and
  fragments. Keep that stronger behavior instead of maintaining an
  auth-parameter denylist that can drift when a new token name is introduced.

### 8.2 Cache and response boundary

"Credentials are not cache keys" does not mean that authorization-dependent
representations may share an entry. If two principals can receive different
bytes for the same parsed resource, derive a bounded, non-secret
**representation partition** (for example a policy or tenant revision) and
include that in the relevant cache identity. Never use the raw credential or
capability as the partition.

For Ishikari entry documents, cache the provider representation below auth,
then perform Origin-dependent and capability-dependent URL rewriting on the
response path. Do not place credential- or capability-bearing style JSON,
TileJSON, or preview HTML in a shared in-process response cache. CDN
`private/no-store` does not protect an incorrectly shared origin cache.

### 8.3 Auth-ready boundaries completed before auth

- Keep Ishikari's domain-specific `ProviderRequest` closed over style, glyph,
  and sprite resources. Its logical identity is safe for diagnostics and future
  authorization; its complete provider URL remains an implementation detail for
  fetching, provider-cache identity, and compatibility-preserving placement.
- Keep public/internal router separation covered by production-router contract
  tests as routes move. Biei's standalone public-content subrouter must remain
  independently layerable even though it shares one listener with operational
  routes. No new shared router abstraction is needed.
- Preserve semantic cache-key constructors as the only way to form cache
  identity; when auth arrives, add tests proving request IDs and credentials do
  not partition shared content, while representation partitions do.
- Preserve typed namespace decomposition for scope matching:
  - Ishikari `TilesetId` enforces `flat-id | namespace/id` (≤ 1 `/`) and exposes
    `namespace() -> Option<&str>` / `local_id() -> &str`.
  - Biei `StyleId` remains arbitrary-depth (`a/b/c`) and exposes its first
    segment through `namespace()`. Match finer scopes as prefixes so `a/`,
    `a/b/`, and the full id all resolve without changing deep identifiers such
    as `carto/gl/voyager-gl-style`.
- Biei classifies each public path and validates its `StyleId` before ingress
  admission. Insert future AuthZ at that seam; keep full tile/static/query
  parsing under the admission guard and avoid constructing an `InternalTask`
  before authorization.
- Biei's tile/static/preview response policy remains server-local. Tiles retain
  shared caching, while static renders and preview HTML use
  `Cache-Control: private, no-store`; do not move this HTTP policy into core
  tasks, semantic cache keys, or peer wire values.

### 8.4 Refactors to defer

Do not create `mmpf-auth`, `Principal`, an `Authenticator` trait hierarchy, a
key-registry abstraction, JWT/JWKS machinery, or a generic rate-limiter yet.
They have no production consumer and several open policy questions below can
change their shape. Implement the first selected verifier locally in its server
crate, implement the second against the same behavior, and extract only the
service-independent overlap. This follows the same two-real-consumer rule used
for the other `mmpf-*` crates.

Ishikari's current `get_origin` helper is also not that shared overlap: it
synthesizes a safe base URL for generated documents from `Origin`,
`X-Forwarded-Proto`, and `Host`. Auth Origin validation accepts or rejects a
client identity claim. The inputs look similar, but the contracts and fallback
behavior differ, so combining them would blur a security boundary.

## 9. Implementation notes

- Once implemented by both servers, consider putting proven
  service-independent verification, auth-specific Origin parsing, and the
  error contract in a small shared crate (for example `crates/mmpf-auth`). Keep
  service-specific URL rewriting and authorization policy in the owning
  server.
- **biei**: run AuthN before public-path parsing and before
  `acquire_admission` (header/query inspection only — no renderer dependency,
  so unlike the degraded gate it belongs at the very front). Then use the
  lightweight parsed `StyleId` for AuthZ before admission; full render/query
  parsing and task construction follow only after authorization and admission.
  The render-output-cache key **must not** include tokens or caps; add a
  non-secret representation partition only if auth policy changes rendered
  bytes. Preview passes the entry credential only to the protected
  style/TileJSON request; derived resource URLs carry the capability instead.
  `redacted_url` removes the entire query from logs.
- **ishikari**: apply one axum layer only to public routes, authorize against the
  parsed logical `TilesetId` or provider resource identity, and keep capability
  embedding in the style.json / TileJSON response path above the shared
  provider cache.
- **Key registry**: `key_id`, secret ref (`kid` for rotation), `scopes`
  (style/tileset prefixes), `allowed_origins`, tier (web/SDK), epoch schedule,
  rate tier. Rotation = accept multiple `kid`s during rollover. Immediate
  revocation of a key stops new entry documents and cap verification at
  origin; already-cached tiles survive at most the CDN TTL.
- **Metering**: origin counters undercount by design (CDN hits never arrive);
  true usage requires CDN log post-processing, on a separate async path.
  Prometheus labels per `key_id` are bounded (registered keys only); reject
  reasons and nonconforming-origin counts get counters so misconfigured
  customers are visible ("your traffic is uncacheable — check client Origin
  handling and the CDN cache-key configuration").

## 10. Open questions

- Issuance service shape and key-registry storage (static file vs small DB).
- Exact wire format: compact custom (`base64url(payload).base64url(mac)`) vs
  JWT (tooling familiarity, ~300 chars in a query param).
- Per-pod rate-limit constants and how they interact with HPA scale changes.
- Whether ishikari's style rewriting should also rewrite third-party provider
  URLs or only self-hosted tile endpoints.
