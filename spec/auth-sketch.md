# Access-token protection for biei / ishikari — design sketch

Status: **design sketch, not implemented.** Captures the July 2026 design
discussion. Nothing here is wired into code yet; `production-spec.md` remains
authoritative for what exists.

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

Token **verification** happens inside biei/ishikari (a shared middleware
crate): self-contained HMAC-signed tokens, verified in ~µs with no I/O.
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

### 3.1 Entry documents — strong auth, never cached

`style.json`, TileJSON, preview HTML, and biei `static` renders are
authenticated with the real credential (customer API key or session token)
and served `Cache-Control: private/no-store`. These are low-volume, so strong
checks and zero caching are affordable. `static` is also the expensive,
abuse-attractive endpoint — it gets the strict tier by design.

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
  decides whether absent is acceptable (§5). biei's preview page sets
  `referrerpolicy="strict-origin"` for its same-origin case.

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

Layer 1 is a trait with a fixed menu of built-ins (Rust; no dynamic loading):

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

**Internal paths are exempt from all of this**: the `:9090` internal
listeners and in-cluster biei→ishikari fetches never pass through the
Authenticator (biei carries one long-lived service token for ishikari if
needed; NetworkPolicy remains the second layer).

## 8. Implementation notes

- **Shared `auth` crate** used by both apps (verification, origin-form
  normalization, error contract; ~100–200 lines). First candidate for the
  shared-crate/monorepo effort; until then, vendorable as a single file.
- **biei**: verify before path parsing and before `acquire_admission` (header/
  query inspection only — no renderer dependency, so unlike the degraded gate
  it belongs at the very front). The render-output-cache key **must not**
  include tokens or caps. Preview propagates the credential into the tile
  URLs it emits. `redacted_url` masks token/cap parameters in logs.
- **ishikari**: one axum layer on public routes; cap embedding lives in the
  style.json / TileJSON handlers.
- **Key registry**: `key_id`, secret ref (`kid` for rotation), `scopes`
  (style/tileset prefixes), `allowed_origins`, tier (web/SDK), epoch schedule,
  rate tier. Rotation = accept multiple `kid`s during rollover. Immediate
  revocation of a key stops new entry documents and cap verification at
  origin; already-cached tiles survive at most the CDN TTL.
- **Metering**: origin counters undercount by design (CDN hits never arrive);
  true usage requires CDN log post-processing, on a separate async path.
  Prometheus labels per `key_id` are bounded (registered keys only); reject
  reasons and nonconforming-origin counts get counters so misconfigured
  customers are visible ("your traffic is uncacheable — check referrerpolicy").

## 9. Open questions

- Issuance service shape and key-registry storage (static file vs small DB).
- Exact wire format: compact custom (`base64url(payload).base64url(mac)`) vs
  JWT (tooling familiarity, ~300 chars in a query param).
- Where the shared crate lives before any monorepo decision.
- Per-pod rate-limit constants and how they interact with HPA scale changes.
- Whether ishikari's style rewriting should also rewrite third-party provider
  URLs or only self-hosted tile endpoints.
