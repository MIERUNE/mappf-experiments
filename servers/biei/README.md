# Biei

A distributed renderer for static map images and tiles, built with MapLibre Native.

> [!WARNING]
> This is an experimental, proof-of-concept project. The behavior, API, and configuration are not stable.

Biei is designed to work both as a simple single-node server and as a scalable MapLibre rendering cluster:

- **Static image rendering** - renders center, bbox, auto-fit, path, GeoJSON, pin, and `addlayer` requests.
- **Rasterized tile rendering** - serves pre-rendered raster tiles from MapLibre styles.
- **Scale-out render pool** - adds gossip membership, peer forwarding, and rendered-image caching when run as a multi-node cluster.

LICENSE: MIT OR Apache-2.0

## Demo

To start a simple single-node server:

```sh
cargo run -p biei -- \
  --style-templates 'carto=https://basemaps.cartocdn.com/{style_id}/style.json'
open http://localhost:8080/carto/gl/voyager-gl-style/static/139.767,35.681,11,0,0/640x360@2x.webp
```

To start a local three-node cluster for development:

```sh
bash demo-deploy/biei/dev-cluster.sh
open http://localhost:8080/carto/gl/voyager-gl-style/preview
```

The script builds `biei`, starts `NUM_NODES` processes on consecutive
HTTP/gossip ports, prefixes logs by node, and stops all nodes on Ctrl-C.

Sample URLs against the default local cluster (`BASE_HTTP_PORT=8080`):

```text
# tile rendering preview page
http://localhost:8080/carto/gl/voyager-gl-style/preview

# static center image around Tokyo
http://localhost:8080/carto/gl/voyager-gl-style/static/139.767,35.681,11,0,0/640x360@2x.webp

# static bbox image
http://localhost:8080/carto/gl/voyager-gl-style/static/[139.55,35.55,139.95,35.85]/640x360@2x.webp

# route-style overlay: blue pin, path, red pin
http://localhost:8080/carto/gl/voyager-gl-style/static/path-5+1a75ff-0.8(g%7DwxEwfatY_q%40vaLgbC_vJ),pin-l-s+1a75ff(139.767,35.681),pin-l-g+fd3344(139.760,35.710)/auto/640x360@2x.webp

# GeoJSON polygon overlay with auto fit
http://localhost:8080/carto/gl/voyager-gl-style/static/geojson(%7B%22type%22%3A%22Feature%22%2C%22properties%22%3A%7B%22fill%22%3A%22%2345cf23%22%2C%22fill-opacity%22%3A0.35%2C%22stroke%22%3A%22%23333%22%2C%22stroke-width%22%3A2%7D%2C%22geometry%22%3A%7B%22type%22%3A%22Polygon%22%2C%22coordinates%22%3A%5B%5B%5B139.65%2C35.62%5D%2C%5B139.85%2C35.62%5D%2C%5B139.85%2C35.78%5D%2C%5B139.65%2C35.78%5D%2C%5B139.65%2C35.62%5D%5D%5D%7D%7D)/auto/640x360@2x.webp

# raster tile
http://localhost:8080/carto/gl/dark-matter-gl-style/5/28/12@2x.webp
```

Override ports or providers with environment variables. If you change
`BASE_HTTP_PORT`, replace `8080` in the sample URLs with that port.

```sh
NUM_NODES=4 BASE_HTTP_PORT=18080 BASE_INTERNAL_PORT=19090 BASE_GOSSIP_PORT=17946 \
STYLE_URL_TEMPLATE='carto=https://basemaps.cartocdn.com/{style_id}/style.json' \
bash demo-deploy/biei/dev-cluster.sh
```

Single-node mode is the default. Cluster mode is explicit and serves two HTTP
listeners: a public port (`--http-bind`, default `:8080`) for render ingress plus
top-level `/livez` `/readyz`, and a separate cluster-internal port
(`--internal-port`, default `9090`) for `/_internal/*` (including metrics) and
peer-to-peer forwarding. The internal port is never exposed publicly; peers
forward to the advertised internal address, so `--internal-advertise-addr` points at the
internal port. Cluster mode also requires an explicit routable gossip address via
`--gossip-advertise-addr` (env `BIEI_GOSSIP_ADVERTISE_ADDR`); `--gossip-bind`
remains the local UDP listener and may use a wildcard IP:

```sh
cargo run -p biei -- \
  --cluster \
  --require-gossip-bootstrap \
  --style-templates 'http://style-provider.svc.cluster.local:8080/styles/{style_id}/style.json' \
  --tileset-url-template 'http://style-provider.svc.cluster.local:8080/tilesets/{tileset_id}/tileset.json' \
  --mln-resource-private-hosts style-provider.svc.cluster.local \
  --mln-resource-cache-bytes 268435456 \
  --internal-port 9090 \
  --internal-advertise-addr "$HOSTNAME.biei.default.svc.cluster.local:9090" \
  --gossip-bind 0.0.0.0:7946 \
  --gossip-advertise-addr "$HOSTNAME.biei.default.svc.cluster.local:7946" \
  --gossip-seeds biei-0.biei:7946
```

`--require-gossip-bootstrap` (env `BIEI_REQUIRE_GOSSIP_BOOTSTRAP`) is an
explicit startup-only readiness policy. It defaults to `false`, including when
seeds are configured. When enabled, readiness waits for one raw live peer
observation, fails open after 30 seconds, and remains open through later
partitions.

### Style templates

`--style-templates` (env `BIEI_STYLE_TEMPLATES`) maps a request's style id to a
`style.json` URL. It is a `;`-separated list of entries; each `<template>` must
be an http(s) URL with `{style_id}` in its path. Placeholders in the authority,
query, or fragment are rejected.

**Single bare template** — every style id is substituted whole:

```sh
--style-templates 'https://basemaps.cartocdn.com/{style_id}/style.json'
# request path          style id            -> resolved style.json
# /gl/voyager-gl-style  gl/voyager-gl-style -> https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json
# /positron             positron            -> https://basemaps.cartocdn.com/positron/style.json
```

**Multiple `namespace=<template>` entries** (+ optional `default=`) — the style
id's **first path segment** picks the template. On a namespace match that
segment is stripped, so only the rest fills `{style_id}`; the `default` (or a
bare entry) is the catch-all and receives the whole id:

```sh
--style-templates '
  carto=https://basemaps.cartocdn.com/{style_id}/style.json;
  example=https://styles.example.test/{style_id}/style.json;
  default=https://basemaps.cartocdn.com/{style_id}/style.json'
```

Without a `default`, an unregistered namespace returns `unknown_style` (404),
which keeps the catalog scoped to providers you list.

### Experimental static-render authentication

Authentication is disabled by default. Setting `BIEI_AUTH_REGISTRIES` to a
semicolon-separated `registry_id=auth-root` catalog protects only static-render
routes; tile, preview, health, metrics, and internal routes keep their current
behavior. For example:

```sh
BIEI_AUTH_REGISTRIES='public=gs://example-auth/registries/public/' \
BIEI_AUTH_PROVIDER_ORIGIN='https://ishikari.example.internal'
```

Each root is resolved to `current.json`. Requests use either
`Authorization: Bearer <registry_id>.<opaque_registry_credential>` or
`?access_token=<registry_id>.<opaque_registry_credential>`. Supplying both, or
repeating either one, is rejected. The token is split only at its first dot, so
the suffix may itself contain dots. Unknown registry IDs are rejected locally
without storage I/O. Query transport is intended for browser/map clients that
cannot set headers; configure Gateway/CDN/request logging to redact it and use a
restrictive `Referrer-Policy`. The current v1 snapshot shape is:

```json
{
  "schema_version": 1,
  "registry_id": "public",
  "revision": 1,
  "credentials": [{
    "credential_sha256": "<64 lowercase hex characters>",
    "principal_id": "demo-browser",
    "enabled": true,
    "namespaces": ["demo"],
    "actions": ["render.static"],
    "allowed_origins": ["https://maps.example"],
    "allow_missing_origin": false
  }]
}
```

For this object-store adapter, `credential_sha256` is SHA-256 over the fixed
bytes `mmpf-object-store-auth-v1\0`, followed by the registry ID's 64-bit
big-endian byte length and bytes, then the opaque credential's 64-bit big-endian
byte length and bytes. The registry stores no raw bearer credential. Loaded
snapshots are verified locally, refreshed conditionally once per minute, and
retained as last-known-good state after refresh failures. This first slice is
not enabled by the demo deployment and has no key issuance tooling yet; see
[the auth sketch](../../specs/auth-sketch.md).

Protected rendered-output cache hits are authorized on every request. Cache
entries currently record the producing caller's complete normalized namespace
grant set as a conservative requirement: callers with equivalent or broader
grants may reuse the image, while weaker and unauthenticated callers miss. This
is intentionally separate from Biei's credential-and-registry-revision-derived
profile cache partition, which prevents style/TileJSON cache, single-flight,
and loaded native style reuse across different credentials or policy revisions
without putting the raw credential in a cache key. Biei carries the bounded,
redacted verified token across its trusted render wire and appends it as
`access_token` only when fetching from `BIEI_AUTH_PROVIDER_ORIGIN`. Ishikari's
same-origin rewrites then propagate it to generated tile, glyph, and sprite
URLs; retained external provider URLs never receive it.

Because this is the original reusable bearer token, deployments must decide
whether their internal network is an accepted trust boundary. If node-to-node
confidentiality or workload identity is required, protect both Biei peer
forwarding and Biei-to-Ishikari traffic with mesh mTLS or an equivalent
deployment-layer mechanism. Biei does not add a second application-level
cryptographic protocol. The demo does not enable this auth mode. End-to-end
cache non-interference and the narrower style dependency descriptor remain
deployment gates.

### Admission knobs

`BIEI_QUEUE_CAPACITY_MULTIPLIER` controls the hard per-renderer-slot queue
boundary over the fixed soft routing limit of one task per slot. It defaults to
`2` and accepts `1` through `4`. Raising it absorbs short bursts while replicas
scale out, but does not add render throughput; compare `queue_full` rejections
with end-to-end tail latency before increasing it.

### Cache knobs

`BIEI_SOURCE_CACHE_CAPACITY` controls the per-renderer warm source cache
capacity (default `1`). `BIEI_RENDER_OUTPUT_CACHE_BYTES` controls the node-local
rendered image cache size (default `268435456`, set `0` to disable).
Rendered entries expire after five minutes even when the style revision is
unchanged, because referenced resources may change at stable URLs.
`BIEI_MLN_RESOURCE_CACHE_BYTES` controls the process-wide in-memory cache for
tiles, glyphs, sprites, and other MapLibre resources (default `268435456`, set
`0` to disable). The resource cache is shared by every renderer slot in the
process and does not persist across restarts.
`BIEI_MLN_BODY_PERMITS` bounds concurrent response-body buffering and defaults
to `max(24, 4 * render_permits)`; tune it only when admission-wait and memory
metrics show that the default is inappropriate.

The `mmpf_mln_resource_*` metrics separate Database cache operations, deferred
refreshes, admission wait, single-flight participation, and actual upstream
HTTP attempts. Use `--disable-mln-file-sources` only as a diagnostic A/B mode
when comparing the Rust cache/loader with MapLibre Native's default leaves.

Map resources are allowed to resolve to public IP addresses by default. Set
`BIEI_MLN_RESOURCE_PRIVATE_HOSTS` to a comma-separated list of exact hosts or
leading-wildcard domains when an operator-managed style intentionally loads
resources from a private network, for example
`resource-api.default.svc.cluster.local,*.tiles.svc.cluster.local`. Loopback,
link-local, and private addresses reached through any other hostname or redirect
are rejected. Keep this exception as narrow as possible: an allowlisted host
bypasses private-address filtering, so broad service-domain wildcards can expose
unrelated internal services when resource URLs are not fully trusted.

## Documentation

- [Production contract and guardrails](../../specs/biei-spec.md)
- [Open Biei work and decisions](../../issues/biei-todo.md)
- [Unlanded MapLibre Native binding needs](../../issues/mln-rs-wishlist.md)
- [Simulator documentation](../../sims/biei-sim/README.md)
