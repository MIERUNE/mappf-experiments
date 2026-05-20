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
bash scripts/dev-cluster.sh
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
bash scripts/dev-cluster.sh
```

Single-node mode is the default. Cluster mode is explicit and serves two HTTP
listeners: a public port (`--http-bind`, default `:8080`) for render ingress plus
top-level `/livez` `/readyz`, and a separate cluster-internal port
(`--internal-http-port`, default `9090`) for `/_internal/*`, `/metrics` and
peer-to-peer forwarding. The internal port is never exposed publicly; peers
forward to the advertised internal address, so `--internal-http-advertise-addr` points at the
internal port:

```sh
cargo run -p biei -- \
  --cluster \
  --style-templates 'http://style-provider.svc.cluster.local:8080/styles/{style_id}/style.json' \
  --tileset-url-template 'http://style-provider.svc.cluster.local:8080/tilesets/{tileset_id}/tileset.json' \
  --maplibre-cache-path /var/cache/biei/maplibre-ambient-cache.sqlite \
  --internal-http-port 9090 \
  --internal-http-advertise-addr "$HOSTNAME.biei.default.svc.cluster.local:9090" \
  --gossip-seeds biei-0.biei:7946
```

### Style templates

`--style-templates` (env `BIEI_STYLE_TEMPLATES`) maps a request's style id to a
`style.json` URL. It is a `;`-separated list of entries; each `<template>` must
contain `{style_id}` and be an http(s) URL.

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

### Cache knobs

`BIEI_SOURCE_CACHE_CAPACITY` controls the per-renderer warm source cache
capacity (default `1`). `BIEI_RENDER_OUTPUT_CACHE_BYTES` controls the node-local
rendered image cache size (default `268435456`, set `0` to disable).
