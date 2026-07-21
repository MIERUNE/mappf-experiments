# Ishikari k8s demo

Runs Ishikari as a tile/style provider. Public demo traffic is routed by the
shared Gateway.

This is a demo, not a production deployment.

## Layout

| Path | Purpose |
|---|---|
| `Dockerfile` | Release Ishikari image. |
| `k8s/base/` | Deployment, ClusterIP HTTP Service, and headless gossip Service. |
| `../../platform/` | Shared `map-demo` namespace and Gateway. Apply once per cluster. |
| `k8s/overlays/gke/` | GKE overlay with provider config, Artifact Registry image, and HTTPRoute. |

## Memory budget

The checked-in pod has a 2 GiB memory limit and assigns at most 1 GiB of
configured Moka material-cache weight:

| Cache | Weight ceiling |
|---|---:|
| Tile payloads | 256 MiB |
| PMTiles chunks | 256 MiB |
| Tileset metadata | 64 MiB |
| PMTiles archive bootstraps | 64 MiB |
| PMTiles leaf directories | 64 MiB |
| Provider resources | 64 MiB |
| Transcoded MLT tiles | 64 MiB |
| Derived terrain tiles | 128 MiB |
| Decoded DEM tiles | 64 MiB |
| **Total** | **1 GiB** |

Ishikari validates Mapterhorn's documented 512px source-tile contract before
image decode, so a cached decoded DEM contributes about 1 MiB of f32 material
rather than the reusable decoder's wider generic safety ceiling.

`ISKR_CACHE_WEIGHT_BUDGET_BYTES` is a startup validation ceiling, not an RSS
limit. Moka/hash-map/key overhead, small entry-count caches, in-flight response
bodies, decompression and terrain-generation working memory, HTTP/object-store
clients, Tokio and gossip tasks, and allocator fragmentation consume the other
headroom. Raising the cache-weight budget therefore requires a corresponding
container-memory review and a warm-cache load test at configured concurrency.
The checked-in deployment sets the budget and the two largest cache shares
explicitly so an application-default change cannot silently consume pod
headroom.

Backend range reads have a separate 128 MiB active-body reserve. Startup rejects
configurations where
`ISKR_CHUNK_SIZE_BYTES × ISKR_MAX_FETCH_CHUNKS × ISKR_BACKEND_FETCH_CONCURRENCY`
overflows or exceeds `ISKR_BACKEND_ACTIVE_BODY_BUDGET_BYTES`; the checked-in
`1 MiB × 4 × 32` settings exactly fit that reserve. This bounds response bodies
while the object-store fetch permits are held. It does not account for bodies
retained afterward by chunk coordination/caching, peer responses, or decoding
work, which still require separate admission bounds and load-test validation.

## GKE

The current GKE demo runs on amd64 Spot nodes in `asia-northeast1`. Autopilot
Arm workloads are not available in this region, so keep the pushed image amd64
unless you move the demo to an Arm-supported region.

Build and push the demo image:

```sh
BUILD_ID="$(gcloud builds submit \
  --config demo-deploy/ishikari/runtime/cloudbuild.yaml \
  --format='value(id)' \
  .)"

# Read the digest recorded by this Cloud Build, pin it in only Ishikari's
# overlay, and verify the rendered Deployment before applying it.
demo-deploy/promote_image.py ishikari "$BUILD_ID"
```

The first build populates a BuildKit `mode=max` cache in the separate
`ishikari-buildcache:latest` Artifact Registry package. `cargo-chef` makes the
dependency build reusable across ephemeral Cloud Build workers; simulator
source, documentation, and deployment-only changes do not recompile the
production binary. Simulator manifest changes still invalidate dependency
resolution, as they must for workspace lockfile validation. Install the
narrowly scoped cleanup policy once so superseded, untagged cache manifests do
not accumulate:

```sh
gcloud artifacts repositories set-cleanup-policies ishikari \
  --location=asia-northeast1 \
  --policy=demo-deploy/ishikari/runtime/artifact-cleanup-policy.json
```

BuildKit pushes the runtime image directly to Artifact Registry, avoiding a
local image load followed by Cloud Build's second push. Buildx writes the
pushed digest into that build's Cloud Build result. Promotion reads the
recorded result rather than the mutable `:dev` convenience tag, so a later
registry change cannot alter the selected artifact.

Create or bind the GCS reader identity once:

```sh
PROJECT_ID=mappf-experiment
BUCKET=mappf-experiment-demo
GSA=ishikari-gcs-reader@${PROJECT_ID}.iam.gserviceaccount.com
KSA=ishikari
NAMESPACE=map-demo

gcloud iam service-accounts create ishikari-gcs-reader \
  --project "${PROJECT_ID}" \
  --display-name "Ishikari demo GCS reader"

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member "serviceAccount:${GSA}" \
  --role roles/storage.objectViewer

gcloud iam service-accounts add-iam-policy-binding "${GSA}" \
  --project "${PROJECT_ID}" \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA}]"
```

The checked-in Gateway listens on HTTPS and uses Google-managed Certificate
Manager certificates through the `mappf-demo-cert-map` certificate map. Create
the Certificate Manager resources once before applying the Gateway. This
repository only owns the Ishikari hostname; other applications that attach to
the shared Gateway should create their own certificate map entries separately:

```sh
gcloud services enable certificatemanager.googleapis.com

gcloud certificate-manager dns-authorizations create mappf-ishikari-demo \
  --domain=ishikari-demo.mierune.dev \
  --location=global

gcloud certificate-manager certificates create mappf-ishikari-demo-cert \
  --domains=ishikari-demo.mierune.dev \
  --dns-authorizations=mappf-ishikari-demo \
  --location=global

gcloud certificate-manager maps create mappf-demo-cert-map --location=global
gcloud certificate-manager maps entries create ishikari-demo \
  --map=mappf-demo-cert-map \
  --hostname=ishikari-demo.mierune.dev \
  --certificates=mappf-ishikari-demo-cert \
  --location=global
```

Add the DNS authorization CNAME records in Cloudflare:

| Name | Type | Target |
|---|---|---|
| `_acme-challenge.ishikari-demo.mierune.dev` | `CNAME` | `bd6f2292-a3a6-43c4-90c4-d3505e35eb8b.14.authorize.certificatemanager.goog` |

Set Cloudflare SSL/TLS mode to `Full` so Cloudflare connects to the Gateway over
HTTPS. The demo host records can stay proxied because certificate authorization
uses the CNAME records above, not load-balancer IP visibility.
Point `ishikari-demo.mierune.dev` at the reserved `mappf-demo-ingress` address.

Then apply the shared Gateway and Ishikari:

```sh
kubectl apply -k demo-deploy/platform
kubectl apply -k demo-deploy/ishikari/runtime/k8s/overlays/gke
kubectl -n map-demo rollout status deploy/ishikari
```

The GKE overlay is configured for the demo bucket:

- PMTiles root: `ISKR_TILESET_SOURCES`
- styles: `ISKR_STYLE_TEMPLATES`
- glyphs: `ISKR_GLYPH_URL_TEMPLATE`
- sprites: `ISKR_SPRITE_TEMPLATES`

For another data source, patch those env vars in
`k8s/overlays/gke/patch-deployment.yaml`.

## Routes

The Gateway routes a catch-all `/` to Ishikari's public listener (`:8080`),
which serves the provider prefixes (`/tilesets/*`, `/styles/*`, `/fonts/*`) plus
top-level `/livez` `/readyz`. In the Kubernetes deployment, readiness waits for
one gossip peer during startup, fails open after 30 seconds, and remains open
through later partitions. `/_internal/*`, including `/_internal/metrics`, and
peer-to-peer forwarding live on a separate cluster-internal port (`:9090`) that the Service
does not expose and the Gateway does not route, so nothing internal is reachable
publicly. The shared Gateway listens on HTTPS only.

**Trust boundary:** the GKE overlay installs `ishikari-internal-boundary`.
Public and kubelet probe traffic remains reachable on TCP `:8080`; peer
forwarding on TCP `:9090` and gossip on UDP `:7946` are limited to Ishikari pods
in `map-demo`; Google Managed Service for Prometheus collectors may scrape
`:9090`. All other ingress to Ishikari pods is denied. The base manifests do not
install this GKE-specific policy, so other overlays must provide an equivalent
NetworkPolicy or service-mesh boundary.

## Checks

```sh
# Render the GKE overlay offline and verify the allowed/denied ingress matrix.
bash demo-deploy/ishikari/runtime/check-network-policy.sh

# In-cluster checks.
kubectl -n map-demo port-forward svc/ishikari 8080:8080
curl 'http://localhost:8080/tilesets/<tileset_id>'

# Metrics / _internal live on the internal port (9090), not the public 8080.
kubectl -n map-demo port-forward deploy/ishikari 9090:9090
curl -s localhost:9090/_internal/metrics

# Gateway checks.
curl 'https://ishikari-demo.mierune.dev/tilesets/<tileset_id>'
curl 'https://ishikari-demo.mierune.dev/styles/<style_id>/style.json'

# Certificate status.
gcloud certificate-manager certificates list \
  --location=global \
  --filter='name~mappf' \
  --format='table(name,managed.state,managed.authorizationAttemptInfo[].domain,managed.authorizationAttemptInfo[].state)'

# Must not route through the Gateway.
curl -i 'https://ishikari-demo.mierune.dev/_internal/metrics'
```

Acceptance: Ishikari serves public provider routes through the Gateway, and
deleting one Ishikari pod does not break the demo after readiness/drain settle.
