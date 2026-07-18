# biei k8s demo

Runs a 3-node biei rendering cluster. The local overlay renders against remote
style/tile providers. The GKE overlay attaches biei to the shared demo Gateway
and can also render styles/tiles served by the in-cluster provider.

This is a demo, not a production deployment.

## Layout

| Path | Purpose |
|---|---|
| `Dockerfile` | Linux OpenGL/EGL image using Mesa llvmpipe for headless rendering. |
| `k8s/base/` | Deployment, ClusterIP HTTP Service, and headless gossip Service. |
| `k8s/overlays/local/` | Local Kubernetes overlay; exposes `svc/biei` with a local LoadBalancer. |
| `k8s/overlays/gke/` | GKE overlay; uses Artifact Registry image and a Gateway `HTTPRoute`. |

## Local

```sh
docker build -f demo-deploy/Dockerfile -t biei:dev .
kubectl apply -k demo-deploy/k8s/overlays/local
kubectl -n biei-demo rollout status deploy/biei

curl 'http://localhost:8080/carto/voyager-gl-style/static/139.767,35.681,11/512x384.webp' -o tokyo.webp
```

If your local Kubernetes has no LoadBalancer controller, port-forward instead:

```sh
kubectl -n biei-demo port-forward svc/biei 8080:8080
```

## GKE

Build and push the image, then deploy the overlay. The shared Gateway
(`demo-gw` in namespace `map-demo`) must already exist. The GKE overlay points
all style and tileset resolution at the in-cluster `ishikari` Service, so deploy
Ishikari's GKE overlay first. Public namespaces such as `/carto/*`, `/mierune/*`,
and `/ishikari/*` are style-id prefixes under Ishikari's backing store.

```sh
gcloud builds submit --config demo-deploy/cloudbuild.yaml .

DIGEST="$(gcloud artifacts docker images describe \
  asia-northeast1-docker.pkg.dev/mappf-experiment/biei/biei:dev \
  --format='value(image_summary.digest)')"

# Keep the GKE overlay pinned to the exact image that was just built. This
# avoids ambiguous rollouts when the mutable :dev tag is reused.
export DIGEST
python3 - <<'PY'
import os
from pathlib import Path
path = Path("demo-deploy/k8s/overlays/gke/kustomization.yaml")
text = path.read_text()
lines = []
for line in text.splitlines():
    if line.strip().startswith("digest: sha256:"):
        line = f"    digest: {os.environ['DIGEST']}"
    lines.append(line)
path.write_text("\\n".join(lines) + "\\n")
PY

kubectl apply -k demo-deploy/k8s/overlays/gke
kubectl -n map-demo rollout status deploy/biei
```

Cloud Build exports a BuildKit `mode=max` cache to the separate
`biei-buildcache:latest` Artifact Registry package. `cargo-chef` keeps dependency
compilation reusable when only Rust sources change, and the final image stage
copies only the production sources (plus workspace manifests) so simulator,
docs, and deploy-only source changes do not rebuild the biei binary. The first
build populates the cache; subsequent builds import
it. Install the narrowly scoped cleanup policy once so superseded, untagged
cache manifests do not grow indefinitely:

BuildKit pushes the runtime image directly to Artifact Registry to avoid a
local `--load` followed by Cloud Build's second push. Consequently the final
Cloud Build summary shows `IMAGES: -`; the `${_IMAGE}` tag in Artifact Registry
is the build output and the build log records its digest.

```sh
gcloud artifacts repositories set-cleanup-policies biei \
  --location=asia-northeast1 \
  --policy=demo-deploy/artifact-cleanup-policy.json
```

The GKE demo also has an explicit low-cost observability profile:

```sh
bash demo-deploy/configure-gke-observability.sh
```

It retains mandatory system metrics and the explicit biei/ishikari Prometheus
scrapes, but disables the unused cAdvisor/Kubelet/kube-state/DCGM packages.
Autopilot retains advanced datapath metrics and requires image streaming even
though the demo images are small and same-region, so a narrow exclusion drops
its informational `gcfsd`/snapshotter noise instead of trying to disable the
feature. The same exclusion covers INFO-only kubelet/runtime noise plus serial
port 3/debug; warnings/errors and serial port 1 remain stored. On Standard GKE,
the script also disables advanced datapath metrics and image streaming because
node provisioning, rather than same-region image transfer, dominates this
demo's scale-out latency.

The shared Gateway uses the Certificate Manager map
`mappf-demo-cert-map`. Add Biei's hostname to that map once:

```sh
gcloud services enable certificatemanager.googleapis.com

gcloud certificate-manager dns-authorizations create mappf-biei-demo \
  --domain=biei-demo.mierune.dev \
  --location=global

gcloud certificate-manager certificates create mappf-biei-demo-cert \
  --domains=biei-demo.mierune.dev \
  --dns-authorizations=mappf-biei-demo \
  --location=global

gcloud certificate-manager maps entries create biei-demo \
  --map=mappf-demo-cert-map \
  --hostname=biei-demo.mierune.dev \
  --certificates=mappf-biei-demo-cert \
  --location=global
```

Add the DNS authorization CNAME shown by:

```sh
gcloud certificate-manager dns-authorizations describe mappf-biei-demo \
  --location=global \
  --format='value(dnsResourceRecord.name,dnsResourceRecord.type,dnsResourceRecord.data)'
```

The Gateway routes a catch-all `/` to biei's public listener (`:8080`), which
serves the render namespaces plus top-level `/livez` `/readyz`. `/_internal/*`,
`/metrics` and peer forwarding live on a separate cluster-internal port
(`:9090`) that the Service does not expose and the Gateway does not route, so
nothing internal is reachable publicly. The shared Gateway listens on HTTPS
only.

**Trust boundary:** the GKE overlay installs `biei-internal-boundary`: public
`:8080` remains reachable, while peer forwarding on TCP `:9090` and gossip on
UDP `:7946` are limited to biei pods in `map-demo`; the managed Prometheus
collector may scrape `:9090`. If you deploy the base or another overlay, install
an equivalent NetworkPolicy or service-mesh policy—the application protocol has
no peer authentication of its own.

## Checks

```sh
# Render the GKE overlay offline and verify CPU HPA, UDP gossip, and the
# internal NetworkPolicy invariants.
bash demo-deploy/check-hpa.sh

kubectl -n map-demo port-forward deploy/biei 8080:8080

curl 'http://localhost:8080/carto/voyager-gl-style/static/[139.6,35.6,139.9,35.8]/512x384.webp?padding=20' -o bbox.webp
curl 'http://localhost:8080/carto/voyager-gl-style/8/227/100.webp' -o tile.webp

# In the GKE overlay, all styles are fetched through Ishikari. The requested
# style id must exist under Ishikari's STYLE_TEMPLATES backing store, for
# example styles/mierune/jp_mierune_streets/style.json for the URL below.
curl 'http://localhost:8080/mierune/jp_mierune_streets/static/139.767,35.681,11/512x384.webp' -o ishikari.webp

# Readiness/liveness are top-level on the public port. A slot loss correlated
# with an active FileSource retry stays eligible for cache hits and routing
# misses to healthy peers;
# unrelated renderer loss fails the probes and is repaired autonomously or by
# the normal Kubernetes liveness window.
# Degraded pods also gossip `renderer.accepting=false`: healthy peers stop
# selecting them for new renders after gossip convergence, while direct exact
# output-cache hits remain reachable through the public Service.
curl -s localhost:8080/readyz
curl -s localhost:8080/livez

# Metrics and /_internal/* are only on the cluster-internal port (:9090),
# which is not Gateway-fronted — port-forward it separately.
kubectl -n map-demo port-forward deploy/biei 9090:9090
curl -s localhost:9090/_internal/metrics
```

## Notes

- The demo uses a Deployment, not a StatefulSet; chitchat handles dynamic
  membership.
- The headless gossip Service uses `publishNotReadyAddresses: true` so pods can
  discover each other during cold start.
- Rendering combines CPU work with in-render provider I/O. Tune `BIEI_CORES`
  and CPU limits together, but do not infer queue health from CPU alone.
- The GKE overlay keeps `minReplicas: 2` for cost and sets
  `BIEI_QUEUE_CAPACITY_MULTIPLIER=3` so each three-slot pod can buffer nine
  tasks while scale-out catches up. The soft routing limit and five-second SLA
  remain unchanged.
