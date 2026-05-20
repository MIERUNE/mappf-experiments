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

**Trust boundary:** the internal port is not network-isolated. With no
NetworkPolicy, any pod anywhere in the cluster (not just the `map-demo`
namespace) that can route to a pod IP can reach `:9090/_internal/*` and gossip
`:7946`. The demo cluster is assumed to host only trusted workloads. Add a
NetworkPolicy restricting `:9090`/`:7946` ingress to peer pods if you need
in-cluster isolation.

## Checks

```sh
kubectl -n map-demo port-forward deploy/biei 8080:8080

curl 'http://localhost:8080/carto/voyager-gl-style/static/[139.6,35.6,139.9,35.8]/512x384.webp?padding=20' -o bbox.webp
curl 'http://localhost:8080/carto/voyager-gl-style/8/227/100.webp' -o tile.webp

# In the GKE overlay, all styles are fetched through Ishikari. The requested
# style id must exist under Ishikari's STYLE_TEMPLATES backing store, for
# example styles/mierune/jp_mierune_streets/style.json for the URL below.
curl 'http://localhost:8080/mierune/jp_mierune_streets/static/139.767,35.681,11/512x384.webp' -o ishikari.webp

# Readiness/liveness are top-level on the public port.
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
- Software rendering is CPU-bound. Tune `BIEI_CORES` and CPU limits together.
