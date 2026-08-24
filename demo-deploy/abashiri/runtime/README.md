# Abashiri GKE demo

This internal-only deployment exercises authenticated style publication against real GCS while Ishikari and Biei remain the public delivery services. It has no Gateway route.

Build and pin the image:

```sh
BUILD_ID="$(gcloud builds submit --config demo-deploy/abashiri/runtime/cloudbuild.yaml --format='value(id)' .)"
demo-deploy/promote_image.py abashiri "$BUILD_ID"
```

The runtime requires three independently authorized roots: `ABASHIRI_AUTH_ROOT`, `ABASHIRI_STATE_ROOT`, and `ABASHIRI_JOURNAL_ROOT`. The checked-in overlay publishes state into the existing demo delivery bucket, uses a private journal bucket, and reads a bootstrap-owned authentication registry from a control bucket. Ishikari receives read access only to the state bucket. Abashiri discovers canonical styles and tilesets by bounded object-storage listing and filters them by namespace grants; no separate resource catalog is required. It also configures named, internal-only Biei and Ishikari status endpoints; callers need `operations.read` in the management-auth registry before `GET /operations/status` succeeds.

Before enabling mutations, run `abashiri check-storage` against the state root with the intended GKE workload identity. GCS conditional replacement requires `storage.objects.delete`; grant it only on the state bucket. Verify the journal's create/read/list path with an authenticated mutation and reconciliation scan, without granting replacement or deletion, and confirm that delivery identities cannot read the private journal. The probe is retained for lifecycle expiry unless `--cleanup` is used.

Apply and inspect the internal service:

```sh
kubectl apply -k demo-deploy/abashiri/runtime/k8s/overlays/gke
kubectl -n map-demo rollout status deploy/abashiri
kubectl -n map-demo port-forward svc/abashiri 8080:8080
```

The Biei and Ishikari NetworkPolicies admit Abashiri only on internal TCP port 9090 for advisory refresh hints. Abashiri receives no gossip access and does not join either cluster.
