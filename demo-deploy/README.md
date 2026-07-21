# Combined deployment

The `demo-deploy/kustomization.yaml` composes the shared GKE platform,
Ishikari, and Biei without taking ownership away from either service overlay.

Render and inspect the complete stack:

```sh
kubectl kustomize demo-deploy
```

Apply it only when both service-local image digests have been reviewed:

```sh
kubectl apply -k demo-deploy
kubectl -n map-demo rollout status deploy/ishikari
kubectl -n map-demo rollout status deploy/biei
```

Use `demo-deploy/promote_image.py <biei|ishikari> <cloud-build-id>` after each
Cloud Build to update and render-check the corresponding digest pin. The
promotion command reads the digest recorded in the selected Cloud Build result;
it does not re-resolve the shared `:dev` tag.

Service-only rollouts may continue to use the kustomizations below:

- `demo-deploy/ishikari/runtime/k8s/overlays/gke`
- `demo-deploy/biei/runtime/k8s/overlays/gke`

The root composition is also the input for future cross-service manifest and
smoke tests. Shared resources should move here only when they are genuinely
owned by the stack rather than one service.
