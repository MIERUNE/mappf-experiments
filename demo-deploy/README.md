# Combined deployment

The repository-root `kustomization.yaml` composes the shared GKE platform,
Ishikari, and Biei without taking ownership away from either service overlay.

Render and inspect the complete stack:

```sh
kubectl kustomize .
```

Apply it only when both service-local image digests have been reviewed:

```sh
kubectl apply -k .
kubectl -n map-demo rollout status deploy/ishikari
kubectl -n map-demo rollout status deploy/biei
```

Service-only rollouts may continue to use the kustomizations below:

- `demo-deploy/ishikari/runtime/k8s/overlays/gke`
- `demo-deploy/biei/runtime/k8s/overlays/gke`

The root composition is also the input for future cross-service manifest and
smoke tests. Shared resources should move here only when they are genuinely
owned by the stack rather than one service.
