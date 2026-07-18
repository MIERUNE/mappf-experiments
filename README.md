# MIERUNE Map Platform rendering stack

This monorepo contains Biei, Ishikari, their simulators, and the reusable map
processing libraries behind them.

- [Biei](servers/biei/README.md) is the distributed MapLibre Native renderer.
- [Ishikari](servers/ishikari/README.md) serves and caches PMTiles, styles,
  glyphs, sprites, and derived terrain products.

Biei resolves production map resources through Ishikari. The services share a
repository and Cargo lockfile, while retaining separate binaries, deployment
lifecycle, cluster membership, scaling policy, and failure semantics.

## Repository layout

```text
crates/
  biei-core/       # renderer, scheduling, and Biei runtime
  ishikari-core/   # PMTiles/resource serving and routing
  mmpf-terrain/    # MIERUNE Map Platform terrain products
servers/
  biei/            # thin Biei executable
  ishikari/        # Ishikari composition root and executable
sims/
  biei-sim/
  ishikari-sim/
demo-deploy/       # per-service images/manifests and combined deployment docs
issues/            # service-scoped engineering backlogs
specs/             # service-scoped specifications
integration/       # cross-service contract and manifest checks
.github/workflows/ # service-scoped and stack CI
Cargo.toml          # one Cargo workspace
Cargo.lock          # one reproducible dependency graph
```

The dependency direction is `servers/sims -> crates`. Domain policy stays in
its owning core crate; similarly named membership, routing, drain, metrics, and
fetch modules are not assumed to have identical semantics.

## Development

Run a service slice without compiling the unrelated native stack:

```sh
cargo test -p biei-core -p biei -p biei-sim
cargo test -p ishikari-core -p ishikari -p ishikari-sim -p mmpf-terrain --all-targets
```

`cargo test --workspace` is also supported. Biei enables reqwest transfer
decompression, while Ishikari explicitly disables it on representation-
preserving clients so workspace feature unification cannot alter proxy bytes.

## Build and deployment

Each service keeps its own image and BuildKit cache:

```sh
gcloud builds submit --config demo-deploy/ishikari/runtime/cloudbuild.yaml .
gcloud builds submit --config demo-deploy/biei/runtime/cloudbuild.yaml .
```

The root `kustomization.yaml` composes the shared Gateway and both GKE service
overlays. See [demo-deploy/README.md](demo-deploy/README.md).

LICENSE: MIT OR Apache-2.0
