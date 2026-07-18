# Release image for Ishikari (pure Rust: PMTiles + HTTP).
#
# glibc build on distroless/cc: mature malloc and DNS behave well under high QPS
# with no allocator tuning. The one rule is that the builder and runtime must
# share the same Debian generation so their glibc matches — here both are
# trixie / Debian 13 (glibc 2.41): `rust:1.96-slim-trixie` and
# `distroless/cc-debian13`. Ishikari is pure Rust and compiles against trixie's
# glibc, so this newer base is safe for the demo image.
# Build multi-arch with `docker buildx --platform linux/amd64,linux/arm64` if
# the target cluster supports Arm nodes. The checked-in GKE demo currently uses
# linux/amd64 because its asia-northeast1 Autopilot cluster cannot provision Arm
# workloads.
FROM rust:1.96-slim-trixie AS chef
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo install --locked --version 0.1.77 cargo-chef

# Generate the dependency recipe independently from source contents. BuildKit
# exports the resulting cook layer to Artifact Registry, so a fresh Cloud Build
# worker can reuse it instead of recompiling the complete Rust dependency graph.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo chef cook --release --locked -p ishikari --recipe-path recipe.json

# Keep the application layer limited to production build inputs. Copy every
# workspace manifest for exact lockfile validation, then only Ishikari's real
# sources so Biei, simulator, specification, and deployment edits stay cached.
COPY Cargo.toml Cargo.lock ./
COPY crates/biei-core/Cargo.toml crates/biei-core/Cargo.toml
COPY crates/ishikari-core/Cargo.toml crates/ishikari-core/Cargo.toml
COPY crates/mmpf-terrain/Cargo.toml crates/mmpf-terrain/Cargo.toml
COPY servers/biei/Cargo.toml servers/biei/Cargo.toml
COPY servers/ishikari/Cargo.toml servers/ishikari/Cargo.toml
COPY sims/biei-sim/Cargo.toml sims/biei-sim/Cargo.toml
COPY sims/ishikari-sim/Cargo.toml sims/ishikari-sim/Cargo.toml
COPY crates/ishikari-core/src crates/ishikari-core/src
COPY crates/mmpf-terrain/src crates/mmpf-terrain/src
COPY servers/ishikari/src servers/ishikari/src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --locked -p ishikari \
    && cp target/release/ishikari /ishikari

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /ishikari /ishikari
# Public HTTP ingress + cluster-internal listener (/_internal/*, metrics, peer
# forwarding) + gossip.
EXPOSE 8080 9090 7946
ENTRYPOINT ["/ishikari"]
