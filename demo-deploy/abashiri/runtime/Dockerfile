# Release image for the pure-Rust Abashiri management API.
FROM rust:1.96-slim-trixie@sha256:31ee7fc65186be7e0e0ccb3f2ca305f14e4739e7642a1ae65753aa5d7b874523 AS chef
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo install --locked --version 0.1.77 cargo-chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo chef cook --release --locked -p abashiri --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates/abashiri-core/Cargo.toml crates/abashiri-core/Cargo.toml
COPY crates/biei-core/Cargo.toml crates/biei-core/Cargo.toml
COPY crates/ishikari-core/Cargo.toml crates/ishikari-core/Cargo.toml
COPY crates/mmpf-auth/Cargo.toml crates/mmpf-auth/Cargo.toml
COPY crates/mmpf-cluster/Cargo.toml crates/mmpf-cluster/Cargo.toml
COPY crates/mmpf-common/Cargo.toml crates/mmpf-common/Cargo.toml
COPY crates/mmpf-http/Cargo.toml crates/mmpf-http/Cargo.toml
COPY crates/mmpf-mln-filesource/Cargo.toml crates/mmpf-mln-filesource/Cargo.toml
COPY crates/mmpf-pmtiles/Cargo.toml crates/mmpf-pmtiles/Cargo.toml
COPY crates/mmpf-terrain/Cargo.toml crates/mmpf-terrain/Cargo.toml
COPY servers/abashiri/Cargo.toml servers/abashiri/Cargo.toml
COPY servers/biei/Cargo.toml servers/biei/Cargo.toml
COPY servers/ishikari/Cargo.toml servers/ishikari/Cargo.toml
COPY sims/biei-sim/Cargo.toml sims/biei-sim/Cargo.toml
COPY sims/ishikari-sim/Cargo.toml sims/ishikari-sim/Cargo.toml
COPY crates/abashiri-core/src crates/abashiri-core/src
COPY crates/mmpf-cluster/src crates/mmpf-cluster/src
COPY crates/mmpf-http/src crates/mmpf-http/src
COPY servers/abashiri/src servers/abashiri/src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --locked -p abashiri \
    && cp target/release/abashiri /abashiri

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:d97bc0a941b8d4be647dc0ee75b264ddbb772f1ac5ba690a4309c00723b23775
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /abashiri /abashiri
EXPOSE 8080
ENTRYPOINT ["/abashiri"]
