# syntax=docker/dockerfile:1.7

# 24.04, NOT 26.04: the prebuilt mbgl-core amalgam is ABI-sensitive to the
# Linux userspace generation. It builds and links on newer Ubuntu releases, but
# has been observed to crash at render time there. Keep build and runtime on the
# same 24.04 base unless mbgl is built from source.
FROM ubuntu:24.04@sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90 AS chef

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl git \
        build-essential cmake pkg-config \
        libssl-dev \
        libcurl4-openssl-dev libjpeg-dev libpng-dev libwebp-dev \
        libgl1-mesa-dev libegl1-mesa-dev \
        libuv1-dev libicu-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain stable --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /src

# Keep dependency compilation in a source-independent layer. Cloud Build
# exports this intermediate layer to Artifact Registry, so ordinary source
# edits reuse both Rust dependencies and the downloaded mbgl amalgam.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo install --locked --version 0.1.77 cargo-chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /src/recipe.json recipe.json

# MLN_PRECOMPILE=1 downloads the prebuilt mbgl-core amalgam instead of compiling
# it from source. `cargo chef cook` primes target/ using only dependency inputs;
# the final build then recompiles the workspace crates but not their dependency
# graph when Cargo.toml/Cargo.lock are unchanged.
ENV MLN_PRECOMPILE=1
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo chef cook --release --locked -p biei --features gl-opengl \
        --recipe-path recipe.json

# The dependency skeleton already contains placeholder sources for every
# workspace member. Copy every manifest for lockfile validation, then only the
# real Biei production sources so simulator and Ishikari edits do not invalidate
# this application layer.
COPY Cargo.toml Cargo.lock ./
COPY crates/biei-core/Cargo.toml crates/biei-core/Cargo.toml
COPY crates/ishikari-core/Cargo.toml crates/ishikari-core/Cargo.toml
COPY crates/mmpf-cluster/Cargo.toml crates/mmpf-cluster/Cargo.toml
COPY crates/mmpf-common/Cargo.toml crates/mmpf-common/Cargo.toml
COPY crates/mmpf-http/Cargo.toml crates/mmpf-http/Cargo.toml
COPY crates/mmpf-mln-filesource/Cargo.toml crates/mmpf-mln-filesource/Cargo.toml
COPY crates/mmpf-pmtiles/Cargo.toml crates/mmpf-pmtiles/Cargo.toml
COPY crates/mmpf-terrain/Cargo.toml crates/mmpf-terrain/Cargo.toml
COPY servers/biei/Cargo.toml servers/biei/Cargo.toml
COPY servers/ishikari/Cargo.toml servers/ishikari/Cargo.toml
COPY sims/biei-sim/Cargo.toml sims/biei-sim/Cargo.toml
COPY sims/ishikari-sim/Cargo.toml sims/ishikari-sim/Cargo.toml
COPY crates/biei-core/src crates/biei-core/src
COPY crates/mmpf-cluster/src crates/mmpf-cluster/src
COPY crates/mmpf-common/src crates/mmpf-common/src
COPY crates/mmpf-http/src crates/mmpf-http/src
COPY crates/mmpf-mln-filesource/src crates/mmpf-mln-filesource/src
COPY servers/biei/src servers/biei/src
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo build --release --locked -p biei --features gl-opengl \
    && cp target/release/biei /biei

FROM ubuntu:24.04@sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90 AS runtime

# Mesa EGL + software GL (llvmpipe). The `opengl` backend renders through an EGL
# surfaceless context — no GPU, no X server.
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        libegl1 libegl-mesa0 libgl1 libgl1-mesa-dri libgbm1 \
        libcurl4t64 libjpeg-turbo8 libpng16-16t64 libwebp7 libuv1t64 libicu74 \
        ca-certificates \
        fonts-dejavu-core \
    && groupadd --gid 10001 biei \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin biei \
    && mkdir -p /var/cache/biei /tmp/biei-cache \
    && chown -R 10001:10001 /var/cache/biei /tmp/biei-cache /home/biei \
    && rm -rf /var/lib/apt/lists/*

ENV EGL_PLATFORM=surfaceless \
    LIBGL_ALWAYS_SOFTWARE=1 \
    GALLIUM_DRIVER=llvmpipe \
    HOME=/home/biei \
    XDG_CACHE_HOME=/tmp/biei-cache

COPY --from=build --chown=10001:10001 /biei /usr/local/bin/biei

# Public HTTP ingress + cluster-internal listener (/_internal/*, metrics, peer
# forwarding) + gossip.
EXPOSE 8080 9090 7946

USER 10001:10001

ENTRYPOINT ["biei"]
