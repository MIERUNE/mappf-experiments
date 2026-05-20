# syntax=docker/dockerfile:1.7

# 24.04, NOT 26.04: the prebuilt mbgl-core amalgam is ABI-sensitive to the
# Linux userspace generation. It builds and links on newer Ubuntu releases, but
# has been observed to crash at render time there. Keep build and runtime on the
# same 24.04 base unless mbgl is built from source.
FROM ubuntu:24.04 AS build

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
COPY . .

# MLN_PRECOMPILE=1 downloads the prebuilt mbgl-core amalgam instead of compiling
# it from source. Keep the build path intentionally simple: on Cloud Build,
# cargo-chef plus inline image cache was measured slower than this direct build.
ENV MLN_PRECOMPILE=1
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo build --release --locked -p biei --features gl-opengl \
    && cp target/release/biei /biei

FROM ubuntu:24.04 AS runtime

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
