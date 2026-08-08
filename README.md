# Map Platform Experiments

[![Ishikari CI](https://github.com/MIERUNE/mappf-experiments/actions/workflows/ishikari-ci.yml/badge.svg)](https://github.com/MIERUNE/mappf-experiments/actions/workflows/ishikari-ci.yml) [![Biei CI](https://github.com/MIERUNE/mappf-experiments/actions/workflows/biei-ci.yml/badge.svg)](https://github.com/MIERUNE/mappf-experiments/actions/workflows/biei-ci.yml) [![Abashiri CI](https://github.com/MIERUNE/mappf-experiments/actions/workflows/abashiri-ci.yml/badge.svg)](https://github.com/MIERUNE/mappf-experiments/actions/workflows/abashiri-ci.yml) [![Runtime E2E](https://github.com/MIERUNE/mappf-experiments/actions/workflows/runtime-e2e.yml/badge.svg)](https://github.com/MIERUNE/mappf-experiments/actions/workflows/runtime-e2e.yml)

This repository explores how to build a scalable map platform for serving, caching and rendering web maps.

> [!WARNING]
> This is an experimental, proof-of-concept project. The behavior, API, and configuration are not stable.

- [Biei](servers/biei/README.md) is a scalable static map and raster tile renderer powered by MapLibre Native.
- [Ishikari](servers/ishikari/README.md) is a distributed cache proxy for PMTiles archives and MapLibre style resources.
- [Abashiri](servers/abashiri/README.md) is the experimental management and publishing API.

Biei and Ishikari can run independently or together, with Ishikari supplying resources either directly to web browsers or to Biei for server-side rendering.

Abashiri is a separate management-plane rather than part of their delivery path.

## Project documents

- [Specifications and design status](specs/README.md)
- [Product-specific work queues](issues/README.md)

LICENSE: MIT OR Apache-2.0
