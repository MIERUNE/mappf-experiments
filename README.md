# Map Platform Experiments

[![Ishikari CI](https://github.com/MIERUNE/mappf-experiments/actions/workflows/ishikari-ci.yml/badge.svg)](https://github.com/MIERUNE/mappf-experiments/actions/workflows/ishikari-ci.yml)
[![Biei CI](https://github.com/MIERUNE/mappf-experiments/actions/workflows/biei-ci.yml/badge.svg)](https://github.com/MIERUNE/mappf-experiments/actions/workflows/biei-ci.yml)
[![Runtime E2E](https://github.com/MIERUNE/mappf-experiments/actions/workflows/runtime-e2e.yml/badge.svg)](https://github.com/MIERUNE/mappf-experiments/actions/workflows/runtime-e2e.yml)

This repository explores how to build a scalable map platform for serving, caching and rendering web maps.

> [!WARNING]
> This is an experimental, proof-of-concept project. The behavior, API, and configuration are not stable.

- [Biei](servers/biei/README.md) is a scalable static map and raster tile renderer powered by MapLibre Native.
- [Ishikari](servers/ishikari/README.md) is a distributed cache proxy for PMTiles archives and MapLibre style resources.

They can run independently or together, with Ishikari supplying resources either directly to web browsers or to Biei for server-side rendering.

## Project documents

- [Specifications and design status](specs/README.md)
- [Product-specific work queues](issues/README.md)
- [Cross-cutting refactoring queue](issues/refactor.md)

LICENSE: MIT OR Apache-2.0
