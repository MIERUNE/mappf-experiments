# Decision and Work Queues

This directory is the source of truth for unresolved work. Product queues hold service-specific decisions; `refactor.md` holds cross-cutting work.

| Document | Scope |
| --- | --- |
| [`abashiri-todo.md`](abashiri-todo.md) | Abashiri management API, authentication, audit, management contract, and PMTiles publication decisions |
| [`auth-todo.md`](auth-todo.md) | Shared authentication adoption gates and unresolved security/CDN decisions |
| [`biei-todo.md`](biei-todo.md) | Biei-specific product and operational triggers; no active implementation work |
| [`ishikari-todo.md`](ishikari-todo.md) | Ishikari-specific decision-triggered experiments and operational contingencies |
| [`mln-rs-wishlist.md`](mln-rs-wishlist.md) | Unlanded `maplibre-native-rs` binding needs observed by Biei |
| [`refactor.md`](refactor.md) | Cross-cutting structural, correctness, performance, and deployment refactoring |

Durable behavior and architectural contracts belong in [`../specs/`](../specs/README.md), not in a work queue. Keep each concern in one queue, delete completed items, and use git history as the archive.
