# Issue and Work Queues

This directory contains unresolved work that is narrower than the repository-wide refactoring queue.

| Document | Scope |
|---|---|
| [`biei-todo.md`](biei-todo.md) | Biei-specific product decisions and evidence-gated follow-ups |
| [`ishikari-todo.md`](ishikari-todo.md) | Ishikari-specific experiments, product decisions, and evidence-gated follow-ups |
| [`mln-rs-wishlist.md`](mln-rs-wishlist.md) | Unlanded `maplibre-native-rs` binding needs observed by Biei |
| [`../refactor.md`](../refactor.md) | Cross-cutting structural, correctness, and performance refactoring |

Durable behavior and architectural contracts belong in [`../specs/`](../specs/README.md), not in a work queue. Keep each concern in one queue, delete completed items, and use git history as the archive.
