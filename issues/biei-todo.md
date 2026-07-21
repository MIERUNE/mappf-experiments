# Biei Work Queue

This file contains unresolved Biei-specific product and operational decisions. Durable behavior belongs in [`../specs/biei-spec.md`](../specs/biei-spec.md), cross-cutting structural work belongs in [`../refactor.md`](../refactor.md), and missing upstream bindings belong in [`mln-rs-wishlist.md`](mln-rs-wishlist.md). Delete completed items; git history is the archive.

## Evidence-gated product work

- Add URL marker images or optional text-layer pin labels only when a concrete compatibility requirement justifies the extra resource and lifecycle handling. Current pins intentionally render labels into request-local bitmaps.
- Add public application-generated ETags and `If-None-Match`/`304` handling only if CDN or gateway validators are insufficient for measured traffic.
- Add persistent FileSource caching only if restart measurements show enough benefit to justify a disk cache and its invalidation policy.
- Add subprocess isolation only if process-level recovery proves insufficient for observed MapLibre Native crashes.
- Propagate per-render context into the process-global FileSource callback only if aggregate resource metrics, cancellation, and global timeouts cannot explain real render behavior.
- Add a per-peer gossip-age metric only if existing membership and readiness signals cannot diagnose an operational incident.
- Optimize cold style JSON double parsing only if setup profiles show material CPU or latency cost.
- Add Helm or broader production packaging policy only if Biei moves beyond the current deployment-demo scope.

## Unresolved decisions

- Which local, fast provider fixture should be the standard for reproducible throughput measurements?
