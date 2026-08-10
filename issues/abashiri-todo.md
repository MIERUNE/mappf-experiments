# Abashiri Decision Queue

Status: **experimental authenticated style GET/PUT implemented over the object-store auth registry, trusted startup catalog, conditional storage, durable mutation journal, and background completion reconciliation; production gates remain open.**

This queue contains only unresolved Abashiri gates and capability decisions. Implemented management, storage, audit, refresh, and deployment-composition contracts live in [`../specs/abashiri-spec.md`](../specs/abashiri-spec.md); git history retains completed work.

## Required before production enablement

- Replace process-only readiness before enabling mutations: initialize required identity adapters and storage, perform a non-mutating availability check, and keep the mutating capability probe outside `/readyz`.
- Run `abashiri check-storage` against production-like GCS published state with the serving identity. Record duplicate-create rejection, stale replacement, object validators, required attributes, and GCS's replacement permission. Verify journal create/read/list isolation and a real reconciliation scan through an authenticated mutation without granting journal replacement or deletion. Configure lifecycle expiry for retained probe objects.
- Publish a small OpenAPI document from the implemented native surface. It must describe Bearer authentication, preconditions, content types, error envelopes, and wildcard/path behavior accurately rather than anticipating unimplemented routes.

## Next capability decisions

- **Collections:** define stable ordering, bounded page size, separate summary representations, and an opaque continuation cursor backed by the authoritative catalog. Ishikari intentionally does not enumerate object-storage prefixes.
- **Delivery credentials:** decide create, rotate, disable, and list semantics. Creation must account for a one-time raw secret without storing that secret in the generic durable audit record. A retried completed request may need an explicit completed-without-secret result.
- **PMTiles publication:** test realistic archive sizes before choosing direct streaming versus resumable object-store staging. Define checksum, validation, activation, replacement, rollback, and abandoned-upload cleanup. Do not proxy multi-gigabyte bodies through Abashiri by default.
- **Sprites:** choose whether to retain individual icon sources or make crop-and-repack authoritative while preserving `sdf`, `content`, `stretchX`, `stretchY`, and `@2x`. Decide the object identity for named sprite bundles before publishing a URL that assumes only one unnamed bundle.
- **Draft styles:** add them only when Console editing needs preview-before-publish or collaboration. A draft must be structurally unreachable from Ishikari and Biei, and publishing must conditionally produce a new delivered revision.
- **Human identity:** add OIDC and session handling before human mutation routes. Recognized invalid credentials must never fall through to another adapter.
- **Console:** build the separately branded web client with runtime-configurable API and UI base paths. Start with authenticated identity and a read-only operational overview; browser mutations wait for OIDC sessions and CSRF protection. Treat aggregated status as untrusted display data and render it only through escaped text or typed components, as required by the specification.
- **Operational overview:** Biei and Ishikari expose bounded versioned internal status snapshots, and Abashiri now polls named endpoints with bounded parallelism, short single-flight freshness, and partial/stale aggregation behind `operations.read`. Next build the read-only Console representation and decide which historical or Prometheus-derived signals add value; Console must not reach delivery services or internal listeners directly.
- **Fonts:** defer ingest routes until MMPF owns TTF/OTF ingestion and glyph generation rather than only proxying pre-generated glyph PBFs.
- **Rate limiting:** introduce explicit read/write budgets and `429` behavior only when capacity or abuse evidence justifies them.
- **Reconciliation indexing:** the current sequential scan is deliberately simple and bounded in memory but performs work proportional to retained journal history every five minutes. Add a lifecycle-independent pending index or checkpoint only when measured mutation volume makes journal listing or completion probes material; the append-only audit journal itself remains authoritative and lifecycle-exempt.

## Not planned

- Wire-level compatibility with another management API without a concrete external client.
- Query-string management credentials.
- Implicit last-writer-wins mutation semantics.
- Style ZIP export, embeddable HTML, WMTS generation, or arbitrary protection flags without a product workflow.
