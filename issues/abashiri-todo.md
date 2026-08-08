# Abashiri Decision Queue

Status: **experimental authenticated style GET/PUT implemented over the object-store auth registry, trusted startup catalog, conditional storage, and durable mutation journal; production gates remain open.**

This queue tracks decisions that block useful Abashiri functionality. Matching another platform's URL or response contract is not a goal. External products may inform capability discovery, but Abashiri owns its HTTP and resource model.

## Settled constraints

- Management credentials use `Authorization: Bearer`. Query-string credentials are not part of the management baseline.
- Accounts are management ownership and authorization boundaries. They may administer delivery namespaces, but the two remain distinct domain types and physical object-store prefixes define neither.
- Client-visible optimistic concurrency and object-store conditional writes work together. Mutations never silently fall back to unconditional overwrite.
- PMTiles upload, validation, activation, replacement, and rollback form one native publication workflow. TileJSON and tile bytes remain delivery concerns.
- Delivery polling remains authoritative. Biei and Ishikari expose a bounded internal style-refresh receiver; Abashiri may notify it only after a durable commit. Trusted-cluster networking is the baseline trust edge, while untrusted deployments add mesh mTLS.
- Object deletion is not a product operation. GCS nevertheless requires `storage.objects.delete` for conditional replacement, so the publisher receives it only on the published-state bucket. The append-only journal does not.
- Lifecycle expiry applies only to the capability-probe prefix. Mutation journals and published current state retain the audit and idempotency evidence required for safe recovery and must be exempt.
- The style writer preserves submitted bytes and conditionally writes the catalog-resolved object. Durable metadata references the audit intent so a retry can finish a missing completion without publishing again. An incomplete retry never overwrites a newer mutation.
- The first publisher-auth adapter uses one revisioned object-store `current.json`, stores only domain-separated credential digests, and grants exact management actions on bounded account IDs. It is separate from delivery authentication.
- The management-auth registry uses a read-only Abashiri serving identity and a separate bootstrap writer. Prefer a dedicated bucket or container rather than treating an object prefix as a portable IAM boundary.
- Published delivery state and the private mutation journal use separate remote buckets or authorities. Ishikari may read the state bucket; its identity never receives journal access.
- Direct style publication is the initial lifecycle. Drafts are optional future product functionality, not a prerequisite for style CRUD.
- The first native route is `/accounts/{account_id}/styles/{style_id}`. GET requires `style.read`; PUT requires `style.publish`, one idempotency key, and an explicit create or replacement precondition. Its opaque ETag retains both backend ETag and object version.

## Required before production enablement

- Add background reconciliation for unfinished mutation intents that receive no client retry.
- Replace process-only readiness before enabling mutations: initialize required identity adapters and storage, perform a non-mutating availability check, and keep the mutating capability probe outside `/readyz`.
- Run `abashiri check-storage` against production-like GCS published state with the serving identity. Record duplicate-create rejection, stale replacement, object validators, required attributes, and GCS's replacement permission. Verify journal create/read isolation through a real authenticated mutation without granting journal replacement. Configure lifecycle expiry for retained probe objects.
- Publish a small OpenAPI document from the implemented native surface. It must describe Bearer authentication, preconditions, content types, error envelopes, and wildcard/path behavior accurately rather than anticipating unimplemented routes.

## Next capability decisions

- **Collections:** define stable ordering, bounded page size, separate summary representations, and an opaque continuation cursor backed by the authoritative catalog. Ishikari intentionally does not enumerate object-storage prefixes.
- **Delivery credentials:** decide create, rotate, disable, and list semantics. Creation must account for a one-time raw secret without storing that secret in the generic durable audit record. A retried completed request may need an explicit completed-without-secret result.
- **PMTiles publication:** test realistic archive sizes before choosing direct streaming versus resumable object-store staging. Define checksum, validation, activation, replacement, rollback, and abandoned-upload cleanup. Do not proxy multi-gigabyte bodies through Abashiri by default.
- **Sprites:** choose whether to retain individual icon sources or make crop-and-repack authoritative while preserving `sdf`, `content`, `stretchX`, `stretchY`, and `@2x`. Decide the object identity for named sprite bundles before publishing a URL that assumes only one unnamed bundle.
- **Draft styles:** add them only when Console editing needs preview-before-publish or collaboration. A draft must be structurally unreachable from Ishikari and Biei, and publishing must conditionally produce a new delivered revision.
- **Human identity:** add OIDC and session handling before human mutation routes. Recognized invalid credentials must never fall through to another adapter.
- **Fonts:** defer ingest routes until MMPF owns TTF/OTF ingestion and glyph generation rather than only proxying pre-generated glyph PBFs.
- **Rate limiting:** introduce explicit read/write budgets and `429` behavior only when capacity or abuse evidence justifies them.

## Not planned

- Wire-level compatibility with another management API without a concrete external client.
- Query-string management credentials.
- Implicit last-writer-wins mutation semantics.
- Style ZIP export, embeddable HTML, WMTS generation, or arbitrary protection flags without a product workflow.
