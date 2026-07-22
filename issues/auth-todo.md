# Shared Authentication Decision Queue

Status: **no auth implementation is authorized.** The proposal remains exploratory in [`../specs/auth-sketch.md`](../specs/auth-sketch.md). This file owns unresolved decisions; it is not a roadmap and does not duplicate service-specific work.

## Settled constraints

- Verify steady-state requests locally from an immutable in-memory snapshot; never query storage per token or repeat a control-plane lookup for every request. One bounded, single-flight cold registry activation is the only planned request-adjacent exception.
- If dynamic delivery credentials are adopted, use one conditionally replaced, self-contained `current.json` per configured registry under its trusted auth root. It contains the complete token verifier set and namespace/action grants for that registry; runtime readers never list a prefix, infer latest state from object names, or fetch one object per token.
- Prefer a dedicated registry bucket/container and workload identity per environment. A prefix is organization, not a portable authorization boundary; do not reuse Ishikari's content-reader identity.
- Store metadata, public verification material, high-entropy API-key verifier digests, and secret references—not raw API keys, HMAC masters, cap subkeys, or private signing keys.
- Prefer a conditional-write admin CLI/object writer before considering a public management API. Management uses cloud/workload identity; application credentials never authorize registry mutation.
- Authentication does not replace edge tenant/IP request limits. Origin-local limits cannot govern CDN cache-hit egress.
- Carry a bounded public `registry_id` in the opaque delivery key and resolve it only through a trusted local `registry_id -> auth_root` catalog. Unknown IDs cause no storage I/O. The token entry owns namespace/action grants; there is initially no namespace-to-registry allowlist or registry-level namespace ceiling because registry writing remains centrally trusted. For built-in credentials, authentication and all registry-derived policy use one captured registry revision; external AuthN may use separate immutable verifier state, but AuthZ still uses one policy snapshot.
- Do not create `mmpf-auth`, move Ishikari's `ObjectStoreRegistry`, or add a generic authenticator/storage/rate-limiter hierarchy before two server implementations prove the shared contract.
- If adopted, start with registry tooling and strong entry/expensive-route auth. Capability URLs are a later phase, not the first implementation.

## Distribution and propagation

The conditionally replaced registry `current.json` is the durable linearization point, so pods never reach consensus *among themselves* about registry state. Each independently installs only a completely validated registry snapshot. The detailed cache, peer, gossip, and internal-refresh contract lives in [`../specs/auth-sketch.md`](../specs/auth-sketch.md#10-configuration-and-registry-distribution); the remaining implementation gates are:

- **Baseline: local cache plus conditional refresh.** Each pod verifies from an immutable in-process registry snapshot. Loading and refresh are single-flight and use the configured object's strong validator or generation. One bounded cold registry load is allowed; no request fetches a per-token object.
- **Raft (and any pod-to-pod consensus) is rejected.** It would create a *second* authority to reconcile with object storage, and its stable-quorum requirement fights the deployment model (HPA-autoscaled 2–6 pods on Spot with frequent preemption → constant membership churn, election storms), while coupling auth availability to consensus health — the correlated-outage SPOF the design avoids. Raft earns its keep only when the replicas themselves *are* the durable authority with linearizable writes; here writes are out-of-band and pods are pure readers.
- **Gossip is an optional bounded advisory hint, never authority.** It carries at most one overwritable latest-update tuple per node, not one persistent key per registry and never credentials, policy bodies, or auth-root URLs. Polling remains the correctness backstop; refreshes are jittered and coalesced so a hint cannot stampede the store.
- **Peer-first loading requires content authority.** HRW may choose peers to collapse cold loads, but an unsigned peer snapshot cannot be installed solely because it came from the internal network. Either verify a management-plane signature over the exact snapshot or independently anchor its digest/generation in object storage; otherwise peer fetch saves bytes but not the authoritative read. Object storage remains the fallback.
- **Internal refresh is a hint.** A bounded internal endpoint may schedule an idempotent registry refresh, but it accepts neither auth-root URLs nor policy bodies and is protected against storage/CPU amplification by trusted-catalog checks, cooldowns, single-flight, and the internal-listener network boundary.
- **Read scopes have a loose revocation SLA, so gossip is an accelerator rather than a correctness requirement.** Read-only access to commodity map data has a low cost-of-staleness, and the real abuse ceiling is edge request/egress limits, not registry-propagation speed (see the edge-limit constraint above) — so a revocation SLA of minutes and relaxed polling remain sufficient when gossip is unavailable. Add gossip propagation when measured polling cost or the chosen revocation target justifies it. This biases the **read tier toward fail-open** on prolonged refresh failure (serve the last-good snapshot with a generous maximum age). Fail-closed is reserved for the private-data strong tier as a conscious per-scope choice (refines "Registry freshness and failure policy" below).

## Adoption gate

Before implementation, explicitly decide that built-in authentication is a product requirement rather than relying only on a customer gateway, `TrustedHeader`, or service mesh. Name the first protected routes and the operator responsible for key issuance, rotation, revocation, and incident response.

**Recommendation:** prove the registry and `StaticApiKeys` behavior on Biei's expensive `static` route and authenticated entry documents first. Do not start with Ishikari capability URLs while the CDN abuse boundary is unresolved.

## Required before registry or strong-auth implementation

1. **Registry freshness and failure policy:** choose refresh interval, maximum last-known-good age, startup behavior without a valid snapshot, and when prolonged refresh failure becomes fail-closed. The interval is the normal revocation-latency SLA.
2. **IAM, signing, and secret backend:** choose a versioned external secret backend where symmetric keys or registry-signing keys are unavoidable; define separate registry-reader, writer, verifier-secret-reader, and issuer identities, rotation overlap, audit ownership, and the dedicated production bucket/container policy. Secret references must pin immutable provider versions retained through the registry rollback window; encrypted key blobs and private signing keys are not stored in registry objects. Decide whether peer distribution is deferred or management-signed from its first release.
3. **Versioned registry schema and current-object contract:** finalize bounds for `registry_id`/audience, monotonic revision, digest, key status, verifier sets, namespace/action grants, Origin policy, and policy revision. Specify whole-candidate validation, backend-generation CAS for `current.json`, replacement and rollback behavior, and the signed envelope if peer distribution is enabled. The first schema has no namespace-to-registry allowlist or registry-level namespace ceiling; adding delegated registry writers requires revisiting that trust decision first.
4. **Backend capability matrix:** test which `object_store` backends support strong validators, conditional reads, version/generation-conditional replacement, and overwrite protection. Define how the CLI refuses unsafe multi-writer operation where those guarantees are unavailable; IAM alone does not require callers to use preconditions. Define current-object version/secret-version retention and garbage collection without deleting required rollback material.
5. **API-key contract:** define transport, bounded `registry_id`/`token_id`/secret syntax, cryptographic-entropy requirement, unambiguous verifier domain separation, one-way verifier construction, constant-time comparison, rotation overlap, one-time secret display, log redaction, and error behavior. Do not store recoverable raw API keys merely because the registry is private.
6. **Authorization grammar:** choose exact allow-only action names, service/audience values, resource kinds, and segment-aware exact/subtree selectors. Specify whether glyphs/sprites inherit a named style grant or use explicit kinds. Define the bounded non-secret policy/representation revision used when authorized principals can receive different bytes; never derive grants from bucket names or physical prefixes.
7. **Registry expiry and rollback safety:** define forward-only rollback that preserves intervening revocation tombstones and requires separately audited explicit reactivation. Decide whether the registry snapshot carries signed `not_before`/`expires_at`, clock-skew bounds, and break-glass behavior. Monotonic numbering protects a running pod from regression but does not give a fresh pod anti-replay; decide whether object-store IAM/audit is sufficient or an external monotonic/transparency anchor is required.
8. **First and second server order:** confirm the initial route set and acceptance tests, then name the second implementation that must exist before shared extraction.

**Sequencing note.** The backend-capability check (#4) is the first concrete task, not a parallel one: a short spike proving a generation-conditional replacement of registry `current.json` rejects a stale writer and that conditional reads return a trustworthy unchanged result on the *actual* production backend (GCS) must pass before committing to the schema or any server code. It converts the foundational CAS/validator assumption into a tested fact; if it fails, the registry-current model itself needs rework, so nothing downstream should start first.

## Deferred capability/CDN decisions

These block capability implementation but do not block registry tooling or strong entry-route authentication:

- Exact canonical payload and bounded parser for `version`, audience, `kid`, `key_id`, epoch, and policy revision; MAC length must provide at least 128 effective bits.
- Capability signing topology: per-key epoch subkeys versus a shared epoch key versus asymmetric signatures. A verifier pod that loads many symmetric keys can forge for every loaded key during the live epochs; per-key derivation does not make whole-pod compromise customer-local.
- Web and native-client epoch lengths. Accepting current and previous epochs permits a capability minted near an epoch start to live for almost two epochs.
- CDN cache-key behavior for the full capability URL and `Origin`, including absent/null/non-origin-form handling and 4xx caching.
- CDN-side request/egress controls, usage alerts, and kill switches. Per-pod token buckets protect only origin traffic and vary with replica count.
- Capability-bearing URL retention and redaction in CDN, Gateway, browser/client, support, and analytics logs.
- Emergency revocation semantics across registry refresh, origin snapshots, accepted epoch overlap, CDN expiry, and explicit purge.
- Whether capability rewriting covers third-party provider URLs or only self-hosted resources.
- Private-data policy: which scopes are always `no-store` and remain on the strong-auth path.
- Offline/bulk export contract; do not turn long-lived tile capabilities into an unbounded download API.

## Revisit object storage only with evidence

A database, public issuance API, global token registry, or push-invalidation subsystem is deferred. Reconsider only if per-registry current-object contention, measured registry size/decode cost, audit/query needs, secret fan-out, or the measured revocation SLA cannot be met by complete registry snapshots, conditional refresh, bounded gossip hints, and optional peer-assisted loading.
