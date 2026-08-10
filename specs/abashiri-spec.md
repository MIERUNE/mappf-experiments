# Abashiri management API

Status: **experimental authenticated style retrieval/publication, background completion reconciliation, and advisory refresh notification implemented; production readiness and backend qualification remain open.**

Abashiri is MMPF's separate management and publishing API. MMPF Console is a client of this API, not a second mutation path. Biei and Ishikari remain read-only delivery applications.

## 1. Native management contract

### 1.1 Capability coverage, not wire compatibility

Abashiri should provide the management capabilities needed to operate a map platform. Another platform's feature set is useful for discovering missing workflows, but its paths, payloads, status-code quirks, credential transport, and lifecycle model are not Abashiri's contract.

MMPF Console is the initial client and can follow a coherent native API. A separate compatibility adapter may be added later for a concrete external client, with fixtures owned by that adapter. Compatibility requirements must not weaken the native API's authentication, concurrency, or storage invariants.

This document governs management only. Style, TileJSON, tile, glyph, sprite, static-map, and static-tile delivery remain Ishikari and Biei concerns.

### 1.2 Resource model

The management model starts with resources rather than URL shapes:

| Resource | Responsibility |
| --- | --- |
| Account | Management ownership and authorization boundary |
| Style | Mutable MapLibre style document and its published revision |
| Sprite bundle | Immutable, content-addressed assets referenced by a style |
| Tileset | Logical identity, metadata, and active PMTiles publication |
| Delivery credential | Grant consumed by delivery services |
| Publication job | Upload, validation, activation, and failure state for large artifacts |

An `account_id` may administer one or more delivery namespaces, but account and namespace remain distinct domain types. Physical object-store prefixes define neither identity. An authoritative Abashiri catalog resolves logical resources to trusted storage locations; handlers never derive a path by concatenating untrusted identifiers.

The first native resource path is `/accounts/{account_id}/styles/{style_id}`. Other HTTP paths are deliberately not frozen yet; they should expose the resource hierarchy directly and consistently rather than copying another service's versioned path families.

The initial trusted style catalog is one bounded startup JSON snapshot:

```json
{
  "schema_version": 1,
  "styles": [
    {
      "account_id": "example",
      "style_id": "basic",
      "object_path": "styles/delivery/basic/style.json"
    }
  ]
}
```

Duplicate logical entries and aliases that map multiple resources onto one object are rejected before listening. A style location must use either `styles/{namespace}/{style_id}/style.json` or the flat `styles/{namespace}/{style_id}.json` form; both produce the same canonical delivery key. The snapshot is not prefix discovery and is not refreshed in this first slice; changing it requires a restart.

### 1.3 HTTP principles

- Management credentials use `Authorization: Bearer`. Query-string credentials are not part of the baseline because URLs routinely reach logs and telemetry.
- Any client-supplied account identity is an assertion checked against the authenticated principal, never an authorization source by itself.
- Mutations accept an idempotency key and expose explicit optimistic concurrency. Replacement and deletion require the current validator; stale writers receive a conflict or precondition response instead of silently overwriting newer state.
- Collections use a stable order, bounded page size, and opaque continuation cursor. Clients do not depend on storage-prefix enumeration or cursor internals.
- Resource and collection-summary representations are distinct types. A consistent structured error envelope carries a stable code, human-readable message, and request identifier.
- Management responses containing identity or mutable state use `Cache-Control: no-store`.
- Unknown routes return `404`. Rate limits and `429` responses are introduced only with an explicit capacity or abuse-control policy, not copied by habit.

The first style endpoints are:

- `GET /accounts/{account_id}/styles/{style_id}` — requires `style.read`, returns exact published bytes and the current opaque strong `ETag`;
- `PUT /accounts/{account_id}/styles/{style_id}` — requires `style.publish`, one bounded `Idempotency-Key`, `application/json`, and either `If-None-Match: *` for create or the Abashiri ETag in `If-Match` for replace.

The opaque ETag encodes both backend ETag and object version. This preserves GCS generation-based conditional writes and avoids pretending an ordinary content ETag is sufficient on every backend. Successful creation returns `201`; replacement and a completed idempotent replay return `200`. Precondition failure returns `412`, idempotency-key reuse returns `409`, and structured error responses include `code`, `message`, and `request_id`. Management responses use `Cache-Control: no-store`. Successful PUT responses also report advisory refresh delivery as `delivered`, `partial_failure`, or `not_configured`.

### 1.4 Style lifecycle

The first style workflow creates, retrieves, and conditionally replaces the published style. A successful mutation produces a content-derived `StyleRevision` and may send the advisory refresh hint described in §4.

Drafts are optional product functionality, not a prerequisite for style CRUD. If the Console later needs collaborative editing or preview-before-publish, a draft becomes a separate management resource that is never addressable through Ishikari or renderable by Biei. Publishing it conditionally replaces the delivered style and creates a new revision. No special path convention is reserved before that need exists.

### 1.5 Capability boundary

Abashiri is expected to manage styles, sprite publication, tileset metadata and PMTiles publication, and delivery credentials. Font ingest is deferred until MMPF owns TTF/OTF ingestion and glyph generation rather than only proxying pre-generated glyph PBFs.

Style ZIP downloads, embeddable HTML, WMTS generation, and arbitrary vendor-specific protection flags are not management priorities. They should be added only for a concrete MMPF workflow.

### 1.6 Sprites: one bundle per style, addressed by content

A style-management workflow initially owns one sprite bundle. The proposed `{sprite_id}` segment exists so that styles do not break when a sprite is updated beneath them, which is the same reason [`resource-layout-sketch.md`](resource-layout-sketch.md) §5 derives a SHA-256 `sprite_id` over the whole bundle. The intended retrieval path is `styles/{namespace}/{style_id}/sprites/{sprite_id}{@2x}.{json,png}`; Ishikari currently exposes only its provider-proxy `styles/{namespace}/{style_id}/sprite{@2x}.{json,png}` route, so the content-addressed route is not yet a delivery contract.

The API may offer convenient per-icon mutations while retaining whole-bundle publication. Such a mutation is a bundle-level read-modify-write: read the bundle the style currently references, apply the icon change, rebuild, derive the new `sprite_id`, publish the new members create-only, conditionally repoint the style, and return the updated sprite document. Every published member stays immutable and create-only.

Two decisions remain:

- **Icon sources.** Rebuilding needs individual icons, but only a packed sheet is stored. Icons are recoverable by cropping via the sprite JSON's `x`, `y`, `width`, `height`, and `pixelRatio`, but `sdf`, `content`, `stretchX`, `stretchY`, and `@2x` consistency must survive the round trip. Either retain source images alongside the bundle or make crop-and-repack authoritative.
- **Identifier naming.** MMPF's `sprite_id` is a content digest, whereas the MapLibre style specification also allows `sprite` to be an _array_ whose `id` is a logical name used in layer references (`roadsigns:stop_sign`, with `default` unprefixed). One bundle per style satisfies §1.5 but is a strict subset of what a MapLibre style can express. If named sprites are supported, the object path needs both a name and a digest; changing it later breaks published URLs.

## 2. PMTiles publication

Abashiri publishes already-built PMTiles archives as durable tilesets. Upload, archive validation, activation, replacement, and rollback form one native publication workflow. Tileset identity and metadata belong to the same resource; TileJSON and tile bytes remain Ishikari delivery concerns.

The final HTTP path and transfer protocol remain open. Before freezing them, the design must settle:

- direct request bodies versus resumable, object-store-native staging;
- maximum archive size and checksum verification;
- create versus replace semantics and expected-version preconditions;
- the atomic transition from uploaded bytes to a readable tileset;
- metadata validation, namespace authorization, audit records, and cleanup of failed or abandoned uploads.

The delivery URL remains an Ishikari concern. A management publication API must not be inferred from the delivery route shape.

## 3. Durable-write invariant

Every create or mutation uses an object-store precondition. Creation rejects an existing target. Replacement and deletion require the current client-visible validator and carry the corresponding object-store version or ETag. A stale writer is rejected; an unconditional storage overwrite is not an acceptable fallback.

Backend support is an operational capability, not an assumption derived from a Rust trait. Before writer routes are enabled in an environment, the `abashiri check-storage` probe must pass against the published-state backend and serving identity. The append-only journal path is verified by a real authenticated mutation; it must not receive replacement permission merely to satisfy a probe for semantics it does not use.

Published state and the mutation journal use independently configured object-store roots. In a remote deployment they must resolve to different buckets or authorities, not merely different prefixes: the delivery identity needs read access to published styles but must not be able to read actor identities, request IDs, timestamps, input digests, or backend version evidence from the private journal. The authentication registry remains a third independently authorized root.

The probe is explicit rather than a startup or readiness action because it mutates storage. It creates a unique hidden object below `.abashiri-capability-check/`, proves duplicate-create and stale-update rejection, and verifies the successful body plus the content type, cache policy, and custom metadata needed by writer recovery. A lifecycle rule expires retained probes, avoiding explicit cleanup. Backend overwrite rules still apply: GCS requires `storage.objects.delete` for conditional replacement, so the publisher receives that permission only on the published-state bucket. Explicit cleanup uses the same permission.

Lifecycle expiry is confined to `.abashiri-capability-check/`. The mutation journal and published current-state paths are lifecycle-exempt: deleting them removes the evidence needed to distinguish retries, conflicts, and completed audit records. A future delete API must publish a durable tombstone or versioned pointer rather than making an old create indistinguishable from one that never committed.

## 4. Service boundary

Abashiri:

- currently authenticates narrow publisher credentials through its object-store registry, and may later add human OIDC or workload-identity adapters;
- authorizes and audits durable management mutations;
- coordinates publication and conditionally updates durable current-state documents.

Abashiri does not:

- serve tiles, styles, glyphs, sprites, or rendered images;
- join Biei or Ishikari membership clusters;
- depend on delivery-node liveness for mutation correctness;
- expose management routes through delivery CDN cache policy.

Delivery services retain bounded polling as the correctness path. Biei and Ishikari additionally implement the same advisory receiver:

```text
POST /_internal/refresh/style
{ "schema_version": 1, "hint_id": "<bounded-id>", "style_id": "namespace/style" }
```

The receiver accepts only a bounded logical style identity and hint id. It accepts no URL, credential, bytes, version authority, or cache policy. Each pod resolves the style through trusted local configuration and merely makes the next ordinary request revalidate through its existing validation and single-flight path. Biei retains its 10-second minimum provider-fetch interval; a hint pulls the check forward only to the earliest permitted opportunity. A fixed 16-slot ring per publishing node propagates the hint inside each service's own gossip cluster without unbounded membership state. Ring overwrite, loss, or delivery after a newer hint is harmless because polling remains authoritative.

Abashiri does not join either gossip cluster. After durable completion, the style PUT route posts the same idempotent hint to every configured Biei or Ishikari internal endpoint in parallel. The mutation-key digest is the `hint_id`; receivers fan the hint out locally. Endpoint count and per-request time are bounded, redirects are disabled, and only the fixed internal receiver path is accepted. Hint failure never rolls back or obscures a durable commit. The response reports partial delivery, and replaying the identical mutation with the same idempotency key retries the hint without rewriting style state. Delivery polling remains the correctness path. Notifier-enabled startup validates every catalog delivery path against the shared hint envelope. A deployment without a notifier does not reject an otherwise valid object path merely because it cannot be transported as a hint.

This introduces narrow reachability from Abashiri to the two internal listeners. In a trusted Kubernetes network the internal-listener boundary is the application trust edge. A deployment on an untrusted network supplies service-mesh mTLS and network policy rather than adding credentials to the hint body. Public listeners never expose the route.

Sensitive management responses use `Cache-Control: no-store`. The experimental server binds to loopback by default and exposes mutation routes only when auth, state storage, and the trusted catalog are all configured.

### 4.1 Deployment composition

Biei and Ishikari are independently deployable delivery applications. Each starts, serves, scales, and recovers without Abashiri or Console, and neither discovers nor calls either management component. They expose bounded, versioned integration interfaces for optional consumers; Abashiri depends on those interfaces for delivery observation and advisory acceleration rather than depending on gossip internals or process implementation details. Abashiri absence, incompatibility, or failure cannot change delivery correctness.

Both delivery services expose `GET /_internal/operations/v1/status` on their internal listener using the shared operational provenance envelope and service-owned payloads. The `/_internal` prefix identifies the listener trust boundary, while the versioned `operations` path keeps this consumer contract separate from deliberately unstable peer protocols. The response is current observation rather than cluster consensus, identifies local or clustered mode, caps each service's reported membership at 256 node identities in total, marks incomplete or truncated views, excludes raw gossip state and resource identities, and permits only a two-second private freshness window. Abashiri's configured adapter accepts at most eight named exact endpoint URLs, polls them in parallel with a one-second deadline and 128 KiB response bound, and single-flights a two-second local observation. Failed sources are returned as `unavailable`, or with a last-known-good snapshot marked `stale` for at most five minutes. Endpoint URLs and transport errors are not part of the management response. Delivery services do not register with Abashiri or push periodic state to it.

MMPF Console is a dedicated Abashiri client. It never calls Biei, Ishikari, object storage, or cluster-internal listeners directly; Abashiri authenticates the user, authorizes the operation, and aggregates any delivery status exposed through configured service adapters. Service-owned status payloads remain untrusted display data: Console renders them through escaped text or typed components and never injects serialized status as HTML or JavaScript source. A Console failure therefore affects only the user interface, while an Abashiri failure affects management availability but not delivery.

Hostname and path layout are deployment composition rather than named application profiles. A deployment may omit Abashiri and Console, expose management on a private origin, give Console and Abashiri different origins, colocate them on one management origin, or mount them alongside delivery routes. MMPF Console receives its API base URL and UI base path from runtime configuration. Abashiri receives its canonical public URL, external API mount path, and exact allowed Console origins from configuration; neither application hard-codes a MIERUNE hostname or infers security-sensitive URLs from untrusted forwarding headers.

Same-origin Console/API deployment avoids credentialed CORS and is the simplest default, but it is not an architectural requirement. A cross-origin deployment uses an exact origin allowlist, credentialed CORS, and cookie attributes appropriate to the configured sites; wildcard CORS is never used with management sessions. OIDC redirects always use Abashiri's configured canonical public URL.

When management shares an origin with delivery, the configured API and Console prefixes route before every delivery wildcard, bypass delivery CDN caching, and never fall through to Biei, Ishikari, or the Console SPA. Management responses remain `private, no-store`, and the host-only human session cookie is restricted to the external API mount path so map-resource requests do not carry it. A private deployment simply omits management routes from the public delivery Gateway. The checked-in GKE demo currently exposes Abashiri only inside the cluster and has no Console.

### 4.2 Object-storage management authentication

Abashiri's first built-in publisher adapter is a management-only bearer registry rooted at the configured `ABASHIRI_AUTH_ROOT`. The root contains one complete, revisioned `current.json`:

```json
{
  "schema_version": 1,
  "revision": 1,
  "credentials": [
    {
      "credential_sha256": "<domain-separated digest>",
      "enabled": true,
      "actor": {
        "kind": "workload",
        "issuer": "object-store",
        "subject": "publisher"
      },
      "accounts": ["example"],
      "actions": ["operations.read", "style.read", "style.publish"]
    }
  ]
}
```

The object contains no raw bearer credential. `abashiri hash-credential` reads one credential from stdin and emits the digest used by this schema. Actors, accounts, actions, credential count, and snapshot bytes are bounded and validated before installation. Credentials must be independently generated with at least 256 bits of entropy; the parser's 32-byte minimum is a transport bound and cannot distinguish random material from a weak repeated string.

The serving workload needs read-only access to this registry. Registry publication is a separate bootstrap administration capability; an ordinary Abashiri publisher credential and the Abashiri serving identity must not be able to rewrite their own authentication policy. Prefer a dedicated bucket or container and reader identity because an object prefix alone is not a portable IAM boundary.

The server loads a valid snapshot before listening, verifies subsequent requests from the in-memory snapshot, and refreshes at most once per 60 seconds under a single-flight lock. A due refresh that cannot read and validate object storage fails closed; it does not serve an indefinitely stale management grant. A credential disabled in the registry may therefore remain valid for up to 60 seconds while the cached snapshot is still fresh. This bounded revocation lag is part of the initial adapter's security contract, not an immediate-revocation guarantee. A five-second failure cooldown prevents rejected requests from amplifying an outage into one object-store read each. Running processes reject revision rollback and changed bytes at the same revision. Fresh-process anti-replay still depends on the configured object-store IAM, version-retention, and audit policy.

This registry is not a delivery-auth registry. Its bearer credentials are accepted only by Abashiri and never forwarded to or accepted by Biei or Ishikari. `style.read` and `style.publish` are checked together with the target account and therefore require at least one account grant. The global `operations.read` action authorizes the cross-service operational overview and may be the sole action on a credential with an empty account list; it is not made account-scoped by inventing a false resource boundary. The initial transport is one `Authorization: Bearer` header. Query-string management credentials are intentionally unsupported. Human OIDC sessions and external workload-identity adapters remain separate decisions. The object-store adapter accepts only `workload` actors; a registry entry cannot self-identify a bearer credential as a human session.

## 5. Audit invariant

Cloud-provider audit logs identify the workload identity that writes object storage, but they cannot identify the human or publishing principal represented inside Abashiri. They supplement rather than replace the application audit record.

Each accepted mutation uses one bounded idempotency key and follows this order:

1. create an immutable mutation intent containing actor, action, target, request/trace identity, and redacted input identity;
2. conditionally commit state that references the mutation intent;
3. write the immutable completion outcome;
4. report success only after the completion outcome is durable.

If state commits but the completion write fails, Abashiri returns an error and reconciliation completes the audit record. A retry with the same idempotency key first checks resource state for that mutation; if no state was written, it re-attempts the original conditional operation. Concurrent identical attempts that lose the conditional-write race re-read state and succeed only when its opaque mutation reference and content identity match. Orphaned intents for changes that never committed are retained as failed or abandoned attempts. Raw delivery tokens, private keys, and uploaded content bodies never enter audit records. Reusing an idempotency key with a different actor, target, action, or canonical input identity is rejected rather than treated as a retry.

The initial `abashiri-core` journal implements the immutable intent and completion ordering in its private journal store. It stores only domain-separated digests of the idempotency key, canonical input, and committed state. A route may also persist a bounded non-secret state locator needed to find committed state after configuration changes; style publication records the already catalog-validated delivery path. A completion contains the committed object-version evidence and state digest, never the route's generic response: token creation and other operations may return a one-time secret that must not become audit data. A completed retry therefore returns a redacted completion proof; the route reconstructs a safe response from current state or applies its operation-specific one-time-secret policy. The journal identity needs create, read, and list for reconciliation, but never conditional replacement or deletion.

The route-specific commit remains responsible for storing the intent's server-generated opaque reference in state and returning the existing result when that reference already committed. The reference is independent of the client idempotency key, so delivery-visible object metadata cannot be used to test guesses about that key. This binding makes recovery after "state committed, completion missing" idempotent. The initial style-publication core stores it as durable custom metadata on the conditionally written style object. It preserves the submitted style bytes, records the resolved object path in both the idempotency identity and new intent records, and recognizes an already committed object when a retry must reconstruct a missing completion. Intent existence alone is not treated as commit evidence. The object path is supplied by the trusted startup catalog; the core does not derive it from the external account or style identifier. Original create/update preconditions prevent an incomplete older mutation from overwriting newer state.

The serving process runs a reconciliation scan immediately in the background and every five minutes thereafter. It streams intent locations rather than retaining the journal in memory, skips intents that already have a valid completion, and creates a completion only when current validated style state still carries that intent's opaque reference. It never replays a state write. Missing state remains retryable by the original client; state naming another intent is retained as superseded evidence. New intents use their persisted trusted state locator, while legacy intents without one fall back to the current catalog. Reconciliation failures are logged and retried by the next scan, and concurrent reconcilers converge through the journal's immutable create-only completion rule. The scan performs storage work proportional to retained journal history; a separate pending index is justified only when measured mutation volume makes that material.

## 6. Readiness and current HTTP contract

These HTTP endpoints exist:

- `GET /livez`
- `GET /readyz`
- `GET /whoami` when object-storage management authentication is configured
- `GET /operations/status` when authentication and at least one operational endpoint are configured
- `GET /accounts/{account_id}/styles/{style_id}` when style publication is configured
- `PUT /accounts/{account_id}/styles/{style_id}` when style publication is configured

The health endpoints return `{"status":"ok"}` as JSON. `/whoami` requires a Bearer credential and returns only its bounded actor, accounts, exact actions, and installed registry revision. Every response uses `Cache-Control: no-store`. Without `ABASHIRI_AUTH_ROOT`, `/whoami` is absent and the process remains a health-only server. Style routes are registered only when auth, `ABASHIRI_STATE_ROOT`, `ABASHIRI_JOURNAL_ROOT`, and `ABASHIRI_STYLE_CATALOG` are all configured. Remote state and journal URLs sharing one authority are rejected at startup. The auth registry and catalog are loaded before listening; partial style configuration is rejected. Readiness still does not continuously prove object storage availability.

Before production deployment, readiness must remain false until the configured identity adapters and storage client have initialized and a non-mutating backend availability check has succeeded. The mutating `check-storage` command remains a deployment/init-stage capability gate and is never run by `/readyz`. Transient dependency policy must be defined per adapter; liveness never fails merely because an external identity provider or object store is unavailable.

## References

- [`resource-layout-sketch.md`](resource-layout-sketch.md) — content object-store layout, including the content-addressed sprite bundle in §5
- [`ishikari-spec.md`](ishikari-spec.md) and [`biei-spec.md`](biei-spec.md) — delivery contracts, which this document deliberately does not govern
- [MapLibre Style Specification](https://maplibre.org/maplibre-style-spec/) — the style document MMPF stores and delivers, including multi-sprite `sprite` arrays (§1.6)
