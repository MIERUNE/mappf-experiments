# Abashiri

Abashiri is the experimental management and publishing API for MMPF. It is the backend for the separately branded MMPF Console and for future publishing automation.

Abashiri is not part of the delivery plane. It does not serve map resources, render maps, join the Biei or Ishikari gossip clusters, or mutate their process state. It coordinates authenticated, audited mutations of durable state that the delivery services observe independently.

The server crate is the CLI and HTTP composition root. Bounded management identities, conditional storage operations, the durable mutation journal, and the style-publication core live in [`../../crates/abashiri-core`](../../crates/abashiri-core).

## Current executable slice

Without configuration the server starts in health-only mode:

```sh
cargo run -p abashiri -- serve
```

The listener defaults to `127.0.0.1:8080`. `/livez` and `/readyz` return JSON with `Cache-Control: no-store`.

Configure the object-storage management registry to enable authenticated identity resolution:

```sh
cargo run -p abashiri -- serve \
  --auth-root gs://example-control-plane/abashiri/auth/
```

The root must contain `current.json` in the schema documented by [`../../specs/abashiri-spec.md`](../../specs/abashiri-spec.md#42-object-storage-management-authentication). Generate a registry digest without placing the raw credential in a command argument. Supply an independently generated credential with at least 256 bits of entropy:

```sh
printf '%s' "$ABASHIRI_PUBLISH_TOKEN" |
  cargo run -q -p abashiri -- hash-credential
```

With auth configured, `GET /whoami` accepts one `Authorization: Bearer` header and returns the bounded workload identity, namespace grants, actions, and registry revision. The registry is cached in memory; it is not fetched for every request. Without auth configuration, `/whoami` remains unavailable and the server is health-only. Give the serving workload read-only registry access; publish `current.json` with a separate bootstrap identity.

`GET /auth/capabilities` is an unauthenticated, non-cacheable bootstrap contract for MMPF Console. It currently advertises `bearer` only when object-store management authentication is configured; future OIDC or trusted-proxy adapters add methods without changing the Console's management APIs.

Before enabling a writer API, verify the real published-state backend with the serving identity:

```sh
cargo run -p abashiri -- check-storage \
  --root gs://example-delivery-state/abashiri/
```

The command performs a probe below `.abashiri-capability-check/`. It requires create-only writes, a usable object version or ETag, stale-writer rejection on conditional replacement, and round-tripping of content type, cache policy, and custom metadata. By default it retains the small object for a bucket lifecycle rule to expire. This avoids an explicit cleanup requirement, but GCS still requires `storage.objects.delete` for the conditional replacement itself. Scope that permission to the published-state bucket; the append-only journal identity does not need it.

Exercise the private journal through a real authenticated mutation and reconciliation scan. The serving identity needs create and read for immutable intents/completions plus list to discover unfinished intents, but no replacement or deletion. Run the full `check-storage` probe on the journal only if a future journal format introduces conditional replacement, using a separately scoped diagnostic identity.

Use `--cleanup` only where the probing identity already has delete permission:

```sh
cargo run -p abashiri -- check-storage \
  --root gs://example-management-journal/abashiri/ \
  --cleanup
```

An interrupted check may leave a probe object even with `--cleanup`, so the dedicated prefix should always have bounded lifecycle expiry. Do not apply that lifecycle rule to `journal/` or published state: those objects carry the durable audit and idempotency evidence used by retries.

To enable style retrieval and publication, configure separate remote buckets or authorities for published state and the private mutation journal. A prefix in the delivery bucket is not an IAM boundary: Ishikari needs read access to the state bucket but must not receive access to the journal bucket.

```sh
cargo run -p abashiri -- serve \
  --auth-root gs://example-management-auth/abashiri/ \
  --state-root gs://example-delivery-state/abashiri/ \
  --journal-root gs://example-management-journal/abashiri/ \
  --style-refresh-endpoint http://biei:9090/_internal/refresh/style \
  --style-refresh-endpoint http://ishikari:9090/_internal/refresh/style \
  --operational-status-endpoint biei=http://biei:9090/_internal/operations/v1/status \
  --operational-status-endpoint ishikari=http://ishikari:9090/_internal/operations/v1/status
```

Abashiri derives style paths from the validated `{namespace}/{style_id}` identity. The default nested layout stores `styles/demo/basic/style.json`; `--style-object-layout flat` stores `styles/demo/basic.json`. Both publish and notify the same logical `demo/basic` style. Inventory is discovered by bounded listing under `styles/` and `tilesets/`; object paths are parsed as canonical resource identities before authorization. A storage prefix is therefore an identity encoding, not an IAM boundary.

`GET /namespaces/{namespace}/styles/{style_id}` requires the `style.read` action and returns the exact style plus an opaque `ETag`. `PUT` requires `style.publish`, one `Idempotency-Key`, and an explicit precondition. The earlier `/accounts/{namespace}/styles/{style_id}` spelling remains a compatibility alias.

When named operational endpoints are configured, `GET /operations/status` requires the global `operations.read` action. Abashiri polls at most eight endpoints in parallel with a one-second request deadline, coalesces requests for two seconds, and returns partial results. A failed source may retain a last-known-good snapshot for at most five minutes, always marked `stale`; internal endpoint URLs and transport details are never returned.

`GET /namespaces` returns namespace choices for the Console. An operator with `operations.read` discovers first-level namespace prefixes below styles and tilesets; an ordinary resource reader receives its granted namespace names without an object-store listing. `GET /inventory?namespace={namespace}` lists canonical styles and PMTiles archives only below that selected namespace and only for resource kinds the principal may read. The response reports `visibility` as `all` or `granted`, and never returns storage locations, non-canonical nested objects, legacy namespace-less tilesets, or mutation-journal data.

```sh
# Create only
curl -i -X PUT http://127.0.0.1:8080/namespaces/example/styles/basic \
  -H "Authorization: Bearer $ABASHIRI_PUBLISH_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: create-basic-1' \
  -H 'If-None-Match: *' \
  --data-binary @style.json

# Replace only the version returned by GET or PUT
curl -i -X PUT http://127.0.0.1:8080/namespaces/example/styles/basic \
  -H "Authorization: Bearer $ABASHIRI_PUBLISH_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: replace-basic-1' \
  -H "If-Match: $CURRENT_ETAG" \
  --data-binary @style.json
```

Publication validates MapLibre style version 8, writes an immutable audit intent before state, conditionally commits exact style bytes, and records a durable completion before reporting success. The ETag encodes both backend ETag and object version so GCS generation-based replacement remains safe.

When publication is enabled, a background reconciliation scan runs immediately and every five minutes. It completes a missing journal record only when the validated current style still names that intent; it never replays a state mutation, and absent or superseded state remains unchanged. New intents retain the resolved object path. Legacy intents without one use the configured deterministic style layout.

Configured refresh endpoints receive the same advisory hint in parallel after the durable completion. The JSON response reports `refresh` as `delivered`, `partial_failure`, or `not_configured`. Notification failure never changes a committed mutation into an HTTP failure. Repeating the identical request with the same idempotency key retries notification without republishing the style; delivery polling remains the correctness fallback. The shared canonical style-key validator guarantees that every derived publication path can form a refresh hint.

See [`../../specs/abashiri-spec.md`](../../specs/abashiri-spec.md) for the native management contract and [`../../issues/abashiri-todo.md`](../../issues/abashiri-todo.md) for unresolved work.
