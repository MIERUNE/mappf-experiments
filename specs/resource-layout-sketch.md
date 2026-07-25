# Content object-storage layout — design sketch

Status: **partly adopted.** The PMTiles source-template rules in §3 are
implemented and belong to Ishikari's serving contract. The publisher, style,
sprite, and glyph layout remains a proposal. [`ishikari-spec.md`](ishikari-spec.md)
and [`biei-spec.md`](biei-spec.md) remain authoritative for runtime behavior.

## 1. Boundary and invariants

Object storage is a write/read seam, not a runtime dependency between services:

- A trusted publisher writes content and must not overwrite a live immutable
  identifier.
- Ishikari maps logical identifiers to object-store locations and serves the
  resulting resources over HTTP.
- Biei is an HTTP consumer. It has no bucket credentials and no PMTiles
  knowledge.

PMTiles archives and content-addressed sprite bundles are immutable. A changed
archive is published under a new opaque logical tileset id. A revision label may
be part of that id, but Ishikari has no separate `version` field or version path
component. Runtime caches intentionally do not poll object generations.

Styles are small coordination objects. They may eventually be mutable and
short-cached, but short HTTP freshness alone does not invalidate Biei's loaded
style state. Until the management plane supplies an explicit style revision to
Biei, publishing a changed style under a new immutable style id is the safe
baseline.

## 2. Logical identifiers and recommended keys

The logical tileset identifier is either `tileset_id` or
`namespace/tileset_id`. A recommended physical convention is:

```text
tilesets/{namespace}/{tileset_id}.pmtiles
styles/{style_id}/style.json
styles/{style_id}/sprites/{sprite_id}{@2x}.{json,png}
fonts/{font_name}/{range}.pbf
```

For a flat tileset id, the optional namespace path segment is omitted. There is
no literal `default` segment. These are conventions rather than identifiers
baked into Ishikari's domain model; deployments can choose another object path
through the source templates below.

TileJSON remains derived from the PMTiles header and metadata. Storing a second
TileJSON object would create another source of truth without adding useful
information.

Glyph objects contain one font and one 256-codepoint range. Ishikari treats a
comma-separated public `{fontstack}` as an ordered composition request: it
fetches and caches each `{font_name}/{range}.pbf` independently, keeps the first
font's glyph when IDs overlap, and byte-bounded-caches the merged
representation. The composite is a runtime cache entry, not another object the
publisher must materialize. Single-font responses remain byte-identical
passthrough. The source template is deployment-configurable, but one request is
bounded to eight distinct component fonts so a public path cannot create
unbounded object-store fan-out.

## 3. PMTiles source templates (adopted)

`ISKR_TILESET_SOURCES` accepts `namespace=value;…;default=value`, or one bare
default value. Each value is either an object-store root or an absolute URL
template.

An explicit template has exactly one `{tileset_id}` and may contain
`{namespace}` once as a complete optional path segment:

```text
regional=gs://regional-bucket/maps/{tileset_id}.pmtiles;
default=gs://main-bucket/tilesets/{tileset_id}.pmtiles
```

In a default template without `{namespace}`, `{tileset_id}` is the complete
logical id. In a named template, or when `{namespace}` is explicit,
`{tileset_id}` is the identifier after the logical namespace. Therefore:

```text
regional/streets   -> gs://regional-bucket/maps/streets.pmtiles
analysis/hrnowc    -> gs://main-bucket/tilesets/analysis/hrnowc.pmtiles
planet             -> gs://main-bucket/tilesets/planet.pmtiles
```

A root value preserves the earlier shorthand: a named entry receives the key
after its matched namespace, while the default root receives the complete
logical id; Ishikari appends `.pmtiles`. The shorthand `gs://…/tilesets`
and the explicit default
`gs://…/tilesets/{namespace}/{tileset_id}.pmtiles` therefore realize the same
object paths as the compact default template above.

Templates are validated at startup. Unknown or repeated placeholders,
placeholders outside the URL path, embedded `{namespace}` segments, and paths
without the `.pmtiles` suffix are rejected.

## 4. Style references and portability

A stored style references logical tileset, glyph, and sprite resources rather
than a deployment-specific public host. Ishikari's response path resolves or
rewrites those references to its HTTP endpoints. This keeps stored content
portable across environments and keeps Biei's resource access behind the
Ishikari boundary.

Provider-catalog and URL-template configuration must be equivalent across peers
in one protocol cluster. The caller currently routes provider work using a
resolved URL, while the peer request carries logical identity; configuration
skew can otherwise make the peer resolve different bytes. A bounded catalog
revision is required before catalogs can roll independently.

## 5. Content-addressed sprite bundles (proposed)

The management publisher should build the complete MapLibre sprite bundle,
derive a lowercase hexadecimal SHA-256 `sprite_id` from a deterministic framing
of every member name and byte sequence, and publish:

```text
styles/{style_id}/sprites/{sprite_id}.json
styles/{style_id}/sprites/{sprite_id}.png
styles/{style_id}/sprites/{sprite_id}@2x.json
styles/{style_id}/sprites/{sprite_id}@2x.png
```

The path needs no `/sha256/` discriminator: the validated `sprite_id` shape and
the publishing contract already identify the algorithm. Hashing the complete
bundle, rather than each file independently, gives the style one atomic logical
sprite identity. The canonical framing must be specified before two independent
publishers are supported.

The publisher uploads all create-only bundle members first, injects the
Ishikari-facing sprite base containing `sprite_id` into the style, and publishes
the style last. Sprite responses can then use a long `immutable` cache policy;
the much smaller style response can use a short, conditionally revalidated
policy. Ishikari must preserve the injected logical sprite reference instead of
unconditionally replacing it with a provider-wide sprite template.

MapLibre's sprite protocol requires the paired JSON index and PNG image. WebP is
not part of this proposed storage contract.

## 6. Publishing and cache identity

The first writer should be a trusted operator CLI using cloud or workload
identity and the repository's existing `object_store` adapters. Immutable puts
must use create-only semantics or an equivalent precondition. If a backend
cannot enforce that safely, the publisher must refuse publication rather than
fall back to last-write-wins.

The publisher must also set explicit `Cache-Control` object metadata. An
authenticated object-store transport may synthesize `private, max-age=0` when
metadata is absent; Ishikari cannot safely distinguish that transport default
from an intentional private policy and therefore must not override it. The
current recommended policies are:

```text
style:        public, max-age=300, s-maxage=3600, stale-while-revalidate=86400
glyph/sprite: public, max-age=86400, s-maxage=604800, stale-while-revalidate=604800
```

Once sprite paths are content-addressed as proposed in §5, the publisher may
use a longer `immutable` policy for the complete bundle. Explicit `no-store`,
`no-cache`, or `private` metadata remains authoritative.

Multi-object publication order is:

1. Write immutable PMTiles or all immutable sprite members.
2. Verify their expected identities and required members.
3. Publish the referencing style last.

A future stable style pointer requires explicit compare-and-swap, rollback,
freshness, and Biei `StyleRevision` semantics. It must not be inferred merely
from a short cache TTL. Content identifiers, authorization capability epochs,
and policy revisions remain independent axes.

## 7. Authorization and portability

Authorization is checked against the parsed logical id before physical source
mapping. A namespace is not a bucket ACL: templates may strip it, retain it, or
route it to a separate bucket. Strong tenant isolation still requires suitable
provider IAM or separate buckets in addition to application authorization.

The publisher's write identity is separate from application read capabilities.
A future multi-tenant management API needs its own administrator identity,
write policy, audit contract, and CSRF boundary. It may eventually share a typed
logical resource selector with read authorization, but not the read key registry
or a generic `admin` content scope.

## 8. Remaining decisions

- Define the canonical byte framing used to derive a bundle-wide `sprite_id`.
- Decide whether styles stay immutable or gain a mutable pointer plus an
  explicit Biei revision contract.
- Decide whether glyphs are global, namespace-owned, or content-addressed.
- Define garbage collection from live style and tileset references without
  racing an in-progress publication.
- Decide whether external style assets must be mirrored into the managed
  backend or may remain provider URLs.
