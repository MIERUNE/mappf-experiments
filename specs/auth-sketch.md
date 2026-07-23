# Authentication boundaries for Biei and Ishikari — design sketch

Status: exploratory overall. Biei now has a disabled-by-default first slice for
object-storage-backed authentication of static renders. It is implementation
evidence, not yet a production or demo commitment. Open decisions are tracked
in [`issues/auth-todo.md`](../issues/auth-todo.md).

## 1. Purpose

Authentication serves different purposes on different surfaces. Treating every
request as if it were an administrative operation would add cost and complexity
without materially improving the ordinary delivery path.

For public map delivery, the main goals are:

- attribute usage to a customer or project;
- assign a rate and egress budget;
- block disabled or obviously abusive credentials; and
- keep expensive origin work away from unauthenticated traffic.

This is not a confidentiality boundary for highly sensitive data. A delivery
key is a bearer credential that may be present in a browser or other
comparatively exposed client. It should be cheap to rotate and have limited
authority, but it should not be mistaken for an end-user identity.

Administrative changes, automated publishing, service-to-service calls, and
ordinary map delivery therefore use separate credentials.

Two distinctions are fundamental:

- Authentication assigns a request to a principal or rate bucket. Rate limits,
  request limits, and egress limits enforce the abuse budget.
- Origin metrics measure origin work. They do not measure all delivered usage
  when a CDN can answer requests without contacting the origin.

## 2. Four independent credential planes

| Plane | Principal | Preferred credential | Main authority |
| --- | --- | --- | --- |
| Human administration | employee or operator | one or more corporate OIDC providers | manage configuration, content, and delivery credentials |
| Automated publishing | workload or service account | workload identity or narrowly scoped service credential | publish and update approved content |
| Map delivery | customer or project | high-entropy opaque delivery key | read ordinary delivery routes within a coarse scope |
| Internal service | Biei or another trusted workload | optional dedicated workload identity | authenticate the workload without enlarging the caller's delivery grants |

These planes are deliberately not interchangeable:

- a delivery key cannot authorize a management or publishing route;
- a browser's delivery key is never forwarded as Biei's identity to Ishikari;
- a human OIDC session is not stored in a style URL or used as a long-lived
  service credential; and
- publishing automation does not impersonate a human administrator.

The issuers, audiences, storage, logging, rotation, and failure policies may
therefore differ. Sharing one token format across all four planes would be a
design regression even if the claims could technically express every role.

Supporting several providers does not mean trying every verifier in sequence.
Each credential plane has explicit selection rules and produces one canonical
principal. A recognized but invalid credential must not fall through to a
weaker mechanism.

## 3. Hot-path rule

Ordinary delivery authentication must be local and bounded:

1. Parse a small credential identifier.
2. Perform an O(1) lookup in an immutable in-process snapshot.
3. Verify the secret in bounded time.
4. Return a compact, typed delivery principal to authorization and metering.

The delivery request path must not perform a database, object-store, IdP, KMS,
forward-auth, or other network call merely to authenticate one request. Registry
or secret refresh happens out of band and atomically replaces the local
snapshot.

A dynamic registry has one bounded cold-start exception to this rule. If a pod
has no valid snapshot for a configured `registry_id`, concurrent requests may
join one single-flight snapshot load or receive a temporary-unavailable response
while that load runs. This is a registry activation event, not a per-token
lookup: once loaded, every request verifies locally. Unknown registry IDs are
rejected without storage access. Deployments may preload known active
registries when avoiding even that first-request delay is more important than
startup time and memory.

Authentication should run before admission to expensive work:

- Biei verifies the caller before render admission or native rendering;
- Ishikari verifies the caller before remote storage, peer routing, or derived
  processing; and
- an output or resource cache hit still requires authentication when the route
  is protected, but authentication itself must be cheap enough that this does
  not erase the cache benefit.

Authorization is evaluated against the already parsed route identity. A route
must not be parsed independently by authentication, authorization, caching, and
metrics because disagreement between those parsers can create bypasses.

## 4. Delivery credentials

### 4.1 Token shape

The public envelope contains a registry selector followed by a registry-specific
opaque credential:

```text
<registry_id>.<opaque_registry_credential>
```

The envelope is split only at its first `.`. `registry_id` selects one entry in
a bounded trusted local catalog and grants no authority by itself; the entire
suffix is passed unchanged to that registry's verifier. It may therefore be a
random opaque key, a JWT, or another bounded representation without changing
the delivery API. Unknown registry IDs are rejected without constructing a
storage URI or performing I/O.

The first object-storage adapter treats the suffix as a high-entropy bearer
secret and indexes a SHA-256 verifier for the complete suffix. Its verifier
construction domain-separates `registry_id` with length-prefixed encoding, and
confirms a hash-table match with constant-time comparison. Other adapters may
validate the opaque suffix differently. Password-hardening such as Argon2 does
not belong on every delivery request; an adapter accepting human passwords
would need a different exchange flow.

The preferred transport is the standard `Authorization: Bearer` header, but it
can be the deployment default only after the supported browser and MapLibre
clients are proven to attach it to every required style subresource. Putting a
stable delivery key in a query string makes it more likely to appear in browser
history, referrers, screenshots, CDN logs, and support transcripts. A client
that cannot set the header may use the explicit `access_token` parameter now
implemented by both servers, but a deployment must first configure URL-log redaction,
referrer policy, and CDN cache-key behavior. Clients must not silently switch
transports, and a request carrying both is rejected.

### 4.2 Registry entry

A request parses the token's `registry_id`, resolves it through trusted local
configuration, verifies the key in that registry, and evaluates its grants
against the canonical resource identity already parsed by the route. The
initial design deliberately does not require a global principal or token index.
A delivery-key entry should contain only bounded policy needed on the hot path:

- registry-specific credential verifier;
- stable customer or project identifier;
- enabled or disabled state;
- rate/egress tier;
- allowed namespace/action grants or other coarse resource selectors;
- optional browser-origin policy; and
- optional validity bounds used for rotation overlap.

A browser-origin policy may contain exact origins and narrowly bounded wildcard
subdomains. When `Origin` is present it is checked first; otherwise the verifier
may compare the origin component of `Referer`. The policy must explicitly say
whether a request with neither header is allowed. Both headers are forgeable by
non-browser clients, so this is an anti-hotlink and abuse-control signal rather
than proof of client identity. Comparisons operate on parsed scheme, host, and
port, never string prefixes.

Namespace and action grants live in the token entry. The ordinary token format
and registry are shared by Biei and Ishikari: one token may carry
`render.static` and `read`, and each service enforces only the action relevant
to its route. The initial `read` grammar is deliberately coarse. It grants
ordinary style, tileset, tile, glyph, sprite, and derived-resource reads within
the allowed namespaces; it does not create separate basemap, terrain, glyph,
or sprite permission classes. A deployment that genuinely needs a harder
boundary should express it with a namespace rather than accumulating resource-
kind flags.

The first design has no
`namespace -> allowed registry IDs` table and no trusted registry-level
`namespace_scopes` ceiling. Consequently, anyone allowed to write a registry
can grant its tokens access to any namespace. Registry mutation therefore
remains a centrally trusted management-plane capability; customer-delegated
registry writers require a separately designed trusted scope ceiling before
they are enabled.

Authorization uses the service's existing parsed resource model. It does not
require Biei to replace a deep style ID with a new universal
`namespace/style_id` domain type, and authentication must not independently
split or reinterpret an identifier that the router has already parsed. In
particular, namespace checks use the same percent-decoded canonical path
identity as the handler; a raw URI spelling such as `%66oo` must not become a
different authorization namespace from `foo`.

It should not grow into an end-user directory, fine-grained RBAC system, or
arbitrary policy language. If authorization requires remote relationships or
unbounded claim sets, it no longer satisfies the delivery-path cost model.

Raw tokens, secret verifiers, and attacker-controlled identifiers must never be
used as metric labels or emitted to ordinary logs. Logs may contain a bounded
internal token ID when operationally necessary, but a stable customer/project
ID is normally the more useful attribution key.

### 4.3 Scope and revocation

The ordinary delivery tier favors low cost over instant global revocation.
Rotation may allow an old and a new key to overlap, and a disabled key may
remain accepted for the bounded registry-refresh interval. A target measured in
minutes is acceptable for commodity reads if it is documented and monitored.

Invalid, unknown, malformed, or disabled credentials fail closed. Refresh
failure is different:

- a running process may continue using its last known-good snapshot according
  to the tier's explicit, observable staleness policy; and
- a fresh process with no valid snapshot must not silently allow protected
  traffic. It should remain unready or reject protected requests.

This preserves availability during a control-plane interruption without
turning missing configuration into anonymous access.

Data that requires immediate revocation or confidentiality belongs to a
separate strong-access tier described in section 9.

### 4.4 Deliberate compromises and hard boundaries

The ordinary delivery credential is best understood as a project-level abuse
and attribution credential, not as proof of an individual end user. Its design
deliberately accepts that:

- a browser-visible bearer credential can be copied;
- authorization scopes are coarse rather than per-user or per-object;
- revocation may take minutes to reach every edge and origin process;
- shared representations remain shared across authorized customers;
- delivery accounting may be eventually consistent; and
- some clients may eventually require short-lived URL capabilities instead of
  an authorization header.

Those compromises keep commodity map delivery cacheable, portable, and cheap.
They do not relax the following boundaries:

- a delivery credential never authorizes management or publishing;
- secrets are high entropy, rotatable, and absent from ordinary logs and
  metrics;
- unauthenticated traffic cannot consume expensive origin work on a protected
  route;
- edge request and egress limits contain the value of a copied credential; and
- confidential or individually authorized data uses the separate strong-access
  tier rather than stretching the delivery credential beyond its threat model.

If a product cannot tolerate the accepted compromises, it is not an ordinary
delivery-tier product. Changing that product's tier is safer than incrementally
adding management-grade machinery to every tile and glyph request.

## 5. Abuse control and accounting

Authentication identifies the bucket; it does not enforce the budget by
itself. When the edge or gateway supports it, it should apply request-rate and
egress limits using the verified customer/project and rate tier. Expensive Biei
routes may also have stricter concurrency or cost budgets than small Ishikari
resource reads.

A basic CDN may offer no customer-aware enforcement at all. In that case,
origin-local limits still protect origin capacity but cannot constrain traffic
served from CDN cache. The deployment must either accept that limitation, add a
separate authenticating gateway, use coarse provider-level controls, or choose a
URL-bearer model whose exposure is bounded by expiry and cache lifetime. The
core design must not pretend that origin rate limiting governs CDN-hit egress.

Useful bounded dimensions include:

- customer or project;
- rate tier;
- route class;
- authentication outcome; and
- coarse authorization outcome.

Do not put raw token IDs, URLs, tileset IDs, style IDs, or other unbounded
request values in Prometheus labels. Detailed per-resource attribution belongs
in sampled or structured logs, not in a high-cardinality time series. Even a
registry-defined customer/project label needs a documented cardinality budget;
larger fleets should aggregate it outside Prometheus.

When a CDN serves a cache hit, the origin sees neither the request nor its
bytes. Therefore, where the chosen CDN exposes adequate logs:

- CDN or edge access logs are the source of truth for delivered request and
  egress usage;
- origin metrics describe cache misses, origin latency, provider I/O, render
  work, failures, and capacity; and
- origin request counts must not be presented as billing-complete delivery
  counts.

Per-customer attribution is available from those logs only if the edge has
validated the credential and records a bounded derived identity. A raw bearer
token in a CDN log is a credential leak, while an anonymous shared-cache log can
support aggregate accounting but cannot retroactively identify the customer.

If the CDN exposes neither identity-enriched logs nor another trustworthy usage
export, exact per-customer delivery accounting is not available. This is a
deployment capability gap, not something the origin can reconstruct later.

The aggregation pipeline may lag. Enforcement should not depend on a billing
export being immediately available.

## 6. Cache and CDN contract

### 6.1 Portability baseline

The baseline CDN contract assumes only conventional caching by URL under
standard HTTP cache directives, plus whatever aggregate logging the provider
normally supplies. It does not require:

- programmable edge compute;
- an edge KV or replicated credential registry;
- custom token validation or HMAC code;
- customer-aware rate limiting;
- arbitrary cache-key rewriting;
- identity-enriched access logs; or
- immediate, globally consistent purge.

Cloud CDN, CloudFront, Akamai, or another provider may offer some or all of
these features, but the core protocol cannot depend on them. Provider-specific
adapters may enable better authentication, cache sharing, enforcement, and
accounting when available.

A deployment must declare the CDN capabilities it relies on. Configuration
should fail validation when a selected security or accounting policy requires a
capability the deployment does not have; silently degrading from authenticated
delivery to public delivery is not acceptable.

### 6.2 Authentication and shared caching

Authentication must not unnecessarily destroy shared-cache efficiency. When
two authorized callers receive identical representation bytes, the semantic
origin cache key should not contain the caller, raw credential, or OIDC subject.
Authorization happens before returning the cached representation; the stored
representation remains principal-independent.

A CDN creates an additional boundary. A CDN that cannot authenticate a request
cannot simultaneously:

1. serve a shared cache hit without reaching the origin; and
2. guarantee that only authenticated callers receive that hit.

The deployment must choose and document one of these models:

- **Authenticating edge:** the edge validates the delivery key from a local
  snapshot or equivalent native credential facility, applies limits, and uses
  a credential-independent cache key. This gives the best shared-cache
  behavior without adding an origin/auth-service call to every hit.
- **Credential-varying cache:** the CDN includes a credential or signed
  capability in the cache key. This is easier for a limited CDN but fragments
  the cache and requires strict log/redaction handling.
- **Intentionally public commodity delivery:** shared assets are treated as
  public or best-effort protected, while expensive or private origin routes are
  authenticated. Origin-only authentication must not be described as
  protecting cache hits that bypass the origin.

An authenticating edge is an optimization and stronger deployment option, not
the portability baseline. With a basic CDN, the honest choices are usually a
bearer URL/capability in the ordinary cache key, intentionally public shared
content, an authenticating component placed before cache access, or bypassing
the CDN for protected traffic. Each has a different cost, leakage, and cache-hit
trade-off.

`Origin` or `Referer` checks can be an optional anti-hotlink signal at the edge,
but they are forgeable and are not authentication. Varying origin caches by
these headers should require evidence that the policy benefit exceeds the cache
fragmentation.

Authentication alone does not imply `Cache-Control: private` or `no-store`.
Those directives are required when the response is personalized, contains a
credential, or has confidentiality requirements. Identical, credential-free
responses can remain shareable when an authenticating edge enforces access
before its cache.

### 6.3 Namespace requirements on cached renders

Token identity is not the semantic render-cache boundary. A verified caller
has a bounded set of readable namespaces, while a cached render carries the
bounded namespace requirements needed to produce its bytes. A cache hit is
eligible only when the current caller is authenticated and its current grants
satisfy one recorded requirement set:

```text
required_namespaces ⊆ caller.readable_namespaces
```

This check occurs before returning bytes on every protected cache hit. Token
revocation and grant removal therefore take effect when the local registry
snapshot advances even if the rendered bytes remain resident. In the target
model, raw tokens are not the semantic rendered-cache partition and never
appear in exported cache metadata, logs, or metrics. Until namespace
requirements are enforced end to end, an in-process transport cache may retain
the complete token-bearing URL as its conservative isolation key; it must not
export or log that key.

The implemented first Biei slice applies the cache-hit check without claiming
to know the exact dependency closure. A protected task carries its normalized
namespace grant set, a domain-separated one-way credential-and-policy cache
partition, and a bounded redacted provider bearer token over the trusted
internal render wire. The verifier digest and principal do not cross. The
partition changes when the authenticated registry revision advances. A newly
rendered output records the complete grant set as a conservative upper bound
on its requirements; neither the credential partition nor bearer token is part
of rendered-output cache identity. Thus equivalent grants can share final
images, a superset can reuse an entry, and a weaker, incomparable, protected,
or unprotected caller cannot cross the recorded boundary.

The credential-and-policy partition isolates Biei's positive and negative style/TileJSON
caches, in-flight profile fetches, and worker-local loaded native style. This is
required because a rewritten profile can contain a token-bearing URL even when
two credentials have identical grants. The partition deliberately does not
enter gossip warmth or metric labels. A render completed under a weaker grant
does not replace a resident stronger requirement. Component contracts prove
that Biei sends the token on the actual exact-origin profile request, FileSource
uses the complete token-bearing URL in positive-cache and single-flight
identity, and Ishikari authorizes the current token before consulting a shared
resource cache. A production-container E2E also composes authenticated Biei,
authenticated Ishikari, and a real native render: after a broad token warms
both services' caches, a weaker token cannot read the protected tile, receive
the broad render, or poison the authorized cache entry. The trusted dependency
descriptor is not implemented. The conservative rule is safe but may discard
valid hits; the descriptor below is the mechanism for narrowing the requirement
from “everything the producer could read” to “what these bytes actually
required.”

The conservative first requirement for a render is the bounded, sorted set of
namespaces in the trusted Ishikari-rewritten style dependency descriptor,
including the style namespace. Ishikari may return that descriptor with the
rewritten style because it owns the canonical rewrite from provider-relative
references to its delivery routes. Biei accepts it only from its explicitly
trusted style provider or with cryptographic integrity; an arbitrary style
origin cannot claim weaker requirements. Until this descriptor and its cache-
hit check are implemented end to end, Biei must retain token-bearing URLs in
FileSource cache identity, retain the credential partition in profile cache and
loaded-renderer identity, and must not treat a broad workload credential as
sufficient authority for user-selected resources.

A later optimization may attach an additional, weaker requirement set to one
exact cached render when a strictly weaker namespace grant independently
produces byte-identical output for the same complete render key, style revision,
and policy epoch. The proof is valid only when the weaker render cannot consume
resource or style-cache entries populated under broader grants. Equality after
such cache contamination proves nothing. Do not take the intersection of
incomparable grant sets: equal bytes produced under `{a,b}` and `{a,c}` do not
prove that `{a}` is sufficient. Keep alternative requirement sets bounded and
discard them on style or authorization-policy revision changes.

This adaptive relaxation is optional. Namespace-closure checking is the safe,
simple baseline, and separate basemap/terrain permission kinds are explicitly
out of scope for the ordinary delivery tier.

## 7. Human management and automated publishing

### 7.1 Human administration

The management surface should integrate with a corporate identity provider
through standard OIDC rather than a vendor-specific login protocol. A typical
web flow uses the authorization-code flow, normally with PKCE, and terminates in
a server-side session with `Secure`, `HttpOnly`, and appropriate `SameSite`
cookies. Mutating cookie-based requests need CSRF protection.

One deployment may configure several named OIDC providers at the same time—for
example, the company's primary IdP and a partner organization. Each provider
has its own allowlisted issuer, client configuration, claim mapping, and
operational status. The verified identity is keyed by `(issuer, subject)`, not
by an email address. Linking identities across issuers is an explicit audited
management action; matching email strings do not link accounts automatically.

The management verifier should enforce an allowlisted issuer, an explicit
management audience, short session validity, and bounded role/group mapping.
MFA and account lifecycle remain the IdP's responsibility. An existing locally
verifiable session may continue until its expiry during an IdP interruption;
new login or refresh fails closed. Mutations produce an audit record containing
actor, action, target, outcome, and request/trace identity.

Management and publishing routes should use a distinct listener, hostname, or
ingress policy where practical. They must not inherit the delivery CDN's shared
cache rules, and sensitive responses should be `no-store`. OIDC is an identity
mechanism, not a substitute for restricting unnecessary network exposure.

The management API may create, rotate, disable, or scope delivery credentials,
but possession of one of those delivery credentials never grants access back to
the management API.

### 7.2 Automated publishing

CI jobs, importers, and content pipelines should use workload identity or a
narrow service credential. Their permissions should describe publishing
actions and content namespaces, not a human role. Long-lived static credentials
are a portability fallback, not the preferred production mechanism.

Human CLI access may use an OIDC device or browser flow if needed. Automation
must not depend on completing an interactive login.

Human OIDC sessions and workload credentials may coexist on the management
plane, but they represent different principal kinds. Authorization policy must
distinguish a human actor from a publishing workload even when both are allowed
to invoke one management API.

### 7.3 Biei to Ishikari

Biei and Ishikari accept the same ordinary delivery token at their public
boundaries. The selected experimental model forwards that same verified token
with the render task and then only to the explicitly configured exact
Ishikari/style-provider origin. Biei represents it as a bounded, redacted wire
value. It is absent from cache identities, rendered-output authorization
requirements, gossip, outcomes, metrics, and logs. The profile preparer appends
it as `access_token` only for the exact configured origin so Ishikari's existing
same-origin style and TileJSON rewrites propagate it through the MapLibre
resource waterfall. Arbitrary external URLs retained from a provider style
never receive it.

Because this model carries the original reusable bearer credential, each
deployment must decide whether its internal network is an accepted trusted
boundary. When node-to-node confidentiality or authenticated workload identity
is required, protect both Biei peer forwarding and Biei-to-Ishikari traffic
with mesh mTLS or an equivalent deployment-layer mechanism. The application
protocol does not add a second partial encryption, signing, or peer-token
scheme. The production-container E2E composes this credential path and its
cache non-interference boundary; deployment-specific mTLS and workload policy
remain outside that application-level test.

A short-lived namespace-attenuated capability remains a possible future
defense-in-depth layer, but is not the baseline. It would not replace mTLS and
would not provide transport confidentiality or peer identity where those are
required. It would add issuance, signing-key, expiry, rotation, revocation,
URL-leakage, and FileSource-cache policy. Revisit it only if bearer reuse after
a renderer compromise is a demonstrated threat that the service trust boundary
does not accept.

A dedicated workload or service identity may additionally authenticate that
the caller is Biei, but it is not a replacement for the ordinary token and
cannot widen its grants. Effective authority is the intersection of workload
policy and caller delivery grants. In particular, a broad Biei service token
must not allow rendering a tile namespace that the caller cannot read merely
because the caller can read the style. A service-token-only resource path would
make Biei a confused deputy.

Bounded customer/project attribution may be propagated separately only when
the transport authenticates Biei and the receiving route explicitly expects
it. Biei derives and overwrites such metadata after verification; it never
relays a client-supplied identity header unchanged.

## 8. Portable verifier seam and composition

The core request path should depend on a small verifier interface that returns
a typed principal or a bounded failure reason. Useful deployment adapters may
include:

- `None` for explicitly unauthenticated local development;
- `StaticApiKeys` for the first built-in delivery implementation;
- `TrustedHeader` behind an authenticated reverse proxy that strips incoming
  copies of the trusted headers;
- external JWT validation with locally cached keys; and
- an OIDC-backed management-session verifier on management routes.

These adapters are selectable building blocks, not an implicit `try each until
one succeeds` chain. Concurrent mechanisms require an unambiguous dispatch rule
based on the route and credential carrier or scheme:

- presenting credentials for more than one mechanism is rejected rather than
  resolved by precedence;
- once a mechanism recognizes its credential, failure is final and cannot fall
  through to `None`, `TrustedHeader`, or another weaker verifier;
- every successful verifier returns the same small canonical principal shape,
  including its credential plane and authentication method; and
- authorization consumes that principal without depending on provider-specific
  raw claims.

The first delivery-auth deployment should configure exactly one mechanism,
normally `StaticApiKeys`. Supporting multiple delivery mechanisms concurrently
adds downgrade, cache, metrics, and incident-response complexity and should be
introduced only for a concrete migration or federation requirement. In
contrast, multiple named OIDC providers and a distinct workload credential are
reasonable management-plane requirements because those principal populations
are inherently different.

Networked forward-auth can remain an integration escape hatch, but it should
not be the default delivery path because it adds latency, cost, and another
availability dependency to every cache hit.

The `None` mode must be explicit and visible in startup logs and health/config
diagnostics. Production manifests should not silently fall back to it because a
secret or issuer setting is missing.

Cloud-specific workload identity belongs in deployment adapters. Core crates
should consume a service credential or authenticated transport abstraction and
must not require GKE-specific metadata APIs.

## 9. Strong/private access and URL capabilities

Some future products may need stronger properties than the ordinary delivery
tier: confidential tilesets, end-user authorization, near-immediate revocation,
or per-document grants. Those requirements justify a distinct route/tier with
short-lived credentials, explicit authorization, conservative caching, and a
fail-closed control-plane policy. They should not silently make every public
glyph or tile request pay the same cost.

Signed URL capabilities remain an optional answer to a specific client/CDN
constraint, not the default architecture. Before adopting them, a concrete
design must resolve:

- which trusted component signs them and how signing authority is isolated;
- expiry and clock-skew behavior for long-running map clients;
- key rotation and revocation expectations;
- canonical URL/path encoding;
- CDN cache fragmentation and cache-key behavior;
- leakage through URLs, logs, referrers, and copied styles; and
- whether the CDN actually validates the capability or merely varies on it.

A capability URL is still a bearer credential. Embedding one in a style does
not create an end-user identity, and a dumb CDN does not become an authorization
service merely because the URL contains a signature.

## 10. Configuration and registry distribution

The implemented experimental slice is shared by Biei and Ishikari and uses an
object-store registry. It is enabled only when `BIEI_AUTH_REGISTRIES` or
`ISKR_AUTH_REGISTRIES` is non-empty. Biei protects static-render routes;
Ishikari protects public delivery content while leaving operational and
cluster-internal peer routes on their existing network trust boundary. Both
accept either `Authorization: Bearer` or an explicit `access_token` query
parameter. Mixed or repeated transports are rejected. Query transport exists
for browser/map clients that cannot set headers and requires URL-log redaction,
restrictive referrer policy, and deliberate CDN cache-key handling.

Ishikari propagates a verified query token only into same-origin URLs produced
by its own style, TileJSON, derived TileJSON, and preview transformations. It
does not attach the token to external URLs retained from provider style JSON,
and it never converts an Authorization header into a URL credential.

The reader implementation currently establishes:

- complete, validated registry snapshots with an explicit revision;
- atomic replacement of the in-process reader view;
- resident last-known-good operation; and
- no steady-state per-request dependency on registry storage.

Production-like enablement additionally requires a single writer or
compare-and-swap semantics for management mutations, freshness telemetry, and
separate read and write identities. Those are deployment gates, not properties
provided by the current reader merely because it can load `current.json`.

This is a control-plane distribution and availability mechanism, not a reason
to invent a general database inside either server. The schema should be driven
by implemented policy rather than speculative claim fields.

Registry freshness and token-revocation latency need explicit metrics and an
alerting threshold before a registry is relied on operationally.

The maximum acceptable snapshot age is a tier-specific availability decision,
not a universal hard-coded timeout. Commodity delivery may intentionally keep
serving a last-known-good snapshot while raising a loud stale-registry alert;
strong/private access should fail closed after its documented bound.

### 10.1 Registry-local current object

The initial object-store shape is one self-contained object per registry:

```text
{auth_root}/current.json
```

The current v1 shared schema contains `schema_version`, `registry_id`, a monotonic
`revision`, and a bounded `credentials` array. Each credential entry contains a
one-way `credential_sha256`, bounded `principal_id`, enabled state, namespace
and exact `render.static` or `read` grants, exact allowed origins, and an explicit
`allow_missing_origin` policy. It never contains raw API keys, recoverable
secrets, private signing keys, or arbitrary secret-manager payloads. Rate tiers,
validity windows, additional actions, and external verifier descriptions remain
future schema work rather than ignored fields.

The object is conditionally replaced as a whole. Readers use its strong object
validator or generation for conditional refresh and never list a prefix or
infer current state from object names. Keeping one complete registry in one
object minimizes storage operations and makes one candidate revision the unit
of validation and in-process replacement. Per-token objects, one global
all-token `current.json`, manifests, and registry shards are intentionally not
the starting design. Split one registry only after measured compressed size,
decode time, update amplification, or resident memory demonstrates a problem.

A bounded trusted catalog maps each `registry_id` to its auth root and reader
configuration. A shared root template may contain `{registry_id}`, but expansion
happens only after the ID is found in that catalog. Request parameters, styles,
delivery keys, and registry contents must never supply an arbitrary auth-root
URI. The loader allows only configured schemes and sources, and its cache
identity includes the registry ID and resolved auth-source identity so a root
change cannot reuse bytes loaded from the previous source.

The first design deliberately omits a registry-level namespace ceiling. The
centrally controlled writer is authoritative for the namespace/action grants in
each token entry. If registry roots or writers are later delegated to customers,
that changes the trust boundary and must first add a trusted ceiling outside the
customer-written registry.

### 10.2 Local cache and refresh behavior

Every ingress pod keeps a separate bounded auth cache keyed by configured
`registry_id`; the source URL is immutable for that process. It is not part of the tile, PMTiles, style,
resource, or rendered-output caches: content-cache eviction pressure must not
silently discard security state. Each cache entry is an immutable snapshot
shared by readers and replaced atomically only after complete validation.

Loading and refresh use:

- single-flight per registry and auth source;
- a process-wide bound on concurrent registry loads;
- conditional reads using a strong validator or generation;
- bounded document size, token count, field lengths, and decode work;
- short negative caching for configured-but-missing registry objects;
- an independent 64 MiB weighted snapshot cache, isolated from content-cache
  eviction pressure; and
- last-known-good retention when a replacement is missing, malformed,
  unverifiable, or temporarily unavailable.

Cold activation and negative caching are bounded independently of attacker-
controlled token strings. Unknown registry IDs never reach the loader. A
deployment using a root template must expand only IDs already present in the
trusted catalog, apply pre-auth request limits, and cap negative entries so
malformed tokens cannot produce unbounded storage reads or cache growth.

Verification of one request remains an O(1) local token lookup followed by one
bounded secret-verifier comparison and authorization against the same captured
snapshot revision. Caching individual allow/deny decisions is not initially
useful: a correct key would include credential, action, origin, resource
selector, and policy revision, adding cardinality and invalidation risk around
an already cheap lookup.

### 10.3 Gossip, HRW, and peer-assisted distribution

Object storage is the durable authority. Pods do not vote on auth state and do
not create a second consensus system. Cluster mechanisms may accelerate
convergence and collapse duplicate reads without becoming authoritative:

- Gossip carries only a bounded update hint such as
  `(registry_id, revision, digest)`. It never carries tokens,
  verifier digests, grants, registry bodies, or auth-root URLs.
- Chitchat state must not grow one persistent key per registry. A node may
  overwrite one bounded latest-update hint; periodic polling is the correctness
  backstop when rapid updates overwrite an intermediate hint.
- A received hint schedules a jittered, single-flight refresh. It cannot force
  a downgrade, change the configured source, or install policy by itself.
- HRW may select bounded peer candidates for a cold snapshot or refresh using
  `(registry_id, revision)`. It must not route every authentication
  decision to an auth owner; warm request verification remains local on every
  ingress pod.
- The selected peer may serve a cached snapshot, and object storage remains the
  fallback when peers miss, disagree, drain, or are unavailable.

Peer identity alone is not sufficient authority for registry contents. Letting
one compromised pod publish unsigned policy would expand a one-pod bypass into
cluster-wide authorization poisoning. A peer-provided snapshot is installable
without an object-store validation read only when it carries a management-plane
signature over its exact registry identity, revision, validity bounds, and
payload digest. Validators hold only the public verification key.
Without that signature, a peer may save body bandwidth only after the receiver
has anchored the expected digest or generation independently in object storage.

A signature proves authorship and integrity, not that a revision is still the
current object-store value. Running pods reject revisions below their observed
floor and periodically reconcile with `current.json`. Fresh pods in a
strong/private tier bootstrap each registry from object storage before accepting
peer state; the commodity-read tier may accept an unexpired signed peer snapshot
only under its explicit last-known-good and anti-replay policy.

Signing the registry snapshot is distinct from signing every delivery token:
opaque browser-visible delivery keys still use the cheap local registry lookup,
while one signature is verified only when a registry snapshot changes.

### 10.4 Internal refresh control

An internal endpoint may expose an idempotent refresh trigger, conceptually:

```text
POST /_internal/auth/refresh
{ "registry_id": "customer-a", "observed_revision": 42 }
```

The endpoint treats its body only as a hint. It validates a bounded canonical
registry ID against the trusted catalog, resolves the auth root from local
configuration, coalesces the work with the registry single-flight, schedules
refresh, and returns without accepting caller-supplied policy. It must not
accept an auth-root URL, registry body, secret, signing key, or authoritative
revision from the caller.

This endpoint lives only on the existing internal listener. That listener's
network trust boundary, bounded body and concurrency limits, configured-
registry validation, and per-registry refresh cooldown must prevent an
internal caller from turning reloads into object-store or CPU amplification.
Gossip receivers normally invoke the same refresh service directly in-process;
HTTP remains useful for operational reloads, tests, and a management-triggered
hint.

Biei and Ishikari converge independently inside their own gossip clusters.
Their shared authority is the configured registry and central writer, not a new
cross-service gossip or consensus cluster.

## 11. Code and ownership boundaries

Authentication should preserve the current separation between entry-point
configuration and reusable service code:

- `servers/*` reads environment/configuration and assembles verifier adapters;
- service core crates consume typed verifier/configuration objects;
- domain routers own route-level authorization decisions;
- cache keys describe representations, not credentials;
- gossip never carries raw external credentials; Biei's render wire may carry
  the bounded redacted ordinary provider token only under the selected
  trusted service boundary, alongside namespace grants and a one-way
  credential-and-policy cache partition;
- simulators model authentication cost only when a measured question requires
  it.

The extracted `mmpf-auth` crate remains limited to the stable verifier, registry
reader, credential-carrier parsing, and bounded namespace-grant model consumed
by both servers. HTTP response policy, style rewriting, render-cache admission,
content storage, peer routing, and rate limiting remain with their owners.

Errors exposed to callers should remain coarse (`missing`, `invalid`,
`forbidden`, or temporarily unavailable where appropriate). Detailed verifier
or registry failures belong in bounded internal telemetry and must not reveal
secret material.

## 12. Suggested adoption order

1. Define route classes and decide which current routes actually require
   delivery authentication.
2. Add a local `StaticApiKeys` verifier, typed delivery principal, bounded
   metrics, and tests proving authentication happens before expensive work.
3. Add edge request/egress limits and validate CDN log-based usage accounting.
4. Carry the ordinary delivery token through the trusted Biei-to-Ishikari style
   path and enforce namespace requirements on Biei cache hits. Add a workload
   identity only as an additional transport identity, never as broader content
   authorization.
5. Add portable OIDC management and workload publishing identity when a
   management/publishing API exists.
6. Introduce a dynamic registry only when static distribution is an observed
   operational constraint.
7. Add signed capabilities or a strong private-data tier only after their
   client, CDN, confidentiality, and revocation requirements are concrete.

Each stage should have a deployable rollback and must preserve shared-cache
behavior unless the security requirement explicitly calls for isolation.

## 13. Acceptance criteria for the first delivery-auth change

Before enabling it in the demo or production-like deployment, tests and metrics
should demonstrate that:

- missing, malformed, unknown, and disabled keys fail closed;
- a valid, authorized key reaches both cached and uncached responses;
- unauthorized requests consume no render or remote-storage admission;
- credentials and unbounded identifiers do not appear in logs or metric labels;
- equivalent authorized callers share the same semantic cache entry;
- registry/config refresh failure keeps only a last-known-good snapshot, never
  an implicit allow-all state;
- startup without required verification material remains unready or rejects the
  protected routes;
- management/internal-only Biei-to-Ishikari requests may use a workload
  identity, while caller-selected content never relies on that identity for
  authority broader than the caller's delivery namespaces;
- a rendered cache entry is returned only when the caller's freshly verified
  readable namespaces satisfy one of the entry's bounded requirement sets;
- warming Biei and Ishikari caches with a broader token cannot make inaccessible
  resource bytes or rendered output visible to a weaker token;
- an invalid recognized credential never falls through to another mechanism;
- requests carrying credentials for multiple mechanisms are rejected; and
- the deployment documents every CDN capability assumed by its access,
  enforcement, cache, and accounting claims.

The object-store registry tests must continue to prove that:

- an unknown `registry_id` is rejected without constructing an auth-root URI or
  performing storage I/O;
- a token cannot authorize a namespace or action absent from its registry entry;
- `Origin`, `Referer`, and missing-browser-context cases follow the captured
  token policy without string-prefix matching;
- concurrent cold loads and refresh hints collapse to one bounded load;
- a malformed, stale, oversized, or unverifiable candidate never replaces the
  last-known-good snapshot;
- changing a registry's auth root cannot reuse a cache entry from the previous
  source;
- gossip and the internal refresh endpoint cannot inject an auth-root URI or
  install policy; and
- unsigned, substituted, expired, or downgraded peer snapshots are rejected
  whenever peer-assisted loading is enabled.

Latency and CPU measurements should include cache-hit-heavy traffic. A verifier
that looks cheap only beside a cold render can still be a significant regression
for glyph, sprite, metadata, and hot tile requests.
