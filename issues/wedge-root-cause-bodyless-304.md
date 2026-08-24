# Resolved: renderer hard-wedge from bodyless 304 on a withheld prior body

Status: fix implemented and deployed (build `9741461b`, digest `b2bbcc38…`);
deterministic A/B confirmation complete (matrix below); overnight production
soak is the last standing check.

## Symptom

A renderer actor's native render never returns. The thread parks in
`epoll_pwait(timeout=-1) → uv_run → currentThreadRunLoopWait → render` with
zero CPU, indefinitely (observed 12h+). Orphan capacity is consumed on the next
hard-wedge; once exhausted, `/livez` fails and kubelet restarts the pod
(exit 0, `reason: Completed` — not a crash). In production this presented as
"a pod left idle for hours can no longer render".

Backtraces: `bt-7p25r-wedge-20260824.txt` (production specimen),
`bt-canary-wedge-20260824.txt` (controlled reproduction, identical stack).

## Root cause

`crates/mmpf-mln-filesource` replaced MapLibre Native's `OnlineFileSource` but
did not replicate its 304 materialization
(`online_file_source.cpp`: `if (response.notModified && resource.priorData)`).

When a cached entry requires revalidation (any `s-maxage`, `no-cache`, or
`must-revalidate` — ishikari's styles/TileJSON carry `s-maxage=3600`), native
withholds the stale body from its consumer and attaches it to the network
request as `priorData`. The stock loader merges that body into an upstream 304
before any consumer sees it. Biei's source returned the bodyless 304 as-is;
glyph/sprite/tile/source consumers treat `notModified` as "keep what you have"
and return **without emitting their load-completed event**. With nothing held,
the still render's completion condition is never reached and the run loop waits
forever.

A test (`not_modified_with_prior_body_stays_bodyless_for_native`) locked in the
wrong contract; the enabling comment claimed "MLN can merge this bodyless
response with priorData itself" — true only of the layer this crate replaced.

## Why it looked like other things

- "Breaks after sleeping": expiry takes `s-maxage` (1h); sparse traffic means
  the first post-expiry request arrives hours later.
- "Only forwarding targets wedge": spurious — which pod held aged entries.
  The single-node canary reproduced it without forwarding.
- Two workers wedging 1.2ms apart: one style-swap burst revalidates the same
  expired resources on every slot; the incident's two TileJSON 304s match the
  two wedged workers.

## Controlled reproduction (A-side)

Same canary pod, same binary (pre-fix + instrumentation), same 24-request
burst of two heavy styles:

- fresh cache: 150+ requests, zero wedges
- 1h after first load (`s-maxage` elapsed): wedged on the first burst
  (`stage="render"`, `op="render"`, generation/tid logged; gdb stack identical
  to the production incident)

## Fix

`not_modified_attempt` now branches on the body's origin:

- `native_data` present (native withheld the representation): native receives
  the materialized body, `not_modified = false` — the stock loader's merge.
- body known only to the Rust shared cache (background refresh): native keeps
  receiving a bodyless 304 (re-sending would force consumers to re-parse an
  unchanged representation); only the Rust cache takes the materialized entry.

`PriorResponse` was split into `native_data` / `cache_data` so the type system
enforces the distinction. Regression tests:
`a_304_for_a_withheld_prior_body_reaches_native_with_that_body`,
`a_304_backed_only_by_the_shared_cache_stays_bodyless_for_native`.

## Deterministic confirmation (completed 2026-08-24/25)

A `mock304` upstream (sprites with `s-maxage=10` + ETag, 304 on If-None-Match,
request log as witness) turned the 1h natural cycle into a ~20s one. Matrix,
same canary pod spec, binary as the only variable:

| condition            | pre-fix (`instr2`)      | fixed (`b2bbcc38`)          |
|----------------------|-------------------------|-----------------------------|
| fresh + concurrent   | no wedge (150+ reqs)    | no wedge                    |
| stale + sequential   | no wedge                | no wedge (6/6, see below)   |
| stale + concurrent   | **wedge, first round**  | **no wedge (3/3 rounds)**   |

Concurrency is a necessary ingredient: sequential stale cycles produce 304s
(mock log) but no wedge on either binary. On the fixed binary the dangerous
condition itself was directly observed handled:
`kind=sprite_* priority=regular prior_body=true prior_validator=true
outcome=ok response_bytes=70` — a natively withheld body came back on a 304,
was materialized, and the render completed.

B-side on production (fixed build, natural 1h expiry, 24-request burst):
zero wedge lines on both pods, orphans 0, replacements 0, renders 200, with
`outcome="not_modified"` background refreshes recorded (glyphs 471, source 6,
sprite 3+3) — the revalidation path was live.

Natural-cycle reproductions of the pre-fix wedge: production incident
(2026-08-23 16:36), production burst (2026-08-24 13:29), canary natural 1h
(16:18), canary mock (17:05). Instrumented wedges log
`stage="render"`, `op="render"`, generation/tid, task/style identity.

Residual caveat: in the fixed × stale × concurrent rounds no `prior_body=true`
fired in-round (the withheld/concurrent coincidence is probabilistic), so that
exact coincidence is covered by the sequential observation plus four pre-fix
wedges, not by an in-round fixed-binary sample. The overnight production soak
covers the original real-world condition.

## Confirmation still outstanding

- Overnight soak: the original real-world condition (idle hours, then render).
- A full-path automated regression is blocked upstream: `ResourceRequest` is
  `#[non_exhaustive]` with a `pub(super)` constructor, so the network source
  cannot be driven end-to-end from tests (recorded in `mln-rs-wishlist.md`).
  The in-cluster `mock304` deployment covers this gap operationally: a ~20s
  deterministic cycle exists for future regression checks.

## Unrelated open issue

Mode A (`eglCreateContext → 0x3001 → SIGSEGV` after 4d5h) is a different
failure with a different signature and remains tracked in
`egl-context-crash-handoff.md`. The pinned core with PR #4332 may have
addressed it; only long uptime will tell.
