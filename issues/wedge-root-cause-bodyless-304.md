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

## Regression coverage

Automated, in CI (this superseded the "blocked upstream" note kept here until
2026-08-27; `ResourceRequest` still cannot be constructed directly, but a real
renderer can drive the source end to end, which is what these do):

- `crates/mmpf-mln-filesource/tests/swr_end_to_end.rs` renders against a live
  HTTP origin that answers `304` after a delay, so a delivered stale body and
  its paired background revalidation are exercised through native.
- maplibre-native-rs PR #280 carries a native Database -> Network -> `304` ->
  render test covering the bridge behavior this fix depends on.

The `mock304` upstream (sprites with `s-maxage=10` + ETag, request log as
witness) turned the 1h natural cycle into a ~20s one and produced the matrix
above. Its deployment was removed on 2026-08-27 once the automated coverage
landed; the manifests stay in `demo-deploy/biei/diagnostics/mock304/` so the
deterministic in-cluster cycle can be recreated on demand.

## Production soak: the original condition, cleared (2026-08-28)

Both pods ran 41h on the fixed build (`62a8f7b` deploy) with essentially no
traffic — 5 renders total, and the background-revalidation counters did not move
for the last 33h of it. Idle time alone proves nothing here: the wedge needs a
render to arrive *after* the caches age, so the aged state was used as the
setup and the missing half was supplied deliberately.

Burst against 41h-aged caches, 12-way concurrent, 24 requests per pod, two
heavy styles each (the shape that wedged the pre-fix binary):

| pod | styles | result |
|-----|--------|--------|
| `jc88b` | `mierune/jp_mierune_{streets,gray}` | 24/24 `200`, 0.07–3.6s |
| `4pllq` | `carto/{voyager,dark-matter}-gl-style` | 24/24 `200` |

Zero wedges, orphans, renderer replacements, rejections, 5xx, or `WARN`/`ERROR`
lines on either pod. The aged-cache path was genuinely exercised, not skipped:
profile preparations went 2 -> 17 (the 1h style-JSON entries had long expired
and were refetched or revalidated), and the two pods pulled 168 and ~160
foreground glyph fetches — 42.7 MB on `4pllq` alone — inserting new entries.

This is the original real-world condition ("a pod left idle for hours can no
longer render") and the fixed build handled it. Same residual caveat as the
matrix above: no `prior_body=true` withheld-body 304 fired in-round (glyph 304
counters were unchanged; see the deferred-refresh note in `biei-todo.md`), so
that exact coincidence remains covered by the four pre-fix wedges plus
`swr_end_to_end.rs`, not by an in-round fixed-binary sample.

## Unrelated open issue

Mode A (`eglCreateContext → 0x3001 → SIGSEGV` after 4d5h) is a different
failure with a different signature and remains tracked in
`egl-context-crash-handoff.md`. The pinned core with PR #4332 may have
addressed it; only long uptime will tell.
