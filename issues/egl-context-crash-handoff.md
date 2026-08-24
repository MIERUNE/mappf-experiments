# Handoff: long-lived pods die with SIGSEGV on `eglCreateContext` (0x3001)

Written for codex. Everything below separates *measured* from *inferred*, because two
of my earlier hypotheses about this symptom were wrong and both cost real time.

## What was measured

`biei-699d7766f8-czx6p`, deployed digest `cab7755e`, uptime **4d5h**. A single static
render sent directly to the pod (no Gateway) produced `504` at 10.02 s, and the pod's
`restartCount` went to 1. From `lastState`:

    exitCode: 139            (SIGSEGV)
    startedAt:  2026-08-19T00:19:31Z
    finishedAt: 2026-08-23T05:31:09Z

From `kubectl logs --previous`, the final four lines:

    05:31:09.036  WARN  biei::http::response: render request failed failure_kind=RenderTimeout
    05:31:09.217  INFO  handle_incoming{... style_id=carto/positron-gl-style}: style content changed
    05:31:09.675  ERROR maplibre_native::bridge: OpenGL (code=-1) eglCreateContext() returned error 0x3001
                  terminate called after throwing an instance of 'std::runtime_error'
                    what():  Error creating the EGL context object.

`0x3001` is `EGL_NOT_INITIALIZED`. So the `504` was not a slow render: the process died
while trying to build a renderer for the next request.

The surviving sibling `biei-699d7766f8-5bhql` (uptime 2d1h) has **not** been rendered
against on purpose — it is the only live specimen. Its state:

    biei_renderer_replacements_total{outcome="success"}  1
    biei_renderer_orphan_threads                        1
    biei_renderer_slots{available}                      3   (of 3)
    biei_renderer_health{state="full"}                   1
    mmpf_mln_resource_refresh_deferred_total{glyphs}   152   (inflight 0)
    mmpf_mln_resource_fresh_cache_race_total             0
    no EGL/OpenGL lines anywhere in its log

## What is established

1. The crash path is `eglCreateContext` failure -> `std::runtime_error` -> `terminate`
   -> SIGSEGV. `platform/linux/src/headless_backend_egl.cpp` throws on every EGL setup
   failure (`eglGetDisplay`, `eglInitialize`, `eglBindAPI`, `eglChooseConfig`,
   `eglCreateContext`), and nothing between there and Rust converts that to an error.
2. One slot's EGL failure therefore kills the whole process, which makes Biei's
   quarantine / orphan-budget / slot-replacement design ineffective for this class of
   fault. That is a defect independent of the root cause.
3. Biei itself performs no EGL calls; the lifecycle lives entirely in the vendored
   maplibre-native platform layer.

## What is NOT established (and two corrections)

**Why the display became `NOT_INITIALIZED` is unknown.**

I first claimed the shared `EGLDisplayConfig` refcount reaches zero and runs
`~EGLDisplayConfig() { eglTerminate(display); }` during operation. **That is wrong.**
`create()` holds the instance in a function-local `static`, so one reference persists
for the life of the process; the destructor cannot run while the process is serving.
I read the `shared_ptr` return and missed the static.

Earlier I also spent two sessions on a deferred-refresh/park hypothesis for this
symptom. That was wrong too: `refresh_deferred_inflight` was 0, `fresh_cache_race_total`
was 0, and the process exited on a signal. The 152 deferred glyph refreshes are
ordinary accumulated background work and unrelated. Checking
`containerStatuses[*].lastState` would have shown `exitCode 139` in one step.

Candidate causes, none verified:

- **Context leak via orphan threads.** A detached renderer thread never returns, so its
  `EGLBackendImpl` is never destroyed and `eglDestroyContext` never runs. The surviving
  pod has exactly one replacement and one orphan. Caveat: exhaustion normally reports
  `EGL_BAD_ALLOC`, not `NOT_INITIALIZED`, so this does not obviously produce 0x3001.
- **Another `eglTerminate` path** somewhere in the vendored tree or in Mesa.
- **Driver-side state loss** (llvmpipe/Mesa) after days of context churn, or an fd /
  memory limit corrupting EGL's internal init state.

These imply different fixes, in different repositories. I am deliberately not guessing
again.

## Questions for codex

You have had `biei-diag` running for 4d9h and `biei-codex-diag` with
`renderer::actor::backend=debug`, so you may already hold observations I do not.

1. Has either diag pod ever logged an `eglCreateContext` / `eglInitialize` failure, or
   any `OpenGL (code=-1)` line? If yes, what was the uptime and what preceded it?
2. Do the diag pods show `renderer_replacements_total` > 0 and `orphan_threads` > 0? I
   want to know whether crashes correlate with *replacements* rather than with uptime.
3. Do you know of any other `eglTerminate` call site in the vendored maplibre-native, or
   anything that tears down the EGL display outside `~EGLDisplayConfig`?
4. What EGL/Mesa implementation and version does the runtime image carry, and is there a
   known context or fd ceiling we could be reaching?
5. Is the detached-orphan design expected to leak the EGL context permanently? If so,
   the orphan budget bounds threads but not GL resources, and that asymmetry may be the
   real accumulation.

## Proposed split, for your agreement or objection

- **mln-rs first, independent of root cause:** catch C++ exceptions at the binding
  boundary and return a `Result`, so an EGL failure quarantines one slot instead of
  killing the process. This is valuable whatever 0x3001 turns out to be, and it makes
  the remaining investigation survivable in production.
- **maplibre-native / Mesa second:** only after the cause is identified. Editing
  `headless_backend_egl.cpp` on my current understanding would be another guess.

If you disagree with that ordering, say so — in particular if you think the leak is
established well enough to fix directly.

## Perishable evidence

`biei-699d7766f8-5bhql` is alive with one replacement and one orphan. Rendering against
it will likely reproduce the crash and destroy the specimen. Nothing has been sent to it.
