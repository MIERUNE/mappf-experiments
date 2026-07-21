# Specifications

This directory records durable product, component, and fidelity contracts. Active implementation work belongs in [`../issues/`](../issues/README.md) or the cross-cutting [`../refactor.md`](../refactor.md) queue.

| Document | Status | Scope |
|---|---|---|
| [`biei-spec.md`](biei-spec.md) | Current production contract | Biei routing, rendering, HTTP, resource loading, and operational boundaries |
| [`ishikari-spec.md`](ishikari-spec.md) | Current production contract | Ishikari positioning, invariants, public behavior, and module boundaries |
| [`ishikari-sim-spec.md`](ishikari-sim-spec.md) | Current simulator contract | Model, fidelity boundaries, calibration, and implemented simulator behavior |
| [`isoline-and-hillshade-spec.md`](isoline-and-hillshade-spec.md) | Experimental component contract | Derived terrain products and their bounded algorithms and HTTP representations |
| [`auth-sketch.md`](auth-sketch.md) | Exploratory; not adopted | Possible access-token design; not an implementation contract |

Code and tests are authoritative when they diverge from these documents. When an intentional contract changes, update the relevant specification and regression tests together. Do not turn specifications into progress logs or duplicate their open work here.
