# Specifications

This directory records durable product, component, and fidelity contracts. Active implementation work belongs in [`../issues/`](../issues/README.md), including the cross-cutting [`refactor.md`](../issues/refactor.md) queue. The two sketches distinguish implemented slices from proposals that remain open.

| Document | Status | Scope |
| --- | --- | --- |
| [`abashiri-spec.md`](abashiri-spec.md) | Experimental implemented slice | Authenticated style GET/PUT, conditional publication, durable audit/reconciliation, refresh notification, and operational aggregation |
| [`biei-spec.md`](biei-spec.md) | Current production contract | Biei routing, rendering, HTTP, resource loading, and operational boundaries |
| [`ishikari-spec.md`](ishikari-spec.md) | Current production contract | Ishikari positioning, invariants, public behavior, and module boundaries |
| [`ishikari-sim-spec.md`](ishikari-sim-spec.md) | Current simulator contract | Model, fidelity boundaries, calibration, and implemented simulator behavior |
| [`isoline-and-hillshade-spec.md`](isoline-and-hillshade-spec.md) | Experimental component contract | Derived terrain products and their bounded algorithms and HTTP representations |
| [`auth-sketch.md`](auth-sketch.md) | Partly implemented | Implemented delivery-auth boundary plus unresolved stronger-auth and distribution design |
| [`resource-layout-sketch.md`](resource-layout-sketch.md) | Partly adopted | Implemented PMTiles templates and style-object layout plus proposed sprite and glyph publishing conventions |

Code and tests are authoritative when they diverge from these documents. When an intentional contract changes, update the relevant specification and regression tests together. Specifications define behavior and fidelity boundaries; queues own unresolved actions and decisions.
