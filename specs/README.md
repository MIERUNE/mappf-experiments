# Specifications

This directory records durable product, component, and fidelity contracts. Active implementation work belongs in [`../issues/`](../issues/README.md), including the cross-cutting [`refactor.md`](../issues/refactor.md) queue. `auth-sketch.md` is an unadopted proposal; `resource-layout-sketch.md` clearly separates its adopted PMTiles template contract from proposed publishing conventions.

| Document | Status | Scope |
| --- | --- | --- |
| [`abashiri-spec.md`](abashiri-spec.md) | Initial authenticated boundary | Management-plane ownership, object-store management auth, HTTP contract, conditional-write invariant, and non-HTTP style publication core |
| [`biei-spec.md`](biei-spec.md) | Current production contract | Biei routing, rendering, HTTP, resource loading, and operational boundaries |
| [`ishikari-spec.md`](ishikari-spec.md) | Current production contract | Ishikari positioning, invariants, public behavior, and module boundaries |
| [`ishikari-sim-spec.md`](ishikari-sim-spec.md) | Current simulator contract | Model, fidelity boundaries, calibration, and implemented simulator behavior |
| [`isoline-and-hillshade-spec.md`](isoline-and-hillshade-spec.md) | Experimental component contract | Derived terrain products and their bounded algorithms and HTTP representations |
| [`auth-sketch.md`](auth-sketch.md) | Exploratory; not adopted | Possible access-token design; not an implementation contract |
| [`resource-layout-sketch.md`](resource-layout-sketch.md) | Partly adopted | Implemented PMTiles source-template contract plus proposed content publishing layout |

Code and tests are authoritative when they diverge from these documents. When an intentional contract changes, update the relevant specification and regression tests together. Specifications define behavior and fidelity boundaries; queues own unresolved actions and decisions.
