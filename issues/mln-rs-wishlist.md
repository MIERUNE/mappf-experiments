# maplibre-native-rs wishlist

This is biei's own wishlist of binding additions that would help us build static-image-API-compatible features on top of maplibre-native-rs. It's just notes from a downstream user, not a roadmap.

We've tried to keep the requests general-purpose rather than biei-specific. biei handles its own application policy — URL grammar, body size and nesting depth, allowed layer types, resolving `source.url` as a tileset id, rejecting direct network URLs — so what's left here is mostly plain style / layer / source operations that happen to be hard to reach from Rust today.

Constraints that come from the MapLibre Native engine itself — thread affinity,
build-time-fixed MapMode / pixel ratio, no render cancellation, GeoJSON
normalization, no provider-specific service features — live in
[`biei-spec.md` §8.6](../specs/biei-spec.md#86-permanent-engine-constraints).
Keep those separate from things a binding could actually fix.

Only unlanded items live here. Once a PR lands upstream, its item is deleted (git history keeps the old wishlists).

This list was checked against the crates.io `maplibre_native` 0.8.7 source.
`AnyLayer`, `AnySource`, JSON construction, mutable GeoJSON source access,
process-global Rust FileSources, request priority/usage, response cache metadata
types, and image removal already exist and are intentionally not requested here.

There are two tiers:

- **Required** — without it, a biei feature simply can't be built, or production isn't safe. There's no usable workaround.
- **Improvement** — a workaround already makes the feature work, but the API would make it more correct, faster, or easier to maintain.

## Required

### 1. Surface C++ exceptions as Rust `Result`

| API shape | What it unblocks |
|---|---|
| Declare each FFI function as `Result<T>` so cxx's generated catch shim hands C++ exceptions back to Rust as `cxx::Exception` | Bad input returns a per-request HTTP error instead of aborting the process |

When a C++ exception goes uncaught it takes the whole process down. `mbgl::LatLng`'s constructor throws `std::domain_error` on out-of-range or NaN latitude, and `Style::loadJSON` throws `std::runtime_error` on bad input. biei added lat / lon / zoom / pitch range checks at ingress to cover the obvious paths, but that's a band-aid: we can't keep mirroring mbgl's entire validation surface ourselves. C++ barely uses `noexcept`, so there's no way to tell from the outside which functions throw, and almost anything can throw `std::bad_alloc` somewhere down the call stack. Letting one bad request kill an entire worker node isn't acceptable in production.

`style_add_layer`, `style_add_source`, `style_add_image`, and the GeoJson `parse` / `stringify` functions already return `Result`, so the pattern is established; extending it across the rest of the bridge would close the gap. From a downstream point of view the functions that bite biei first are `camera_for_*` (the LatLng `domain_error` path), `style_load_from_*`, and the render entry points.

An upstream issue should enumerate the first unsafe bridge calls and add focused
throwing tests; treating every allocation failure as a recoverable request error
is not required for the initial improvement.

### 2. Filter operations on an existing layer

| API shape | What it unblocks |
|---|---|
| Look up a layer by id and apply a filter JSON — e.g. `style.set_layer_filter(layer_id, filter_json) -> Result<(), StyleError>` | `setfilter` URL parameter |
| Return filter conversion errors | Validating filter expressions and turning failures into client errors |

`setfilter=[...]&layer_id=...` rewrites the filter on an *existing* base-style layer at runtime, and the landed new-layer path (`AnyLayer::from_json_str`) can't stand in for that. Faking it with remove → rebuild → add would mean fully restoring the layer's compiled state and its position in the style, so it isn't a real workaround. `setfilter` stays blocked until this lands.

The C++ engine already has layer lookup and `Layer::setFilter`, so one binding unblocks it.

## Improvement

### 3. Layer introspection

| API shape | Use |
|---|---|
| `style.has_layer(id) -> bool` | Strictly validate `before_layer={X}` — today a missing id silently falls back to "append at top" |
| `style.layer_ids() -> Vec<String>` (or an iterator) | Semantic positioning like "insert before the first symbol layer"; debugging |

A missing `before_layer` is documented as "falls back to top" and accepted, so
a typo cannot currently be rejected with a 400. `layer_ids()` would also enable
semantic positioning such as inserting below the first symbol layer. Source
existence no longer belongs on this list: `style.source_mut(id).is_some()` is
already a usable lookup when biei needs to validate a base-style source.

### 4. Style image introspection / fallible removal

| API shape | Use |
|---|---|
| `style.has_image(id) -> bool` | Avoid noisy cleanup attempts for request-local images |
| `style.remove_image(id) -> Result<bool, StyleError>` or `Option<ImageId>` | Distinguish "removed" from "not present" without relying on MapLibre Native warnings |

biei registers request-local marker images with `style.add_image` and removes
them after rendering. The current `remove_image` wrapper returns `()` while
MapLibre Native logs a warning when the image id is not present:
`Image '...' is not present in style, cannot remove`. biei now tracks successful
registrations and avoids speculative pre-removal, so the normal path is quiet;
introspection would still make partial-failure reconciliation and independent
downstream implementations explicit.

This is an improvement, not a blocker. A small image introspection/removal
result API would make the lifecycle explicit without requiring downstream code
to infer presence from its own bookkeeping.

## Notes

Priorities follow current biei blockers and measured maintenance cost, not a
historical stage number. When something lands upstream, delete its item here.

Things that work through an awkward workaround on biei's side — baking pin
labels into bitmaps, accepting the silent `before_layer` append, range-checking
coordinates at ingress — go in the improvement tier. Things with no workaround
at all (`setfilter`, FFI exception safety) are required. Things already covered
by `AnyLayer::from_json_str` — expressions, filters on newly constructed layers,
`source-layer`, visibility, detailed paint/layout properties, and untyped layer
types through `OpaqueLayer` — are deliberately absent. The same applies to
SymbolLayer text/icon rendering: the JSON construction path exists, so adopting
it for pin labels and adding end-to-end tests is biei work rather than a missing
binding. Render cancellation is an engine constraint tracked in
[`biei-spec.md` §8.6](../specs/biei-spec.md#86-permanent-engine-constraints),
not a Rust-binding wishlist item. And things biei
should own outright — the URL parser, polyline decoder, simplestyle property
reading, and tileset catalog — belong in biei's own roadmap, not here.

The process-global FileSource registration API landed in `maplibre_native`
0.8.4 and is sufficient for biei's one-process/one-provider model. Version 0.8.6
also exposes `ResourceRequest.priority` and `usage`, and 0.8.7 preserves all
FileSource response fields across the C++ bridge. The reusable
`mmpf-mln-filesource` crate consumes that API, and biei uses the crates.io
release. A renderer-scoped variant
is not a current wishlist item; revisit it only if a real multi-tenant or
per-renderer cache-isolation requirement appears — or if per-render in-render
I/O attribution ever needs to be exact in production: `ResourceRequest` carries
no requester identity (mbgl's engine-global `FileSource` interface has none to
forward), so the process-global source cannot know which render a fetch blocks.
biei currently estimates this statistically over verified resource-warm windows
and obtains exact attribution only in single-renderer-slot benches where every
regular-lane fetch belongs to the only in-flight render.
