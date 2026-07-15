# Derived Isoline and Hillshade Specification

## Status and Scope

Ishikari can derive contours and several hillshade representations from the
configured Mapterhorn Terrarium DEM tileset. This is an experimental extension
to Ishikari's primary PMTiles delivery path, not a general rendering framework.

This document describes the current implementation and the research decisions
behind it. The Rust implementation remains the source of truth:

- `ishikari/src/server/tileset/terrain/mod.rs`: HTTP and generation orchestration
- `terrain/dem.rs`: Terrarium decode and neighborhood access
- `terrain/contours.rs`: contour generation
- `terrain/hillshade.rs`: illumination, quantization, vector/raster products
- `terrain/topology.rs`: shared boundaries and polygon assembly
- `server/tileset/preview.rs`: reference preview styles

Open optimization work is tracked in `ishikari-todo-spec.md`; it is not repeated
as an implementation plan here.

## Products and Public Contract

Derived resources use:

```text
/tilesets/{tileset_id}/derived/{product}/{z}/{x}/{y}
```

They are available only for the configured Mapterhorn composite tileset.

| Product | Representation | Current role |
|---|---|---|
| `contours` | MVT, optionally transcoded to MLT | Isolines with elevation and major-line classification |
| `hillshade` | MVT, optionally transcoded to MLT | Baked vector shadow/highlight polygons |
| `hillshade-raster` | lossless WebP | Quantized signed shade codes for `color-relief` |
| `hillshade-webp-lossy` | lossy WebP, quality 80 | Continuous signed shade field for size/quality comparison |
| `hillshade-jpeg` | JPEG, quality 85 | Continuous-field comparison baseline |

Vector layers have these schemas:

```text
contours: ele: Number, level: Number
hillshade: class: "shadow" | "highlight", level: Number
```

The vector hillshade uses a fixed sun and a reference palette. `level` is not a
raw Lambert illumination value: it is a perceptual tone code derived from the
composited reference MapLibre style. Consumers must not reinterpret it as slope,
aspect, elevation, or arbitrary palette-independent lighting.

The raster products carry the same signed tone code as grayscale RGB. The
preview exposes them as pseudo-Terrarium `raster-dem` sources and applies the
palette with a `color-relief` expression. This is an encoding convention for a
shade field, not actual elevation data.

## Shared DEM and Execution Pipeline

Every product reuses Ishikari's normal tileset machinery:

```text
request
  -> Mapterhorn base/detail archive resolution
  -> ResourceResolver / HRW routing
  -> tile and chunk caches / backend range batching
  -> Terrarium decode cache
  -> derived-product generation
  -> generated-output cache
```

Generation obtains the center DEM and eight neighbors concurrently. The center
is mandatory; missing or transiently failing neighbors fall back to the nearest
center edge. This permits sparse detail coverage while keeping derivatives at
available edges defined.

The 3x3 neighborhood is an input halo, not a 3x3 output metatile. Only the
requested output tile is generated. Adjacent outputs share six source tiles, so
decoded DEM initialization is single-flighted by source tile and cached as
`Arc<DemTile>`. Contours and all hillshade products therefore reuse decoded
elevations.

The current resource controls are:

- generated outputs are single-flighted and byte-weighted in a pod-local cache;
- decoded source DEMs are single-flighted and byte-weighted;
- authoritative center absence is negative-cached for the tile-negative TTL and
  returned as a cacheable `404`;
- transient center errors are not negative-cached;
- object-store/peer fetch happens before a CPU permit is acquired;
- WebP decode, contour/hillshade generation, and MLT transcode use
  `spawn_blocking` behind the shared `ISKR_CPU_WORK_CONCURRENCY` limit.

Generated outputs are not currently assigned to a cluster-wide HRW owner. Two
replicas can generate the same cold derived tile independently.

## Contours

### Baseline and Current Algorithm

The research baseline was `maplibre-contour`, whose isoline implementation is a
pragmatic d3-contour-derived Marching Squares pipeline. Its important ideas are
retained: neighboring DEM access, pixel-center to grid-point conversion, linear
threshold interpolation, fragment stitching, and scanning only thresholds that
cross each cell's minimum/maximum range.

Ishikari's current implementation adds three material refinements:

1. The elevation grid is filtered with a separable binomial `[1 2 1]^2` pass.
   It suppresses pixel noise while preserving linear fields and therefore does
   not move contours on a uniform slope.
2. Ambiguous Marching Squares cases 5 and 10 use an asymptotic decider based on
   the bilinear surface rather than a fixed diagonal.
3. Completed lines use deterministic, heap-driven Visvalingam-Whyatt
   simplification. Vertices within one source pixel of a tile edge are pinned.

The cell loop covers a one-source-pixel buffer outside the output tile. Crossing
positions are linearly interpolated, then quantized to the normal MVT extent of
4096. Endpoint-keyed fragment maps stitch segments into lines.

### Zoom Profile

| Zoom | Minor interval | Major interval |
|---:|---:|---:|
| 0-10 | 250 m | 500 m |
| 11 | 100 m | 500 m |
| 12 | 50 m | 200 m |
| 13 | 20 m | 100 m |
| 14+ | 10 m | 50 m |

Each feature stores `ele` in meters. `level=0` is a minor contour and `level=1`
is a major contour used by the preview for stronger lines and labels.

## Hillshade Field

### Reference Rendering Model

The current field follows MapLibre's `standard` raster hillshade behavior more
closely than the initial Horn-plus-Lambert research proposal:

- sample the DEM directly at a 512x512 grid with a one-cell halo;
- do not pre-blur the hillshade DEM field;
- compute Horn/Sobel derivatives from eight neighbors;
- convert horizontal distance with latitude- and zoom-dependent meters/pixel;
- apply MapLibre's piecewise low-zoom derivative exaggeration;
- use map-anchored azimuth 315 degrees;
- reproduce the shader's aspect-driven shadow/highlight mix and slope-only
  accent term, including sRGB operation order;
- composite against the reference background and convert the result to a
  signed CIE L* shift.

The reference colors are:

```text
shadow:    #253033
accent:    #657074
background:#c9ccca
highlight: #ffffff
```

This deliberately bakes the final visual effect rather than storing normals.
Interactive relighting should use the original raster DEM and MapLibre's
hillshade layer; slope/aspect polygons would be larger, faceted, and inferior to
GPU per-pixel shading.

### Perceptual Quantization

The vector field has a total budget of 32 non-neutral tone levels. Shadow and
highlight do not receive equal counts: their counts are calculated from the
square root of each side's CIE L* span. This gives more levels to the larger
shadow range without starving highlights.

Near-neutral shadow steps are perceptually important, so a smooth companding
curve is solved such that the first shadow level is approximately 2.5 L*. The
remaining levels expand progressively across the available span. A signed code
with magnitude below half a level is neutral.

Both forms are retained during vector generation:

- integer labels determine region connectivity and output properties;
- continuous signed codes locate threshold crossings between cells.

This separation is important. Label-only tracing creates pixel-edge staircases;
continuous crossings preserve sub-cell boundary positions without making
topology dependent on floating-point equality.

## Vector Hillshade Topology

The current polygonizer is a hybrid of the research approaches rather than a
literal 81-case isoband table or a VTracer integration:

1. Four-connected components are found in the quantized label grid.
2. Isolated one-cell components are merged into the best adjacent stable
   component. The one-cell halo makes the decision deterministic at output tile
   boundaries; larger regions are retained to preserve terrain texture.
3. Boundaries between label pairs are represented once as shared arcs.
4. The crossing for each arc edge is interpolated from the continuous tone
   field. Shadow/highlight boundaries use zero; same-side bands use their
   midpoint threshold.
5. Arcs stop at junctions and tile anchors, are simplified once, and are reused
   in opposite directions by adjacent faces.
6. Directed arcs are assembled into outer rings and holes. Neutral label zero is
   omitted, leaving the background visible.

Coordinates use a 1024 extent: half-source-pixel fixed point for a 512 grid.
This is enough to preserve useful threshold interpolation while keeping deltas
smaller than a conventional 4096 extent.

The simplifier removes collinear points and uses a VTracer-inspired geometric
penalty. The older categorical path also contains signed-area staircase
removal, but current vector hillshade boundaries use the interpolated path and
do not need grid staircase cleanup. Because each shared arc is simplified once,
adjacent shade faces cannot independently drift apart into slivers.

The simplifier is not yet fully topology-constrained: it does not test every
candidate against unrelated arcs, self-intersection, face reversal, or narrow
face collapse. Tolerance must not be increased until those constraints are
implemented and tested.

## Raster Alternatives

The initial research assumed vector polygons were the target. Current evidence
does not justify making that assumption permanent. High-frequency hillshade can
require many polygons and vertices; a quantized or continuous raster avoids all
geometry overhead.

The three experimental raster products evaluate that tradeoff:

- lossless WebP stores rounded signed levels exactly and compresses the small
  alphabet well;
- lossy WebP stores the continuous code, avoiding vector band boundaries while
  permitting small sub-level codec error;
- JPEG provides a broadly understood size baseline, but generally has worse
  edge behavior and no advantage for transparency.

All are palette-colored client-side in the preview. This preserves recoloring
within the current signed-tone contract, but the fixed sun and reference
composite remain baked into the scalar field.

Raster is expected to win when fixed resolution and overzoom blur are
acceptable. Vector can still be useful for a vector-only stack, print/export,
crisp overzoom, feature inspection, or MLT-native delivery. Selection must come
from measurement rather than format preference.

## Preview Contract

The Mapterhorn preview exposes:

- MapLibre raster hillshade from the original DEM (default/reference);
- vector hillshade;
- lossless WebP, lossy WebP, and JPEG `color-relief` variants;
- raw DEM raster;
- independently enabled contours and labels;
- tile-boundary debug rendering.

The vector style uses separate fill layers for `shadow` and `highlight`, a
transparent outline, and opacity stops generated from the same tone profile as
the encoder. The raster `color-relief` stops use those same opacities.

## External Research Retained

### MapLibre Contour

Marching Squares remains the appropriate general isoline algorithm. The useful
`maplibre-contour` optimization is processing all thresholds crossing a cell in
one grid pass instead of scanning the full matrix once per level. Ishikari keeps
that model while addressing its fixed saddle choice and lack of simplification.

### VTracer

VTracer is not a drop-in fit because its main output advantage is Bezier curves,
which MVT/MLT cannot represent, and independently traced color regions do not
share topology. The useful borrowed ideas are component/speckle handling and a
penalty-based boundary simplifier. RGB clustering and curve fitting are not used.

### Inspected Vector Tile

One inspected vector PBF sample contained a `hillshade` polygon layer with 425
features, 724 rings, 8,564 vertices, and attributes `class` and `level`. Classes
were `shadow` and `highlight`; six distinct level values occurred. The complete
tile, including non-hillshade layers, was 37,678 bytes.

These are verified observations about that file, not proof of Mapbox's private
generation algorithm. They support baked shadow/highlight polygons, sparse
neutral output, small attributes, and aggressive simplification as reasonable
design choices. They do not verify the illumination model, thresholds,
metatiling, shared topology, or simplifier used to create the sample.

## Correctness Invariants

The implementation and tests should preserve these invariants:

- adjacent contour tiles agree at their shared boundary;
- linear slopes retain contour position through smoothing and simplification;
- saddle connectivity follows the interpolated bilinear surface;
- adjacent hillshade tiles sample identical shared illumination;
- one-cell region merging does not make different decisions at a shared output
  edge;
- each boundary is stored once and reused by both neighboring shade faces;
- continuous tones place boundaries at the correct signed threshold;
- neutral terrain emits no visible hillshade polygons;
- cache cancellation or transient source failure does not become a permanent
  negative result;
- CPU-heavy work remains bounded independently of request fan-out.

## Remaining Evaluation

The unresolved question is the Pareto frontier, not whether vectorization is
technically possible. Representative fixtures and zooms should compare:

- vector MVT and MLT;
- quantized lossless WebP;
- continuous lossy WebP and, if useful, AVIF;
- the original DEM rendered by MapLibre's raster hillshade;
- several tone budgets and safe simplification tolerances;
- single-tile generation and request-coalesced metatiles only where measured.

Record compressed bytes, features/rings/vertices, source/decode/generation time,
client decode/render time, peak memory, seam failures, zoom stability, SSIM or
equivalent structural error, and perceptual color error such as OKLab Delta E.
The target is the best rendered result under byte, CPU, latency, and complexity
budgets, not pixel-for-pixel preservation at any cost.
