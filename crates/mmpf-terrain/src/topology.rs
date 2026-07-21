//! Shared boundary arcs for categorical terrain fields.
//!
//! Every grid edge separating two labels is stored once, stitched into a
//! maximal arc, simplified once, then referenced in opposite directions by the
//! adjacent faces. This keeps shade bands gap-free after simplification.

use std::collections::BTreeMap;

pub(super) type Point = (i32, i32);
type LabelPair = (i8, i8);

const PENALTY_TOLERANCE: f64 = 1.5;

const VERTICAL: usize = 0;
const HORIZONTAL: usize = 1;

/// Dense edge index over the vertex grid: two slots per vertex, one for the
/// edge running to `(x, y + 1)` and one for the edge running to `(x + 1, y)`.
/// Slot order (vertex x-major, vertical before horizontal) equals the sorted
/// canonical `(start, end)` order of the edge-keyed maps it replaced, so arc
/// seeding — and therefore output bytes — are unchanged.
struct EdgeGrid {
    origin: i32,
    vertex_width: usize,
    vertex_height: usize,
    output_scale: i32,
    info: Vec<Option<EdgeInfo>>,
}

impl EdgeGrid {
    fn vertex_index(&self, point: Point) -> Option<usize> {
        let x = point.0 - self.origin;
        let y = point.1 - self.origin;
        if x < 0 || y < 0 || x >= self.vertex_width as i32 || y >= self.vertex_height as i32 {
            return None;
        }
        Some(x as usize * self.vertex_height + y as usize)
    }

    fn edge_id(&self, vertex: Point, kind: usize) -> Option<usize> {
        Some(self.vertex_index(vertex)? * 2 + kind)
    }

    fn endpoints(&self, id: usize) -> (Point, Point) {
        let vertex = id / 2;
        let x = (vertex / self.vertex_height) as i32 + self.origin;
        let y = (vertex % self.vertex_height) as i32 + self.origin;
        if id % 2 == VERTICAL {
            ((x, y), (x, y + 1))
        } else {
            ((x, y), (x + 1, y))
        }
    }

    /// Edge between two grid-adjacent points and whether `a -> b` runs in the
    /// canonical (start -> end) direction.
    fn edge_between(&self, a: Point, b: Point) -> (usize, bool) {
        let (vertex, kind, forward) = if a.0 == b.0 {
            if a.1 < b.1 {
                (a, VERTICAL, true)
            } else {
                (b, VERTICAL, false)
            }
        } else {
            debug_assert_eq!(a.1, b.1, "points are not grid-adjacent");
            if a.0 < b.0 {
                (a, HORIZONTAL, true)
            } else {
                (b, HORIZONTAL, false)
            }
        };
        (
            self.edge_id(vertex, kind).expect("edge endpoints in grid"),
            forward,
        )
    }

    fn incident_ids(&self, point: Point) -> [Option<usize>; 4] {
        [
            self.edge_id((point.0, point.1 - 1), VERTICAL),
            self.edge_id(point, VERTICAL),
            self.edge_id((point.0 - 1, point.1), HORIZONTAL),
            self.edge_id(point, HORIZONTAL),
        ]
    }

    /// Incident edges separating exactly this label pair.
    fn pair_edges(&self, point: Point, pair: LabelPair) -> impl Iterator<Item = usize> + '_ {
        self.incident_ids(point)
            .into_iter()
            .flatten()
            .filter(move |&id| self.info[id].is_some_and(|info| info.labels == pair))
    }

    fn set(&mut self, vertex: Point, kind: usize, right: i8, left: i8, crossing: Option<Point>) {
        let id = self.edge_id(vertex, kind).expect("edge endpoints in grid");
        let labels = if right < left {
            (right, left)
        } else {
            (left, right)
        };
        debug_assert!(self.info[id].is_none());
        self.info[id] = Some(EdgeInfo {
            labels,
            right,
            left,
            crossing,
        });
    }
}

#[derive(Clone, Copy, Debug)]
struct EdgeInfo {
    labels: LabelPair,
    /// Label on the right while traversing `EdgeKey::start -> EdgeKey::end`.
    right: i8,
    left: i8,
    /// Continuous threshold crossing in output coordinates. `None` retains the
    /// categorical grid boundary used by callers without a scalar field.
    crossing: Option<Point>,
}

#[derive(Debug)]
struct SharedArc {
    points: Vec<Point>,
    topology_start: Point,
    topology_end: Point,
    shared_render_endpoints: bool,
    right: i8,
    left: i8,
    start_direction: i32,
    end_direction: i32,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DirectedArc {
    id: usize,
    reversed: bool,
}

/// Traces categorical topology from `labels`, then places each shared boundary
/// on the linearly interpolated crossing of a continuous scalar field.
/// Connectivity still comes from `labels`, so speckle merging and shared-face
/// guarantees are unchanged; only geometry gains sub-cell detail.
pub(super) fn trace_interpolated_shared_rings(
    labels: &[i8],
    tones: &[f32],
    width: usize,
    height: usize,
    origin: i32,
    tile_size: i32,
    output_scale: i32,
) -> BTreeMap<i8, Vec<Vec<Point>>> {
    assert_eq!(labels.len(), tones.len());
    assert!(output_scale > 0);
    trace_rings(
        labels,
        Some(tones),
        width,
        height,
        origin,
        tile_size,
        output_scale,
    )
}

fn trace_rings(
    labels: &[i8],
    tones: Option<&[f32]>,
    width: usize,
    height: usize,
    origin: i32,
    tile_size: i32,
    output_scale: i32,
) -> BTreeMap<i8, Vec<Vec<Point>>> {
    let graph_started = std::time::Instant::now();
    let graph = boundary_graph(labels, tones, width, height, origin, output_scale);
    let graph_elapsed = graph_started.elapsed();
    let arcs_started = std::time::Instant::now();
    let arcs = build_arcs(&graph, tile_size);
    let arcs_elapsed = arcs_started.elapsed();
    let rings_started = std::time::Instant::now();
    let rings = assemble_rings(&arcs);
    tracing::debug!(
        graph_ms = graph_elapsed.as_millis() as u64,
        arcs_ms = arcs_elapsed.as_millis() as u64,
        rings_ms = rings_started.elapsed().as_millis() as u64,
        "trace stages"
    );
    rings
}

fn boundary_graph(
    labels: &[i8],
    tones: Option<&[f32]>,
    width: usize,
    height: usize,
    origin: i32,
    output_scale: i32,
) -> EdgeGrid {
    let label_at = |x: isize, y: isize| -> i8 {
        if x < 0 || y < 0 || x >= width as isize || y >= height as isize {
            0
        } else {
            labels[y as usize * width + x as usize]
        }
    };
    let mut grid = EdgeGrid {
        origin,
        vertex_width: width + 1,
        vertex_height: height + 1,
        output_scale,
        info: vec![None; (width + 1) * (height + 1) * 2],
    };

    let tone_at = |x: isize, y: isize| -> Option<f64> {
        if x < 0 || y < 0 || x >= width as isize || y >= height as isize {
            return None;
        }
        let value = f64::from(tones?[y as usize * width + x as usize]);
        value.is_finite().then_some(value)
    };

    // Canonical vertical direction is top -> bottom; its right side is west.
    for y in 0..height as i32 {
        for x in 0..=width as i32 {
            let west = label_at(x as isize - 1, y as isize);
            let east = label_at(x as isize, y as isize);
            if west != east {
                let vertex = (origin + x, origin + y);
                let crossing = tones.map(|_| {
                    edge_crossing(
                        vertex,
                        VERTICAL,
                        west,
                        east,
                        tone_at(x as isize - 1, y as isize),
                        tone_at(x as isize, y as isize),
                        output_scale,
                    )
                });
                grid.set((origin + x, origin + y), VERTICAL, west, east, crossing);
            }
        }
    }

    // Canonical horizontal direction is left -> right; its right side is south.
    for y in 0..=height as i32 {
        for x in 0..width as i32 {
            let north = label_at(x as isize, y as isize - 1);
            let south = label_at(x as isize, y as isize);
            if north != south {
                let vertex = (origin + x, origin + y);
                let crossing = tones.map(|_| {
                    edge_crossing(
                        vertex,
                        HORIZONTAL,
                        north,
                        south,
                        tone_at(x as isize, y as isize - 1),
                        tone_at(x as isize, y as isize),
                        output_scale,
                    )
                });
                grid.set((origin + x, origin + y), HORIZONTAL, south, north, crossing);
            }
        }
    }
    grid
}

/// Crossing point for an edge: the tone-interpolated position when both tones
/// are finite, falling back to the geometric edge midpoint otherwise.
fn edge_crossing(
    vertex: Point,
    kind: usize,
    first_label: i8,
    second_label: i8,
    first_tone: Option<f64>,
    second_tone: Option<f64>,
    output_scale: i32,
) -> Point {
    interpolated_crossing(
        vertex,
        kind,
        first_label,
        second_label,
        first_tone,
        second_tone,
        output_scale,
    )
    .unwrap_or_else(|| edge_midpoint(vertex, kind, output_scale))
}

fn interpolated_crossing(
    vertex: Point,
    kind: usize,
    first_label: i8,
    second_label: i8,
    first_tone: Option<f64>,
    second_tone: Option<f64>,
    output_scale: i32,
) -> Option<Point> {
    let (first_tone, second_tone) = (first_tone?, second_tone?);
    let threshold =
        if first_label.signum() != second_label.signum() && first_label != 0 && second_label != 0 {
            0.0
        } else {
            (f64::from(first_label) + f64::from(second_label)) * 0.5
        };
    let denominator = second_tone - first_tone;
    let fraction = if denominator.abs() <= f64::EPSILON
        || (threshold - first_tone) * (threshold - second_tone) > 0.0
    {
        0.5
    } else {
        ((threshold - first_tone) / denominator).clamp(0.0, 1.0)
    };
    let scale = f64::from(output_scale);
    let x = f64::from(vertex.0);
    let y = f64::from(vertex.1);
    let point = if kind == VERTICAL {
        ((x - 0.5 + fraction) * scale, (y + 0.5) * scale)
    } else {
        ((x + 0.5) * scale, (y - 0.5 + fraction) * scale)
    };
    Some((point.0.round() as i32, point.1.round() as i32))
}

fn edge_midpoint(vertex: Point, kind: usize, output_scale: i32) -> Point {
    let scale = f64::from(output_scale);
    let x = f64::from(vertex.0);
    let y = f64::from(vertex.1);
    let point = if kind == VERTICAL {
        (x * scale, (y + 0.5) * scale)
    } else {
        ((x + 0.5) * scale, y * scale)
    };
    (point.0.round() as i32, point.1.round() as i32)
}

fn build_arcs(grid: &EdgeGrid, tile_size: i32) -> Vec<SharedArc> {
    let mut remaining = grid.info.iter().map(Option::is_some).collect::<Vec<_>>();
    let mut arcs = Vec::new();
    // `graph_has_crossings` reads only the immutable `grid`, so its full-grid
    // scan is loop-invariant — compute it once instead of per seed arc.
    let interpolated = graph_has_crossings(grid);
    // Dense ids ascend in the same order the map-based `remaining.first()`
    // yielded, and extending an arc only consumes edges at or after the
    // smallest remaining one, so a single forward scan seeds identically.
    for seed in 0..remaining.len() {
        if !remaining[seed] {
            continue;
        }
        remaining[seed] = false;
        let pair = grid.info[seed].expect("remaining edge has info").labels;
        let (seed_start, seed_end) = grid.endpoints(seed);
        let mut points = vec![seed_start, seed_end];
        let closed = extend_arc(&mut points, false, pair, &mut remaining, grid, tile_size);
        if !closed {
            extend_arc(&mut points, true, pair, &mut remaining, grid, tile_size);
        }

        let (first_edge, forward) = grid.edge_between(points[0], points[1]);
        let first_info = grid.info[first_edge].expect("arc edge has info");
        let (right, left) = if forward {
            (first_info.right, first_info.left)
        } else {
            (first_info.left, first_info.right)
        };
        debug_assert!(points.windows(2).all(|segment| {
            let (edge, forward) = grid.edge_between(segment[0], segment[1]);
            let info = grid.info[edge].expect("arc edge has info");
            let edge_right = if forward { info.right } else { info.left };
            edge_right == right
        }));

        let topology_start = points[0];
        let topology_end = *points.last().expect("arc has endpoints");
        let raw_start_direction = direction(points[0], points[1]);
        let raw_end_direction = direction(points[points.len() - 2], points[points.len() - 1]);
        let shared_render_endpoints = !interpolated || topology_start != topology_end;
        let points = if interpolated {
            interpolated_arc_points(&points, grid, right, left)
        } else {
            simplify_arc(&points, right, left)
        };
        arcs.push(SharedArc {
            points,
            topology_start,
            topology_end,
            shared_render_endpoints,
            right,
            left,
            start_direction: raw_start_direction,
            end_direction: raw_end_direction,
        });
    }
    arcs
}

fn graph_has_crossings(grid: &EdgeGrid) -> bool {
    grid.info
        .iter()
        .flatten()
        .any(|info| info.crossing.is_some())
}

fn interpolated_arc_points(
    topology_points: &[Point],
    grid: &EdgeGrid,
    right: i8,
    left: i8,
) -> Vec<Point> {
    let closed = topology_points.first() == topology_points.last();
    let mut points = topology_points
        .windows(2)
        .map(|segment| {
            let (edge, _) = grid.edge_between(segment[0], segment[1]);
            grid.info[edge]
                .and_then(|info| info.crossing)
                .expect("interpolated graph edge has a crossing")
        })
        .collect::<Vec<_>>();
    if closed && !points.is_empty() {
        points.push(points[0]);
    } else if !closed {
        let scale = grid.output_scale;
        points.insert(
            0,
            (topology_points[0].0 * scale, topology_points[0].1 * scale),
        );
        let end = *topology_points.last().expect("arc has endpoints");
        points.push((end.0 * scale, end.1 * scale));
    }
    simplify_interpolated_arc(&points, right, left, grid.output_scale)
}

/// Extends one end of an arc until a junction, tile boundary, or loop closure.
/// Walking both ends is necessary because the ordered seed edge may lie in the
/// middle of an otherwise open chain.
fn extend_arc(
    points: &mut Vec<Point>,
    prepend: bool,
    pair: LabelPair,
    remaining: &mut [bool],
    grid: &EdgeGrid,
    tile_size: i32,
) -> bool {
    let mut prefix = Vec::new();
    loop {
        let current = if prepend {
            prefix.last().copied().unwrap_or(points[0])
        } else {
            *points.last().expect("arc has endpoints")
        };
        if is_arc_endpoint(current, pair, grid, tile_size) {
            finish_prefix(points, prefix);
            return false;
        }
        let mut sole_candidate = None;
        for edge in grid.pair_edges(current, pair) {
            if !remaining[edge] {
                continue;
            }
            if sole_candidate.replace(edge).is_some() {
                sole_candidate = None;
                break;
            }
        }
        let Some(edge) = sole_candidate else {
            finish_prefix(points, prefix);
            return false;
        };
        remaining[edge] = false;
        let (start, end) = grid.endpoints(edge);
        let next = if start == current { end } else { start };
        if prepend {
            prefix.push(next);
        } else {
            points.push(next);
        }
        if !prepend && points.first() == points.last() {
            return true;
        }
    }
}

fn finish_prefix(points: &mut Vec<Point>, mut prefix: Vec<Point>) {
    prefix.reverse();
    prefix.append(points);
    *points = prefix;
}

fn is_arc_endpoint(point: Point, pair: LabelPair, grid: &EdgeGrid, tile_size: i32) -> bool {
    if is_tile_anchor(point, pair, grid, tile_size) {
        return true;
    }
    grid.pair_edges(point, pair).count() != 2
}

fn is_tile_anchor(point: Point, pair: LabelPair, grid: &EdgeGrid, tile_size: i32) -> bool {
    let on_x = point.0 == 0 || point.0 == tile_size;
    let on_y = point.1 == 0 || point.1 == tile_size;
    if on_x && on_y {
        return true;
    }
    if on_x {
        return grid
            .pair_edges(point, pair)
            .any(|edge| edge % 2 == HORIZONTAL);
    }
    if on_y {
        return grid
            .pair_edges(point, pair)
            .any(|edge| edge % 2 == VERTICAL);
    }
    false
}

fn assemble_rings(arcs: &[SharedArc]) -> BTreeMap<i8, Vec<Vec<Point>>> {
    let mut by_label: BTreeMap<i8, Vec<DirectedArc>> = BTreeMap::new();
    for (id, arc) in arcs.iter().enumerate() {
        for (label, reversed) in [(arc.right, false), (arc.left, true)] {
            if label == 0 {
                continue;
            }
            by_label
                .entry(label)
                .or_default()
                .push(DirectedArc { id, reversed });
        }
    }
    if by_label.is_empty() {
        return BTreeMap::new();
    }

    // Arc endpoints live on a small integer grid, so a flat slot array with
    // O(1) point lookup (reused across labels, touched slots cleared after
    // each) replaces the point-keyed BTreeMap that dominated assembly time.
    // Start points are visited in Point order and candidate lists stay sorted,
    // preserving the exact ring order of the map-based implementation.
    let mut min_x = i32::MAX;
    let mut min_y = i32::MAX;
    let mut max_x = i32::MIN;
    let mut max_y = i32::MIN;
    for arc in arcs {
        for point in [
            arc.points[0],
            *arc.points.last().expect("arc has endpoints"),
        ] {
            min_x = min_x.min(point.0);
            max_x = max_x.max(point.0);
            min_y = min_y.min(point.1);
            max_y = max_y.max(point.1);
        }
    }
    let slot_height = (max_y - min_y + 1) as usize;
    let slot_count = (max_x - min_x + 1) as usize * slot_height;
    let slot_index = |point: Point| -> usize {
        (point.0 - min_x) as usize * slot_height + (point.1 - min_y) as usize
    };
    let mut slots: Vec<Vec<DirectedArc>> = vec![Vec::new(); slot_count];

    let mut result = BTreeMap::new();
    for (label, directed_arcs) in by_label {
        let mut touched = Vec::with_capacity(directed_arcs.len());
        let mut starts = Vec::with_capacity(directed_arcs.len());
        for directed in directed_arcs {
            let start = directed_start(&arcs[directed.id], directed);
            let index = slot_index(start);
            if slots[index].is_empty() {
                touched.push(index);
                starts.push(start);
            }
            slots[index].push(directed);
        }
        for &index in &touched {
            slots[index].sort_unstable();
        }
        starts.sort_unstable();

        let mut rings = Vec::new();
        for start in starts {
            loop {
                let slot = &mut slots[slot_index(start)];
                if slot.is_empty() {
                    break;
                }
                let first = slot.remove(0);
                let first_arc = &arcs[first.id];
                let mut ring = directed_points(first_arc, first);
                let mut current = directed_end(first_arc, first);
                let mut incoming = directed_end_direction(first_arc, first);
                let mut closed = current == start;

                for _ in 0..=arcs.len() {
                    if closed {
                        break;
                    }
                    let Some(next) =
                        take_turning_arc(&mut slots[slot_index(current)], incoming, arcs)
                    else {
                        break;
                    };
                    let arc = &arcs[next.id];
                    let points = directed_points(arc, next);
                    if arc.shared_render_endpoints {
                        ring.extend(points.into_iter().skip(1));
                    } else {
                        ring.extend(points);
                    }
                    current = directed_end(arc, next);
                    incoming = directed_end_direction(arc, next);
                    closed = current == start;
                }
                if closed {
                    if ring.first() != ring.last() {
                        ring.push(ring[0]);
                    }
                    if ring.len() >= 4 {
                        rings.push(ring);
                    }
                }
            }
        }
        for &index in &touched {
            slots[index].clear();
        }
        if !rings.is_empty() {
            result.insert(label, rings);
        }
    }
    result
}

fn take_turning_arc(
    candidates: &mut Vec<DirectedArc>,
    incoming: i32,
    arcs: &[SharedArc],
) -> Option<DirectedArc> {
    let (index, _) = candidates.iter().enumerate().min_by_key(|(_, directed)| {
        let outgoing = directed_start_direction(&arcs[directed.id], **directed);
        let turn = (outgoing - incoming).rem_euclid(4);
        let rank = match turn {
            1 => 0,
            0 => 1,
            3 => 2,
            _ => 3,
        };
        (rank, **directed)
    })?;
    Some(candidates.remove(index))
}

fn directed_points(arc: &SharedArc, directed: DirectedArc) -> Vec<Point> {
    if directed.reversed {
        arc.points.iter().rev().copied().collect()
    } else {
        arc.points.clone()
    }
}

fn directed_start(arc: &SharedArc, directed: DirectedArc) -> Point {
    if directed.reversed {
        arc.topology_end
    } else {
        arc.topology_start
    }
}

fn directed_end(arc: &SharedArc, directed: DirectedArc) -> Point {
    if directed.reversed {
        arc.topology_start
    } else {
        arc.topology_end
    }
}

fn directed_start_direction(arc: &SharedArc, directed: DirectedArc) -> i32 {
    if directed.reversed {
        (arc.end_direction + 2).rem_euclid(4)
    } else {
        arc.start_direction
    }
}

fn directed_end_direction(arc: &SharedArc, directed: DirectedArc) -> i32 {
    if directed.reversed {
        (arc.start_direction + 2).rem_euclid(4)
    } else {
        arc.end_direction
    }
}

/// VTracer-inspired polygon mode: combine straight runs, remove one-pixel
/// staircases by signed area, then greedily replace low-penalty subpaths.
/// Adapted from visioncortex's MIT/Apache-2.0 `PathSimplify` implementation.
fn simplify_arc(points: &[Point], right: i8, left: i8) -> Vec<Point> {
    if points.len() <= 2 {
        return points.to_vec();
    }
    let closed = points.first() == points.last();
    let mut simplified = remove_collinear(points, closed);
    let clockwise = if closed {
        signed_area(&simplified) > 0
    } else {
        right > left
    };
    simplified = remove_staircases(&simplified, closed, clockwise);
    simplified = limit_penalties(&simplified, PENALTY_TOLERANCE);
    let minimum = if closed { 4 } else { 2 };
    if simplified.len() < minimum {
        points.to_vec()
    } else {
        simplified
    }
}

fn simplify_interpolated_arc(
    points: &[Point],
    _right: i8,
    _left: i8,
    output_scale: i32,
) -> Vec<Point> {
    if points.len() <= 2 {
        return points.to_vec();
    }
    let closed = points.first() == points.last();
    let mut simplified = remove_collinear(points, closed);
    // `evaluate_penalty` scales cubically with coordinates. Preserve the
    // categorical simplifier's physical tolerance after moving to fixed-point
    // sub-cell coordinates; unlike grid paths, crossings have no staircases to
    // remove first.
    let scale = f64::from(output_scale);
    simplified = limit_penalties(&simplified, PENALTY_TOLERANCE * scale.powi(3));
    let minimum = if closed { 4 } else { 1 };
    if simplified.len() < minimum {
        points.to_vec()
    } else {
        simplified
    }
}

fn remove_collinear(points: &[Point], closed: bool) -> Vec<Point> {
    let mut points = points.to_vec();
    if closed && points.first() == points.last() {
        points.pop();
    }
    loop {
        if points.len() <= if closed { 3 } else { 2 } {
            break;
        }
        let mut keep = vec![true; points.len()];
        let range = if closed {
            0..points.len()
        } else {
            1..points.len() - 1
        };
        for index in range {
            let previous = if index == 0 {
                points.len() - 1
            } else {
                index - 1
            };
            let next = (index + 1) % points.len();
            if triangle_area(points[previous], points[index], points[next]) == 0 {
                keep[index] = false;
            }
        }
        if keep.iter().all(|keep| *keep) {
            break;
        }
        let mut index = 0;
        points.retain(|_| {
            let keep = keep[index];
            index += 1;
            keep
        });
    }
    if closed && !points.is_empty() {
        points.push(points[0]);
    }
    points
}

fn remove_staircases(points: &[Point], closed: bool, clockwise: bool) -> Vec<Point> {
    let mut path = points;
    if closed && points.first() == points.last() {
        path = &points[..points.len() - 1];
    }
    if path.len() <= 2 {
        return points.to_vec();
    }
    let mut result = Vec::with_capacity(points.len());
    for index in 0..path.len() {
        let previous = if index == 0 {
            path.len() - 1
        } else {
            index - 1
        };
        let next = (index + 1) % path.len();
        let endpoint = !closed && (index == 0 || index + 1 == path.len());
        let unit_stair =
            manhattan(path[index], path[previous]) == 1 || manhattan(path[index], path[next]) == 1;
        let area = triangle_area(path[previous], path[index], path[next]);
        if endpoint || !unit_stair || (area != 0 && (area > 0) == clockwise) {
            result.push(path[index]);
        }
    }
    if closed && !result.is_empty() {
        result.push(result[0]);
    }
    result
}

fn limit_penalties(points: &[Point], tolerance: f64) -> Vec<Point> {
    if points.len() <= 2 {
        return points.to_vec();
    }
    let mut result = vec![points[0]];
    let mut last = 0;
    for index in 1..points.len() {
        if index == last + 1 {
            if index + 1 == points.len() {
                result.push(points[index]);
            }
            continue;
        }
        let penalty = (last + 1..index)
            .map(|between| evaluate_penalty(points[last], points[between], points[index]))
            .fold(0.0_f64, f64::max);
        if penalty >= tolerance {
            last = index - 1;
            result.push(points[last]);
        }
        if index + 1 == points.len() {
            result.push(points[index]);
        }
    }
    result.dedup();
    result
}

fn evaluate_penalty(a: Point, b: Point, c: Point) -> f64 {
    let base = squared_distance(a, c).sqrt();
    if base == 0.0 {
        return f64::INFINITY;
    }
    let area2 = triangle_area(a, b, c).unsigned_abs() as f64;
    // Heron's `area² / base` from VTracer, expressed through the cross product.
    area2 * area2 / (4.0 * base)
}

fn squared_distance(a: Point, b: Point) -> f64 {
    let dx = f64::from(a.0 - b.0);
    let dy = f64::from(a.1 - b.1);
    dx * dx + dy * dy
}

fn manhattan(a: Point, b: Point) -> i32 {
    (a.0 - b.0).abs() + (a.1 - b.1).abs()
}

fn triangle_area(a: Point, b: Point, c: Point) -> i64 {
    i64::from(b.0 - a.0) * i64::from(c.1 - a.1) - i64::from(c.0 - a.0) * i64::from(b.1 - a.1)
}

fn signed_area(points: &[Point]) -> i64 {
    points
        .iter()
        .zip(points.iter().cycle().skip(1))
        .take(points.len())
        .map(|(a, b)| i64::from(a.0) * i64::from(b.1) - i64::from(a.1) * i64::from(b.0))
        .sum()
}

fn direction(from: Point, to: Point) -> i32 {
    match ((to.0 - from.0).signum(), (to.1 - from.1).signum()) {
        (1, 0) => 0,
        (0, 1) => 1,
        (-1, 0) => 2,
        (0, -1) => 3,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staircase_is_reduced_to_a_shared_diagonal() {
        let points = vec![(0, 0), (1, 0), (1, 1), (2, 1), (2, 2), (3, 2)];
        let simplified = simplify_arc(&points, 2, 1);
        assert!(simplified.len() < points.len());
        assert_eq!(simplified.first(), points.first());
        assert_eq!(simplified.last(), points.last());
    }

    #[test]
    fn shared_boundary_is_stored_once() {
        let labels = [1, 2, 1, 2];
        let graph = boundary_graph(&labels, None, 2, 2, 0, 1);
        let arcs = build_arcs(&graph, 2);
        let shared = arcs
            .iter()
            .filter(|arc| (arc.left, arc.right) == (1, 2) || (arc.left, arc.right) == (2, 1))
            .count();
        assert_eq!(shared, 1);
    }

    #[test]
    fn tile_boundary_points_split_arcs() {
        let labels = [1, 1, 2, 2];
        let graph = boundary_graph(&labels, None, 2, 2, -1, 1);
        let arcs = build_arcs(&graph, 1);
        assert!(arcs.iter().all(|arc| {
            let interior = &arc.points[1..arc.points.len().saturating_sub(1)];
            interior
                .iter()
                .all(|point| point.0 != 0 && point.1 != 0 && point.0 != 1 && point.1 != 1)
        }));
    }

    #[test]
    fn continuous_tone_places_a_shared_boundary_at_its_threshold() {
        let labels = [-1, 1];
        let tones = [-0.25, 0.75];
        let graph = boundary_graph(&labels, Some(&tones), 2, 1, 0, 4);
        let edge = graph.edge_id((1, 0), VERTICAL).unwrap();

        // The zero crossing lies 1/4 of the way from the west center (x=0.5)
        // to the east center (x=1.5): x=0.75, y=0.5 in grid coordinates.
        assert_eq!(graph.info[edge].unwrap().crossing, Some((3, 2)));
    }
}
