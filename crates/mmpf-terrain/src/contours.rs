//! Multi-level Marching Squares contour generation.
//!
//! Goes beyond maplibre-contour in three ways:
//! - the elevation grid is pre-smoothed with a binomial `[1 2 1]²` filter, so
//!   pixel-noise wiggles disappear in the *field* (linear slopes are preserved
//!   exactly, so contour positions on uniform terrain do not move);
//! - saddle cells (Marching Squares cases 5/10) are disambiguated with the
//!   asymptotic decider — the sign of the bilinear surface's saddle value —
//!   instead of d3-contour's fixed choice, so connections follow the
//!   interpolated surface's actual topology;
//! - finished polylines are simplified with Visvalingam–Whyatt. Vertices within
//!   one source pixel of the tile edge are pinned, and the grid (hence the raw
//!   polyline) is identical across neighboring tiles, so seams stay exact.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use fast_mvt::{
    DEFAULT_EXTENT, MvtCoord, MvtFeature, MvtGeometry, MvtLayer, MvtLineString, MvtMultiLineString,
    MvtTile,
};

use super::dem::DemNeighborhood;

/// Visvalingam–Whyatt threshold as *twice* the triangle area (the integer
/// cross product), i.e. an area of 12 squared MVT units. That removes
/// deviations under ≈1.5 units — about a fifth of a source pixel, invisible at
/// render time.
const SIMPLIFY_MIN_CROSS: u64 = 64;

type EdgePoint = (i32, i32);
type Segment = ([i32; 2], [i32; 2]);

#[derive(Debug)]
struct Fragment {
    start: EdgePoint,
    end: EdgePoint,
    points: Vec<MvtCoord>,
}

impl Fragment {
    fn new(start: EdgePoint, end: EdgePoint) -> Self {
        Self {
            start,
            end,
            points: Vec::new(),
        }
    }
}

/// Elevation intervals modeled after maplibre-contour's documented profile.
/// The first value is the generated interval; later values classify major lines.
pub fn levels_for_zoom(zoom: u8) -> &'static [i32] {
    // maplibre-contour-style [minor, major] profile; each entry applies at its
    // zoom and up to the next. Major (index/label) intervals stay values that
    // actually occur (500/200/100/50), unlike the old 2500 m at low zoom.
    match zoom {
        0..=10 => &[250, 500],
        11 => &[100, 500],
        12 => &[50, 200],
        13 => &[20, 100],
        _ => &[10, 50],
    }
}

pub fn generate(neighborhood: &DemNeighborhood, zoom: u8) -> Result<Vec<u8>> {
    let levels = levels_for_zoom(zoom);
    let grid = ElevationGrid::smoothed(neighborhood);
    let isolines = marching_squares(&grid, levels[0]);
    // Pin vertices within one source pixel of the tile edge so neighboring
    // tiles (which compute identical raw polylines there) stay seam-exact.
    let pin_margin = i64::from(DEFAULT_EXTENT.get()) / neighborhood.width() as i64;
    let extent = i64::from(DEFAULT_EXTENT.get());
    let mut layer = MvtLayer::new("contours", DEFAULT_EXTENT);

    for (elevation, lines) in isolines {
        let lines = lines
            .into_iter()
            .filter_map(|mut line| {
                line.dedup();
                simplify_line(&mut line, |point| {
                    let (x, y) = (i64::from(point.x), i64::from(point.y));
                    x <= pin_margin
                        || y <= pin_margin
                        || x >= extent - pin_margin
                        || y >= extent - pin_margin
                });
                (line.len() >= 2).then_some(line)
            })
            .map(MvtLineString::new)
            .collect::<Vec<_>>();
        if lines.is_empty() {
            continue;
        }
        let mut feature =
            MvtFeature::new(MvtGeometry::MultiLineString(MvtMultiLineString::new(lines)));
        feature.add_tag_int("ele", i64::from(elevation));
        let level = levels
            .iter()
            .enumerate()
            .filter_map(|(index, interval)| (elevation % interval == 0).then_some(index))
            .max()
            .unwrap_or_default();
        feature.add_tag_uint("level", level as u64);
        layer.add_feature(feature);
    }

    let mut tile = MvtTile::new();
    tile.add_layer(layer);
    tile.encode().context("encode contour MVT")
}

/// Grid-corner elevations for the buffered tile, materialized once (each
/// corner is shared by four cells) and pre-smoothed with a separable binomial
/// `[1 2 1]` filter. The filter preserves linear fields exactly, so contours on
/// uniform slopes keep their positions; only pixel noise is rounded away. All
/// samples come from absolute source pixels shared with neighboring tiles, so
/// the smoothed grid is seam-consistent.
struct ElevationGrid {
    values: Vec<f32>,
    /// Grid points span `-2..=tile + 2`; `stride = tile + 5`.
    stride: i32,
    tile: i32,
}

impl ElevationGrid {
    fn smoothed(neighborhood: &DemNeighborhood) -> Self {
        let tile = neighborhood.width() as i32;
        let stride = tile + 5;
        let raw: Vec<f32> = (0..stride * stride)
            .map(|index| {
                let x = index % stride - 2;
                let y = index / stride - 2;
                neighborhood.grid_elevation(x, y)
            })
            .collect();
        let horizontal = binomial_pass(&raw, stride, 1);
        let values = binomial_pass(&horizontal, stride, stride);
        Self {
            values,
            stride,
            tile,
        }
    }

    /// Reads a smoothed grid-corner elevation; `x`/`y` in `-1..=tile + 1`.
    fn get(&self, x: i32, y: i32) -> f32 {
        self.values[((y + 2) * self.stride + x + 2) as usize]
    }
}

/// One `[1 2 1]` pass along `step` (1 = rows, stride = columns), averaging the
/// finite samples so nodata does not spread.
fn binomial_pass(values: &[f32], stride: i32, step: i32) -> Vec<f32> {
    let len = values.len() as i32;
    (0..len)
        .map(|index| {
            let lane = if step == 1 {
                index % stride
            } else {
                index / stride
            };
            let mut sum = 0.0_f32;
            let mut weight = 0.0_f32;
            for (offset, w) in [(-1_i32, 1.0_f32), (0, 2.0), (1, 1.0)] {
                let neighbor_lane = lane + offset;
                if neighbor_lane < 0 || neighbor_lane >= stride {
                    continue;
                }
                let value = values[(index + offset * step) as usize];
                if value.is_finite() {
                    sum += value * w;
                    weight += w;
                }
            }
            if weight == 0.0 {
                f32::NAN
            } else {
                sum / weight
            }
        })
        .collect()
}

fn marching_squares(grid: &ElevationGrid, interval: i32) -> BTreeMap<i32, Vec<Vec<MvtCoord>>> {
    let width = grid.tile;
    let height = grid.tile;
    let scale = f64::from(DEFAULT_EXTENT.get()) / f64::from(width);
    let mut completed: BTreeMap<i32, Vec<Vec<MvtCoord>>> = BTreeMap::new();
    let mut starts: BTreeMap<i32, BTreeMap<EdgePoint, Fragment>> = BTreeMap::new();
    let mut ends: BTreeMap<i32, BTreeMap<EdgePoint, EdgePoint>> = BTreeMap::new();

    // One source-pixel buffer produces coordinates just outside [0, extent],
    // allowing MapLibre line joins to remain continuous across tile seams.
    for y in -1..=height {
        for x in -1..=width {
            let tl = grid.get(x, y);
            let tr = grid.get(x + 1, y);
            let br = grid.get(x + 1, y + 1);
            let bl = grid.get(x, y + 1);
            if ![tl, tr, br, bl].into_iter().all(f32::is_finite) {
                continue;
            }
            let minimum = tl.min(tr).min(br).min(bl);
            let maximum = tl.max(tr).max(br).max(bl);
            let start = (minimum / interval as f32).ceil() as i32 * interval;
            let end = (maximum / interval as f32).floor() as i32 * interval;

            for threshold in (start..=end).step_by(interval as usize) {
                let case = ((tl > threshold as f32) as usize) << 3
                    | ((tr > threshold as f32) as usize) << 2
                    | ((br > threshold as f32) as usize) << 1
                    | (bl > threshold as f32) as usize;
                let saddle = saddle_above([tl, tr, br, bl], threshold as f32);
                for &(segment_start, segment_end) in segments_for(case, saddle) {
                    stitch_segment(
                        threshold,
                        edge_key(x, y, segment_start),
                        edge_key(x, y, segment_end),
                        interpolate(
                            x,
                            y,
                            segment_start,
                            threshold as f32,
                            [tl, tr, br, bl],
                            scale,
                        ),
                        interpolate(x, y, segment_end, threshold as f32, [tl, tr, br, bl], scale),
                        &mut starts,
                        &mut ends,
                        &mut completed,
                    );
                }
            }
        }
    }

    for (level, fragments) in starts {
        let lines = completed.entry(level).or_default();
        lines.extend(
            fragments
                .into_values()
                .filter(|fragment| fragment.points.len() >= 2)
                .map(|fragment| fragment.points),
        );
    }
    completed
}

#[allow(clippy::too_many_arguments)]
fn stitch_segment(
    level: i32,
    start_key: EdgePoint,
    end_key: EdgePoint,
    start_point: MvtCoord,
    end_point: MvtCoord,
    starts_by_level: &mut BTreeMap<i32, BTreeMap<EdgePoint, Fragment>>,
    ends_by_level: &mut BTreeMap<i32, BTreeMap<EdgePoint, EdgePoint>>,
    completed: &mut BTreeMap<i32, Vec<Vec<MvtCoord>>>,
) {
    let starts = starts_by_level.entry(level).or_default();
    let ends = ends_by_level.entry(level).or_default();

    if let Some(fragment_start) = ends.remove(&start_key) {
        let mut fragment = starts
            .remove(&fragment_start)
            .expect("end index must reference a fragment");
        if fragment.start == end_key {
            fragment.points.push(end_point);
            completed.entry(level).or_default().push(fragment.points);
            return;
        }
        if let Some(other) = starts.remove(&end_key) {
            ends.remove(&other.end);
            fragment.points.extend(other.points);
            fragment.end = other.end;
        } else {
            fragment.points.push(end_point);
            fragment.end = end_key;
        }
        ends.insert(fragment.end, fragment.start);
        starts.insert(fragment.start, fragment);
    } else if let Some(mut fragment) = starts.remove(&end_key) {
        fragment.points.insert(0, start_point);
        fragment.start = start_key;
        ends.insert(fragment.end, fragment.start);
        starts.insert(fragment.start, fragment);
    } else {
        let mut fragment = Fragment::new(start_key, end_key);
        fragment.points.push(start_point);
        fragment.points.push(end_point);
        ends.insert(end_key, start_key);
        starts.insert(start_key, fragment);
    }
}

fn edge_key(x: i32, y: i32, point: [i32; 2]) -> EdgePoint {
    (x * 2 + point[0], y * 2 + point[1])
}

fn interpolate(
    x: i32,
    y: i32,
    point: [i32; 2],
    threshold: f32,
    [tl, tr, br, bl]: [f32; 4],
    scale: f64,
) -> MvtCoord {
    let ratio = |a: f32, b: f32| f64::from((threshold - a) / (b - a));
    let (px, py) = if point[0] == 0 {
        (f64::from(x), f64::from(y) + ratio(tl, bl))
    } else if point[0] == 2 {
        (f64::from(x + 1), f64::from(y) + ratio(tr, br))
    } else if point[1] == 0 {
        (f64::from(x) + ratio(tl, tr), f64::from(y))
    } else {
        (f64::from(x) + ratio(bl, br), f64::from(y + 1))
    };
    MvtCoord {
        x: (px * scale).round() as i32,
        y: (py * scale).round() as i32,
    }
}

const A: [i32; 2] = [1, 0];
const B: [i32; 2] = [2, 1];
const C: [i32; 2] = [1, 2];
const D: [i32; 2] = [0, 1];

/// Segment table with saddle disambiguation. For the ambiguous cases (5, 10)
/// the asymptotic decider (see [`saddle_above`]) picks which diagonal pair
/// connects instead of d3-contour's fixed choice, so ridges and valleys keep
/// contours consistent with the bilinear surface.
fn segments_for(case: usize, saddle_above: bool) -> &'static [Segment] {
    match case {
        1 => &[(C, D)],
        2 => &[(B, C)],
        3 => &[(B, D)],
        4 => &[(A, B)],
        // tr/bl above: a saddle above the threshold joins them across the cell.
        5 if saddle_above => &[(A, D), (C, B)],
        5 => &[(C, D), (A, B)],
        6 => &[(A, C)],
        7 => &[(A, D)],
        8 => &[(D, A)],
        9 => &[(C, A)],
        // tl/br above: a saddle above the threshold joins them across the cell.
        10 if saddle_above => &[(B, A), (D, C)],
        10 => &[(D, A), (B, C)],
        11 => &[(B, A)],
        12 => &[(D, B)],
        13 => &[(C, B)],
        14 => &[(D, C)],
        _ => &[],
    }
}

/// Asymptotic decider for Marching Squares saddles: the bilinear interpolation
/// over the cell has a saddle point at `(a·c − b·d) / (a + c − b − d)` relative
/// to the threshold (`a..d` = threshold-relative corners tl, tr, br, bl). Its
/// sign says which diagonal pair the contour actually separates — the corner
/// mean is only an approximation and can pick the wrong topology. A degenerate
/// (flat) saddle falls back to d3-contour's fixed choice.
fn saddle_above([tl, tr, br, bl]: [f32; 4], threshold: f32) -> bool {
    let threshold = f64::from(threshold);
    let (a, b) = (f64::from(tl) - threshold, f64::from(tr) - threshold);
    let (c, d) = (f64::from(br) - threshold, f64::from(bl) - threshold);
    let denominator = a + c - b - d;
    denominator != 0.0 && (a * c - b * d) / denominator > 0.0
}

/// Visvalingam–Whyatt: repeatedly removes the vertex spanning the smallest
/// triangle until every remaining triangle reaches `SIMPLIFY_MIN_CROSS`.
/// Endpoints and `pinned` vertices (near tile edges) are never removed.
/// Heap-driven with lazy invalidation: O(n log n), all-integer areas, and the
/// (cross, index) ordering makes removal order — hence output — deterministic.
fn simplify_line(line: &mut Vec<MvtCoord>, pinned: impl Fn(MvtCoord) -> bool) {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let n = line.len();
    if n <= 2 {
        return;
    }
    let keep_always: Vec<bool> = line.iter().map(|&point| pinned(point)).collect();
    // Doubly-linked indices over the original vector.
    let mut previous: Vec<usize> = (0..n).map(|i| i.wrapping_sub(1)).collect();
    let mut next: Vec<usize> = (1..=n).collect();
    let mut alive = vec![true; n];
    // Entries are invalidated by bumping the vertex's stamp when a neighbor is
    // removed; stale heap entries are skipped on pop.
    let mut stamp = vec![0_u32; n];
    let cross = |a: MvtCoord, b: MvtCoord, c: MvtCoord| -> u64 {
        (i64::from(b.x - a.x) * i64::from(c.y - a.y) - i64::from(b.y - a.y) * i64::from(c.x - a.x))
            .unsigned_abs()
    };
    let mut heap: BinaryHeap<Reverse<(u64, usize, u32)>> = (1..n - 1)
        .filter(|&index| !keep_always[index])
        .map(|index| {
            Reverse((
                cross(line[index - 1], line[index], line[index + 1]),
                index,
                0,
            ))
        })
        .collect();

    while let Some(Reverse((area, index, version))) = heap.pop() {
        if !alive[index] || version != stamp[index] {
            continue;
        }
        if area >= SIMPLIFY_MIN_CROSS {
            break;
        }
        alive[index] = false;
        let (before, after) = (previous[index], next[index]);
        next[before] = after;
        previous[after] = before;
        for neighbor in [before, after] {
            if neighbor > 0 && neighbor < n - 1 && alive[neighbor] && !keep_always[neighbor] {
                stamp[neighbor] += 1;
                heap.push(Reverse((
                    cross(
                        line[previous[neighbor]],
                        line[neighbor],
                        line[next[neighbor]],
                    ),
                    neighbor,
                    stamp[neighbor],
                )));
            }
        }
    }
    let mut write = 0;
    for read in 0..n {
        if alive[read] {
            line[write] = line[read];
            write += 1;
        }
    }
    line.truncate(write);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zoom_profile_gets_more_detailed() {
        assert_eq!(levels_for_zoom(10), &[250, 500]);
        assert_eq!(levels_for_zoom(12), &[50, 200]);
        assert_eq!(levels_for_zoom(15), &[10, 50]);
    }

    #[test]
    fn saddles_resolve_by_asymptotic_decider() {
        // Both saddle cases emit two segments, but the pairing flips with the
        // decider.
        for case in [5, 10] {
            assert_eq!(segments_for(case, false).len(), 2);
            assert_eq!(segments_for(case, true).len(), 2);
            assert_ne!(segments_for(case, false), segments_for(case, true));
        }
        assert!(segments_for(0, false).is_empty());
        assert!(segments_for(15, false).is_empty());

        // Clear-cut saddles: strongly connected above vs below diagonals.
        assert!(saddle_above([-0.1, 5.0, -0.1, 5.0], 0.0));
        assert!(!saddle_above([-4.0, 0.5, -4.0, 0.5], 0.0));
        // The corner MEAN (+0.5) would connect the above-diagonal here, but the
        // bilinear saddle value (1 - 0.39) / (-6) < 0 says below connects:
        // asymptotic decider != center decider.
        let corners = [-1.0, 3.9, -1.0, 0.1];
        assert!((corners.iter().sum::<f32>() / 4.0) > 0.0);
        assert!(!saddle_above(corners, 0.0));
        // Degenerate saddle (denominator 0) falls back to the fixed choice.
        assert!(!saddle_above([-1.0, 1.0, -1.0, 1.0], 0.0));
    }

    #[test]
    fn adjacent_tiles_agree_on_their_shared_boundary() {
        // Two horizontally adjacent tiles over one continuous field must emit
        // identical vertices in the shared boundary strip (where simplification
        // pins them), so seams line up exactly.
        fn field(x: i32, y: i32) -> f32 {
            let (fx, fy) = (x as f32 * 0.11, y as f32 * 0.13);
            300.0 + 220.0 * (fx.sin() * fy.cos()) + 90.0 * ((1.7 * fx).cos() * (2.3 * fy).sin())
        }
        let size = 64_i32;
        let left = DemNeighborhood::synthetic(size as usize, field);
        let right = DemNeighborhood::synthetic(size as usize, move |x, y| field(x + size, y));

        let extent = i64::from(DEFAULT_EXTENT.get());
        let pin_margin = extent / i64::from(size);
        let pinned = |point: MvtCoord| {
            let (x, y) = (i64::from(point.x), i64::from(point.y));
            x <= pin_margin
                || y <= pin_margin
                || x >= extent - pin_margin
                || y >= extent - pin_margin
        };
        let strip = |neighborhood: &DemNeighborhood,
                     keep: &dyn Fn(i64) -> bool,
                     shift: i64|
         -> std::collections::BTreeSet<(i32, i64, i64)> {
            let grid = ElevationGrid::smoothed(neighborhood);
            let mut vertices = std::collections::BTreeSet::new();
            for (elevation, lines) in marching_squares(&grid, 50) {
                for mut line in lines {
                    line.dedup();
                    simplify_line(&mut line, pinned);
                    for point in line {
                        let x = i64::from(point.x);
                        if keep(x) {
                            vertices.insert((elevation, x - shift, i64::from(point.y)));
                        }
                    }
                }
            }
            vertices
        };

        // Left tile's right edge strip vs right tile's left edge strip, in the
        // right tile's coordinates.
        let from_left = strip(&left, &|x| x >= extent - pin_margin, extent);
        let from_right = strip(&right, &|x| x <= pin_margin, 0);
        assert!(!from_left.is_empty());
        assert_eq!(from_left, from_right);
    }

    #[test]
    fn visvalingam_flattens_small_wiggles_but_keeps_pins() {
        let wiggle = |points: &[(i32, i32)]| -> Vec<MvtCoord> {
            points.iter().map(|&(x, y)| MvtCoord { x, y }).collect()
        };
        // Sub-threshold wiggles (deviation 1 unit over 16-unit spans) collapse;
        // endpoints always survive.
        let mut line = wiggle(&[(0, 0), (8, 1), (16, 0), (24, 1), (32, 0)]);
        simplify_line(&mut line, |_| false);
        assert!(line.len() <= 3, "wiggles kept: {line:?}");
        assert_eq!(line.first(), Some(&MvtCoord { x: 0, y: 0 }));
        assert_eq!(line.last(), Some(&MvtCoord { x: 32, y: 0 }));

        // A pinned interior vertex survives even though its triangle is tiny.
        let mut line = wiggle(&[(0, 0), (8, 1), (16, 0)]);
        simplify_line(&mut line, |p| p.x == 8);
        assert_eq!(line.len(), 3);

        // Genuine corners stay.
        let mut line = wiggle(&[(0, 0), (32, 32), (64, 0)]);
        simplify_line(&mut line, |_| false);
        assert_eq!(line.len(), 3);
    }

    #[test]
    fn linear_ramp_contours_stay_put_and_simplify() {
        // e = 8 m per pixel along x: thresholds at exact positions. The
        // binomial blur must not move them (linear fields are preserved), and
        // straight lines must collapse to a few vertices.
        let neighborhood = DemNeighborhood::synthetic(64, |x, _y| 8.0 * x as f32);
        let grid = ElevationGrid::smoothed(&neighborhood);
        let isolines = marching_squares(&grid, 50);
        // Threshold 50 crosses grid where 8x - 4 = 50 → x = 6.75 → 6.75 * 64 = 432.
        let lines = &isolines[&50];
        assert_eq!(lines.len(), 1);
        assert!(lines[0].iter().all(|point| point.x == 432));

        // The vertical line simplifies down to its pinned + end vertices.
        let mut line = lines[0].clone();
        let before = line.len();
        let extent = i64::from(DEFAULT_EXTENT.get());
        simplify_line(&mut line, |point| {
            let y = i64::from(point.y);
            y <= 64 || y >= extent - 64
        });
        assert!(line.len() <= 6, "expected collapse, got {}", line.len());
        assert!(before > 20);
    }
}
