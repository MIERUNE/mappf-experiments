use std::collections::BTreeMap;
use std::time::Duration;

use rand::{Rng, RngExt};

use crate::calibration::{CalibrationHistogram, CalibrationProfile, CalibrationSeries};

pub(super) fn histogram<'a>(
    profile: &'a CalibrationProfile,
    metric: &str,
) -> Option<&'a CalibrationHistogram> {
    profile
        .histograms
        .iter()
        .find(|histogram| histogram.metric == metric)
}

/// Disjoint-bucket histogram merged across every series passing the filter.
/// One family shares bucket bounds, so summing per finite bound is exact; the
/// `+Inf` buckets contribute only to the total count.
#[derive(Clone, Debug)]
pub(super) struct MergedHistogram {
    pub(super) count: f64,
    /// `(finite_upper_bound_seconds, disjoint_count)`, ascending.
    pub(super) buckets: Vec<(f64, f64)>,
}

pub(super) fn merge_series(
    histogram: &CalibrationHistogram,
    filter: impl Fn(&BTreeMap<String, String>) -> bool,
) -> MergedHistogram {
    let mut merged: BTreeMap<u64, (f64, f64)> = BTreeMap::new();
    let mut count = 0.0f64;
    for series in histogram
        .series
        .iter()
        .filter(|series| filter(&series.labels))
    {
        count += series.sample_count;
        for bucket in &series.buckets {
            let Some(bound) = bucket.upper_bound_seconds else {
                continue; // +Inf remainder is covered by sample_count.
            };
            let entry = merged.entry(bound.to_bits()).or_insert((bound, 0.0));
            entry.1 += bucket.count;
        }
    }
    MergedHistogram {
        count,
        buckets: merged.into_values().collect(),
    }
}

impl MergedHistogram {
    pub(super) fn from_series(series: &CalibrationSeries) -> Self {
        let mut buckets = series
            .buckets
            .iter()
            .filter_map(|bucket| {
                bucket
                    .upper_bound_seconds
                    .map(|bound| (bound, bucket.count))
            })
            .collect::<Vec<_>>();
        buckets.sort_by(|left, right| left.0.total_cmp(&right.0));
        Self {
            count: series.sample_count,
            buckets,
        }
    }

    pub(super) fn empty() -> Self {
        Self {
            count: 0.0,
            buckets: Vec::new(),
        }
    }

    /// Interpolated quantile. Samples in the `+Inf` remainder clamp to the
    /// largest finite bound: an unbounded tail cannot be interpolated
    /// honestly.
    pub(super) fn quantile(&self, q: f64) -> Option<Duration> {
        if self.count <= 0.0 || self.buckets.is_empty() {
            return None;
        }
        let target = (q.clamp(0.0, 1.0) * self.count).max(1.0);
        let mut previous_bound = 0.0f64;
        let mut cumulative = 0.0f64;
        for &(bound, in_bucket) in &self.buckets {
            let next_cumulative = cumulative + in_bucket;
            if next_cumulative >= target {
                let position = if in_bucket <= 0.0 {
                    1.0
                } else {
                    (target - cumulative) / in_bucket
                };
                let seconds = previous_bound + (bound - previous_bound) * position.clamp(0.0, 1.0);
                return Some(Duration::from_secs_f64(seconds.max(0.0)));
            }
            previous_bound = bound;
            cumulative = next_cumulative;
        }
        Some(Duration::from_secs_f64(previous_bound.max(0.0)))
    }

    /// Draw a value uniformly within the selected histogram bucket. The
    /// exporter stores disjoint bucket counts, so selection is exact at the
    /// bucket level; the within-bucket shape is intentionally unspecified.
    pub(super) fn sample(&self, rng: &mut impl Rng) -> Option<Duration> {
        if self.count <= 0.0 || self.buckets.is_empty() {
            return None;
        }
        let target = rng.random_range(0.0..self.count);
        let mut previous_bound = 0.0f64;
        let mut cumulative = 0.0f64;
        for &(bound, in_bucket) in &self.buckets {
            let next_cumulative = cumulative + in_bucket;
            if target < next_cumulative {
                let seconds = if bound <= previous_bound {
                    bound
                } else {
                    rng.random_range(previous_bound..bound)
                };
                return Some(Duration::from_secs_f64(seconds.max(0.0)));
            }
            previous_bound = bound;
            cumulative = next_cumulative;
        }
        // The target landed in the +Inf remainder. An unbounded tail cannot
        // be sampled honestly, so use the largest observed finite bound.
        Some(Duration::from_secs_f64(previous_bound.max(0.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_interpolate_and_clamp_the_unbounded_tail() {
        let merged = MergedHistogram {
            count: 100.0,
            buckets: vec![(0.1, 50.0), (0.3, 40.0)],
        };
        assert_eq!(merged.quantile(0.5), Some(Duration::from_secs_f64(0.1)));
        assert_eq!(merged.quantile(0.7), Some(Duration::from_secs_f64(0.2)));
        // q99 falls in the +Inf remainder: clamp to the largest finite bound.
        assert_eq!(merged.quantile(0.99), Some(Duration::from_secs_f64(0.3)));
    }
}
