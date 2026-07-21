use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::Serialize;

use biei_core::config::{CostConfig, CostRange};

use super::empirical::{
    CalibrationRenderState, EmpiricalCostModel, RenderShapeKey, render_shape_from_labels,
    render_state_from_labels,
};
use super::histogram::{MergedHistogram, histogram, merge_series};
use super::provenance::ensure_compatible_provenance;
use super::{MIN_OPTIONAL_SAMPLES, MIN_WARM_RENDER_SAMPLES};
use crate::calibration::{CalibrationHistogram, CalibrationProfile};

/// The maximum render-blocking fetch ratio accepted for the independently
/// captured resource-warm reference window.
const WARM_WINDOW_FETCHES_PER_RENDER_WARN: f64 = 0.05;

/// Quantile band mapped onto `CostRange` for directly-observed costs.
const DIRECT_BAND: (f64, f64) = (0.25, 0.75);
/// Wall-clock band whose excess over the CPU estimate becomes the modeled
/// in-render resource wait.
const RESOURCE_BAND: (f64, f64) = (0.50, 0.90);

/// Costs derived from a calibration profile, plus human-readable derivation
/// notes (fallbacks, approximations) that belong in the run report.
pub struct CalibratedCosts {
    pub costs: CostConfig,
    pub notes: Vec<String>,
    pub coverage: CalibrationCoverage,
    pub sampling_model: EmpiricalCostModel,
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CalibrationValueSource {
    Measured,
    Derived,
    Default,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct CalibrationStageCoverage {
    pub source: CalibrationValueSource,
    pub samples: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct CalibrationCoverage {
    pub style_setup: CalibrationStageCoverage,
    pub source_load: CalibrationStageCoverage,
    pub render_cpu: CalibrationStageCoverage,
    pub warm_render_resource: CalibrationStageCoverage,
    pub first_render_resource: CalibrationStageCoverage,
    pub hop_latency: CalibrationStageCoverage,
    pub sla: CalibrationStageCoverage,
}

impl CalibrationStageCoverage {
    const fn measured(samples: f64) -> Self {
        Self {
            source: CalibrationValueSource::Measured,
            samples,
        }
    }

    const fn derived(samples: f64) -> Self {
        Self {
            source: CalibrationValueSource::Derived,
            samples,
        }
    }

    pub(super) const fn default() -> Self {
        Self {
            source: CalibrationValueSource::Default,
            samples: 0.0,
        }
    }
}
/// Map profile histograms onto `CostConfig`. `base` supplies `hop_latency`,
/// `sla`, and the fallback for histograms with too few samples.
/// Two-window fusion: a **verified resource-warm** reference window supplies
/// CPU service-wall proxy, and a realistic-traffic window supplies the wall
/// distributions whose excess becomes the modeled resource waits. The traffic
/// window is *expected* to contain provider I/O, so its upstream
/// activity is recorded as context rather than treated as CPU evidence.
///
/// Both windows must come from the same renderer revision, hardware, and node
/// shape — subtracting a reference measured under another implementation or
/// concurrency layout is meaningless.
pub fn derive_costs_with_cpu_reference(
    traffic: &CalibrationProfile,
    cpu_reference: &CalibrationProfile,
    base: &CostConfig,
) -> Result<CalibratedCosts> {
    ensure_compatible_provenance(&cpu_reference.provenance, &traffic.provenance)?;
    ensure!(
        cpu_reference.provenance.capture_concurrency == Some(1),
        "cpu reference must record capture_concurrency=1; a concurrent service-wall window may already include CPU scheduling contention that the simulator would apply again"
    );
    ensure_uncensored_render_tail(cpu_reference, "cpu reference")?;
    ensure_uncensored_render_tail(traffic, "traffic")?;

    let mut notes = Vec::new();

    // The reference must provide strong evidence that it was resource-warm:
    // without upstream instrumentation, or with more than the accepted fetch
    // ratio, it is not a usable service-wall reference.
    let reference_render = histogram(cpu_reference, "biei_render_duration_seconds")
        .context("cpu reference profile has no render duration histogram")?;
    let reference_warm_actual = merge_series(reference_render, |labels| {
        labels.get("state").is_some_and(|state| state == "warm")
    });
    ensure!(
        reference_warm_actual.count >= MIN_WARM_RENDER_SAMPLES,
        "cpu reference has {} warm render samples; at least {MIN_WARM_RENDER_SAMPLES} are required",
        reference_warm_actual.count
    );
    let traffic_render = histogram(traffic, "biei_render_duration_seconds")
        .context("traffic profile has no render duration histogram")?;
    let reference_shapes =
        render_shape_counts(reference_render, Some(CalibrationRenderState::Warm))?;
    let traffic_shapes = render_shape_counts(traffic_render, None)?;
    for (shape, samples) in traffic_shapes
        .iter()
        .filter(|(_, samples)| **samples >= MIN_OPTIONAL_SAMPLES)
    {
        ensure!(
            reference_shapes.get(shape).copied().unwrap_or_default() >= MIN_OPTIONAL_SAMPLES,
            "cpu reference does not cover traffic render shape {shape:?} with at least {MIN_OPTIONAL_SAMPLES:.0} warm samples ({samples:.0} traffic samples); cross-shape wall subtraction would misclassify CPU/encoding as provider I/O"
        );
    }
    let traffic_warm_shapes =
        render_shape_counts(traffic_render, Some(CalibrationRenderState::Warm))?;
    for (shape, samples) in &traffic_warm_shapes {
        ensure!(
            reference_shapes.get(shape).copied().unwrap_or_default() > 0.0,
            "cpu reference has no warm samples for traffic render shape {shape:?} ({samples:.0} warm traffic samples)"
        );
    }
    let reference_warm = reweight_reference_for_traffic_mix(
        reference_render,
        &reference_shapes,
        &traffic_warm_shapes,
    )?;
    let reference_upstream = histogram(
        cpu_reference,
        "mmpf_mln_resource_upstream_attempt_duration_seconds",
    )
    .context(
        "cpu reference profile lacks upstream fetch activity, so its resource-warm state cannot \
         be verified; recapture it with the current exporter",
    )?;
    ensure!(
        !reference_upstream.series.is_empty(),
        "cpu reference profile has an empty upstream fetch histogram; zero observed fetches and \
         missing instrumentation cannot be distinguished — recapture it from a deployment that \
         exposes FileSource upstream series",
    );
    let reference_fetches = merge_series(reference_upstream, |labels| {
        labels
            .get("priority")
            .is_some_and(|priority| priority == "regular")
    });
    let reference_ratio = reference_fetches.count / reference_warm_actual.count.max(1.0);
    ensure!(
        reference_ratio <= WARM_WINDOW_FETCHES_PER_RENDER_WARN,
        "cpu reference window saw {reference_ratio:.3} render-blocking upstream fetches per \
         render (limit {WARM_WINDOW_FETCHES_PER_RENDER_WARN}); it is not a clean service-wall \
         reference",
    );
    // The whole reference window is constrained to low upstream activity, so
    // use its representative band rather than assuming only the fast tail was
    // resource-warm. This remains a wall-clock proxy, not OS CPU time.
    let cpu_low = reference_warm
        .quantile(DIRECT_BAND.0)
        .expect("reference count checked");
    let cpu_high = reference_warm
        .quantile(DIRECT_BAND.1)
        .expect("reference count checked");
    let render_cpu_cost = ordered_range(cpu_low, cpu_high);
    let cpu_mid = (cpu_low + cpu_high) / 2;
    notes.push(format!(
        "render cpu approximated by a verified resource-warm, shape-conditioned service-wall \
         reference ({reference_ratio:.3} fetches per render), reweighted to the traffic mix, \
         q{:02}..q{:02} of warm render walls",
        (DIRECT_BAND.0 * 100.0) as u32,
        (DIRECT_BAND.1 * 100.0) as u32,
    ));

    let mut derived = derive_with_cpu_reference(
        traffic,
        base,
        (
            render_cpu_cost,
            cpu_mid,
            CalibrationStageCoverage::derived(reference_warm_actual.count),
        ),
        notes,
    )?;
    derived.sampling_model.add_cpu_reference(cpu_reference);
    Ok(derived)
}

fn render_shape_counts(
    render: &CalibrationHistogram,
    state: Option<CalibrationRenderState>,
) -> Result<BTreeMap<RenderShapeKey, f64>> {
    let mut counts = BTreeMap::new();
    for series in &render.series {
        if series.sample_count <= 0.0 {
            continue;
        }
        let series_state = render_state_from_labels(&series.labels).with_context(|| {
            format!(
                "render histogram series is missing a supported state label: {:?}",
                series.labels
            )
        })?;
        if state.is_some_and(|expected| series_state != expected) {
            continue;
        }
        let shape = render_shape_from_labels(&series.labels).with_context(|| {
            format!(
                "render histogram series lacks bounded render-shape labels: {:?}",
                series.labels
            )
        })?;
        *counts.entry(shape).or_default() += series.sample_count;
    }
    ensure!(
        !counts.is_empty(),
        "render histogram has no positive samples for the requested state"
    );
    Ok(counts)
}

fn reweight_reference_for_traffic_mix(
    reference: &CalibrationHistogram,
    reference_shapes: &BTreeMap<RenderShapeKey, f64>,
    traffic_shapes: &BTreeMap<RenderShapeKey, f64>,
) -> Result<MergedHistogram> {
    let mut merged: BTreeMap<u64, (f64, f64)> = BTreeMap::new();
    let mut count = 0.0;
    for series in &reference.series {
        if !series
            .labels
            .get("state")
            .is_some_and(|state| state == "warm")
        {
            continue;
        }
        let Some(shape) = render_shape_from_labels(&series.labels) else {
            continue;
        };
        let Some(traffic_count) = traffic_shapes.get(&shape) else {
            continue;
        };
        let reference_count = reference_shapes.get(&shape).copied().unwrap_or_default();
        ensure!(
            reference_count > 0.0,
            "cpu reference has no samples for traffic render shape {shape:?}"
        );
        let weight = traffic_count / reference_count;
        count += series.sample_count * weight;
        for bucket in &series.buckets {
            let Some(bound) = bucket.upper_bound_seconds else {
                continue;
            };
            let entry = merged.entry(bound.to_bits()).or_insert((bound, 0.0));
            entry.1 += bucket.count * weight;
        }
    }
    ensure!(
        count > 0.0,
        "cpu reference and traffic profile have no overlapping warm render shapes"
    );
    Ok(MergedHistogram {
        count,
        buckets: merged.into_values().collect(),
    })
}

pub(super) fn ensure_uncensored_render_tail(
    profile: &CalibrationProfile,
    label: &str,
) -> Result<()> {
    let censored = histogram(profile, "biei_render_timeout_lower_bound_seconds").context(
        "calibration profile lacks render-timeout censoring evidence; recapture it with the current exporter",
    )?;
    ensure!(
        !censored.series.is_empty(),
        "{label} calibration profile has no render-timeout censoring series; zero timeouts cannot be distinguished from missing instrumentation, so recapture it with the current exporter",
    );
    let timeout_count = censored
        .series
        .iter()
        .map(|series| series.sample_count)
        .sum::<f64>();
    ensure!(
        timeout_count == 0.0,
        "{label} calibration window contains {timeout_count:.0} timed-out renders; successful render histograms are right-censored, so choose a clean window or model the censored tail explicitly",
    );
    Ok(())
}

/// Shared derivation tail. Every stage is independent: missing or sparse
/// families retain their base value while usable families still calibrate the
/// run. `cpu_reference` carries a CPU range measured from a verified
/// resource-warm reference window.
pub(super) fn derive_with_cpu_reference(
    profile: &CalibrationProfile,
    base: &CostConfig,
    cpu_reference: (CostRange, Duration, CalibrationStageCoverage),
    mut notes: Vec<String>,
) -> Result<CalibratedCosts> {
    let render = histogram(profile, "biei_render_duration_seconds");
    let warm = render.map_or_else(MergedHistogram::empty, |render| {
        let render_shapes = render
            .series
            .iter()
            .map(|series| {
                let mut labels = series.labels.clone();
                labels.remove("state");
                labels
            })
            .collect::<BTreeSet<_>>()
            .len();
        if render_shapes > 1 {
            notes.push(format!(
                "collapsed {render_shapes} bounded render shapes into workload-weighted global cost ranges; do not reuse them for a materially different render-shape mix"
            ));
        }
        merge_series(render, |labels| {
            labels.get("state").is_some_and(|state| state == "warm")
        })
    });
    let first = render.map_or_else(MergedHistogram::empty, |render| {
        merge_series(render, |labels| {
            labels
                .get("state")
                .is_some_and(|state| state == "cold" || state == "swap")
        })
    });

    let (style_setup_cost, style_setup_coverage) = band_or_fallback(
        histogram(profile, "biei_style_setup_duration_seconds")
            .map(|series| merge_series(series, |_| true)),
        DIRECT_BAND,
        base.style_setup_cost,
        "style_setup",
        &mut notes,
    );
    let (source_load_cost, source_load_coverage) = band_or_fallback(
        histogram(profile, "biei_source_setup_duration_seconds")
            .map(|series| merge_series(series, |_| true)),
        DIRECT_BAND,
        base.source_load_cost,
        "source_load",
        &mut notes,
    );

    let warm_usable = warm.count >= MIN_WARM_RENDER_SAMPLES;
    if warm.count > 0.0 && !warm_usable {
        notes.push(format!(
            "warm render kept from base config ({} samples < {MIN_WARM_RENDER_SAMPLES})",
            warm.count
        ));
    } else if warm.count == 0.0 {
        notes.push("warm render kept from base config (no samples)".to_owned());
    }
    record_traffic_resource_context(profile, warm.count + first.count, &mut notes);

    let (render_cpu_cost, cpu_mid, render_cpu_coverage) = cpu_reference;

    let (render_resource_cost, warm_resource_coverage) = if warm_usable {
        (
            residency_excess(&warm, cpu_mid),
            CalibrationStageCoverage::derived(warm.count),
        )
    } else {
        (
            base.render_resource_cost,
            CalibrationStageCoverage::default(),
        )
    };
    let (first_render_resource_cost, first_resource_coverage) = if first.count
        >= MIN_OPTIONAL_SAMPLES
    {
        (
            residency_excess(&first, cpu_mid),
            CalibrationStageCoverage::derived(first.count),
        )
    } else {
        notes.push(format!(
            "first_render_resource kept from base config ({} first-render samples < {MIN_OPTIONAL_SAMPLES})",
            first.count
        ));
        (
            base.first_render_resource_cost,
            CalibrationStageCoverage::default(),
        )
    };

    Ok(CalibratedCosts {
        costs: CostConfig {
            style_setup_cost,
            source_load_cost,
            render_cpu_cost,
            render_resource_cost,
            first_render_resource_cost,
            hop_latency: base.hop_latency,
            sla: base.sla,
        },
        notes,
        coverage: CalibrationCoverage {
            style_setup: style_setup_coverage,
            source_load: source_load_coverage,
            render_cpu: render_cpu_coverage,
            warm_render_resource: warm_resource_coverage,
            first_render_resource: first_resource_coverage,
            hop_latency: CalibrationStageCoverage::default(),
            sla: CalibrationStageCoverage::default(),
        },
        sampling_model: EmpiricalCostModel::from_profile(profile),
    })
}

/// Record resource activity in the realistic traffic window. This window is
/// deliberately allowed to contain provider I/O; CPU comes exclusively from
/// the independently verified reference window.
fn record_traffic_resource_context(
    profile: &CalibrationProfile,
    renders: f64,
    notes: &mut Vec<String>,
) {
    let Some(upstream) = histogram(
        profile,
        "mmpf_mln_resource_upstream_attempt_duration_seconds",
    ) else {
        notes.push(
            "traffic profile lacks upstream fetch instrumentation; wall-minus-reference-cpu \
             remains usable, but resource activity cannot be reported"
                .to_owned(),
        );
        return;
    };
    if upstream.series.is_empty() {
        notes.push(
            "traffic profile has an empty upstream fetch histogram; wall-minus-reference-cpu \
             remains usable, but resource activity cannot be reported"
                .to_owned(),
        );
        return;
    }
    let render_blocking = merge_series(upstream, |labels| {
        labels
            .get("priority")
            .is_some_and(|priority| priority == "regular")
    });
    if renders <= 0.0 {
        return;
    }
    let fetches_per_render = render_blocking.count / renders;
    notes.push(format!(
        "traffic window saw {fetches_per_render:.3} render-blocking upstream fetches per \
         render; its walls feed resource waits, cpu comes from the reference window"
    ));
}
fn band_or_fallback(
    merged: Option<MergedHistogram>,
    band: (f64, f64),
    fallback: CostRange,
    label: &str,
    notes: &mut Vec<String>,
) -> (CostRange, CalibrationStageCoverage) {
    let Some(merged) = merged.filter(|merged| merged.count >= MIN_OPTIONAL_SAMPLES) else {
        notes.push(format!(
            "{label} kept from base config (fewer than {MIN_OPTIONAL_SAMPLES} samples)"
        ));
        return (fallback, CalibrationStageCoverage::default());
    };
    let low = merged.quantile(band.0).expect("count checked");
    let high = merged.quantile(band.1).expect("count checked");
    (
        ordered_range(low, high),
        CalibrationStageCoverage::measured(merged.count),
    )
}

/// Wall-clock band minus the CPU estimate, clamped at zero: the modeled
/// in-render resource wait.
fn residency_excess(merged: &MergedHistogram, cpu_mid: Duration) -> CostRange {
    let low = merged
        .quantile(RESOURCE_BAND.0)
        .expect("caller checked count")
        .saturating_sub(cpu_mid);
    let high = merged
        .quantile(RESOURCE_BAND.1)
        .expect("caller checked count")
        .saturating_sub(cpu_mid);
    ordered_range(low, high)
}

fn ordered_range(low: Duration, high: Duration) -> CostRange {
    CostRange::new(low.min(high), low.max(high))
}
