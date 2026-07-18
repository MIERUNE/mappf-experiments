//! M12b: derive simulator `CostConfig` ranges from an exported calibration
//! profile (`calibration::CalibrationProfile`, M12a).
//!
//! The profile carries raw, provenance-bearing histograms; every modeling
//! approximation lives here at import time so stored profiles stay free of
//! fabricated numbers. Production cannot observe the CPU/resource split inside
//! `renderStill` directly. A single-window import therefore uses the fastest
//! warm renders as a provisional CPU+encode proxy. Prefer two-window fusion: a
//! verified resource-warm reference supplies a representative service-wall
//! proxy, while realistic-traffic walls supply the modeled in-render resource
//! waits above it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use rand::{Rng, RngExt};
use serde::Serialize;

use crate::calibration::{
    CALIBRATION_PROFILE_KIND, CALIBRATION_PROFILE_SCHEMA_VERSION, CalibrationHistogram,
    CalibrationProfile, CalibrationProvenance, CalibrationSeries,
};
use crate::config::SimConfig;
use biei::config::{CostConfig, CostRange};
use biei::types::{ImageFormat, InternalTask, RenderMode, RenderRequest, Scale};

/// Below this many warm-render samples the profile is not calibration
/// evidence and the import fails.
const MIN_WARM_RENDER_SAMPLES: f64 = 30.0;
/// Optional histograms (setup, first render) below this fall back to the base
/// config with a recorded note instead of failing the import.
const MIN_OPTIONAL_SAMPLES: f64 = 10.0;

/// Above this many render-blocking upstream fetch attempts per render, the
/// capture window was clearly not resource-cache-warm and warm render walls
/// cannot be read as CPU service demand: that stage falls back to base costs.
const WARM_WINDOW_FETCHES_PER_RENDER_ERROR: f64 = 0.5;
/// Above this ratio the derivation still proceeds but records a warning note.
const WARM_WINDOW_FETCHES_PER_RENDER_WARN: f64 = 0.05;

/// Quantile band mapped onto `CostRange` for directly-observed costs.
const DIRECT_BAND: (f64, f64) = (0.25, 0.75);
/// Warm renders at/below this quantile band approximate resource-cache-hit
/// CPU+encode service demand.
const CPU_BAND: (f64, f64) = (0.10, 0.25);
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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CalibrationRenderState {
    Warm,
    Cold,
    Swap,
}

impl CalibrationRenderState {
    fn label(self) -> &'static str {
        match self {
            Self::Warm => "warm",
            Self::Cold => "cold",
            Self::Swap => "swap",
        }
    }
}

/// Shape-aware empirical distributions used only by the simulator runtime.
/// Routing continues to use the representative `CostConfig` ranges above.
#[derive(Clone, Debug, Default)]
pub struct EmpiricalCostModel {
    render_exact: BTreeMap<RenderSampleKey, MergedHistogram>,
    render_by_state: BTreeMap<CalibrationRenderState, MergedHistogram>,
    /// Resource-warm CPU/service-wall references keyed by render shape. Kept
    /// separate from traffic totals so per-shape encoding cost is never
    /// mistaken for provider wait.
    render_cpu_exact: BTreeMap<RenderShapeKey, MergedHistogram>,
    style_exact: BTreeMap<StyleSampleKey, MergedHistogram>,
    style_by_state: BTreeMap<CalibrationRenderState, MergedHistogram>,
    source_exact: BTreeMap<SourceSampleKey, MergedHistogram>,
    source_global: Option<MergedHistogram>,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct EmpiricalSamplingCoverage {
    pub render_exact_shapes: usize,
    pub render_state_fallbacks: usize,
    pub render_cpu_exact_shapes: usize,
    pub style_exact_shapes: usize,
    pub style_state_fallbacks: usize,
    pub source_exact_shapes: usize,
    pub source_global_fallback: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RenderSampleKey {
    render_mode: String,
    scale: String,
    format: String,
    size: String,
    state: CalibrationRenderState,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RenderShapeKey {
    render_mode: String,
    scale: String,
    format: String,
    size: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StyleSampleKey {
    render_mode: String,
    scale: String,
    state: CalibrationRenderState,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SourceSampleKey {
    render_mode: String,
    scale: String,
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

    const fn default() -> Self {
        Self {
            source: CalibrationValueSource::Default,
            samples: 0.0,
        }
    }
}

pub fn load_calibration_profile(path: impl AsRef<Path>) -> Result<CalibrationProfile> {
    let path = path.as_ref();
    let bytes =
        fs::read(path).with_context(|| format!("read calibration profile {}", path.display()))?;
    let profile: CalibrationProfile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse calibration profile {}", path.display()))?;
    ensure!(
        profile.schema_version == CALIBRATION_PROFILE_SCHEMA_VERSION,
        "unsupported calibration profile schema version {}",
        profile.schema_version
    );
    ensure!(
        profile.kind == CALIBRATION_PROFILE_KIND,
        "not a calibration profile (kind {:?})",
        profile.kind
    );
    Ok(profile)
}

impl EmpiricalCostModel {
    pub(crate) fn from_profile(profile: &CalibrationProfile) -> Self {
        let mut model = Self::default();
        if let Some(render) = histogram(profile, "biei_render_duration_seconds") {
            for series in &render.series {
                let Some(key) = render_key_from_labels(&series.labels) else {
                    continue;
                };
                if series.sample_count >= MIN_OPTIONAL_SAMPLES {
                    model
                        .render_exact
                        .insert(key, MergedHistogram::from_series(series));
                }
            }
            for state in [
                CalibrationRenderState::Warm,
                CalibrationRenderState::Cold,
                CalibrationRenderState::Swap,
            ] {
                let merged = merge_series(render, |labels| {
                    labels
                        .get("state")
                        .is_some_and(|value| value == state.label())
                });
                let minimum = if state == CalibrationRenderState::Warm {
                    MIN_WARM_RENDER_SAMPLES
                } else {
                    MIN_OPTIONAL_SAMPLES
                };
                if merged.count >= minimum {
                    model.render_by_state.insert(state, merged);
                }
            }
        }
        if let Some(style) = histogram(profile, "biei_style_setup_duration_seconds") {
            for series in &style.series {
                let Some(key) = style_key_from_labels(&series.labels) else {
                    continue;
                };
                if series.sample_count >= MIN_OPTIONAL_SAMPLES {
                    model
                        .style_exact
                        .insert(key, MergedHistogram::from_series(series));
                }
            }
            for state in [CalibrationRenderState::Cold, CalibrationRenderState::Swap] {
                let merged = merge_series(style, |labels| {
                    labels
                        .get("state")
                        .is_some_and(|value| value == state.label())
                });
                if merged.count >= MIN_OPTIONAL_SAMPLES {
                    model.style_by_state.insert(state, merged);
                }
            }
        }
        if let Some(source) = histogram(profile, "biei_source_setup_duration_seconds") {
            for series in &source.series {
                let Some(key) = source_key_from_labels(&series.labels) else {
                    continue;
                };
                if series.sample_count >= MIN_OPTIONAL_SAMPLES {
                    model
                        .source_exact
                        .insert(key, MergedHistogram::from_series(series));
                }
            }
            let global = merge_series(source, |_| true);
            if global.count >= MIN_OPTIONAL_SAMPLES {
                model.source_global = Some(global);
            }
        }
        model
    }

    pub fn coverage(&self) -> EmpiricalSamplingCoverage {
        EmpiricalSamplingCoverage {
            render_exact_shapes: self.render_exact.len(),
            render_state_fallbacks: self.render_by_state.len(),
            render_cpu_exact_shapes: self.render_cpu_exact.len(),
            style_exact_shapes: self.style_exact.len(),
            style_state_fallbacks: self.style_by_state.len(),
            source_exact_shapes: self.source_exact.len(),
            source_global_fallback: self.source_global.is_some(),
        }
    }

    pub fn sample_render(
        &self,
        task: &InternalTask,
        state: CalibrationRenderState,
        rng: &mut impl Rng,
    ) -> Option<Duration> {
        let key = render_key_for_task(task, state);
        self.render_exact
            .get(&key)
            .or_else(|| self.render_by_state.get(&state))
            .and_then(|histogram| histogram.sample(rng))
    }

    pub fn sample_render_cpu(&self, task: &InternalTask, rng: &mut impl Rng) -> Option<Duration> {
        self.render_cpu_exact
            .get(&render_shape_for_task(task))
            .and_then(|histogram| histogram.sample(rng))
    }

    fn add_cpu_reference(&mut self, profile: &CalibrationProfile) {
        let Some(render) = histogram(profile, "biei_render_duration_seconds") else {
            return;
        };
        let shapes = render
            .series
            .iter()
            .filter(|series| {
                series
                    .labels
                    .get("state")
                    .is_some_and(|state| state == "warm")
            })
            .filter_map(|series| render_shape_from_labels(&series.labels))
            .collect::<BTreeSet<_>>();
        for shape in shapes {
            let merged = merge_series(render, |labels| {
                labels.get("state").is_some_and(|state| state == "warm")
                    && render_shape_from_labels(labels).as_ref() == Some(&shape)
            });
            if merged.count >= MIN_OPTIONAL_SAMPLES {
                self.render_cpu_exact.insert(shape, merged);
            }
        }
    }

    pub fn sample_style_setup(
        &self,
        task: &InternalTask,
        state: CalibrationRenderState,
        rng: &mut impl Rng,
    ) -> Option<Duration> {
        let key = StyleSampleKey {
            render_mode: render_mode_label(task.request.render_mode()).to_owned(),
            scale: task.pixel_ratio.to_scale().as_gossip_value().to_owned(),
            state,
        };
        self.style_exact
            .get(&key)
            .or_else(|| self.style_by_state.get(&state))
            .and_then(|histogram| histogram.sample(rng))
    }

    pub fn sample_source_setup(
        &self,
        render_mode: RenderMode,
        scale: Scale,
        rng: &mut impl Rng,
    ) -> Option<Duration> {
        let key = SourceSampleKey {
            render_mode: render_mode_label(render_mode).to_owned(),
            scale: scale.as_gossip_value().to_owned(),
        };
        self.source_exact
            .get(&key)
            .or(self.source_global.as_ref())
            .and_then(|histogram| histogram.sample(rng))
    }
}

fn render_key_from_labels(labels: &BTreeMap<String, String>) -> Option<RenderSampleKey> {
    let shape = render_shape_from_labels(labels)?;
    Some(RenderSampleKey {
        render_mode: shape.render_mode,
        scale: shape.scale,
        format: shape.format,
        size: shape.size,
        state: render_state_from_labels(labels)?,
    })
}

fn render_shape_from_labels(labels: &BTreeMap<String, String>) -> Option<RenderShapeKey> {
    Some(RenderShapeKey {
        render_mode: labels.get("render_mode")?.clone(),
        scale: labels.get("scale")?.clone(),
        format: labels.get("format")?.clone(),
        size: labels.get("size")?.clone(),
    })
}

fn style_key_from_labels(labels: &BTreeMap<String, String>) -> Option<StyleSampleKey> {
    Some(StyleSampleKey {
        render_mode: labels.get("render_mode")?.clone(),
        scale: labels.get("scale")?.clone(),
        state: render_state_from_labels(labels)?,
    })
}

fn source_key_from_labels(labels: &BTreeMap<String, String>) -> Option<SourceSampleKey> {
    Some(SourceSampleKey {
        render_mode: labels.get("render_mode")?.clone(),
        scale: labels.get("scale")?.clone(),
    })
}

fn render_state_from_labels(labels: &BTreeMap<String, String>) -> Option<CalibrationRenderState> {
    match labels.get("state")?.as_str() {
        "warm" => Some(CalibrationRenderState::Warm),
        "cold" => Some(CalibrationRenderState::Cold),
        "swap" => Some(CalibrationRenderState::Swap),
        _ => None,
    }
}

fn render_key_for_task(task: &InternalTask, state: CalibrationRenderState) -> RenderSampleKey {
    let shape = render_shape_for_task(task);
    RenderSampleKey {
        render_mode: shape.render_mode,
        scale: shape.scale,
        format: shape.format,
        size: shape.size,
        state,
    }
}

fn render_shape_for_task(task: &InternalTask) -> RenderShapeKey {
    RenderShapeKey {
        render_mode: render_mode_label(task.request.render_mode()).to_owned(),
        scale: task.pixel_ratio.to_scale().as_gossip_value().to_owned(),
        format: image_format_label(task.output_format).to_owned(),
        size: render_size_label(task).to_owned(),
    }
}

fn render_mode_label(mode: RenderMode) -> &'static str {
    mode.as_gossip_value()
}

fn image_format_label(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Png => "png",
        ImageFormat::Webp => "webp",
        ImageFormat::Jpeg => "jpeg",
    }
}

fn render_size_label(task: &InternalTask) -> &'static str {
    let logical_edge = match &task.request {
        RenderRequest::Tile { tile_size, .. } => u32::from(*tile_size),
        RenderRequest::StaticImage { width, height, .. } => u32::from((*width).max(*height)),
    };
    let scale = match task.pixel_ratio.to_scale() {
        Scale::X1 => 1,
        Scale::X2 => 2,
    };
    match logical_edge.saturating_mul(scale) {
        0..=256 => "le_256px",
        257..=512 => "le_512px",
        513..=1_024 => "le_1024px",
        1_025..=2_048 => "le_2048px",
        _ => "gt_2048px",
    }
}

/// Map profile histograms onto `CostConfig`. `base` supplies `hop_latency`,
/// `sla`, and the fallback for histograms with too few samples.
/// Two-window fusion: a **verified resource-warm** reference window supplies
/// CPU service-wall proxy, and a realistic-traffic window supplies the wall
/// distributions whose excess becomes the modeled resource waits. This is a
/// stronger fallback than single-window inference without per-render attribution
/// (which the engine-global FileSource interface cannot provide): the traffic
/// window is *expected* to contain provider I/O, so its warmth check is
/// informational instead of fatal.
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
        "biei_mln_resource_upstream_attempt_duration_seconds",
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

    let mut derived = derive_with_cpu(
        traffic,
        base,
        Some((render_cpu_cost, cpu_mid, reference_warm_actual.count)),
        notes,
    )?;
    derived.sampling_model.add_cpu_reference(cpu_reference);
    Ok(derived)
}

fn ensure_compatible_provenance(
    reference: &CalibrationProvenance,
    traffic: &CalibrationProvenance,
) -> Result<()> {
    ensure!(
        reference.hardware_profile == traffic.hardware_profile
            && reference.architecture == traffic.architecture
            && reference.cpu_cores_per_node == traffic.cpu_cores_per_node,
        "cpu reference ({} / {} / {} cores) and traffic profile ({} / {} / {} cores) come from \
         different machines; wall-minus-cpu subtraction across hardware is meaningless",
        reference.hardware_profile,
        reference.architecture,
        reference.cpu_cores_per_node,
        traffic.hardware_profile,
        traffic.architecture,
        traffic.cpu_cores_per_node,
    );
    ensure!(
        reference.deployment_revision == traffic.deployment_revision,
        "cpu reference revision {:?} and traffic profile revision {:?} differ; service-wall \
         subtraction across renderer revisions is meaningless",
        reference.deployment_revision,
        traffic.deployment_revision,
    );
    ensure!(
        reference.renderer_slots_per_node == traffic.renderer_slots_per_node
            && reference.execution_permits_per_node == traffic.execution_permits_per_node
            && reference.native_render_permits_per_node == traffic.native_render_permits_per_node,
        "cpu reference node shape ({} slots / {} execution / {} native) and traffic profile \
         node shape ({} slots / {} execution / {} native) differ; concurrent service walls \
         are not comparable",
        reference.renderer_slots_per_node,
        reference.execution_permits_per_node,
        reference.native_render_permits_per_node,
        traffic.renderer_slots_per_node,
        traffic.execution_permits_per_node,
        traffic.native_render_permits_per_node,
    );
    Ok(())
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

pub fn derive_costs(profile: &CalibrationProfile, base: &CostConfig) -> Result<CalibratedCosts> {
    ensure_uncensored_render_tail(profile, "single-window")?;
    derive_with_cpu(profile, base, None, Vec::new())
}

fn ensure_uncensored_render_tail(profile: &CalibrationProfile, label: &str) -> Result<()> {
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
/// run. `cpu_override` carries a CPU range measured from a verified
/// resource-warm reference window.
fn derive_with_cpu(
    profile: &CalibrationProfile,
    base: &CostConfig,
    cpu_override: Option<(CostRange, Duration, f64)>,
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

    let mut warm_usable = warm.count >= MIN_WARM_RENDER_SAMPLES;
    if warm.count > 0.0 && !warm_usable {
        notes.push(format!(
            "warm render kept from base config ({} samples < {MIN_WARM_RENDER_SAMPLES})",
            warm.count
        ));
    } else if warm.count == 0.0 {
        notes.push("warm render kept from base config (no samples)".to_owned());
    }
    if warm_usable
        && cpu_override.is_none()
        && let Err(error) = verify_resource_warm_window(
            profile,
            warm.count + first.count,
            WarmthWindowRole::CpuSource,
            &mut notes,
        )
    {
        notes.push(format!(
            "warm render ignored and kept from base config: {error}"
        ));
        warm_usable = false;
    } else if warm_usable && cpu_override.is_some() {
        verify_resource_warm_window(
            profile,
            warm.count + first.count,
            WarmthWindowRole::TrafficContext,
            &mut notes,
        )?;
    }

    let (render_cpu_cost, cpu_mid, render_cpu_coverage) = match cpu_override {
        Some((range, mid, samples)) => (range, mid, CalibrationStageCoverage::derived(samples)),
        None if warm_usable => {
            // Fastest warm renders approximate resource-cache-hit CPU+encode
            // service.
            let cpu_low = warm.quantile(CPU_BAND.0).expect("warm count checked");
            let cpu_high = warm.quantile(CPU_BAND.1).expect("warm count checked");
            notes.push(format!(
                "render cpu approximated from warm render wall q{:02}..q{:02}; resource waits are wall excess over that estimate",
                (CPU_BAND.0 * 100.0) as u32,
                (CPU_BAND.1 * 100.0) as u32,
            ));
            (
                ordered_range(cpu_low, cpu_high),
                (cpu_low + cpu_high) / 2,
                CalibrationStageCoverage::derived(warm.count),
            )
        }
        None => (
            base.render_cpu_cost,
            base.render_cpu_cost.mid(),
            CalibrationStageCoverage::default(),
        ),
    };

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

/// Apply the measured node shape before running with derived costs. Keeping
/// provenance only in the report would silently combine service times measured
/// on one machine/permit layout with the simulator's unrelated defaults.
pub fn apply_profile_provenance(
    profile: &CalibrationProfile,
    config: &mut SimConfig,
) -> Result<()> {
    let provenance = &profile.provenance;
    ensure!(
        provenance.cpu_cores_per_node > 0,
        "calibration profile has zero CPU cores per node"
    );
    ensure!(
        provenance.renderer_slots_per_node > 0,
        "calibration profile has zero renderer slots per node"
    );
    ensure!(
        provenance.execution_permits_per_node > 0
            && provenance.execution_permits_per_node <= provenance.renderer_slots_per_node,
        "calibration execution permits must be in 1..=renderer slots"
    );
    ensure!(
        provenance.native_render_permits_per_node > 0
            && provenance.native_render_permits_per_node <= provenance.renderer_slots_per_node,
        "calibration native-render permits must be in 1..=renderer slots"
    );
    config.cpu_cores_per_node = provenance.cpu_cores_per_node;
    config.cluster.renderer_slots_per_node = provenance.renderer_slots_per_node;
    config.cluster.render_permits_per_node = Some(provenance.execution_permits_per_node);
    config.cluster.cpu_render_permits_per_node = Some(provenance.native_render_permits_per_node);
    Ok(())
}

#[derive(Clone, Copy)]
enum WarmthWindowRole {
    /// This window supplies the CPU service-wall proxy and must be warm enough
    /// for that inference.
    CpuSource,
    /// This is a realistic traffic window; upstream activity is expected and
    /// recorded only as context.
    TrafficContext,
}

/// A warm render wall only approximates CPU service when the capture window
/// was resource-cache-warm. Render-blocking (regular-lane) upstream fetch
/// attempts during the window prove it was not: `state="warm"` means
/// style-warm, and the wall still contains provider I/O. Fail loudly instead
/// of exporting an I/O-contaminated number as CPU demand.
fn verify_resource_warm_window(
    profile: &CalibrationProfile,
    renders: f64,
    role: WarmthWindowRole,
    notes: &mut Vec<String>,
) -> Result<()> {
    let Some(upstream) = histogram(
        profile,
        "biei_mln_resource_upstream_attempt_duration_seconds",
    ) else {
        if matches!(role, WarmthWindowRole::CpuSource) {
            bail!(
                "profile lacks upstream fetch instrumentation; the capture window cannot prove \
                 it was resource-cache-warm — recapture it with the current exporter"
            );
        }
        notes.push(
            "traffic profile lacks upstream fetch instrumentation; wall-minus-reference-cpu \
             remains usable, but resource activity cannot be reported"
                .to_owned(),
        );
        return Ok(());
    };
    if upstream.series.is_empty() {
        if matches!(role, WarmthWindowRole::CpuSource) {
            bail!(
                "profile has an empty upstream fetch histogram; zero observed fetches and \
                 missing instrumentation cannot be distinguished — recapture it from a \
                 deployment that exposes FileSource upstream series"
            );
        }
        notes.push(
            "traffic profile has an empty upstream fetch histogram; wall-minus-reference-cpu \
             remains usable, but resource activity cannot be reported"
                .to_owned(),
        );
        return Ok(());
    }
    let render_blocking = merge_series(upstream, |labels| {
        labels
            .get("priority")
            .is_some_and(|priority| priority == "regular")
    });
    if renders <= 0.0 {
        return Ok(());
    }
    let fetches_per_render = render_blocking.count / renders;
    if matches!(role, WarmthWindowRole::TrafficContext) {
        // A traffic window feeding wall-minus-cpu subtraction is *supposed*
        // to contain provider I/O; record the ratio as context.
        notes.push(format!(
            "traffic window saw {fetches_per_render:.3} render-blocking upstream fetches per \
             render; its walls feed resource waits, cpu comes from the reference window"
        ));
        return Ok(());
    }
    ensure!(
        fetches_per_render <= WARM_WINDOW_FETCHES_PER_RENDER_ERROR,
        "capture window was not resource-cache-warm: {fetches_per_render:.2} render-blocking \
         upstream fetches per render (limit {WARM_WINDOW_FETCHES_PER_RENDER_ERROR}); warm render \
         walls would be read as CPU demand while still containing provider I/O — recapture from a \
         warmed working set, or supply a verified-warm --cpu-profile alongside this window"
    );
    if fetches_per_render > WARM_WINDOW_FETCHES_PER_RENDER_WARN {
        notes.push(format!(
            "capture window saw {fetches_per_render:.3} render-blocking upstream fetches per \
             render; render cpu may be slightly I/O-inflated"
        ));
    } else {
        notes.push(format!(
            "capture window verified resource-cache-warm ({fetches_per_render:.3} render-blocking \
             upstream fetches per render)"
        ));
    }
    Ok(())
}

fn histogram<'a>(
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
struct MergedHistogram {
    count: f64,
    /// `(finite_upper_bound_seconds, disjoint_count)`, ascending.
    buckets: Vec<(f64, f64)>,
}

fn merge_series(
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
    fn from_series(series: &CalibrationSeries) -> Self {
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

    fn empty() -> Self {
        Self {
            count: 0.0,
            buckets: Vec::new(),
        }
    }

    /// Interpolated quantile. Samples in the `+Inf` remainder clamp to the
    /// largest finite bound: an unbounded tail cannot be interpolated
    /// honestly.
    fn quantile(&self, q: f64) -> Option<Duration> {
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
    fn sample(&self, rng: &mut impl Rng) -> Option<Duration> {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rand::SeedableRng;
    use rand_xoshiro::Xoshiro256PlusPlus;
    use tokio::sync::Semaphore;
    use tokio::time::Instant;

    use super::*;
    use crate::calibration::{
        CalibrationBucket, CalibrationCollection, CalibrationProvenance, CalibrationSeries,
    };
    use crate::stub_renderer::StubRenderer;
    use biei::renderer::Renderer;

    fn series(state: &str, render_mode: &str, buckets: &[(Option<f64>, f64)]) -> CalibrationSeries {
        let sample_count = buckets.iter().map(|(_, count)| count).sum();
        CalibrationSeries {
            labels: BTreeMap::from([
                ("state".to_owned(), state.to_owned()),
                ("render_mode".to_owned(), render_mode.to_owned()),
                ("scale".to_owned(), "1x".to_owned()),
                ("format".to_owned(), "png".to_owned()),
                ("size".to_owned(), "le_512px".to_owned()),
            ]),
            sample_count,
            buckets: buckets
                .iter()
                .map(|&(upper_bound_seconds, count)| CalibrationBucket {
                    upper_bound_seconds,
                    count,
                })
                .collect(),
        }
    }

    fn profile(mut histograms: Vec<CalibrationHistogram>) -> CalibrationProfile {
        if !histograms
            .iter()
            .any(|histogram| histogram.metric == "biei_render_timeout_lower_bound_seconds")
        {
            histograms.push(CalibrationHistogram {
                metric: "biei_render_timeout_lower_bound_seconds".to_owned(),
                unit: "seconds".to_owned(),
                query: "test".to_owned(),
                series: vec![CalibrationSeries {
                    labels: BTreeMap::new(),
                    sample_count: 0.0,
                    buckets: vec![
                        CalibrationBucket {
                            upper_bound_seconds: Some(10.0),
                            count: 0.0,
                        },
                        CalibrationBucket {
                            upper_bound_seconds: None,
                            count: 0.0,
                        },
                    ],
                }],
            });
        }
        CalibrationProfile {
            schema_version: CALIBRATION_PROFILE_SCHEMA_VERSION,
            kind: CALIBRATION_PROFILE_KIND.to_owned(),
            exporter: "test".to_owned(),
            exporter_version: "0".to_owned(),
            exported_at_unix_seconds: 0,
            collection: CalibrationCollection {
                prometheus_url: "http://prometheus.test/api/v1/query".to_owned(),
                start_unix_seconds: 0,
                end_unix_seconds: 900,
                window_seconds: 900,
                evaluation_unix_seconds: 900,
                match_labels: BTreeMap::new(),
            },
            provenance: CalibrationProvenance {
                deployment_revision: "test".to_owned(),
                architecture: "arm64".to_owned(),
                hardware_profile: "m1-pro metal".to_owned(),
                cpu_cores_per_node: 2,
                renderer_slots_per_node: 3,
                execution_permits_per_node: 2,
                native_render_permits_per_node: 2,
                capture_concurrency: Some(1),
                notes: None,
            },
            histograms,
            warnings: Vec::new(),
        }
    }

    /// Upstream fetch-attempt histogram: `attempts` regular-lane attempts plus
    /// a low-priority refresh series that warmth verification must ignore.
    fn upstream_histogram(attempts: f64) -> CalibrationHistogram {
        let lane = |priority: &str, count: f64| CalibrationSeries {
            labels: BTreeMap::from([
                ("kind".to_owned(), "tile".to_owned()),
                ("priority".to_owned(), priority.to_owned()),
            ]),
            sample_count: count,
            buckets: vec![
                CalibrationBucket {
                    upper_bound_seconds: Some(0.1),
                    count,
                },
                CalibrationBucket {
                    upper_bound_seconds: None,
                    count: 0.0,
                },
            ],
        };
        CalibrationHistogram {
            metric: "biei_mln_resource_upstream_attempt_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![lane("regular", attempts), lane("low", 500.0)],
        }
    }

    fn render_histogram() -> CalibrationHistogram {
        CalibrationHistogram {
            metric: "biei_render_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![
                // Two warm series merge: 30 in (0, 0.05], 45 in (0.05, 0.2],
                // 25 in (0.2, 0.5].
                series(
                    "warm",
                    "tile",
                    &[
                        (Some(0.05), 20.0),
                        (Some(0.2), 40.0),
                        (Some(0.5), 20.0),
                        (None, 0.0),
                    ],
                ),
                series(
                    "warm",
                    "static",
                    &[
                        (Some(0.05), 10.0),
                        (Some(0.2), 5.0),
                        (Some(0.5), 5.0),
                        (None, 0.0),
                    ],
                ),
                series(
                    "cold",
                    "tile",
                    &[(Some(0.5), 5.0), (Some(2.0), 5.0), (None, 0.0)],
                ),
                series(
                    "swap",
                    "tile",
                    &[(Some(0.5), 2.0), (Some(2.0), 2.0), (None, 0.0)],
                ),
            ],
        }
    }

    fn base_costs() -> CostConfig {
        CostConfig {
            style_setup_cost: CostRange::new(
                Duration::from_millis(200),
                Duration::from_millis(300),
            ),
            source_load_cost: CostRange::new(Duration::from_millis(30), Duration::from_millis(70)),
            render_cpu_cost: CostRange::new(Duration::from_millis(5), Duration::from_millis(30)),
            render_resource_cost: CostRange::new(
                Duration::from_millis(1),
                Duration::from_millis(10),
            ),
            first_render_resource_cost: CostRange::new(
                Duration::from_millis(50),
                Duration::from_millis(400),
            ),
            hop_latency: Duration::from_millis(5),
            sla: Duration::from_millis(1_000),
        }
    }

    fn tile_task(scale: Scale, format: ImageFormat, tile_size: u16) -> InternalTask {
        InternalTask {
            id: 1,
            request_id: biei::types::RequestId::from_string("calibration-test"),
            style: biei::types::StyleRevision {
                id: biei::types::StyleId("style-0".to_owned()),
                version: 1,
            },
            source: None,
            request: RenderRequest::Tile {
                z: 10,
                x: 0,
                y: 0,
                tile_size,
            },
            pixel_ratio: scale.into(),
            output_format: format,
            arrived_at: Instant::now(),
            deadline: Instant::now() + Duration::from_secs(1),
            forwarding_hops: 0,
        }
    }

    fn shaped_series(
        labels: &[(&str, &str)],
        upper_bound_seconds: f64,
        count: f64,
    ) -> CalibrationSeries {
        CalibrationSeries {
            labels: labels
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            sample_count: count,
            buckets: vec![
                CalibrationBucket {
                    upper_bound_seconds: Some(upper_bound_seconds),
                    count,
                },
                CalibrationBucket {
                    upper_bound_seconds: None,
                    count: 0.0,
                },
            ],
        }
    }

    #[test]
    fn derives_costs_with_documented_bands_and_fallbacks() {
        let profile = profile(vec![render_histogram()]);
        let base = base_costs();

        let derived = derive_costs(&profile, &base).expect("derive");

        // Warm q10/q25 both land inside the merged (0, 0.05] bucket of 30.
        let cpu = derived.costs.render_cpu_cost;
        assert!(cpu.min < cpu.max && cpu.max <= Duration::from_millis(50));
        // Cold+swap walls reach 2s, so the first-render resource wait must
        // exceed the warm resource wait.
        assert!(
            derived.costs.first_render_resource_cost.max > derived.costs.render_resource_cost.max
        );
        // No setup histograms in the profile: base ranges with notes.
        assert_eq!(
            derived.costs.style_setup_cost.min,
            base.style_setup_cost.min
        );
        assert_eq!(
            derived.costs.style_setup_cost.max,
            base.style_setup_cost.max
        );
        assert_eq!(
            derived.costs.source_load_cost.min,
            base.source_load_cost.min
        );
        assert_eq!(
            derived.costs.source_load_cost.max,
            base.source_load_cost.max
        );
        assert!(
            derived
                .notes
                .iter()
                .any(|note| note.contains("style_setup kept from base"))
        );
        assert!(
            derived
                .notes
                .iter()
                .any(|note| note.contains("collapsed 2 bounded render shapes"))
        );
        // hop latency and SLA always come from the base config.
        assert_eq!(derived.costs.hop_latency, base.hop_latency);
        assert_eq!(derived.costs.sla, base.sla);
    }

    #[test]
    fn calibration_rejects_right_censored_render_timeouts() {
        let timeout_histogram = CalibrationHistogram {
            metric: "biei_render_timeout_lower_bound_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![CalibrationSeries {
                labels: BTreeMap::new(),
                sample_count: 1.0,
                buckets: vec![
                    CalibrationBucket {
                        upper_bound_seconds: Some(5.0),
                        count: 1.0,
                    },
                    CalibrationBucket {
                        upper_bound_seconds: None,
                        count: 0.0,
                    },
                ],
            }],
        };
        let profile = profile(vec![render_histogram(), timeout_histogram]);

        let error = match derive_costs(&profile, &base_costs()) {
            Ok(_) => panic!("successful render samples are incomplete when a timeout was censored"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("right-censored"));
    }

    #[test]
    fn calibration_rejects_empty_render_timeout_instrumentation() {
        let timeout_histogram = CalibrationHistogram {
            metric: "biei_render_timeout_lower_bound_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: Vec::new(),
        };
        let profile = profile(vec![render_histogram(), timeout_histogram]);

        let error = match derive_costs(&profile, &base_costs()) {
            Ok(_) => panic!("an empty family cannot prove that the timeout count was zero"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("missing instrumentation"));
    }

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

    #[test]
    fn empirical_render_sampling_prefers_exact_shape_then_state_fallback() {
        let render = CalibrationHistogram {
            metric: "biei_render_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![
                shaped_series(
                    &[
                        ("render_mode", "tile"),
                        ("scale", "2x"),
                        ("format", "png"),
                        ("size", "le_1024px"),
                        ("state", "warm"),
                    ],
                    0.01,
                    10.0,
                ),
                shaped_series(
                    &[
                        ("render_mode", "static"),
                        ("scale", "1x"),
                        ("format", "webp"),
                        ("size", "le_512px"),
                        ("state", "warm"),
                    ],
                    1.0,
                    20.0,
                ),
            ],
        };
        let model = EmpiricalCostModel::from_profile(&profile(vec![render]));
        let coverage = model.coverage();
        assert_eq!(coverage.render_exact_shapes, 2);
        assert_eq!(coverage.render_state_fallbacks, 1);
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(7);

        let exact = model
            .sample_render(
                &tile_task(Scale::X2, ImageFormat::Png, 512),
                CalibrationRenderState::Warm,
                &mut rng,
            )
            .expect("exact shape sample");
        assert!(exact <= Duration::from_millis(10));

        let fallback = model.sample_render(
            &tile_task(Scale::X1, ImageFormat::Jpeg, 256),
            CalibrationRenderState::Warm,
            &mut rng,
        );
        assert!(fallback.is_some(), "30 warm samples form state fallback");
    }

    #[test]
    fn empirical_setup_sampling_uses_available_partial_families() {
        let style = CalibrationHistogram {
            metric: "biei_style_setup_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![shaped_series(
                &[("render_mode", "tile"), ("scale", "2x"), ("state", "cold")],
                0.2,
                10.0,
            )],
        };
        let source = CalibrationHistogram {
            metric: "biei_source_setup_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![shaped_series(
                &[("render_mode", "tile"), ("scale", "2x")],
                0.05,
                10.0,
            )],
        };
        let model = EmpiricalCostModel::from_profile(&profile(vec![style, source]));
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(11);
        let task = tile_task(Scale::X2, ImageFormat::Png, 512);

        assert!(
            model
                .sample_style_setup(&task, CalibrationRenderState::Cold, &mut rng)
                .is_some()
        );
        assert!(
            model
                .sample_source_setup(RenderMode::Tile, Scale::X2, &mut rng)
                .is_some()
        );
        assert!(
            model
                .sample_render(&task, CalibrationRenderState::Warm, &mut rng)
                .is_none()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stub_renderer_uses_shape_conditioned_render_distribution() {
        let task = tile_task(Scale::X2, ImageFormat::Png, 512);
        let render = CalibrationHistogram {
            metric: "biei_render_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![shaped_series(
                &[
                    ("render_mode", "tile"),
                    ("scale", "2x"),
                    ("format", "png"),
                    ("size", "le_1024px"),
                    ("state", "cold"),
                ],
                0.1,
                10.0,
            )],
        };
        let model = EmpiricalCostModel::from_profile(&profile(vec![render]));
        let mut renderer = StubRenderer::new(
            CostRange::fixed(Duration::ZERO),
            CostRange::fixed(Duration::ZERO),
            CostRange::fixed(Duration::from_secs(5)),
            CostRange::fixed(Duration::from_secs(5)),
            CostRange::fixed(Duration::ZERO),
            std::sync::Arc::new(Semaphore::new(1)),
            19,
        )
        .with_calibration_model(Some(std::sync::Arc::new(model)));

        renderer
            .setup_profile(&task, None)
            .await
            .expect("setup profile");
        let started = Instant::now();
        renderer.render(&task).await.expect("render");

        assert!(started.elapsed() <= Duration::from_millis(100));
    }

    #[test]
    fn two_window_fusion_takes_cpu_from_reference_and_walls_from_traffic() {
        // Reference: verified-warm (0 regular fetches), walls all in (0, 0.05].
        let reference_render = CalibrationHistogram {
            metric: "biei_render_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![
                series("warm", "tile", &[(Some(0.05), 80.0), (None, 0.0)]),
                series("warm", "static", &[(Some(0.05), 20.0), (None, 0.0)]),
            ],
        };
        let reference = profile(vec![reference_render, upstream_histogram(0.0)]);
        // Traffic: heavily I/O-contaminated (400 fetches / 114 renders) —
        // rejected alone, but valid as the resource-wait source in fusion.
        let traffic = profile(vec![render_histogram(), upstream_histogram(400.0)]);
        let base = base_costs();

        let partial = derive_costs(&traffic, &base).expect("unsafe render stage falls back");
        assert!(matches!(
            partial.coverage.render_cpu.source,
            CalibrationValueSource::Default
        ));
        let derived = derive_costs_with_cpu_reference(&traffic, &reference, &base).expect("fusion");

        // CPU comes from the reference window's representative band (≤ 50ms),
        // not the traffic window's contaminated walls.
        assert!(derived.costs.render_cpu_cost.max <= Duration::from_millis(50));
        // Resource waits reflect traffic walls (up to 0.5s warm / 2s first)
        // minus the small reference CPU.
        assert!(derived.costs.render_resource_cost.max >= Duration::from_millis(300));
        assert!(
            derived.costs.first_render_resource_cost.max > derived.costs.render_resource_cost.max
        );
        assert!(
            derived
                .notes
                .iter()
                .any(|note| note.contains("verified resource-warm, shape-conditioned"))
        );
        assert_eq!(derived.sampling_model.coverage().render_cpu_exact_shapes, 2);
        assert!(
            derived
                .notes
                .iter()
                .any(|note| note.contains("traffic window saw"))
        );
    }

    #[test]
    fn fusion_rejects_unverified_reference_and_hardware_mismatch() {
        let clean_render = CalibrationHistogram {
            metric: "biei_render_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![
                series("warm", "tile", &[(Some(0.05), 80.0), (None, 0.0)]),
                series("warm", "static", &[(Some(0.05), 20.0), (None, 0.0)]),
            ],
        };
        let traffic = profile(vec![render_histogram(), upstream_histogram(400.0)]);
        let base = base_costs();

        let mut concurrent_reference = profile(vec![clean_render.clone(), upstream_histogram(0.0)]);
        concurrent_reference.provenance.capture_concurrency = Some(4);
        let Err(err) = derive_costs_with_cpu_reference(&traffic, &concurrent_reference, &base)
        else {
            panic!("a contended service-wall reference must not become CPU demand");
        };
        assert!(err.to_string().contains("capture_concurrency=1"));

        // Reference without upstream evidence cannot prove warmth.
        let unverified = profile(vec![clean_render.clone()]);
        assert!(derive_costs_with_cpu_reference(&traffic, &unverified, &base).is_err());

        // An exporter snapshot contains optional histogram entries even when
        // Prometheus returned no series. That is absence of evidence, not a
        // verified zero-fetch window.
        let empty_upstream = CalibrationHistogram {
            metric: "biei_mln_resource_upstream_attempt_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: Vec::new(),
        };
        let empty_evidence = profile(vec![clean_render.clone(), empty_upstream]);
        let Err(err) = derive_costs_with_cpu_reference(&traffic, &empty_evidence, &base) else {
            panic!("empty upstream evidence must fail");
        };
        assert!(err.to_string().contains("empty upstream fetch histogram"));

        // Reference with render-blocking fetches is not a CPU measurement.
        let contaminated = profile(vec![clean_render.clone(), upstream_histogram(50.0)]);
        assert!(derive_costs_with_cpu_reference(&traffic, &contaminated, &base).is_err());

        // Reference from different hardware must not be subtracted.
        let mut foreign = profile(vec![clean_render, upstream_histogram(0.0)]);
        foreign.provenance.hardware_profile = "different-machine".to_owned();
        let Err(err) = derive_costs_with_cpu_reference(&traffic, &foreign, &base) else {
            panic!("hardware mismatch must fail");
        };
        assert!(err.to_string().contains("different machines"));

        // Same hardware is insufficient when a different renderer revision or
        // concurrency shape produced the service-wall reference.
        let verified = profile(vec![
            CalibrationHistogram {
                metric: "biei_render_duration_seconds".to_owned(),
                unit: "seconds".to_owned(),
                query: "test".to_owned(),
                series: vec![series("warm", "tile", &[(Some(0.05), 100.0), (None, 0.0)])],
            },
            upstream_histogram(0.0),
        ]);
        let mut foreign_revision = verified.clone();
        foreign_revision.provenance.deployment_revision = "other-revision".to_owned();
        let Err(err) = derive_costs_with_cpu_reference(&traffic, &foreign_revision, &base) else {
            panic!("renderer revision mismatch must fail");
        };
        assert!(err.to_string().contains("renderer revisions"));

        let mut foreign_shape = verified;
        foreign_shape.provenance.native_render_permits_per_node = 1;
        let Err(err) = derive_costs_with_cpu_reference(&traffic, &foreign_shape, &base) else {
            panic!("node-shape mismatch must fail");
        };
        assert!(err.to_string().contains("node shape"));
    }

    #[test]
    fn fusion_rejects_cross_shape_cpu_subtraction() {
        let reference_render = CalibrationHistogram {
            metric: "biei_render_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![shaped_series(
                &[
                    ("render_mode", "tile"),
                    ("scale", "1x"),
                    ("format", "png"),
                    ("size", "le_256px"),
                    ("state", "warm"),
                ],
                0.02,
                100.0,
            )],
        };
        let traffic_render = CalibrationHistogram {
            metric: "biei_render_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![shaped_series(
                &[
                    ("render_mode", "static"),
                    ("scale", "2x"),
                    ("format", "webp"),
                    ("size", "le_2048px"),
                    ("state", "warm"),
                ],
                0.5,
                100.0,
            )],
        };
        let reference = profile(vec![reference_render, upstream_histogram(0.0)]);
        let traffic = profile(vec![traffic_render, upstream_histogram(100.0)]);

        let Err(error) = derive_costs_with_cpu_reference(&traffic, &reference, &base_costs())
        else {
            panic!("a CPU profile for another render shape is not calibration evidence");
        };
        assert!(
            error
                .to_string()
                .contains("does not cover traffic render shape")
        );
    }

    #[test]
    fn contaminated_render_stage_falls_back_and_hot_window_is_verified() {
        // 114 renders (100 warm + 14 first) with 400 render-blocking fetches
        // during the window: warm walls contain provider I/O, not CPU demand.
        let contaminated = profile(vec![render_histogram(), upstream_histogram(400.0)]);
        let base = base_costs();
        let derived = derive_costs(&contaminated, &base).expect("partial calibration survives");
        assert_eq!(derived.costs.render_cpu_cost.min, base.render_cpu_cost.min);
        assert!(
            derived
                .notes
                .iter()
                .any(|note| note.contains("warm render ignored"))
        );

        // 2 render-blocking fetches over 114 renders: verified warm. The 500
        // low-priority background refreshes must not count against warmth.
        let hot = profile(vec![render_histogram(), upstream_histogram(2.0)]);
        let derived = derive_costs(&hot, &base_costs()).expect("hot window derives");
        assert!(
            derived
                .notes
                .iter()
                .any(|note| note.contains("verified resource-cache-warm"))
        );

        // No upstream histogram at all: setup stages may still derive, but
        // warm render walls cannot be promoted to CPU evidence.
        let unverifiable = profile(vec![render_histogram()]);
        let base = base_costs();
        let derived = derive_costs(&unverifiable, &base).expect("partial derivation survives");
        assert_eq!(derived.costs.render_cpu_cost.min, base.render_cpu_cost.min);
        assert!(
            derived
                .notes
                .iter()
                .any(|note| note.contains("lacks upstream fetch instrumentation"))
        );

        // An empty exported family is equally unverifiable: it may mean the
        // current zero series was never initialized on the captured pods.
        let empty = CalibrationHistogram {
            metric: "biei_mln_resource_upstream_attempt_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: Vec::new(),
        };
        let empty_evidence = profile(vec![render_histogram(), empty]);
        let derived =
            derive_costs(&empty_evidence, &base).expect("other partial stages remain usable");
        assert_eq!(derived.costs.render_cpu_cost.min, base.render_cpu_cost.min);
        assert!(
            derived
                .notes
                .iter()
                .any(|note| note.contains("empty upstream fetch histogram"))
        );
    }

    #[test]
    fn sparse_warm_evidence_falls_back_without_discarding_the_profile() {
        let sparse = profile(vec![CalibrationHistogram {
            metric: "biei_render_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![series("warm", "tile", &[(Some(0.1), 5.0), (None, 0.0)])],
        }]);

        let base = base_costs();
        let derived = derive_costs(&sparse, &base).expect("partial profile derives");
        assert_eq!(derived.costs.render_cpu_cost.min, base.render_cpu_cost.min);
        assert_eq!(
            derived.costs.render_resource_cost.max,
            base.render_resource_cost.max
        );
        assert!(matches!(
            derived.coverage.render_cpu.source,
            CalibrationValueSource::Default
        ));
        assert!(derived.notes.iter().any(|note| note.contains("5 samples")));
    }

    #[test]
    fn setup_only_profile_calibrates_available_stages() {
        let setup = |metric: &str, count: f64| CalibrationHistogram {
            metric: metric.to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![CalibrationSeries {
                labels: BTreeMap::new(),
                sample_count: count,
                buckets: vec![
                    CalibrationBucket {
                        upper_bound_seconds: Some(0.1),
                        count,
                    },
                    CalibrationBucket {
                        upper_bound_seconds: None,
                        count: 0.0,
                    },
                ],
            }],
        };
        let partial = profile(vec![
            setup("biei_style_setup_duration_seconds", 20.0),
            setup("biei_source_setup_duration_seconds", 12.0),
        ]);
        let base = base_costs();

        let derived = derive_costs(&partial, &base).expect("setup-only profile derives");

        assert!(matches!(
            derived.coverage.style_setup.source,
            CalibrationValueSource::Measured
        ));
        assert_eq!(derived.coverage.style_setup.samples, 20.0);
        assert!(matches!(
            derived.coverage.source_load.source,
            CalibrationValueSource::Measured
        ));
        assert!(matches!(
            derived.coverage.render_cpu.source,
            CalibrationValueSource::Default
        ));
        assert_eq!(derived.costs.render_cpu_cost.min, base.render_cpu_cost.min);
    }

    #[test]
    fn load_rejects_wrong_kind_and_schema() {
        let dir = std::env::temp_dir();

        let mut wrong_kind = profile(vec![render_histogram()]);
        wrong_kind.kind = "something-else".to_owned();
        let path = dir.join("biei-sim-import-kind-test.json");
        std::fs::write(&path, serde_json::to_vec(&wrong_kind).expect("json")).expect("write");
        assert!(load_calibration_profile(&path).is_err());
        let _ = std::fs::remove_file(&path);

        let mut wrong_schema = profile(vec![render_histogram()]);
        wrong_schema.schema_version = 999;
        let path = dir.join("biei-sim-import-schema-test.json");
        std::fs::write(&path, serde_json::to_vec(&wrong_schema).expect("json")).expect("write");
        assert!(load_calibration_profile(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn profile_provenance_replaces_unrelated_simulator_node_defaults() {
        let profile = profile(vec![render_histogram()]);
        let mut config = SimConfig::default();
        assert_ne!(config.cpu_cores_per_node, 2);

        apply_profile_provenance(&profile, &mut config).expect("apply provenance");

        assert_eq!(config.cpu_cores_per_node, 2);
        assert_eq!(config.cluster.renderer_slots_per_node, 3);
        assert_eq!(config.cluster.render_permits_per_node, Some(2));
        assert_eq!(config.cluster.cpu_render_permits_per_node, Some(2));
    }
}
