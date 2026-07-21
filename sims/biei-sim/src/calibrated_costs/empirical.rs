use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use rand::Rng;
use serde::Serialize;

use biei_core::types::{ImageFormat, InternalTask, RenderMode, RenderRequest, Scale};

use super::histogram::{MergedHistogram, histogram, merge_series};
use super::{MIN_OPTIONAL_SAMPLES, MIN_WARM_RENDER_SAMPLES};
use crate::calibration::{
    CALIBRATION_PROFILE_KIND, CALIBRATION_PROFILE_SCHEMA_VERSION, CalibrationProfile,
};

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
pub(super) struct RenderShapeKey {
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

    pub(super) fn add_cpu_reference(&mut self, profile: &CalibrationProfile) {
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

pub(super) fn render_shape_from_labels(
    labels: &BTreeMap<String, String>,
) -> Option<RenderShapeKey> {
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

pub(super) fn render_state_from_labels(
    labels: &BTreeMap<String, String>,
) -> Option<CalibrationRenderState> {
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
