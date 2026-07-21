//! Derive simulator cost ranges and empirical runtime samples from exported
//! calibration profiles.
//!
//! Profiles retain raw, provenance-bearing histograms. Modeling approximations
//! are applied only during import: a verified resource-warm reference supplies
//! a service-wall CPU proxy, while realistic-traffic walls supply modeled
//! in-render resource waits.

mod derivation;
mod empirical;
mod histogram;
mod provenance;

pub use derivation::{
    CalibratedCosts, CalibrationCoverage, CalibrationStageCoverage, CalibrationValueSource,
    derive_costs_with_cpu_reference,
};
pub use empirical::{
    CalibrationRenderState, EmpiricalCostModel, EmpiricalSamplingCoverage, load_calibration_profile,
};
pub use provenance::apply_profile_provenance;

/// Below this many warm-render samples the profile is not calibration evidence.
pub(super) const MIN_WARM_RENDER_SAMPLES: f64 = 30.0;
/// Sparse optional histograms fall back to the base configuration.
pub(super) const MIN_OPTIONAL_SAMPLES: f64 = 10.0;

#[cfg(test)]
use derivation::{derive_with_cpu_reference, ensure_uncensored_render_tail};

#[cfg(test)]
mod tests;
