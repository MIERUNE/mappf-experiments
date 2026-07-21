//! Service-independent primitives shared across MMPF services.
//!
//! Admission rule: a module belongs here only if it (1) carries no domain
//! types, (2) is service-agnostic, and (3) already has two or more consuming
//! crates. Single-consumer or domain-coupled code stays in its owning crate —
//! this crate is a foundation of shared primitives, not a catch-all.

pub mod metrics;
pub mod path;
pub mod resource_templates;
pub mod rng;
pub mod singleflight;
pub mod sync;
