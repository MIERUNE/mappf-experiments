//! HTTP protocol contracts shared across MMPF services.

pub mod cache_control;
pub mod content_type;
pub mod operational;
pub mod request_id;
#[cfg(feature = "serve")]
pub mod serve;
