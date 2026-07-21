//! HTTP-backed MapLibre Native `FileSource` integration.
//!
//! The crate owns resource HTTP safety, cache freshness, retry and
//! single-flight behavior, provider-health evidence, and process-global
//! registration. Renderer scheduling and application routing remain with the
//! caller.

pub mod http;
pub mod policy;
mod source;

pub use source::{
    FileSourceIoPermits, ProviderHealthTracker, ProviderRetryGuard, build_profile_http_client,
    gather_metrics, provider_health, register_file_sources,
};
