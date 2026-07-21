//! Server-side renderer modules: the MapLibre-backed `Renderer`/`ProfilePreparer`
//! implementations and the renderer-actor supervisor.
//!
//! Re-exports the `Renderer` trait and shared renderer types from `biei-core`
//! so server code can keep writing `crate::renderer::Renderer` while the
//! MapLibre-dependent submodules live here in the binary crate.

pub(crate) use biei_core::renderer::*;

pub(crate) mod actor;
pub(crate) mod maplibre;
pub(crate) mod overlay;

pub(crate) const RESOURCE_USER_AGENT: &str = concat!("biei/", env!("CARGO_PKG_VERSION"));
