//! `Renderer` trait — profile setup / source / render worker hooks.

pub mod actor;
pub(crate) mod file_source;
pub(crate) mod http_fetch;
pub mod maplibre;
pub(crate) mod overlay;

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use crate::types::{
    AddLayerSource, InternalTask, RenderOutput, RendererError, SourceHash, StyleRevision,
};

#[derive(Clone, Debug)]
pub struct PreparedProfile {
    pub revision: StyleRevision,
    pub style_json: Arc<str>,
    /// Request-local addlayer source after resolving its TileJSON into a
    /// concrete style-spec source with `tiles`. Kept here because preparers
    /// run before worker admission; workers may be warm and skip style setup,
    /// but still need this per-request source for render.
    pub addlayer_source: Option<AddLayerSource>,
}

#[derive(Debug, Clone)]
pub struct RendererOutput {
    pub output: RenderOutput,
    /// Time spent constructing and installing a request-local source in the
    /// renderer. `None` means that no source setup was needed.
    pub source_setup_duration: Option<Duration>,
}

impl From<RenderOutput> for RendererOutput {
    fn from(output: RenderOutput) -> Self {
        Self {
            output,
            source_setup_duration: None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum StyleAvailabilityError {
    NotFound(RendererError),
    Unavailable(RendererError),
}

#[async_trait]
pub trait ProfilePreparer: Send + Sync {
    /// Fetch and validate profile data before a task enters a renderer slot.
    async fn prepare_profile(
        &self,
        _task: &InternalTask,
    ) -> Result<Option<PreparedProfile>, RendererError> {
        Ok(None)
    }

    /// Verify that a style is actually fetchable (i.e. the provider has it),
    /// reusing the same fetch / cache / single-flight / negative-cache path as
    /// [`prepare_profile`](Self::prepare_profile). The preview endpoint uses
    /// this to return 404 for styles that merely *resolve* in the catalog (e.g.
    /// via a URL template, which accepts any id) but do not exist at the
    /// provider. The default assumes availability — preparers that don't fetch
    /// (e.g. the simulator stub) have nothing to check.
    async fn ensure_style_available(
        &self,
        _revision: &StyleRevision,
        _deadline: Instant,
    ) -> Result<(), StyleAvailabilityError> {
        Ok(())
    }

    /// Temporarily suppress a revision that fetched successfully but was
    /// rejected by the renderer's semantic style validation. The default is
    /// a no-op for preparers without a style cache.
    fn mark_style_load_failed(&self, _revision: &StyleRevision) {}
}

#[derive(Default)]
pub struct NoopProfilePreparer;

#[async_trait]
impl ProfilePreparer for NoopProfilePreparer {}

#[async_trait]
pub trait Renderer: Send + Sync {
    /// Apply the task's full worker profile to the renderer: style revision,
    /// render mode, and scale. Version mismatch means the style was updated
    /// and a fresh load is required.
    async fn setup_profile(
        &mut self,
        task: &InternalTask,
        prepared: Option<PreparedProfile>,
    ) -> Result<(), RendererError>;
    /// Load the source identified by `hash`. Sources are the expensive thing
    /// (geometry parse / index build); style application is cheap and is
    /// folded into `render`.
    async fn ensure_source(&mut self, hash: SourceHash) -> Result<(), RendererError>;
    async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError>;
    /// Stop using the current native actor after a caller-side timeout.
    ///
    /// Implementations must not try to kill an in-flight native render. They
    /// may detach it under a bounded orphan budget and immediately install a
    /// replacement so one wedged native call cannot permanently consume the
    /// renderer slot.
    fn retire_after_current(&mut self) {}

    /// Attempt to restore an actor that could not be replaced at timeout time.
    /// Called periodically even when no requests are admitted, so readiness
    /// recovery never depends on a new task reaching the worker.
    fn repair_if_needed(&mut self) -> Result<bool, RendererError> {
        Ok(false)
    }
}

pub type BoxRenderer = Box<dyn Renderer + Send>;
