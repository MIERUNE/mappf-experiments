//! `Renderer` trait — profile setup / source / render worker hooks.

pub mod actor;
pub mod maplibre;
pub(crate) mod overlay;

use async_trait::async_trait;
use std::sync::Arc;
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
    async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError>;
    /// Stop using the current native actor after its in-flight command returns.
    ///
    /// Implementations should not try to kill an in-flight native render. The
    /// intended behavior is to mark the actor as retiring, let the blocking
    /// call return naturally, then drop/recreate the backend before accepting
    /// more work.
    fn retire_after_current(&mut self) {}
}

pub type BoxRenderer = Box<dyn Renderer + Send>;
