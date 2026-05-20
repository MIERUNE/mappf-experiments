//! Dedicated blocking renderer actor for production MapLibre integration.
//!
//! MapLibre Native rendering is treated as thread-affine blocking work. This
//! actor owns the backend on one OS thread and exposes async request/reply
//! methods to worker tasks.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::thread;

use tokio::sync::oneshot;
use tokio::time::Instant;

mod addlayer;
mod camera;
mod encode;

use super::overlay::{OverlaySlotPool, build_overlay_geojson, populate_static_slots};
use addlayer::{AddLayerSourceCache, render_static_with_overlays_and_addlayer};
use camera::{auto_padding_for_overlays, padding_to_edge_insets};
use encode::encode_image;
#[cfg(test)]
use encode::rgba_to_rgb_on_white;

use crate::types::{
    ImageFormat, InternalTask, PixelRatio, RenderOutput, RenderRequest, RendererError, SourceRef,
    StyleRevision, TaskId, WorkerId,
};

const ACTOR_JOIN_GRACE: std::time::Duration = std::time::Duration::from_millis(10);

#[derive(Clone, Debug)]
pub struct RendererActorConfig {
    pub worker_id: WorkerId,
    pub ambient_cache_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct ResolvedStyle {
    pub revision: StyleRevision,
    pub style_json: Arc<str>,
}

#[derive(Clone, Debug)]
pub struct RenderTaskView {
    pub id: TaskId,
    pub style: StyleRevision,
    pub source: Option<SourceRef>,
    pub request: RenderRequest,
    pub pixel_ratio: PixelRatio,
    pub output_format: ImageFormat,
    pub deadline: Instant,
}

impl From<&InternalTask> for RenderTaskView {
    fn from(task: &InternalTask) -> Self {
        Self {
            id: task.id,
            style: task.style.clone(),
            source: task.source.clone(),
            request: task.request.clone(),
            pixel_ratio: task.pixel_ratio,
            output_format: task.output_format,
            deadline: task.deadline,
        }
    }
}

/// Blocking renderer implementation owned by `RendererActor`'s OS thread.
///
/// Keeping the backend synchronous makes thread affinity explicit and prevents
/// accidental calls from tokio worker tasks.
pub trait BlockingRenderBackend: 'static {
    fn load_profile(
        &mut self,
        style: &ResolvedStyle,
        task: &RenderTaskView,
    ) -> Result<(), RendererError>;
    fn render(&mut self, task: &RenderTaskView) -> Result<RenderOutput, RendererError>;
    fn error_invalidates_loaded_state(&self, _err: &RendererError) -> bool {
        true
    }
    fn reset(&mut self) {}
}

pub struct RendererActor {
    tx: mpsc::Sender<RenderCmd>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

enum RenderCmd {
    LoadProfile {
        style: ResolvedStyle,
        task: RenderTaskView,
        reply: oneshot::Sender<Result<(), RendererError>>,
    },
    Render {
        task: RenderTaskView,
        reply: oneshot::Sender<Result<RenderOutput, RendererError>>,
    },
    Retire,
    Shutdown,
}

impl RendererActor {
    pub fn spawn(config: RendererActorConfig) -> Result<Self, RendererError> {
        let ambient_cache_path = config.ambient_cache_path.clone();
        Self::spawn_with_backend_factory(config, move || {
            MapLibreNativeBackend::new(ambient_cache_path)
        })
    }

    pub fn spawn_with_backend<B>(
        config: RendererActorConfig,
        backend: B,
    ) -> Result<Self, RendererError>
    where
        B: BlockingRenderBackend + Send,
    {
        Self::spawn_with_backend_factory(config, || backend)
    }

    fn spawn_with_backend_factory<F, B>(
        config: RendererActorConfig,
        backend_factory: F,
    ) -> Result<Self, RendererError>
    where
        F: FnOnce() -> B + Send + 'static,
        B: BlockingRenderBackend,
    {
        let (tx, rx) = mpsc::channel();
        let thread = thread::Builder::new()
            .name(format!("biei-renderer-{}", config.worker_id))
            .spawn(move || run_actor(rx, backend_factory()))
            .map_err(|err| {
                RendererError::RenderFailed(format!("failed to spawn renderer actor: {err}"))
            })?;

        Ok(Self {
            tx,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub fn is_alive(&self) -> bool {
        let Ok(thread) = self.thread.lock() else {
            return false;
        };
        thread.as_ref().is_some_and(|t| !t.is_finished())
    }

    pub async fn load_profile(
        &self,
        style: ResolvedStyle,
        task: RenderTaskView,
    ) -> Result<(), RendererError> {
        let (reply, rx) = oneshot::channel();
        let deadline = task.deadline;
        self.tx
            .send(RenderCmd::LoadProfile { style, task, reply })
            .map_err(|_| RendererError::ActorDead)?;
        await_actor_reply(deadline, rx).await
    }

    pub async fn render(&self, task: RenderTaskView) -> Result<RenderOutput, RendererError> {
        let (reply, rx) = oneshot::channel();
        let deadline = task.deadline;
        self.tx
            .send(RenderCmd::Render { task, reply })
            .map_err(|_| RendererError::ActorDead)?;
        await_actor_reply(deadline, rx).await
    }

    pub fn retire_after_current(&self) {
        let _ = self.tx.send(RenderCmd::Retire);
    }
}

impl Drop for RendererActor {
    fn drop(&mut self) {
        let _ = self.tx.send(RenderCmd::Shutdown);
        let Ok(mut thread) = self.thread.lock() else {
            return;
        };
        if let Some(thread) = thread.take() {
            let join_deadline = std::time::Instant::now() + ACTOR_JOIN_GRACE;
            while !thread.is_finished() && std::time::Instant::now() < join_deadline {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            if thread.is_finished() {
                let _ = thread.join();
            } else {
                tracing::warn!(
                    "renderer actor thread did not stop promptly; detaching to avoid blocking shutdown"
                );
            }
        }
    }
}

async fn await_actor_reply<T>(
    deadline: Instant,
    rx: oneshot::Receiver<Result<T, RendererError>>,
) -> Result<T, RendererError> {
    match tokio::time::timeout_at(deadline, rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(RendererError::ActorDead),
        Err(_) => Err(RendererError::Timeout),
    }
}

fn run_actor<B>(rx: mpsc::Receiver<RenderCmd>, mut backend: B)
where
    B: BlockingRenderBackend,
{
    let mut loaded: Option<StyleRevision> = None;

    while let Ok(cmd) = rx.recv() {
        match cmd {
            RenderCmd::LoadProfile { style, task, reply } => {
                let revision = style.revision.clone();
                let result =
                    catch_backend_unwind("load_profile", || backend.load_profile(&style, &task));
                if result.is_ok() {
                    loaded = Some(revision);
                } else {
                    loaded = None;
                    reset_backend_after_error(&mut backend);
                }
                let _ = reply.send(result);
            }
            RenderCmd::Render { task, reply } => {
                let result = if loaded.as_ref() == Some(&task.style) {
                    catch_backend_unwind("render", || backend.render(&task))
                } else {
                    Err(RendererError::StyleNotReady {
                        style_id: task.style.id.clone(),
                        version: task.style.version,
                    })
                };
                let panicked = result.as_ref().is_err_and(renderer_error_is_actor_panic);
                if panicked
                    || result
                        .as_ref()
                        .is_err_and(|err| backend.error_invalidates_loaded_state(err))
                {
                    loaded = None;
                    reset_backend_after_error(&mut backend);
                }
                let _ = reply.send(result);
            }
            RenderCmd::Retire => break,
            RenderCmd::Shutdown => break,
        }
    }
}

fn reset_backend_after_error<B>(backend: &mut B)
where
    B: BlockingRenderBackend,
{
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| backend.reset())) {
        let message = panic_payload_message(&payload);
        tracing::error!(
            panic = %message,
            "renderer actor backend reset panicked; keeping actor alive with cleared warm state"
        );
    }
}

fn catch_backend_unwind<T>(
    operation: &'static str,
    f: impl FnOnce() -> Result<T, RendererError>,
) -> Result<T, RendererError> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let message = panic_payload_message(&payload);
            tracing::error!(
                operation,
                panic = %message,
                "renderer actor backend panicked; invalidating loaded state"
            );
            Err(RendererError::RenderFailed(format!(
                "renderer actor panicked during {operation}: {message}"
            )))
        }
    }
}

fn renderer_error_is_actor_panic(err: &RendererError) -> bool {
    matches!(err, RendererError::RenderFailed(message) if message.starts_with("renderer actor panicked during "))
}

fn panic_payload_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

struct MapLibreNativeBackend {
    loaded_style: Option<ResolvedStyle>,
    active_renderer: Option<ActiveRenderer>,
    ambient_cache_path: Option<PathBuf>,
}

enum ActiveRenderer {
    Static {
        key: RendererKey,
        loaded_style: Option<StyleRevision>,
        observer_state: ObserverState,
        renderer: maplibre_native::ImageRenderer<maplibre_native::Static>,
        /// Pre-allocated overlay slots (style-setup-time fixed). Per-request
        /// overlay rendering only updates each slot's GeoJSON source via
        /// `source_mut(...).set_geojson(...)` and never adds/removes layers,
        /// so per-request expression-compile cost is paid once at style load.
        slots: OverlaySlotPool,
        /// Stable request-local addlayer sources kept on the loaded style.
        /// Request-local layers are removed after each render; unreferenced
        /// sources are harmless and let repeated tilesets avoid add_source.
        addlayer_sources: AddLayerSourceCache,
    },
    Tile {
        key: RendererKey,
        loaded_style: Option<StyleRevision>,
        observer_state: ObserverState,
        renderer: maplibre_native::ImageRenderer<maplibre_native::Tile>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RendererKey {
    render_mode: crate::types::RenderMode,
    pixel_ratio_bits: u32,
}

impl RendererKey {
    fn new(render_mode: crate::types::RenderMode, pixel_ratio: PixelRatio) -> Self {
        Self {
            render_mode,
            pixel_ratio_bits: pixel_ratio.as_f32().to_bits(),
        }
    }

    fn pixel_ratio(self) -> f32 {
        f32::from_bits(self.pixel_ratio_bits)
    }
}

impl MapLibreNativeBackend {
    fn new(ambient_cache_path: Option<PathBuf>) -> Self {
        Self {
            loaded_style: None,
            active_renderer: None,
            ambient_cache_path,
        }
    }

    fn style(&self) -> Result<&ResolvedStyle, RendererError> {
        self.loaded_style.as_ref().ok_or_else(|| {
            RendererError::RenderFailed("style has not been loaded in renderer backend".to_string())
        })
    }

    fn ensure_static_renderer(
        &mut self,
        key: RendererKey,
        size: maplibre_native::Size,
        deadline: Instant,
    ) -> Result<
        (
            &mut maplibre_native::ImageRenderer<maplibre_native::Static>,
            &mut OverlaySlotPool,
            &mut AddLayerSourceCache,
        ),
        RendererError,
    > {
        let style = self.style()?.clone();
        let needs_rebuild = !matches!(
            self.active_renderer,
            Some(ActiveRenderer::Static { key: existing, .. }) if existing == key
        );
        if needs_rebuild {
            let mut renderer = build_renderer(key, size, self.ambient_cache_path.as_deref())?
                .build_static_renderer();
            let observer_state = ObserverState::default();
            install_map_observer(&mut renderer, observer_state.clone());
            load_style_json(&mut renderer, &style, &observer_state, deadline)?;
            let slots = populate_static_slots(&mut renderer).map_err(|err| {
                RendererError::StyleLoadFailed {
                    style_id: style.revision.id.clone(),
                    source: err.to_string(),
                }
            })?;
            self.active_renderer = Some(ActiveRenderer::Static {
                key,
                loaded_style: Some(style.revision.clone()),
                observer_state,
                renderer,
                slots,
                addlayer_sources: AddLayerSourceCache::new(),
            });
        }
        let Some(ActiveRenderer::Static {
            loaded_style,
            observer_state,
            renderer,
            slots,
            addlayer_sources,
            ..
        }) = self.active_renderer.as_mut()
        else {
            unreachable!("static renderer was inserted")
        };
        if loaded_style.as_ref() != Some(&style.revision) {
            load_style_json(renderer, &style, observer_state, deadline)?;
            *loaded_style = Some(style.revision.clone());
            *slots =
                populate_static_slots(renderer).map_err(|err| RendererError::StyleLoadFailed {
                    style_id: style.revision.id.clone(),
                    source: err.to_string(),
                })?;
            *addlayer_sources = AddLayerSourceCache::new();
        }
        renderer.set_map_size(size);
        Ok((renderer, slots, addlayer_sources))
    }

    fn ensure_tile_renderer(
        &mut self,
        key: RendererKey,
        size: maplibre_native::Size,
        deadline: Instant,
    ) -> Result<&mut maplibre_native::ImageRenderer<maplibre_native::Tile>, RendererError> {
        let style = self.style()?.clone();
        let needs_rebuild = !matches!(
            self.active_renderer,
            Some(ActiveRenderer::Tile { key: existing, .. }) if existing == key
        );
        if needs_rebuild {
            let mut renderer = build_renderer(key, size, self.ambient_cache_path.as_deref())?
                .build_tile_renderer();
            let observer_state = ObserverState::default();
            install_map_observer(&mut renderer, observer_state.clone());
            load_style_json(&mut renderer, &style, &observer_state, deadline)?;
            self.active_renderer = Some(ActiveRenderer::Tile {
                key,
                loaded_style: Some(style.revision.clone()),
                observer_state,
                renderer,
            });
        }
        let Some(ActiveRenderer::Tile {
            loaded_style,
            observer_state,
            renderer,
            ..
        }) = self.active_renderer.as_mut()
        else {
            unreachable!("tile renderer was inserted")
        };
        if loaded_style.as_ref() != Some(&style.revision) {
            load_style_json(renderer, &style, observer_state, deadline)?;
            *loaded_style = Some(style.revision.clone());
        }
        renderer.set_map_size(size);
        Ok(renderer)
    }

    fn ensure_renderer_for_task(&mut self, task: &RenderTaskView) -> Result<(), RendererError> {
        match task.request {
            RenderRequest::Tile { tile_size, .. } => {
                let key = RendererKey::new(crate::types::RenderMode::Tile, task.pixel_ratio);
                let size = render_size(tile_size, tile_size)?;
                self.ensure_tile_renderer(key, size, task.deadline)?;
            }
            RenderRequest::StaticImage { width, height, .. } => {
                let key = RendererKey::new(crate::types::RenderMode::Static, task.pixel_ratio);
                let size = render_size(width, height)?;
                self.ensure_static_renderer(key, size, task.deadline)?;
            }
        }
        Ok(())
    }

    fn reset_loaded_state(&mut self) {
        self.loaded_style = None;
        match self.active_renderer.as_mut() {
            Some(ActiveRenderer::Static { loaded_style, .. })
            | Some(ActiveRenderer::Tile { loaded_style, .. }) => {
                *loaded_style = None;
            }
            None => {}
        }
    }
}

#[derive(Clone, Default)]
struct ObserverState {
    inner: Arc<Mutex<ObserverFlags>>,
}

#[derive(Default)]
struct ObserverFlags {
    style_loaded: bool,
    failure: Option<MapLoadFailure>,
}

#[derive(Clone, Debug)]
struct MapLoadFailure {
    error: maplibre_native::MapLoadError,
}

struct ObserverSnapshot {
    style_loaded: bool,
    failure: Option<MapLoadFailure>,
}

impl ObserverState {
    fn start_loading(&self) {
        let mut flags = lock_observer_flags(&self.inner);
        flags.style_loaded = false;
        flags.failure = None;
    }

    fn finish_loading_style(&self) {
        let mut flags = lock_observer_flags(&self.inner);
        flags.style_loaded = true;
    }

    fn fail_loading_map(&self, error: maplibre_native::MapLoadError) {
        let mut flags = lock_observer_flags(&self.inner);
        flags.style_loaded = false;
        flags.failure = Some(MapLoadFailure { error });
    }

    fn snapshot(&self) -> ObserverSnapshot {
        let flags = lock_observer_flags(&self.inner);
        ObserverSnapshot {
            style_loaded: flags.style_loaded,
            failure: flags.failure.clone(),
        }
    }
}

fn lock_observer_flags(mutex: &Mutex<ObserverFlags>) -> MutexGuard<'_, ObserverFlags> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl BlockingRenderBackend for MapLibreNativeBackend {
    fn load_profile(
        &mut self,
        style: &ResolvedStyle,
        task: &RenderTaskView,
    ) -> Result<(), RendererError> {
        self.loaded_style = Some(style.clone());
        self.ensure_renderer_for_task(task)
    }

    fn render(&mut self, task: &RenderTaskView) -> Result<RenderOutput, RendererError> {
        let image = match task.request {
            RenderRequest::Tile { z, x, y, tile_size } => {
                let key = RendererKey::new(crate::types::RenderMode::Tile, task.pixel_ratio);
                let size = render_size(tile_size, tile_size)?;
                self.ensure_tile_renderer(key, size, task.deadline)?
                    .render_tile(z, x, y)
            }
            RenderRequest::StaticImage {
                positioning:
                    crate::types::Positioning::Center {
                        lon,
                        lat,
                        zoom,
                        bearing,
                        pitch,
                    },
                width,
                height,
                ref overlays,
                ref before_layer,
                padding: _,
                ref addlayer,
            } => {
                let key = RendererKey::new(crate::types::RenderMode::Static, task.pixel_ratio);
                let size = render_size(width, height)?;
                let (renderer, slots, addlayer_sources) =
                    self.ensure_static_renderer(key, size, task.deadline)?;
                let camera = maplibre_native::CameraUpdate::new()
                    .center(maplibre_native::LatLng { lat, lng: lon })
                    .zoom(zoom)
                    .bearing(f64::from(bearing))
                    .pitch(f64::from(pitch));
                render_static_with_overlays_and_addlayer(
                    renderer,
                    slots,
                    addlayer_sources,
                    &camera,
                    overlays,
                    before_layer.as_deref(),
                    addlayer.as_ref(),
                    task.id,
                )
            }
            RenderRequest::StaticImage {
                positioning:
                    crate::types::Positioning::Bbox {
                        min_lon,
                        min_lat,
                        max_lon,
                        max_lat,
                    },
                width,
                height,
                ref overlays,
                ref before_layer,
                padding,
                ref addlayer,
            } => {
                let key = RendererKey::new(crate::types::RenderMode::Static, task.pixel_ratio);
                let size = render_size(width, height)?;
                let (renderer, slots, addlayer_sources) =
                    self.ensure_static_renderer(key, size, task.deadline)?;
                let bounds = maplibre_native::LatLngBounds {
                    southwest: maplibre_native::LatLng {
                        lat: min_lat,
                        lng: min_lon,
                    },
                    northeast: maplibre_native::LatLng {
                        lat: max_lat,
                        lng: max_lon,
                    },
                };
                let camera = renderer.camera_for_bounds(
                    bounds,
                    Some(padding_to_edge_insets(padding)),
                    0.0,
                    0.0,
                );
                render_static_with_overlays_and_addlayer(
                    renderer,
                    slots,
                    addlayer_sources,
                    &camera,
                    overlays,
                    before_layer.as_deref(),
                    addlayer.as_ref(),
                    task.id,
                )
            }
            RenderRequest::StaticImage {
                positioning: crate::types::Positioning::Auto,
                width,
                height,
                ref overlays,
                ref before_layer,
                padding,
                ref addlayer,
            } => {
                let key = RendererKey::new(crate::types::RenderMode::Static, task.pixel_ratio);
                let size = render_size(width, height)?;
                let (renderer, slots, addlayer_sources) =
                    self.ensure_static_renderer(key, size, task.deadline)?;
                // Build the overlay geometry collection once and ask mbgl
                // for a camera that fits it. The same overlays then get
                // installed by `assign_slots` below — that re-builds a
                // separate (idx-tagged) GeoJSON, which is a small cost we
                // accept for path simplicity.
                let fit_geojson = build_overlay_geojson(overlays)
                    .map_err(|err| RendererError::RenderFailed(err.to_string()))?;
                let auto_padding = padding_to_edge_insets(auto_padding_for_overlays(
                    padding, overlays, width, height,
                ));
                let Some(camera) =
                    renderer.camera_for_geojson(&fit_geojson, Some(auto_padding), 0.0, 0.0)
                else {
                    return Err(RendererError::RenderFailed(
                        "auto positioning: overlays produced no fittable geometry".to_string(),
                    ));
                };
                render_static_with_overlays_and_addlayer(
                    renderer,
                    slots,
                    addlayer_sources,
                    &camera,
                    overlays,
                    before_layer.as_deref(),
                    addlayer.as_ref(),
                    task.id,
                )
            }
        }
        .map_err(|err| RendererError::RenderFailed(err.to_string()))?;

        encode_image(&image, task.output_format)
    }

    fn error_invalidates_loaded_state(&self, err: &RendererError) -> bool {
        !matches!(err, RendererError::RenderFailed(_))
    }

    fn reset(&mut self) {
        self.reset_loaded_state();
    }
}

fn build_renderer(
    key: RendererKey,
    size: maplibre_native::Size,
    ambient_cache_path: Option<&std::path::Path>,
) -> Result<maplibre_native::ImageRendererBuilder, RendererError> {
    use std::num::NonZeroU32;

    let width = NonZeroU32::new(size.width)
        .ok_or_else(|| RendererError::RenderFailed("render width must be non-zero".to_string()))?;
    let height = NonZeroU32::new(size.height)
        .ok_or_else(|| RendererError::RenderFailed("render height must be non-zero".to_string()))?;

    let mut builder = maplibre_native::ImageRendererBuilder::new()
        .with_size(width, height)
        .with_pixel_ratio(key.pixel_ratio());
    if let Some(path) = ambient_cache_path {
        let resource_options =
            maplibre_native::ResourceOptions::default().with_cache_path(path.to_path_buf());
        builder = builder.with_resource_options(resource_options);
    }
    Ok(builder)
}

fn install_map_observer<S>(renderer: &mut maplibre_native::ImageRenderer<S>, state: ObserverState) {
    let observer = renderer.map_observer();
    observer.set_will_start_loading_map_callback({
        let state = state.clone();
        move || state.start_loading()
    });
    observer.set_did_finish_loading_style_callback({
        let state = state.clone();
        move || state.finish_loading_style()
    });
    observer.set_did_fail_loading_map_callback(move |error| {
        state.fail_loading_map(error);
    });
}

fn render_size(width: u16, height: u16) -> Result<maplibre_native::Size, RendererError> {
    if width == 0 || height == 0 {
        return Err(RendererError::RenderFailed(
            "render size must be non-zero".to_string(),
        ));
    }
    Ok(maplibre_native::Size {
        width: u32::from(width),
        height: u32::from(height),
    })
}

fn load_style_json<S>(
    renderer: &mut maplibre_native::ImageRenderer<S>,
    style: &ResolvedStyle,
    observer_state: &ObserverState,
    deadline: Instant,
) -> Result<(), RendererError> {
    observer_state.start_loading();
    // load_style_from_json_str returns a non-Send request handle; immediate
    // drop is fine. Completion is observed via did_finish_loading_style and
    // wait_for_style_load's tick loop.
    let _ = renderer.load_style_from_json_str(&style.style_json);
    wait_for_style_load(observer_state, style, deadline)
}

fn wait_for_style_load(
    state: &ObserverState,
    style: &ResolvedStyle,
    deadline: Instant,
) -> Result<(), RendererError> {
    let run_loop = maplibre_native::RunLoopHandle::current();
    loop {
        let snapshot = state.snapshot();
        if let Some(failure) = snapshot.failure {
            return Err(style_load_failure(&style.revision, failure));
        }
        if snapshot.style_loaded {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(RendererError::Timeout);
        }
        run_loop.tick();
    }
}

fn style_load_failure(revision: &StyleRevision, failure: MapLoadFailure) -> RendererError {
    RendererError::StyleLoadFailed {
        style_id: revision.id.clone(),
        source: failure.error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LngLat, Padding, PinOverlay, PinSize, Positioning, StaticOverlay, StyleId};

    struct FakeBackend;

    impl BlockingRenderBackend for FakeBackend {
        fn load_profile(
            &mut self,
            _style: &ResolvedStyle,
            _task: &RenderTaskView,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        fn render(&mut self, task: &RenderTaskView) -> Result<RenderOutput, RendererError> {
            Ok(RenderOutput {
                bytes: vec![task.id as u8].into(),
                format: task.output_format,
            })
        }
    }

    struct SlowBackend;

    impl BlockingRenderBackend for SlowBackend {
        fn load_profile(
            &mut self,
            _style: &ResolvedStyle,
            _task: &RenderTaskView,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        fn render(&mut self, task: &RenderTaskView) -> Result<RenderOutput, RendererError> {
            std::thread::sleep(std::time::Duration::from_millis(50));
            Ok(RenderOutput {
                bytes: vec![task.id as u8].into(),
                format: task.output_format,
            })
        }
    }

    struct ResetCountingBackend {
        resets: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl BlockingRenderBackend for ResetCountingBackend {
        fn load_profile(
            &mut self,
            _style: &ResolvedStyle,
            _task: &RenderTaskView,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        fn render(&mut self, _task: &RenderTaskView) -> Result<RenderOutput, RendererError> {
            Err(RendererError::RenderFailed(
                "test render failure".to_string(),
            ))
        }

        fn reset(&mut self) {
            self.resets
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    struct PanickingBackend {
        resets: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl BlockingRenderBackend for PanickingBackend {
        fn load_profile(
            &mut self,
            _style: &ResolvedStyle,
            _task: &RenderTaskView,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        fn render(&mut self, _task: &RenderTaskView) -> Result<RenderOutput, RendererError> {
            panic!("synthetic renderer panic");
        }

        fn reset(&mut self) {
            self.resets
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    fn revision() -> StyleRevision {
        StyleRevision {
            id: StyleId("carto/voyager".to_string()),
            version: 1,
        }
    }

    fn resolved_style() -> ResolvedStyle {
        ResolvedStyle {
            revision: revision(),
            style_json: Arc::from(r#"{"version":8,"sources":{},"layers":[]}"#),
        }
    }

    fn pin(size: PinSize) -> StaticOverlay {
        StaticOverlay::Pin(PinOverlay {
            size,
            label: None,
            color: "4c78a8".to_string(),
            coordinate: LngLat {
                lon: 139.767,
                lat: 35.681,
            },
        })
    }

    #[test]
    fn stable_addlayer_source_id_depends_on_tileset_and_json() {
        let source = crate::types::AddLayerSource {
            tileset_id: "rain".to_string(),
            json: r#"{"type":"vector","tiles":["https://example.test/{z}/{x}/{y}.pbf"]}"#
                .to_string(),
        };
        let same = crate::types::AddLayerSource {
            tileset_id: "rain".to_string(),
            json: source.json.clone(),
        };
        let different_tileset = crate::types::AddLayerSource {
            tileset_id: "snow".to_string(),
            json: source.json.clone(),
        };
        let different_json = crate::types::AddLayerSource {
            tileset_id: "rain".to_string(),
            json: r#"{"type":"vector","tiles":["https://other.example.test/{z}/{x}/{y}.pbf"]}"#
                .to_string(),
        };

        assert_eq!(source.stable_source_id(), same.stable_source_id());
        assert_ne!(
            source.stable_source_id(),
            different_tileset.stable_source_id()
        );
        assert_ne!(source.stable_source_id(), different_json.stable_source_id());
    }

    #[test]
    fn rgba_to_rgb_on_white_blends_alpha_for_jpeg() {
        let rgba = [
            10, 20, 30, 255, //
            10, 20, 30, 0, //
            0, 0, 0, 128,
        ];

        assert_eq!(
            rgba_to_rgb_on_white(&rgba),
            vec![
                10, 20, 30, //
                255, 255, 255, //
                127, 127, 127,
            ]
        );
    }

    fn task_view(style: StyleRevision) -> RenderTaskView {
        RenderTaskView {
            id: 7,
            style,
            source: None,
            request: RenderRequest::StaticImage {
                positioning: Positioning::Center {
                    lon: 139.767,
                    lat: 35.681,
                    zoom: 12.0,
                    bearing: 0.0,
                    pitch: 0.0,
                },
                width: 512,
                height: 512,
                overlays: Vec::new(),
                before_layer: None,
                padding: Padding::default(),
                addlayer: None,
            },
            pixel_ratio: PixelRatio::X1,
            output_format: ImageFormat::Png,
            deadline: Instant::now() + std::time::Duration::from_secs(1),
        }
    }

    #[test]
    fn auto_padding_adds_pin_top_inset() {
        let base = Padding {
            top: 20,
            right: 26,
            bottom: 20,
            left: 26,
        };
        let overlays = vec![
            StaticOverlay::Path(crate::types::PathOverlay {
                stroke_width: None,
                stroke_color: None,
                stroke_opacity: None,
                fill_color: None,
                fill_opacity: None,
                coordinates: vec![
                    LngLat {
                        lon: 139.767,
                        lat: 35.0,
                    },
                    LngLat {
                        lon: 139.767,
                        lat: 35.681,
                    },
                ],
            }),
            pin(PinSize::Large),
        ];

        assert_eq!(
            auto_padding_for_overlays(base, &overlays, 300, 190),
            Padding {
                top: 46,
                right: 26,
                bottom: 20,
                left: 26,
            }
        );
    }

    #[test]
    fn auto_padding_ignores_non_pin_overlays() {
        let base = Padding::all(10);
        assert_eq!(auto_padding_for_overlays(base, &[], 300, 190), base);
    }

    #[test]
    fn auto_padding_only_counts_pins_on_bounds_edges() {
        let base = Padding::all(10);
        let overlays = vec![
            pin(PinSize::Small),
            StaticOverlay::Path(crate::types::PathOverlay {
                stroke_width: None,
                stroke_color: None,
                stroke_opacity: None,
                fill_color: None,
                fill_opacity: None,
                coordinates: vec![
                    LngLat {
                        lon: 138.0,
                        lat: 36.0,
                    },
                    LngLat {
                        lon: 140.0,
                        lat: 36.0,
                    },
                ],
            }),
        ];

        assert_eq!(
            auto_padding_for_overlays(base, &overlays, 300, 190),
            Padding {
                top: 10,
                right: 10,
                bottom: 10,
                left: 10,
            }
        );
    }

    #[test]
    fn auto_padding_counts_pins_near_bounds_edges() {
        let base = Padding::all(10);
        let overlays = vec![
            StaticOverlay::GeoJson(crate::types::GeoJsonOverlay {
                feature_collection: serde_json::json!({
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-122.4111, 37.770025],
                            [-122.372037, 37.738775],
                            [-122.309537, 37.762213],
                            [-122.270475, 37.801275],
                            [-122.293912, 37.863775],
                            [-122.340787, 37.895025],
                            [-122.395475, 37.84815],
                            [-122.4111, 37.770025]
                        ]]
                    },
                    "properties": {}
                }),
            }),
            StaticOverlay::Pin(PinOverlay {
                size: PinSize::Small,
                label: None,
                color: "4682b4".to_string(),
                coordinate: LngLat {
                    lon: -122.4486,
                    lat: 37.8269,
                },
            }),
            StaticOverlay::Pin(PinOverlay {
                size: PinSize::Small,
                label: None,
                color: "4682b4".to_string(),
                coordinate: LngLat {
                    lon: -122.54,
                    lat: 36.7761,
                },
            }),
        ];

        let padding = auto_padding_for_overlays(base, &overlays, 300, 190);
        assert!(
            padding.top > base.top,
            "pin near the north edge needs extra top padding"
        );
        assert_eq!(padding.right, base.right);
        assert_eq!(padding.left, base.left);
    }

    #[tokio::test]
    async fn actor_loads_style_and_renders_on_backend_thread() {
        let actor = RendererActor::spawn_with_backend(
            RendererActorConfig {
                worker_id: 3,
                ambient_cache_path: None,
            },
            FakeBackend,
        )
        .expect("actor spawns");
        let style = resolved_style();
        let rev = style.revision.clone();

        let task = task_view(rev);
        actor
            .load_profile(style, task.clone())
            .await
            .expect("profile loads");
        let output = actor.render(task).await.expect("render succeeds");

        assert_eq!(output.bytes.as_ref(), &[7]);
        assert_eq!(output.format, ImageFormat::Png);
        assert!(actor.is_alive());
    }

    #[tokio::test]
    async fn actor_rejects_render_before_matching_style_is_loaded() {
        let actor = RendererActor::spawn_with_backend(
            RendererActorConfig {
                worker_id: 4,
                ambient_cache_path: None,
            },
            FakeBackend,
        )
        .expect("actor spawns");

        let err = actor
            .render(task_view(revision()))
            .await
            .expect_err("style must be loaded first");

        assert!(matches!(err, RendererError::StyleNotReady { .. }));
    }

    #[tokio::test]
    async fn actor_reply_wait_respects_task_deadline() {
        let actor = RendererActor::spawn_with_backend(
            RendererActorConfig {
                worker_id: 5,
                ambient_cache_path: None,
            },
            SlowBackend,
        )
        .expect("actor spawns");
        let style = resolved_style();
        let rev = style.revision.clone();
        let mut task = task_view(rev);
        actor
            .load_profile(style, task.clone())
            .await
            .expect("profile loads");
        task.deadline = Instant::now() + std::time::Duration::from_millis(5);

        let err = actor
            .render(task)
            .await
            .expect_err("actor reply wait times out at task deadline");

        assert!(matches!(err, RendererError::Timeout));
    }

    #[tokio::test]
    async fn actor_retires_after_current_command_returns() {
        let actor = RendererActor::spawn_with_backend(
            RendererActorConfig {
                worker_id: 15,
                ambient_cache_path: None,
            },
            FakeBackend,
        )
        .expect("actor spawns");
        actor.retire_after_current();

        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
        while actor.is_alive() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        assert!(!actor.is_alive(), "actor exits after retire command");
    }

    #[tokio::test]
    async fn actor_resets_loaded_state_after_render_failure() {
        let resets = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let actor = RendererActor::spawn_with_backend(
            RendererActorConfig {
                worker_id: 6,
                ambient_cache_path: None,
            },
            ResetCountingBackend {
                resets: resets.clone(),
            },
        )
        .expect("actor spawns");
        let style = resolved_style();
        let rev = style.revision.clone();
        let task = task_view(rev);
        actor
            .load_profile(style, task.clone())
            .await
            .expect("profile loads");

        let err = actor
            .render(task.clone())
            .await
            .expect_err("render failure is returned");
        assert!(matches!(err, RendererError::RenderFailed(_)));
        assert_eq!(resets.load(std::sync::atomic::Ordering::Acquire), 1);

        let err = actor
            .render(task)
            .await
            .expect_err("failed render clears actor warm state");
        assert!(matches!(err, RendererError::StyleNotReady { .. }));
    }

    #[tokio::test]
    async fn actor_survives_backend_panic_and_clears_loaded_state() {
        let resets = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let actor = RendererActor::spawn_with_backend(
            RendererActorConfig {
                worker_id: 7,
                ambient_cache_path: None,
            },
            PanickingBackend {
                resets: resets.clone(),
            },
        )
        .expect("actor spawns");
        let style = resolved_style();
        let rev = style.revision.clone();
        let task = task_view(rev);
        actor
            .load_profile(style, task.clone())
            .await
            .expect("profile loads");

        let err = actor
            .render(task.clone())
            .await
            .expect_err("panic is mapped");
        assert!(matches!(
            err,
            RendererError::RenderFailed(message)
                if message.contains("renderer actor panicked during render")
        ));
        assert_eq!(resets.load(std::sync::atomic::Ordering::Acquire), 1);
        assert!(actor.is_alive());

        let err = actor
            .render(task)
            .await
            .expect_err("panic clears actor warm state");
        assert!(matches!(err, RendererError::StyleNotReady { .. }));
    }
}
