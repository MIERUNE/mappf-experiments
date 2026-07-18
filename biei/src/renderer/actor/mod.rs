//! Dedicated blocking renderer actor for production MapLibre integration.
//!
//! MapLibre Native rendering is treated as thread-affine blocking work. This
//! actor owns the backend on one OS thread and exposes async request/reply
//! methods to worker tasks.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use tokio::sync::oneshot;
use tokio::time::Instant;

mod addlayer;
mod camera;
mod encode;

use super::RendererOutput;
use super::overlay::{OverlaySlotPool, build_overlay_geojson, populate_static_slots};
use addlayer::{AddLayerSourceCache, render_static_with_overlays_and_addlayer};
use camera::{auto_padding_for_overlays, padding_to_edge_insets};
use encode::encode_image;
#[cfg(test)]
use encode::rgba_to_rgb_on_white;

#[cfg(test)]
use crate::types::RenderOutput;
use crate::types::{
    ImageFormat, InternalTask, PixelRatio, RenderRequest, RendererError, SourceRef, StyleRevision,
    TaskId, WorkerId,
};
use crate::util::lock_unpoisoned;

// Native renderer destruction flushes backend state and may take a few tens of
// milliseconds even when no render is in flight. Keep shutdown bounded, but
// avoid treating normal destruction as a stuck actor.
const ACTOR_JOIN_GRACE: std::time::Duration = std::time::Duration::from_millis(100);
const THREAD_RUNNING: u8 = 0;
const THREAD_ORPHANED: u8 = 1;
const THREAD_FINISHED: u8 = 2;

#[derive(Clone, Debug)]
pub struct RendererActorSupervisor {
    inner: Arc<RendererActorSupervisorInner>,
}

#[derive(Debug)]
struct RendererActorSupervisorInner {
    total_slots: usize,
    available_slots: AtomicUsize,
    max_orphaned_threads: usize,
    orphaned_threads: AtomicUsize,
    orphaned_by_worker: Mutex<HashMap<WorkerId, usize>>,
    replacements_succeeded: AtomicU64,
    replacements_exhausted: AtomicU64,
    replacements_failed: AtomicU64,
    provider_health: crate::renderer::file_source::ProviderHealthTracker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RendererHealth {
    Full,
    ExternalDegraded,
    InternalUnrecoverable,
}

impl RendererHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::ExternalDegraded => "external_degraded",
            Self::InternalUnrecoverable => "internal_unrecoverable",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RendererActorHealthSnapshot {
    pub total_slots: usize,
    pub available_slots: usize,
    pub orphaned_threads: usize,
    pub replacements_succeeded: u64,
    pub replacements_exhausted: u64,
    pub replacements_failed: u64,
    pub health: RendererHealth,
}

impl RendererActorSupervisor {
    pub fn new(total_slots: usize) -> Self {
        Self::with_provider_health(
            total_slots,
            crate::renderer::file_source::ProviderHealthTracker::new(),
        )
    }

    pub(crate) fn with_provider_health(
        total_slots: usize,
        provider_health: crate::renderer::file_source::ProviderHealthTracker,
    ) -> Self {
        let total_slots = total_slots.max(1);
        Self {
            inner: Arc::new(RendererActorSupervisorInner {
                total_slots,
                available_slots: AtomicUsize::new(total_slots),
                // One abandoned native render per configured slot is enough
                // to recover a complete first-wave wedge without allowing an
                // attacker to leak threads indefinitely.
                max_orphaned_threads: total_slots,
                orphaned_threads: AtomicUsize::new(0),
                orphaned_by_worker: Mutex::new(HashMap::new()),
                replacements_succeeded: AtomicU64::new(0),
                replacements_exhausted: AtomicU64::new(0),
                replacements_failed: AtomicU64::new(0),
                provider_health,
            }),
        }
    }

    pub fn is_ready(&self) -> bool {
        !matches!(self.health(), RendererHealth::InternalUnrecoverable)
    }

    /// Per-slot render capacity: true while any slot is available, even when
    /// `ExternalDegraded`. Gating on `Full` would let one lost slot stop every
    /// healthy slot; a systemic outage self-limits via the orphan budget.
    pub fn can_start_render(&self) -> bool {
        self.inner.available_slots.load(Ordering::Acquire) > 0
    }

    /// A `can_start_render` closure for `Node::set_render_admission_probe`, so
    /// the ingress and peer-forward wiring share one construction.
    pub(crate) fn render_admission_probe(&self) -> Arc<dyn Fn() -> bool + Send + Sync> {
        let supervisor = self.clone();
        Arc::new(move || supervisor.can_start_render())
    }

    /// External provider degradation is not repaired by restarting this
    /// process, so it remains live while an actual FileSource retry sequence is
    /// active. An unavailable slot without that evidence is an internal
    /// failure; autonomous repair gets the probe grace before process restart.
    pub fn is_livable(&self) -> bool {
        !matches!(self.health(), RendererHealth::InternalUnrecoverable)
    }

    pub fn health(&self) -> RendererHealth {
        if self.inner.available_slots.load(Ordering::Acquire) == self.inner.total_slots {
            return RendererHealth::Full;
        }

        // Elapsed time cannot turn a provider outage into an internal fault:
        // restarting still cannot repair the provider and destroys warm cache.
        // Slow-attempt evidence is promoted only after admission and a network
        // threshold, so normal fast traffic does not mask a renderer loss.
        if self.inner.provider_health.has_external_evidence() {
            RendererHealth::ExternalDegraded
        } else {
            RendererHealth::InternalUnrecoverable
        }
    }

    pub fn snapshot(&self) -> RendererActorHealthSnapshot {
        RendererActorHealthSnapshot {
            total_slots: self.inner.total_slots,
            available_slots: self.inner.available_slots.load(Ordering::Acquire),
            orphaned_threads: self.inner.orphaned_threads.load(Ordering::Acquire),
            replacements_succeeded: self.inner.replacements_succeeded.load(Ordering::Relaxed),
            replacements_exhausted: self.inner.replacements_exhausted.load(Ordering::Relaxed),
            replacements_failed: self.inner.replacements_failed.load(Ordering::Relaxed),
            health: self.health(),
        }
    }

    fn try_reserve_orphan(&self, worker_id: WorkerId) -> bool {
        let mut by_worker = lock_unpoisoned(&self.inner.orphaned_by_worker);
        if by_worker.contains_key(&worker_id)
            || self.inner.orphaned_threads.load(Ordering::Acquire)
                >= self.inner.max_orphaned_threads
        {
            return false;
        }
        by_worker.insert(worker_id, 1);
        self.inner.orphaned_threads.fetch_add(1, Ordering::AcqRel);
        true
    }

    fn reserve_orphan_unchecked(&self, worker_id: WorkerId) {
        *lock_unpoisoned(&self.inner.orphaned_by_worker)
            .entry(worker_id)
            .or_default() += 1;
        self.inner.orphaned_threads.fetch_add(1, Ordering::AcqRel);
    }

    fn release_orphan(&self, worker_id: WorkerId) {
        let mut by_worker = lock_unpoisoned(&self.inner.orphaned_by_worker);
        let Some(count) = by_worker.get_mut(&worker_id) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            by_worker.remove(&worker_id);
        }
        self.inner.orphaned_threads.fetch_sub(1, Ordering::AcqRel);
    }

    pub(crate) fn record_replacement_succeeded(&self) {
        self.inner
            .replacements_succeeded
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_replacement_exhausted(&self) {
        self.inner
            .replacements_exhausted
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_replacement_failed(&self) {
        self.inner
            .replacements_failed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_slot_available(&self, available: &mut bool, next: bool) {
        if *available == next {
            return;
        }
        if next {
            self.inner.available_slots.fetch_add(1, Ordering::AcqRel);
        } else {
            self.inner.available_slots.fetch_sub(1, Ordering::AcqRel);
        }
        *available = next;
    }
}

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
    fn render(&mut self, task: &RenderTaskView) -> Result<RendererOutput, RendererError>;
    fn error_invalidates_loaded_state(&self, _err: &RendererError) -> bool {
        true
    }
    fn reset(&mut self) {}
}

pub struct RendererActor {
    worker_id: WorkerId,
    tx: mpsc::Sender<RenderCmd>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
    thread_status: Arc<AtomicU8>,
    supervisor: RendererActorSupervisor,
}

enum RenderCmd {
    LoadProfile {
        style: ResolvedStyle,
        task: RenderTaskView,
        reply: oneshot::Sender<Result<(), RendererError>>,
    },
    Render {
        task: RenderTaskView,
        reply: oneshot::Sender<Result<RendererOutput, RendererError>>,
    },
    Retire,
    Shutdown,
}

impl RendererActor {
    pub fn spawn(config: RendererActorConfig) -> Result<Self, RendererError> {
        Self::spawn_supervised(config, RendererActorSupervisor::new(1))
    }

    pub fn spawn_supervised(
        config: RendererActorConfig,
        supervisor: RendererActorSupervisor,
    ) -> Result<Self, RendererError> {
        let ambient_cache_path = config.ambient_cache_path.clone();
        Self::spawn_with_backend_factory(config, supervisor, move || {
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
        Self::spawn_with_backend_supervised(config, RendererActorSupervisor::new(1), backend)
    }

    pub(crate) fn spawn_with_backend_supervised<B>(
        config: RendererActorConfig,
        supervisor: RendererActorSupervisor,
        backend: B,
    ) -> Result<Self, RendererError>
    where
        B: BlockingRenderBackend + Send,
    {
        Self::spawn_with_backend_factory(config, supervisor, || backend)
    }

    fn spawn_with_backend_factory<F, B>(
        config: RendererActorConfig,
        supervisor: RendererActorSupervisor,
        backend_factory: F,
    ) -> Result<Self, RendererError>
    where
        F: FnOnce() -> B + Send + 'static,
        B: BlockingRenderBackend,
    {
        let (tx, rx) = mpsc::channel();
        let thread_status = Arc::new(AtomicU8::new(THREAD_RUNNING));
        let exit_status = Arc::clone(&thread_status);
        let exit_supervisor = supervisor.clone();
        let worker_id = config.worker_id;
        let thread = thread::Builder::new()
            .name(format!("biei-renderer-{}", config.worker_id))
            .spawn(move || {
                let _exit = ActorThreadExit {
                    worker_id,
                    status: exit_status,
                    supervisor: exit_supervisor,
                };
                run_actor(rx, backend_factory());
            })
            .map_err(|err| {
                RendererError::RenderFailed(format!("failed to spawn renderer actor: {err}"))
            })?;

        Ok(Self {
            worker_id,
            tx,
            thread: Mutex::new(Some(thread)),
            thread_status,
            supervisor,
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

    pub async fn render(&self, task: RenderTaskView) -> Result<RendererOutput, RendererError> {
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

    /// Detach a wedged actor thread after reserving bounded orphan capacity.
    /// The thread decrements the orphan count itself if the native call ever
    /// returns. A detached actor has no join handle and can be dropped cheaply.
    pub(crate) fn try_abandon(&self) -> bool {
        let mut thread = lock_unpoisoned(&self.thread);
        let Some(handle) = thread.as_ref() else {
            return true;
        };
        if handle.is_finished() {
            let handle = thread.take().expect("renderer thread exists");
            drop(thread);
            let _ = handle.join();
            return true;
        }
        if !self.supervisor.try_reserve_orphan(self.worker_id) {
            return false;
        }
        match self.thread_status.compare_exchange(
            THREAD_RUNNING,
            THREAD_ORPHANED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Dropping a JoinHandle detaches the still-running thread.
                thread.take();
                true
            }
            Err(THREAD_FINISHED) => {
                self.supervisor.release_orphan(self.worker_id);
                let handle = thread.take().expect("renderer thread exists");
                drop(thread);
                let _ = handle.join();
                true
            }
            Err(_) => {
                self.supervisor.release_orphan(self.worker_id);
                false
            }
        }
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
                if self
                    .thread_status
                    .compare_exchange(
                        THREAD_RUNNING,
                        THREAD_ORPHANED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.supervisor.reserve_orphan_unchecked(self.worker_id);
                }
                tracing::warn!(
                    "renderer actor thread did not stop promptly; detaching to avoid blocking shutdown"
                );
            }
        }
    }
}

struct ActorThreadExit {
    worker_id: WorkerId,
    status: Arc<AtomicU8>,
    supervisor: RendererActorSupervisor,
}

impl Drop for ActorThreadExit {
    fn drop(&mut self) {
        if self.status.swap(THREAD_FINISHED, Ordering::AcqRel) == THREAD_ORPHANED {
            self.supervisor.release_orphan(self.worker_id);
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
            load_style_json(&mut renderer, &style)?;
            let slots = populate_static_slots(&mut renderer).map_err(|err| {
                RendererError::StyleLoadFailed {
                    style_id: style.revision.id.clone(),
                    source: err.to_string(),
                }
            })?;
            self.active_renderer = Some(ActiveRenderer::Static {
                key,
                loaded_style: Some(style.revision.clone()),
                renderer,
                slots,
                addlayer_sources: AddLayerSourceCache::new(),
            });
        }
        let Some(ActiveRenderer::Static {
            loaded_style,
            renderer,
            slots,
            addlayer_sources,
            ..
        }) = self.active_renderer.as_mut()
        else {
            unreachable!("static renderer was inserted")
        };
        if loaded_style.as_ref() != Some(&style.revision) {
            load_style_json(renderer, &style)?;
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
    ) -> Result<&mut maplibre_native::ImageRenderer<maplibre_native::Tile>, RendererError> {
        let style = self.style()?.clone();
        let needs_rebuild = !matches!(
            self.active_renderer,
            Some(ActiveRenderer::Tile { key: existing, .. }) if existing == key
        );
        if needs_rebuild {
            let mut renderer = build_renderer(key, size, self.ambient_cache_path.as_deref())?
                .build_tile_renderer();
            load_style_json(&mut renderer, &style)?;
            self.active_renderer = Some(ActiveRenderer::Tile {
                key,
                loaded_style: Some(style.revision.clone()),
                renderer,
            });
        }
        let Some(ActiveRenderer::Tile {
            loaded_style,
            renderer,
            ..
        }) = self.active_renderer.as_mut()
        else {
            unreachable!("tile renderer was inserted")
        };
        if loaded_style.as_ref() != Some(&style.revision) {
            load_style_json(renderer, &style)?;
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
                self.ensure_tile_renderer(key, size)?;
            }
            RenderRequest::StaticImage { width, height, .. } => {
                let key = RendererKey::new(crate::types::RenderMode::Static, task.pixel_ratio);
                let size = render_size(width, height)?;
                self.ensure_static_renderer(key, size)?;
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

impl BlockingRenderBackend for MapLibreNativeBackend {
    fn load_profile(
        &mut self,
        style: &ResolvedStyle,
        task: &RenderTaskView,
    ) -> Result<(), RendererError> {
        self.loaded_style = Some(style.clone());
        self.ensure_renderer_for_task(task)
    }

    fn render(&mut self, task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
        let (image, source_setup_duration) = match task.request {
            RenderRequest::Tile { z, x, y, tile_size } => {
                let key = RendererKey::new(crate::types::RenderMode::Tile, task.pixel_ratio);
                let size = render_size(tile_size, tile_size)?;
                self.ensure_tile_renderer(key, size)?
                    .render_tile(z, x, y)
                    .map(|image| (image, None))
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
                let (renderer, slots, addlayer_sources) = self.ensure_static_renderer(key, size)?;
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
                let (renderer, slots, addlayer_sources) = self.ensure_static_renderer(key, size)?;
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
                let (renderer, slots, addlayer_sources) = self.ensure_static_renderer(key, size)?;
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

        Ok(RendererOutput {
            output: encode_image(&image, task.output_format)?,
            source_setup_duration,
        })
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
) -> Result<(), RendererError> {
    renderer
        .load_style_from_json_str(&style.style_json)
        .wait()
        .map_err(|error| RendererError::StyleLoadFailed {
            style_id: style.revision.id.clone(),
            source: error.to_string(),
        })
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

        fn render(&mut self, task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
            Ok(RenderOutput {
                bytes: vec![task.id as u8].into(),
                format: task.output_format,
            }
            .into())
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

        fn render(&mut self, task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
            std::thread::sleep(std::time::Duration::from_millis(50));
            Ok(RenderOutput {
                bytes: vec![task.id as u8].into(),
                format: task.output_format,
            }
            .into())
        }
    }

    struct SlowDropBackend {
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl BlockingRenderBackend for SlowDropBackend {
        fn load_profile(
            &mut self,
            _style: &ResolvedStyle,
            _task: &RenderTaskView,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        fn render(&mut self, task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
            Ok(RenderOutput {
                bytes: vec![task.id as u8].into(),
                format: task.output_format,
            }
            .into())
        }
    }

    impl Drop for SlowDropBackend {
        fn drop(&mut self) {
            std::thread::sleep(std::time::Duration::from_millis(25));
            self.dropped
                .store(true, std::sync::atomic::Ordering::Release);
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

        fn render(&mut self, _task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
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

        fn render(&mut self, _task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
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

        assert_eq!(output.output.bytes.as_ref(), &[7]);
        assert_eq!(output.output.format, ImageFormat::Png);
        assert!(actor.is_alive());
    }

    #[test]
    fn actor_drop_waits_for_normal_backend_destruction() {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let actor = RendererActor::spawn_with_backend(
            RendererActorConfig {
                worker_id: 16,
                ambient_cache_path: None,
            },
            SlowDropBackend {
                dropped: Arc::clone(&dropped),
            },
        )
        .expect("actor spawns");

        drop(actor);

        assert!(
            dropped.load(std::sync::atomic::Ordering::Acquire),
            "actor shutdown joins a backend with a normal slow destructor"
        );
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
    async fn abandoned_actor_threads_are_bounded_and_released_on_exit() {
        let supervisor = RendererActorSupervisor::new(1);
        let actor = RendererActor::spawn_with_backend_supervised(
            RendererActorConfig {
                worker_id: 17,
                ambient_cache_path: None,
            },
            supervisor.clone(),
            SlowBackend,
        )
        .expect("actor spawns");
        let style = resolved_style();
        let mut task = task_view(style.revision.clone());
        actor
            .load_profile(style, task.clone())
            .await
            .expect("profile loads");
        task.deadline = Instant::now() + std::time::Duration::from_millis(5);
        assert!(matches!(
            actor.render(task).await,
            Err(RendererError::Timeout)
        ));
        actor.retire_after_current();
        assert!(actor.try_abandon());
        assert_eq!(supervisor.snapshot().orphaned_threads, 1);

        let second = RendererActor::spawn_with_backend_supervised(
            RendererActorConfig {
                worker_id: 18,
                ambient_cache_path: None,
            },
            supervisor.clone(),
            FakeBackend,
        )
        .expect("second actor spawns");
        assert!(
            !second.try_abandon(),
            "orphan budget must prevent unbounded detached threads"
        );
        drop(second);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while supervisor.snapshot().orphaned_threads != 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(supervisor.snapshot().orphaned_threads, 0);
    }

    #[test]
    fn orphan_budget_is_fair_across_workers() {
        let supervisor = RendererActorSupervisor::new(2);

        assert!(supervisor.try_reserve_orphan(7));
        assert!(
            !supervisor.try_reserve_orphan(7),
            "one hot worker must not consume another slot's orphan budget"
        );
        assert!(supervisor.try_reserve_orphan(8));
        assert_eq!(supervisor.snapshot().orphaned_threads, 2);

        supervisor.release_orphan(7);
        assert!(supervisor.try_reserve_orphan(7));
        supervisor.release_orphan(7);
        supervisor.release_orphan(8);
        assert_eq!(supervisor.snapshot().orphaned_threads, 0);
    }

    #[test]
    fn unavailable_slot_sheds_readiness_before_process_recovery() {
        let supervisor = RendererActorSupervisor::new(2);
        let mut first_slot_available = true;

        supervisor.set_slot_available(&mut first_slot_available, false);

        assert!(
            !supervisor.is_ready(),
            "a degraded pod must stop accepting new work before restart"
        );
        assert!(
            !supervisor.is_livable(),
            "a permanently lost slot at exhausted budget needs process recovery"
        );
    }

    #[test]
    fn one_lost_slot_with_budget_remaining_requires_process_recovery() {
        // A hot worker may consume only its own orphan allowance while global
        // budget remains. If it wedges again, replacement is still impossible
        // for that slot. Readiness sheds the pod even though other slots remain;
        // liveness may eventually restore capacity after its recovery grace.
        let supervisor = RendererActorSupervisor::new(16);
        let mut lost_slot_available = true;

        // First wedge orphaned one thread; the second wedge on the same worker
        // is refused a replacement and marks the slot unavailable.
        assert!(supervisor.try_reserve_orphan(3));
        supervisor.set_slot_available(&mut lost_slot_available, false);

        let health = supervisor.snapshot();
        assert_eq!(health.available_slots, 15);
        assert_eq!(health.orphaned_threads, 1);
        assert!(
            !supervisor.is_ready(),
            "one unavailable slot must shed traffic before liveness restarts the pod"
        );
        assert!(
            !supervisor.is_livable(),
            "an unavailable slot must not remain hidden behind unused global orphan budget"
        );
    }

    #[test]
    fn health_distinguishes_active_provider_failure_from_internal_loss() {
        let provider = crate::renderer::file_source::ProviderHealthTracker::new();
        let supervisor = RendererActorSupervisor::with_provider_health(2, provider.clone());
        assert_eq!(supervisor.health(), RendererHealth::Full);
        assert!(supervisor.can_start_render());

        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);
        assert_eq!(supervisor.health(), RendererHealth::InternalUnrecoverable);
        assert!(!supervisor.is_ready());
        assert!(!supervisor.is_livable());

        let retry = provider.begin_retry();
        assert_eq!(supervisor.health(), RendererHealth::ExternalDegraded);
        assert!(
            supervisor.is_ready(),
            "external degradation must keep cached responses reachable"
        );
        assert!(supervisor.is_livable());
        assert!(
            supervisor.can_start_render(),
            "the remaining healthy slot still renders while externally degraded"
        );

        drop(retry);
        assert_eq!(supervisor.health(), RendererHealth::InternalUnrecoverable);
    }

    #[tokio::test(start_paused = true)]
    async fn continuing_provider_evidence_never_becomes_restart_pressure() {
        let provider = crate::renderer::file_source::ProviderHealthTracker::new();
        let supervisor = RendererActorSupervisor::with_provider_health(2, provider.clone());
        // Hold a process-global provider retry for the whole test.
        let _retry = provider.begin_retry();
        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);

        assert_eq!(supervisor.health(), RendererHealth::ExternalDegraded);
        assert!(supervisor.is_livable());

        tokio::time::advance(std::time::Duration::from_secs(24 * 60 * 60)).await;
        assert_eq!(supervisor.health(), RendererHealth::ExternalDegraded);
        assert!(
            supervisor.is_livable(),
            "elapsed time alone must not cause a cache-destroying restart"
        );

        supervisor.set_slot_available(&mut slot_available, true);
        assert_eq!(supervisor.health(), RendererHealth::Full);
    }

    #[test]
    fn one_lost_slot_does_not_stop_the_remaining_healthy_slots() {
        let supervisor = RendererActorSupervisor::new(3);
        assert!(supervisor.can_start_render());

        // One slot is lost: the pod is no longer `Full`, but the two healthy
        // slots must keep accepting renders. Gating on `Full` would amplify one
        // slot's fault into a whole-pod render outage, blocking even renders
        // that only touch already-cached resources.
        let mut a = true;
        supervisor.set_slot_available(&mut a, false);
        assert_ne!(supervisor.health(), RendererHealth::Full);
        assert!(
            supervisor.can_start_render(),
            "healthy slots keep rendering while one slot is down"
        );

        // All slots lost: only now, with no capacity at all, does admission
        // close.
        let mut b = true;
        supervisor.set_slot_available(&mut b, false);
        let mut c = true;
        supervisor.set_slot_available(&mut c, false);
        assert!(
            !supervisor.can_start_render(),
            "with no slot available the pod finally stops starting native work"
        );
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
