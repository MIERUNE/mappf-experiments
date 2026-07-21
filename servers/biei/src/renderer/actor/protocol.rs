//! Generic renderer actor protocol, thread lifecycle, and panic containment.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use biei_core::types::{
    ImageFormat, InternalTask, PixelRatio, RenderRequest, RendererError, StyleRevision, TaskId,
    WorkerId,
};
use mmpf_common::sync::lock_unpoisoned;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::super::RendererOutput;
use super::backend::MapLibreNativeBackend;
use super::supervisor::RendererActorSupervisor;

// Native renderer destruction flushes backend state and may take a few tens of
// milliseconds even when no render is in flight. Keep shutdown bounded, but
// avoid treating normal destruction as a stuck actor.
const ACTOR_JOIN_GRACE: std::time::Duration = std::time::Duration::from_millis(100);
const THREAD_RUNNING: u8 = 0;
const THREAD_ORPHANED: u8 = 1;
const THREAD_FINISHED: u8 = 2;

#[derive(Clone, Debug)]
pub(crate) struct RendererActorConfig {
    pub worker_id: WorkerId,
    pub ambient_cache_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedStyle {
    pub revision: StyleRevision,
    pub style_json: Arc<str>,
}

#[derive(Clone, Debug)]
pub(crate) struct RenderTaskView {
    pub id: TaskId,
    pub style: StyleRevision,
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
pub(crate) trait BlockingRenderBackend: 'static {
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

pub(crate) struct RendererActor {
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
    pub(crate) fn spawn_supervised(
        config: RendererActorConfig,
        supervisor: RendererActorSupervisor,
    ) -> Result<Self, RendererError> {
        let ambient_cache_path = config.ambient_cache_path.clone();
        Self::spawn_with_backend_factory(config, supervisor, move || {
            MapLibreNativeBackend::new(ambient_cache_path)
        })
    }

    #[cfg(test)]
    pub(crate) fn spawn_with_backend<B>(
        config: RendererActorConfig,
        backend: B,
    ) -> Result<Self, RendererError>
    where
        B: BlockingRenderBackend + Send,
    {
        Self::spawn_with_backend_supervised(config, RendererActorSupervisor::new(1), backend)
    }

    #[cfg(test)]
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

    pub(crate) fn is_alive(&self) -> bool {
        let Ok(thread) = self.thread.lock() else {
            return false;
        };
        thread.as_ref().is_some_and(|t| !t.is_finished())
    }

    pub(crate) async fn load_profile(
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

    pub(crate) async fn render(
        &self,
        task: RenderTaskView,
    ) -> Result<RendererOutput, RendererError> {
        let (reply, rx) = oneshot::channel();
        let deadline = task.deadline;
        self.tx
            .send(RenderCmd::Render { task, reply })
            .map_err(|_| RendererError::ActorDead)?;
        await_actor_reply(deadline, rx).await
    }

    pub(crate) fn retire_after_current(&self) {
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
            RenderCmd::Retire | RenderCmd::Shutdown => break,
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

#[cfg(test)]
mod tests {
    use super::*;
    use biei_core::types::{Padding, Positioning, RenderOutput, StyleId};

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
        resets: Arc<std::sync::atomic::AtomicUsize>,
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
        resets: Arc<std::sync::atomic::AtomicUsize>,
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

    fn task_view(style: StyleRevision) -> RenderTaskView {
        RenderTaskView {
            id: 7,
            style,
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
        let resets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
        let resets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
