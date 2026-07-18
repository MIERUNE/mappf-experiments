//! `worker_loop` + per-slot LRU `SourceCache`.

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant;

use crate::renderer::{BoxRenderer, PreparedProfile};
use crate::types::{
    CachePolicy, CompletedInfo, DeadlineStage, InternalTask, NodeId, RejectionReason,
    RenderObservation, RenderRequest, RendererError, RouteTier, SourceHash, TaskOutcome,
    TaskResult, WorkerId, WorkerProfile,
};
use crate::worker_pool::WorkerCompletion;

const RENDERER_REPAIR_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Debug)]
pub enum WorkerCmd {
    Process {
        task: InternalTask,
        prepared_profile: Option<PreparedProfile>,
        route_tier: RouteTier,
        admitted_at_overflow: bool,
        respond_to: oneshot::Sender<TaskOutcome>,
        completion: WorkerCompletion,
    },
}

#[derive(Debug)]
enum StageFailure {
    DeadlineExceeded {
        at: DeadlineStage,
    },
    RendererError {
        at: DeadlineStage,
        err: RendererError,
    },
    PermitClosed {
        at: DeadlineStage,
    },
}

struct StageSuccess {
    output: crate::types::RenderOutput,
    started_at: Instant,
    cpu_started_at: Instant,
    cpu_completed_at: Instant,
    style_swap: bool,
    cold_start: bool,
    source_loaded: bool,
    style_setup_duration: Option<std::time::Duration>,
    source_setup_duration: Option<std::time::Duration>,
}

/// LRU source cache per renderer slot. Keyed by `SourceHash` — same source
/// content → cache hit (skip `ensure_source`).
struct SourceCache {
    capacity: usize,
    entries: VecDeque<SourceHash>,
}

impl SourceCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::new(),
        }
    }

    fn contains(&self, h: SourceHash) -> bool {
        self.entries.iter().any(|&x| x == h)
    }

    fn touch(&mut self, h: SourceHash) {
        if let Some(pos) = self.entries.iter().position(|&x| x == h) {
            self.entries.remove(pos);
        }
        self.entries.push_back(h);
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

pub async fn worker_loop(
    id: WorkerId,
    node_id: NodeId,
    mut rx: mpsc::Receiver<WorkerCmd>,
    mut renderer: BoxRenderer,
    render_permits: Arc<Semaphore>,
    cpu_render_permits: Arc<Semaphore>,
    source_cache_capacity: usize,
) {
    // The worker's view of warm state is style revision + render mode + scale.
    // Static/Tile and @1x/@2x intentionally use separate warm workers.
    let mut current_profile: Option<WorkerProfile> = None;
    let mut cache = SourceCache::new(source_cache_capacity);

    let mut repair_tick = tokio::time::interval(RENDERER_REPAIR_INTERVAL);
    repair_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let cmd = tokio::select! {
            cmd = rx.recv() => match cmd {
                Some(cmd) => cmd,
                None => break,
            },
            _ = repair_tick.tick() => {
                if let Err(error) = renderer.repair_if_needed() {
                    tracing::debug!(worker_id = id, %error, "renderer actor is not repairable yet");
                }
                continue;
            }
        };
        match cmd {
            WorkerCmd::Process {
                task,
                prepared_profile,
                route_tier,
                admitted_at_overflow,
                respond_to,
                completion,
            } => {
                let had_source = task.has_source();
                let outcome = match run_stages(
                    &mut renderer,
                    &mut current_profile,
                    &mut cache,
                    render_permits.clone(),
                    cpu_render_permits.clone(),
                    &task,
                    prepared_profile,
                )
                .await
                {
                    Ok(success) => TaskOutcome {
                        task_id: task.id,
                        request_id: task.request_id.clone(),
                        arrived_at: task.arrived_at,
                        had_source,
                        deadline_stage: None,
                        result: TaskResult::Completed {
                            info: CompletedInfo {
                                node_id: node_id.clone(),
                                worker_id: Some(id),
                                route_tier,
                                started_at: success.started_at,
                                cpu_started_at: success.cpu_started_at,
                                cpu_completed_at: success.cpu_completed_at,
                                completed_at: Instant::now(),
                                style_swap: success.style_swap,
                                cold_start: success.cold_start,
                                source_loaded: success.source_loaded,
                                admitted_at_overflow,
                                render_observation: Some(RenderObservation::from_task(
                                    &task,
                                    success.style_setup_duration,
                                    success.source_setup_duration,
                                )),
                            },
                            output: success.output,
                        },
                    },
                    Err(StageFailure::DeadlineExceeded { at }) => {
                        deadline_rejected_outcome(task, had_source, at)
                    }
                    Err(StageFailure::RendererError { at, err }) => {
                        let _ = at;
                        if matches!(err, RendererError::Timeout) {
                            renderer.retire_after_current();
                        }
                        if renderer_error_invalidates_warm_state(&err) {
                            current_profile = None;
                            cache.clear();
                        }
                        failed_outcome(task, had_source, err)
                    }
                    Err(StageFailure::PermitClosed { at }) => {
                        let _ = at;
                        failed_outcome(task, had_source, RendererError::ActorDead)
                    }
                };
                // Finalize shared warm-state and queue accounting even when
                // the request future (and therefore the response receiver)
                // was dropped after dispatch.
                completion.finish(&outcome);
                let _ = respond_to.send(outcome);
            }
        }
    }
}

fn renderer_error_invalidates_warm_state(err: &RendererError) -> bool {
    matches!(
        err,
        RendererError::StyleLoadFailed { .. }
            | RendererError::StyleNotReady { .. }
            | RendererError::Timeout
            | RendererError::ActorDead
    )
}

async fn run_stages(
    renderer: &mut BoxRenderer,
    current_profile: &mut Option<WorkerProfile>,
    cache: &mut SourceCache,
    render_permits: Arc<Semaphore>,
    cpu_render_permits: Arc<Semaphore>,
    task: &InternalTask,
    prepared_profile: Option<PreparedProfile>,
) -> Result<StageSuccess, StageFailure> {
    let task_profile = task.worker_profile();
    let style_swap = current_profile.as_ref() != Some(&task_profile);
    let cold_start = current_profile.is_none() && style_swap;
    let prepared_addlayer_source = prepared_profile
        .as_ref()
        .and_then(|prepared| prepared.addlayer_source.clone());

    let permit = acquire_permit(render_permits, task, DeadlineStage::AcquireRenderPermit).await?;
    let started_at = Instant::now();
    let mut style_setup_duration = None;

    if style_swap {
        check_deadline_at(task, DeadlineStage::StyleSwap)?;
        let setup_started_at = Instant::now();
        renderer
            .setup_profile(task, prepared_profile)
            .await
            .map_err(|err| StageFailure::RendererError {
                at: DeadlineStage::StyleSwap,
                err,
            })?;
        style_setup_duration = Some(Instant::now().duration_since(setup_started_at));
        *current_profile = Some(task_profile);
        cache.clear();
    }

    check_deadline_at(task, DeadlineStage::EnsureSource)?;
    let mut source_loaded = false;
    let mut source_setup_duration = None;
    if let Some(src) = &task.source {
        let cached = cache.contains(src.hash);
        if !cached {
            let source_started_at = Instant::now();
            renderer
                .ensure_source(src.hash)
                .await
                .map_err(|err| StageFailure::RendererError {
                    at: DeadlineStage::EnsureSource,
                    err,
                })?;
            source_setup_duration = Some(Instant::now().duration_since(source_started_at));
            source_loaded = true;
        }
        if src.policy == CachePolicy::Cacheable {
            cache.touch(src.hash);
        }
    }

    let cpu_permit =
        acquire_permit(cpu_render_permits, task, DeadlineStage::AcquireCpuPermit).await?;
    let cpu_started_at = Instant::now();

    check_deadline_at(task, DeadlineStage::Render)?;
    let render_task;
    let task_for_render = if let Some(source) = prepared_addlayer_source {
        render_task = task_with_prepared_addlayer_source(task, source);
        &render_task
    } else {
        task
    };
    let rendered =
        renderer
            .render(task_for_render)
            .await
            .map_err(|err| StageFailure::RendererError {
                at: DeadlineStage::Render,
                err,
            })?;
    source_loaded |= rendered.source_setup_duration.is_some();
    source_setup_duration = match (source_setup_duration, rendered.source_setup_duration) {
        (Some(before_render), Some(during_render)) => Some(before_render + during_render),
        (duration @ Some(_), None) | (None, duration @ Some(_)) => duration,
        (None, None) => None,
    };
    let cpu_completed_at = Instant::now();
    drop(cpu_permit);
    drop(permit);

    if cpu_completed_at > task.deadline {
        return Err(StageFailure::RendererError {
            at: DeadlineStage::Render,
            err: RendererError::Timeout,
        });
    }

    Ok(StageSuccess {
        output: rendered.output,
        started_at,
        cpu_started_at,
        cpu_completed_at,
        style_swap,
        cold_start,
        source_loaded,
        style_setup_duration,
        source_setup_duration,
    })
}

fn task_with_prepared_addlayer_source(
    task: &InternalTask,
    source: crate::types::AddLayerSource,
) -> InternalTask {
    let mut task = task.clone();
    if let RenderRequest::StaticImage { addlayer, .. } = &mut task.request
        && let Some(addlayer) = addlayer
    {
        addlayer.source = Some(source);
    }
    task
}

async fn acquire_permit(
    permits: Arc<Semaphore>,
    task: &InternalTask,
    at: DeadlineStage,
) -> Result<OwnedSemaphorePermit, StageFailure> {
    check_deadline_at(task, at)?;
    tokio::select! {
        permit = permits.acquire_owned() => permit.map_err(|_| StageFailure::PermitClosed { at }),
        _ = tokio::time::sleep_until(task.deadline) => {
            Err(StageFailure::DeadlineExceeded { at })
        }
    }
}

fn check_deadline_at(task: &InternalTask, at: DeadlineStage) -> Result<(), StageFailure> {
    if Instant::now() >= task.deadline {
        Err(StageFailure::DeadlineExceeded { at })
    } else {
        Ok(())
    }
}

fn failed_outcome(task: InternalTask, had_source: bool, error: RendererError) -> TaskOutcome {
    TaskOutcome {
        task_id: task.id,
        request_id: task.request_id,
        arrived_at: task.arrived_at,
        had_source,
        deadline_stage: None,
        result: TaskResult::Failed {
            kind: crate::types::FailureKind::from_renderer_error(&error),
            error: error.to_string(),
        },
    }
}

fn deadline_rejected_outcome(
    task: InternalTask,
    had_source: bool,
    at: DeadlineStage,
) -> TaskOutcome {
    let mut outcome = rejected_outcome(task, had_source, RejectionReason::DeadlineExceeded);
    outcome.deadline_stage = Some(at);
    outcome
}

fn rejected_outcome(task: InternalTask, had_source: bool, reason: RejectionReason) -> TaskOutcome {
    TaskOutcome {
        task_id: task.id,
        request_id: task.request_id,
        arrived_at: task.arrived_at,
        had_source,
        deadline_stage: None,
        result: TaskResult::Rejected { reason },
    }
}
