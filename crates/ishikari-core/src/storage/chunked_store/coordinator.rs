//! In-flight chunk fetch coordination and waiter management.

use std::{
    collections::{BTreeSet, HashMap},
    ops::Range,
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use bytes::Bytes;
use tokio::{
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError, oneshot},
    time::{self, Instant},
};
use tracing::{debug, error};

use crate::{interned::TilesetId, metrics::NodeMetrics};

use super::{
    fetcher::{ChunkFetchError, ChunkFetcher},
    store::ChunkedStore,
};

const IMMEDIATE_CHUNK_INDEX: u64 = 0;
const MAX_CHUNK_GAP: u64 = 1;
const MAX_CONCURRENT_FETCHES_PER_TILESET: usize = 32;

/// Coordinates shared inflight chunk fetches.
#[derive(Clone)]
pub(super) struct ChunkFetchCoordinator {
    fetcher: ChunkFetcher,
    metrics: NodeMetrics,
    max_fetch_chunks: u64,
    merge_window: Duration,
    /// Hard process-wide bound over groups waiting for an active backend slot
    /// plus groups performing I/O. Reserved before a detached task is spawned.
    backend_group_permits: Arc<Semaphore>,
    /// Per-tileset fetch state keyed by tileset id.
    tileset_states: Arc<Mutex<HashMap<TilesetId, TilesetFetchState>>>,
}

/// Inflight and pending fetch coordination state for a single tileset.
#[derive(Default)]
struct TilesetFetchState {
    /// Chunks queued for the next backend fetch batch.
    pending_chunks: BTreeSet<u64>,
    /// Time when the current pending set first became non-empty.
    first_pending_at: Option<Instant>,
    /// Chunks currently being fetched from the backend.
    inflight_chunks: BTreeSet<u64>,
    /// Per-chunk waiters that are released when the shared fetch completes.
    waiters: HashMap<u64, Vec<oneshot::Sender<Result<Bytes, ChunkFetchError>>>>,
    /// Whether the per-tileset scheduler task is currently running.
    scheduler_running: bool,
    /// Number of backend fetches currently inflight for this tileset.
    inflight_fetch_count: usize,
    /// Wakes the scheduler when an inflight fetch releases a per-tileset slot.
    capacity_available: Arc<Notify>,
    archive_len: u64,
}

/// How a newly requested chunk joined the fetch state, for the wait metric.
enum EnqueueOutcome {
    /// Chunk is already being fetched; the caller joins that inflight fetch.
    JoinedInflight,
    /// Chunk was newly queued for the next batch.
    Queued,
    /// Chunk was already queued by another caller; this caller joins it.
    JoinedPending,
}

struct EnqueuedChunk {
    chunk_index: u64,
    receiver: oneshot::Receiver<Result<Bytes, ChunkFetchError>>,
    outcome: EnqueueOutcome,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GroupWaiterOutcome {
    delivered: usize,
    cancelled: usize,
}

/// Ensures a dispatched group reaches the state-completion path even when its
/// task is aborted or unwinds before normal completion.
struct GroupCompletionGuard {
    tileset_states: Arc<Mutex<HashMap<TilesetId, TilesetFetchState>>>,
    metrics: NodeMetrics,
    tileset_id: TilesetId,
    chunk_range: Range<u64>,
    result: Option<Result<HashMap<u64, Bytes>, ChunkFetchError>>,
    completed: bool,
}

impl GroupCompletionGuard {
    fn new(
        coordinator: &ChunkFetchCoordinator,
        tileset_id: TilesetId,
        chunk_range: Range<u64>,
    ) -> Self {
        Self {
            tileset_states: Arc::clone(&coordinator.tileset_states),
            metrics: coordinator.metrics.clone(),
            tileset_id,
            chunk_range,
            result: None,
            completed: false,
        }
    }

    async fn complete(mut self, result: Result<HashMap<u64, Bytes>, ChunkFetchError>) {
        self.result = Some(result);
        complete_dispatched_group(
            Arc::clone(&self.tileset_states),
            self.metrics.clone(),
            self.tileset_id.clone(),
            self.chunk_range.clone(),
            self.result
                .as_ref()
                .expect("completion result was just installed"),
        )
        .await;
        self.completed = true;
    }
}

impl Drop for GroupCompletionGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        let result = self.result.take().unwrap_or_else(|| {
            Err(ChunkFetchError::Message(
                "chunk fetch task cancelled or panicked".to_string(),
            ))
        });
        let tileset_states = Arc::clone(&self.tileset_states);
        let metrics = self.metrics.clone();
        let tileset_id = self.tileset_id.clone();
        let chunk_range = self.chunk_range.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            error!(
                tileset_id = %tileset_id,
                start_chunk = chunk_range.start,
                end_chunk = chunk_range.end,
                "unable to schedule chunk fetch completion outside a Tokio runtime"
            );
            return;
        };
        runtime.spawn(async move {
            complete_dispatched_group(tileset_states, metrics, tileset_id, chunk_range, &result)
                .await;
        });
    }
}

impl EnqueueOutcome {
    fn metric_label(&self) -> &'static str {
        match self {
            Self::JoinedInflight => "joined_inflight",
            Self::Queued => "queued",
            Self::JoinedPending => "joined_pending",
        }
    }
}

/// Outcome of a scheduler pass asking the state what to dispatch next.
enum DispatchDecision {
    /// Nothing pending; the scheduler stops (state set non-running).
    Idle,
    /// Pending work exists but the concurrency cap leaves no slot this pass.
    Throttled(Arc<Notify>),
    /// Dispatch these contiguous chunk ranges; carries dispatch metric inputs.
    Dispatch {
        groups: Vec<Range<u64>>,
        archive_len: u64,
        queue_delay: Duration,
        pending_at_dispatch: usize,
    },
}

impl TilesetFetchState {
    /// Whether this tileset has no scheduled, pending, or inflight work — the
    /// idle state a fresh fetch can flush immediately from.
    fn is_idle(&self) -> bool {
        !self.scheduler_running
            && self.pending_chunks.is_empty()
            && self.inflight_chunks.is_empty()
            && self.inflight_fetch_count == 0
    }

    /// Whether the state holds no scheduled or inflight work and can be dropped
    /// from the coordinator map.
    fn is_drainable(&self) -> bool {
        !self.scheduler_running && self.pending_chunks.is_empty() && self.inflight_fetch_count == 0
    }

    /// Registers a waiter per requested chunk, queuing chunks not already
    /// inflight or pending. Returns each chunk's receiver and how it joined, so
    /// the caller can await it and record the wait metric.
    fn enqueue_chunks(
        &mut self,
        required_chunks: &[u64],
        queued_at: Instant,
    ) -> Vec<EnqueuedChunk> {
        let mut joined = Vec::with_capacity(required_chunks.len());
        for &chunk_index in required_chunks {
            let (tx, rx) = oneshot::channel();
            // Each caller waits on its own oneshot, but the backend fetch is shared.
            self.waiters.entry(chunk_index).or_default().push(tx);
            let outcome = if self.inflight_chunks.contains(&chunk_index) {
                EnqueueOutcome::JoinedInflight
            } else if self.pending_chunks.insert(chunk_index) {
                if self.first_pending_at.is_none() {
                    self.first_pending_at = Some(queued_at);
                }
                EnqueueOutcome::Queued
            } else {
                EnqueueOutcome::JoinedPending
            };
            joined.push(EnqueuedChunk {
                chunk_index,
                receiver: rx,
                outcome,
            });
        }
        joined
    }

    /// Removes closed receivers before dispatch. Pending chunks with no live
    /// waiters are unscheduled; already-inflight chunks are left running so a
    /// shared read may still warm the cache for later callers.
    fn prune_cancelled_waiters(&mut self) -> usize {
        let mut cancelled_waiters = 0;
        self.waiters.retain(|_, waiters| {
            let before = waiters.len();
            waiters.retain(|waiter| !waiter.is_closed());
            cancelled_waiters += before - waiters.len();
            !waiters.is_empty()
        });
        self.pending_chunks
            .retain(|chunk_index| self.waiters.contains_key(chunk_index));
        if self.pending_chunks.is_empty() {
            self.first_pending_at = None;
        }
        cancelled_waiters
    }

    /// Selects the next batch of contiguous chunk ranges to dispatch, moving
    /// them from pending to inflight and reserving fetch slots. Stops the
    /// scheduler when nothing is pending.
    fn select_dispatch_groups(&mut self, max_fetch_chunks: u64) -> DispatchDecision {
        if self.pending_chunks.is_empty() {
            self.scheduler_running = false;
            return DispatchDecision::Idle;
        }
        if self.inflight_fetch_count >= MAX_CONCURRENT_FETCHES_PER_TILESET {
            return DispatchDecision::Throttled(Arc::clone(&self.capacity_available));
        }
        let available_slots = MAX_CONCURRENT_FETCHES_PER_TILESET - self.inflight_fetch_count;
        let groups: Vec<Range<u64>> =
            contiguous_chunk_ranges(&self.pending_chunks, max_fetch_chunks, MAX_CHUNK_GAP)
                .into_iter()
                .take(available_slots)
                .collect();
        if groups.is_empty() {
            return DispatchDecision::Throttled(Arc::clone(&self.capacity_available));
        }
        // Snapshot metric inputs before the chunks leave the pending set.
        let pending_at_dispatch = self.pending_chunks.len();
        let queue_delay = self
            .first_pending_at
            .map(|instant| instant.elapsed())
            .unwrap_or_default();
        for chunk_range in &groups {
            self.inflight_chunks
                .extend(chunk_range.start..chunk_range.end);
            for chunk_index in chunk_range.start..chunk_range.end {
                self.pending_chunks.remove(&chunk_index);
            }
        }
        if self.pending_chunks.is_empty() {
            self.first_pending_at = None;
        }
        self.inflight_fetch_count += groups.len();
        DispatchDecision::Dispatch {
            groups,
            archive_len: self.archive_len,
            queue_delay,
            pending_at_dispatch,
        }
    }

    /// Releases a finished fetch group: frees its slot, clears the inflight
    /// chunks, and delivers `result` to every live waiter. Closed receivers are
    /// classified as cancellations at the send boundary so completion races do
    /// not inflate the released-waiter metric.
    fn complete_group(
        &mut self,
        chunk_range: Range<u64>,
        result: &Result<HashMap<u64, Bytes>, ChunkFetchError>,
    ) -> GroupWaiterOutcome {
        let scheduler_needs_capacity = self.inflight_fetch_count
            >= MAX_CONCURRENT_FETCHES_PER_TILESET
            && !self.pending_chunks.is_empty();
        self.inflight_fetch_count = self.inflight_fetch_count.saturating_sub(1);
        if scheduler_needs_capacity {
            self.capacity_available.notify_one();
        }
        let mut waiter_outcome = GroupWaiterOutcome::default();
        for chunk_index in chunk_range.start..chunk_range.end {
            self.inflight_chunks.remove(&chunk_index);
            if let Some(waiters) = self.waiters.remove(&chunk_index) {
                let chunk_result = match result {
                    Ok(chunks) => chunks.get(&chunk_index).cloned().ok_or_else(|| {
                        ChunkFetchError::Message(format!(
                            "fetched group omitted chunk {chunk_index}"
                        ))
                    }),
                    Err(error) => Err(error.clone()),
                };
                for waiter in waiters {
                    if waiter.send(chunk_result.clone()).is_ok() {
                        waiter_outcome.delivered += 1;
                    } else {
                        waiter_outcome.cancelled += 1;
                    }
                }
            }
        }
        waiter_outcome
    }
}

impl ChunkFetchCoordinator {
    pub(super) fn new(
        fetcher: ChunkFetcher,
        max_fetch_chunks: u64,
        merge_window: Duration,
        backend_fetch_max_inflight: usize,
        metrics: NodeMetrics,
    ) -> Self {
        metrics.set_chunk_fetch_merge_window(merge_window);
        let backend_fetch_max_inflight = backend_fetch_max_inflight.max(1);
        metrics.set_backend_fetch_max_inflight(backend_fetch_max_inflight);
        Self {
            fetcher,
            metrics,
            max_fetch_chunks,
            merge_window,
            backend_group_permits: Arc::new(Semaphore::new(backend_fetch_max_inflight)),
            tileset_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn chunk_size(&self) -> u64 {
        self.fetcher.chunk_size()
    }

    pub(super) fn received_bytes(&self) -> u64 {
        self.fetcher.received_bytes()
    }

    pub(super) fn metrics(&self) -> &NodeMetrics {
        &self.metrics
    }

    /// Reads the format-defined initial bootstrap without expanding it to a
    /// chunk boundary. Bootstrap singleflight lives one layer above, so this
    /// direct fetch does not need per-chunk waiter coordination.
    pub(super) async fn fetch_exact_range(
        &self,
        tileset_id: &TilesetId,
        range: Range<u64>,
    ) -> std::result::Result<Bytes, ChunkFetchError> {
        let _group_permit = self.try_reserve_backend_group()?;
        self.fetcher.fetch_exact_range(tileset_id, range).await
    }

    fn try_reserve_backend_group(
        &self,
    ) -> std::result::Result<OwnedSemaphorePermit, ChunkFetchError> {
        match self.backend_group_permits.clone().try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(TryAcquireError::NoPermits) => Err(ChunkFetchError::Overloaded(
                "backend fetch group limit reached".to_string(),
            )),
            Err(TryAcquireError::Closed) => Err(ChunkFetchError::Message(
                "backend fetch group admission closed".to_string(),
            )),
        }
    }

    /// Fetches chunks for a tileset while coalescing concurrent requests.
    pub(super) async fn fetch_chunks(
        &self,
        store: ChunkedStore,
        tileset_id: &TilesetId,
        required_chunks: &[u64],
        archive_len: u64,
    ) -> std::result::Result<HashMap<u64, Bytes>, ChunkFetchError> {
        let mut receivers = Vec::with_capacity(required_chunks.len());
        let queued_at = Instant::now();

        {
            let mut tileset_states = self.tileset_states.lock().await;
            let tileset_state = tileset_states.entry(tileset_id.clone()).or_default();
            let was_idle = tileset_state.is_idle();
            tileset_state.archive_len = tileset_state.archive_len.max(archive_len);

            for enqueued in tileset_state.enqueue_chunks(required_chunks, queued_at) {
                self.metrics
                    .record_chunk_fetch_wait(enqueued.outcome.metric_label());
                receivers.push((enqueued.chunk_index, enqueued.receiver));
            }

            if !tileset_state.scheduler_running && !tileset_state.pending_chunks.is_empty() {
                tileset_state.scheduler_running = true;
                let flush_immediately = was_idle
                    && tileset_state
                        .pending_chunks
                        .contains(&IMMEDIATE_CHUNK_INDEX);
                let coordinator = self.clone();
                let tileset_id = tileset_id.clone();
                let store = store.clone();
                tokio::spawn(async move {
                    coordinator
                        .run_scheduler(store, tileset_id, flush_immediately)
                        .await;
                });
            }
        }

        let mut chunks = HashMap::with_capacity(receivers.len());
        for (chunk_index, receiver) in receivers {
            let result = receiver.await.map_err(|_| {
                ChunkFetchError::Message(anyhow!("chunk fetch waiter dropped").to_string())
            })?;
            chunks.insert(chunk_index, result?);
        }

        Ok(chunks)
    }

    async fn run_scheduler(
        &self,
        store: ChunkedStore,
        tileset_id: TilesetId,
        mut flush_immediately: bool,
    ) {
        loop {
            let flushed_immediately = flush_immediately;
            if flush_immediately {
                flush_immediately = false;
            } else {
                time::sleep(self.merge_window).await;
            }

            let (dispatch, capacity_available) = {
                let mut tileset_states = self.tileset_states.lock().await;
                let Some(state) = tileset_states.get_mut(&tileset_id) else {
                    return;
                };
                let cancelled_waiters = state.prune_cancelled_waiters();
                if cancelled_waiters > 0 {
                    self.metrics
                        .record_cancelled_chunk_fetch_waiters(cancelled_waiters);
                }
                match state.select_dispatch_groups(self.max_fetch_chunks) {
                    DispatchDecision::Idle => {
                        remove_drainable_state(&mut tileset_states, &tileset_id);
                        return;
                    }
                    DispatchDecision::Throttled(capacity_available) => {
                        (None, Some(capacity_available))
                    }
                    DispatchDecision::Dispatch {
                        groups,
                        archive_len,
                        queue_delay,
                        pending_at_dispatch,
                    } => {
                        let flush_label = if flushed_immediately {
                            "immediate"
                        } else {
                            "window"
                        };
                        self.metrics.record_chunk_fetch_dispatch(
                            flush_label,
                            queue_delay,
                            pending_at_dispatch,
                        );
                        (Some((groups, archive_len)), None)
                    }
                }
            };

            if let Some(capacity_available) = capacity_available {
                capacity_available.notified().await;
                flush_immediately = true;
                continue;
            }

            let Some((groups, archive_len)) = dispatch else {
                continue;
            };

            for chunk_range in groups {
                match self.try_reserve_backend_group() {
                    Ok(group_permit) => {
                        let coordinator = self.clone();
                        let tileset_id = tileset_id.clone();
                        let store = store.clone();
                        tokio::spawn(async move {
                            coordinator
                                .run_fetch_chunk_group(
                                    store,
                                    tileset_id,
                                    chunk_range,
                                    archive_len,
                                    group_permit,
                                )
                                .await;
                        });
                    }
                    Err(error) => {
                        complete_dispatched_group(
                            Arc::clone(&self.tileset_states),
                            self.metrics.clone(),
                            tileset_id.clone(),
                            chunk_range,
                            &Err(error),
                        )
                        .await;
                    }
                }
            }
        }
    }

    async fn run_fetch_chunk_group(
        &self,
        store: ChunkedStore,
        tileset_id: TilesetId,
        chunk_range: Range<u64>,
        archive_len: u64,
        _group_permit: OwnedSemaphorePermit,
    ) {
        let completion = GroupCompletionGuard::new(self, tileset_id.clone(), chunk_range.clone());
        let result = self
            .fetcher
            .fetch_chunk_group(&tileset_id, chunk_range.clone(), archive_len)
            .await
            .and_then(|bytes| {
                store
                    .cache_chunk_group(&tileset_id, chunk_range, archive_len, bytes)
                    .map_err(|error| ChunkFetchError::Message(error.to_string()))
            });
        completion.complete(result).await;
    }
}

async fn complete_dispatched_group(
    tileset_states: Arc<Mutex<HashMap<TilesetId, TilesetFetchState>>>,
    metrics: NodeMetrics,
    tileset_id: TilesetId,
    chunk_range: Range<u64>,
    result: &Result<HashMap<u64, Bytes>, ChunkFetchError>,
) {
    let mut tileset_states = tileset_states.lock().await;
    let Some(state) = tileset_states.get_mut(&tileset_id) else {
        return;
    };

    let waiter_outcome = state.complete_group(chunk_range.clone(), result);
    let completion_outcome = match result {
        Ok(_) => "success",
        Err(ChunkFetchError::Overloaded(_)) => "shed",
        Err(_) => "error",
    };
    metrics.record_chunk_fetch_group_waiters(completion_outcome, waiter_outcome.delivered);
    metrics.record_cancelled_chunk_fetch_waiters(waiter_outcome.cancelled);

    let waiter_count: usize = state.waiters.values().map(Vec::len).sum();
    debug!(
        tileset_id = %tileset_id,
        start_chunk = chunk_range.start,
        end_chunk = chunk_range.end,
        fetch_succeeded = result.is_ok(),
        pending_chunks = ?state.pending_chunks,
        inflight_chunks = ?state.inflight_chunks,
        inflight_fetches = state.inflight_fetch_count,
        waiter_keys = state.waiters.len(),
        waiters = waiter_count,
        "completed chunk fetch group"
    );

    remove_drainable_state(&mut tileset_states, &tileset_id);
}

/// Removes a fully drained per-tileset coordination state while the coordinator
/// map lock is held. Both scheduler-idle and final-fetch completion use this
/// single lifecycle transition.
fn remove_drainable_state(
    tileset_states: &mut HashMap<TilesetId, TilesetFetchState>,
    tileset_id: &TilesetId,
) -> bool {
    if !tileset_states
        .get(tileset_id)
        .is_some_and(TilesetFetchState::is_drainable)
    {
        return false;
    }
    tileset_states.remove(tileset_id);
    debug!(tileset_id = %tileset_id, "removed empty chunk fetch state");
    true
}

fn contiguous_chunk_ranges(
    chunks: &BTreeSet<u64>,
    max_fetch_chunks: u64,
    max_chunk_gap: u64,
) -> Vec<Range<u64>> {
    let mut ranges = Vec::new();
    let mut iter = chunks.iter().copied();
    let Some(mut start) = iter.next() else {
        return ranges;
    };
    let max_fetch_chunks = max_fetch_chunks.max(1);
    let mut end = start + 1;

    for chunk in iter {
        if chunk <= end + max_chunk_gap && chunk + 1 - start <= max_fetch_chunks {
            end = chunk + 1;
            continue;
        }
        ranges.push(start..end);
        start = chunk;
        end = chunk + 1;
    }
    ranges.push(start..end);
    ranges
}

/// Plans the backend ranges used by the production chunk coordinator.
#[cfg(feature = "simulator-support")]
pub fn plan_chunk_fetch_ranges(chunks: &BTreeSet<u64>, max_fetch_chunks: u64) -> Vec<Range<u64>> {
    contiguous_chunk_ranges(chunks, max_fetch_chunks, MAX_CHUNK_GAP)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn set(values: &[u64]) -> BTreeSet<u64> {
        values.iter().copied().collect()
    }

    type CompletionGuardFixture = (
        GroupCompletionGuard,
        oneshot::Receiver<Result<Bytes, ChunkFetchError>>,
        Arc<Mutex<HashMap<TilesetId, TilesetFetchState>>>,
        TilesetId,
    );

    fn completion_guard_fixture() -> CompletionGuardFixture {
        let tileset_id = TilesetId::try_new("completion-guard").expect("tileset id");
        let mut state = TilesetFetchState::default();
        let receiver = state
            .enqueue_chunks(&[7], Instant::now())
            .pop()
            .expect("enqueued chunk")
            .receiver;
        let DispatchDecision::Dispatch { groups, .. } = state.select_dispatch_groups(1) else {
            panic!("expected a dispatch");
        };
        assert_eq!(groups, vec![7..8]);

        let tileset_states = Arc::new(Mutex::new(HashMap::from([(tileset_id.clone(), state)])));
        let guard = GroupCompletionGuard {
            tileset_states: Arc::clone(&tileset_states),
            metrics: NodeMetrics::new(),
            tileset_id: tileset_id.clone(),
            chunk_range: 7..8,
            result: None,
            completed: false,
        };
        (guard, receiver, tileset_states, tileset_id)
    }

    #[test]
    fn enqueue_queues_new_chunks_then_joins_existing() {
        let mut state = TilesetFetchState::default();
        let now = Instant::now();

        let queued = state.enqueue_chunks(&[5], now);
        assert!(matches!(queued[0].outcome, EnqueueOutcome::Queued));
        assert!(state.pending_chunks.contains(&5));
        assert!(state.first_pending_at.is_some());

        // A second waiter for the same still-pending chunk joins it.
        let joined = state.enqueue_chunks(&[5], now);
        assert!(matches!(joined[0].outcome, EnqueueOutcome::JoinedPending));

        // A chunk already inflight is joined, not re-queued.
        state.inflight_chunks.insert(9);
        let inflight = state.enqueue_chunks(&[9], now);
        assert!(matches!(
            inflight[0].outcome,
            EnqueueOutcome::JoinedInflight
        ));
        assert!(!state.pending_chunks.contains(&9));
    }

    #[test]
    fn cancelled_pending_waiters_are_pruned_before_dispatch() {
        let mut state = TilesetFetchState {
            scheduler_running: true,
            ..Default::default()
        };
        let receivers = state.enqueue_chunks(&[1, 2], Instant::now());
        drop(receivers);

        assert_eq!(state.prune_cancelled_waiters(), 2);
        assert!(state.pending_chunks.is_empty());
        assert!(state.waiters.is_empty());
        assert!(state.first_pending_at.is_none());
        assert!(matches!(
            state.select_dispatch_groups(4),
            DispatchDecision::Idle
        ));
        assert!(state.is_drainable());

        let tileset_id = TilesetId::try_new("cancelled").expect("tileset id");
        let mut states = HashMap::from([(tileset_id.clone(), state)]);
        assert!(remove_drainable_state(&mut states, &tileset_id));
        assert!(states.is_empty());
    }

    #[test]
    fn one_live_waiter_preserves_shared_pending_work() {
        let mut state = TilesetFetchState::default();
        let cancelled = state
            .enqueue_chunks(&[5], Instant::now())
            .pop()
            .expect("cancelled waiter");
        let live = state
            .enqueue_chunks(&[5], Instant::now())
            .pop()
            .expect("live waiter");
        drop(cancelled.receiver);

        assert_eq!(state.prune_cancelled_waiters(), 1);
        assert!(state.pending_chunks.contains(&5));
        assert_eq!(state.waiters.get(&5).map(Vec::len), Some(1));
        assert!(matches!(
            state.select_dispatch_groups(4),
            DispatchDecision::Dispatch { .. }
        ));
        drop(live.receiver);
    }

    #[test]
    fn cancelled_inflight_waiter_does_not_cancel_active_work() {
        let mut state = TilesetFetchState::default();
        let receiver = state
            .enqueue_chunks(&[9], Instant::now())
            .pop()
            .expect("waiter")
            .receiver;
        let _ = state.select_dispatch_groups(4);
        drop(receiver);

        assert_eq!(state.prune_cancelled_waiters(), 1);
        assert!(state.waiters.is_empty());
        assert!(state.inflight_chunks.contains(&9));
        assert_eq!(state.inflight_fetch_count, 1);
        assert!(!state.is_drainable());
    }

    #[test]
    fn select_dispatch_moves_pending_to_inflight() {
        let mut state = TilesetFetchState::default();
        state.enqueue_chunks(&[1, 2, 3], Instant::now());

        let DispatchDecision::Dispatch { groups, .. } = state.select_dispatch_groups(4) else {
            panic!("expected a dispatch");
        };
        assert_eq!(groups, vec![1..4]);
        assert!(state.pending_chunks.is_empty());
        assert_eq!(state.inflight_chunks, set(&[1, 2, 3]));
        assert_eq!(state.inflight_fetch_count, 1);
        assert!(state.first_pending_at.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn queue_delay_follows_tokio_virtual_time() {
        let mut state = TilesetFetchState::default();
        state.enqueue_chunks(&[1], Instant::now());

        time::advance(Duration::from_millis(10)).await;

        let DispatchDecision::Dispatch { queue_delay, .. } = state.select_dispatch_groups(4) else {
            panic!("expected a dispatch");
        };
        assert_eq!(queue_delay, Duration::from_millis(10));
    }

    #[test]
    fn select_dispatch_idle_stops_scheduler() {
        let mut state = TilesetFetchState {
            scheduler_running: true,
            ..Default::default()
        };
        assert!(matches!(
            state.select_dispatch_groups(4),
            DispatchDecision::Idle
        ));
        assert!(!state.scheduler_running);
        assert!(state.is_drainable());
    }

    #[test]
    fn complete_group_releases_waiters_and_drains() {
        let mut state = TilesetFetchState::default();
        let mut receivers: Vec<_> = state
            .enqueue_chunks(&[1, 2], Instant::now())
            .into_iter()
            .map(|enqueued| enqueued.receiver)
            .collect();
        let _ = state.select_dispatch_groups(4);

        let chunks = HashMap::from([
            (1, Bytes::from_static(b"chunk one")),
            (2, Bytes::from_static(b"chunk two")),
        ]);
        let waiter_outcome = state.complete_group(1..3, &Ok(chunks));
        assert_eq!(
            waiter_outcome,
            GroupWaiterOutcome {
                delivered: 2,
                cancelled: 0,
            }
        );
        assert_eq!(state.inflight_fetch_count, 0);
        assert!(state.inflight_chunks.is_empty());
        assert_eq!(
            receivers[0].try_recv().expect("chunk 1").expect("ok"),
            "chunk one"
        );
        assert_eq!(
            receivers[1].try_recv().expect("chunk 2").expect("ok"),
            "chunk two"
        );
        assert!(state.is_drainable());
    }

    #[test]
    fn completion_classifies_closed_inflight_receivers_as_cancelled() {
        let mut state = TilesetFetchState::default();
        let receiver = state
            .enqueue_chunks(&[3], Instant::now())
            .pop()
            .expect("waiter")
            .receiver;
        let _ = state.select_dispatch_groups(4);
        drop(receiver);

        let waiter_outcome = state.complete_group(
            3..4,
            &Ok(HashMap::from([(3, Bytes::from_static(b"chunk"))])),
        );

        assert_eq!(
            waiter_outcome,
            GroupWaiterOutcome {
                delivered: 0,
                cancelled: 1,
            }
        );
        assert!(state.is_drainable());
    }

    #[tokio::test]
    async fn completion_guard_releases_waiters_on_normal_error() {
        let (guard, receiver, tileset_states, tileset_id) = completion_guard_fixture();
        guard
            .complete(Err(ChunkFetchError::Message(
                "simulated fetch error".to_string(),
            )))
            .await;

        let error = receiver
            .await
            .expect("completion sender dropped")
            .expect_err("fetch error must fail the waiter");
        assert_eq!(error.to_string(), "simulated fetch error");
        assert!(!tileset_states.lock().await.contains_key(&tileset_id));
    }

    #[tokio::test]
    async fn completion_guard_releases_waiters_when_group_task_panics() {
        let (guard, receiver, tileset_states, tileset_id) = completion_guard_fixture();
        let task = tokio::spawn(async move {
            let _guard = guard;
            panic!("simulated fetch panic");
        });
        assert!(task.await.expect_err("task must panic").is_panic());

        let error = time::timeout(Duration::from_secs(1), receiver)
            .await
            .expect("waiter release timed out")
            .expect("completion sender dropped")
            .expect_err("panic must fail the waiter");
        assert!(error.to_string().contains("cancelled or panicked"));
        assert!(!tileset_states.lock().await.contains_key(&tileset_id));
    }

    #[tokio::test]
    async fn completion_guard_releases_waiters_when_group_task_is_cancelled() {
        let (guard, receiver, tileset_states, tileset_id) = completion_guard_fixture();
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = guard;
            let _ = ready_tx.send(());
            std::future::pending::<()>().await;
        });
        ready_rx.await.expect("guard installed");
        task.abort();
        assert!(
            task.await
                .expect_err("task must be cancelled")
                .is_cancelled()
        );

        let error = time::timeout(Duration::from_secs(1), receiver)
            .await
            .expect("waiter release timed out")
            .expect("completion sender dropped")
            .expect_err("cancellation must fail the waiter");
        assert!(error.to_string().contains("cancelled or panicked"));
        assert!(!tileset_states.lock().await.contains_key(&tileset_id));
    }

    #[tokio::test]
    async fn completing_fetch_wakes_capacity_waiter() {
        let mut state = TilesetFetchState {
            scheduler_running: true,
            inflight_fetch_count: MAX_CONCURRENT_FETCHES_PER_TILESET,
            ..Default::default()
        };
        state.inflight_chunks.insert(1);
        state.enqueue_chunks(&[2], Instant::now());

        let DispatchDecision::Throttled(capacity_available) = state.select_dispatch_groups(4)
        else {
            panic!("expected capacity throttling");
        };
        let notified = capacity_available.notified();

        state.complete_group(1..2, &Ok(HashMap::from([(1, Bytes::new())])));
        tokio::time::timeout(Duration::from_secs(1), notified)
            .await
            .expect("fetch completion must wake the scheduler");
    }

    #[test]
    fn groups_contiguous_chunks_into_one_backend_range() {
        assert_eq!(contiguous_chunk_ranges(&set(&[2, 3, 4]), 4, 1), vec![2..5]);
    }

    #[test]
    fn prefetches_across_small_chunk_gaps() {
        assert_eq!(contiguous_chunk_ranges(&set(&[2, 4]), 4, 1), vec![2..5]);
    }

    #[test]
    fn respects_max_fetch_chunks_even_when_gaps_are_mergeable() {
        assert_eq!(
            contiguous_chunk_ranges(&set(&[2, 4, 6]), 4, 1),
            vec![2..5, 6..7]
        );
    }

    #[test]
    fn splits_large_gaps() {
        assert_eq!(
            contiguous_chunk_ranges(&set(&[2, 5]), 4, 1),
            vec![2..3, 5..6]
        );
    }
}
