//! In-flight chunk fetch coordination and waiter management.

use std::{
    collections::{BTreeSet, HashMap},
    ops::Range,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use tokio::{
    sync::{Mutex, oneshot},
    time,
};
use tracing::debug;

use crate::{interned::TilesetId, metrics::NodeMetrics};

use super::{
    fetcher::{ChunkFetchError, ChunkFetcher},
    store::ChunkedStore,
};

const FETCH_MERGE_WINDOW: Duration = Duration::from_millis(10);
const IMMEDIATE_CHUNK_INDEX: u64 = 0;
const MAX_CHUNK_GAP: u64 = 1;
const MAX_CONCURRENT_FETCHES_PER_TILESET: usize = 32;

/// Coordinates shared inflight chunk fetches.
#[derive(Clone)]
pub struct ChunkFetchCoordinator {
    fetcher: ChunkFetcher,
    metrics: NodeMetrics,
    max_fetch_chunks: u64,
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
    waiters: HashMap<u64, Vec<oneshot::Sender<Result<(), ChunkFetchError>>>>,
    /// Whether the per-tileset scheduler task is currently running.
    scheduler_running: bool,
    /// Number of backend fetches currently inflight for this tileset.
    inflight_fetch_count: usize,
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
    Throttled,
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
    ) -> Vec<(
        oneshot::Receiver<Result<(), ChunkFetchError>>,
        EnqueueOutcome,
    )> {
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
            joined.push((rx, outcome));
        }
        joined
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
            return DispatchDecision::Throttled;
        }
        let available_slots = MAX_CONCURRENT_FETCHES_PER_TILESET - self.inflight_fetch_count;
        let groups: Vec<Range<u64>> =
            contiguous_chunk_ranges(&self.pending_chunks, max_fetch_chunks, MAX_CHUNK_GAP)
                .into_iter()
                .take(available_slots)
                .collect();
        if groups.is_empty() {
            return DispatchDecision::Throttled;
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
    /// chunks, and delivers `result` to every waiter. Returns the number of
    /// waiters released (for the group-waiters metric).
    fn complete_group(
        &mut self,
        chunk_range: Range<u64>,
        result: &Result<(), ChunkFetchError>,
    ) -> usize {
        self.inflight_fetch_count = self.inflight_fetch_count.saturating_sub(1);
        let mut released_waiters = 0;
        for chunk_index in chunk_range.start..chunk_range.end {
            self.inflight_chunks.remove(&chunk_index);
            if let Some(waiters) = self.waiters.remove(&chunk_index) {
                released_waiters += waiters.len();
                for waiter in waiters {
                    let _ = waiter.send(result.clone());
                }
            }
        }
        released_waiters
    }
}

impl ChunkFetchCoordinator {
    pub fn new(fetcher: ChunkFetcher, max_fetch_chunks: u64, metrics: NodeMetrics) -> Self {
        metrics.set_chunk_fetch_merge_window(FETCH_MERGE_WINDOW);
        Self {
            fetcher,
            metrics,
            max_fetch_chunks,
            tileset_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn chunk_size(&self) -> u64 {
        self.fetcher.chunk_size()
    }

    pub fn received_bytes(&self) -> u64 {
        self.fetcher.received_bytes()
    }

    pub fn metrics(&self) -> &NodeMetrics {
        &self.metrics
    }

    /// Fetches chunks for a tileset while coalescing concurrent requests.
    pub async fn fetch_chunks(
        &self,
        store: ChunkedStore,
        tileset_id: &TilesetId,
        required_chunks: &[u64],
        archive_len: u64,
    ) -> std::result::Result<(), ChunkFetchError> {
        let mut receivers = Vec::with_capacity(required_chunks.len());
        let queued_at = Instant::now();

        {
            let mut tileset_states = self.tileset_states.lock().await;
            let tileset_state = tileset_states.entry(tileset_id.clone()).or_default();
            let was_idle = tileset_state.is_idle();
            tileset_state.archive_len = tileset_state.archive_len.max(archive_len);

            for (rx, outcome) in tileset_state.enqueue_chunks(required_chunks, queued_at) {
                self.metrics.record_chunk_fetch_wait(outcome.metric_label());
                receivers.push(rx);
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

        for receiver in receivers {
            let result = receiver.await.map_err(|_| {
                ChunkFetchError::Message(anyhow!("chunk fetch waiter dropped").to_string())
            })?;
            result?;
        }

        Ok(())
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
                time::sleep(FETCH_MERGE_WINDOW).await;
            }

            let dispatch = {
                let mut tileset_states = self.tileset_states.lock().await;
                let Some(state) = tileset_states.get_mut(&tileset_id) else {
                    return;
                };
                match state.select_dispatch_groups(self.max_fetch_chunks) {
                    DispatchDecision::Idle => {
                        if state.is_drainable() {
                            tileset_states.remove(&tileset_id);
                            debug!(tileset_id = %tileset_id, "removed empty chunk fetch state");
                        }
                        return;
                    }
                    DispatchDecision::Throttled => None,
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
                        Some((groups, archive_len))
                    }
                }
            };

            let Some((groups, archive_len)) = dispatch else {
                continue;
            };

            for chunk_range in groups {
                let coordinator = self.clone();
                let tileset_id = tileset_id.clone();
                let store = store.clone();
                tokio::spawn(async move {
                    coordinator
                        .run_fetch_chunk_group(store, tileset_id, chunk_range, archive_len)
                        .await;
                });
            }
        }
    }

    async fn run_fetch_chunk_group(
        &self,
        store: ChunkedStore,
        tileset_id: TilesetId,
        chunk_range: Range<u64>,
        archive_len: u64,
    ) {
        let result = self
            .fetcher
            .fetch_chunk_group(&tileset_id, chunk_range.clone(), archive_len)
            .await
            .and_then(|bytes| {
                store
                    .cache_chunk_group(&tileset_id, chunk_range.clone(), archive_len, bytes)
                    .map_err(|error| ChunkFetchError::Message(error.to_string()))
            });

        let mut tileset_states = self.tileset_states.lock().await;
        let Some(state) = tileset_states.get_mut(&tileset_id) else {
            return;
        };

        let released_waiters = state.complete_group(chunk_range.clone(), &result);
        self.metrics.record_chunk_fetch_group_waiters(
            if result.is_ok() { "success" } else { "error" },
            released_waiters,
        );

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

        if state.is_drainable() {
            tileset_states.remove(&tileset_id);
            debug!(tileset_id = %tileset_id, "removed empty chunk fetch state");
        }
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Instant;

    use super::*;

    fn set(values: &[u64]) -> BTreeSet<u64> {
        values.iter().copied().collect()
    }

    #[test]
    fn enqueue_queues_new_chunks_then_joins_existing() {
        let mut state = TilesetFetchState::default();
        let now = Instant::now();

        let queued = state.enqueue_chunks(&[5], now);
        assert!(matches!(queued[0].1, EnqueueOutcome::Queued));
        assert!(state.pending_chunks.contains(&5));
        assert!(state.first_pending_at.is_some());

        // A second waiter for the same still-pending chunk joins it.
        let joined = state.enqueue_chunks(&[5], now);
        assert!(matches!(joined[0].1, EnqueueOutcome::JoinedPending));

        // A chunk already inflight is joined, not re-queued.
        state.inflight_chunks.insert(9);
        let inflight = state.enqueue_chunks(&[9], now);
        assert!(matches!(inflight[0].1, EnqueueOutcome::JoinedInflight));
        assert!(!state.pending_chunks.contains(&9));
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
            .map(|(rx, _)| rx)
            .collect();
        let _ = state.select_dispatch_groups(4);

        let released = state.complete_group(1..3, &Ok(()));
        assert_eq!(released, 2);
        assert_eq!(state.inflight_fetch_count, 0);
        assert!(state.inflight_chunks.is_empty());
        for rx in &mut receivers {
            assert!(matches!(rx.try_recv(), Ok(Ok(()))));
        }
        assert!(state.is_drainable());
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
