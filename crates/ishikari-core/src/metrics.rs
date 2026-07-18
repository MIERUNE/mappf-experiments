//! Prometheus-backed node metrics.
//!
//! Counters are incremented at the call sites; point-in-time gauges (cache
//! sizes, membership, drain state, cumulative backend bytes) are refreshed at
//! scrape time by the `/_internal/metrics` handler. Labels never contain
//! attacker-controlled values such as `tileset_id`; only bounded route
//! patterns and status codes are used.

use std::{sync::Arc, time::Duration};

use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder, core::Collector,
};

/// Cloneable handle to the node's Prometheus registry and metric families.
#[derive(Clone)]
pub struct NodeMetrics(Arc<Inner>);

struct Inner {
    registry: Registry,
    egress_bytes: IntCounter,
    internal_bytes: IntCounter,
    http_requests: IntCounterVec,
    http_request_duration: HistogramVec,
    tiles_served: IntCounterVec,
    tile_cache: IntCounterVec,
    peer_forward: IntCounterVec,
    peer_fetch: IntCounterVec,
    peer_fetch_duplicate_inflight: IntCounterVec,
    internal_resource_requests: IntCounterVec,
    provider_resource_cache: IntCounterVec,
    mapterhorn_resolve: IntCounterVec,
    cache_bytes: IntGaugeVec,
    backend_fetch_bytes: IntCounter,
    backend_fetch_duration: HistogramVec,
    backend_fetch_size_bytes: HistogramVec,
    backend_fetch_chunks: HistogramVec,
    backend_fetch_queue_duration: Histogram,
    backend_fetch_concurrency: IntGaugeVec,
    chunk_size_bytes: IntGauge,
    max_fetch_chunks: IntGauge,
    chunk_fetch_merge_window_seconds: Gauge,
    chunk_fetch_queue_delay: HistogramVec,
    chunk_fetch_pending_chunks: HistogramVec,
    chunk_fetch_group_waiters: HistogramVec,
    chunk_cache: IntCounterVec,
    chunk_fetch_wait: IntCounterVec,
    cpu_work_admission: IntCounterVec,
    cpu_work_queue_duration: HistogramVec,
    cpu_work: IntGaugeVec,
    terrain_source_duration: HistogramVec,
    terrain_generation_duration: HistogramVec,
    terrain_source_tiles: HistogramVec,
    terrain_output_size_bytes: HistogramVec,
    membership_size: IntGaugeVec,
    drain_state: IntGauge,
}

/// Point-in-time counters used by tests and the in-process simulator.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, serde::Serialize)]
pub struct NodeMetricsSnapshot {
    pub peer_forward_successes: u64,
    pub peer_forward_not_found: u64,
    pub peer_forward_retryable: u64,
    pub peer_forward_fatal: u64,
    pub peer_forward_backoff_skips: u64,
    pub peer_tile_fetches: u64,
    pub peer_bootstrap_fetches: u64,
    pub peer_leaf_fetches: u64,
    pub peer_provider_fetches: u64,
    pub peer_tile_duplicate_inflight: u64,
    pub peer_bootstrap_duplicate_inflight: u64,
    pub peer_leaf_duplicate_inflight: u64,
    pub peer_provider_duplicate_inflight: u64,
    pub internal_tile_requests: u64,
    pub internal_bootstrap_requests: u64,
    pub internal_leaf_requests: u64,
    pub internal_provider_requests: u64,
    pub backend_fetches: u64,
    pub backend_fetch_successes: u64,
    pub backend_fetch_not_found: u64,
    pub backend_fetch_errors: u64,
    pub backend_fetch_timeouts: u64,
    pub backend_fetched_chunks: u64,
    pub chunk_cache_hits: u64,
    pub chunk_cache_misses: u64,
    pub chunk_cache_post_fetch_hits: u64,
    pub chunk_fetch_queued: u64,
    pub chunk_fetch_joined_pending: u64,
    pub chunk_fetch_joined_inflight: u64,
    pub chunk_dispatch_immediate: u64,
    pub chunk_dispatch_window: u64,
    pub chunk_dispatch_pending_chunks: u64,
    pub chunk_waiters_released: u64,
}

/// One Prometheus histogram captured as mergeable cumulative buckets.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum: f64,
    pub buckets: Vec<HistogramBucketSnapshot>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize)]
pub struct HistogramBucketSnapshot {
    pub upper_bound: f64,
    pub cumulative_count: u64,
}

impl HistogramSnapshot {
    /// Adds another histogram with the same bucket layout.
    pub fn merge(&mut self, other: &Self) {
        if self.buckets.is_empty() {
            self.buckets = other.buckets.clone();
        } else {
            debug_assert_eq!(self.buckets.len(), other.buckets.len());
            for (target, source) in self.buckets.iter_mut().zip(&other.buckets) {
                debug_assert_eq!(target.upper_bound, source.upper_bound);
                target.cumulative_count += source.cumulative_count;
            }
        }
        self.count += other.count;
        self.sum += other.sum;
    }
}

/// Scheduler/backend histograms used by the simulator and structured tests.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct NodeHistogramSnapshot {
    pub backend_fetch_duration_seconds: HistogramSnapshot,
    pub backend_fetch_queue_duration_seconds: HistogramSnapshot,
    pub backend_fetch_size_bytes: HistogramSnapshot,
    pub backend_fetch_chunks: HistogramSnapshot,
    pub queue_delay_immediate_seconds: HistogramSnapshot,
    pub queue_delay_window_seconds: HistogramSnapshot,
    pub pending_chunks_immediate: HistogramSnapshot,
    pub pending_chunks_window: HistogramSnapshot,
    pub group_waiters: HistogramSnapshot,
}

impl NodeHistogramSnapshot {
    pub fn merge(&mut self, other: &Self) {
        self.backend_fetch_duration_seconds
            .merge(&other.backend_fetch_duration_seconds);
        self.backend_fetch_queue_duration_seconds
            .merge(&other.backend_fetch_queue_duration_seconds);
        self.backend_fetch_size_bytes
            .merge(&other.backend_fetch_size_bytes);
        self.backend_fetch_chunks.merge(&other.backend_fetch_chunks);
        self.queue_delay_immediate_seconds
            .merge(&other.queue_delay_immediate_seconds);
        self.queue_delay_window_seconds
            .merge(&other.queue_delay_window_seconds);
        self.pending_chunks_immediate
            .merge(&other.pending_chunks_immediate);
        self.pending_chunks_window
            .merge(&other.pending_chunks_window);
        self.group_waiters.merge(&other.group_waiters);
    }
}

impl NodeMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let egress_bytes = IntCounter::new(
            "ishikari_external_egress_bytes_total",
            "Bytes served to external clients",
        )
        .expect("valid metric");
        let internal_bytes = IntCounter::new(
            "ishikari_internal_egress_bytes_total",
            "Bytes served to peers over internal endpoints",
        )
        .expect("valid metric");
        let http_requests = IntCounterVec::new(
            Opts::new(
                "ishikari_http_requests_total",
                "HTTP requests by route and status",
            ),
            &["endpoint", "status"],
        )
        .expect("valid metric");
        let http_request_duration = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_http_request_duration_seconds",
                "End-to-end HTTP request duration by route and status class",
            )
            .buckets(vec![
                0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
            &["endpoint", "status_class"],
        )
        .expect("valid metric");
        let tiles_served = IntCounterVec::new(
            Opts::new(
                "ishikari_tiles_served_total",
                "External tile responses by where they were served from",
            ),
            &["source"],
        )
        .expect("valid metric");
        let tile_cache = IntCounterVec::new(
            Opts::new(
                "ishikari_tile_cache_total",
                "Tile cache lookups and inserts by outcome",
            ),
            &["outcome"],
        )
        .expect("valid metric");
        let peer_forward = IntCounterVec::new(
            Opts::new(
                "ishikari_peer_forward_total",
                "Peer forwarding attempts and backoff skips by outcome",
            ),
            &["outcome"],
        )
        .expect("valid metric");
        let peer_fetch = IntCounterVec::new(
            Opts::new(
                "ishikari_peer_fetch_total",
                "Internal peer fetch attempts by resource and outcome",
            ),
            &["resource", "outcome"],
        )
        .expect("valid metric");
        let peer_fetch_duplicate_inflight = IntCounterVec::new(
            Opts::new(
                "ishikari_peer_fetch_duplicate_inflight_total",
                "Peer fetches overlapping an identical in-flight peer/path request",
            ),
            &["resource"],
        )
        .expect("valid metric");
        let internal_resource_requests = IntCounterVec::new(
            Opts::new(
                "ishikari_internal_resource_requests_total",
                "Internal resource requests served by resource and outcome",
            ),
            &["resource", "outcome"],
        )
        .expect("valid metric");
        let provider_resource_cache = IntCounterVec::new(
            Opts::new(
                "ishikari_provider_resource_cache_total",
                "Provider resource cache activity by resource and outcome",
            ),
            &["resource", "outcome"],
        )
        .expect("valid metric");
        let mapterhorn_resolve = IntCounterVec::new(
            Opts::new(
                "ishikari_mapterhorn_resolve_total",
                "Mapterhorn composite tile resolutions by outcome",
            ),
            &["outcome"],
        )
        .expect("valid metric");
        let cache_bytes = IntGaugeVec::new(
            Opts::new("ishikari_cache_bytes", "Weighted byte size of each cache"),
            &["cache"],
        )
        .expect("valid metric");
        let backend_fetch_bytes = IntCounter::new(
            "ishikari_backend_fetch_bytes_total",
            "Cumulative bytes fetched from object storage / upstream",
        )
        .expect("valid metric");
        let backend_fetch_duration = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_backend_fetch_duration_seconds",
                "Duration of object-storage chunk group fetches by outcome",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0,
            ]),
            &["outcome"],
        )
        .expect("valid metric");
        let backend_fetch_size_bytes = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_backend_fetch_size_bytes",
                "Byte size of object-storage chunk group fetches by outcome",
            )
            .buckets(vec![
                16_384.0,
                65_536.0,
                262_144.0,
                1_048_576.0,
                2_097_152.0,
                4_194_304.0,
                8_388_608.0,
                16_777_216.0,
                33_554_432.0,
            ]),
            &["outcome"],
        )
        .expect("valid metric");
        let backend_fetch_chunks = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_backend_fetch_chunks",
                "Number of fixed-size chunks covered by each object-storage fetch by outcome",
            )
            .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0]),
            &["outcome"],
        )
        .expect("valid metric");
        let backend_fetch_queue_duration = Histogram::with_opts(
            HistogramOpts::new(
                "ishikari_backend_fetch_queue_duration_seconds",
                "Time an object-storage range fetch waits for the process-wide concurrency limit",
            )
            .buckets(vec![
                0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        )
        .expect("valid metric");
        let backend_fetch_concurrency = IntGaugeVec::new(
            Opts::new(
                "ishikari_backend_fetch_concurrency",
                "Process-wide object-storage range-fetch admission state",
            ),
            &["state"],
        )
        .expect("valid metric");
        let chunk_size_bytes = IntGauge::new(
            "ishikari_chunk_size_bytes",
            "Configured backend chunk size in bytes",
        )
        .expect("valid metric");
        let max_fetch_chunks = IntGauge::new(
            "ishikari_max_fetch_chunks",
            "Configured maximum chunks to fetch in one backend request",
        )
        .expect("valid metric");
        let chunk_fetch_merge_window_seconds = Gauge::new(
            "ishikari_chunk_fetch_merge_window_seconds",
            "Configured scheduler delay used to merge nearby chunk fetch requests",
        )
        .expect("valid metric");
        let chunk_fetch_queue_delay = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_chunk_fetch_queue_delay_seconds",
                "Time from the first queued missing chunk to backend fetch dispatch",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
            &["flush"],
        )
        .expect("valid metric");
        let chunk_fetch_pending_chunks = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_chunk_fetch_pending_chunks",
                "Number of pending chunks visible when the scheduler dispatches backend fetches",
            )
            .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]),
            &["flush"],
        )
        .expect("valid metric");
        let chunk_fetch_group_waiters = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_chunk_fetch_group_waiters",
                "Number of chunk waiters released by each completed backend fetch group",
            )
            .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]),
            &["outcome"],
        )
        .expect("valid metric");
        let chunk_cache = IntCounterVec::new(
            Opts::new(
                "ishikari_chunk_cache_total",
                "Chunk cache lookups and post-fetch reads by outcome",
            ),
            &["outcome"],
        )
        .expect("valid metric");
        let chunk_fetch_wait = IntCounterVec::new(
            Opts::new(
                "ishikari_chunk_fetch_wait_total",
                "Chunk wait registrations by whether they queued a new fetch or joined existing work",
            ),
            &["outcome"],
        )
        .expect("valid metric");
        let cpu_work_admission = IntCounterVec::new(
            Opts::new(
                "ishikari_cpu_work_admission_total",
                "CPU-heavy work admission attempts by work kind and outcome",
            ),
            &["work", "outcome"],
        )
        .expect("valid metric");
        let cpu_work_queue_duration = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_cpu_work_queue_duration_seconds",
                "Time admitted CPU-heavy work waits for a blocking-work permit",
            )
            .buckets(vec![
                0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["work"],
        )
        .expect("valid metric");
        let cpu_work = IntGaugeVec::new(
            Opts::new(
                "ishikari_cpu_work",
                "Current and configured CPU-heavy work admission state",
            ),
            &["state"],
        )
        .expect("valid metric");
        let terrain_source_duration = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_terrain_source_duration_seconds",
                "Time to fetch and decode a derived terrain product's DEM neighborhood",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
            &["product"],
        )
        .expect("valid metric");
        let terrain_generation_duration = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_terrain_generation_duration_seconds",
                "CPU time to generate and compress a derived terrain product",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
            &["product"],
        )
        .expect("valid metric");
        let terrain_source_tiles = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_terrain_source_tiles",
                "Number of present DEM source tiles used by a derived terrain generation",
            )
            .buckets(vec![1.0, 4.0, 6.0, 8.0, 9.0]),
            &["product"],
        )
        .expect("valid metric");
        let terrain_output_size_bytes = HistogramVec::new(
            HistogramOpts::new(
                "ishikari_terrain_output_size_bytes",
                "Compressed byte size of generated terrain tile responses",
            )
            .buckets(vec![
                4_096.0,
                16_384.0,
                65_536.0,
                131_072.0,
                262_144.0,
                524_288.0,
                1_048_576.0,
                2_097_152.0,
            ]),
            &["product"],
        )
        .expect("valid metric");
        let membership_size = IntGaugeVec::new(
            Opts::new("ishikari_membership_size", "Cluster member count by state"),
            &["state"],
        )
        .expect("valid metric");
        let drain_state = IntGauge::new(
            "ishikari_drain_state",
            "1 if this node is draining, otherwise 0",
        )
        .expect("valid metric");

        for collector in [
            Box::new(egress_bytes.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(internal_bytes.clone()),
            Box::new(http_requests.clone()),
            Box::new(http_request_duration.clone()),
            Box::new(tiles_served.clone()),
            Box::new(tile_cache.clone()),
            Box::new(peer_forward.clone()),
            Box::new(peer_fetch.clone()),
            Box::new(peer_fetch_duplicate_inflight.clone()),
            Box::new(internal_resource_requests.clone()),
            Box::new(provider_resource_cache.clone()),
            Box::new(mapterhorn_resolve.clone()),
            Box::new(cache_bytes.clone()),
            Box::new(backend_fetch_bytes.clone()),
            Box::new(backend_fetch_duration.clone()),
            Box::new(backend_fetch_size_bytes.clone()),
            Box::new(backend_fetch_chunks.clone()),
            Box::new(backend_fetch_queue_duration.clone()),
            Box::new(backend_fetch_concurrency.clone()),
            Box::new(chunk_size_bytes.clone()),
            Box::new(max_fetch_chunks.clone()),
            Box::new(chunk_fetch_merge_window_seconds.clone()),
            Box::new(chunk_fetch_queue_delay.clone()),
            Box::new(chunk_fetch_pending_chunks.clone()),
            Box::new(chunk_fetch_group_waiters.clone()),
            Box::new(chunk_cache.clone()),
            Box::new(chunk_fetch_wait.clone()),
            Box::new(cpu_work_admission.clone()),
            Box::new(cpu_work_queue_duration.clone()),
            Box::new(cpu_work.clone()),
            Box::new(terrain_source_duration.clone()),
            Box::new(terrain_generation_duration.clone()),
            Box::new(terrain_source_tiles.clone()),
            Box::new(terrain_output_size_bytes.clone()),
            Box::new(membership_size.clone()),
            Box::new(drain_state.clone()),
        ] {
            registry.register(collector).expect("unique metric");
        }

        Self(Arc::new(Inner {
            registry,
            egress_bytes,
            internal_bytes,
            http_requests,
            http_request_duration,
            tiles_served,
            tile_cache,
            peer_forward,
            peer_fetch,
            peer_fetch_duplicate_inflight,
            internal_resource_requests,
            provider_resource_cache,
            mapterhorn_resolve,
            cache_bytes,
            backend_fetch_bytes,
            backend_fetch_duration,
            backend_fetch_size_bytes,
            backend_fetch_chunks,
            backend_fetch_queue_duration,
            backend_fetch_concurrency,
            chunk_size_bytes,
            max_fetch_chunks,
            chunk_fetch_merge_window_seconds,
            chunk_fetch_queue_delay,
            chunk_fetch_pending_chunks,
            chunk_fetch_group_waiters,
            chunk_cache,
            chunk_fetch_wait,
            cpu_work_admission,
            cpu_work_queue_duration,
            cpu_work,
            terrain_source_duration,
            terrain_generation_duration,
            terrain_source_tiles,
            terrain_output_size_bytes,
            membership_size,
            drain_state,
        }))
    }

    pub fn add_egress_bytes(&self, bytes: u64) {
        self.0.egress_bytes.inc_by(bytes);
    }

    pub fn add_internal_bytes(&self, bytes: u64) {
        self.0.internal_bytes.inc_by(bytes);
    }

    pub fn egress_bytes(&self) -> u64 {
        self.0.egress_bytes.get()
    }

    pub fn internal_bytes(&self) -> u64 {
        self.0.internal_bytes.get()
    }

    /// Records one completed HTTP request against a bounded route pattern.
    pub fn record_http(&self, endpoint: &str, status: u16, duration: Duration) {
        self.record_http_request(endpoint, status);
        self.record_http_duration(endpoint, status, duration);
    }

    /// Records an HTTP request count without adding a duration observation.
    pub fn record_http_request(&self, endpoint: &str, status: u16) {
        self.0
            .http_requests
            .with_label_values(&[endpoint, &status.to_string()])
            .inc();
    }

    /// Records an HTTP duration observation without incrementing request count.
    pub fn record_http_duration(&self, endpoint: &str, status: u16, duration: Duration) {
        self.0
            .http_request_duration
            .with_label_values(&[endpoint, status_class(status)])
            .observe(duration.as_secs_f64());
    }

    /// Records one external tile response by its served-from source.
    pub fn record_tile_served(&self, source: &str) {
        self.0.tiles_served.with_label_values(&[source]).inc();
    }

    /// Records one tile-cache event.
    pub fn record_tile_cache(&self, outcome: &str) {
        self.0.tile_cache.with_label_values(&[outcome]).inc();
    }

    /// Records one peer forwarding outcome or one routing skip due to backoff.
    pub fn record_peer_forward(&self, outcome: &str) {
        self.0.peer_forward.with_label_values(&[outcome]).inc();
    }

    /// Records one internal peer network attempt by bounded resource kind.
    pub fn record_peer_fetch(&self, resource: &str, outcome: &str) {
        self.0
            .peer_fetch
            .with_label_values(&[resource, outcome])
            .inc();
    }

    /// Records a peer fetch that overlaps an identical peer/path request.
    pub fn record_peer_fetch_duplicate_inflight(&self, resource: &str) {
        self.0
            .peer_fetch_duplicate_inflight
            .with_label_values(&[resource])
            .inc();
    }

    /// Records one internal resource request served by this node.
    pub fn record_internal_resource_request(&self, resource: &str, outcome: &str) {
        self.0
            .internal_resource_requests
            .with_label_values(&[resource, outcome])
            .inc();
    }

    /// Records provider-cache activity for the bounded style/glyph/sprite kinds.
    pub fn record_provider_resource_cache(&self, resource: &str, outcome: &str) {
        self.0
            .provider_resource_cache
            .with_label_values(&[resource, outcome])
            .inc();
    }

    /// Records one Mapterhorn composite resolution outcome: `base`, `detail`,
    /// `detail_negative` (archive absent), or `detail_error` (transient probe
    /// failure, not cached).
    pub fn record_mapterhorn(&self, outcome: &str) {
        self.0
            .mapterhorn_resolve
            .with_label_values(&[outcome])
            .inc();
    }

    /// Sets the weighted byte size gauge for a named cache.
    pub fn set_cache_bytes(&self, cache: &str, bytes: u64) {
        self.0
            .cache_bytes
            .with_label_values(&[cache])
            .set(bytes as i64);
    }

    /// Advances the backend-fetch counter to a cumulative total.
    ///
    /// The source value lives in the storage layer as a monotonic cumulative
    /// count; this folds it into a real Prometheus counter at scrape time. Both
    /// reset to 0 together on process restart, so `rate()` stays correct.
    pub fn sync_backend_fetch_bytes(&self, cumulative: u64) {
        let current = self.0.backend_fetch_bytes.get();
        if cumulative > current {
            self.0.backend_fetch_bytes.inc_by(cumulative - current);
        }
    }

    /// Records one object-store chunk group fetch.
    pub fn record_backend_fetch(&self, outcome: &str, duration: Duration, chunks: u64, bytes: u64) {
        self.0
            .backend_fetch_duration
            .with_label_values(&[outcome])
            .observe(duration.as_secs_f64());
        self.0
            .backend_fetch_size_bytes
            .with_label_values(&[outcome])
            .observe(bytes as f64);
        self.0
            .backend_fetch_chunks
            .with_label_values(&[outcome])
            .observe(chunks as f64);
    }

    /// Exposes the process-wide backend-fetch concurrency ceiling.
    pub fn set_backend_fetch_concurrency_limit(&self, limit: usize) {
        self.0
            .backend_fetch_concurrency
            .with_label_values(&["active"])
            .set(0);
        self.0
            .backend_fetch_concurrency
            .with_label_values(&["waiting"])
            .set(0);
        self.0
            .backend_fetch_concurrency
            .with_label_values(&["limit"])
            .set(limit as i64);
    }

    /// Adjusts the current backend-fetch admission state (`active` or `waiting`).
    pub fn adjust_backend_fetch_concurrency(&self, state: &str, delta: i64) {
        self.0
            .backend_fetch_concurrency
            .with_label_values(&[state])
            .add(delta);
    }

    /// Records time spent waiting for the process-wide backend-fetch permit.
    pub fn record_backend_fetch_queue(&self, duration: Duration) {
        self.0
            .backend_fetch_queue_duration
            .observe(duration.as_secs_f64());
    }

    /// Exposes backend chunking configuration for comparing deployments.
    pub fn set_chunk_config(&self, chunk_size_bytes: u64, max_fetch_chunks: u64) {
        self.0.chunk_size_bytes.set(chunk_size_bytes as i64);
        self.0.max_fetch_chunks.set(max_fetch_chunks as i64);
    }

    /// Exposes the configured merge window used by the chunk fetch scheduler.
    pub fn set_chunk_fetch_merge_window(&self, duration: Duration) {
        self.0
            .chunk_fetch_merge_window_seconds
            .set(duration.as_secs_f64());
    }

    /// Records one scheduler dispatch after coalescing pending chunk requests.
    pub fn record_chunk_fetch_dispatch(
        &self,
        flush: &str,
        queue_delay: Duration,
        pending_chunks: usize,
    ) {
        self.0
            .chunk_fetch_queue_delay
            .with_label_values(&[flush])
            .observe(queue_delay.as_secs_f64());
        self.0
            .chunk_fetch_pending_chunks
            .with_label_values(&[flush])
            .observe(pending_chunks as f64);
    }

    /// Records how many chunk waiters were satisfied by a completed backend group.
    pub fn record_chunk_fetch_group_waiters(&self, outcome: &str, waiters: usize) {
        self.0
            .chunk_fetch_group_waiters
            .with_label_values(&[outcome])
            .observe(waiters as f64);
    }

    /// Records one chunk cache lookup/read outcome.
    pub fn record_chunk_cache(&self, outcome: &str) {
        self.0.chunk_cache.with_label_values(&[outcome]).inc();
    }

    /// Records one required missing chunk's relationship to pending/inflight work.
    pub fn record_chunk_fetch_wait(&self, outcome: &str) {
        self.0.chunk_fetch_wait.with_label_values(&[outcome]).inc();
    }

    /// Records admission or shedding for one of the fixed CPU-work kinds.
    pub fn record_cpu_work_admission(&self, work: &str, outcome: &str) {
        self.0
            .cpu_work_admission
            .with_label_values(&[work, outcome])
            .inc();
    }

    /// Records how long admitted work waited for a CPU-work permit.
    pub fn record_cpu_work_queue_duration(&self, work: &str, duration: Duration) {
        self.0
            .cpu_work_queue_duration
            .with_label_values(&[work])
            .observe(duration.as_secs_f64());
    }

    /// Sets current and configured CPU-work admission values.
    pub fn set_cpu_work(&self, inflight: usize, running: usize, concurrency: usize, max: usize) {
        for (state, value) in [
            ("inflight", inflight),
            ("running", running),
            ("concurrency", concurrency),
            ("max_inflight", max),
        ] {
            self.0
                .cpu_work
                .with_label_values(&[state])
                .set(value as i64);
        }
    }

    /// Records the successful cold-generation cost for one fixed terrain product.
    pub fn record_terrain_generation(
        &self,
        product: &str,
        source_duration: Duration,
        generation_duration: Duration,
        source_tiles: usize,
        output_bytes: usize,
    ) {
        self.0
            .terrain_source_duration
            .with_label_values(&[product])
            .observe(source_duration.as_secs_f64());
        self.0
            .terrain_generation_duration
            .with_label_values(&[product])
            .observe(generation_duration.as_secs_f64());
        self.0
            .terrain_source_tiles
            .with_label_values(&[product])
            .observe(source_tiles as f64);
        self.0
            .terrain_output_size_bytes
            .with_label_values(&[product])
            .observe(output_bytes as f64);
    }

    /// Returns a structured snapshot without parsing Prometheus text output.
    pub fn snapshot(&self) -> NodeMetricsSnapshot {
        let backend_count = |outcome: &str| {
            self.0
                .backend_fetch_duration
                .with_label_values(&[outcome])
                .get_sample_count()
        };
        let backend_fetch_successes = backend_count("success");
        let backend_fetch_not_found = backend_count("not_found");
        let backend_fetch_errors = backend_count("error");
        let backend_fetch_timeouts = backend_count("timeout");
        let counter =
            |metric: &IntCounterVec, label: &str| metric.with_label_values(&[label]).get();
        let dispatch_count = |flush: &str| {
            self.0
                .chunk_fetch_queue_delay
                .with_label_values(&[flush])
                .get_sample_count()
        };
        let peer_fetch_count = |resource: &str| {
            ["success", "not_found", "retryable", "fatal"]
                .into_iter()
                .map(|outcome| {
                    self.0
                        .peer_fetch
                        .with_label_values(&[resource, outcome])
                        .get()
                })
                .sum()
        };
        let internal_request_count = |resource: &str| {
            ["success", "not_found", "retryable", "error"]
                .into_iter()
                .map(|outcome| {
                    self.0
                        .internal_resource_requests
                        .with_label_values(&[resource, outcome])
                        .get()
                })
                .sum()
        };

        NodeMetricsSnapshot {
            peer_forward_successes: counter(&self.0.peer_forward, "success"),
            peer_forward_not_found: counter(&self.0.peer_forward, "not_found"),
            peer_forward_retryable: counter(&self.0.peer_forward, "retryable"),
            peer_forward_fatal: counter(&self.0.peer_forward, "fatal"),
            peer_forward_backoff_skips: counter(&self.0.peer_forward, "backoff"),
            peer_tile_fetches: peer_fetch_count("tile"),
            peer_bootstrap_fetches: peer_fetch_count("bootstrap"),
            peer_leaf_fetches: peer_fetch_count("leaf"),
            peer_provider_fetches: ["style", "glyph", "sprite", "derived", "other"]
                .into_iter()
                .map(peer_fetch_count)
                .sum(),
            peer_tile_duplicate_inflight: self
                .0
                .peer_fetch_duplicate_inflight
                .with_label_values(&["tile"])
                .get(),
            peer_bootstrap_duplicate_inflight: self
                .0
                .peer_fetch_duplicate_inflight
                .with_label_values(&["bootstrap"])
                .get(),
            peer_leaf_duplicate_inflight: self
                .0
                .peer_fetch_duplicate_inflight
                .with_label_values(&["leaf"])
                .get(),
            peer_provider_duplicate_inflight: ["style", "glyph", "sprite", "derived", "other"]
                .into_iter()
                .map(|resource| {
                    self.0
                        .peer_fetch_duplicate_inflight
                        .with_label_values(&[resource])
                        .get()
                })
                .sum(),
            internal_tile_requests: internal_request_count("tile"),
            internal_bootstrap_requests: internal_request_count("bootstrap"),
            internal_leaf_requests: internal_request_count("leaf"),
            internal_provider_requests: ["style", "glyph", "sprite", "derived", "other"]
                .into_iter()
                .map(internal_request_count)
                .sum(),
            backend_fetches: backend_fetch_successes
                + backend_fetch_not_found
                + backend_fetch_errors
                + backend_fetch_timeouts,
            backend_fetch_successes,
            backend_fetch_not_found,
            backend_fetch_errors,
            backend_fetch_timeouts,
            backend_fetched_chunks: self
                .0
                .backend_fetch_chunks
                .with_label_values(&["success"])
                .get_sample_sum()
                .round() as u64,
            chunk_cache_hits: counter(&self.0.chunk_cache, "hit"),
            chunk_cache_misses: counter(&self.0.chunk_cache, "miss"),
            chunk_cache_post_fetch_hits: counter(&self.0.chunk_cache, "post_fetch_hit"),
            chunk_fetch_queued: counter(&self.0.chunk_fetch_wait, "queued"),
            chunk_fetch_joined_pending: counter(&self.0.chunk_fetch_wait, "joined_pending"),
            chunk_fetch_joined_inflight: counter(&self.0.chunk_fetch_wait, "joined_inflight"),
            chunk_dispatch_immediate: dispatch_count("immediate"),
            chunk_dispatch_window: dispatch_count("window"),
            chunk_dispatch_pending_chunks: ["immediate", "window"]
                .into_iter()
                .map(|flush| {
                    self.0
                        .chunk_fetch_pending_chunks
                        .with_label_values(&[flush])
                        .get_sample_sum()
                        .round() as u64
                })
                .sum(),
            chunk_waiters_released: ["success", "error"]
                .into_iter()
                .map(|outcome| {
                    self.0
                        .chunk_fetch_group_waiters
                        .with_label_values(&[outcome])
                        .get_sample_sum()
                        .round() as u64
                })
                .sum(),
        }
    }

    /// Returns mergeable backend/scheduler histogram buckets.
    pub fn histogram_snapshot(&self) -> NodeHistogramSnapshot {
        NodeHistogramSnapshot {
            backend_fetch_duration_seconds: merge_histograms(
                &self.0.backend_fetch_duration,
                &["success", "not_found", "error", "timeout"],
            ),
            backend_fetch_queue_duration_seconds: histogram_snapshot(
                &self.0.backend_fetch_queue_duration,
            ),
            backend_fetch_size_bytes: merge_histograms(
                &self.0.backend_fetch_size_bytes,
                &["success", "not_found", "error", "timeout"],
            ),
            backend_fetch_chunks: merge_histograms(
                &self.0.backend_fetch_chunks,
                &["success", "not_found", "error", "timeout"],
            ),
            queue_delay_immediate_seconds: histogram_snapshot(
                &self
                    .0
                    .chunk_fetch_queue_delay
                    .with_label_values(&["immediate"]),
            ),
            queue_delay_window_seconds: histogram_snapshot(
                &self
                    .0
                    .chunk_fetch_queue_delay
                    .with_label_values(&["window"]),
            ),
            pending_chunks_immediate: histogram_snapshot(
                &self
                    .0
                    .chunk_fetch_pending_chunks
                    .with_label_values(&["immediate"]),
            ),
            pending_chunks_window: histogram_snapshot(
                &self
                    .0
                    .chunk_fetch_pending_chunks
                    .with_label_values(&["window"]),
            ),
            group_waiters: merge_histograms(
                &self.0.chunk_fetch_group_waiters,
                &["success", "error"],
            ),
        }
    }

    /// Sets the live/dead membership gauges.
    pub fn set_membership(&self, live: i64, dead: i64) {
        self.0
            .membership_size
            .with_label_values(&["live"])
            .set(live);
        self.0
            .membership_size
            .with_label_values(&["dead"])
            .set(dead);
    }

    /// Sets the drain-state gauge.
    pub fn set_drain(&self, draining: bool) {
        self.0.drain_state.set(draining as i64);
    }

    /// Encodes the registry in Prometheus text exposition format.
    pub fn encode(&self) -> String {
        let metric_families = self.0.registry.gather();
        let mut buffer = Vec::new();
        if TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .is_err()
        {
            return String::new();
        }
        String::from_utf8(buffer).unwrap_or_default()
    }
}

fn status_class(status: u16) -> &'static str {
    match status / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

fn merge_histograms(metrics: &HistogramVec, values: &[&str]) -> HistogramSnapshot {
    let mut merged = HistogramSnapshot::default();
    for value in values {
        merged.merge(&histogram_snapshot(&metrics.with_label_values(&[*value])));
    }
    merged
}

fn histogram_snapshot(histogram: &Histogram) -> HistogramSnapshot {
    let families = histogram.collect();
    let Some(metric) = families
        .first()
        .and_then(|family| family.get_metric().first())
    else {
        return HistogramSnapshot::default();
    };
    let histogram = metric.get_histogram();
    HistogramSnapshot {
        count: histogram.get_sample_count(),
        sum: histogram.get_sample_sum(),
        buckets: histogram
            .get_bucket()
            .iter()
            .map(|bucket| HistogramBucketSnapshot {
                upper_bound: bucket.upper_bound(),
                cumulative_count: bucket.cumulative_count(),
            })
            .collect(),
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}
