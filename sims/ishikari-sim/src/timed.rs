use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use mmpf_common::rng::{splitmix64, uniform_unit};
use serde::Serialize;
use tokio::{task::JoinSet, time::Instant};

use crate::{
    TraceEntry,
    cluster::{PreparedRequest, ServedRequest, SimCluster, execute_request},
    report::SourceCategory,
    viewport_batch_ranges,
};

const MAX_FAILURE_SAMPLES: usize = 10;
const MAX_FAILURE_SAMPLE_CHARS: usize = 512;

#[derive(Debug, Clone, Serialize)]
pub struct TimedConfig {
    pub think_time_ms: u64,
    pub think_jitter_ms: u64,
    pub request_overhead_ms: u64,
    pub request_timeout_ms: u64,
    pub seed: u64,
}

impl Default for TimedConfig {
    fn default() -> Self {
        Self {
            think_time_ms: 1_200,
            think_jitter_ms: 500,
            request_overhead_ms: 1,
            request_timeout_ms: 10_000,
            seed: 1,
        }
    }
}

/// Exact latency observations from requests that completed successfully.
#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencySummary {
    pub requests: usize,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimedReport {
    pub config: TimedConfig,
    /// Total attempts. This always reconciles with `completed + failed + timed_out`.
    pub requests: usize,
    pub completed: usize,
    pub failed: usize,
    /// Bounded diagnostic examples for failed attempts.
    pub failure_samples: Vec<String>,
    pub timed_out: usize,
    /// Right-censoring bound for each timed-out request. Timeout observations are
    /// excluded from the exact `latency` distributions below.
    pub timeout_lower_bound_ms: f64,
    pub elapsed_ms: f64,
    pub throughput_rps: f64,
    pub latency: LatencySummary,
    pub latency_by_source: BTreeMap<String, LatencySummary>,
    pub node_peak_inflight: Vec<usize>,
}

struct PreparedBatch {
    step: u64,
    requests: Vec<PreparedRequest>,
}

struct RequestRecord {
    latency: Duration,
    result: RequestResult,
}

enum RequestResult {
    Completed(ServedRequest),
    Failed(anyhow::Error),
    TimedOut,
}

struct UserResult {
    records: Vec<RequestRecord>,
    completed_at: Instant,
}

struct InflightTracker {
    current: Vec<AtomicUsize>,
    peak: Vec<AtomicUsize>,
}

impl InflightTracker {
    fn new(nodes: usize) -> Self {
        Self {
            current: (0..nodes).map(|_| AtomicUsize::new(0)).collect(),
            peak: (0..nodes).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    fn enter(&self, node: usize) {
        let current = self.current[node].fetch_add(1, Ordering::Relaxed) + 1;
        self.peak[node].fetch_max(current, Ordering::Relaxed);
    }

    fn leave(&self, node: usize) {
        self.current[node].fetch_sub(1, Ordering::Relaxed);
    }

    fn peaks(&self) -> Vec<usize> {
        self.peak
            .iter()
            .map(|value| value.load(Ordering::Relaxed))
            .collect()
    }
}

pub async fn run_timed_trace(
    cluster: &mut SimCluster,
    entries: &[TraceEntry],
    config: TimedConfig,
) -> Result<TimedReport> {
    ensure!(
        config.request_timeout_ms > 0,
        "request timeout must be positive"
    );

    let batches = prepare_user_batches(cluster, entries)?;
    let tracker = Arc::new(InflightTracker::new(cluster.node_count()));
    let started_at = Instant::now();
    let mut users = JoinSet::new();

    for (user, batches) in batches {
        let tracker = tracker.clone();
        let config = config.clone();
        users.spawn(run_user(user, batches, config, tracker));
    }

    let mut records = Vec::with_capacity(entries.len());
    let mut completed_at = started_at;
    while let Some(result) = users.join_next().await {
        let user = result.context("timed user task failed")??;
        completed_at = completed_at.max(user.completed_at);
        records.extend(user.records);
    }

    let requests = records.len();
    ensure!(
        requests == entries.len(),
        "timed runner produced {requests} records for {} input requests",
        entries.len()
    );
    let mut completed = 0;
    let mut failed = 0;
    let mut failure_samples = Vec::new();
    let mut timed_out = 0;
    let mut latencies = Vec::with_capacity(records.len());
    let mut by_source: BTreeMap<SourceCategory, Vec<Duration>> = BTreeMap::new();
    for record in records {
        match record.result {
            RequestResult::Completed(served) => {
                completed += 1;
                latencies.push(record.latency);
                by_source
                    .entry(SourceCategory::from_tile_source(served.source))
                    .or_default()
                    .push(record.latency);
                cluster.record(served);
            }
            RequestResult::Failed(error) => {
                failed += 1;
                if failure_samples.len() < MAX_FAILURE_SAMPLES {
                    failure_samples.push(bounded_failure_sample(&error));
                }
            }
            RequestResult::TimedOut => timed_out += 1,
        }
    }
    debug_assert_eq!(requests, completed + failed + timed_out);

    let elapsed = completed_at.saturating_duration_since(started_at);
    let throughput_rps = if elapsed.is_zero() {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    let latency_by_source = by_source
        .into_iter()
        .map(|(source, values)| (source.report_label().to_owned(), summarize(values)))
        .collect();
    let timeout_lower_bound_ms = config.request_timeout_ms as f64;

    Ok(TimedReport {
        config,
        requests,
        completed,
        failed,
        failure_samples,
        timed_out,
        timeout_lower_bound_ms,
        elapsed_ms: duration_ms(elapsed),
        throughput_rps,
        latency: summarize(latencies),
        latency_by_source,
        node_peak_inflight: tracker.peaks(),
    })
}

fn prepare_user_batches(
    cluster: &SimCluster,
    entries: &[TraceEntry],
) -> Result<BTreeMap<usize, Vec<PreparedBatch>>> {
    let mut users: BTreeMap<usize, Vec<PreparedBatch>> = BTreeMap::new();
    for range in viewport_batch_ranges(entries)? {
        let batch = &entries[range];
        let first = batch.first().context("viewport batch is empty")?;
        let requests = batch
            .iter()
            .map(|entry| cluster.prepare(entry))
            .collect::<Result<Vec<_>>>()?;
        users.entry(first.user).or_default().push(PreparedBatch {
            step: first.step,
            requests,
        });
    }
    for batches in users.values_mut() {
        batches.sort_by_key(|batch| batch.step);
    }
    Ok(users)
}

async fn run_user(
    user: usize,
    batches: Vec<PreparedBatch>,
    config: TimedConfig,
    tracker: Arc<InflightTracker>,
) -> Result<UserResult> {
    let mut records = Vec::new();
    let mut previous_step = None;

    for batch in batches {
        if let Some(step) = previous_step {
            for iteration in step..batch.step {
                tokio::time::sleep(think_time(&config, user, iteration)).await;
            }
        }
        previous_step = Some(batch.step);

        let arrived_at = Instant::now();
        let mut requests = JoinSet::new();
        for request in batch.requests {
            let tracker = tracker.clone();
            let overhead = Duration::from_millis(config.request_overhead_ms);
            let timeout = Duration::from_millis(config.request_timeout_ms);
            requests.spawn(async move {
                let node = request.node_index;
                tracker.enter(node);
                let result = tokio::time::timeout(timeout, async move {
                    if !overhead.is_zero() {
                        tokio::time::sleep(overhead).await;
                    }
                    execute_request(request).await
                })
                .await;
                tracker.leave(node);
                let result = match result {
                    Ok(Ok(served)) => RequestResult::Completed(served),
                    Ok(Err(error)) => RequestResult::Failed(error),
                    Err(_) => RequestResult::TimedOut,
                };
                Ok::<_, anyhow::Error>(RequestRecord {
                    latency: Instant::now().saturating_duration_since(arrived_at),
                    result,
                })
            });
        }
        while let Some(result) = requests.join_next().await {
            records.push(result.context("timed request task failed")??);
        }
    }

    Ok(UserResult {
        records,
        completed_at: Instant::now(),
    })
}

fn think_time(config: &TimedConfig, user: usize, iteration: u64) -> Duration {
    let unit = uniform_unit(splitmix64(
        config.seed ^ (user as u64).rotate_left(17) ^ iteration.rotate_left(31),
    ));
    let signed = unit * 2.0 - 1.0;
    let millis = config.think_time_ms as f64 + signed * config.think_jitter_ms as f64;
    Duration::from_secs_f64((millis.max(0.0)) / 1_000.0)
}

fn bounded_failure_sample(error: &anyhow::Error) -> String {
    format!("{error:#}")
        .chars()
        .take(MAX_FAILURE_SAMPLE_CHARS)
        .collect()
}

fn summarize(mut latencies: Vec<Duration>) -> LatencySummary {
    latencies.sort_unstable();
    LatencySummary {
        requests: latencies.len(),
        p50_ms: percentile(&latencies, 0.50),
        p90_ms: percentile(&latencies, 0.90),
        p95_ms: percentile(&latencies, 0.95),
        p99_ms: percentile(&latencies, 0.99),
        max_ms: latencies.last().copied().map(duration_ms).unwrap_or(0.0),
    }
}

fn percentile(values: &[Duration], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    duration_ms(values[index])
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{TimedConfig, percentile, run_timed_trace, summarize, think_time};
    use crate::{ClusterConfig, SimCluster, TraceEntry};

    #[test]
    fn latency_summary_uses_nearest_rank_positions() {
        let values = (1..=100).map(Duration::from_millis).collect();
        let summary = summarize(values);

        assert_eq!(summary.p50_ms, 51.0);
        assert_eq!(summary.p90_ms, 90.0);
        assert_eq!(summary.p99_ms, 99.0);
        assert_eq!(summary.max_ms, 100.0);
        assert_eq!(percentile(&[], 0.5), 0.0);
    }

    #[test]
    fn think_time_is_deterministic_and_bounded() {
        let config = TimedConfig::default();
        let first = think_time(&config, 7, 11);

        assert_eq!(first, think_time(&config, 7, 11));
        assert!(first >= Duration::from_millis(700));
        assert!(first <= Duration::from_millis(1_700));
    }

    #[tokio::test(start_paused = true)]
    async fn timed_runner_starts_users_concurrently_and_enforces_timeout() {
        let mut cluster = SimCluster::new(ClusterConfig {
            node_count: 1,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");
        let entries: Vec<_> = (0..2)
            .map(|user| TraceEntry {
                step: 0,
                user,
                ordinal: 0,
                tileset: "missing".to_string(),
                z: 0,
                x: 0,
                y: 0,
                entry_node: Some(0),
            })
            .collect();

        let report = run_timed_trace(
            &mut cluster,
            &entries,
            TimedConfig {
                think_time_ms: 0,
                think_jitter_ms: 0,
                request_overhead_ms: 20,
                request_timeout_ms: 5,
                seed: 1,
            },
        )
        .await
        .expect("timed run");

        assert_eq!(report.requests, 2);
        assert_eq!(report.completed, 0);
        assert_eq!(report.failed, 0);
        assert!(report.failure_samples.is_empty());
        assert_eq!(report.timed_out, 2);
        assert_eq!(
            report.requests,
            report.completed + report.failed + report.timed_out
        );
        assert_eq!(report.timeout_lower_bound_ms, 5.0);
        assert_eq!(report.latency.requests, 0);
        assert_eq!(report.latency.p50_ms, 0.0);
        assert_eq!(report.latency_by_source.len(), 0);
        assert_eq!(report.node_peak_inflight, [2]);
    }
}
