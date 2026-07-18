//! Simulator-side per-task `TaskRecord`s and the aggregate `Report` rolled up
//! at sim end.
//!
//! This lives in `biei-sim`, not the production `biei` crate: only the
//! simulator collects an in-memory record per task. Production exposes
//! Prometheus metrics via `biei_core::metrics::NodeMetrics` instead.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use biei_core::types::{RejectionReason, RouteTier, TaskOutcome, TaskResult};

const LATENCY_HISTOGRAM_BOUNDS_MS: &[u64] = &[
    5, 10, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1_000, 1_500, 2_000, 3_000, 5_000, 10_000,
];

#[derive(Debug, Clone)]
struct TaskRecord {
    arrived_at: Instant,
    completed_at: Option<Instant>,
    native_render_started_at: Option<Instant>,
    native_render_completed_at: Option<Instant>,
    route_tier: Option<RouteTier>,
    style_swap: bool,
    cold_start: bool,
    /// Whether the task carried an addlayer source.
    had_source: bool,
    /// Whether processing the task required a source cache miss
    /// (= called `ensure_source`).
    source_loaded: bool,
    /// True if the task was admitted while the chosen worker's queue already
    /// sat at or above the soft limit.
    admitted_at_overflow: bool,
    rejection_reason: Option<RejectionReason>,
    failure_error: Option<String>,
}

/// Synchronous, mutex-guarded collector. Each `record` call pushes one
/// `TaskRecord` derived from a `TaskOutcome`. Workload tasks call this
/// directly after their `handle_incoming(...).await`.
pub struct MetricsCollector {
    state: Mutex<MetricsState>,
}

#[derive(Default)]
struct MetricsState {
    records: Vec<TaskRecord>,
    observation: MetricsObservation,
    native_capacity_events: Vec<(Instant, usize)>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MetricsObservation {
    pub total: usize,
    pub completed: usize,
    pub rejected: usize,
    pub failed: usize,
    pub cold_starts: usize,
    pub style_swaps: usize,
    pub tasks_with_sources: usize,
    pub source_loads: usize,
    pub source_hits: usize,
    pub tier_counts: HashMap<RouteTier, usize>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self::with_native_render_permits(0)
    }

    pub fn with_native_render_permits(native_render_permits_total: usize) -> Self {
        Self {
            state: Mutex::new(MetricsState {
                native_capacity_events: vec![(Instant::now(), native_render_permits_total)],
                ..MetricsState::default()
            }),
        }
    }

    pub fn record(&self, outcome: TaskOutcome) {
        let TaskOutcome {
            arrived_at,
            had_source,
            result,
            ..
        } = outcome;
        let record = match result {
            TaskResult::Completed { info: c, .. } => TaskRecord {
                arrived_at,
                completed_at: Some(c.completed_at),
                // Production retains historical `cpu_*` field names, but the
                // interval covers the whole native render call, including I/O.
                native_render_started_at: Some(c.cpu_started_at),
                native_render_completed_at: Some(c.cpu_completed_at),
                route_tier: Some(c.route_tier),
                style_swap: c.style_swap,
                cold_start: c.cold_start,
                had_source,
                source_loaded: c.source_loaded,
                admitted_at_overflow: c.admitted_at_overflow,
                rejection_reason: None,
                failure_error: None,
            },
            TaskResult::Rejected { reason } => TaskRecord {
                arrived_at,
                completed_at: None,
                native_render_started_at: None,
                native_render_completed_at: None,
                route_tier: None,
                style_swap: false,
                cold_start: false,
                had_source,
                source_loaded: false,
                admitted_at_overflow: false,
                rejection_reason: Some(reason),
                failure_error: None,
            },
            TaskResult::Failed { error, .. } => TaskRecord {
                arrived_at,
                completed_at: None,
                native_render_started_at: None,
                native_render_completed_at: None,
                route_tier: None,
                style_swap: false,
                cold_start: false,
                had_source,
                source_loaded: false,
                admitted_at_overflow: false,
                rejection_reason: None,
                failure_error: Some(error),
            },
        };
        let mut state = self.state.lock().expect("metrics poisoned");
        let observation = &mut state.observation;
        observation.total += 1;
        if let Some(tier) = record.route_tier {
            observation.completed += 1;
            *observation.tier_counts.entry(tier).or_default() += 1;
            observation.cold_starts += usize::from(record.cold_start);
            observation.style_swaps += usize::from(record.style_swap);
            observation.tasks_with_sources += usize::from(record.had_source);
            observation.source_loads += usize::from(record.source_loaded);
            observation.source_hits += usize::from(record.had_source && !record.source_loaded);
        } else if record.rejection_reason.is_some() {
            observation.rejected += 1;
        } else {
            observation.failed += 1;
        }
        state.records.push(record);
    }

    pub(crate) fn observation(&self) -> MetricsObservation {
        self.state
            .lock()
            .expect("metrics poisoned")
            .observation
            .clone()
    }

    pub(crate) fn completed_latencies_between(&self, start: usize, end: usize) -> Vec<Duration> {
        let state = self.state.lock().expect("metrics poisoned");
        let end = end.min(state.records.len());
        if start >= end {
            return Vec::new();
        }
        state.records[start..end]
            .iter()
            .filter_map(|record| {
                record
                    .completed_at
                    .map(|completed_at| completed_at.saturating_duration_since(record.arrived_at))
            })
            .collect()
    }

    pub(crate) fn set_native_render_permits_total(&self, total: usize) {
        let mut state = self.state.lock().expect("metrics poisoned");
        if state
            .native_capacity_events
            .last()
            .is_none_or(|(_, current)| *current != total)
        {
            state.native_capacity_events.push((Instant::now(), total));
        }
    }

    pub fn report(&self, sla: Duration) -> Report {
        let state = self.state.lock().expect("metrics poisoned");
        let records = &state.records;
        let total = records.len();
        let completed: Vec<&TaskRecord> = records
            .iter()
            .filter(|r| r.completed_at.is_some())
            .collect();
        let completed_count = completed.len();

        let mut latencies: Vec<Duration> = completed
            .iter()
            .map(|r| r.completed_at.unwrap().duration_since(r.arrived_at))
            .collect();
        latencies.sort();
        let latency_histogram = build_latency_histogram(&latencies);

        let percentile = |q: f64| -> Duration {
            if latencies.is_empty() {
                return Duration::ZERO;
            }
            let idx = ((latencies.len() - 1) as f64 * q).round() as usize;
            latencies[idx]
        };

        let first_arrival = records.iter().map(|r| r.arrived_at).min();
        let last_completion = completed.iter().filter_map(|r| r.completed_at).max();
        let elapsed = match (first_arrival, last_completion) {
            (Some(a), Some(b)) => b.duration_since(a),
            _ => Duration::ZERO,
        };
        let throughput = if elapsed.as_secs_f64() > 0.0 {
            completed_count as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let cold_starts = completed.iter().filter(|r| r.cold_start).count();
        let style_swaps = completed.iter().filter(|r| r.style_swap).count();
        let overflow_admissions = completed.iter().filter(|r| r.admitted_at_overflow).count();
        let tasks_with_sources = completed.iter().filter(|r| r.had_source).count();
        let source_loads = completed.iter().filter(|r| r.source_loaded).count();
        let source_hits = tasks_with_sources.saturating_sub(source_loads);

        let mut tier_counts: HashMap<RouteTier, usize> = HashMap::new();
        for r in records.iter() {
            if let Some(t) = r.route_tier {
                *tier_counts.entry(t).or_default() += 1;
            }
        }

        let rejected: Vec<&TaskRecord> = records
            .iter()
            .filter(|r| r.rejection_reason.is_some())
            .collect();
        let mut rejection_by_reason: HashMap<RejectionReason, usize> = HashMap::new();
        for r in &rejected {
            if let Some(reason) = r.rejection_reason {
                *rejection_by_reason.entry(reason).or_default() += 1;
            }
        }
        let failed: Vec<&TaskRecord> = records
            .iter()
            .filter(|r| r.failure_error.is_some())
            .collect();
        let mut failure_by_error: HashMap<String, usize> = HashMap::new();
        for r in &failed {
            if let Some(error) = &r.failure_error {
                *failure_by_error.entry(error.clone()).or_default() += 1;
            }
        }

        let sla_violations = latencies.iter().filter(|&&d| d > sla).count();
        let native_render_busy = completed.iter().fold(Duration::ZERO, |acc, r| {
            match (r.native_render_started_at, r.native_render_completed_at) {
                (Some(start), Some(end)) if end >= start => acc + end.duration_since(start),
                _ => acc,
            }
        });
        let native_render_avg_inflight = if elapsed.as_secs_f64() > 0.0 {
            native_render_busy.as_secs_f64() / elapsed.as_secs_f64()
        } else {
            0.0
        };
        let native_render_peak_inflight = peak_inflight(&completed);
        let native_render_permits_total = state
            .native_capacity_events
            .last()
            .map_or(0, |(_, permits)| *permits);
        let avg_native_capacity = average_capacity(
            &state.native_capacity_events,
            first_arrival,
            last_completion,
        );
        let native_render_utilization_pct = if avg_native_capacity > 0.0 {
            native_render_avg_inflight / avg_native_capacity * 100.0
        } else {
            0.0
        };

        Report {
            total,
            completed: completed_count,
            rejected: rejected.len(),
            failed: failed.len(),
            rejection_by_reason,
            failure_by_error,
            sla_violations,
            sla,
            throughput,
            latency_p50: percentile(0.50),
            latency_p90: percentile(0.90),
            latency_p95: percentile(0.95),
            latency_p99: percentile(0.99),
            latency_max: latencies.last().copied().unwrap_or(Duration::ZERO),
            latency_histogram,
            cold_starts,
            style_swaps,
            overflow_admissions,
            tasks_with_sources,
            source_loads,
            source_hits,
            tier_counts,
            elapsed,
            native_render_permits_total,
            native_render_busy,
            native_render_avg_inflight,
            native_render_peak_inflight,
            native_render_utilization_pct,
        }
    }
}

fn build_latency_histogram(latencies: &[Duration]) -> Vec<LatencyHistogramBucket> {
    let mut counts = vec![0; LATENCY_HISTOGRAM_BOUNDS_MS.len() + 1];
    for latency in latencies {
        let bucket = LATENCY_HISTOGRAM_BOUNDS_MS
            .iter()
            .position(|bound_ms| *latency <= Duration::from_millis(*bound_ms))
            .unwrap_or(LATENCY_HISTOGRAM_BOUNDS_MS.len());
        counts[bucket] += 1;
    }

    counts
        .into_iter()
        .enumerate()
        .map(|(index, count)| LatencyHistogramBucket {
            upper_bound: LATENCY_HISTOGRAM_BOUNDS_MS
                .get(index)
                .map(|bound_ms| Duration::from_millis(*bound_ms)),
            count,
        })
        .collect()
}

fn average_capacity(
    events: &[(Instant, usize)],
    start: Option<Instant>,
    end: Option<Instant>,
) -> f64 {
    let (Some(start), Some(end)) = (start, end) else {
        return events.last().map_or(0.0, |(_, value)| *value as f64);
    };
    if end <= start {
        return events.last().map_or(0.0, |(_, value)| *value as f64);
    }
    let mut current = events
        .iter()
        .take_while(|(at, _)| *at <= start)
        .last()
        .map_or(0, |(_, value)| *value);
    let mut cursor = start;
    let mut capacity_seconds = 0.0;
    for (at, value) in events.iter().filter(|(at, _)| *at > start && *at < end) {
        capacity_seconds += at.duration_since(cursor).as_secs_f64() * current as f64;
        cursor = *at;
        current = *value;
    }
    capacity_seconds += end.duration_since(cursor).as_secs_f64() * current as f64;
    capacity_seconds / end.duration_since(start).as_secs_f64()
}

fn peak_inflight(records: &[&TaskRecord]) -> usize {
    let mut events = Vec::new();
    for r in records {
        if let (Some(start), Some(end)) = (r.native_render_started_at, r.native_render_completed_at)
            && end >= start
        {
            events.push((start, 0_u8, 1_i32));
            events.push((end, 1_u8, -1_i32));
        }
    }
    events.sort_by_key(|(at, order, _)| (*at, *order));
    let mut current = 0_i32;
    let mut peak = 0_i32;
    for (_, _, delta) in events {
        current += delta;
        peak = peak.max(current);
    }
    peak as usize
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct Report {
    pub total: usize,
    pub completed: usize,
    pub rejected: usize,
    pub failed: usize,
    pub rejection_by_reason: HashMap<RejectionReason, usize>,
    pub failure_by_error: HashMap<String, usize>,
    pub sla_violations: usize,
    pub sla: Duration,
    pub throughput: f64,
    pub latency_p50: Duration,
    pub latency_p90: Duration,
    pub latency_p95: Duration,
    pub latency_p99: Duration,
    pub latency_max: Duration,
    /// Non-cumulative completed-request latency counts. The final bucket has
    /// no upper bound and contains values above 10 seconds.
    pub latency_histogram: Vec<LatencyHistogramBucket>,
    pub cold_starts: usize,
    pub style_swaps: usize,
    /// Number of tasks that landed between the soft queue limit (BL) and hard
    /// queue limit at admission time. High values mean the pool is leaning on
    /// transient absorption rather than rejecting; correlates with tail
    /// latency growth.
    pub overflow_admissions: usize,
    /// Tasks that carried an addlayer source.
    pub tasks_with_sources: usize,
    /// Count of tasks whose source was a cache miss (= had to load).
    pub source_loads: usize,
    /// `tasks_with_sources - source_loads`.
    pub source_hits: usize,
    pub tier_counts: HashMap<RouteTier, usize>,
    pub elapsed: Duration,
    pub native_render_permits_total: usize,
    pub native_render_busy: Duration,
    pub native_render_avg_inflight: f64,
    pub native_render_peak_inflight: usize,
    pub native_render_utilization_pct: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatencyHistogramBucket {
    pub upper_bound: Option<Duration>,
    pub count: usize,
}

impl Report {
    pub fn to_human_readable(&self) -> String {
        let pct = |n: usize| -> f64 {
            if self.total > 0 {
                n as f64 / self.total as f64 * 100.0
            } else {
                0.0
            }
        };
        let pct_complete = |n: usize| -> f64 {
            if self.completed > 0 {
                n as f64 / self.completed as f64 * 100.0
            } else {
                0.0
            }
        };
        let tier = |t: RouteTier| -> usize { self.tier_counts.get(&t).copied().unwrap_or(0) };
        let reasons = if self.rejection_by_reason.is_empty() {
            String::from("-")
        } else {
            let mut entries: Vec<_> = self.rejection_by_reason.iter().collect();
            entries.sort_by_key(|(_, v)| std::cmp::Reverse(**v));
            entries
                .iter()
                .map(|(r, n)| format!("{:?}={}", r, n))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let failures = if self.failure_by_error.is_empty() {
            String::from("-")
        } else {
            let mut entries: Vec<_> = self.failure_by_error.iter().collect();
            entries.sort_by_key(|(_, v)| std::cmp::Reverse(**v));
            entries
                .iter()
                .map(|(error, n)| format!("{error}={n}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "=== Simulation Report ===\n\
             Total submitted:   {}\n\
             Completed:         {} ({:.2}%)\n\
             Rejected:          {} ({:.2}%)  reasons: {}\n\
             Failed:            {} ({:.2}%)  errors: {}\n\
             SLA ({:?}):         {} violations ({:.2}% of completed)\n\
             Elapsed (sim):     {:?}\n\
             Throughput:        {:.2} req/s\n\
             Native render util:   {:.1}% avg={:.2} peak={} permits={} busy={:?}\n\
             Latency p50:       {:?}\n\
             Latency p90:       {:?}\n\
             Latency p95:       {:?}\n\
             Latency p99:       {:?}\n\
             Latency max:       {:?}\n\
             Tier 1 (warm):     {} ({:.2}%)\n\
             Tier 2 (hrw/bl):   {} ({:.2}%)\n\
             Tier 3 (drain):    {} ({:.2}%)\n\
             Tier 4 (overflow): {} ({:.2}%)\n\
             Cold starts:       {} ({:.2}%)\n\
             Style swaps:       {} ({:.2}%)\n\
             Overflow admits:   {} ({:.2}% of completed) — queued past soft limit\n\
             Sources (tasks/loads/hits): {} / {} / {} ({})\n\
             Tasks w/ sources:  {} ({:.2}%)\n",
            self.total,
            self.completed,
            pct(self.completed),
            self.rejected,
            pct(self.rejected),
            reasons,
            self.failed,
            pct(self.failed),
            failures,
            self.sla,
            self.sla_violations,
            pct_complete(self.sla_violations),
            self.elapsed,
            self.throughput,
            self.native_render_utilization_pct,
            self.native_render_avg_inflight,
            self.native_render_peak_inflight,
            self.native_render_permits_total,
            self.native_render_busy,
            self.latency_p50,
            self.latency_p90,
            self.latency_p95,
            self.latency_p99,
            self.latency_max,
            tier(RouteTier::Tier1WarmTracking),
            pct(tier(RouteTier::Tier1WarmTracking)),
            tier(RouteTier::Tier2HrwBl),
            pct(tier(RouteTier::Tier2HrwBl)),
            tier(RouteTier::Tier3DrainSwap),
            pct(tier(RouteTier::Tier3DrainSwap)),
            tier(RouteTier::Tier4Overflow),
            pct(tier(RouteTier::Tier4Overflow)),
            self.cold_starts,
            pct_complete(self.cold_starts),
            self.style_swaps,
            pct_complete(self.style_swaps),
            self.overflow_admissions,
            pct_complete(self.overflow_admissions),
            self.tasks_with_sources,
            self.source_loads,
            self.source_hits,
            if self.tasks_with_sources > 0 {
                format!(
                    "{:.2}% hit rate",
                    self.source_hits as f64 / self.tasks_with_sources as f64 * 100.0
                )
            } else {
                String::from("n/a")
            },
            self.tasks_with_sources,
            pct_complete(self.tasks_with_sources),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{LATENCY_HISTOGRAM_BOUNDS_MS, average_capacity, build_latency_histogram};

    #[test]
    fn averages_capacity_across_churn_events() {
        let start = Instant::now();
        let end = start + Duration::from_secs(10);
        let events = vec![(start, 2), (start + Duration::from_secs(5), 4)];

        assert_eq!(average_capacity(&events, Some(start), Some(end)), 3.0);
    }

    #[test]
    fn latency_histogram_is_fixed_and_accounts_for_every_sample() {
        let latencies = [
            Duration::ZERO,
            Duration::from_millis(5),
            Duration::from_millis(6),
            Duration::from_millis(10_001),
        ];

        let buckets = build_latency_histogram(&latencies);

        assert_eq!(buckets.len(), LATENCY_HISTOGRAM_BOUNDS_MS.len() + 1);
        assert_eq!(buckets.iter().map(|bucket| bucket.count).sum::<usize>(), 4);
        assert_eq!(buckets[0].count, 2);
        assert_eq!(buckets[1].count, 1);
        assert_eq!(buckets.last().expect("overflow bucket").count, 1);
        assert_eq!(buckets.last().expect("overflow bucket").upper_bound, None);
    }
}
