//! Simulator-side per-task `TaskRecord`s and the aggregate `Report` rolled up
//! at sim end.
//!
//! This lives in `biei-sim`, not the production `biei` crate: only the
//! simulator collects an in-memory record per task. Production exposes
//! Prometheus metrics via `biei_core::metrics::NodeMetrics` instead.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use biei_core::types::{RejectionReason, RouteTier, TaskOutcome, TaskResult};
use serde::Serialize;

const LATENCY_HISTOGRAM_BOUNDS_MS: &[u64] = &[
    5, 10, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1_000, 1_500, 2_000, 3_000, 5_000, 10_000,
];

#[derive(Debug, Clone)]
struct TaskRecord {
    submission_epoch: u64,
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
    submission_cohorts: BTreeMap<u64, SubmissionCohortState>,
    native_capacity_events: Vec<(Instant, usize)>,
}

#[derive(Default)]
struct SubmissionCohortState {
    submitted: u64,
    outcomes: MetricsObservation,
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

impl MetricsObservation {
    fn record(&mut self, record: &TaskRecord) {
        self.total += 1;
        if let Some(tier) = record.route_tier {
            self.completed += 1;
            *self.tier_counts.entry(tier).or_default() += 1;
            self.cold_starts += usize::from(record.cold_start);
            self.style_swaps += usize::from(record.style_swap);
            self.tasks_with_sources += usize::from(record.had_source);
            self.source_loads += usize::from(record.source_loaded);
            self.source_hits += usize::from(record.had_source && !record.source_loaded);
        } else if record.rejection_reason.is_some() {
            self.rejected += 1;
        } else {
            self.failed += 1;
        }
    }
}

/// Final outcomes grouped by the topology epoch in which requests were
/// submitted. Unlike completion windows, a cohort never changes when a slow
/// request crosses a sampling or churn boundary before terminating.
#[derive(Clone, Debug, Serialize)]
pub struct SubmissionCohortObservation {
    pub submission_epoch: u64,
    pub submitted: u64,
    pub terminal_outcomes: usize,
    pub outstanding: u64,
    pub completed: usize,
    pub rejected: usize,
    pub failed: usize,
    pub cold_starts: usize,
    pub style_swaps: usize,
    pub source_hits: usize,
    pub source_loads: usize,
    pub tier_counts: BTreeMap<String, usize>,
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

    pub(crate) fn submit(&self, submission_epoch: u64) {
        let mut state = self.state.lock().expect("metrics poisoned");
        state
            .submission_cohorts
            .entry(submission_epoch)
            .or_default()
            .submitted += 1;
    }

    pub fn record(&self, outcome: TaskOutcome, submission_epoch: u64) {
        let TaskOutcome {
            arrived_at,
            had_source,
            result,
            ..
        } = outcome;
        let record = match result {
            TaskResult::Completed { info: c, .. } => TaskRecord {
                submission_epoch,
                arrived_at,
                completed_at: Some(c.completed_at),
                native_render_started_at: Some(c.native_render_started_at),
                native_render_completed_at: Some(c.native_render_completed_at),
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
                submission_epoch,
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
                submission_epoch,
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
        state.observation.record(&record);
        let submission_epoch = record.submission_epoch;
        let cohort = state
            .submission_cohorts
            .get_mut(&submission_epoch)
            .expect("measured request cohort registered before task spawn");
        cohort.outcomes.record(&record);
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

    pub(crate) fn submission_cohorts(&self) -> Vec<SubmissionCohortObservation> {
        self.state
            .lock()
            .expect("metrics poisoned")
            .submission_cohorts
            .iter()
            .map(|(submission_epoch, cohort)| {
                let outcomes = &cohort.outcomes;
                SubmissionCohortObservation {
                    submission_epoch: *submission_epoch,
                    submitted: cohort.submitted,
                    terminal_outcomes: outcomes.total,
                    outstanding: cohort.submitted.saturating_sub(outcomes.total as u64),
                    completed: outcomes.completed,
                    rejected: outcomes.rejected,
                    failed: outcomes.failed,
                    cold_starts: outcomes.cold_starts,
                    style_swaps: outcomes.style_swaps,
                    source_hits: outcomes.source_hits,
                    source_loads: outcomes.source_loads,
                    tier_counts: outcomes
                        .tier_counts
                        .iter()
                        .map(|(tier, count)| (format!("{tier:?}"), *count))
                        .collect(),
                }
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

        let completed_latency_sla_violations = latencies.iter().filter(|&&d| d > sla).count();
        let completed_latency_sla_denominator = completed_count;
        let completed_latency_sla_violation_rate = ratio(
            completed_latency_sla_violations,
            completed_latency_sla_denominator,
        );
        let request_successes = completed_count;
        let request_success_denominator = total;
        let request_success_rate = ratio(request_successes, request_success_denominator);
        debug_assert_eq!(total, completed_count + rejected.len() + failed.len());
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
            completed_latency_sla_violations,
            completed_latency_sla_denominator,
            completed_latency_sla_violation_rate,
            request_successes,
            request_success_denominator,
            request_success_rate,
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

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    (denominator > 0).then(|| numerator as f64 / denominator as f64)
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
    let mut deltas = BTreeMap::<Instant, i32>::new();
    for record in records {
        if let (Some(start), Some(end)) = (
            record.native_render_started_at,
            record.native_render_completed_at,
        ) && end > start
        {
            *deltas.entry(start).or_default() += 1;
            *deltas.entry(end).or_default() -= 1;
        }
    }
    let mut current = 0_i32;
    let mut peak = 0_i32;
    for delta in deltas.into_values() {
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
    /// Completed requests whose observed latency exceeded `sla`.
    pub completed_latency_sla_violations: usize,
    /// Completed requests eligible for the latency SLA calculation.
    pub completed_latency_sla_denominator: usize,
    /// `completed_latency_sla_violations / completed_latency_sla_denominator`.
    pub completed_latency_sla_violation_rate: Option<f64>,
    /// Requests that completed successfully.
    pub request_successes: usize,
    /// All terminal requests: completed, rejected, or failed.
    pub request_success_denominator: usize,
    /// `request_successes / request_success_denominator`.
    pub request_success_rate: Option<f64>,
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

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::Duration};

    use biei_core::types::{RejectionReason, RequestId, RouteTier, TaskOutcome, TaskResult};
    use tokio::time::Instant;

    use super::{
        LATENCY_HISTOGRAM_BOUNDS_MS, MetricsCollector, MetricsState, TaskRecord, average_capacity,
        build_latency_histogram, peak_inflight,
    };

    fn terminal_record(
        arrived_at: Instant,
        completed_after: Option<Duration>,
        rejection_reason: Option<RejectionReason>,
        failure_error: Option<&str>,
    ) -> TaskRecord {
        let completed_at = completed_after.map(|duration| arrived_at + duration);
        TaskRecord {
            submission_epoch: 0,
            arrived_at,
            completed_at,
            native_render_started_at: completed_at,
            native_render_completed_at: completed_at,
            route_tier: completed_at.map(|_| RouteTier::Tier2HrwBl),
            style_swap: false,
            cold_start: false,
            had_source: false,
            source_loaded: false,
            admitted_at_overflow: false,
            rejection_reason,
            failure_error: failure_error.map(str::to_owned),
        }
    }

    fn report_for(records: Vec<TaskRecord>, sla: Duration) -> super::Report {
        MetricsCollector {
            state: Mutex::new(MetricsState {
                records,
                ..MetricsState::default()
            }),
        }
        .report(sla)
    }

    fn rejected_outcome(task_id: u64) -> TaskOutcome {
        TaskOutcome {
            task_id,
            request_id: RequestId::from_string(format!("cohort-{task_id}")),
            arrived_at: Instant::now(),
            had_source: false,
            deadline_stage: None,
            result: TaskResult::Rejected {
                reason: RejectionReason::QueueFull,
            },
        }
    }

    #[test]
    fn adjacent_half_open_renders_do_not_overlap_at_the_boundary() {
        let start = Instant::now();
        let boundary = start + Duration::from_millis(10);
        let end = boundary + Duration::from_millis(10);
        let mut first = terminal_record(start, Some(Duration::from_millis(10)), None, None);
        first.native_render_started_at = Some(start);
        first.native_render_completed_at = Some(boundary);
        let mut second = terminal_record(boundary, Some(Duration::from_millis(10)), None, None);
        second.native_render_started_at = Some(boundary);
        second.native_render_completed_at = Some(end);

        assert_eq!(peak_inflight(&[&first, &second]), 1);
    }

    #[test]
    fn averages_capacity_across_churn_events() {
        let start = Instant::now();
        let end = start + Duration::from_secs(10);
        let events = vec![(start, 2), (start + Duration::from_secs(5), 4)];

        assert_eq!(average_capacity(&events, Some(start), Some(end)), 3.0);
    }

    #[test]
    fn late_terminal_outcome_stays_in_its_submission_cohort() {
        let metrics = MetricsCollector::new();
        metrics.submit(0);
        metrics.submit(1);

        // The newer request terminates first. At this boundary the old epoch
        // still has one outstanding request.
        metrics.record(rejected_outcome(1), 1);
        let at_boundary = metrics.submission_cohorts();
        assert_eq!(at_boundary[0].submission_epoch, 0);
        assert_eq!(at_boundary[0].outstanding, 1);
        assert_eq!(at_boundary[1].submission_epoch, 1);
        assert_eq!(at_boundary[1].terminal_outcomes, 1);

        // Completing later must update epoch 0, not whichever epoch happens
        // to be current when record() runs.
        metrics.record(rejected_outcome(0), 0);
        let final_cohorts = metrics.submission_cohorts();
        assert_eq!(final_cohorts[0].terminal_outcomes, 1);
        assert_eq!(final_cohorts[0].rejected, 1);
        assert_eq!(final_cohorts[0].outstanding, 0);
        assert_eq!(final_cohorts[1].terminal_outcomes, 1);
    }

    #[test]
    fn sla_and_request_success_rates_expose_their_cohorts() {
        let now = Instant::now();
        let report = report_for(
            vec![
                terminal_record(now, Some(Duration::from_millis(50)), None, None),
                terminal_record(now, Some(Duration::from_millis(200)), None, None),
                terminal_record(now, None, Some(RejectionReason::QueueFull), None),
                terminal_record(now, None, None, Some("renderer failed")),
            ],
            Duration::from_millis(100),
        );

        assert_eq!(report.total, 4);
        assert_eq!(report.completed, 2);
        assert_eq!(report.rejected, 1);
        assert_eq!(report.failed, 1);
        assert_eq!(
            report.total,
            report.completed + report.rejected + report.failed
        );
        assert_eq!(report.completed_latency_sla_violations, 1);
        assert_eq!(report.completed_latency_sla_denominator, 2);
        assert_eq!(report.completed_latency_sla_violation_rate, Some(0.5));
        assert_eq!(report.request_successes, 2);
        assert_eq!(report.request_success_denominator, 4);
        assert_eq!(report.request_success_rate, Some(0.5));
    }

    #[test]
    fn rejection_heavy_run_cannot_look_perfectly_successful() {
        let now = Instant::now();
        let mut records = vec![terminal_record(
            now,
            Some(Duration::from_millis(50)),
            None,
            None,
        )];
        records.extend(
            (0..8).map(|_| terminal_record(now, None, Some(RejectionReason::NoCapacity), None)),
        );
        records.push(terminal_record(now, None, None, Some("renderer failed")));

        let report = report_for(records, Duration::from_millis(100));

        assert_eq!(report.completed_latency_sla_violations, 0);
        assert_eq!(report.completed_latency_sla_denominator, 1);
        assert_eq!(report.completed_latency_sla_violation_rate, Some(0.0));
        assert_eq!(report.request_successes, 1);
        assert_eq!(report.request_success_denominator, 10);
        assert_eq!(report.request_success_rate, Some(0.1));
    }

    #[test]
    fn empty_rate_denominators_produce_undefined_rates() {
        let report = report_for(Vec::new(), Duration::from_millis(100));

        assert_eq!(report.completed_latency_sla_denominator, 0);
        assert_eq!(report.completed_latency_sla_violation_rate, None);
        assert_eq!(report.request_success_denominator, 0);
        assert_eq!(report.request_success_rate, None);
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
