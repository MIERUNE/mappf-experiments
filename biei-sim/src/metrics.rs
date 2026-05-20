//! Simulator-side per-task `TaskRecord`s and the aggregate `Report` rolled up
//! at sim end.
//!
//! This lives in `biei-sim`, not the production `biei` crate: only the
//! simulator collects an in-memory record per task. Production exposes
//! Prometheus metrics via `biei::metrics::NodeMetrics` instead.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use biei::types::{RejectionReason, RouteTier, TaskOutcome, TaskResult};

#[derive(Debug, Clone)]
struct TaskRecord {
    arrived_at: Instant,
    completed_at: Option<Instant>,
    cpu_started_at: Option<Instant>,
    cpu_completed_at: Option<Instant>,
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
    records: Mutex<Vec<TaskRecord>>,
    cpu_render_permits_total: usize,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self::with_cpu_render_permits(0)
    }

    pub fn with_cpu_render_permits(cpu_render_permits_total: usize) -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            cpu_render_permits_total,
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
                cpu_started_at: Some(c.cpu_started_at),
                cpu_completed_at: Some(c.cpu_completed_at),
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
                cpu_started_at: None,
                cpu_completed_at: None,
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
                cpu_started_at: None,
                cpu_completed_at: None,
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
        self.records.lock().expect("metrics poisoned").push(record);
    }

    pub fn report(&self, sla: Duration) -> Report {
        let records = self.records.lock().expect("metrics poisoned");
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
        let cpu_render_busy = completed.iter().fold(Duration::ZERO, |acc, r| {
            match (r.cpu_started_at, r.cpu_completed_at) {
                (Some(start), Some(end)) if end >= start => acc + end.duration_since(start),
                _ => acc,
            }
        });
        let cpu_render_avg_inflight = if elapsed.as_secs_f64() > 0.0 {
            cpu_render_busy.as_secs_f64() / elapsed.as_secs_f64()
        } else {
            0.0
        };
        let cpu_render_peak_inflight = peak_inflight(&completed);
        let cpu_render_utilization_pct = if self.cpu_render_permits_total > 0 {
            cpu_render_avg_inflight / self.cpu_render_permits_total as f64 * 100.0
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
            cold_starts,
            style_swaps,
            overflow_admissions,
            tasks_with_sources,
            source_loads,
            source_hits,
            tier_counts,
            elapsed,
            cpu_render_permits_total: self.cpu_render_permits_total,
            cpu_render_busy,
            cpu_render_avg_inflight,
            cpu_render_peak_inflight,
            cpu_render_utilization_pct,
        }
    }
}

fn peak_inflight(records: &[&TaskRecord]) -> usize {
    let mut events = Vec::new();
    for r in records {
        if let (Some(start), Some(end)) = (r.cpu_started_at, r.cpu_completed_at)
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
    pub cpu_render_permits_total: usize,
    pub cpu_render_busy: Duration,
    pub cpu_render_avg_inflight: f64,
    pub cpu_render_peak_inflight: usize,
    pub cpu_render_utilization_pct: f64,
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
             CPU render util:   {:.1}% avg={:.2} peak={} permits={} busy={:?}\n\
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
            self.cpu_render_utilization_pct,
            self.cpu_render_avg_inflight,
            self.cpu_render_peak_inflight,
            self.cpu_render_permits_total,
            self.cpu_render_busy,
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
