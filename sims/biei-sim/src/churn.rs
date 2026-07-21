use anyhow::{Result, ensure};
pub use mmpf_cluster::simulation::{ChurnEvent, ChurnPlan};
use serde::Serialize;

use crate::harness::WorkloadCluster;
use crate::metrics::{MetricsCollector, SubmissionCohortObservation};

#[derive(Debug, Serialize)]
pub struct ChurnReport {
    pub request_clock: &'static str,
    pub sample_every_requests: u64,
    pub submitted_total: u64,
    pub submitted_measured: u64,
    pub events: Vec<AppliedChurnEvent>,
    pub unapplied_events: Vec<ChurnEvent>,
    pub submission_cohorts: Vec<SubmissionCohortObservation>,
    pub samples: Vec<ChurnSample>,
}

#[derive(Debug, Serialize)]
pub struct AppliedChurnEvent {
    pub requested_at_request: u64,
    pub applied_at_request: u64,
    pub action: &'static str,
    pub node_id: String,
    pub active_nodes: usize,
}

#[derive(Debug, Serialize)]
pub struct ChurnSample {
    pub at_request: u64,
    /// Topology epoch assigned to a request submitted after this boundary.
    pub submission_epoch: u64,
    pub reason: &'static str,
    #[serde(flatten)]
    pub observation: ClusterObservation,
    pub completion_window: CompletionWindowObservation,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ClusterObservation {
    pub submitted_total: u64,
    pub submitted_measured: u64,
    pub terminal_outcomes_measured: usize,
    pub outstanding_measured: u64,
    pub completed: usize,
    pub rejected: usize,
    pub failed: usize,
    pub active_nodes: usize,
    pub draining_nodes: usize,
    pub total_queue_depth: usize,
    pub loaded_workers: usize,
    pub cold_starts: usize,
    pub style_swaps: usize,
    pub source_hits: usize,
    pub source_loads: usize,
    pub tier_counts: std::collections::BTreeMap<String, usize>,
    pub forward_attempts: u64,
    pub forward_successes: u64,
    pub nodes: Vec<NodeObservation>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct CompletionWindowObservation {
    /// Requests whose submission was observed between the two boundaries.
    pub submissions_started: u64,
    /// Terminal outcomes observed between the two boundaries. These may
    /// belong to an earlier submission epoch.
    pub terminal_outcomes_observed: usize,
    pub completed_observed: usize,
    pub rejected_observed: usize,
    pub failed_observed: usize,
    pub cold_starts_observed: usize,
    pub style_swaps_observed: usize,
    pub source_hits_observed: usize,
    pub source_loads_observed: usize,
    pub tier_counts: std::collections::BTreeMap<String, usize>,
    /// Physical forwarding attempts started during this observation window.
    pub forward_attempts_started: u64,
    /// Physical forwarding attempts that completed successfully during this
    /// window. The corresponding attempt may have started in an earlier one.
    pub forward_successes_observed: u64,
    pub latency_samples: usize,
    pub latency_p50_ms: Option<f64>,
    pub latency_p99_ms: Option<f64>,
    pub latency_max_ms: Option<f64>,
}

impl CompletionWindowObservation {
    fn between(
        current: &ClusterObservation,
        previous: &ClusterObservation,
        mut completed_latencies: Vec<std::time::Duration>,
    ) -> Self {
        let tier_counts = current
            .tier_counts
            .iter()
            .map(|(tier, current)| {
                let previous = previous.tier_counts.get(tier).copied().unwrap_or(0);
                (tier.clone(), current.saturating_sub(previous))
            })
            .collect();
        completed_latencies.sort_unstable();
        let percentile_ms = |quantile: f64| {
            if completed_latencies.is_empty() {
                return None;
            }
            let index = ((completed_latencies.len() - 1) as f64 * quantile).round() as usize;
            Some(completed_latencies[index].as_secs_f64() * 1_000.0)
        };
        let completed_observed = current.completed.saturating_sub(previous.completed);
        let rejected_observed = current.rejected.saturating_sub(previous.rejected);
        let failed_observed = current.failed.saturating_sub(previous.failed);
        Self {
            submissions_started: current
                .submitted_measured
                .saturating_sub(previous.submitted_measured),
            terminal_outcomes_observed: completed_observed
                .saturating_add(rejected_observed)
                .saturating_add(failed_observed),
            completed_observed,
            rejected_observed,
            failed_observed,
            cold_starts_observed: current.cold_starts.saturating_sub(previous.cold_starts),
            style_swaps_observed: current.style_swaps.saturating_sub(previous.style_swaps),
            source_hits_observed: current.source_hits.saturating_sub(previous.source_hits),
            source_loads_observed: current.source_loads.saturating_sub(previous.source_loads),
            tier_counts,
            forward_attempts_started: current
                .forward_attempts
                .saturating_sub(previous.forward_attempts),
            forward_successes_observed: current
                .forward_successes
                .saturating_sub(previous.forward_successes),
            latency_samples: completed_latencies.len(),
            latency_p50_ms: percentile_ms(0.50),
            latency_p99_ms: percentile_ms(0.99),
            latency_max_ms: completed_latencies
                .last()
                .map(|duration| duration.as_secs_f64() * 1_000.0),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NodeObservation {
    pub node_id: String,
    pub active: bool,
    pub draining: bool,
    pub submitted_total: u64,
    pub completed_total: u64,
    pub rejected_total: u64,
    pub failed_total: u64,
    pub submitted_measured: u64,
    pub completed_measured: u64,
    pub rejected_measured: u64,
    pub failed_measured: u64,
    pub queue_depth: usize,
    pub loaded_workers: usize,
}

pub(crate) struct ChurnTracker {
    plan: ChurnPlan,
    next_event: usize,
    next_sample: u64,
    measurement_started: bool,
    submission_epoch: u64,
    previous_observation: ClusterObservation,
    report: ChurnReport,
}

impl ChurnTracker {
    pub(crate) fn new(
        plan: ChurnPlan,
        sample_every_requests: u64,
        initial: ClusterObservation,
    ) -> Result<Self> {
        plan.validate()?;
        ensure!(
            sample_every_requests > 0,
            "sample_every_requests must be greater than zero"
        );
        let initial_sample = ChurnSample {
            at_request: 0,
            submission_epoch: 0,
            reason: "initial",
            observation: initial.clone(),
            completion_window: CompletionWindowObservation::default(),
        };
        Ok(Self {
            plan,
            next_event: 0,
            next_sample: sample_every_requests,
            measurement_started: false,
            submission_epoch: 0,
            previous_observation: initial,
            report: ChurnReport {
                request_clock: "measured_after_warmup",
                sample_every_requests,
                submitted_total: 0,
                submitted_measured: 0,
                events: Vec::new(),
                unapplied_events: Vec::new(),
                submission_cohorts: Vec::new(),
                samples: vec![initial_sample],
            },
        })
    }

    pub(crate) async fn before_request(
        &mut self,
        cluster: &mut WorkloadCluster,
        metrics: &MetricsCollector,
        at_request: u64,
    ) -> Result<u64> {
        if cluster.reap_drained_nodes().await? > 0 {
            metrics.set_native_render_permits_total(cluster.native_render_permits_total());
        }
        if !self.measurement_started {
            self.sample(
                cluster.observation(metrics),
                metrics,
                at_request,
                "measurement_start",
            );
            self.measurement_started = true;
        }
        while let Some(event) = self.plan.events.get(self.next_event).cloned() {
            if event.at_request() > at_request {
                break;
            }
            self.sample(
                cluster.observation(metrics),
                metrics,
                at_request,
                "pre_event",
            );
            let (action, node_id) = match &event {
                ChurnEvent::Add { .. } => ("add", cluster.add_node().await?),
                ChurnEvent::Remove { node_id, .. } => {
                    cluster.remove_node(node_id).await?;
                    ("remove", node_id.clone())
                }
            };
            metrics.set_native_render_permits_total(cluster.native_render_permits_total());
            self.submission_epoch = self.submission_epoch.saturating_add(1);
            let observation = cluster.observation(metrics);
            self.report.events.push(AppliedChurnEvent {
                requested_at_request: event.at_request(),
                applied_at_request: at_request,
                action,
                node_id,
                active_nodes: observation.active_nodes,
            });
            self.sample(observation, metrics, at_request, "post_event");
            self.next_event += 1;
        }
        if at_request >= self.next_sample {
            self.sample(
                cluster.observation(metrics),
                metrics,
                at_request,
                "periodic",
            );
            while self.next_sample <= at_request {
                self.next_sample += self.report.sample_every_requests;
            }
        }
        Ok(self.submission_epoch)
    }

    pub(crate) async fn after_workload(
        &mut self,
        cluster: &mut WorkloadCluster,
        metrics: &MetricsCollector,
    ) -> Result<()> {
        if cluster.reap_drained_nodes().await? > 0 {
            metrics.set_native_render_permits_total(cluster.native_render_permits_total());
        }
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
        cluster: &WorkloadCluster,
        metrics: &MetricsCollector,
        at_request: u64,
        submitted_total: u64,
    ) -> ChurnReport {
        self.report.submitted_total = submitted_total;
        self.report.submitted_measured = at_request;
        self.report
            .unapplied_events
            .extend_from_slice(&self.plan.events[self.next_event..]);
        self.sample(cluster.observation(metrics), metrics, at_request, "final");
        self.report.submission_cohorts = metrics.submission_cohorts();
        self.report
    }

    fn sample(
        &mut self,
        observation: ClusterObservation,
        metrics: &MetricsCollector,
        at_request: u64,
        reason: &'static str,
    ) {
        let completed_latencies = metrics.completed_latencies_between(
            self.previous_observation.terminal_outcomes_measured,
            observation.terminal_outcomes_measured,
        );
        let completion_window = CompletionWindowObservation::between(
            &observation,
            &self.previous_observation,
            completed_latencies,
        );
        self.previous_observation = observation.clone();
        self.report.samples.push(ChurnSample {
            at_request,
            submission_epoch: self.submission_epoch,
            reason,
            observation,
            completion_window,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ClusterObservation, CompletionWindowObservation};

    #[test]
    fn completion_window_summarizes_only_new_terminal_outcomes_and_latencies() {
        let previous = ClusterObservation {
            terminal_outcomes_measured: 10,
            completed: 8,
            ..ClusterObservation::default()
        };
        let current = ClusterObservation {
            terminal_outcomes_measured: 13,
            completed: 11,
            ..ClusterObservation::default()
        };
        let window = CompletionWindowObservation::between(
            &current,
            &previous,
            vec![
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(100),
            ],
        );

        assert_eq!(window.terminal_outcomes_observed, 3);
        assert_eq!(window.completed_observed, 3);
        assert_eq!(window.latency_samples, 3);
        assert_eq!(window.latency_p50_ms, Some(20.0));
        assert_eq!(window.latency_p99_ms, Some(100.0));
        assert_eq!(window.latency_max_ms, Some(100.0));
    }

    #[test]
    fn completion_window_names_allow_cross_boundary_forward_success() {
        let previous = ClusterObservation {
            forward_attempts: 10,
            forward_successes: 5,
            ..ClusterObservation::default()
        };
        let current = ClusterObservation {
            forward_attempts: 10,
            forward_successes: 6,
            ..ClusterObservation::default()
        };

        let window = CompletionWindowObservation::between(&current, &previous, Vec::new());

        assert_eq!(window.forward_attempts_started, 0);
        assert_eq!(window.forward_successes_observed, 1);
    }
}
