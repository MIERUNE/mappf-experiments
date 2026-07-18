use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::harness::WorkloadCluster;
use crate::metrics::MetricsCollector;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChurnPlan {
    pub events: Vec<ChurnEvent>,
}

impl ChurnPlan {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file =
            File::open(path).with_context(|| format!("open churn plan {}", path.display()))?;
        let plan: Self = serde_json::from_reader(BufReader::new(file))
            .with_context(|| format!("parse churn plan {}", path.display()))?;
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<()> {
        let mut previous = 0;
        for (index, event) in self.events.iter().enumerate() {
            ensure!(
                index == 0 || event.at_request() >= previous,
                "churn events must be ordered by at_request"
            );
            previous = event.at_request();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ChurnEvent {
    Add { at_request: u64 },
    Remove { at_request: u64, node_id: String },
}

impl ChurnEvent {
    pub(crate) fn at_request(&self) -> u64 {
        match self {
            Self::Add { at_request } | Self::Remove { at_request, .. } => *at_request,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ChurnReport {
    pub request_clock: &'static str,
    pub sample_every_requests: u64,
    pub submitted_total: u64,
    pub submitted_measured: u64,
    pub events: Vec<AppliedChurnEvent>,
    pub unapplied_events: Vec<ChurnEvent>,
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
    pub reason: &'static str,
    #[serde(flatten)]
    pub observation: ClusterObservation,
    pub interval: IntervalObservation,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ClusterObservation {
    pub submitted_total: u64,
    pub submitted_measured: u64,
    pub measured_outcomes: usize,
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
pub struct IntervalObservation {
    pub submitted_measured: u64,
    pub completed: usize,
    pub rejected: usize,
    pub failed: usize,
    pub cold_starts: usize,
    pub style_swaps: usize,
    pub source_hits: usize,
    pub source_loads: usize,
    pub tier_counts: std::collections::BTreeMap<String, usize>,
    pub forward_attempts: u64,
    pub forward_successes: u64,
    pub latency_samples: usize,
    pub latency_p50_ms: Option<f64>,
    pub latency_p99_ms: Option<f64>,
    pub latency_max_ms: Option<f64>,
}

impl IntervalObservation {
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
        Self {
            submitted_measured: current
                .submitted_measured
                .saturating_sub(previous.submitted_measured),
            completed: current.completed.saturating_sub(previous.completed),
            rejected: current.rejected.saturating_sub(previous.rejected),
            failed: current.failed.saturating_sub(previous.failed),
            cold_starts: current.cold_starts.saturating_sub(previous.cold_starts),
            style_swaps: current.style_swaps.saturating_sub(previous.style_swaps),
            source_hits: current.source_hits.saturating_sub(previous.source_hits),
            source_loads: current.source_loads.saturating_sub(previous.source_loads),
            tier_counts,
            forward_attempts: current
                .forward_attempts
                .saturating_sub(previous.forward_attempts),
            forward_successes: current
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
            reason: "initial",
            observation: initial.clone(),
            interval: IntervalObservation::default(),
        };
        Ok(Self {
            plan,
            next_event: 0,
            next_sample: sample_every_requests,
            measurement_started: false,
            previous_observation: initial,
            report: ChurnReport {
                request_clock: "measured_after_warmup",
                sample_every_requests,
                submitted_total: 0,
                submitted_measured: 0,
                events: Vec::new(),
                unapplied_events: Vec::new(),
                samples: vec![initial_sample],
            },
        })
    }

    pub(crate) async fn before_request(
        &mut self,
        cluster: &mut WorkloadCluster,
        metrics: &MetricsCollector,
        at_request: u64,
    ) -> Result<()> {
        if cluster.reap_drained_nodes() > 0 {
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
        Ok(())
    }

    pub(crate) fn after_workload(
        &mut self,
        cluster: &mut WorkloadCluster,
        metrics: &MetricsCollector,
    ) {
        if cluster.reap_drained_nodes() > 0 {
            metrics.set_native_render_permits_total(cluster.native_render_permits_total());
        }
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
            self.previous_observation.measured_outcomes,
            observation.measured_outcomes,
        );
        let interval = IntervalObservation::between(
            &observation,
            &self.previous_observation,
            completed_latencies,
        );
        self.previous_observation = observation.clone();
        self.report.samples.push(ChurnSample {
            at_request,
            reason,
            observation,
            interval,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ChurnEvent, ChurnPlan, ClusterObservation, IntervalObservation};

    #[test]
    fn parses_flat_churn_events() {
        let plan: ChurnPlan = serde_json::from_str(
            r#"{"events":[{"at_request":10,"action":"add"},{"at_request":20,"action":"remove","node_id":"node-0"}]}"#,
        )
        .expect("churn plan");
        assert!(matches!(plan.events[0], ChurnEvent::Add { at_request: 10 }));
        plan.validate().expect("valid plan");
    }

    #[test]
    fn rejects_out_of_order_events() {
        let plan = ChurnPlan {
            events: vec![
                ChurnEvent::Add { at_request: 20 },
                ChurnEvent::Add { at_request: 10 },
            ],
        };
        assert!(plan.validate().is_err());
    }

    #[test]
    fn interval_observation_summarizes_only_new_completed_latencies() {
        let previous = ClusterObservation {
            measured_outcomes: 10,
            completed: 8,
            ..ClusterObservation::default()
        };
        let current = ClusterObservation {
            measured_outcomes: 13,
            completed: 11,
            ..ClusterObservation::default()
        };
        let interval = IntervalObservation::between(
            &current,
            &previous,
            vec![
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(100),
            ],
        );

        assert_eq!(interval.completed, 3);
        assert_eq!(interval.latency_samples, 3);
        assert_eq!(interval.latency_p50_ms, Some(20.0));
        assert_eq!(interval.latency_p99_ms, Some(100.0));
        assert_eq!(interval.latency_max_ms, Some(100.0));
    }
}
