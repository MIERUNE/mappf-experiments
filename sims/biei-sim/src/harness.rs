//! Simulator cluster lifecycle, including dynamic node churn.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Result, ensure};
use rand::{Rng, RngExt};
use tokio::sync::Semaphore;

use crate::calibrated_costs::EmpiricalCostModel;
use crate::channel_transport::{ChannelTransport, NodeEntry, NodeRegistry};
use crate::chitchat_bus::ChitchatGossipNetwork;
use crate::churn::{ChurnPlan, ChurnReport, ChurnTracker, ClusterObservation, NodeObservation};
use crate::config::SimConfig;
use crate::metrics::{MetricsCollector, Report};
use crate::report::RunReport;
use crate::stub_renderer::StubRenderer;
use crate::workload::run_workload;
use biei_core::gossip::GossipBus;
use biei_core::internal_transport::InternalTransport;
use biei_core::node::{DispatcherEntropy, Node, NodeSpawn};
use biei_core::renderer::{BoxRenderer, NoopProfilePreparer};
use biei_core::style_catalog::StyleCatalog;
use biei_core::types::{NodeId, TaskOutcome, TaskResult};

const SEED_MIX: u64 = 0x9E37_79B9_7F4A_7C15;
const NODE_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub struct Simulation {
    pub config: SimConfig,
    calibration_model: Option<Arc<EmpiricalCostModel>>,
}

pub struct SimulationOptions {
    pub churn_plan: Option<ChurnPlan>,
    pub sample_every_requests: u64,
}

impl Default for SimulationOptions {
    fn default() -> Self {
        Self {
            churn_plan: None,
            sample_every_requests: 1_000,
        }
    }
}

impl Simulation {
    pub fn new(config: SimConfig) -> Self {
        Self {
            config,
            calibration_model: None,
        }
    }

    pub fn with_calibration_model(mut self, model: EmpiricalCostModel) -> Self {
        self.calibration_model = Some(Arc::new(model));
        self
    }

    pub async fn run_report(self, options: SimulationOptions) -> Result<RunReport> {
        let config = self.config.clone();
        let empirical_sampling = self
            .calibration_model
            .as_ref()
            .map(|model| model.coverage());
        let (result, churn) = self
            .execute(options.churn_plan, options.sample_every_requests)
            .await?;
        let mut report = RunReport::new(&config, &result, churn);
        if let Some(coverage) = empirical_sampling {
            report.config["empirical_sampling"] = serde_json::to_value(coverage)
                .expect("empirical sampling coverage is always serializable");
        }
        Ok(report)
    }

    async fn execute(
        self,
        churn_plan: Option<ChurnPlan>,
        sample_every_requests: u64,
    ) -> Result<(Report, Option<ChurnReport>)> {
        self.config.validate()?;
        if let Some(plan) = &churn_plan {
            plan.validate()?;
            ensure!(
                sample_every_requests > 0,
                "sample_every_requests must be greater than zero"
            );
        }
        let native_render_permits = self
            .config
            .cluster
            .resolved_native_render_permits_per_node();
        let metrics = Arc::new(MetricsCollector::with_native_render_permits(
            native_render_permits * self.config.node_count,
        ));
        let mut cluster =
            WorkloadCluster::new(self.config.clone(), self.calibration_model.clone()).await?;
        let mut churn = churn_plan
            .map(|plan| {
                ChurnTracker::new(plan, sample_every_requests, cluster.observation(&metrics))
            })
            .transpose()?;

        let workload_result = run_workload(
            self.config.workload.clone(),
            &mut cluster,
            metrics.clone(),
            self.config.costs.sla,
            self.config.seed,
            churn.as_mut(),
        )
        .await;
        let workload = match workload_result {
            Ok(workload) => workload,
            Err(error) => {
                return match cluster.shutdown().await {
                    Ok(()) => Err(error),
                    Err(shutdown_error) => Err(error.context(format!(
                        "simulated membership cleanup also failed: {shutdown_error}"
                    ))),
                };
            }
        };
        let churn_report = churn.map(|tracker| {
            tracker.finish(
                &cluster,
                &metrics,
                workload.submitted_measured,
                workload.submitted_total,
            )
        });
        let report = metrics.report(self.config.costs.sla);
        cluster.shutdown().await?;
        Ok((report, churn_report))
    }
}

pub(crate) struct WorkloadCluster {
    config: SimConfig,
    gossip: ChitchatGossipNetwork,
    style_catalog: Arc<StyleCatalog>,
    registry: Arc<NodeRegistry>,
    transport: Arc<ChannelTransport>,
    calibration_model: Option<Arc<EmpiricalCostModel>>,
    nodes: Vec<ActiveNode>,
    draining: Vec<ActiveNode>,
    retired: Vec<RetiredNode>,
    next_node_index: usize,
}

struct ActiveNode {
    id: NodeId,
    node: Node,
    _registry_entry: Arc<NodeEntry>,
    counters: Arc<NodeCounters>,
}

struct RetiredNode {
    id: NodeId,
    counters: Arc<NodeCounters>,
}

#[derive(Default)]
pub(crate) struct NodeCounters {
    submitted: AtomicU64,
    completed: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
    submitted_measured: AtomicU64,
    completed_measured: AtomicU64,
    rejected_measured: AtomicU64,
    failed_measured: AtomicU64,
}

impl NodeCounters {
    pub(crate) fn submit(&self, measured: bool) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
        if measured {
            self.submitted_measured.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record(&self, outcome: &TaskOutcome, measured: bool) {
        let (total, measured_counter) = match &outcome.result {
            TaskResult::Completed { .. } => (&self.completed, &self.completed_measured),
            TaskResult::Rejected { .. } => (&self.rejected, &self.rejected_measured),
            TaskResult::Failed { .. } => (&self.failed, &self.failed_measured),
        };
        total.fetch_add(1, Ordering::Relaxed);
        if measured {
            measured_counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn observation(&self, node_id: String, active: bool, draining: bool) -> NodeObservation {
        NodeObservation {
            node_id,
            active,
            draining,
            submitted_total: self.submitted.load(Ordering::Relaxed),
            completed_total: self.completed.load(Ordering::Relaxed),
            rejected_total: self.rejected.load(Ordering::Relaxed),
            failed_total: self.failed.load(Ordering::Relaxed),
            submitted_measured: self.submitted_measured.load(Ordering::Relaxed),
            completed_measured: self.completed_measured.load(Ordering::Relaxed),
            rejected_measured: self.rejected_measured.load(Ordering::Relaxed),
            failed_measured: self.failed_measured.load(Ordering::Relaxed),
            ..NodeObservation::default()
        }
    }
}

impl WorkloadCluster {
    async fn new(
        config: SimConfig,
        calibration_model: Option<Arc<EmpiricalCostModel>>,
    ) -> Result<Self> {
        let gossip =
            ChitchatGossipNetwork::new(config.gossip.publish_interval, config.costs.hop_latency);
        let catalog = StyleCatalog::new();
        catalog.set_url_template("http://simulator.local/styles/{style_id}/style.json");
        let registry = NodeRegistry::new();
        let transport = Arc::new(ChannelTransport::new(
            config.costs.hop_latency,
            registry.clone(),
        ));
        let initial_nodes = config.node_count;
        let mut cluster = Self {
            config,
            gossip,
            style_catalog: Arc::new(catalog),
            registry,
            transport,
            calibration_model,
            nodes: Vec::with_capacity(initial_nodes),
            draining: Vec::new(),
            retired: Vec::new(),
            next_node_index: 0,
        };
        for _ in 0..initial_nodes {
            if let Err(error) = cluster.add_node().await {
                return match cluster.shutdown().await {
                    Ok(()) => Err(error),
                    Err(shutdown_error) => Err(error.context(format!(
                        "simulated membership cleanup also failed: {shutdown_error}"
                    ))),
                };
            }
        }
        Ok(cluster)
    }

    pub(crate) async fn add_node(&mut self) -> Result<String> {
        let index = self.next_node_index;
        self.next_node_index += 1;
        let node_id = NodeId::from_index(index);
        let gossip = Arc::new(self.gossip.add_node(node_id.clone()).await?);

        let cpu_cores = Arc::new(Semaphore::new(self.config.cpu_cores_per_node.max(1)));
        let renderers: Vec<BoxRenderer> = (0..self.config.cluster.renderer_slots_per_node)
            .map(|worker| {
                let renderer_seed = self.config.seed.wrapping_add(
                    ((index as u64).wrapping_mul(SEED_MIX))
                        .wrapping_add((worker as u64).wrapping_mul(SEED_MIX.wrapping_mul(3))),
                );
                Box::new(
                    StubRenderer::new(
                        self.config.costs.style_setup_cost,
                        self.config.costs.source_load_cost,
                        self.config.costs.render_resource_cost,
                        self.config.costs.first_render_resource_cost,
                        self.config.costs.render_cpu_cost,
                        cpu_cores.clone(),
                        renderer_seed,
                    )
                    .with_calibration_model(self.calibration_model.clone()),
                ) as BoxRenderer
            })
            .collect();
        let node_config = self
            .config
            .cluster
            .resolve_node_config(
                self.config.routing.clone(),
                self.config.costs.clone(),
                self.config.gossip.clone(),
            )
            .map_err(anyhow::Error::msg)?;
        let gossip: Arc<dyn GossipBus> = gossip;
        let transport: Arc<dyn InternalTransport> = self.transport.clone();
        let node = Node::spawn(NodeSpawn {
            id: node_id.clone(),
            renderers,
            profile_preparer: Arc::new(NoopProfilePreparer),
            gossip,
            transport,
            style_catalog: self.style_catalog.clone(),
            config: node_config,
            dispatcher_entropy: DispatcherEntropy::Deterministic {
                run_seed: self.config.seed,
            },
            render_admission: Arc::new(|| true),
        });
        let entry = self.registry.register(node_id.clone(), node.clone());
        self.nodes.push(ActiveNode {
            id: node_id.clone(),
            node,
            _registry_entry: entry,
            counters: Arc::new(NodeCounters::default()),
        });
        Ok(node_id.to_string())
    }

    pub(crate) async fn remove_node(&mut self, node_id: &str) -> Result<()> {
        ensure!(
            self.nodes.len() > 1,
            "cannot remove the final simulator node"
        );
        let Some(index) = self
            .nodes
            .iter()
            .position(|node| node.id.to_string() == node_id)
        else {
            anyhow::bail!("unknown active simulator node {node_id}");
        };
        let node = self.nodes.remove(index);
        self.registry.unregister(&node.id);
        self.gossip.remove_node(&node.id).await?;
        // Stop new ingress/forwarding immediately, but retain the node until
        // every task selected before the event has completed. This mirrors
        // production drain semantics and keeps its queue visible in samples.
        self.draining.push(node);
        Ok(())
    }

    pub(crate) async fn reap_drained_nodes(&mut self) -> Result<usize> {
        let mut reaped = 0;
        let mut index = 0;
        while index < self.draining.len() {
            // Reap on the node's *local* work only: no queued tasks and no
            // task currently executing on a worker (render permit held in any
            // stage). Deliberately independent of entry-side counters, which
            // also track tasks this node forwarded to peers — under load those
            // linger in the peers' queues and must not keep a locally-idle
            // node pinned as draining. Forwarded-task accounting is preserved
            // because the retired node keeps its counters.
            let node = &self.draining[index].node;
            let drained = node.render_permits_inuse() == 0
                && node
                    .worker_snapshot()
                    .iter()
                    .all(|worker| worker.queue_depth == 0);
            if drained {
                let node = self.draining.swap_remove(index);
                let shutdown = node
                    .node
                    .shutdown(tokio::time::Instant::now() + NODE_SHUTDOWN_TIMEOUT)
                    .await;
                self.retired.push(RetiredNode {
                    id: node.id,
                    counters: node.counters,
                });
                ensure!(
                    shutdown.is_complete(),
                    "retired node worker shutdown timed out: joined={} timed_out={}",
                    shutdown.joined,
                    shutdown.timed_out
                );
                reaped += 1;
            } else {
                index += 1;
            }
        }
        Ok(reaped)
    }

    pub(crate) fn select(&self, rng: &mut impl Rng) -> (Node, Arc<NodeCounters>) {
        let index = rng.random_range(0..self.nodes.len());
        let selected = &self.nodes[index];
        (selected.node.clone(), selected.counters.clone())
    }

    pub(crate) fn native_render_permits_total(&self) -> usize {
        (self.nodes.len() + self.draining.len())
            * self
                .config
                .cluster
                .resolved_native_render_permits_per_node()
    }

    pub(crate) fn observation(&self, metrics: &MetricsCollector) -> ClusterObservation {
        let metrics = metrics.observation();
        let mut total_queue_depth = 0;
        let mut loaded_workers = 0;
        let mut nodes =
            Vec::with_capacity(self.nodes.len() + self.draining.len() + self.retired.len());
        for active in &self.nodes {
            let workers = active.node.worker_snapshot();
            let queue_depth = workers.iter().map(|worker| worker.queue_depth).sum();
            let loaded = workers
                .iter()
                .filter(|worker| worker.loaded_profile.is_some())
                .count();
            total_queue_depth += queue_depth;
            loaded_workers += loaded;
            let mut observation = active
                .counters
                .observation(active.id.to_string(), true, false);
            observation.queue_depth = queue_depth;
            observation.loaded_workers = loaded;
            nodes.push(observation);
        }
        for draining in &self.draining {
            let workers = draining.node.worker_snapshot();
            let queue_depth = workers.iter().map(|worker| worker.queue_depth).sum();
            let loaded = workers
                .iter()
                .filter(|worker| worker.loaded_profile.is_some())
                .count();
            total_queue_depth += queue_depth;
            loaded_workers += loaded;
            let mut observation =
                draining
                    .counters
                    .observation(draining.id.to_string(), false, true);
            observation.queue_depth = queue_depth;
            observation.loaded_workers = loaded;
            nodes.push(observation);
        }
        nodes.extend(self.retired.iter().map(|retired| {
            retired
                .counters
                .observation(retired.id.to_string(), false, false)
        }));
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        let transport = self.transport.snapshot();
        let submitted_measured = nodes.iter().map(|node| node.submitted_measured).sum();
        let terminal_outcomes_measured = metrics.total;
        ClusterObservation {
            submitted_total: nodes.iter().map(|node| node.submitted_total).sum(),
            submitted_measured,
            terminal_outcomes_measured,
            outstanding_measured: submitted_measured
                .saturating_sub(terminal_outcomes_measured as u64),
            completed: metrics.completed,
            rejected: metrics.rejected,
            failed: metrics.failed,
            active_nodes: self.nodes.len(),
            draining_nodes: self.draining.len(),
            total_queue_depth,
            loaded_workers,
            cold_starts: metrics.cold_starts,
            style_swaps: metrics.style_swaps,
            source_hits: metrics.source_hits,
            source_loads: metrics.source_loads,
            tier_counts: metrics
                .tier_counts
                .iter()
                .map(|(tier, count)| (format!("{tier:?}"), *count))
                .collect(),
            forward_attempts: transport.attempts,
            forward_successes: transport.successes,
            nodes,
        }
    }

    async fn shutdown(mut self) -> Result<()> {
        // Join every live node's renderer workers within a bound so the
        // simulation leaves no worker tasks running (a plain `Node` drop detaches
        // them). Retired nodes were already dropped at reap time.
        let deadline = tokio::time::Instant::now() + NODE_SHUTDOWN_TIMEOUT;
        for node in self.nodes.drain(..) {
            node.node.shutdown(deadline).await;
            self.registry.unregister(&node.id);
        }
        for node in self.draining.drain(..) {
            node.node.shutdown(deadline).await;
        }
        self.gossip.shutdown_all().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Simulation, SimulationOptions};
    use crate::churn::{ChurnEvent, ChurnPlan};
    use crate::config::SimConfig;

    #[tokio::test]
    async fn applies_add_and_remove_churn_events() {
        tokio::time::pause();
        let mut workload = SimConfig::default().workload;
        workload.duration = Duration::from_millis(100);
        workload.warmup = Duration::ZERO;
        workload.total_rate = 1_000.0;
        let config = SimConfig {
            node_count: 2,
            workload,
            ..SimConfig::default()
        };
        let report = Simulation::new(config)
            .run_report(SimulationOptions {
                churn_plan: Some(ChurnPlan {
                    events: vec![
                        ChurnEvent::Add { at_request: 5 },
                        ChurnEvent::Remove {
                            at_request: 10,
                            node_id: "node-0".to_string(),
                        },
                    ],
                }),
                sample_every_requests: 4,
            })
            .await
            .expect("simulation");
        let churn = report.churn.expect("churn report");
        assert_eq!(churn.events.len(), 2);
        assert_eq!(churn.events[0].active_nodes, 3);
        assert_eq!(churn.events[1].active_nodes, 2);
        assert_eq!(churn.submission_cohorts[0].submission_epoch, 0);
        assert_eq!(churn.submission_cohorts[0].submitted, 5);
        assert_eq!(churn.submission_cohorts[1].submission_epoch, 1);
        assert_eq!(churn.submission_cohorts[1].submitted, 5);
        assert!(churn.samples.iter().all(|sample| {
            sample.observation.submitted_measured
                == sample.observation.terminal_outcomes_measured as u64
                    + sample.observation.outstanding_measured
        }));
        assert!(churn.submission_cohorts.iter().all(|cohort| {
            cohort.submitted == cohort.terminal_outcomes as u64 + cohort.outstanding
                && cohort.terminal_outcomes == cohort.completed + cohort.rejected + cohort.failed
        }));
        let remove_sample = churn
            .samples
            .iter()
            .find(|sample| {
                sample.reason == "post_event"
                    && sample.at_request == churn.events[1].applied_at_request
            })
            .expect("remove sample");
        assert_eq!(remove_sample.observation.draining_nodes, 1);
        let final_sample = churn.samples.last().expect("final sample");
        assert_eq!(final_sample.observation.draining_nodes, 0);
        let removed = final_sample
            .observation
            .nodes
            .iter()
            .find(|node| node.node_id == "node-0")
            .expect("removed node");
        assert_eq!(
            removed.submitted_total,
            removed.completed_total + removed.rejected_total + removed.failed_total
        );
        assert!(
            churn
                .samples
                .iter()
                .any(|sample| sample.reason == "periodic")
        );
    }

    #[tokio::test]
    async fn drained_node_is_reaped_mid_run_even_under_saturation() {
        use biei_core::config::{CostConfig, CostRange};

        tokio::time::pause();
        // Saturate the cluster: tiny render costs but a rate far above capacity
        // so the surviving peers keep deep queues the entire run. The removed
        // node's *local* queue still drains quickly, so it must be reaped
        // mid-run — its reap must not wait on tasks it forwarded to the (still
        // saturated) peers.
        let mut config = SimConfig {
            node_count: 3,
            ..SimConfig::default()
        };
        config.cluster.renderer_slots_per_node = 2;
        config.costs = CostConfig {
            style_setup_cost: CostRange::fixed(Duration::from_millis(1)),
            source_load_cost: CostRange::fixed(Duration::ZERO),
            render_cpu_cost: CostRange::fixed(Duration::from_millis(1)),
            render_resource_cost: CostRange::fixed(Duration::ZERO),
            first_render_resource_cost: CostRange::fixed(Duration::ZERO),
            hop_latency: Duration::ZERO,
            sla: Duration::from_millis(1_000),
        };
        config.workload.duration = Duration::from_millis(300);
        config.workload.warmup = Duration::ZERO;
        config.workload.total_rate = 12_000.0;
        config.workload.source_pattern = None;
        config.workload.new_style_rate = 0.0;

        let report = Simulation::new(config)
            .run_report(SimulationOptions {
                churn_plan: Some(ChurnPlan {
                    events: vec![ChurnEvent::Remove {
                        at_request: 200,
                        node_id: "node-1".to_string(),
                    }],
                }),
                sample_every_requests: 50,
            })
            .await
            .expect("simulation");
        let churn = report.churn.expect("churn report");
        let remove_at = churn.events[0].applied_at_request;

        // It was genuinely draining right after the event...
        let post_event = churn
            .samples
            .iter()
            .find(|sample| sample.reason == "post_event")
            .expect("post-event sample");
        assert_eq!(post_event.observation.draining_nodes, 1);

        // ...and it was reaped while the run was still going (a non-final
        // sample past the event shows zero draining nodes). The old
        // entry-counter reap kept it pinned until the final global join
        // because its forwarded tasks were stuck in the saturated peers.
        let reaped_mid_run = churn.samples.iter().any(|sample| {
            sample.reason != "final"
                && sample.at_request > remove_at
                && sample.observation.draining_nodes == 0
        });
        assert!(
            reaped_mid_run,
            "draining node was not reaped until the run ended: {:?}",
            churn
                .samples
                .iter()
                .map(|s| (s.reason, s.at_request, s.observation.draining_nodes))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn churn_request_clock_starts_after_warmup() {
        tokio::time::pause();
        let mut workload = SimConfig::default().workload;
        workload.duration = Duration::from_millis(200);
        workload.warmup = Duration::from_millis(100);
        workload.total_rate = 1_000.0;
        let report = Simulation::new(SimConfig {
            node_count: 2,
            workload,
            ..SimConfig::default()
        })
        .run_report(SimulationOptions {
            churn_plan: Some(ChurnPlan {
                events: vec![ChurnEvent::Add { at_request: 5 }],
            }),
            sample_every_requests: 25,
        })
        .await
        .expect("simulation");
        let churn = report.churn.expect("churn report");

        assert_eq!(churn.request_clock, "measured_after_warmup");
        assert_eq!(churn.events[0].applied_at_request, 5);
        assert!(churn.submitted_total > churn.submitted_measured);
        let post_event = churn
            .samples
            .iter()
            .find(|sample| sample.reason == "post_event")
            .expect("post-event sample");
        assert_eq!(post_event.observation.submitted_measured, 5);
        assert!(
            churn
                .samples
                .iter()
                .any(|sample| sample.reason == "measurement_start")
        );
    }

    #[tokio::test]
    async fn unreachable_churn_events_are_reported_without_failing_the_run() {
        tokio::time::pause();
        let mut workload = SimConfig::default().workload;
        workload.duration = Duration::from_millis(20);
        workload.warmup = Duration::ZERO;
        workload.total_rate = 100.0;
        let report = Simulation::new(SimConfig {
            node_count: 2,
            workload,
            ..SimConfig::default()
        })
        .run_report(SimulationOptions {
            churn_plan: Some(ChurnPlan {
                events: vec![ChurnEvent::Add {
                    at_request: 999_999,
                }],
            }),
            sample_every_requests: 25,
        })
        .await
        .expect("simulation still produces a report");
        let churn = report.churn.expect("churn report");

        assert!(churn.events.is_empty());
        assert_eq!(churn.unapplied_events.len(), 1);
    }

    #[tokio::test]
    async fn event_at_unreached_request_zero_is_not_applied_after_workload() {
        tokio::time::pause();
        let mut workload = SimConfig::default().workload;
        workload.duration = Duration::from_millis(20);
        workload.warmup = workload.duration;
        workload.total_rate = 100.0;
        let report = Simulation::new(SimConfig {
            node_count: 2,
            workload,
            ..SimConfig::default()
        })
        .run_report(SimulationOptions {
            churn_plan: Some(ChurnPlan {
                events: vec![ChurnEvent::Add { at_request: 0 }],
            }),
            sample_every_requests: 25,
        })
        .await
        .expect("simulation still produces a report");
        let churn = report.churn.expect("churn report");

        assert_eq!(churn.submitted_measured, 0);
        assert!(churn.events.is_empty());
        assert_eq!(churn.unapplied_events.len(), 1);
    }
}
