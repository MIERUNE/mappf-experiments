use std::{fs::File, io::BufReader, path::Path};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    EntryAffinity, ModeledCluster, SimCluster, TraceEntry, report::ClusterObservation,
    viewport_batch_ranges,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChurnPlan {
    pub events: Vec<ChurnEvent>,
}

impl ChurnPlan {
    /// Creates an event-free plan for steady-state replay with dynamic ingress assignment.
    pub fn empty() -> Self {
        Self { events: Vec::new() }
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file =
            File::open(path).with_context(|| format!("open churn plan {}", path.display()))?;
        let plan: Self = serde_json::from_reader(BufReader::new(file))
            .with_context(|| format!("parse churn plan {}", path.display()))?;
        plan.validate()?;
        Ok(plan)
    }

    fn validate(&self) -> Result<()> {
        let mut previous = 0;
        for (index, event) in self.events.iter().enumerate() {
            let at_request = event.at_request();
            ensure!(
                index == 0 || at_request >= previous,
                "churn events must be ordered by at_request"
            );
            previous = at_request;
        }
        Ok(())
    }

    fn validate_trace_length(&self, requests: usize) -> Result<()> {
        if let Some(event) = self.events.last() {
            ensure!(
                event.at_request() <= requests as u64,
                "churn event at request {} is beyond trace length {requests}",
                event.at_request()
            );
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
    fn at_request(&self) -> u64 {
        match self {
            Self::Add { at_request } | Self::Remove { at_request, .. } => *at_request,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ChurnConfig {
    pub seed: u64,
    pub entry_affinity: EntryAffinity,
    pub sample_every_requests: u64,
}

impl Default for ChurnConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            entry_affinity: EntryAffinity::PerRequest,
            sample_every_requests: 1_000,
        }
    }
}

impl ChurnConfig {
    fn validate(self) -> Result<()> {
        ensure!(
            self.sample_every_requests > 0,
            "sample_every_requests must be greater than zero"
        );
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct ChurnReport {
    pub config: ChurnConfig,
    pub events: Vec<AppliedChurnEvent>,
    pub samples: Vec<ChurnSample>,
}

#[derive(Debug, Serialize)]
pub struct AppliedChurnEvent {
    pub requested_at_request: u64,
    pub applied_at_request: u64,
    pub virtual_elapsed_ms: Option<u64>,
    pub action: &'static str,
    pub node_id: String,
    pub active_nodes: usize,
    pub membership_stale_nodes: usize,
}

#[derive(Debug, Serialize)]
pub struct ChurnSample {
    pub at_request: u64,
    pub reason: &'static str,
    #[serde(flatten)]
    pub observation: ClusterObservation,
}

pub async fn run_churn_trace(
    cluster: &mut SimCluster,
    entries: &[TraceEntry],
    viewport_batches: bool,
    plan: &ChurnPlan,
    config: ChurnConfig,
) -> Result<ChurnReport> {
    config.validate()?;
    plan.validate_trace_length(entries.len())?;
    let mut state = ChurnState::new(config, cluster.observation().await);
    apply_real_events(cluster, plan, &mut state, 0).await?;

    if viewport_batches {
        for range in viewport_batch_ranges(entries)? {
            let slice = &entries[range];
            let assignments = assign_entries(slice, cluster.node_count(), config);
            cluster.serve_viewport_on(slice, &assignments).await?;
            let processed = cluster.request_count();
            if state.update_processed(processed) {
                state.record_periodic(cluster.observation().await);
            }
            apply_real_events(cluster, plan, &mut state, processed).await?;
        }
    } else {
        for entry in entries {
            let entry_node = assign_entry(entry, cluster.node_count(), config);
            cluster.serve_on(entry, entry_node).await?;
            let processed = cluster.request_count();
            if state.update_processed(processed) {
                state.record_periodic(cluster.observation().await);
            }
            apply_real_events(cluster, plan, &mut state, processed).await?;
        }
    }
    state.finish(cluster.observation().await);
    Ok(state.report())
}

pub fn run_modeled_churn_trace(
    cluster: &mut ModeledCluster,
    entries: &[TraceEntry],
    viewport_batches: bool,
    plan: &ChurnPlan,
    config: ChurnConfig,
) -> Result<ChurnReport> {
    config.validate()?;
    plan.validate_trace_length(entries.len())?;
    let mut state = ChurnState::new(config, cluster.observation());
    apply_modeled_events(cluster, plan, &mut state, 0)?;

    if viewport_batches {
        for range in viewport_batch_ranges(entries)? {
            let slice = &entries[range];
            let assignments = assign_entries(slice, cluster.node_count(), config);
            cluster.serve_viewport_on(slice, &assignments)?;
            let processed = cluster.request_count();
            if state.update_processed(processed) {
                state.record_periodic(cluster.observation());
            }
            apply_modeled_events(cluster, plan, &mut state, processed)?;
        }
    } else {
        for entry in entries {
            let entry_node = assign_entry(entry, cluster.node_count(), config);
            cluster.serve_on(entry, entry_node)?;
            let processed = cluster.request_count();
            if state.update_processed(processed) {
                state.record_periodic(cluster.observation());
            }
            apply_modeled_events(cluster, plan, &mut state, processed)?;
        }
    }
    state.finish(cluster.observation());
    Ok(state.report())
}

struct ChurnState {
    config: ChurnConfig,
    processed: u64,
    next_event: usize,
    next_sample: u64,
    events: Vec<AppliedChurnEvent>,
    samples: Vec<ChurnSample>,
}

impl ChurnState {
    fn new(config: ChurnConfig, initial: ClusterObservation) -> Self {
        Self {
            config,
            processed: 0,
            next_event: 0,
            next_sample: config.sample_every_requests,
            events: Vec::new(),
            samples: vec![ChurnSample {
                at_request: 0,
                reason: "initial",
                observation: initial,
            }],
        }
    }

    fn update_processed(&mut self, processed: u64) -> bool {
        self.processed = processed;
        self.processed >= self.next_sample
    }

    fn record_periodic(&mut self, observation: ClusterObservation) {
        debug_assert_eq!(observation.requests, self.processed);
        self.samples.push(ChurnSample {
            at_request: self.processed,
            reason: "periodic",
            observation,
        });
        while self.next_sample <= self.processed {
            self.next_sample += self.config.sample_every_requests;
        }
    }

    fn record_event(
        &mut self,
        event: &ChurnEvent,
        action: &'static str,
        node_id: String,
        observation: ClusterObservation,
    ) {
        self.events.push(AppliedChurnEvent {
            requested_at_request: event.at_request(),
            applied_at_request: self.processed,
            virtual_elapsed_ms: observation.virtual_elapsed_ms,
            action,
            node_id,
            active_nodes: observation.active_nodes,
            membership_stale_nodes: observation.membership_stale_nodes,
        });
        self.samples.push(ChurnSample {
            at_request: self.processed,
            reason: "post_event",
            observation,
        });
        self.next_event += 1;
    }

    fn finish(&mut self, observation: ClusterObservation) {
        let duplicate = self.samples.last().is_some_and(|sample| {
            sample.at_request == observation.requests && sample.reason == "final"
        });
        if !duplicate {
            self.samples.push(ChurnSample {
                at_request: observation.requests,
                reason: "final",
                observation,
            });
        }
    }

    fn report(self) -> ChurnReport {
        ChurnReport {
            config: self.config,
            events: self.events,
            samples: self.samples,
        }
    }
}

async fn apply_real_events(
    cluster: &mut SimCluster,
    plan: &ChurnPlan,
    state: &mut ChurnState,
    processed: u64,
) -> Result<()> {
    while let Some(event) = plan.events.get(state.next_event) {
        if event.at_request() > processed {
            break;
        }
        state.samples.push(ChurnSample {
            at_request: state.processed,
            reason: "pre_event",
            observation: cluster.observation().await,
        });
        match event {
            ChurnEvent::Add { .. } => {
                let id = cluster.add_node().await?;
                let observation = cluster.observation().await;
                state.record_event(event, "add", id, observation);
            }
            ChurnEvent::Remove { node_id, .. } => {
                cluster.remove_node(node_id).await?;
                let observation = cluster.observation().await;
                state.record_event(event, "remove", node_id.clone(), observation);
            }
        }
    }
    Ok(())
}

fn apply_modeled_events(
    cluster: &mut ModeledCluster,
    plan: &ChurnPlan,
    state: &mut ChurnState,
    processed: u64,
) -> Result<()> {
    while let Some(event) = plan.events.get(state.next_event) {
        if event.at_request() > processed {
            break;
        }
        state.samples.push(ChurnSample {
            at_request: state.processed,
            reason: "pre_event",
            observation: cluster.observation(),
        });
        match event {
            ChurnEvent::Add { .. } => {
                let id = cluster.add_node()?;
                state.record_event(event, "add", id, cluster.observation());
            }
            ChurnEvent::Remove { node_id, .. } => {
                cluster.remove_node(node_id)?;
                state.record_event(event, "remove", node_id.clone(), cluster.observation());
            }
        }
    }
    Ok(())
}

fn assign_entries(entries: &[TraceEntry], node_count: usize, config: ChurnConfig) -> Vec<usize> {
    entries
        .iter()
        .map(|entry| assign_entry(entry, node_count, config))
        .collect()
}

fn assign_entry(entry: &TraceEntry, node_count: usize, config: ChurnConfig) -> usize {
    let mut key = config.seed ^ (entry.user as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    if config.entry_affinity == EntryAffinity::PerRequest {
        key ^= entry.step.rotate_left(7);
        key ^= (entry.ordinal as u64).rotate_left(17);
        key ^= (u64::from(entry.z) << 58) ^ (u64::from(entry.x) << 29) ^ u64::from(entry.y);
    }
    (mix64(key) % node_count as u64) as usize
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::{ChurnConfig, ChurnEvent, ChurnPlan, run_churn_trace};
    use crate::{ClusterConfig, SimCluster};

    #[test]
    fn parses_flat_churn_events() {
        let plan: ChurnPlan = serde_json::from_str(
            r#"{"events":[{"at_request":10,"action":"add"},{"at_request":20,"action":"remove","node_id":"node-0"}]}"#,
        )
        .expect("churn plan");

        assert!(matches!(plan.events[0], ChurnEvent::Add { at_request: 10 }));
        assert!(matches!(
            &plan.events[1],
            ChurnEvent::Remove { node_id, .. } if node_id == "node-0"
        ));
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

    #[tokio::test(start_paused = true)]
    async fn applies_zero_request_events_and_reports_active_membership() {
        let mut cluster = SimCluster::new(ClusterConfig {
            node_count: 1,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");
        let plan = ChurnPlan {
            events: vec![
                ChurnEvent::Add { at_request: 0 },
                ChurnEvent::Remove {
                    at_request: 0,
                    node_id: "node-0".to_string(),
                },
            ],
        };

        let churn = run_churn_trace(&mut cluster, &[], false, &plan, ChurnConfig::default())
            .await
            .expect("run churn");

        assert_eq!(churn.events.len(), 2);
        assert_eq!(cluster.active_node_ids(), ["node-1"]);
        let event_samples: Vec<_> = churn
            .samples
            .iter()
            .filter(|sample| matches!(sample.reason, "pre_event" | "post_event"))
            .collect();
        assert_eq!(event_samples.len(), 4);
        assert_eq!(event_samples[0].at_request, event_samples[1].at_request);
        assert_eq!(event_samples[0].observation.active_nodes, 1);
        assert_eq!(event_samples[1].observation.active_nodes, 2);
        assert_eq!(
            churn
                .samples
                .last()
                .expect("final sample")
                .observation
                .active_nodes,
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn survives_majority_node_removal() {
        let mut cluster = SimCluster::new(ClusterConfig {
            node_count: 10,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");
        let plan = ChurnPlan {
            events: (0..7)
                .map(|index| ChurnEvent::Remove {
                    at_request: 0,
                    node_id: format!("node-{index}"),
                })
                .collect(),
        };

        let churn = run_churn_trace(&mut cluster, &[], false, &plan, ChurnConfig::default())
            .await
            .expect("run majority failure");

        assert_eq!(churn.events.len(), 7);
        assert_eq!(cluster.active_node_ids(), ["node-7", "node-8", "node-9"]);
        assert_eq!(
            churn
                .samples
                .last()
                .expect("final sample")
                .observation
                .active_nodes,
            3
        );
    }
}
