//! `Simulation::run` — cluster setup (nodes, gossip backend, transport,
//! activity tracker) + workload driver + final `Report`.

use std::sync::Arc;

use crate::config::SimConfig;
use crate::metrics::{MetricsCollector, Report};
use biei::activity::ProfileActivityTracker;
use biei::gossip::GossipBus;
use biei::node::{Node, NodeSpawn};
use biei::renderer::BoxRenderer;
use biei::renderer::NoopProfilePreparer;
use biei::style_catalog::StyleCatalog;
use biei::transport::Transport;
use biei::types::NodeId;

use super::channel_transport::{ChannelTransport, NodeRegistry};
use super::chitchat_bus::ChitchatGossipBus;
use super::stub_renderer::StubRenderer;
use super::workload::run_workload;

const SEED_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

pub struct Simulation {
    pub config: SimConfig,
}

impl Simulation {
    pub fn new(config: SimConfig) -> Self {
        Self { config }
    }

    pub async fn run(self) -> Report {
        let node_count = self.config.node_count;
        let cpu_render_permits = self.config.cluster.resolved_cpu_render_permits_per_node();
        let metrics = Arc::new(MetricsCollector::with_cpu_render_permits(
            cpu_render_permits * node_count,
        ));
        let members: Vec<NodeId> = (0..node_count).map(NodeId::from_index).collect();

        let gossip: Arc<dyn GossipBus> = Arc::new(
            ChitchatGossipBus::new(
                members.clone(),
                self.config.gossip.publish_interval,
                self.config.costs.hop_latency,
            )
            .await
            .expect("chitchat gossip bus init"),
        );

        let activity = Arc::new(ProfileActivityTracker::new());
        let catalog = StyleCatalog::new();
        catalog.set_url_template("http://simulator.local/styles/{style_id}/style.json");
        let style_catalog = Arc::new(catalog);

        let queue_limits = self
            .config
            .cluster
            .resolved_queue_limits(&self.config.costs);
        let bl_capacity = queue_limits.soft;
        let queue_capacity = queue_limits.hard;
        let render_permits = self.config.cluster.resolved_render_permits_per_node();
        let registry = NodeRegistry::new();
        let transport: Arc<dyn Transport> = Arc::new(ChannelTransport::new(
            self.config.costs.hop_latency,
            registry.clone(),
        ));

        let renderer_slots_per_node = self.config.cluster.renderer_slots_per_node;
        let mut nodes: Vec<Node> = Vec::with_capacity(node_count);
        let mut registry_entries = Vec::with_capacity(node_count);
        for i in 0..node_count {
            let node_id = NodeId::from_index(i);
            let renderers: Vec<BoxRenderer> = (0..renderer_slots_per_node)
                .map(|w| {
                    let renderer_seed = self.config.seed.wrapping_add(
                        ((i as u64).wrapping_mul(SEED_MIX))
                            .wrapping_add((w as u64).wrapping_mul(SEED_MIX.wrapping_mul(3))),
                    );
                    Box::new(StubRenderer::new(
                        self.config.costs.style_setup_cost,
                        self.config.costs.source_load_cost,
                        self.config.costs.render_cost,
                        renderer_seed,
                    )) as BoxRenderer
                })
                .collect();

            let dispatcher_seed = self
                .config
                .seed
                .wrapping_add((i as u64 + 1).wrapping_mul(SEED_MIX.wrapping_mul(5)));

            let node = Node::spawn(NodeSpawn {
                id: node_id.clone(),
                renderers,
                profile_preparer: Arc::new(NoopProfilePreparer),
                gossip: gossip.clone(),
                transport: transport.clone(),
                style_catalog: style_catalog.clone(),
                activity: activity.clone(),
                routing: self.config.routing.clone(),
                costs: self.config.costs.clone(),
                gossip_cfg: self.config.gossip.clone(),
                bl_capacity,
                queue_capacity,
                render_permits,
                cpu_render_permits,
                source_cache_capacity: self.config.cluster.source_cache_capacity,
                render_output_cache_capacity_bytes: self
                    .config
                    .cluster
                    .render_output_cache_capacity_bytes,
                dispatcher_seed,
            });
            let entry = registry.register(node_id, node.clone());
            registry_entries.push(entry);
            nodes.push(node);
        }

        run_workload(
            self.config.workload.clone(),
            nodes.clone(),
            metrics.clone(),
            activity.clone(),
            self.config.seed,
        )
        .await;

        // Workload returned only after every spawned `handle_incoming` task
        // completed, so no in-flight work remains. Drop nodes (registry
        // entries last) — gossip publishers abort on Node drop, workers
        // see their channels close and exit.
        drop(nodes);
        drop(registry_entries);

        metrics.report(self.config.costs.sla)
    }
}
