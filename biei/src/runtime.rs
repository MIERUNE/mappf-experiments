//! Server runtime assembly.
//!
//! The default path keeps a single-node baseline for lightweight checks. In
//! cluster mode, the same `Node` stack is wired to real chitchat membership and
//! HTTP peer forwarding.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::activity::ProfileActivityTracker;
use crate::config::{CostConfig, CostRange, GossipConfig, RoutingConfig, Tier1Strategy};
use crate::drain::DrainController;
use crate::gossip::GossipBus;
use crate::http::ingress::HttpIngress;
use crate::node::{Node, NodeSpawn};
use crate::options::Options;
use crate::renderer::BoxRenderer;
use crate::renderer::actor::RendererActorConfig;
use crate::renderer::maplibre::{MapLibreProfilePreparer, MapLibreRenderer};
use crate::style_catalog::StyleCatalog;
use crate::tileset_catalog::TilesetCatalog;
use crate::transport::{ForwardError, Transport};
use crate::types::{ClusterView, NodeId, NodeKvs, NodeStateView};
use crate::wire::{ForwardRequest, ForwardResponse};

const MEMBERSHIP_DRAIN_PUBLISH_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct Runtime {
    node: Node,
    style_catalog: Arc<StyleCatalog>,
    tileset_catalog: Arc<TilesetCatalog>,
    drain: DrainController,
    ingress_concurrency_limit: usize,
    membership: Option<crate::membership::Membership>,
}

impl Runtime {
    pub fn spawn_single_node(options: &Options) -> anyhow::Result<Self> {
        let style_catalog = Arc::new(options.build_style_catalog());
        let tileset_catalog = Arc::new(options.build_tileset_catalog());
        spawn_single_node_with_catalog(style_catalog, tileset_catalog, options)
    }

    pub async fn spawn_cluster_node(options: &Options) -> anyhow::Result<Self> {
        let style_catalog = Arc::new(options.build_style_catalog());
        let tileset_catalog = Arc::new(options.build_tileset_catalog());
        spawn_cluster_node_with_catalog(style_catalog, tileset_catalog, options).await
    }

    pub fn node(&self) -> Node {
        self.node.clone()
    }

    pub fn style_catalog(&self) -> Arc<StyleCatalog> {
        self.style_catalog.clone()
    }

    pub fn tileset_catalog(&self) -> Arc<TilesetCatalog> {
        self.tileset_catalog.clone()
    }

    pub fn http_ingress(&self, sla_budget: Duration) -> HttpIngress {
        HttpIngress::with_drain_and_limit(
            self.node(),
            self.style_catalog(),
            self.tileset_catalog(),
            sla_budget,
            self.drain.clone(),
            self.ingress_concurrency_limit,
        )
    }

    pub fn drain_controller(&self) -> DrainController {
        self.drain.clone()
    }

    pub fn membership(&self) -> Option<crate::membership::Membership> {
        self.membership.clone()
    }

    pub async fn begin_draining(&self) {
        self.drain.begin_draining();
        if let Some(membership) = &self.membership {
            match tokio::time::timeout(
                MEMBERSHIP_DRAIN_PUBLISH_TIMEOUT,
                membership.set_draining(true),
            )
            .await
            {
                Ok(()) => {
                    tracing::info!("published draining state to membership");
                }
                Err(_) => {
                    tracing::warn!(
                        timeout_ms = MEMBERSHIP_DRAIN_PUBLISH_TIMEOUT.as_millis(),
                        "timed out publishing draining state to membership; continuing shutdown"
                    );
                }
            }
        }
    }

    pub async fn wait_for_drain(&self, timeout: Duration) -> bool {
        self.drain.wait_idle(timeout).await
    }
}

fn spawn_single_node_with_catalog(
    style_catalog: Arc<StyleCatalog>,
    tileset_catalog: Arc<TilesetCatalog>,
    options: &Options,
) -> anyhow::Result<Runtime> {
    let node_id = options.node_id.clone();
    let gossip = Arc::new(LocalGossipBus::new(vec![node_id.clone()]));
    let transport = Arc::new(SingleNodeTransport);
    spawn_node_with_backends(
        style_catalog,
        tileset_catalog,
        options,
        gossip,
        transport,
        None,
    )
}

async fn spawn_cluster_node_with_catalog(
    style_catalog: Arc<StyleCatalog>,
    tileset_catalog: Arc<TilesetCatalog>,
    options: &Options,
) -> anyhow::Result<Runtime> {
    let membership =
        crate::membership::Membership::spawn(options, Duration::from_millis(50)).await?;
    let gossip: Arc<dyn GossipBus> = Arc::new(membership.clone());
    let transport: Arc<dyn Transport> = Arc::new(crate::http::internal::HttpTransport::new(
        Arc::new(membership.clone()),
    )?);
    spawn_node_with_backends(
        style_catalog,
        tileset_catalog,
        options,
        gossip,
        transport,
        Some(membership),
    )
}

fn spawn_node_with_backends(
    style_catalog: Arc<StyleCatalog>,
    tileset_catalog: Arc<TilesetCatalog>,
    options: &Options,
    gossip: Arc<dyn GossipBus>,
    transport: Arc<dyn Transport>,
    membership: Option<crate::membership::Membership>,
) -> anyhow::Result<Runtime> {
    let cluster = options.cluster_config();
    cluster
        .validate_standby_ratio()
        .map_err(anyhow::Error::msg)?;
    let costs = default_production_costs(options.sla);
    let queue_limits = cluster.resolved_queue_limits(&costs);
    let renderer_slots = cluster.renderer_slots_per_node;
    let ingress_concurrency_limit = ingress_concurrency_limit(renderer_slots, queue_limits.hard);
    let render_permits = cluster.resolved_render_permits_per_node();
    let cpu_render_permits = cluster.resolved_cpu_render_permits_per_node();

    let renderers: Vec<BoxRenderer> = (0..renderer_slots)
        .map(|worker_id| {
            let renderer = MapLibreRenderer::spawn(RendererActorConfig {
                worker_id: worker_id as u32,
                ambient_cache_path: Some(options.maplibre_cache_path.clone()),
            })?;
            Ok(Box::new(renderer) as BoxRenderer)
        })
        .collect::<Result<_, crate::types::RendererError>>()?;
    let profile_preparer = Arc::new(MapLibreProfilePreparer::new(
        style_catalog.clone(),
        render_permits,
    ));

    let activity = Arc::new(ProfileActivityTracker::new());

    let node = Node::spawn(NodeSpawn {
        id: options.node_id.clone(),
        renderers,
        profile_preparer,
        gossip,
        transport,
        style_catalog: style_catalog.clone(),
        activity,
        routing: RoutingConfig {
            tier1_strategy: Tier1Strategy::PowerOfTwo,
            tier3_enabled: true,
            drain_max_queue: queue_limits.soft,
        },
        costs,
        gossip_cfg: GossipConfig {
            publish_interval: Duration::from_millis(50),
        },
        bl_capacity: queue_limits.soft,
        queue_capacity: queue_limits.hard,
        render_permits,
        cpu_render_permits,
        source_cache_capacity: cluster.source_cache_capacity,
        render_output_cache_capacity_bytes: cluster.render_output_cache_capacity_bytes,
        dispatcher_seed: 0,
    });

    Ok(Runtime {
        node,
        style_catalog,
        tileset_catalog,
        drain: DrainController::new(),
        ingress_concurrency_limit,
        membership,
    })
}

fn ingress_concurrency_limit(renderer_slots: usize, hard_queue_limit: usize) -> usize {
    renderer_slots
        .saturating_mul(hard_queue_limit)
        .saturating_mul(3)
        .div_ceil(2)
        .max(1)
}

fn default_production_costs(sla: Duration) -> CostConfig {
    CostConfig {
        style_setup_cost: CostRange::fixed(Duration::from_millis(250)),
        source_load_cost: CostRange::fixed(Duration::ZERO),
        render_cost: CostRange::fixed(Duration::from_millis(50)),
        hop_latency: Duration::from_millis(25),
        sla,
    }
}

struct LocalGossipBus {
    members: Vec<NodeId>,
    kvs: Mutex<HashMap<NodeId, NodeKvs>>,
}

impl LocalGossipBus {
    fn new(members: Vec<NodeId>) -> Self {
        Self {
            members,
            kvs: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl GossipBus for LocalGossipBus {
    async fn set(&self, node_id: NodeId, key: String, value: String) {
        self.kvs
            .lock()
            .expect("local gossip mutex poisoned")
            .entry(node_id)
            .or_default()
            .insert(key, value);
    }

    async fn view(&self) -> ClusterView {
        let states = self
            .kvs
            .lock()
            .expect("local gossip mutex poisoned")
            .iter()
            .map(|(node_id, kvs)| {
                (
                    node_id.clone(),
                    NodeStateView::from_kvs(node_id.clone(), kvs),
                )
            })
            .collect();
        ClusterView {
            members: self.members.clone(),
            states,
            generated_at: Instant::now(),
        }
    }
}

struct SingleNodeTransport;

#[async_trait]
impl Transport for SingleNodeTransport {
    async fn send(
        &self,
        target: NodeId,
        _fwd: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError> {
        Err(ForwardError::Fatal(format!(
            "single-node runtime cannot forward to node {target}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StyleId;

    fn options() -> Options {
        Options::try_parse_from([
            "biei",
            "--style-templates",
            "https://example.test/styles/{style_id}/style.json",
            "--node-id",
            "biei-0",
            "--cores",
            "1",
        ])
        .expect("options parse")
    }

    #[tokio::test]
    async fn local_gossip_decodes_published_worker_state() {
        let node_id = NodeId::from("node-7");
        let gossip = LocalGossipBus::new(vec![node_id.clone()]);
        gossip
            .set(
                node_id.clone(),
                "worker.0.style".to_string(),
                "carto/voyager@1".to_string(),
            )
            .await;
        gossip
            .set(
                node_id.clone(),
                "worker.0.mode".to_string(),
                "static".to_string(),
            )
            .await;
        gossip
            .set(
                node_id.clone(),
                "worker.0.scale".to_string(),
                "2x".to_string(),
            )
            .await;
        gossip
            .set(
                node_id.clone(),
                "worker.0.queue".to_string(),
                "0".to_string(),
            )
            .await;

        let view = gossip.view().await;
        let worker = &view.states.get(&node_id).expect("node state").workers[0];
        assert_eq!(worker.id, 0);
        assert_eq!(worker.queue_depth, 0);
        assert_eq!(
            worker
                .loaded_profile
                .as_ref()
                .expect("loaded profile")
                .style
                .id
                .as_str(),
            "carto/voyager"
        );
    }

    #[tokio::test]
    async fn single_node_runtime_builds_catalog_and_node() {
        let options = options();

        let runtime = Runtime::spawn_single_node(&options).expect("runtime spawns");
        assert_eq!(runtime.node().id(), NodeId::from("biei-0"));
        assert!(
            runtime
                .style_catalog()
                .resolve_latest(&StyleId("carto/voyager".to_string()))
                .is_some()
        );
    }

    #[tokio::test]
    async fn single_node_runtime_uses_lazy_catalog() {
        let options = options();
        let runtime = Runtime::spawn_single_node(&options).expect("runtime spawns");

        assert_eq!(runtime.style_catalog().len(), 0);
        assert!(
            runtime
                .style_catalog()
                .resolve_latest(&StyleId("carto/voyager".to_string()))
                .is_some()
        );
    }

    #[tokio::test]
    async fn single_node_runtime_exposes_http_ingress() {
        let options = options();
        let catalog = Arc::new(options.build_style_catalog());
        let tilesets = Arc::new(options.build_tileset_catalog());
        let runtime =
            spawn_single_node_with_catalog(catalog, tilesets, &options).expect("runtime spawns");
        let _ingress = runtime.http_ingress(Duration::from_secs(30));
    }

    #[test]
    fn ingress_concurrency_limit_is_derived_from_slots_and_hard_queue() {
        assert_eq!(ingress_concurrency_limit(20, 28), 840);
        assert_eq!(ingress_concurrency_limit(1, 1), 2);
        assert_eq!(ingress_concurrency_limit(0, 0), 1);
    }

    #[tokio::test]
    async fn runtime_ingress_rejects_new_requests_after_draining() {
        let options = options();
        let runtime = Runtime::spawn_single_node(&options).expect("runtime spawns");
        let ingress = runtime.http_ingress(Duration::from_secs(30));

        runtime.begin_draining().await;

        let response = ingress
            .handle_path(
                "/carto/voyager/static/auto/256x256.png",
                tokio::time::Instant::now(),
            )
            .await;
        assert_eq!(response.status, 503);
        assert!(
            std::str::from_utf8(&response.body)
                .expect("json body")
                .contains("service_draining")
        );
    }
}
