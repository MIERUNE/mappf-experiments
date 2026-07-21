//! Server runtime assembly.
//!
//! The default path keeps a single-node baseline for lightweight checks. In
//! cluster mode, the same `Node` stack is wired to real chitchat membership and
//! HTTP peer forwarding.

mod run;

pub(crate) use run::run;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::drain::DrainController;
use crate::http::ingress::HttpIngress;
use crate::membership::{Membership, MembershipConfig};
use crate::options::Options;
use crate::renderer::BoxRenderer;
use crate::renderer::actor::{RendererActorConfig, RendererActorSupervisor};
use crate::renderer::maplibre::{MapLibreProfilePreparer, MapLibreRenderer};
use biei_core::config::{CostConfig, CostRange, GossipConfig, RoutingConfig, Tier1Strategy};
use biei_core::gossip::GossipBus;
use biei_core::internal_transport::{ForwardError, InternalTransport};
use biei_core::node::{DispatcherEntropy, Node, NodeSpawn};
use biei_core::style_catalog::StyleCatalog;
use biei_core::types::{ClusterView, NodeId, NodeKvs, NodeStateView};
use biei_core::wire::{ForwardRequest, ForwardResponse};
use mmpf_cluster::ClusterOwner;

const GOSSIP_INTERVAL: Duration = Duration::from_millis(50);
const MEMBERSHIP_DRAIN_PUBLISH_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct Runtime {
    node: Node,
    style_catalog: Arc<StyleCatalog>,
    tileset_url_template: Arc<str>,
    drain: DrainController,
    ingress_concurrency_limit: usize,
    internal_forward_concurrency_limit: usize,
    membership: Option<Membership>,
    renderer_supervisor: RendererActorSupervisor,
}

impl Runtime {
    pub(crate) fn spawn_single_node(options: &Options) -> anyhow::Result<Self> {
        let style_catalog = Arc::new(options.build_style_catalog());
        let tileset_url_template = Arc::<str>::from(options.tileset_url_template.clone());
        spawn_single_node_with_catalog(style_catalog, tileset_url_template, options)
    }

    pub(crate) async fn spawn_cluster_node(
        options: &Options,
    ) -> anyhow::Result<(Self, ClusterOwner)> {
        let style_catalog = Arc::new(options.build_style_catalog());
        let tileset_url_template = Arc::<str>::from(options.tileset_url_template.clone());
        spawn_cluster_node_with_catalog(style_catalog, tileset_url_template, options).await
    }

    pub(crate) fn node(&self) -> Node {
        self.node.clone()
    }

    pub(crate) fn style_catalog(&self) -> Arc<StyleCatalog> {
        self.style_catalog.clone()
    }

    pub(crate) fn tileset_url_template(&self) -> Arc<str> {
        self.tileset_url_template.clone()
    }

    pub(crate) fn http_ingress(&self, sla_budget: Duration) -> HttpIngress {
        HttpIngress::with_drain_and_limit(
            self.node(),
            self.style_catalog(),
            self.tileset_url_template(),
            sla_budget,
            self.drain.clone(),
            self.ingress_concurrency_limit,
            self.renderer_supervisor.clone(),
        )
    }

    pub(crate) fn drain_controller(&self) -> DrainController {
        self.drain.clone()
    }

    pub(crate) fn internal_forward_concurrency_limit(&self) -> usize {
        self.internal_forward_concurrency_limit
    }

    pub(crate) fn membership(&self) -> Option<Membership> {
        self.membership.clone()
    }

    #[cfg(test)]
    pub(crate) fn renderer_supervisor(&self) -> RendererActorSupervisor {
        self.renderer_supervisor.clone()
    }

    pub(crate) async fn begin_draining(&self) {
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

    pub(crate) async fn wait_for_drain(&self, timeout: Duration) -> bool {
        self.drain.wait_idle(timeout).await
    }
}

fn spawn_single_node_with_catalog(
    style_catalog: Arc<StyleCatalog>,
    tileset_url_template: Arc<str>,
    options: &Options,
) -> anyhow::Result<Runtime> {
    let node_id = options.node_id.clone();
    let gossip = Arc::new(LocalGossipBus::new(node_id));
    let transport = Arc::new(SingleNodeTransport);
    spawn_node_with_backends(
        style_catalog,
        tileset_url_template,
        options,
        gossip,
        transport,
        None,
    )
}

async fn spawn_cluster_node_with_catalog(
    style_catalog: Arc<StyleCatalog>,
    tileset_url_template: Arc<str>,
    options: &Options,
) -> anyhow::Result<(Runtime, ClusterOwner)> {
    let (membership, owner) = Membership::spawn(MembershipConfig {
        node_id: options.node_id.clone(),
        gossip_endpoint: options.gossip_endpoint,
        http_advertise_addr: options.internal_advertise_addr,
        seed_nodes: options.gossip_seeds.clone(),
        gossip_interval: GOSSIP_INTERVAL,
    })
    .await?;
    let runtime_result = (|| {
        let gossip: Arc<dyn GossipBus> = Arc::new(membership.clone());
        let transport: Arc<dyn InternalTransport> = Arc::new(
            crate::http::internal::HttpTransport::new(Arc::new(membership.clone()))?,
        );
        spawn_node_with_backends(
            style_catalog,
            tileset_url_template,
            options,
            gossip,
            transport,
            Some(membership),
        )
    })();
    match runtime_result {
        Ok(runtime) => Ok((runtime, owner)),
        Err(error) => match owner.shutdown().await {
            Ok(()) => Err(error),
            Err(shutdown_error) => {
                Err(error.context(format!("membership cleanup also failed: {shutdown_error}")))
            }
        },
    }
}

fn spawn_node_with_backends(
    style_catalog: Arc<StyleCatalog>,
    tileset_url_template: Arc<str>,
    options: &Options,
    gossip: Arc<dyn GossipBus>,
    transport: Arc<dyn InternalTransport>,
    membership: Option<Membership>,
) -> anyhow::Result<Runtime> {
    let cluster = options.cluster_config();
    let renderer_slots = cluster.renderer_slots_per_node;
    let costs = default_production_costs(options.sla);
    let drain_max_queue = cluster.resolved_bl_capacity(&costs);
    let node_config = cluster
        .resolve_node_config(
            RoutingConfig {
                tier1_strategy: Tier1Strategy::PowerOfTwo,
                tier3_enabled: true,
                drain_max_queue,
            },
            costs,
            GossipConfig {
                publish_interval: GOSSIP_INTERVAL,
            },
        )
        .map_err(anyhow::Error::msg)?;
    let ingress_concurrency_limit =
        ingress_concurrency_limit(renderer_slots, node_config.queue_limits.hard);
    let internal_forward_concurrency_limit =
        internal_forward_concurrency_limit(renderer_slots, node_config.queue_limits.hard);
    let render_permits = node_config.render_permits;
    let renderer_supervisor = RendererActorSupervisor::with_provider_health(
        renderer_slots,
        mmpf_mln_filesource::provider_health(),
    );

    let renderers: Vec<BoxRenderer> = (0..renderer_slots)
        .map(|worker_id| {
            let renderer = MapLibreRenderer::spawn_supervised(
                RendererActorConfig {
                    worker_id: worker_id as u32,
                    ambient_cache_path: Some(options.maplibre_cache_path.clone()),
                },
                renderer_supervisor.clone(),
            )?;
            Ok(Box::new(renderer) as BoxRenderer)
        })
        .collect::<Result<_, biei_core::types::RendererError>>()?;
    let profile_preparer = Arc::new(MapLibreProfilePreparer::new(
        style_catalog.clone(),
        render_permits,
        options.mln_resource_private_hosts.clone(),
    )?);

    let node = Node::spawn(NodeSpawn {
        id: options.node_id.clone(),
        renderers,
        profile_preparer,
        gossip,
        transport,
        style_catalog: style_catalog.clone(),
        config: node_config,
        dispatcher_entropy: DispatcherEntropy::Production,
        render_admission: renderer_supervisor.render_admission_probe(),
    });
    // Fold the process-global Rust FileSource metrics into every scrape. The
    // core `NodeMetrics` gathers only node-scoped families; the composition
    // root injects the MapLibre-backed collector so `biei-core` (and the
    // simulator) stay independent of `mmpf-mln-filesource`.
    node.metrics()
        .set_extra_metrics_source(Box::new(mmpf_mln_filesource::gather_metrics));

    Ok(Runtime {
        node,
        style_catalog,
        tileset_url_template,
        drain: DrainController::new(),
        ingress_concurrency_limit,
        internal_forward_concurrency_limit,
        membership,
        renderer_supervisor,
    })
}

fn ingress_concurrency_limit(renderer_slots: usize, hard_queue_limit: usize) -> usize {
    renderer_slots
        .saturating_mul(hard_queue_limit)
        .saturating_mul(3)
        .div_ceil(2)
        .max(1)
}

fn internal_forward_concurrency_limit(renderer_slots: usize, hard_queue_limit: usize) -> usize {
    renderer_slots.saturating_mul(hard_queue_limit).max(1)
}

fn default_production_costs(sla: Duration) -> CostConfig {
    CostConfig {
        style_setup_cost: CostRange::fixed(Duration::from_millis(250)),
        source_load_cost: CostRange::fixed(Duration::ZERO),
        render_cpu_cost: CostRange::fixed(Duration::from_millis(50)),
        // Production routing uses a conservative fixed BL until these two
        // distributions are imported from a bounded calibration profile.
        render_resource_cost: CostRange::fixed(Duration::ZERO),
        first_render_resource_cost: CostRange::fixed(Duration::ZERO),
        hop_latency: Duration::from_millis(25),
        sla,
    }
}

struct LocalGossipBus {
    node_id: NodeId,
    kvs: Mutex<NodeKvs>,
}

impl LocalGossipBus {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            kvs: Mutex::new(NodeKvs::new()),
        }
    }
}

#[async_trait]
impl GossipBus for LocalGossipBus {
    async fn set(&self, key: String, value: String) {
        self.kvs
            .lock()
            .expect("local gossip mutex poisoned")
            .insert(key, value);
    }

    async fn set_many(&self, kvs: NodeKvs) {
        self.kvs
            .lock()
            .expect("local gossip mutex poisoned")
            .extend(kvs);
    }

    async fn view(&self) -> ClusterView {
        let state = NodeStateView::from_kvs(
            self.node_id.clone(),
            self.kvs.lock().expect("local gossip mutex poisoned").iter(),
        );
        ClusterView {
            members: vec![self.node_id.clone()],
            states: HashMap::from([(self.node_id.clone(), state)]),
            generated_at: Instant::now(),
        }
    }
}

struct SingleNodeTransport;

#[async_trait]
impl InternalTransport for SingleNodeTransport {
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
    use biei_core::types::StyleId;

    fn options() -> Options {
        crate::options::test_options("https://example.test/styles/{style_id}/style.json", 1)
    }

    #[tokio::test]
    async fn local_gossip_decodes_published_worker_state() {
        let node_id = NodeId::from("node-7");
        let gossip = LocalGossipBus::new(node_id.clone());
        gossip
            .set_many(NodeKvs::from([
                ("worker.0.style".to_string(), "carto/voyager@1".to_string()),
                ("worker.0.mode".to_string(), "static".to_string()),
                ("worker.0.scale".to_string(), "2x".to_string()),
                ("worker.0.queue".to_string(), "0".to_string()),
            ]))
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

        // Lazy: an id that was never explicitly registered still resolves via
        // the URL template, proving the catalog was not eagerly seeded.
        assert_eq!(
            runtime
                .style_catalog()
                .resolve_latest(&StyleId("never/registered".to_string())),
            Some(1),
            "lazy catalog resolves arbitrary ids via the URL template",
        );
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
        let tileset_url_template = Arc::<str>::from(options.tileset_url_template.clone());
        let runtime = spawn_single_node_with_catalog(catalog, tileset_url_template, &options)
            .expect("runtime spawns");
        let _ingress = runtime.http_ingress(Duration::from_secs(30));
    }

    #[test]
    fn ingress_concurrency_limit_is_derived_from_slots_and_hard_queue() {
        assert_eq!(ingress_concurrency_limit(20, 28), 840);
        assert_eq!(ingress_concurrency_limit(1, 1), 2);
        assert_eq!(ingress_concurrency_limit(0, 0), 1);
    }

    #[test]
    fn internal_forward_limit_is_independent_from_public_ingress() {
        assert_eq!(internal_forward_concurrency_limit(20, 28), 560);
        assert_eq!(internal_forward_concurrency_limit(0, 0), 1);
        assert_ne!(
            internal_forward_concurrency_limit(20, 28),
            ingress_concurrency_limit(20, 28)
        );
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
