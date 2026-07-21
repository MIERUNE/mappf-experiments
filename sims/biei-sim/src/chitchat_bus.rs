//! Node-local Biei gossip adapters backed by the shared simulated cluster network.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use mmpf_cluster::{Cluster, Config, SimulatedNetwork, SimulatedNodeContext};
use tokio::sync::RwLock;
use tokio::time::Instant;

use biei_core::gossip::GossipBus;
use biei_core::types::{ClusterView, NodeId, NodeKvs, NodeStateView};

struct ActiveMembers {
    ids: RwLock<Vec<NodeId>>,
    epoch: AtomicU64,
}

impl ActiveMembers {
    fn new() -> Self {
        Self {
            ids: RwLock::new(Vec::new()),
            epoch: AtomicU64::new(0),
        }
    }

    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }
}

/// Owns Biei's authoritative active membership and shared Chitchat lifecycle.
pub struct ChitchatGossipNetwork {
    network: SimulatedNetwork,
    members: Arc<ActiveMembers>,
    gossip_interval: Duration,
}

impl ChitchatGossipNetwork {
    pub fn new(gossip_interval: Duration, hop_latency: Duration) -> Self {
        Self {
            network: SimulatedNetwork::new(hop_latency),
            members: Arc::new(ActiveMembers::new()),
            gossip_interval,
        }
    }

    /// Adds one authoritative member and returns its node-local gossip adapter.
    pub async fn add_node(&self, node_id: NodeId) -> Result<ChitchatGossipBus> {
        let spawned = self
            .network
            .spawn(node_id.to_string(), |context| {
                cluster_config(context, self.gossip_interval)
            })
            .await?;
        {
            let mut members = self.members.ids.write().await;
            ensure!(
                !members.contains(&node_id),
                "duplicate simulator member {node_id}"
            );
            members.push(node_id.clone());
            self.members.epoch.fetch_add(1, Ordering::AcqRel);
        }
        Ok(ChitchatGossipBus {
            handle: spawned.cluster(),
            members: Arc::clone(&self.members),
            local_revision: AtomicU64::new(0),
            view_cache: Mutex::new(None),
        })
    }

    /// Removes a member from routing immediately, then awaits owner termination.
    pub async fn remove_node(&self, node_id: &NodeId) -> Result<()> {
        {
            let mut members = self.members.ids.write().await;
            ensure!(
                members.contains(node_id),
                "unknown active simulator node {node_id}"
            );
            members.retain(|member| member != node_id);
            self.members.epoch.fetch_add(1, Ordering::AcqRel);
        }
        self.network
            .remove(node_id.as_str())
            .await
            .with_context(|| format!("failed to remove Biei gossip node {node_id}"))
    }

    pub async fn shutdown_all(&self) -> Result<()> {
        {
            let mut members = self.members.ids.write().await;
            if !members.is_empty() {
                members.clear();
                self.members.epoch.fetch_add(1, Ordering::AcqRel);
            }
        }
        self.network.shutdown_all().await
    }
}

fn cluster_config(context: SimulatedNodeContext, gossip_interval: Duration) -> Config {
    Config::new(
        "biei-sim",
        context.node_id,
        context.gossip_endpoint,
        context.seed_nodes,
        gossip_interval,
        Duration::from_secs(3_600),
    )
}

/// One Biei node's self-scoped writes and local Chitchat membership perspective.
pub struct ChitchatGossipBus {
    handle: Cluster,
    members: Arc<ActiveMembers>,
    local_revision: AtomicU64,
    view_cache: Mutex<Option<CachedView>>,
}

struct CachedView {
    membership_epoch: u64,
    local_revision: u64,
    view: ClusterView,
}

impl ChitchatGossipBus {
    fn invalidate_local_view(&self) {
        self.local_revision.fetch_add(1, Ordering::AcqRel);
        *self.view_cache.lock().expect("view cache poisoned") = None;
    }

    async fn load_view(&self) -> (u64, u64, ClusterView) {
        loop {
            let membership_epoch = self.members.epoch();
            let local_revision = self.local_revision.load(Ordering::Acquire);
            let members = self.members.ids.read().await.clone();
            let active_members: HashSet<_> = members.iter().cloned().collect();
            let states = self
                .handle
                .inspect(|state| {
                    let mut out: HashMap<NodeId, NodeStateView> = HashMap::new();
                    for node in state.live_nodes() {
                        let node_id = NodeId::from(node.id());
                        if !active_members.contains(&node_id) {
                            continue;
                        }
                        out.insert(
                            node_id.clone(),
                            NodeStateView::from_kvs(node_id, node.key_values()),
                        );
                    }
                    out
                })
                .await;
            if self.members.epoch() == membership_epoch
                && self.local_revision.load(Ordering::Acquire) == local_revision
            {
                return (
                    membership_epoch,
                    local_revision,
                    ClusterView {
                        members,
                        states,
                        generated_at: Instant::now(),
                    },
                );
            }
        }
    }
}

#[async_trait]
impl GossipBus for ChitchatGossipBus {
    async fn set(&self, key: String, value: String) {
        self.handle.set(&key, &value).await;
        self.invalidate_local_view();
    }

    async fn set_many(&self, kvs: NodeKvs) {
        if kvs.is_empty() {
            return;
        }
        self.handle
            .set_many(
                kvs.iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .await;
        self.invalidate_local_view();
    }

    fn view_epoch(&self) -> u64 {
        self.members.epoch()
    }

    async fn view(&self) -> ClusterView {
        let now = Instant::now();
        let membership_epoch = self.members.epoch();
        let local_revision = self.local_revision.load(Ordering::Acquire);
        if let Some(cached) = self
            .view_cache
            .lock()
            .expect("view cache poisoned")
            .as_ref()
            && cached.membership_epoch == membership_epoch
            && cached.local_revision == local_revision
            && cached.view.generated_at == now
        {
            return cached.view.clone();
        }

        let (loaded_membership_epoch, loaded_local_revision, view) = self.load_view().await;
        *self.view_cache.lock().expect("view cache poisoned") = Some(CachedView {
            membership_epoch: loaded_membership_epoch,
            local_revision: loaded_local_revision,
            view: view.clone(),
        });
        view
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use biei_core::types::RENDER_ADMISSION_GOSSIP_KEY;

    #[tokio::test(start_paused = true)]
    async fn nodes_can_temporarily_observe_different_local_views() {
        let network =
            ChitchatGossipNetwork::new(Duration::from_millis(20), Duration::from_millis(100));
        let first_id = NodeId::from("node-a");
        let second_id = NodeId::from("node-b");
        let first = network
            .add_node(first_id.clone())
            .await
            .expect("first node");
        let second = network
            .add_node(second_id.clone())
            .await
            .expect("second node");
        second
            .set(RENDER_ADMISSION_GOSSIP_KEY.to_string(), "true".to_string())
            .await;

        let first_view = first.view().await;
        let second_view = second.view().await;
        assert!(
            second_view
                .states
                .get(&second_id)
                .is_some_and(|state| state.accepts_new_renders)
        );
        assert!(
            !first_view
                .states
                .get(&second_id)
                .is_some_and(|state| state.accepts_new_renders),
            "the unpropagated remote state must not appear in node-a's local view"
        );

        network.shutdown_all().await.expect("shutdown network");
    }

    #[tokio::test(start_paused = true)]
    async fn removed_states_are_immediately_excluded_from_routing_views() {
        let network = ChitchatGossipNetwork::new(Duration::from_millis(20), Duration::ZERO);
        let first_id = NodeId::from("node-a");
        let removed_id = NodeId::from("node-b");
        let first = network.add_node(first_id).await.expect("first node");
        let removed = network
            .add_node(removed_id.clone())
            .await
            .expect("removed node");
        removed
            .set(RENDER_ADMISSION_GOSSIP_KEY.to_string(), "true".to_string())
            .await;

        let mut propagated = false;
        for _ in 0..50 {
            tokio::time::advance(Duration::from_millis(20)).await;
            tokio::task::yield_now().await;
            if first
                .view()
                .await
                .states
                .get(&removed_id)
                .is_some_and(|state| state.accepts_new_renders)
            {
                propagated = true;
                break;
            }
        }
        assert!(
            propagated,
            "the remote state should propagate before removal"
        );

        network.remove_node(&removed_id).await.expect("remove node");
        let projected = first.view().await;
        assert!(!projected.members.contains(&removed_id));
        assert!(!projected.states.contains_key(&removed_id));
        assert!(
            projected
                .states
                .keys()
                .all(|node_id| projected.members.contains(node_id))
        );

        network.shutdown_all().await.expect("shutdown network");
    }
}
