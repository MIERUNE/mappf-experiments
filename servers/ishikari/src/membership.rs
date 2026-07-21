//! Production Ishikari membership adapter backed by Chitchat.

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use ishikari_core::{
    cluster_metadata::{
        CLUSTER_ID, DEAD_NODE_GRACE_PERIOD, HTTP_ADVERTISE_ADDR_KEY,
        MARKED_FOR_DELETION_GRACE_PERIOD, project_peers,
    },
    storage::{Peer, PeerDirectory, PeerFuture, PeerSnapshotCache},
};
use mmpf_cluster::{Cluster, ClusterOwner, Config as ClusterNodeConfig, GossipEndpoint};
use tracing::info;

/// Runtime configuration for one production membership node.
pub(crate) struct MembershipConfig {
    pub(crate) node_id: String,
    pub(crate) gossip_endpoint: GossipEndpoint,
    pub(crate) http_advertise_addr: SocketAddr,
    pub(crate) seed_nodes: Vec<String>,
    pub(crate) gossip_interval: Duration,
}

/// Handle for querying and updating production cluster membership state.
#[derive(Clone)]
pub(crate) struct Membership {
    handle: Cluster,
    self_node_id: Arc<str>,
    peers_cache: PeerSnapshotCache,
}

/// Snapshot of the current cluster state exposed by the diagnostics API.
#[derive(serde::Serialize)]
pub(crate) struct ClusterView {
    cluster_id: String,
    nodes: BTreeMap<String, NodeView>,
    pub(crate) live_ids: Vec<String>,
    pub(crate) dead_ids: Vec<String>,
}

/// Per-node diagnostic snapshot containing membership key-value pairs.
#[derive(serde::Serialize)]
struct NodeView {
    key_values: BTreeMap<String, String>,
}

impl Membership {
    /// Starts production Chitchat and begins logging membership changes.
    pub(crate) async fn spawn(config: MembershipConfig) -> Result<(Self, ClusterOwner)> {
        let self_node_id = Arc::<str>::from(config.node_id.clone());
        let (cluster_config, peers_cache_ttl) = cluster_config(config);
        let owner = ClusterOwner::spawn(cluster_config)
            .await
            .context("failed to start chitchat")?;
        let membership = Self {
            handle: owner.handle(),
            self_node_id,
            peers_cache: PeerSnapshotCache::new(peers_cache_ttl),
        };
        membership.spawn_membership_watcher().await;
        Ok((membership, owner))
    }

    /// Marks this node as draining or active in membership state.
    pub(crate) async fn set_draining(&self, draining: bool) {
        self.handle.set_draining(draining).await;
    }

    /// Returns a cluster-wide diagnostic snapshot.
    pub(crate) async fn cluster_view(&self) -> ClusterView {
        self.handle
            .inspect(|state| {
                let nodes = state
                    .nodes()
                    .map(|node| {
                        let key_values = node
                            .key_values()
                            .map(|(key, value)| (key.to_string(), value.to_string()))
                            .collect();
                        (node.id().to_string(), NodeView { key_values })
                    })
                    .collect();

                let mut live_ids: Vec<_> = state
                    .live_nodes()
                    .map(|node| node.id().to_string())
                    .collect();
                live_ids.sort();

                let mut dead_ids: Vec<_> = state.dead_node_ids().map(str::to_string).collect();
                dead_ids.sort();

                ClusterView {
                    cluster_id: state.cluster_id().to_string(),
                    nodes,
                    live_ids,
                    dead_ids,
                }
            })
            .await
    }

    /// Returns routable live peers, excluding draining nodes.
    ///
    /// The short TTL is one gossip tick so routing avoids taking the Chitchat
    /// lock on every cache-missing resource request.
    async fn peers(&self) -> Arc<[Peer]> {
        self.peers_cache
            .get_or_load(|| self.read_live_peers())
            .await
    }

    /// Returns whether raw membership contains another non-draining live node.
    ///
    /// Bootstrap readiness intentionally bypasses the projected HTTP peer cache:
    /// a live node does not need routable service metadata to satisfy discovery.
    pub(crate) async fn has_other_live_node(&self) -> bool {
        self.handle
            .has_other_live_node(self.self_node_id.as_ref())
            .await
    }

    async fn read_live_peers(&self) -> Arc<[Peer]> {
        self.handle
            .inspect(|state| {
                project_peers(
                    state
                        .live_nodes()
                        .map(|node| (node.id(), node.get(HTTP_ADVERTISE_ADDR_KEY))),
                )
            })
            .await
    }

    /// Sets multiple values on the self node's membership state.
    pub(crate) async fn set_many(&self, kvs: &[(&str, String)]) {
        self.handle
            .set_many(kvs.iter().map(|(key, value)| (*key, value.as_str())))
            .await;
    }

    async fn spawn_membership_watcher(&self) {
        let mut live_nodes = self.handle.live_nodes_watcher().await;
        tokio::spawn(async move {
            let mut previous_peers: Option<Arc<[Peer]>> = None;
            loop {
                let peers = live_nodes.inspect(|state| {
                    project_peers(
                        state
                            .nodes()
                            .map(|node| (node.id(), node.get(HTTP_ADVERTISE_ADDR_KEY))),
                    )
                });
                if previous_peers.as_ref() != Some(&peers) {
                    let peers_str = format!(
                        "[{}]",
                        peers
                            .iter()
                            .map(|peer| format!("\"{}\"", peer.addr))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    info!(peers = %peers_str, "membership changed");
                    previous_peers = Some(peers);
                }

                if live_nodes.changed().await.is_err() {
                    break;
                }
            }
        });
    }
}

impl PeerDirectory for Membership {
    fn peers(&self) -> PeerFuture<'_> {
        Box::pin(Membership::peers(self))
    }
}

fn cluster_config(config: MembershipConfig) -> (ClusterNodeConfig, Duration) {
    let peers_cache_ttl = config.gossip_interval;
    (
        ClusterNodeConfig::new(
            CLUSTER_ID,
            config.node_id,
            config.gossip_endpoint,
            config.seed_nodes,
            config.gossip_interval,
            MARKED_FOR_DELETION_GRACE_PERIOD,
        )
        .with_dead_node_grace_period(DEAD_NODE_GRACE_PERIOD)
        .with_initial_key_values(vec![(
            HTTP_ADVERTISE_ADDR_KEY.to_string(),
            config.http_advertise_addr.to_string(),
        )]),
        peers_cache_ttl,
    )
}
