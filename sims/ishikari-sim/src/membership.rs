//! Per-node simulated Ishikari membership adapter.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use ishikari_core::{
    cluster_metadata::{
        CLUSTER_ID, DEAD_NODE_GRACE_PERIOD, HTTP_ADVERTISE_ADDR_KEY,
        MARKED_FOR_DELETION_GRACE_PERIOD, project_peers,
    },
    storage::{Peer, PeerDirectory, PeerFuture, PeerSnapshotCache},
};
use mmpf_cluster::{Cluster, Config as ClusterNodeConfig, SimulatedNodeContext};

pub(crate) struct MembershipConfig {
    pub(crate) http_advertise_addr: SocketAddr,
    pub(crate) gossip_interval: Duration,
}

/// One simulated node's local membership perspective and routing cache.
#[derive(Clone)]
pub(crate) struct Membership {
    handle: Cluster,
    peers_cache: PeerSnapshotCache,
}

impl Membership {
    pub(crate) fn new(handle: Cluster, peers_cache_ttl: Duration) -> Self {
        Self {
            handle,
            peers_cache: PeerSnapshotCache::new(peers_cache_ttl),
        }
    }

    /// Returns routable live peers from a cache lasting exactly one gossip tick.
    pub(crate) async fn peers(&self) -> Arc<[Peer]> {
        self.peers_cache
            .get_or_load(|| self.read_live_peers())
            .await
    }

    /// Observes this node's local view without populating or extending its routing cache.
    pub(crate) async fn peers_for_observation(&self) -> Arc<[Peer]> {
        if let Some(peers) = self.peers_cache.get() {
            return peers;
        }
        self.read_live_peers().await
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
}

impl PeerDirectory for Membership {
    fn peers(&self) -> PeerFuture<'_> {
        Box::pin(Membership::peers(self))
    }
}

pub(crate) fn cluster_config(
    config: &MembershipConfig,
    context: SimulatedNodeContext,
) -> ClusterNodeConfig {
    ClusterNodeConfig::new(
        CLUSTER_ID,
        context.node_id,
        context.gossip_endpoint,
        context.seed_nodes,
        config.gossip_interval,
        MARKED_FOR_DELETION_GRACE_PERIOD,
    )
    .with_dead_node_grace_period(DEAD_NODE_GRACE_PERIOD)
    .with_initial_key_values(vec![(
        HTTP_ADVERTISE_ADDR_KEY.to_string(),
        config.http_advertise_addr.to_string(),
    )])
}
