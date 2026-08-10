//! Ishikari membership adapters for local and Chitchat-backed operation.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use ishikari_core::{
    cluster_metadata::{
        CLUSTER_ID, DEAD_NODE_GRACE_PERIOD, HTTP_ADVERTISE_ADDR_KEY,
        MARKED_FOR_DELETION_GRACE_PERIOD, project_peers,
    },
    storage::{Peer, PeerDirectory, PeerFuture, PeerSnapshotCache},
};
use mmpf_cluster::{
    Cluster, ClusterOwner, Config as ClusterNodeConfig, GossipEndpoint, HintAdmission,
    RefreshHintDedup, StyleRefreshHint, StyleRefreshHintTracker,
};
use tracing::info;

/// Runtime configuration for one production membership node.
pub(crate) struct MembershipConfig {
    pub(crate) node_id: String,
    pub(crate) gossip_endpoint: GossipEndpoint,
    pub(crate) http_advertise_addr: SocketAddr,
    pub(crate) seed_nodes: Vec<String>,
    pub(crate) gossip_interval: Duration,
}

#[derive(Clone)]
enum MembershipBackend {
    Local,
    Cluster {
        handle: Cluster,
        peers_cache: PeerSnapshotCache,
    },
}

/// Handle for querying and updating production cluster membership state.
#[derive(Clone)]
pub(crate) struct Membership {
    backend: MembershipBackend,
    self_node_id: Arc<str>,
    /// Suppresses repeated HTTP refresh hints. Without it every publisher retry
    /// performs another invalidation, and each invalidation discards concurrent
    /// in-flight provider work.
    refresh_dedup: Arc<RefreshHintDedup>,
}

/// Bounded-input membership projection used by metrics and operational status.
pub(crate) struct MembershipSnapshot {
    pub(crate) cluster_id: String,
    pub(crate) live_ids: Vec<String>,
    pub(crate) dead_ids: Vec<String>,
}

impl Membership {
    /// Creates a process-local membership view without opening a UDP socket.
    pub(crate) fn local(node_id: String) -> Self {
        Self {
            backend: MembershipBackend::Local,
            self_node_id: node_id.into(),
            refresh_dedup: Arc::new(RefreshHintDedup::default()),
        }
    }

    /// Starts production Chitchat and begins logging membership changes.
    pub(crate) async fn spawn_cluster(config: MembershipConfig) -> Result<(Self, ClusterOwner)> {
        let self_node_id = Arc::<str>::from(config.node_id.clone());
        let (cluster_config, peers_cache_ttl) = cluster_config(config);
        let owner = ClusterOwner::spawn(cluster_config)
            .await
            .context("failed to start chitchat")?;
        let membership = Self {
            backend: MembershipBackend::Cluster {
                handle: owner.handle(),
                peers_cache: PeerSnapshotCache::new(peers_cache_ttl),
            },
            self_node_id,
            refresh_dedup: Arc::new(RefreshHintDedup::default()),
        };
        membership.spawn_membership_watcher().await;
        Ok((membership, owner))
    }

    /// Marks this node as draining or active in membership state.
    pub(crate) async fn set_draining(&self, draining: bool) {
        if let MembershipBackend::Cluster { handle, .. } = &self.backend {
            handle.set_draining(draining).await;
        }
    }

    pub(crate) fn node_id(&self) -> &str {
        &self.self_node_id
    }

    pub(crate) fn is_clustered(&self) -> bool {
        matches!(self.backend, MembershipBackend::Cluster { .. })
    }

    /// Records a hint and reports whether this process has already applied it.
    ///
    /// The gossip receiver needs no equivalent: an unchanged slot value is
    /// already ignored there.
    pub(crate) fn admit_style_refresh(&self, hint: &StyleRefreshHint) -> HintAdmission {
        self.refresh_dedup.admit(hint)
    }

    /// Publishes one advisory style refresh into the bounded membership ring.
    pub(crate) async fn publish_style_refresh(&self, hint: &StyleRefreshHint) -> Result<()> {
        let encoded = hint.encode()?;
        if let MembershipBackend::Cluster { handle, .. } = &self.backend {
            handle.set(&hint.gossip_key(), &encoded).await;
        }
        Ok(())
    }

    /// Applies new cluster refresh hints with a service-owned local callback.
    pub(crate) async fn spawn_style_refresh_watcher<F>(&self, apply: F)
    where
        F: Fn(StyleRefreshHint) + Send + Sync + 'static,
    {
        let MembershipBackend::Cluster { handle, .. } = &self.backend else {
            return;
        };
        let mut live_nodes = handle.live_nodes_watcher().await;
        let self_node_id = Arc::clone(&self.self_node_id);
        tokio::spawn(async move {
            let mut tracker = StyleRefreshHintTracker::default();
            loop {
                let batch =
                    live_nodes.inspect(|nodes| tracker.observe(nodes, Some(self_node_id.as_ref())));
                if batch.invalid != 0 {
                    tracing::warn!(
                        invalid = batch.invalid,
                        "ignored invalid style refresh gossip hints"
                    );
                }
                for hint in batch.hints {
                    apply(hint);
                }
                if live_nodes.changed().await.is_err() {
                    break;
                }
            }
        });
    }

    /// Returns the membership identities needed by metrics and status without
    /// cloning every service-owned gossip key-value pair.
    pub(crate) async fn snapshot(&self) -> MembershipSnapshot {
        match &self.backend {
            MembershipBackend::Local => MembershipSnapshot {
                cluster_id: CLUSTER_ID.to_string(),
                live_ids: vec![self.self_node_id.to_string()],
                dead_ids: Vec::new(),
            },
            MembershipBackend::Cluster { handle, .. } => {
                handle
                    .inspect(|state| {
                        let mut live_ids: Vec<_> = state
                            .live_nodes()
                            .map(|node| node.id().to_string())
                            .collect();
                        live_ids.sort();

                        let mut dead_ids: Vec<_> =
                            state.dead_node_ids().map(str::to_string).collect();
                        dead_ids.sort();

                        MembershipSnapshot {
                            cluster_id: state.cluster_id().to_string(),
                            live_ids,
                            dead_ids,
                        }
                    })
                    .await
            }
        }
    }

    /// Returns routable live peers, excluding draining nodes.
    ///
    /// The short TTL is one gossip tick so routing avoids taking the Chitchat
    /// lock on every cache-missing resource request.
    async fn peers(&self) -> Arc<[Peer]> {
        match &self.backend {
            MembershipBackend::Local => Arc::from([]),
            MembershipBackend::Cluster { peers_cache, .. } => {
                peers_cache.get_or_load(|| self.read_live_peers()).await
            }
        }
    }

    /// Returns whether raw membership contains another non-draining live node.
    ///
    /// Bootstrap readiness intentionally bypasses the projected HTTP peer cache:
    /// a live node does not need routable service metadata to satisfy discovery.
    pub(crate) async fn has_other_live_node(&self) -> bool {
        match &self.backend {
            MembershipBackend::Local => false,
            MembershipBackend::Cluster { handle, .. } => {
                handle.has_other_live_node(self.self_node_id.as_ref()).await
            }
        }
    }

    async fn read_live_peers(&self) -> Arc<[Peer]> {
        let MembershipBackend::Cluster { handle, .. } = &self.backend else {
            return Arc::from([]);
        };
        handle
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
        if let MembershipBackend::Cluster { handle, .. } = &self.backend {
            handle
                .set_many(kvs.iter().map(|(key, value)| (*key, value.as_str())))
                .await;
        }
    }

    async fn spawn_membership_watcher(&self) {
        let MembershipBackend::Cluster { handle, .. } = &self.backend else {
            return;
        };
        let mut live_nodes = handle.live_nodes_watcher().await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_membership_has_one_node_and_no_routable_peers() {
        let membership = Membership::local("local-node".to_string());

        assert!(!membership.is_clustered());
        assert!(!membership.has_other_live_node().await);
        assert!(PeerDirectory::peers(&membership).await.is_empty());
        let snapshot = membership.snapshot().await;
        assert_eq!(snapshot.cluster_id, CLUSTER_ID);
        assert_eq!(snapshot.live_ids, ["local-node"]);
        assert!(snapshot.dead_ids.is_empty());

        membership.set_draining(true).await;
        membership
            .set_many(&[("ignored", "value".to_string())])
            .await;
        membership
            .publish_style_refresh(&StyleRefreshHint::new("hint", "default/style").unwrap())
            .await
            .unwrap();
    }
}
