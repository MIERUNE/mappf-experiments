//! Chitchat membership adapter.
//!
//! This owns exactly one real chitchat instance for the current process. It
//! implements the shared `GossipBus` used by `Node` and exposes peer advertise
//! addresses for HTTP forwarding.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use mmpf_cluster::{Cluster, ClusterOwner, Config as ClusterNodeConfig, GossipEndpoint};
use tokio::sync::Notify;
use tokio::time::Instant;

use biei_core::gossip::GossipBus;
use biei_core::types::{ClusterView, NodeId, NodeKvs, NodeStateView};
use mmpf_common::sync::{lock_unpoisoned, wait_for_change};

// Bump this epoch whenever the gossip or internal-forwarding contract changes.
// Different epochs must not route work to one another during a rolling deploy.
const CLUSTER_ID: &str = "biei-production-v2";
const KV_ADVERTISE_ADDR: &str = "advertise-addr";

const MARKED_FOR_DELETION_GRACE_PERIOD: Duration = Duration::from_secs(300);
const PEER_ADDRESS_CACHE_TTL: Duration = Duration::from_millis(100);

/// Runtime configuration for one production membership node.
pub(crate) struct MembershipConfig {
    pub(crate) node_id: NodeId,
    pub(crate) gossip_endpoint: GossipEndpoint,
    pub(crate) http_advertise_addr: SocketAddr,
    pub(crate) seed_nodes: Vec<String>,
    pub(crate) gossip_interval: Duration,
}

#[derive(Clone)]
pub(crate) struct Membership {
    inner: Arc<MembershipInner>,
}

struct MembershipInner {
    self_node_id: NodeId,
    handle: Cluster,
    peer_addresses: Mutex<PeerAddressCacheState>,
    peer_addresses_changed: Notify,
}

struct CachedPeerAddresses {
    expires_at: Instant,
    addresses: HashMap<NodeId, SocketAddr>,
}

#[derive(Default)]
struct PeerAddressCacheState {
    snapshot: Option<CachedPeerAddresses>,
    refreshing: bool,
}

struct PeerAddressRefreshGuard {
    inner: Arc<MembershipInner>,
    completed: bool,
}

impl Drop for PeerAddressRefreshGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = lock_unpoisoned(&self.inner.peer_addresses);
        state.refreshing = false;
        drop(state);
        self.inner.peer_addresses_changed.notify_waiters();
    }
}

impl Membership {
    pub(crate) async fn spawn(config: MembershipConfig) -> anyhow::Result<(Self, ClusterOwner)> {
        let MembershipConfig {
            node_id,
            gossip_endpoint,
            http_advertise_addr,
            seed_nodes,
            gossip_interval,
        } = config;
        // Pass seed *hostnames* (e.g. a headless Service's `biei-gossip:7946`)
        // straight through so Chitchat retains its DNS refresh behavior.
        let owner = ClusterOwner::spawn(
            ClusterNodeConfig::new(
                CLUSTER_ID,
                node_id.to_string(),
                gossip_endpoint,
                seed_nodes,
                gossip_interval,
                MARKED_FOR_DELETION_GRACE_PERIOD,
            )
            .with_initial_key_values(vec![(
                KV_ADVERTISE_ADDR.to_string(),
                http_advertise_addr.to_string(),
            )]),
        )
        .await
        .context("spawn production chitchat")?;
        let handle = owner.handle();
        Ok((
            Self {
                inner: Arc::new(MembershipInner {
                    self_node_id: node_id,
                    handle,
                    peer_addresses: Mutex::new(PeerAddressCacheState::default()),
                    peer_addresses_changed: Notify::new(),
                }),
            },
            owner,
        ))
    }

    pub(crate) async fn set_draining(&self, draining: bool) {
        self.inner.handle.set_draining(draining).await;
    }

    pub(crate) async fn advertise_addr_of(&self, node_id: &NodeId) -> Option<SocketAddr> {
        enum Lookup {
            Return(Option<SocketAddr>),
            Refresh,
        }

        // `Continue` here means "wait for a refresh another caller owns"; the
        // refresh producer signals via `notify_waiters` after publishing the
        // snapshot, so `wait_for_change` cannot lose that wakeup.
        let lookup = wait_for_change(&self.inner.peer_addresses_changed, || {
            let mut state = lock_unpoisoned(&self.inner.peer_addresses);
            match state.snapshot.as_ref() {
                Some(snapshot) if snapshot.expires_at > Instant::now() => {
                    ControlFlow::Break(Lookup::Return(snapshot.addresses.get(node_id).copied()))
                }
                Some(snapshot) if state.refreshing => {
                    // The address snapshot is a routing hint. Serve it briefly
                    // while one caller refreshes, rather than stampeding on the
                    // chitchat lock at every expiry.
                    ControlFlow::Break(Lookup::Return(snapshot.addresses.get(node_id).copied()))
                }
                Some(_) | None if !state.refreshing => {
                    state.refreshing = true;
                    ControlFlow::Break(Lookup::Refresh)
                }
                None => ControlFlow::Continue(()),
                Some(_) => unreachable!("refreshing stale snapshot handled above"),
            }
        })
        .await;

        match lookup {
            Lookup::Return(address) => address,
            Lookup::Refresh => {
                // Cancellation while awaiting chitchat must not leave the cache
                // permanently marked as refreshing.
                let mut refresh_guard = PeerAddressRefreshGuard {
                    inner: Arc::clone(&self.inner),
                    completed: false,
                };
                let addresses = self.load_peer_addresses().await;
                let address = addresses.get(node_id).copied();
                let mut state = lock_unpoisoned(&self.inner.peer_addresses);
                state.snapshot = Some(CachedPeerAddresses {
                    expires_at: Instant::now() + PEER_ADDRESS_CACHE_TTL,
                    addresses,
                });
                state.refreshing = false;
                drop(state);
                refresh_guard.completed = true;
                self.inner.peer_addresses_changed.notify_waiters();
                address
            }
        }
    }

    async fn load_peer_addresses(&self) -> HashMap<NodeId, SocketAddr> {
        self.inner
            .handle
            .inspect(|state| {
                state
                    .live_nodes()
                    .filter_map(|node| {
                        let address = node.get(KV_ADVERTISE_ADDR)?.parse().ok()?;
                        Some((NodeId::from(node.id()), address))
                    })
                    .collect()
            })
            .await
    }

    /// Returns whether raw membership contains another non-draining live node.
    ///
    /// Bootstrap readiness intentionally bypasses the peer-address cache: a live
    /// node does not need routable forwarding metadata to satisfy discovery.
    pub(crate) async fn has_other_live_node(&self) -> bool {
        self.inner
            .handle
            .has_other_live_node(self.inner.self_node_id.as_str())
            .await
    }
}

#[async_trait]
impl GossipBus for Membership {
    async fn set(&self, key: String, value: String) {
        self.inner.handle.set(&key, &value).await;
    }

    async fn set_many(&self, kvs: NodeKvs) {
        if kvs.is_empty() {
            return;
        }
        self.inner
            .handle
            .set_many(
                kvs.iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .await;
    }

    async fn view(&self) -> ClusterView {
        let generated_at = Instant::now();
        self.inner
            .handle
            .inspect(|state| {
                let mut members = Vec::new();
                let mut states = HashMap::new();
                for node in state.live_nodes() {
                    let node_id = NodeId::from(node.id());
                    members.push(node_id.clone());
                    states.insert(
                        node_id.clone(),
                        NodeStateView::from_kvs(node_id, node.key_values()),
                    );
                }
                ClusterView {
                    members,
                    states,
                    generated_at,
                }
            })
            .await
    }
}
