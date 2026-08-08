//! Chitchat membership adapter.
//!
//! This owns exactly one real chitchat instance for the current process. It
//! implements the shared `GossipBus` used by `Node` and exposes peer advertise
//! addresses for HTTP forwarding.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use mmpf_cluster::{
    Cluster, ClusterOwner, Config as ClusterNodeConfig, GossipEndpoint, HintAdmission,
    RefreshHintDedup, StyleRefreshHint, StyleRefreshHintTracker,
};
use tokio::sync::Notify;
use tokio::time::Instant;

use biei_core::gossip::GossipBus;
use biei_core::style_catalog::{
    STYLE_REVISION_GOSSIP_SLOTS, StyleRevisionObservation, style_revision_gossip_key,
};
use biei_core::types::{ClusterView, NodeId, NodeKvs, NodeStateView};
use mmpf_common::sync::{lock_unpoisoned, wait_for_change};

// Bump this epoch whenever the gossip or internal-forwarding contract changes.
// Different epochs must not route work to one another during a rolling deploy.
const CLUSTER_ID: &str = "biei-production-v3";
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
    /// Suppresses repeated HTTP refresh hints. Each hint pulls the next provider
    /// check forward and invalidates in-flight fetch fences, so a retry sequence
    /// must not be applied more than once.
    refresh_dedup: Arc<RefreshHintDedup>,
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
                    refresh_dedup: Arc::new(RefreshHintDedup::default()),
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

    /// Hands out the deduplicator so the HTTP receiver shares one window with
    /// the gossip receiver — a hint that arrives both ways is applied once.
    ///
    /// Ownership sits on the HTTP state rather than here because `Membership` is
    /// absent in single-node mode while the refresh route is mounted either way:
    /// reading the window through an `Option<Membership>` silently disabled the
    /// retry bound in exactly the configuration that has no gossip to fall back
    /// on.
    pub(crate) fn refresh_dedup(&self) -> Arc<RefreshHintDedup> {
        Arc::clone(&self.inner.refresh_dedup)
    }

    /// Publishes one advisory style refresh into the bounded membership ring.
    pub(crate) async fn publish_style_refresh(
        &self,
        hint: &StyleRefreshHint,
    ) -> anyhow::Result<()> {
        let encoded = hint.encode()?;
        self.inner.handle.set(&hint.gossip_key(), &encoded).await;
        Ok(())
    }

    /// Applies refresh hints before revision observations from each membership
    /// snapshot, preserving their causal order even when both first appear at
    /// once on a joining node.
    pub(crate) async fn spawn_style_state_watcher<F, R>(&self, apply_hint: F, apply_revision: R)
    where
        F: Fn(StyleRefreshHint) + Send + Sync + 'static,
        R: Fn(StyleRevisionObservation) + Send + Sync + 'static,
    {
        let mut live_nodes = self.inner.handle.live_nodes_watcher().await;
        let self_node_id = self.inner.self_node_id.to_string();
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let mut tracker = StyleRefreshHintTracker::default();
            let mut revision_tracker = StyleRevisionTracker::default();
            loop {
                let (batch, revisions) = live_nodes.inspect(|nodes| {
                    (
                        tracker.observe(nodes, Some(&self_node_id)),
                        revision_tracker.observe(nodes, Some(&self_node_id)),
                    )
                });
                if batch.invalid != 0 {
                    tracing::warn!(
                        invalid = batch.invalid,
                        "ignored invalid style refresh gossip hints"
                    );
                }
                for hint in batch.hints {
                    if inner.refresh_dedup.admit(&hint) == HintAdmission::Accepted {
                        apply_hint(hint);
                    }
                }
                if revisions.invalid != 0 {
                    tracing::warn!(
                        invalid = revisions.invalid,
                        "ignored invalid observed style revision gossip"
                    );
                }
                for observation in revisions.observations {
                    apply_revision(observation);
                }
                if live_nodes.changed().await.is_err() {
                    break;
                }
            }
        });
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

#[derive(Default)]
struct StyleRevisionTracker {
    seen: BTreeMap<(String, usize), String>,
}

struct StyleRevisionBatch {
    observations: Vec<StyleRevisionObservation>,
    invalid: usize,
}

impl StyleRevisionTracker {
    fn observe(
        &mut self,
        nodes: mmpf_cluster::LiveNodesRef<'_>,
        excluded_node_id: Option<&str>,
    ) -> StyleRevisionBatch {
        let mut current = BTreeMap::new();
        let mut observations = Vec::new();
        let mut invalid = 0;
        for node in nodes.nodes() {
            if excluded_node_id == Some(node.id()) {
                continue;
            }
            for slot in 0..STYLE_REVISION_GOSSIP_SLOTS {
                let key = style_revision_gossip_key(slot);
                let Some(value) = node.get(&key) else {
                    continue;
                };
                let identity = (node.id().to_owned(), slot);
                current.insert(identity.clone(), value.to_owned());
                if self.seen.get(&identity).is_some_and(|seen| seen == value) {
                    continue;
                }
                match StyleRevisionObservation::decode(value) {
                    Some(observation) => observations.push(observation),
                    None => invalid += 1,
                }
            }
        }
        self.seen = current;
        StyleRevisionBatch {
            observations,
            invalid,
        }
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
