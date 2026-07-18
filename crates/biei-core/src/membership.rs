//! Chitchat membership adapter.
//!
//! This owns exactly one real chitchat instance for the current process. It
//! implements the shared `GossipBus` used by `Node` and exposes peer advertise
//! addresses for HTTP forwarding.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use async_trait::async_trait;
use chitchat::transport::UdpTransport;
use chitchat::{
    ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState, spawn_chitchat,
};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::gossip::GossipBus;
use crate::options::Options;
use crate::types::{ClusterView, NodeId, NodeKvs, NodeStateView};
use crate::util::lock_unpoisoned;

const CLUSTER_ID: &str = "biei-production-v1";
const KV_NODE_ID: &str = "node-id";
const KV_ADVERTISE_ADDR: &str = "advertise-addr";
const KV_DRAINING: &str = "draining";
const MARKED_FOR_DELETION_GRACE_PERIOD: Duration = Duration::from_secs(300);
const PEER_ADDRESS_CACHE_TTL: Duration = Duration::from_millis(100);
/// How long a seeded node waits to discover a peer before reporting
/// gossip-ready anyway. Peer presence is a bootstrap check, not an ongoing
/// quorum — rendering needs no peers.
const GOSSIP_BOOTSTRAP_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct Membership {
    inner: Arc<MembershipInner>,
}

struct MembershipInner {
    self_node_id: NodeId,
    handle: ChitchatHandle,
    requires_peer_for_readiness: bool,
    // Bootstrap-only peer requirement: latched true once any peer has been
    // seen, and treated as satisfied past `bootstrap_deadline` regardless, so
    // an established node stays ready when gossip later partitions.
    gossip_bootstrapped: AtomicBool,
    bootstrap_deadline: Instant,
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
    pub async fn spawn(options: &Options, gossip_interval: Duration) -> anyhow::Result<Self> {
        // chitchat uses `generation_id` to detect restarts: a node that returns
        // with a HIGHER generation is recognized as a new incarnation, so peers
        // discard the old (now version-regressed) state and re-converge. A fixed
        // 0 breaks this — after an in-place restart (e.g. OOMKill) the node reuses
        // the same identity with reset versions, peers keep their stale higher
        // versions and treat it as the same dead instance, and the cluster never
        // re-forms. Use wall-clock millis at startup, which increases on every
        // restart, as chitchat recommends.
        let generation_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0);
        let chitchat_id = ChitchatId::new(
            options.node_id.to_string(),
            generation_id,
            options.gossip_bind,
        );
        // Pass seed *hostnames* (e.g. a headless Service's `biei-gossip:7946`)
        // straight through — do NOT pre-resolve to IPs here. chitchat treats any
        // entry that does not parse as a SocketAddr as a DNS name and runs its
        // own refresh loop: it resolves immediately, expands every A record (all
        // live pod IPs, so free scale + headless Service still works), and
        // re-resolves every 60s. Pre-resolving would defeat that — chitchat skips
        // the refresh for literal addresses, so a node that started before DNS
        // was ready (e.g. a freshly provisioned Spot node) would stay seedless and
        // never join. Forwarding the hostname lets such a node recover on the next
        // poll instead of deadlocking.
        let config = ChitchatConfig {
            chitchat_id,
            cluster_id: CLUSTER_ID.to_string(),
            gossip_interval,
            listen_addr: options.gossip_bind,
            seed_nodes: options.gossip_seeds.clone(),
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: MARKED_FOR_DELETION_GRACE_PERIOD,
            catchup_callback: None,
            extra_liveness_predicate: Some(Box::new(is_not_draining)),
        };
        let initial_kvs = vec![
            (KV_NODE_ID.to_string(), options.node_id.to_string()),
            (
                KV_ADVERTISE_ADDR.to_string(),
                options.internal_advertise_addr.to_string(),
            ),
            (KV_DRAINING.to_string(), "false".to_string()),
        ];
        let handle = spawn_chitchat(config, initial_kvs, &UdpTransport)
            .await
            .context("spawn production chitchat")?;
        Ok(Self {
            inner: Arc::new(MembershipInner {
                self_node_id: options.node_id.clone(),
                gossip_bootstrapped: AtomicBool::new(false),
                bootstrap_deadline: Instant::now() + GOSSIP_BOOTSTRAP_GRACE,
                handle,
                requires_peer_for_readiness: !options.gossip_seeds.is_empty(),
                peer_addresses: Mutex::new(PeerAddressCacheState::default()),
                peer_addresses_changed: Notify::new(),
            }),
        })
    }

    pub async fn set_draining(&self, draining: bool) {
        self.inner
            .handle
            .with_chitchat(|c| {
                c.self_node_state()
                    .set(KV_DRAINING, if draining { "true" } else { "false" });
            })
            .await;
    }

    pub async fn advertise_addr_of(&self, node_id: &NodeId) -> Option<SocketAddr> {
        enum Lookup {
            Return(Option<SocketAddr>),
            Refresh,
            Wait,
        }

        loop {
            let notified = self.inner.peer_addresses_changed.notified();
            let lookup = {
                let mut state = lock_unpoisoned(&self.inner.peer_addresses);
                match state.snapshot.as_ref() {
                    Some(snapshot) if snapshot.expires_at > Instant::now() => {
                        Lookup::Return(snapshot.addresses.get(node_id).copied())
                    }
                    Some(snapshot) if state.refreshing => {
                        // The address snapshot is a routing hint. Serve it
                        // briefly while one caller refreshes, rather than
                        // stampeding on the chitchat lock at every expiry.
                        Lookup::Return(snapshot.addresses.get(node_id).copied())
                    }
                    Some(_) | None if !state.refreshing => {
                        state.refreshing = true;
                        Lookup::Refresh
                    }
                    None => Lookup::Wait,
                    Some(_) => unreachable!("refreshing stale snapshot handled above"),
                }
            };

            match lookup {
                Lookup::Return(address) => return address,
                Lookup::Wait => notified.await,
                Lookup::Refresh => {
                    // Cancellation while awaiting chitchat must not leave the
                    // cache permanently marked as refreshing.
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
                    return address;
                }
            }
        }
    }

    async fn load_peer_addresses(&self) -> HashMap<NodeId, SocketAddr> {
        self.inner
            .handle
            .with_chitchat(|c| {
                let live = live_node_ids(c);
                c.node_states()
                    .iter()
                    .filter(|(cid, state)| live.contains(cid) && !is_draining(state))
                    .filter_map(|(cid, state)| {
                        let address = state_value(state, KV_ADVERTISE_ADDR)?.parse().ok()?;
                        Some((NodeId::from(cid.node_id.as_ref()), address))
                    })
                    .collect()
            })
            .await
    }

    pub async fn is_gossip_ready(&self) -> bool {
        if !self.inner.requires_peer_for_readiness {
            return true;
        }
        // Bootstrap-only: once a peer has been seen (latch) or the grace has
        // elapsed, stay ready through later partitions. Only a node that never
        // bootstrapped still waits.
        if self.inner.gossip_bootstrapped.load(Ordering::Acquire)
            || Instant::now() >= self.inner.bootstrap_deadline
        {
            return true;
        }
        let has_peer = self
            .inner
            .handle
            .with_chitchat(|c| {
                let live = live_node_ids(c);
                has_ready_peer(&self.inner.self_node_id, c.node_states(), &live)
            })
            .await;
        if has_peer {
            self.inner
                .gossip_bootstrapped
                .store(true, Ordering::Release);
        }
        has_peer
    }
}

#[async_trait]
impl GossipBus for Membership {
    async fn set(&self, node_id: NodeId, key: String, value: String) {
        if node_id != self.inner.self_node_id {
            return;
        }
        self.inner
            .handle
            .with_chitchat(|c| {
                c.self_node_state().set(&key, &value);
            })
            .await;
    }

    async fn set_many(&self, node_id: NodeId, kvs: NodeKvs) {
        if node_id != self.inner.self_node_id || kvs.is_empty() {
            return;
        }
        self.inner
            .handle
            .with_chitchat(|c| {
                let state = c.self_node_state();
                for (key, value) in &kvs {
                    state.set(key, value);
                }
            })
            .await;
    }

    async fn view(&self) -> ClusterView {
        let generated_at = Instant::now();
        self.inner
            .handle
            .with_chitchat(|c| {
                let live = live_node_ids(c);
                let mut members = Vec::new();
                let mut states = HashMap::new();
                for (cid, state) in c.node_states() {
                    if !live.contains(cid) || is_draining(state) {
                        continue;
                    }
                    let node_id = NodeId::from(cid.node_id.as_ref());
                    members.push(node_id.clone());
                    states.insert(
                        node_id.clone(),
                        NodeStateView::from_kvs(node_id, state.key_values()),
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

fn live_node_ids(c: &chitchat::Chitchat) -> HashSet<ChitchatId> {
    c.live_nodes().cloned().collect()
}

fn is_not_draining(state: &NodeState) -> bool {
    !is_draining(state)
}

fn is_draining(state: &NodeState) -> bool {
    state_value(state, KV_DRAINING) == Some("true")
}

fn state_value<'a>(state: &'a NodeState, key: &str) -> Option<&'a str> {
    state
        .key_values()
        .find_map(|(k, v)| if k == key { Some(v) } else { None })
}

fn has_ready_peer<'a, I>(self_node_id: &NodeId, states: I, live: &HashSet<ChitchatId>) -> bool
where
    I: IntoIterator<Item = (&'a ChitchatId, &'a NodeState)>,
{
    states.into_iter().any(|(cid, state)| {
        cid.node_id.as_ref() != self_node_id.as_str() && live.contains(cid) && !is_draining(state)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn draining_predicate_accepts_missing_or_false_and_rejects_true() {
        let mut state = NodeState::for_test();
        assert!(is_not_draining(&state));

        state.set(KV_DRAINING, "false");
        assert!(is_not_draining(&state));

        state.set(KV_DRAINING, "true");
        assert!(!is_not_draining(&state));
    }

    #[test]
    fn ready_peer_requires_live_non_draining_remote() {
        let self_id = NodeId::from("node-a");
        let self_cid = ChitchatId::new("node-a".to_string(), 0, "127.0.0.1:9001".parse().unwrap());
        let peer_cid = ChitchatId::new("node-b".to_string(), 0, "127.0.0.1:9002".parse().unwrap());
        let self_state = NodeState::for_test();
        let mut peer_state = NodeState::for_test();
        let mut live = HashSet::new();

        live.insert(self_cid.clone());
        assert!(!has_ready_peer(
            &self_id,
            [(&self_cid, &self_state), (&peer_cid, &peer_state)],
            &live,
        ));

        live.insert(peer_cid.clone());
        peer_state.set(KV_DRAINING, "true");
        assert!(!has_ready_peer(
            &self_id,
            [(&self_cid, &self_state), (&peer_cid, &peer_state)],
            &live,
        ));

        peer_state.set(KV_DRAINING, "false");
        assert!(has_ready_peer(
            &self_id,
            [(&self_cid, &self_state), (&peer_cid, &peer_state)],
            &live,
        ));
    }
}
