//! Chitchat membership adapter.
//!
//! This owns exactly one real chitchat instance for the current process. It
//! implements the shared `GossipBus` used by `Node` and exposes peer advertise
//! addresses for HTTP forwarding.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use async_trait::async_trait;
use chitchat::transport::UdpTransport;
use chitchat::{
    ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState, spawn_chitchat,
};
use tokio::time::Instant;

use crate::gossip::GossipBus;
use crate::options::Options;
use crate::types::{ClusterView, NodeId, NodeStateView};

const CLUSTER_ID: &str = "biei-production-v1";
const KV_NODE_ID: &str = "node-id";
const KV_ADVERTISE_ADDR: &str = "advertise-addr";
const KV_DRAINING: &str = "draining";
const MARKED_FOR_DELETION_GRACE_PERIOD: Duration = Duration::from_secs(300);

#[derive(Clone)]
pub struct Membership {
    inner: Arc<MembershipInner>,
}

struct MembershipInner {
    self_node_id: NodeId,
    handle: ChitchatHandle,
    requires_peer_for_readiness: bool,
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
                handle,
                requires_peer_for_readiness: !options.gossip_seeds.is_empty(),
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
        self.inner
            .handle
            .with_chitchat(|c| {
                let live = live_node_ids(c);
                c.node_states().iter().find_map(|(cid, state)| {
                    if cid.node_id.as_ref() != node_id.as_str()
                        || !live.contains(cid)
                        || is_draining(state)
                    {
                        return None;
                    }
                    state_value(state, KV_ADVERTISE_ADDR)?.parse().ok()
                })
            })
            .await
    }

    pub async fn is_gossip_ready(&self) -> bool {
        if !self.inner.requires_peer_for_readiness {
            return true;
        }
        self.inner
            .handle
            .with_chitchat(|c| {
                let live = live_node_ids(c);
                has_ready_peer(&self.inner.self_node_id, c.node_states(), &live)
            })
            .await
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
