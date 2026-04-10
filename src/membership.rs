//! Cluster membership built on chitchat.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use chitchat::{
    ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState, spawn_chitchat,
    transport::UdpTransport,
};
use tracing::info;

const CLUSTER_ID: &str = "ishikari";
const HTTP_PORT_KEY: &str = "http-port";
const HTTP_ADVERTISE_ADDR_KEY: &str = "http-advertise-addr";
const DRAINING_KEY: &str = "draining";
const DEFAULT_HTTP_PORT: u16 = 8080;

/// Runtime configuration for the chitchat membership node.
pub struct MembershipConfig {
    pub node_id: String,
    pub listen_addr: SocketAddr,
    pub advertise_addr: SocketAddr,
    pub http_advertise_addr: SocketAddr,
    pub http_port: u16,
    pub seed_nodes: Vec<String>,
    pub gossip_interval: Duration,
}

/// Short-TTL cache of the decoded routable peer list, with the time it was built.
type PeersCache = Arc<Mutex<Option<(Instant, Vec<Peer>)>>>;

/// Handle for querying and updating cluster membership state.
#[derive(Clone)]
pub struct Membership {
    node_id: String,
    handle: Arc<ChitchatHandle>,
    ready: Arc<AtomicBool>,
    peers_cache: PeersCache,
    peers_cache_ttl: Duration,
}

/// Snapshot of the current cluster state exposed by the HTTP API.
#[derive(serde::Serialize)]
pub struct ClusterView {
    pub cluster_id: String,
    pub nodes: BTreeMap<String, NodeView>,
    pub live_ids: Vec<String>,
    pub dead_ids: Vec<String>,
}

/// Per-node snapshot containing the gossip key-value pairs.
#[derive(serde::Serialize)]
pub struct NodeView {
    pub key_values: BTreeMap<String, String>,
}

/// Reachable peer information derived from membership gossip state.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct Peer {
    pub id: String,
    pub addr: SocketAddr,
}

impl Membership {
    /// Starts chitchat and begins logging membership changes.
    pub async fn spawn(config: MembershipConfig) -> Result<Self> {
        // Routing reads the peer list per cache-missing tile; cache it for one
        // gossip tick so the hot path does not lock chitchat on every request.
        let peers_cache_ttl = config.gossip_interval;
        let generation_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_millis() as u64;
        let chitchat_id =
            ChitchatId::new(config.node_id.clone(), generation_id, config.advertise_addr);
        let chitchat_config = ChitchatConfig {
            chitchat_id,
            cluster_id: CLUSTER_ID.to_string(),
            gossip_interval: config.gossip_interval,
            listen_addr: config.listen_addr,
            seed_nodes: config.seed_nodes,
            failure_detector_config: FailureDetectorConfig {
                dead_node_grace_period: Duration::from_secs(30),
                ..FailureDetectorConfig::default()
            },
            marked_for_deletion_grace_period: Duration::from_hours(1),
            catchup_callback: None,
            extra_liveness_predicate: Some(Box::new(|node_state| {
                node_state.get(DRAINING_KEY) != Some("true")
            })),
        };
        let initial_key_values = vec![
            (HTTP_PORT_KEY.to_string(), config.http_port.to_string()),
            (
                HTTP_ADVERTISE_ADDR_KEY.to_string(),
                config.http_advertise_addr.to_string(),
            ),
            (DRAINING_KEY.to_string(), "false".to_string()),
        ];
        let has_seeds = !chitchat_config.seed_nodes.is_empty();
        let handle = spawn_chitchat(chitchat_config, initial_key_values, &UdpTransport)
            .await
            .context("failed to start chitchat")?;
        let ready = Arc::new(AtomicBool::new(!has_seeds));
        let membership = Self {
            node_id: config.node_id,
            handle: Arc::new(handle),
            ready,
            peers_cache: Arc::new(Mutex::new(None)),
            peers_cache_ttl,
        };

        membership.spawn_membership_watcher().await;

        Ok(membership)
    }

    /// Marks this node as draining or active in membership state.
    pub async fn set_draining(&self, draining: bool) {
        self.handle
            .with_chitchat(|chitchat| {
                chitchat.self_node_state().set(DRAINING_KEY, draining);
            })
            .await;
    }

    /// Returns whether this node has joined the cluster.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    /// Returns whether this node currently advertises a draining state.
    pub async fn is_draining(&self) -> bool {
        let node_id = self.node_id.clone();
        self.handle
            .with_chitchat(move |chitchat| {
                chitchat
                    .node_states()
                    .iter()
                    .find(|(peer_id, _)| *peer_id.node_id == *node_id)
                    .and_then(|(_, node_state)| node_state.get(DRAINING_KEY))
                    .map(|v| v == "true")
                    .unwrap_or(false)
            })
            .await
    }

    /// Starts a chitchat shutdown sequence.
    pub fn shutdown(&self) -> Result<()> {
        self.handle
            .initiate_shutdown()
            .context("failed to initiate chitchat shutdown")
    }

    /// Returns a cluster-wide membership snapshot.
    pub async fn cluster_view(&self) -> ClusterView {
        self.handle
            .with_chitchat(|chitchat| {
                let snapshot = chitchat.state_snapshot();

                let nodes = snapshot
                    .node_states
                    .iter()
                    .map(|node_state| {
                        let id = node_state.chitchat_id().node_id.to_string();
                        let key_values = node_state
                            .key_values()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();
                        (id, NodeView { key_values })
                    })
                    .collect();

                let mut live_ids: Vec<_> = chitchat
                    .live_nodes()
                    .map(|node| node.node_id.to_string())
                    .collect();
                live_ids.sort();

                let mut dead_ids: Vec<_> = chitchat
                    .dead_nodes()
                    .map(|node| node.node_id.to_string())
                    .collect();
                dead_ids.sort();

                ClusterView {
                    cluster_id: chitchat.cluster_id().to_string(),
                    nodes,
                    live_ids,
                    dead_ids,
                }
            })
            .await
    }

    /// Returns routable live peers, excluding draining nodes.
    ///
    /// Served from a short-TTL cache (one gossip tick) so the routing hot path
    /// avoids locking chitchat on every cache-missing tile request.
    pub async fn peers(&self) -> Vec<Peer> {
        if let Some(peers) = self.cached_peers() {
            return peers;
        }
        let peers = self
            .handle
            .with_chitchat(|chitchat| {
                let live_nodes = chitchat
                    .live_nodes()
                    .filter_map(|peer_id| {
                        chitchat
                            .node_state(peer_id)
                            .cloned()
                            .map(|node_state| (peer_id.clone(), node_state))
                    })
                    .collect::<BTreeMap<_, _>>();
                collect_live_peers_from_nodes(&live_nodes)
            })
            .await;
        *self.peers_cache.lock().expect("peers cache poisoned") =
            Some((Instant::now(), peers.clone()));
        peers
    }

    /// Returns the cached peer list if it is still within the TTL.
    fn cached_peers(&self) -> Option<Vec<Peer>> {
        let guard = self.peers_cache.lock().expect("peers cache poisoned");
        guard.as_ref().and_then(|(stored_at, peers)| {
            (stored_at.elapsed() < self.peers_cache_ttl).then(|| peers.clone())
        })
    }

    /// Sets multiple key-value pairs on the self node's chitchat state.
    pub async fn set_many(&self, kvs: &[(&str, String)]) {
        self.handle
            .with_chitchat(|chitchat| {
                let state = chitchat.self_node_state();
                for (key, value) in kvs {
                    state.set(*key, value.as_str());
                }
            })
            .await;
    }

    /// Spawns a background task that tracks membership changes and readiness.
    async fn spawn_membership_watcher(&self) {
        let mut live_nodes = self
            .handle
            .with_chitchat(|chitchat| chitchat.live_nodes_watcher())
            .await;
        let node_id = self.node_id.clone();
        let ready = self.ready.clone();

        tokio::spawn(async move {
            let mut previous_peers: Option<Vec<Peer>> = None;
            loop {
                let peers = collect_live_peers_from_nodes(&live_nodes.borrow());
                if !ready.load(Ordering::Relaxed) && peers.iter().any(|peer| peer.id != node_id) {
                    ready.store(true, Ordering::Relaxed);
                    info!("node is ready");
                }
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
                    previous_peers = Some(peers.clone());
                }

                if live_nodes.changed().await.is_err() {
                    break;
                }
            }
        });
    }
}

/// Converts live chitchat nodes into routable HTTP peers.
fn collect_live_peers_from_nodes(live_nodes: &BTreeMap<ChitchatId, NodeState>) -> Vec<Peer> {
    let mut peers: Vec<_> = live_nodes
        .iter()
        .map(|(peer_id, node_state)| Peer {
            id: peer_id.node_id.to_string(),
            addr: peer_http_addr(peer_id, node_state),
        })
        .collect();
    peers.sort_by(|left, right| left.id.cmp(&right.id));
    peers
}

/// Resolves a peer's HTTP forwarding address from gossip state.
///
/// Prefers the explicitly published `http-advertise-addr`; falls back to the
/// gossip advertise IP plus the published HTTP port for older peers.
fn peer_http_addr(peer_id: &ChitchatId, node_state: &NodeState) -> SocketAddr {
    if let Some(addr) = node_state
        .get(HTTP_ADVERTISE_ADDR_KEY)
        .and_then(|value| value.parse::<SocketAddr>().ok())
    {
        return addr;
    }
    let http_port = node_state
        .get(HTTP_PORT_KEY)
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(DEFAULT_HTTP_PORT);
    SocketAddr::new(peer_id.gossip_advertise_addr.ip(), http_port)
}
