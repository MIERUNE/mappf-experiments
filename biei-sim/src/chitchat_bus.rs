//! `GossipBus` backed by real `chitchat`, with a `SimTransport` injecting
//! `hop_latency` per send so paused-time simulation stays deterministic.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chitchat::transport::{
    ChannelTransport, Socket as ChitchatSocket, Transport as ChitchatTransport,
};
use chitchat::{
    ChitchatConfig, ChitchatHandle, ChitchatId, ChitchatMessage, FailureDetectorConfig,
    spawn_chitchat,
};
use tokio::time::Instant;

use biei::gossip::GossipBus;
use biei::types::{ClusterView, NodeId, NodeStateView};

/// `GossipBus` impl backed by N real `chitchat` instances (one per simulator
/// node) talking over a shared in-process transport with a configurable
/// `hop_latency` per send. Each call to `set` maps directly to chitchat's
/// native per-key `NodeState::set`, so unchanged values short-circuit and
/// only deltas propagate.
///
/// `view()` reads node 0's chitchat snapshot. All chitchat instances converge
/// to (approximately) the same snapshot in steady state; modelling per-node
/// view divergence under gossip lag is out of scope for now. A small cache
/// keyed by `(view_epoch, generated_at)` short-circuits repeat reads from
/// concurrent `handle_incoming` tasks landing on the same paused-time tick.
pub struct ChitchatGossipBus {
    members: Vec<NodeId>,
    handles: HashMap<NodeId, ChitchatHandle>,
    view_epoch: AtomicU64,
    view_cache: Mutex<Option<CachedView>>,
}

struct CachedView {
    epoch: u64,
    view: ClusterView,
}

impl ChitchatGossipBus {
    pub async fn new(
        members: Vec<NodeId>,
        gossip_interval: Duration,
        hop_latency: Duration,
    ) -> Result<Self> {
        let transport = SimTransport {
            inner: ChannelTransport::default(),
            hop_latency,
        };

        let addrs: HashMap<NodeId, SocketAddr> = members
            .iter()
            .enumerate()
            .map(|(i, nid)| {
                let port: u16 = 10_000 + i as u16;
                let addr: SocketAddr = ([127, 0, 0, 1], port).into();
                (nid.clone(), addr)
            })
            .collect();
        let all_addr_strings: Vec<String> = addrs.values().map(|a| a.to_string()).collect();

        let mut handles = HashMap::new();
        for nid in &members {
            let addr = addrs[nid];
            let chitchat_id = ChitchatId::new(nid.to_string(), 0, addr);
            let seed_nodes: Vec<String> = all_addr_strings
                .iter()
                .filter(|s| *s != &addr.to_string())
                .cloned()
                .collect();
            let config = ChitchatConfig {
                chitchat_id,
                cluster_id: "biei-sim".to_string(),
                gossip_interval,
                listen_addr: addr,
                seed_nodes,
                failure_detector_config: FailureDetectorConfig::default(),
                marked_for_deletion_grace_period: Duration::from_secs(3_600),
                catchup_callback: None,
                extra_liveness_predicate: None,
            };
            let handle = spawn_chitchat(config, vec![], &transport).await?;
            handles.insert(nid.clone(), handle);
        }

        Ok(Self {
            members,
            handles,
            view_epoch: AtomicU64::new(0),
            view_cache: Mutex::new(None),
        })
    }

    pub async fn shutdown(self) {
        for (_, h) in self.handles {
            let _ = h.shutdown().await;
        }
    }
}

#[async_trait]
impl GossipBus for ChitchatGossipBus {
    async fn set(&self, node_id: NodeId, key: String, value: String) {
        let Some(handle) = self.handles.get(&node_id) else {
            return;
        };
        handle
            .with_chitchat(|c| {
                c.self_node_state().set(&key, &value);
            })
            .await;
        self.view_epoch.fetch_add(1, Ordering::AcqRel);
        *self.view_cache.lock().expect("view cache poisoned") = None;
    }

    async fn view(&self) -> ClusterView {
        let now = Instant::now();
        let epoch = self.view_epoch.load(Ordering::Acquire);
        if let Some(cached) = self
            .view_cache
            .lock()
            .expect("view cache poisoned")
            .as_ref()
            && cached.epoch == epoch
            && cached.view.generated_at == now
        {
            return cached.view.clone();
        }

        // Always sample from `members[0]`'s chitchat snapshot — never
        // `handles.iter().next()`, which would be HashMap-order-dependent
        // and produce a non-reproducible view across runs.
        let Some(handle) = self.members.first().and_then(|nid| self.handles.get(nid)) else {
            return ClusterView {
                members: self.members.clone(),
                states: HashMap::new(),
                generated_at: now,
            };
        };
        let states = handle
            .with_chitchat(|c| {
                let mut out: HashMap<NodeId, NodeStateView> = HashMap::new();
                for (cid, ns) in c.node_states() {
                    let nid = NodeId::from(cid.node_id.as_ref());
                    // Decode straight from chitchat's `(&str, &str)` borrows
                    // — no intermediate `BTreeMap`, no per-key string clones.
                    out.insert(nid.clone(), NodeStateView::from_kvs(nid, ns.key_values()));
                }
                out
            })
            .await;
        let view = ClusterView {
            members: self.members.clone(),
            states,
            generated_at: now,
        };
        if self.view_epoch.load(Ordering::Acquire) == epoch {
            *self.view_cache.lock().expect("view cache poisoned") = Some(CachedView {
                epoch,
                view: view.clone(),
            });
        }
        view
    }
}

/// Adds `hop_latency` to every send. Wraps chitchat's in-memory
/// `ChannelTransport`.
#[derive(Clone)]
struct SimTransport {
    inner: ChannelTransport,
    hop_latency: Duration,
}

#[async_trait]
impl ChitchatTransport for SimTransport {
    async fn open(&self, listen_addr: SocketAddr) -> Result<Box<dyn ChitchatSocket>> {
        let inner = self.inner.open(listen_addr).await?;
        Ok(Box::new(SimSocket {
            inner,
            hop_latency: self.hop_latency,
        }))
    }
}

struct SimSocket {
    inner: Box<dyn ChitchatSocket>,
    hop_latency: Duration,
}

#[async_trait]
impl ChitchatSocket for SimSocket {
    async fn send(&mut self, to: SocketAddr, msg: ChitchatMessage) -> Result<()> {
        tokio::time::sleep(self.hop_latency).await;
        self.inner.send(to, msg).await
    }

    async fn recv(&mut self) -> Result<(SocketAddr, ChitchatMessage)> {
        self.inner.recv().await
    }
}
