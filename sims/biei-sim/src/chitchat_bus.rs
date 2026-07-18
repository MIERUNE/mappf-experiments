//! `GossipBus` backed by real `chitchat`, with a `SimTransport` injecting
//! `hop_latency` per send so paused-time simulation stays deterministic.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
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
use tokio::sync::RwLock;
use tokio::time::Instant;

use biei_core::gossip::GossipBus;
use biei_core::types::{ClusterView, NodeId, NodeKvs, NodeStateView};

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
    members: RwLock<Vec<NodeId>>,
    handles: RwLock<HashMap<NodeId, ChitchatHandle>>,
    addrs: RwLock<HashMap<NodeId, SocketAddr>>,
    transport: SimTransport,
    gossip_interval: Duration,
    next_port: AtomicU32,
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
        let bus = Self {
            members: RwLock::new(Vec::new()),
            handles: RwLock::new(HashMap::new()),
            addrs: RwLock::new(HashMap::new()),
            transport: SimTransport {
                inner: ChannelTransport::default(),
                hop_latency,
            },
            gossip_interval,
            next_port: AtomicU32::new(10_000),
            view_epoch: AtomicU64::new(0),
            view_cache: Mutex::new(None),
        };
        for member in members {
            if let Err(error) = bus.add_node(member).await {
                bus.shutdown_all().await;
                return Err(error);
            }
        }
        Ok(bus)
    }

    pub async fn add_node(&self, node_id: NodeId) -> Result<()> {
        if self.handles.read().await.contains_key(&node_id) {
            anyhow::bail!("duplicate simulator node {node_id}");
        }
        let port: u16 = self
            .next_port
            .fetch_add(1, Ordering::Relaxed)
            .try_into()
            .map_err(|_| anyhow::anyhow!("simulator exhausted chitchat listen ports"))?;
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();
        let seed_nodes = self
            .addrs
            .read()
            .await
            .values()
            .map(ToString::to_string)
            .collect();
        let config = ChitchatConfig {
            chitchat_id: ChitchatId::new(node_id.to_string(), 0, addr),
            cluster_id: "biei-sim".to_string(),
            gossip_interval: self.gossip_interval,
            listen_addr: addr,
            seed_nodes,
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(3_600),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };
        let handle = spawn_chitchat(config, vec![], &self.transport).await?;
        // Mutations take locks in the same members -> handles -> addrs order
        // used by view(). Holding the members write lock makes the three-map
        // update appear atomic to readers.
        let mut members = self.members.write().await;
        let mut handles = self.handles.write().await;
        let mut addrs = self.addrs.write().await;
        handles.insert(node_id.clone(), handle);
        addrs.insert(node_id.clone(), addr);
        members.push(node_id);
        drop(addrs);
        drop(handles);
        drop(members);
        self.invalidate_view();
        Ok(())
    }

    pub async fn remove_node(&self, node_id: &NodeId) -> Result<()> {
        let mut members = self.members.write().await;
        let mut handles = self.handles.write().await;
        let mut addrs = self.addrs.write().await;
        let handle = handles.remove(node_id);
        addrs.remove(node_id);
        members.retain(|member| member != node_id);
        drop(addrs);
        drop(handles);
        drop(members);
        self.invalidate_view();
        if let Some(handle) = handle {
            handle.shutdown().await?;
        }
        Ok(())
    }

    pub async fn shutdown_all(&self) {
        let mut members = self.members.write().await;
        let mut handles_guard = self.handles.write().await;
        let mut addrs = self.addrs.write().await;
        let handles = std::mem::take(&mut *handles_guard);
        members.clear();
        addrs.clear();
        drop(addrs);
        drop(handles_guard);
        drop(members);
        self.invalidate_view();
        for (_, h) in handles {
            let _ = h.shutdown().await;
        }
    }

    fn invalidate_view(&self) {
        self.view_epoch.fetch_add(1, Ordering::AcqRel);
        *self.view_cache.lock().expect("view cache poisoned") = None;
    }
}

#[async_trait]
impl GossipBus for ChitchatGossipBus {
    async fn set(&self, node_id: NodeId, key: String, value: String) {
        let handles = self.handles.read().await;
        let Some(handle) = handles.get(&node_id) else {
            return;
        };
        handle
            .with_chitchat(|c| {
                c.self_node_state().set(&key, &value);
            })
            .await;
        drop(handles);
        self.invalidate_view();
    }

    async fn set_many(&self, node_id: NodeId, kvs: NodeKvs) {
        let handles = self.handles.read().await;
        let Some(handle) = handles.get(&node_id) else {
            return;
        };
        handle
            .with_chitchat(|c| {
                let state = c.self_node_state();
                for (key, value) in &kvs {
                    state.set(key, value);
                }
            })
            .await;
        drop(handles);
        self.invalidate_view();
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

        let members_guard = self.members.read().await;
        let handles = self.handles.read().await;
        // Always sample from `members[0]`'s chitchat snapshot — never
        // `handles.iter().next()`, which would be HashMap-order-dependent
        // and produce a non-reproducible view across runs.
        let Some(handle) = members_guard.first().and_then(|nid| handles.get(nid)) else {
            return ClusterView {
                members: members_guard.clone(),
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
            members: members_guard.clone(),
            states,
            generated_at: now,
        };
        drop(handles);
        drop(members_guard);
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
