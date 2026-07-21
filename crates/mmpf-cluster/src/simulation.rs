//! Deterministic in-process transport and node lifecycle for cluster simulations.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv4Addr, SocketAddr},
    ops::RangeInclusive,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use chitchat::{
    ChitchatMessage,
    transport::{ChannelTransport, Socket, Transport},
};
use mmpf_common::sync::lock_unpoisoned;
use tokio::{runtime::Handle, sync::Mutex as AsyncMutex, task::JoinHandle};

use crate::{Cluster, ClusterOwner, Config, GossipEndpoint};

mod churn;
pub use churn::{ChurnEvent, ChurnPlan};

const DEFAULT_FIRST_PORT: u16 = 10_000;

/// Message and byte counters from the shared in-memory gossip transport.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimulationTransportStats {
    pub messages_total: u64,
    pub bytes_total: u64,
}

/// Runs real Chitchat nodes over an in-process network with virtual hop delay.
///
/// The delay uses Tokio time, so simulations using a paused clock remain fully
/// deterministic. Clones share the same channel registry and statistics.
#[derive(Clone)]
pub struct SimulationTransport {
    inner: ChannelTransport,
    hop_latency: Duration,
}

impl SimulationTransport {
    pub fn new(hop_latency: Duration) -> Self {
        Self {
            inner: ChannelTransport::default(),
            hop_latency,
        }
    }

    pub fn statistics(&self) -> SimulationTransportStats {
        let statistics = self.inner.statistics();
        SimulationTransportStats {
            messages_total: statistics.num_messages_total,
            bytes_total: statistics.num_bytes_total,
        }
    }
}

#[async_trait]
impl Transport for SimulationTransport {
    async fn open(&self, listen_addr: SocketAddr) -> Result<Box<dyn Socket>> {
        let inner = self.inner.open(listen_addr).await?;
        Ok(Box::new(SimulationSocket {
            inner,
            hop_latency: self.hop_latency,
        }))
    }
}

struct SimulationSocket {
    inner: Box<dyn Socket>,
    hop_latency: Duration,
}

#[async_trait]
impl Socket for SimulationSocket {
    async fn send(&mut self, to: SocketAddr, message: ChitchatMessage) -> Result<()> {
        if !self.hop_latency.is_zero() {
            tokio::time::sleep(self.hop_latency).await;
        }
        self.inner.send(to, message).await
    }

    async fn recv(&mut self) -> Result<(SocketAddr, ChitchatMessage)> {
        self.inner.recv().await
    }
}

/// Network-owned values supplied to a service's simulated-node config builder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulatedNodeContext {
    pub node_id: String,
    pub gossip_endpoint: GossipEndpoint,
    pub seed_nodes: Vec<String>,
}

/// One spawned node's identity and cloneable local cluster perspective.
#[derive(Clone)]
pub struct SimulatedNode {
    node_id: String,
    gossip_endpoint: GossipEndpoint,
    generation_id: u64,
    cluster: Cluster,
}

impl SimulatedNode {
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub const fn gossip_endpoint(&self) -> GossipEndpoint {
        self.gossip_endpoint
    }

    pub const fn generation_id(&self) -> u64 {
        self.generation_id
    }

    pub fn cluster(&self) -> Cluster {
        self.cluster.clone()
    }
}

struct ActiveNode {
    endpoint: GossipEndpoint,
    owner: ClusterOwner,
}

struct NetworkState {
    active: BTreeMap<String, ActiveNode>,
    reserved_node_ids: BTreeSet<String>,
    allocated_ports: BTreeSet<u16>,
    next_generation: u64,
    #[cfg(test)]
    reservation_released: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct ReservationKey {
    node_id: String,
    endpoint: GossipEndpoint,
}

/// Releases a reserved logical ID and port unless activation commits them to
/// `NetworkState::active`. Owning the state keeps retirement leases valid after
/// their originating [`SimulatedNetwork`] or lifecycle future is dropped.
struct ReservationGuard {
    state: Arc<StdMutex<NetworkState>>,
    key: Option<ReservationKey>,
}

impl ReservationGuard {
    fn new(state: Arc<StdMutex<NetworkState>>, node_id: String, endpoint: GossipEndpoint) -> Self {
        Self {
            state,
            key: Some(ReservationKey { node_id, endpoint }),
        }
    }

    fn node_id(&self) -> &str {
        &self.key.as_ref().expect("reservation is armed").node_id
    }

    fn activate(mut self, generation_id: u64, owner: ClusterOwner) -> SimulatedNode {
        // Disarm before installing the active owner. This synchronous section
        // cannot be cancelled between the state transition and guard commit.
        let key = self.key.take().expect("reservation is armed");
        let cluster = owner.handle();
        let mut state = lock_unpoisoned(&self.state);
        let was_reserved = state.reserved_node_ids.remove(&key.node_id);
        debug_assert!(was_reserved, "activated node ID must be reserved");
        let previous = state.active.insert(
            key.node_id.clone(),
            ActiveNode {
                endpoint: key.endpoint,
                owner,
            },
        );
        debug_assert!(previous.is_none(), "reserved node cannot already be active");
        SimulatedNode {
            node_id: key.node_id,
            gossip_endpoint: key.endpoint,
            generation_id,
            cluster,
        }
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        release_reservation(&self.state, &key);
    }
}

/// An active node removed from routing while its owner shuts down.
///
/// This value is moved into a Tokio task so cancellation of the lifecycle caller
/// cannot drop either the owner or its reservation. The custom drop order is a
/// fallback for runtime shutdown: the owner is always dropped before the lease.
struct RetiringNode {
    reservation: ReservationGuard,
    node: Option<ActiveNode>,
}

impl RetiringNode {
    fn node_id(&self) -> &str {
        self.reservation.node_id()
    }

    fn owner(&self) -> &ClusterOwner {
        &self
            .node
            .as_ref()
            .expect("retiring node owns an actor")
            .owner
    }
}

impl Drop for RetiringNode {
    fn drop(&mut self) {
        // `ClusterOwner::drop` initiates shutdown. Do this before the
        // ReservationGuard field releases the address for later reuse.
        drop(self.node.take());
    }
}

/// Shared deterministic lifecycle for a set of simulated Chitchat nodes.
///
/// The network owns all shutdown authority. Returned [`SimulatedNode`] values
/// expose only cloneable local [`Cluster`] handles. Lifecycle operations are
/// serialized so an awaited removal releases its loopback address before a
/// later spawn can reuse it.
pub struct SimulatedNetwork {
    transport: SimulationTransport,
    ports: RangeInclusive<u16>,
    lifecycle: AsyncMutex<()>,
    state: Arc<StdMutex<NetworkState>>,
}

impl SimulatedNetwork {
    /// Creates a network allocating loopback endpoints from port 10,000 upward.
    pub fn new(hop_latency: Duration) -> Self {
        Self::with_port_range(hop_latency, DEFAULT_FIRST_PORT..=u16::MAX)
            .expect("the default simulated port range is valid")
    }

    /// Creates a network with an explicit inclusive port range.
    ///
    /// This is primarily useful for bounded simulations and exhaustion tests.
    pub fn with_port_range(hop_latency: Duration, ports: RangeInclusive<u16>) -> Result<Self> {
        ensure!(!ports.is_empty(), "simulated gossip port range is empty");
        ensure!(
            *ports.start() != 0,
            "simulated gossip port range cannot include port zero"
        );
        Ok(Self {
            transport: SimulationTransport::new(hop_latency),
            ports,
            lifecycle: AsyncMutex::new(()),
            state: Arc::new(StdMutex::new(NetworkState {
                active: BTreeMap::new(),
                reserved_node_ids: BTreeSet::new(),
                allocated_ports: BTreeSet::new(),
                next_generation: 1,
                #[cfg(test)]
                reservation_released: Arc::new(tokio::sync::Notify::new()),
            })),
        })
    }

    /// Spawns one node from a service-owned complete cluster configuration.
    ///
    /// The builder receives the deterministic endpoint and a snapshot of all
    /// active seed addresses. Its returned config must preserve those values and
    /// the requested logical node ID; every other setting remains domain-owned.
    pub async fn spawn<F>(
        &self,
        node_id: impl Into<String>,
        build_config: F,
    ) -> Result<SimulatedNode>
    where
        F: FnOnce(SimulatedNodeContext) -> Config,
    {
        let _lifecycle = self.lifecycle.lock().await;
        let (reservation, context, generation_id) = self.reserve_node(node_id.into())?;

        let config = build_config(context.clone());
        validate_config(&context, &config)?;

        let owner = ClusterOwner::spawn_simulated(config, generation_id, &self.transport)
            .await
            .context("failed to start simulated Chitchat node")?;
        Ok(reservation.activate(generation_id, owner))
    }

    /// Removes one active node, awaiting task termination before releasing its endpoint.
    ///
    /// Once retirement starts, owned cleanup continues even if the caller is
    /// cancelled. A missing Tokio runtime is reported before state is changed.
    pub async fn remove(&self, node_id: &str) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let runtime =
            Handle::try_current().context("simulated node removal requires a Tokio runtime")?;
        let cleanup = self.start_remove(&runtime, node_id)?;

        cleanup
            .await
            .context("simulated node retirement task did not complete")?
    }

    /// Shuts down every active node and releases every endpoint.
    ///
    /// Once retirement starts, owned cleanup continues even if the caller is
    /// cancelled. A missing Tokio runtime is reported before state is changed.
    pub async fn shutdown_all(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let runtime =
            Handle::try_current().context("simulated network shutdown requires a Tokio runtime")?;
        let cleanup = self.start_shutdown_all(&runtime);

        cleanup
            .await
            .context("simulated network retirement task did not complete")?
    }

    /// Returns a deterministic snapshot of currently active seed addresses.
    #[allow(
        clippy::unused_async,
        reason = "preserve the existing lazy async API while bookkeeping uses a synchronous lock"
    )]
    pub async fn seed_addresses(&self) -> Vec<String> {
        let state = lock_unpoisoned(&self.state);
        seed_addresses(&state)
    }

    pub fn statistics(&self) -> SimulationTransportStats {
        self.transport.statistics()
    }

    fn reserve_node(
        &self,
        node_id: String,
    ) -> Result<(ReservationGuard, SimulatedNodeContext, u64)> {
        let mut state = lock_unpoisoned(&self.state);
        ensure!(
            !state.active.contains_key(&node_id) && !state.reserved_node_ids.contains(&node_id),
            "simulated node ID {node_id} is already active"
        );
        let port = self
            .ports
            .clone()
            .find(|port| !state.allocated_ports.contains(port))
            .with_context(|| {
                format!(
                    "simulated gossip ports {}-{} are exhausted",
                    self.ports.start(),
                    self.ports.end()
                )
            })?;
        let generation_id = state.next_generation;
        state.next_generation = state
            .next_generation
            .checked_add(1)
            .context("simulated Chitchat generation IDs are exhausted")?;
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let gossip_endpoint = GossipEndpoint::clustered(addr, addr)
            .expect("a loopback advertise address is always routable");
        let seed_nodes = seed_addresses(&state);
        state.allocated_ports.insert(port);
        state.reserved_node_ids.insert(node_id.clone());
        drop(state);

        let reservation =
            ReservationGuard::new(Arc::clone(&self.state), node_id.clone(), gossip_endpoint);
        let context = SimulatedNodeContext {
            node_id,
            gossip_endpoint,
            seed_nodes,
        };
        Ok((reservation, context, generation_id))
    }

    fn start_remove(&self, runtime: &Handle, node_id: &str) -> Result<JoinHandle<Result<()>>> {
        let node = self.retire_node(node_id)?;
        Ok(runtime.spawn(shutdown_node(node)))
    }

    fn start_shutdown_all(&self, runtime: &Handle) -> JoinHandle<Result<()>> {
        let nodes = self.retire_all();
        runtime.spawn(shutdown_nodes(nodes))
    }

    fn retire_node(&self, node_id: &str) -> Result<RetiringNode> {
        let mut state = lock_unpoisoned(&self.state);
        let node = state
            .active
            .remove(node_id)
            .with_context(|| format!("active simulated node {node_id} does not exist"))?;
        state.reserved_node_ids.insert(node_id.to_string());
        drop(state);

        Ok(RetiringNode {
            reservation: ReservationGuard::new(
                Arc::clone(&self.state),
                node_id.to_string(),
                node.endpoint,
            ),
            node: Some(node),
        })
    }

    fn retire_all(&self) -> Vec<RetiringNode> {
        let mut state = lock_unpoisoned(&self.state);
        let nodes = std::mem::take(&mut state.active);
        state.reserved_node_ids.extend(nodes.keys().cloned());
        drop(state);

        nodes
            .into_iter()
            .map(|(node_id, node)| RetiringNode {
                reservation: ReservationGuard::new(Arc::clone(&self.state), node_id, node.endpoint),
                node: Some(node),
            })
            .collect()
    }
}

async fn shutdown_node(node: RetiringNode) -> Result<()> {
    let node_id = node.node_id().to_string();
    let shutdown = node
        .owner()
        .shutdown()
        .await
        .with_context(|| format!("failed to stop simulated node {node_id}"));
    drop(node);
    shutdown
}

async fn shutdown_nodes(nodes: Vec<RetiringNode>) -> Result<()> {
    let mut first_error = None;
    for node in nodes {
        let shutdown = shutdown_node(node).await;
        if first_error.is_none() {
            first_error = shutdown.err();
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn release_reservation(state: &StdMutex<NetworkState>, key: &ReservationKey) {
    let mut state = lock_unpoisoned(state);
    state.reserved_node_ids.remove(&key.node_id);
    state
        .allocated_ports
        .remove(&key.endpoint.listen_addr().port());
    #[cfg(test)]
    state.reservation_released.notify_one();
}

fn seed_addresses(state: &NetworkState) -> Vec<String> {
    let mut addresses: Vec<_> = state
        .active
        .values()
        .map(|node| node.endpoint.advertise_addr().to_string())
        .collect();
    addresses.sort_unstable();
    addresses
}

fn validate_config(context: &SimulatedNodeContext, config: &Config) -> Result<()> {
    ensure!(
        config.node_id == context.node_id,
        "simulated config node ID {} does not match allocated node ID {}",
        config.node_id,
        context.node_id
    );
    ensure!(
        config.gossip_endpoint == context.gossip_endpoint,
        "simulated config for {} did not use its allocated gossip endpoint",
        context.node_id
    );
    ensure!(
        config.seed_nodes == context.seed_nodes,
        "simulated config for {} did not use its seed-address snapshot",
        context.node_id
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{future::Future, future::poll_fn, pin::Pin, task::Poll};

    use super::*;
    use crate::FailureDetectorConfig;

    fn config(context: SimulatedNodeContext) -> Config {
        Config {
            cluster_id: "simulation-test".to_string(),
            node_id: context.node_id,
            gossip_endpoint: context.gossip_endpoint,
            seed_nodes: context.seed_nodes,
            gossip_interval: Duration::from_millis(20),
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(60),
            initial_key_values: Vec::new(),
        }
    }

    fn bookkeeping(network: &SimulatedNetwork) -> (usize, usize, usize) {
        let state = lock_unpoisoned(&network.state);
        (
            state.active.len(),
            state.reserved_node_ids.len(),
            state.allocated_ports.len(),
        )
    }

    fn reservation_release(network: &SimulatedNetwork) -> Arc<tokio::sync::Notify> {
        Arc::clone(&lock_unpoisoned(&network.state).reservation_released)
    }

    #[test]
    fn new_transport_has_zero_statistics() {
        assert_eq!(
            SimulationTransport::new(Duration::from_millis(5)).statistics(),
            SimulationTransportStats::default()
        );
    }

    #[test]
    fn dropped_spawn_reservation_releases_id_and_port() {
        let network =
            SimulatedNetwork::with_port_range(Duration::ZERO, 12_003..=12_003).expect("network");
        let (reservation, first_context, _) = network
            .reserve_node("node-a".to_string())
            .expect("reserve node");
        assert_eq!(bookkeeping(&network), (0, 1, 1));

        // Cancelling spawn after allocation drops this local guard.
        drop(reservation);
        assert_eq!(bookkeeping(&network), (0, 0, 0));

        let (reused, second_context, _) = network
            .reserve_node("node-a".to_string())
            .expect("reuse reservation");
        assert_eq!(
            second_context.gossip_endpoint,
            first_context.gossip_endpoint
        );
        drop(reused);
    }

    async fn poll_once_pending<F: Future>(mut future: Pin<&mut F>) {
        poll_fn(|cx| {
            assert!(
                future.as_mut().poll(cx).is_pending(),
                "lifecycle call must wait for its owned cleanup task"
            );
            Poll::Ready(())
        })
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_remove_keeps_single_endpoint_reserved_until_termination() {
        let network =
            SimulatedNetwork::with_port_range(Duration::ZERO, 12_004..=12_004).expect("network");
        let first = network.spawn("node-a", config).await.expect("spawn node");
        let first_endpoint = first.gossip_endpoint();
        let first_generation = first.generation_id();

        let released = reservation_release(&network);
        let mut removal = Box::pin(network.remove("node-a"));
        poll_once_pending(removal.as_mut()).await;
        drop(removal);

        assert_eq!(bookkeeping(&network), (0, 1, 1));
        let exhausted = network
            .reserve_node("node-b".to_string())
            .err()
            .expect("the retiring endpoint must remain reserved");
        assert!(
            exhausted
                .to_string()
                .contains("ports 12004-12004 are exhausted")
        );

        released.notified().await;
        assert_eq!(bookkeeping(&network), (0, 0, 0));

        let restarted = network
            .spawn("node-b", config)
            .await
            .expect("reuse endpoint");
        assert_eq!(restarted.gossip_endpoint(), first_endpoint);
        assert!(restarted.generation_id() > first_generation);
        network.shutdown_all().await.expect("shutdown network");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_shutdown_all_keeps_every_endpoint_reserved_until_termination() {
        let network =
            SimulatedNetwork::with_port_range(Duration::ZERO, 12_005..=12_006).expect("network");
        let first = network.spawn("node-a", config).await.expect("spawn node-a");
        let second = network.spawn("node-b", config).await.expect("spawn node-b");
        let mut original_endpoints = [first.gossip_endpoint(), second.gossip_endpoint()];
        original_endpoints.sort_unstable_by_key(|endpoint| endpoint.listen_addr());

        let released = reservation_release(&network);
        let mut shutdown = Box::pin(network.shutdown_all());
        poll_once_pending(shutdown.as_mut()).await;
        drop(shutdown);

        assert_eq!(bookkeeping(&network), (0, 2, 2));
        let exhausted = network
            .reserve_node("node-c".to_string())
            .err()
            .expect("every retiring endpoint must remain reserved");
        assert!(
            exhausted
                .to_string()
                .contains("ports 12005-12006 are exhausted")
        );

        while bookkeeping(&network) != (0, 0, 0) {
            released.notified().await;
        }

        let third = network
            .spawn("node-c", config)
            .await
            .expect("reuse first endpoint");
        let fourth = network
            .spawn("node-d", config)
            .await
            .expect("reuse second endpoint");
        let mut reused_endpoints = [third.gossip_endpoint(), fourth.gossip_endpoint()];
        reused_endpoints.sort_unstable_by_key(|endpoint| endpoint.listen_addr());
        assert_eq!(reused_endpoints, original_endpoints);
        network.shutdown_all().await.expect("shutdown network");
    }

    #[tokio::test]
    async fn rejects_duplicate_active_ids_and_exhausted_ports() {
        let network =
            SimulatedNetwork::with_port_range(Duration::ZERO, 12_001..=12_001).expect("network");
        let first = network.spawn("node-a", config).await.expect("first node");

        let duplicate = network
            .spawn("node-a", config)
            .await
            .err()
            .expect("duplicate ID must fail");
        assert!(duplicate.to_string().contains("already active"));
        let exhausted = network
            .spawn("node-b", config)
            .await
            .err()
            .expect("exhausted range must fail");
        assert!(
            exhausted
                .to_string()
                .contains("ports 12001-12001 are exhausted")
        );

        assert_eq!(first.generation_id(), 1);
        network.shutdown_all().await.expect("shutdown network");
    }

    #[tokio::test]
    async fn awaited_removal_reuses_endpoint_and_advances_generation() {
        let network =
            SimulatedNetwork::with_port_range(Duration::ZERO, 12_002..=12_002).expect("network");
        let first = network.spawn("node-a", config).await.expect("first node");
        let first_endpoint = first.gossip_endpoint();
        let first_generation = first.generation_id();

        network.remove("node-a").await.expect("remove first node");
        let restarted = network.spawn("node-a", config).await.expect("restart node");

        assert_eq!(restarted.gossip_endpoint(), first_endpoint);
        assert!(restarted.generation_id() > first_generation);
        network.shutdown_all().await.expect("shutdown network");
    }
}
