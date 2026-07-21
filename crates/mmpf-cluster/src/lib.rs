//! One-node Chitchat lifecycle and borrowed cluster-state inspection.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use chitchat::{
    Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, NodeState, spawn_chitchat,
    transport::UdpTransport,
};

mod bootstrap;
pub use bootstrap::{
    BootstrapReadinessGate, BootstrapReadinessObservation, BootstrapReadinessState,
    BootstrapReadinessTransition, DEFAULT_BOOTSTRAP_GRACE,
};

#[cfg(feature = "simulation")]
pub mod simulation;
#[cfg(feature = "simulation")]
pub use simulation::{
    SimulatedNetwork, SimulatedNode, SimulatedNodeContext, SimulationTransport,
    SimulationTransportStats,
};

/// Canonical membership key used to remove draining nodes from live routing.
const DRAINING_KEY: &str = "draining";

/// Gossip listener and the address published to other nodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GossipEndpoint {
    listen_addr: SocketAddr,
    advertise_addr: SocketAddr,
}

impl GossipEndpoint {
    /// Builds an endpoint for standalone, local, or test use.
    ///
    /// This intentionally permits wildcard IPs and port zero.
    pub const fn standalone(listen_addr: SocketAddr, advertise_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            advertise_addr,
        }
    }

    /// Builds a clustered endpoint whose advertised IP can be reached by peers.
    pub fn clustered(
        listen_addr: SocketAddr,
        advertise_addr: SocketAddr,
    ) -> Result<Self, GossipEndpointError> {
        if advertise_addr.ip().is_unspecified() {
            return Err(GossipEndpointError { advertise_addr });
        }
        Ok(Self {
            listen_addr,
            advertise_addr,
        })
    }

    pub const fn listen_addr(self) -> SocketAddr {
        self.listen_addr
    }

    pub const fn advertise_addr(self) -> SocketAddr {
        self.advertise_addr
    }
}

/// Error returned when a clustered gossip endpoint would advertise a wildcard IP.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GossipEndpointError {
    advertise_addr: SocketAddr,
}

impl GossipEndpointError {
    pub const fn advertise_addr(self) -> SocketAddr {
        self.advertise_addr
    }
}

impl fmt::Display for GossipEndpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "gossip advertise address {} is a wildcard",
            self.advertise_addr
        )
    }
}

impl Error for GossipEndpointError {}

/// Failure detector tuning for one cluster node.
#[derive(Clone, Debug)]
pub struct FailureDetectorConfig {
    /// Phi threshold above which a node is considered faulty.
    pub phi_threshold: f64,
    /// Number of heartbeat intervals retained for sampling.
    pub sampling_window_size: usize,
    /// Heartbeat intervals longer than this are discarded.
    pub max_interval: Duration,
    /// Startup interval used before heartbeat history exists.
    pub initial_interval: Duration,
    /// Time a dead node remains in cluster state before removal.
    pub dead_node_grace_period: Duration,
}

impl FailureDetectorConfig {
    pub const fn new(
        phi_threshold: f64,
        sampling_window_size: usize,
        max_interval: Duration,
        initial_interval: Duration,
        dead_node_grace_period: Duration,
    ) -> Self {
        Self {
            phi_threshold,
            sampling_window_size,
            max_interval,
            initial_interval,
            dead_node_grace_period,
        }
    }

    fn into_chitchat(self) -> chitchat::FailureDetectorConfig {
        chitchat::FailureDetectorConfig::new(
            self.phi_threshold,
            self.sampling_window_size,
            self.max_interval,
            self.initial_interval,
            self.dead_node_grace_period,
        )
    }
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            phi_threshold: 8.0,
            sampling_window_size: 1_000,
            max_interval: Duration::from_secs(10),
            initial_interval: Duration::from_secs(5),
            dead_node_grace_period: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// Configuration for one Chitchat node.
pub struct Config {
    pub cluster_id: String,
    pub node_id: String,
    pub gossip_endpoint: GossipEndpoint,
    pub seed_nodes: Vec<String>,
    pub gossip_interval: Duration,
    pub failure_detector_config: FailureDetectorConfig,
    pub marked_for_deletion_grace_period: Duration,
    pub initial_key_values: Vec<(String, String)>,
}

impl Config {
    /// Creates a cluster node with the standard failure detector and no
    /// service-specific metadata.
    pub fn new(
        cluster_id: impl Into<String>,
        node_id: impl Into<String>,
        gossip_endpoint: GossipEndpoint,
        seed_nodes: Vec<String>,
        gossip_interval: Duration,
        marked_for_deletion_grace_period: Duration,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            node_id: node_id.into(),
            gossip_endpoint,
            seed_nodes,
            gossip_interval,
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period,
            initial_key_values: Vec::new(),
        }
    }

    /// Overrides only the dead-node retention policy while preserving the
    /// standard detector tuning.
    pub fn with_dead_node_grace_period(mut self, grace: Duration) -> Self {
        self.failure_detector_config.dead_node_grace_period = grace;
        self
    }

    /// Adds the service-specific metadata published with the initial node state.
    pub fn with_initial_key_values(mut self, values: Vec<(String, String)>) -> Self {
        self.initial_key_values = values;
        self
    }
}

/// Non-cloneable owner of one running Chitchat node.
///
/// Dropping the owner initiates shutdown, while [`Self::shutdown`] additionally
/// waits for the background task to terminate and release its socket. Cloneable
/// [`Cluster`] handles deliberately carry no shutdown authority.
pub struct ClusterOwner {
    handle: ChitchatHandle,
    shutdown_initiated: AtomicBool,
}

impl ClusterOwner {
    /// Starts a production node over UDP with a restart-safe generation ID.
    pub async fn spawn(config: Config) -> Result<Self> {
        let generation_id = generation_id_at(SystemTime::now())?;
        Self::spawn_inner(config, generation_id, &UdpTransport).await
    }

    /// Starts a node over the shared in-process transport with an explicit
    /// generation ID for deterministic simulation.
    #[cfg(feature = "simulation")]
    pub async fn spawn_simulated(
        config: Config,
        generation_id: u64,
        transport: &SimulationTransport,
    ) -> Result<Self> {
        Self::spawn_inner(config, generation_id, transport).await
    }

    async fn spawn_inner(
        config: Config,
        generation_id: u64,
        transport: &dyn chitchat::transport::Transport,
    ) -> Result<Self> {
        let mut initial_key_values = config.initial_key_values;
        initial_key_values.retain(|(key, _)| key != DRAINING_KEY);
        initial_key_values.push((DRAINING_KEY.to_string(), "false".to_string()));

        let chitchat_config = ChitchatConfig {
            chitchat_id: ChitchatId::new(
                config.node_id,
                generation_id,
                config.gossip_endpoint.advertise_addr(),
            ),
            cluster_id: config.cluster_id,
            gossip_interval: config.gossip_interval,
            listen_addr: config.gossip_endpoint.listen_addr(),
            seed_nodes: config.seed_nodes,
            failure_detector_config: config.failure_detector_config.into_chitchat(),
            marked_for_deletion_grace_period: config.marked_for_deletion_grace_period,
            catchup_callback: None,
            extra_liveness_predicate: Some(Box::new(is_not_draining)),
        };
        let handle = spawn_chitchat(chitchat_config, initial_key_values, transport).await?;
        Ok(Self {
            handle,
            shutdown_initiated: AtomicBool::new(false),
        })
    }

    /// Returns a cloneable operational handle for this node.
    pub fn handle(&self) -> Cluster {
        Cluster {
            chitchat: self.handle.chitchat(),
        }
    }

    /// Initiates shutdown once and waits for the background task and socket to terminate.
    ///
    /// Calls are idempotent and may wait concurrently. Chitchat's termination
    /// result is authoritative when its command channel has already closed.
    pub async fn shutdown(&self) -> Result<()> {
        self.initiate_shutdown_once();
        self.handle.termination_watcher().await
    }

    fn initiate_shutdown_once(&self) {
        if self
            .shutdown_initiated
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let _ = self.handle.initiate_shutdown();
        }
    }
}

impl Drop for ClusterOwner {
    fn drop(&mut self) {
        self.initiate_shutdown_once();
    }
}

/// Cloneable operational handle to one Chitchat node.
///
/// Keeping handles alive does not keep the node running after its owner is
/// dropped, and handles cannot initiate shutdown.
#[derive(Clone)]
pub struct Cluster {
    chitchat: Arc<tokio::sync::Mutex<Chitchat>>,
}

impl Cluster {
    /// Updates one self-node metadata value.
    pub async fn set(&self, key: &str, value: &str) {
        self.with_chitchat(|chitchat| chitchat.self_node_state().set(key, value))
            .await;
    }

    /// Updates multiple self-node metadata values under one Chitchat lock.
    pub async fn set_many<'a>(&self, values: impl IntoIterator<Item = (&'a str, &'a str)>) {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            return;
        }
        self.with_chitchat(|chitchat| {
            let state = chitchat.self_node_state();
            for &(key, value) in &values {
                state.set(key, value);
            }
        })
        .await;
    }

    /// Marks the local node as draining or active.
    pub async fn set_draining(&self, draining: bool) {
        self.set(DRAINING_KEY, if draining { "true" } else { "false" })
            .await;
    }

    /// Inspects current cluster state without cloning the node-state map.
    pub async fn inspect<R, F>(&self, mut inspect: F) -> R
    where
        F: for<'a> FnMut(StateRef<'a>) -> R,
    {
        self.with_chitchat(|chitchat| inspect(StateRef { chitchat }))
            .await
    }

    /// Returns whether another non-draining live node is present.
    ///
    /// This reads raw Chitchat membership and does not project service-specific
    /// metadata or populate any routing cache.
    pub async fn has_other_live_node(&self, self_node_id: &str) -> bool {
        self.inspect(|state| state.live_nodes().any(|node| node.id() != self_node_id))
            .await
    }

    /// Subscribes to live-node changes for low-frequency diagnostics.
    pub async fn live_nodes_watcher(&self) -> LiveNodesWatcher {
        let receiver = self
            .with_chitchat(|chitchat| chitchat.live_nodes_watcher())
            .await;
        LiveNodesWatcher { receiver }
    }

    async fn with_chitchat<R>(&self, mut operation: impl FnMut(&mut Chitchat) -> R) -> R {
        let mut chitchat = self.chitchat.lock().await;
        operation(&mut chitchat)
    }
}

/// Borrowed view of current Chitchat state.
#[derive(Clone, Copy)]
pub struct StateRef<'a> {
    chitchat: &'a Chitchat,
}

impl StateRef<'_> {
    pub fn cluster_id(&self) -> &str {
        self.chitchat.cluster_id()
    }

    pub fn nodes(&self) -> impl Iterator<Item = NodeRef<'_>> + '_ {
        self.chitchat
            .node_states()
            .iter()
            .map(|(id, state)| NodeRef { id, state })
    }

    /// Returns nodes currently considered live by Chitchat's failure detector
    /// and the configured non-draining liveness predicate.
    pub fn live_nodes(&self) -> impl Iterator<Item = NodeRef<'_>> + '_ {
        self.chitchat.live_nodes().filter_map(|id| {
            self.chitchat
                .node_state(id)
                .filter(|state| is_not_draining(state))
                .map(|state| NodeRef { id, state })
        })
    }

    pub fn dead_node_ids(&self) -> impl Iterator<Item = &str> + '_ {
        self.chitchat.dead_nodes().map(|id| id.node_id.as_ref())
    }
}

/// Borrowed node identity and key-value state.
#[derive(Clone, Copy)]
pub struct NodeRef<'a> {
    id: &'a ChitchatId,
    state: &'a NodeState,
}

impl<'a> NodeRef<'a> {
    pub fn id(&self) -> &'a str {
        self.id.node_id.as_ref()
    }

    pub fn get(&self, key: &str) -> Option<&'a str> {
        self.state.get(key)
    }

    pub fn key_values(&self) -> impl Iterator<Item = (&str, &str)> {
        self.state.key_values()
    }
}

/// Receiver for live-node changes with Chitchat types kept private.
pub struct LiveNodesWatcher {
    receiver: tokio::sync::watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
}

impl LiveNodesWatcher {
    /// Inspects the current watched state without cloning node ids or their
    /// service-specific key/value maps. The borrowed view cannot escape the
    /// callback, so the watch receiver remains free to advance afterward.
    pub fn inspect<R, F>(&self, mut inspect: F) -> R
    where
        F: for<'a> FnMut(LiveNodesRef<'a>) -> R,
    {
        let state = self.receiver.borrow();
        inspect(LiveNodesRef { nodes: &state })
    }

    pub async fn changed(&mut self) -> Result<(), tokio::sync::watch::error::RecvError> {
        self.receiver.changed().await
    }
}

/// Callback-scoped borrowed state from [`LiveNodesWatcher`].
#[derive(Clone, Copy)]
pub struct LiveNodesRef<'a> {
    nodes: &'a BTreeMap<ChitchatId, NodeState>,
}

impl LiveNodesRef<'_> {
    pub fn nodes(&self) -> impl Iterator<Item = NodeRef<'_>> + '_ {
        self.nodes
            .iter()
            .filter(|(_, state)| is_not_draining(state))
            .map(|(id, state)| NodeRef { id, state })
    }
}

fn generation_id_at(now: SystemTime) -> Result<u64> {
    let milliseconds = now
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_millis();
    u64::try_from(milliseconds).context("unix epoch milliseconds exceed u64")
}

fn is_not_draining(state: &NodeState) -> bool {
    state.get(DRAINING_KEY) == Some("false")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_endpoints_allow_wildcards_and_port_zero() {
        let endpoint =
            GossipEndpoint::standalone("0.0.0.0:0".parse().unwrap(), "0.0.0.0:0".parse().unwrap());

        assert_eq!(endpoint.listen_addr(), "0.0.0.0:0".parse().unwrap());
        assert_eq!(endpoint.advertise_addr(), "0.0.0.0:0".parse().unwrap());
    }

    #[test]
    fn clustered_endpoints_reject_wildcard_advertise_ips() {
        let error = GossipEndpoint::clustered(
            "0.0.0.0:7946".parse().unwrap(),
            "[::]:7946".parse().unwrap(),
        )
        .expect_err("wildcard advertise IP must be rejected");

        assert_eq!(error.advertise_addr(), "[::]:7946".parse().unwrap());
        assert!(
            GossipEndpoint::clustered(
                "0.0.0.0:7946".parse().unwrap(),
                "127.0.0.1:7946".parse().unwrap(),
            )
            .is_ok()
        );
    }

    #[test]
    fn failure_detector_defaults_match_chitchat_0_11_1() {
        let config = FailureDetectorConfig::default();
        assert_eq!(config.phi_threshold, 8.0);
        assert_eq!(config.sampling_window_size, 1_000);
        assert_eq!(config.max_interval, Duration::from_secs(10));
        assert_eq!(config.initial_interval, Duration::from_secs(5));
        assert_eq!(
            config.dead_node_grace_period,
            Duration::from_secs(24 * 60 * 60)
        );
    }

    #[test]
    fn generation_id_uses_unix_epoch_milliseconds() {
        assert_eq!(
            generation_id_at(UNIX_EPOCH + Duration::from_millis(42)).unwrap(),
            42
        );
        assert!(generation_id_at(UNIX_EPOCH - Duration::from_millis(1)).is_err());
    }

    #[test]
    fn draining_predicate_requires_explicit_false() {
        let mut state = NodeState::for_test();
        assert!(!is_not_draining(&state));
        state.set(DRAINING_KEY, "false");
        assert!(is_not_draining(&state));
        state.set(DRAINING_KEY, "true");
        assert!(!is_not_draining(&state));
        state.set(DRAINING_KEY, "malformed");
        assert!(!is_not_draining(&state));
    }

    fn test_config(node_id: &str, addr: SocketAddr) -> Config {
        Config {
            cluster_id: "test-cluster".to_string(),
            node_id: node_id.to_string(),
            gossip_endpoint: GossipEndpoint::standalone(addr, addr),
            seed_nodes: Vec::new(),
            gossip_interval: Duration::from_millis(20),
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(60),
            initial_key_values: vec![("http-port".to_string(), "8080".to_string())],
        }
    }

    #[tokio::test]
    async fn starts_updates_inspects_and_shuts_down_one_node() {
        let owner = ClusterOwner::spawn(test_config("node-a", "127.0.0.1:0".parse().unwrap()))
            .await
            .unwrap();
        let cluster = owner.handle();

        let values = [
            ("http-port".to_string(), "8081".to_string()),
            ("capacity".to_string(), "4".to_string()),
            ("http-port".to_string(), "9090".to_string()),
        ];
        let update = cluster.set_many(
            values
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&update);
        update.await;
        let snapshot = cluster
            .inspect(|state| {
                let node = state.live_nodes().next().unwrap();
                (
                    state.cluster_id().to_string(),
                    node.id().to_string(),
                    node.get("http-port").map(str::to_string),
                    node.get("capacity").map(str::to_string),
                    node.get(DRAINING_KEY).map(str::to_string),
                )
            })
            .await;
        assert_eq!(snapshot.0, "test-cluster");
        assert_eq!(snapshot.1, "node-a");
        assert_eq!(snapshot.2.as_deref(), Some("9090"));
        assert_eq!(snapshot.3.as_deref(), Some("4"));
        assert_eq!(snapshot.4.as_deref(), Some("false"));
        assert!(!cluster.has_other_live_node("node-a").await);
        assert!(cluster.has_other_live_node("not-node-a").await);

        cluster.set_draining(true).await;
        let (live_count, diagnostic_draining) = cluster
            .inspect(|state| {
                (
                    state.live_nodes().count(),
                    state
                        .nodes()
                        .find(|node| node.id() == "node-a")
                        .and_then(|node| node.get(DRAINING_KEY).map(str::to_string)),
                )
            })
            .await;
        assert_eq!(live_count, 0);
        assert_eq!(diagnostic_draining.as_deref(), Some("true"));

        owner.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn owner_drop_initiates_shutdown() {
        let owner = ClusterOwner::spawn(test_config("drop-node", "127.0.0.1:0".parse().unwrap()))
            .await
            .unwrap();
        let termination = owner.handle.termination_watcher();

        drop(owner);

        tokio::time::timeout(Duration::from_secs(1), termination)
            .await
            .expect("dropped owner should initiate shutdown")
            .expect("chitchat should terminate cleanly");
    }

    #[tokio::test]
    async fn awaited_shutdown_supports_concurrent_and_repeated_waits() {
        let owner = ClusterOwner::spawn(test_config(
            "concurrent-node",
            "127.0.0.1:0".parse().unwrap(),
        ))
        .await
        .unwrap();

        let (first, second) = tokio::join!(owner.shutdown(), owner.shutdown());
        first.unwrap();
        second.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[cfg(feature = "simulation")]
    #[tokio::test]
    async fn awaited_simulated_shutdown_allows_immediate_address_reuse() {
        let transport = SimulationTransport::new(Duration::ZERO);
        let addr = "127.0.0.1:12001".parse().unwrap();
        let first = ClusterOwner::spawn_simulated(test_config("first", addr), 1, &transport)
            .await
            .unwrap();

        first.shutdown().await.unwrap();

        let second = ClusterOwner::spawn_simulated(test_config("second", addr), 2, &transport)
            .await
            .expect("awaited shutdown should release the simulated address");
        second.shutdown().await.unwrap();
    }
}
