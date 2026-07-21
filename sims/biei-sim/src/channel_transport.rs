//! In-process `InternalTransport` impl: `NodeRegistry` (Weak refs) + per-hop sleep,
//! calling the target node's `handle_forwarded` directly.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;

use biei_core::internal_transport::{ForwardError, InternalTransport};
use biei_core::node::Node;
use biei_core::types::NodeId;
use biei_core::wire::{ForwardRequest, ForwardResponse};

/// Shared registry mapping `NodeId` to live `Node` handles. Held weakly to
/// avoid keeping nodes alive past the harness's own ownership, which lets
/// shutdown drop nodes cleanly.
#[derive(Default)]
pub struct NodeRegistry {
    nodes: RwLock<HashMap<NodeId, Weak<NodeEntry>>>,
}

pub struct NodeEntry {
    node: Node,
}

impl NodeRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a node. Returns the `Arc<NodeEntry>` the caller must hold to
    /// keep the entry alive for the registry.
    pub fn register(&self, id: NodeId, node: Node) -> Arc<NodeEntry> {
        let entry = Arc::new(NodeEntry { node });
        self.nodes
            .write()
            .expect("registry poisoned")
            .insert(id, Arc::downgrade(&entry));
        entry
    }

    pub fn get(&self, id: &NodeId) -> Option<Node> {
        self.nodes
            .read()
            .expect("registry poisoned")
            .get(id)
            .and_then(|w| w.upgrade())
            .map(|e| e.node.clone())
    }

    pub fn unregister(&self, id: &NodeId) {
        self.nodes.write().expect("registry poisoned").remove(id);
    }
}

/// In-process transport that simulates per-hop latency by sleeping
/// `hop_latency` before delegating to the target node's
/// [`Node::handle_forwarded`].
pub struct ChannelTransport {
    hop_latency: Duration,
    registry: Arc<NodeRegistry>,
    attempts: AtomicU64,
    successes: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TransportSnapshot {
    pub attempts: u64,
    pub successes: u64,
}

impl ChannelTransport {
    pub fn new(hop_latency: Duration, registry: Arc<NodeRegistry>) -> Self {
        Self {
            hop_latency,
            registry,
            attempts: AtomicU64::new(0),
            successes: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> TransportSnapshot {
        TransportSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl InternalTransport for ChannelTransport {
    async fn send(
        &self,
        target: NodeId,
        fwd: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        let response_deadline = tokio::time::Instant::now()
            + Duration::from_millis(fwd.origin_response_budget_ms as u64);
        wait_for_hop(self.hop_latency, response_deadline).await?;
        let style_id = fwd.task.style.id.clone();
        let node = self
            .registry
            .get(&target)
            .ok_or_else(|| ForwardError::Retryable(format!("unknown node {target}")))?;
        let outcome = node.handle_forwarded(fwd).await;
        wait_for_hop(self.hop_latency, response_deadline).await?;
        self.successes.fetch_add(1, Ordering::Relaxed);
        Ok(ForwardResponse::from_task_outcome(outcome, style_id))
    }
}

async fn wait_for_hop(
    hop_latency: Duration,
    response_deadline: tokio::time::Instant,
) -> Result<(), ForwardError> {
    if !hop_latency.is_zero() {
        tokio::time::sleep(hop_latency).await;
    }
    if tokio::time::Instant::now() >= response_deadline {
        Err(ForwardError::Retryable("deadline_exceeded".to_string()))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn simulated_round_trip_charges_both_network_hops() {
        let hop = Duration::from_millis(40);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let started = tokio::time::Instant::now();

        wait_for_hop(hop, deadline).await.unwrap();
        wait_for_hop(hop, deadline).await.unwrap();

        assert_eq!(started.elapsed(), hop * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_duration_hop_still_checks_the_origin_response_deadline() {
        let now = tokio::time::Instant::now();

        wait_for_hop(Duration::ZERO, now + Duration::from_secs(1))
            .await
            .expect("future deadline");
        assert_eq!(tokio::time::Instant::now(), now);
        assert!(matches!(
            wait_for_hop(Duration::ZERO, now).await,
            Err(ForwardError::Retryable(message)) if message == "deadline_exceeded"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn simulated_hop_fails_at_the_origin_response_deadline() {
        let hop = Duration::from_millis(40);
        let deadline = tokio::time::Instant::now() + hop;

        assert!(matches!(
            wait_for_hop(hop, deadline).await,
            Err(ForwardError::Retryable(message)) if message == "deadline_exceeded"
        ));
    }
}
