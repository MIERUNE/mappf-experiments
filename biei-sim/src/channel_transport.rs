//! In-process `Transport` impl: `NodeRegistry` (Weak refs) + per-hop sleep,
//! calling the target node's `handle_forwarded` directly.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;

use biei::node::Node;
use biei::transport::{ForwardError, Transport};
use biei::types::NodeId;
use biei::wire::{ForwardRequest, ForwardResponse};

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
}

/// In-process transport that simulates per-hop latency by sleeping
/// `hop_latency` before delegating to the target node's
/// [`Node::handle_forwarded`].
pub struct ChannelTransport {
    hop_latency: Duration,
    registry: Arc<NodeRegistry>,
}

impl ChannelTransport {
    pub fn new(hop_latency: Duration, registry: Arc<NodeRegistry>) -> Self {
        Self {
            hop_latency,
            registry,
        }
    }
}

#[async_trait]
impl Transport for ChannelTransport {
    async fn send(
        &self,
        target: NodeId,
        fwd: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError> {
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(fwd.task.remaining_budget_ms as u64);
        tokio::time::sleep(self.hop_latency).await;
        if tokio::time::Instant::now() >= deadline {
            return Err(ForwardError::Retryable("deadline_exceeded".to_string()));
        }
        let style_id = fwd.task.style.id.clone();
        let node = self
            .registry
            .get(&target)
            .ok_or_else(|| ForwardError::Retryable(format!("unknown node {target}")))?;
        let outcome = node.handle_forwarded(fwd).await;
        Ok(ForwardResponse::from_task_outcome(outcome, style_id))
    }
}
