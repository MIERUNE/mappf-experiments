//! In-process `Transport` impl: `NodeRegistry` (Weak refs) + per-hop sleep,
//! calling the target node's `handle_forwarded` directly.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;

use biei_core::node::Node;
use biei_core::transport::{ForwardError, Transport};
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
impl Transport for ChannelTransport {
    async fn send(
        &self,
        target: NodeId,
        fwd: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
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
        self.successes.fetch_add(1, Ordering::Relaxed);
        Ok(ForwardResponse::from_task_outcome(outcome, style_id))
    }
}
