//! `GossipBus` trait — per-key cluster state propagation (chitchat-style).

use async_trait::async_trait;

use crate::types::{ClusterView, NodeId};

/// Per-key gossip backend (`chitchat`-style). Each node has its own KV
/// namespace; backends are responsible for propagation (delay model varies).
#[async_trait]
pub trait GossipBus: Send + Sync {
    /// Upsert `key` under node `node_id`'s namespace with `value`. Idempotent
    /// — backends should skip propagation when `value` is unchanged.
    async fn set(&self, node_id: NodeId, key: String, value: String);

    /// Cluster-wide view of currently-visible state, decoded into
    /// `NodeStateView` per node.
    async fn view(&self) -> ClusterView;
}
