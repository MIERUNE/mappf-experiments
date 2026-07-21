//! `GossipBus` trait — per-key cluster state propagation (chitchat-style).

use async_trait::async_trait;

use crate::types::{ClusterView, NodeKvs};

/// Node-local per-key gossip backend (`chitchat`-style). Each adapter writes
/// only its own KV namespace; backends are responsible for propagation.
#[async_trait]
pub trait GossipBus: Send + Sync {
    /// Upsert `key` in this node's namespace. Idempotent backends should skip
    /// propagation when `value` is unchanged.
    async fn set(&self, key: String, value: String);

    /// Upsert a set of changed self-node keys. Backends with a shared state lock
    /// should override this to apply the batch in one critical section.
    async fn set_many(&self, kvs: NodeKvs) {
        for (key, value) in kvs {
            self.set(key, value).await;
        }
    }

    /// Monotonic local epoch for authoritative membership changes.
    ///
    /// Buses without an external active-membership authority can keep the
    /// default. Node view caches use this to discard removed routing members
    /// immediately rather than serving a bounded stale snapshot.
    fn view_epoch(&self) -> u64 {
        0
    }

    /// This node's currently-visible cluster state, decoded into
    /// `NodeStateView` values.
    async fn view(&self) -> ClusterView;
}
