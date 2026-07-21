//! Shared configuration for the real and modeled Ishikari simulators.

use std::time::Duration;

use anyhow::{Result, ensure};
use ishikari_core::storage::{ResolverTuning, ResolverTuningInput};
use serde::{Deserialize, Serialize};

use crate::{latency::BackendLatencyConfig, topology::MAX_SIMULATED_NODE_COUNT};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub node_count: usize,
    pub tileset_sources: String,
    pub candidate_count: usize,
    pub tile_group_size: u64,
    pub chunk_size_bytes: u64,
    pub max_fetch_chunks: u64,
    pub chunk_fetch_merge_window_ms: u64,
    pub backend_fetch_concurrency: usize,
    pub backend_fetch_max_inflight: usize,
    #[serde(flatten, default)]
    pub backend_latency: BackendLatencyConfig,
    pub peer_latency_ms: u64,
    pub gossip_interval_ms: u64,
    pub gossip_hop_latency_ms: u64,
    pub tile_cache_max_bytes: u64,
    pub chunk_cache_max_bytes: u64,
    pub cache_peer_tiles: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            tileset_sources: "data".to_string(),
            candidate_count: 3,
            tile_group_size: 512,
            chunk_size_bytes: 1024 * 1024,
            max_fetch_chunks: 4,
            chunk_fetch_merge_window_ms: 10,
            backend_fetch_concurrency: 32,
            backend_fetch_max_inflight: 128,
            backend_latency: BackendLatencyConfig::default(),
            peer_latency_ms: 0,
            gossip_interval_ms: 200,
            gossip_hop_latency_ms: 1,
            tile_cache_max_bytes: 512 * 1024 * 1024,
            chunk_cache_max_bytes: 512 * 1024 * 1024,
            cache_peer_tiles: true,
        }
    }
}

impl ClusterConfig {
    pub(crate) fn validate(&self) -> Result<ResolverTuning> {
        ensure!(self.node_count > 0, "node_count must be greater than zero");
        ensure!(
            self.node_count <= MAX_SIMULATED_NODE_COUNT,
            "node_count exceeds the simulator address range"
        );

        ensure!(
            self.gossip_interval_ms > 0,
            "gossip_interval_ms must be greater than zero"
        );
        self.backend_latency.model_for_node(0)?;
        Ok(ResolverTuningInput {
            candidate_count: self.candidate_count,
            tile_group_size: self.tile_group_size,
            chunk_size_bytes: self.chunk_size_bytes,
            max_fetch_chunks: self.max_fetch_chunks,
            chunk_fetch_merge_window: Duration::from_millis(self.chunk_fetch_merge_window_ms),
            backend_fetch_concurrency: self.backend_fetch_concurrency,
            backend_fetch_max_inflight: self.backend_fetch_max_inflight,
            tile_cache_max_bytes: self.tile_cache_max_bytes,
            chunk_cache_max_bytes: self.chunk_cache_max_bytes,
            // Mirrors the production default. Simulator runs never republish an
            // archive mid-run, so negative entries do not expire during a run.
            tile_negative_ttl: Duration::from_secs(60),
        }
        .resolve()?)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ClusterConfig;
    use crate::latency::BackendLatencyConfig;

    #[test]
    fn rejects_empty_cluster() {
        let result = ClusterConfig {
            node_count: 0,
            ..ClusterConfig::default()
        }
        .validate();

        assert!(result.is_err());
    }

    #[test]
    fn rejects_invalid_cluster_dimensions_and_latency() {
        let invalid = [
            ClusterConfig {
                chunk_size_bytes: 0,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                gossip_interval_ms: 0,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                backend_latency: BackendLatencyConfig {
                    lognormal_sigma: f64::NAN,
                    ..BackendLatencyConfig::default()
                },
                ..ClusterConfig::default()
            },
        ];

        for config in invalid {
            assert!(config.validate().is_err(), "accepted {config:?}");
        }
    }

    #[test]
    fn resolver_boundaries_use_core_normalization_without_changing_raw_fields() {
        let config = ClusterConfig {
            candidate_count: 0,
            tile_group_size: 0,
            max_fetch_chunks: 0,
            chunk_fetch_merge_window_ms: 0,
            backend_fetch_concurrency: 0,
            backend_fetch_max_inflight: 0,
            tile_cache_max_bytes: 0,
            chunk_cache_max_bytes: 17,
            ..ClusterConfig::default()
        };

        let tuning = config.validate().expect("resolver boundaries are valid");

        assert_eq!(config.candidate_count, 0);
        assert_eq!(config.tile_group_size, 0);
        assert_eq!(config.max_fetch_chunks, 0);
        assert_eq!(config.backend_fetch_concurrency, 0);
        assert_eq!(config.backend_fetch_max_inflight, 0);
        assert_eq!(tuning.candidate_count(), 1);
        assert_eq!(tuning.tile_group_size(), 1);
        assert_eq!(tuning.max_fetch_chunks(), 1);
        assert_eq!(tuning.backend_fetch_concurrency(), 1);
        assert_eq!(tuning.backend_fetch_max_inflight(), 1);
        assert_eq!(tuning.chunk_fetch_merge_window(), Duration::ZERO);
        assert_eq!(tuning.tile_cache_max_bytes(), 0);
        assert_eq!(tuning.chunk_cache_max_bytes(), 17);
    }
}
