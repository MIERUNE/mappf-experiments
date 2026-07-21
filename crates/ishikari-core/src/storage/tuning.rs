//! Shared resolver tuning validation and normalization.

use std::time::Duration;

use thiserror::Error;

/// Raw resolver tuning supplied by a production or simulation composition root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolverTuningInput {
    pub candidate_count: usize,
    pub tile_group_size: u64,
    pub chunk_size_bytes: u64,
    pub max_fetch_chunks: u64,
    pub chunk_fetch_merge_window: Duration,
    pub backend_fetch_concurrency: usize,
    pub backend_fetch_max_inflight: usize,
    pub tile_cache_max_bytes: u64,
    pub chunk_cache_max_bytes: u64,
    pub tile_negative_ttl: Duration,
}

impl ResolverTuningInput {
    /// Validates required values and applies Ishikari's canonical lower bounds.
    pub fn resolve(self) -> Result<ResolverTuning, ResolverTuningError> {
        if self.chunk_size_bytes == 0 {
            return Err(ResolverTuningError::ZeroChunkSizeBytes);
        }

        let backend_fetch_concurrency = self.backend_fetch_concurrency.max(1);
        Ok(ResolverTuning {
            candidate_count: self.candidate_count.max(1),
            tile_group_size: self.tile_group_size.max(1),
            chunk_size_bytes: self.chunk_size_bytes,
            max_fetch_chunks: self.max_fetch_chunks.max(1),
            chunk_fetch_merge_window: self.chunk_fetch_merge_window,
            backend_fetch_concurrency,
            backend_fetch_max_inflight: self
                .backend_fetch_max_inflight
                .max(backend_fetch_concurrency),
            tile_cache_max_bytes: self.tile_cache_max_bytes,
            chunk_cache_max_bytes: self.chunk_cache_max_bytes,
            tile_negative_ttl: self.tile_negative_ttl,
        })
    }
}

/// Validated resolver tuning shared by production and simulation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolverTuning {
    candidate_count: usize,
    tile_group_size: u64,
    chunk_size_bytes: u64,
    max_fetch_chunks: u64,
    chunk_fetch_merge_window: Duration,
    backend_fetch_concurrency: usize,
    backend_fetch_max_inflight: usize,
    tile_cache_max_bytes: u64,
    chunk_cache_max_bytes: u64,
    tile_negative_ttl: Duration,
}

impl ResolverTuning {
    pub fn candidate_count(self) -> usize {
        self.candidate_count
    }

    pub fn tile_group_size(self) -> u64 {
        self.tile_group_size
    }

    pub fn chunk_size_bytes(self) -> u64 {
        self.chunk_size_bytes
    }

    pub fn max_fetch_chunks(self) -> u64 {
        self.max_fetch_chunks
    }

    pub fn chunk_fetch_merge_window(self) -> Duration {
        self.chunk_fetch_merge_window
    }

    pub fn backend_fetch_concurrency(self) -> usize {
        self.backend_fetch_concurrency
    }

    pub fn backend_fetch_max_inflight(self) -> usize {
        self.backend_fetch_max_inflight
    }

    pub fn tile_cache_max_bytes(self) -> u64 {
        self.tile_cache_max_bytes
    }

    pub fn chunk_cache_max_bytes(self) -> u64 {
        self.chunk_cache_max_bytes
    }

    pub fn tile_negative_ttl(self) -> Duration {
        self.tile_negative_ttl
    }
}

/// Invalid resolver tuning that cannot be normalized safely.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ResolverTuningError {
    #[error("chunk_size_bytes must be greater than zero")]
    ZeroChunkSizeBytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> ResolverTuningInput {
        ResolverTuningInput {
            candidate_count: 3,
            tile_group_size: 512,
            chunk_size_bytes: 1024 * 1024,
            max_fetch_chunks: 4,
            chunk_fetch_merge_window: Duration::from_millis(10),
            backend_fetch_concurrency: 32,
            backend_fetch_max_inflight: 128,
            tile_cache_max_bytes: 512 * 1024 * 1024,
            chunk_cache_max_bytes: 256 * 1024 * 1024,
            tile_negative_ttl: Duration::from_secs(60),
        }
    }

    #[test]
    fn normalizes_lower_bounds_and_preserves_unbounded_values() {
        let tuning = ResolverTuningInput {
            candidate_count: 0,
            tile_group_size: 0,
            max_fetch_chunks: 0,
            chunk_fetch_merge_window: Duration::ZERO,
            backend_fetch_concurrency: 0,
            backend_fetch_max_inflight: 0,
            tile_cache_max_bytes: 0,
            chunk_cache_max_bytes: 17,
            tile_negative_ttl: Duration::ZERO,
            ..input()
        }
        .resolve()
        .expect("boundary values resolve");

        assert_eq!(tuning.candidate_count(), 1);
        assert_eq!(tuning.tile_group_size(), 1);
        assert_eq!(tuning.chunk_size_bytes(), 1024 * 1024);
        assert_eq!(tuning.max_fetch_chunks(), 1);
        assert_eq!(tuning.backend_fetch_concurrency(), 1);
        assert_eq!(tuning.backend_fetch_max_inflight(), 1);
        assert_eq!(tuning.chunk_fetch_merge_window(), Duration::ZERO);
        assert_eq!(tuning.tile_cache_max_bytes(), 0);
        assert_eq!(tuning.chunk_cache_max_bytes(), 17);
        assert_eq!(tuning.tile_negative_ttl(), Duration::ZERO);
    }

    #[test]
    fn rejects_zero_chunk_size() {
        let error = ResolverTuningInput {
            chunk_size_bytes: 0,
            ..input()
        }
        .resolve()
        .expect_err("zero chunk size must be rejected");

        assert_eq!(error, ResolverTuningError::ZeroChunkSizeBytes);
        assert_eq!(
            error.to_string(),
            "chunk_size_bytes must be greater than zero"
        );
    }
}
