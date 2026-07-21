//! Chunked byte-range planning, caching, and inflight fetch coordination.

mod cache;
mod coordinator;
mod fetcher;
mod store;

pub use fetcher::BackendLatencyModel;
pub(super) use fetcher::ChunkFetchError;
pub(super) use store::ChunkedStore;
pub(crate) use store::{ChunkReadSource, ChunkedStoreConfig};

#[cfg(feature = "simulator-support")]
pub use coordinator::plan_chunk_fetch_ranges;
