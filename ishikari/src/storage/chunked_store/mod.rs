//! Chunked byte-range planning, caching, and inflight fetch coordination.

mod cache;
mod coordinator;
mod fetcher;
mod store;

pub use fetcher::{BackendLatencyModel, ChunkFetchError};
pub(crate) use store::ChunkReadSource;
pub use store::ChunkedStore;

#[cfg(feature = "simulator-support")]
pub use coordinator::plan_chunk_fetch_ranges;
