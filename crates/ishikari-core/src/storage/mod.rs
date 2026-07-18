//! Storage integrations for local chunked reads and peer forwarding.

mod chunked_store;
mod peer;
mod pmtiles;
mod resolver;
mod routing;
mod store_registry;

#[cfg(not(feature = "simulator-support"))]
pub(crate) use peer::InternalFetchResponse;
#[cfg(not(feature = "simulator-support"))]
pub(crate) use peer::{InternalTileSource, internal_resource_kind};
pub(crate) use peer::{
    PROVIDER_AGE_HEADER, PROVIDER_CACHE_CONTROL_HEADER, PROVIDER_ETAG_HEADER,
    PROVIDER_LAST_MODIFIED_HEADER, TILE_SOURCE_HEADER,
};
pub use resolver::{
    ResourceResolver, ResourceResolverConfig, TileSource, TilesetError, TilesetInfo,
};
pub use store_registry::ObjectStoreRegistry;

#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub use crate::interned::TilesetId;
#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub use chunked_store::BackendLatencyModel;
#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub use chunked_store::plan_chunk_fetch_ranges;
#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub use peer::{
    FetchFuture, InternalFetchResponse, InternalTileSource, InternalTransport, PeerBackend,
    PeerDirectory, PeerFetchError, PeerFuture, internal_resource_kind,
};
#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub use resolver::{PeerTileCachePolicy, ResourceResolverStorageConfig};
#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub use routing::HrwRouter;
