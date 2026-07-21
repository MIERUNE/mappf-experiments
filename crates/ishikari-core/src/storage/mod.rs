//! Storage integrations for local chunked reads and peer forwarding.

// These modules contain a deliberately hidden simulator-injection surface.
// Its items are publicly re-exported only with `simulator-support`, which the
// production-only lint pass cannot infer.
#[cfg_attr(not(feature = "simulator-support"), allow(unreachable_pub))]
mod chunked_store;
#[cfg_attr(not(feature = "simulator-support"), allow(unreachable_pub))]
mod peer;
mod pmtiles;
#[cfg_attr(not(feature = "simulator-support"), allow(unreachable_pub))]
mod resolver;
#[cfg_attr(not(feature = "simulator-support"), allow(unreachable_pub))]
mod routing;
mod store_registry;
mod tuning;

// Internal transport surface consumed by the `ishikari` server binary. Not a
// stable public API, hence `doc(hidden)`.
#[doc(hidden)]
pub use peer::InternalTileSource;
#[doc(hidden)]
pub use peer::{
    InternalFetchResponse, InternalProviderNegative, ProviderRequest, ProviderResourceKind,
    ProviderRouteOutcome, ProviderSpriteVariant,
};
#[doc(hidden)]
pub use peer::{
    PROVIDER_AGE_HEADER, PROVIDER_CACHE_CONTROL_HEADER, PROVIDER_ETAG_HEADER,
    PROVIDER_LAST_MODIFIED_HEADER, PROVIDER_NEGATIVE_HEADER, TILE_SOURCE_HEADER,
};
pub use peer::{Peer, PeerDirectory, PeerFuture, PeerSnapshotCache};
#[doc(hidden)]
pub use peer::{
    internal_peer_request_timeout, internal_resource_kind, internal_response_body_limit,
};
// The internal peer transport is injected by the `ishikari` server binary,
// which owns the concrete (reqwest-based) implementation. These are the seam
// types it needs; not a stable public API, hence `doc(hidden)`.
#[doc(hidden)]
pub use peer::{FetchFuture, InternalTransport, PeerFetchError};
pub use resolver::{
    ArchivePresence, LeafBytesError, ResourceCacheCapacities, ResourceResolver,
    ResourceResolverConfig, TileSource, TilesetError, TilesetInfo,
};
pub use store_registry::ObjectStoreRegistry;
pub use tuning::{ResolverTuning, ResolverTuningError, ResolverTuningInput};

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
pub use peer::PeerBackend;
#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub use resolver::{PeerTileCachePolicy, ResourceResolverStorageConfig};
#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub use routing::HrwRouter;
