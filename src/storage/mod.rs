//! Storage integrations for local chunked reads and peer forwarding.

mod chunked_store;
mod peer;
mod pmtiles;
mod resolver;
mod routing;
mod store_registry;

pub use resolver::{
    ResourceResolver, ResourceResolverConfig, TileSource, TilesetError, TilesetInfo,
};
pub use store_registry::ObjectStoreRegistry;
