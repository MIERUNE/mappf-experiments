//! Ishikari tile-serving core: PMTiles resource resolution, tile/chunk caching,
//! HRW routing, and domain interfaces shared by the Ishikari server
//! (`servers/ishikari`) and simulator.
#![deny(unreachable_pub)]

mod cache;
#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub mod cache_policy;
#[cfg(not(feature = "simulator-support"))]
#[allow(unreachable_pub)]
mod cache_policy;
#[doc(hidden)]
pub mod cluster_metadata;
pub mod interned;
pub mod metrics;
// The tile-derivation helpers built on this reader live in the `ishikari`
// binary crate, so its types are exposed unconditionally (behind `doc(hidden)`
// as they are not a stable public surface). The `simulator-support` feature
// only widens the reader re-exports below for the simulator.
#[doc(hidden)]
pub mod pmtiles;
pub mod storage;
