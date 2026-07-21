//! Ishikari's distributed orchestration around the `mmpf-pmtiles` reader core.

mod cache;
// `reader` exposes `pub` traits/structs that are only re-exported for the
// simulator feature. Without that feature they are internal, so the
// `unreachable_pub` deny at the crate root cannot see their reachability.
#[cfg_attr(not(feature = "simulator-support"), allow(unreachable_pub))]
mod reader;

pub(crate) use cache::{DEFAULT_ARCHIVE_CACHE_MAX_BYTES, DEFAULT_LEAF_CACHE_MAX_BYTES};

pub use mmpf_pmtiles::MLT_CONTENT_TYPE;
#[cfg(feature = "simulator-support")]
pub use mmpf_pmtiles::TileLookupTrace as TileAccessPlan;
pub use mmpf_pmtiles::{
    ArchiveRange, Header, Metadata, ReaderLimits, TileCoord, TileData, TileId, TileLocation,
    TileType, Tilestats, TilestatsAttribute, TilestatsLayer, VectorLayer,
};
pub(crate) use reader::LocalLeafError;
#[cfg(not(feature = "simulator-support"))]
pub(crate) use reader::{BootstrapTransfer, Reader, Storage, StorageError};
#[cfg(feature = "simulator-support")]
#[doc(hidden)]
pub use reader::{BootstrapTransfer, Reader, Storage, StorageError};
