//! PMTiles decoding and archive reader abstractions.

mod cache;
mod format;
mod metadata;
mod reader;

pub(crate) use format::MLT_CONTENT_TYPE;
pub use format::{Header, TileCoord, TileData, TileId, TileType};
pub use metadata::{Metadata, Tilestats, TilestatsAttribute, TilestatsLayer, VectorLayer};
#[cfg(feature = "simulator-support")]
pub use reader::TileAccessPlan;
pub use reader::{ArchiveRange, BootstrapTransfer, Reader, Storage, StorageError, TileLocation};
