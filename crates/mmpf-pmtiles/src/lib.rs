//! Customizable PMTiles v3 decoding and range-reading primitives.
//!
//! [`ArchiveReader`] owns tile-id validation, bounded directory traversal, and
//! tile assembly. Its standard range-backed form is bound to one immutable
//! archive generation; distributed services can instead implement
//! [`ArchiveBackend`] while retaining their own peer and cache policy.

mod format;
mod metadata;
mod reader;

pub use format::{
    Compression, Directory, DirectoryEntry, HEADER_SIZE, Header, MLT_CONTENT_TYPE, TileCoord,
    TileData, TileId, TileType, decompress_bytes_with_limit,
};
pub use metadata::{
    Metadata, MetadataJson, Tilestats, TilestatsAttribute, TilestatsLayer, VectorLayer,
};
pub use reader::{
    ArchiveBackend, ArchiveBootstrap, ArchiveIdentity, ArchiveIdentityError, ArchiveRange,
    ArchiveReader, DEFAULT_MAX_DECOMPRESSED_BYTES, DEFAULT_MAX_DIRECTORY_DEPTH, DirectoryStep,
    DirectoryStore, DirectoryWalker, LeafDirectoryKey, LeafDirectoryRequest, MIN_BOOTSTRAP_BYTES,
    NoDirectoryStore, NoopObserver, RangeArchiveBackend, RangeSource, ReadKind, ReadObserver,
    ReadRequest, ReaderConfigError, ReaderError, ReaderEvent, ReaderLimits, TileLocation,
    TileLookupTrace, archive_len, decode_bootstrap_bytes, decode_metadata_bytes,
};
