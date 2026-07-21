//! A single-archive PMTiles reader with injectable I/O, cache, and observation.

use std::{error::Error, fmt, future::Future, sync::Arc};

use anyhow::{Context, Result as AnyResult, anyhow, bail};
use bytes::Bytes;

use crate::{
    Compression, Directory, HEADER_SIZE, Header, Metadata, TileData, TileId,
    decompress_bytes_with_limit,
};

/// PMTiles v3 requires the header and root directory to fit in the first 16 KiB.
pub const MIN_BOOTSTRAP_BYTES: usize = 16_384;
pub const DEFAULT_MAX_DIRECTORY_DEPTH: u8 = 64;
pub const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

/// Stable identity for one immutable generation of an archive.
///
/// Directory caches must include both fields in their key. When an object is
/// replaced at the same path, construct a reader with a new generation (for
/// example an ETag, version id, or content digest).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ArchiveIdentity {
    name: Arc<str>,
    generation: Arc<str>,
}

impl ArchiveIdentity {
    pub fn new(
        name: impl Into<Arc<str>>,
        generation: impl Into<Arc<str>>,
    ) -> Result<Self, ArchiveIdentityError> {
        let name = name.into();
        let generation = generation.into();
        if name.is_empty() {
            return Err(ArchiveIdentityError(
                "archive identity name must not be empty".into(),
            ));
        }
        if generation.is_empty() {
            return Err(ArchiveIdentityError(
                "archive identity generation must not be empty".into(),
            ));
        }
        Ok(Self { name, generation })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveIdentityError(String);

impl fmt::Display for ArchiveIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ArchiveIdentityError {}

/// Purpose of a source range read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadKind {
    Bootstrap,
    Metadata,
    LeafDirectory,
    Tile,
}

/// One requested archive byte range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadRequest {
    pub offset: u64,
    pub length: usize,
    pub archive_len: Option<u64>,
    pub kind: ReadKind,
}

/// Raw range source for one archive.
///
/// HTTP, object storage, files, chunk caches, and layered sources can implement
/// this trait without exposing their routing policy to the PMTiles reader.
pub trait RangeSource: Send + Sync {
    type Error: Error + Send + Sync + 'static;

    fn read_range(
        &self,
        request: ReadRequest,
    ) -> impl Future<Output = Result<Bytes, Self::Error>> + Send;
}

/// Cached header and root directory for one archive generation.
#[derive(Clone)]
pub struct ArchiveBootstrap {
    pub header: Header,
    pub root: Arc<Directory>,
    pub metadata: Option<Arc<Metadata>>,
    archive_len: u64,
}

impl ArchiveBootstrap {
    pub fn new(
        header: Header,
        root: Arc<Directory>,
        metadata: Option<Arc<Metadata>>,
    ) -> AnyResult<Self> {
        let archive_len = archive_len(&header)?;
        Ok(Self {
            header,
            root,
            metadata,
            archive_len,
        })
    }

    pub fn archive_len(&self) -> u64 {
        self.archive_len
    }
}

/// Cache key for a decoded leaf directory.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LeafDirectoryKey {
    pub archive: ArchiveIdentity,
    pub offset: u64,
    pub length: u32,
}

/// Storage for decoded directories.
///
/// Implementations may be local, distributed, bounded, or no-op. Reads are
/// deliberately synchronous: remote peer lookup belongs in a layered
/// [`RangeSource`], while this contract remains usable by low-overhead caches.
pub trait DirectoryStore: Send + Sync {
    fn get_bootstrap(&self, archive: &ArchiveIdentity) -> Option<ArchiveBootstrap>;
    fn put_bootstrap(&self, archive: &ArchiveIdentity, bootstrap: ArchiveBootstrap);
    fn get_leaf(&self, key: &LeafDirectoryKey) -> Option<Arc<Directory>>;
    fn put_leaf(&self, key: LeafDirectoryKey, directory: Arc<Directory>);
}

impl<T> DirectoryStore for Arc<T>
where
    T: DirectoryStore + ?Sized,
{
    fn get_bootstrap(&self, archive: &ArchiveIdentity) -> Option<ArchiveBootstrap> {
        (**self).get_bootstrap(archive)
    }

    fn put_bootstrap(&self, archive: &ArchiveIdentity, bootstrap: ArchiveBootstrap) {
        (**self).put_bootstrap(archive, bootstrap);
    }

    fn get_leaf(&self, key: &LeafDirectoryKey) -> Option<Arc<Directory>> {
        (**self).get_leaf(key)
    }

    fn put_leaf(&self, key: LeafDirectoryKey, directory: Arc<Directory>) {
        (**self).put_leaf(key, directory);
    }
}

/// Directory policy that performs no caching.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoDirectoryStore;

impl DirectoryStore for NoDirectoryStore {
    fn get_bootstrap(&self, _archive: &ArchiveIdentity) -> Option<ArchiveBootstrap> {
        None
    }

    fn put_bootstrap(&self, _archive: &ArchiveIdentity, _bootstrap: ArchiveBootstrap) {}

    fn get_leaf(&self, _key: &LeafDirectoryKey) -> Option<Arc<Directory>> {
        None
    }

    fn put_leaf(&self, _key: LeafDirectoryKey, _directory: Arc<Directory>) {}
}

/// Observable reader event. It contains no source error or tile data bodies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReaderEvent {
    CacheHit { kind: ReadKind, offset: u64 },
    ReadStarted(ReadRequest),
    ReadCompleted { request: ReadRequest, bytes: usize },
    ShortRead { request: ReadRequest, bytes: usize },
    ReadFailed(ReadRequest),
    DirectoryDecoded { kind: ReadKind, entries: usize },
}

pub trait ReadObserver: Send + Sync {
    fn observe(&self, event: ReaderEvent);
}

impl<T> ReadObserver for Arc<T>
where
    T: ReadObserver + ?Sized,
{
    fn observe(&self, event: ReaderEvent) {
        (**self).observe(event);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopObserver;

impl ReadObserver for NoopObserver {
    fn observe(&self, _event: ReaderEvent) {}
}

/// Resource and traversal limits enforced by the reader.
#[derive(Clone, Copy, Debug)]
pub struct ReaderLimits {
    pub bootstrap_bytes: usize,
    pub max_range_bytes: usize,
    pub max_directory_depth: u8,
    pub max_decompressed_directory_bytes: usize,
    pub max_decompressed_metadata_bytes: usize,
}

impl Default for ReaderLimits {
    fn default() -> Self {
        Self {
            bootstrap_bytes: MIN_BOOTSTRAP_BYTES,
            max_range_bytes: 64 * 1024 * 1024,
            max_directory_depth: DEFAULT_MAX_DIRECTORY_DEPTH,
            max_decompressed_directory_bytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
            max_decompressed_metadata_bytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReaderConfigError(String);

impl fmt::Display for ReaderConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ReaderConfigError {}

/// Typed reader error preserving the source's native error type.
#[derive(Debug)]
pub enum ReaderError<E> {
    Source(E),
    InvalidTileId(anyhow::Error),
    InvalidArchive(anyhow::Error),
}

impl<E: fmt::Display> fmt::Display for ReaderError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => write!(formatter, "archive source read failed: {error}"),
            Self::InvalidTileId(error) => write!(formatter, "invalid PMTiles tile id: {error}"),
            Self::InvalidArchive(error) => write!(formatter, "invalid PMTiles archive: {error:#}"),
        }
    }
}

impl<E: Error + 'static> Error for ReaderError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::InvalidTileId(error) | Self::InvalidArchive(error) => Some(error.as_ref()),
        }
    }
}

/// Byte range occupied by one tile in an archive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TileLocation {
    pub offset: u64,
    pub length: u32,
    pub archive_len: u64,
}

/// A range consulted while resolving a tile directory entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveRange {
    pub offset: u64,
    pub length: u32,
}

/// Actual directory reads needed to resolve a tile lookup, plus the payload
/// range when the tile exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TileLookupTrace {
    pub bootstrap: ArchiveRange,
    pub leaves: Vec<ArchiveRange>,
    pub tile: Option<TileLocation>,
    pub archive_len: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectoryStep {
    Missing,
    Leaf(ArchiveRange),
    Tile(TileLocation),
}

/// Stateful, I/O-independent traversal of a PMTiles directory tree.
///
/// Call [`DirectoryWalker::step`] with the root directory and then each leaf
/// requested by [`DirectoryStep::Leaf`]. The walker owns all PMTiles section
/// offset validation and traversal-depth accounting; callers only decide how
/// leaf bytes are fetched and cached.
pub struct DirectoryWalker {
    header: Header,
    archive_len: u64,
    tile_id: TileId,
    depth: u8,
    max_depth: u8,
}

impl DirectoryWalker {
    pub fn new(header: Header, tile_id: TileId, max_depth: u8) -> AnyResult<Self> {
        if max_depth == 0 {
            bail!("PMTiles directory depth limit must be greater than zero");
        }
        let archive_len = archive_len(&header)?;
        Ok(Self {
            header,
            archive_len,
            tile_id,
            depth: 0,
            max_depth,
        })
    }

    pub fn step(&mut self, directory: &Directory) -> AnyResult<DirectoryStep> {
        let Some((_, entry)) = directory.find_tile_id(self.tile_id) else {
            return Ok(DirectoryStep::Missing);
        };

        if entry.is_leaf() {
            if self.depth >= self.max_depth {
                bail!("PMTiles directory depth exceeds {}", self.max_depth);
            }
            self.depth += 1;
            let offset = checked_section_offset(
                "leaf directory",
                self.header.leaf_offset,
                self.header.leaf_length,
                entry.offset,
                u64::from(entry.length),
            )?;
            return Ok(DirectoryStep::Leaf(ArchiveRange {
                offset,
                length: entry.length,
            }));
        }

        let offset = checked_section_offset(
            "tile data",
            self.header.data_offset,
            self.header.data_length,
            entry.offset,
            u64::from(entry.length),
        )?;
        Ok(DirectoryStep::Tile(TileLocation {
            offset,
            length: entry.length,
            archive_len: self.archive_len,
        }))
    }
}

/// One decoded leaf directory requested while traversing an archive.
#[derive(Clone, Copy, Debug)]
pub struct LeafDirectoryRequest {
    pub offset: u64,
    pub length: u32,
    pub compression: Compression,
    pub archive_len: u64,
}

/// Parsed archive access used by [`ArchiveReader`].
///
/// [`RangeArchiveBackend`] is the ordinary implementation for a raw
/// [`RangeSource`]. Services with a distributed index tier can instead supply
/// already decoded, single-flighted bootstraps and leaf directories without
/// forcing that policy into the PMTiles format layer.
pub trait ArchiveBackend: Send + Sync {
    type Error: Send;

    fn load_bootstrap(&self) -> impl Future<Output = Result<ArchiveBootstrap, Self::Error>> + Send;

    fn load_metadata(&self) -> impl Future<Output = Result<Arc<Metadata>, Self::Error>> + Send;

    fn load_leaf(
        &self,
        request: LeafDirectoryRequest,
    ) -> impl Future<Output = Result<Arc<Directory>, Self::Error>> + Send;

    fn read_range(
        &self,
        request: ReadRequest,
    ) -> impl Future<Output = Result<Bytes, Self::Error>> + Send;

    fn invalid_tile_id(&self, error: anyhow::Error) -> Self::Error;
    fn invalid_archive(&self, error: anyhow::Error) -> Self::Error;
}

/// Raw-range implementation of [`ArchiveBackend`].
pub struct RangeArchiveBackend<S, D = NoDirectoryStore, O = NoopObserver> {
    source: S,
    identity: ArchiveIdentity,
    directories: D,
    observer: O,
    limits: ReaderLimits,
}

/// PMTiles reader over an injected archive backend.
///
/// The backend owns I/O, decoded-directory caching, coalescing, and observation
/// policy. The reader owns tile-id validation, bounded directory traversal,
/// access traces, and tile representation assembly.
pub struct ArchiveReader<B> {
    backend: B,
    limits: ReaderLimits,
}

impl<S> ArchiveReader<RangeArchiveBackend<S>> {
    pub fn new(source: S, identity: ArchiveIdentity) -> Self {
        Self {
            backend: RangeArchiveBackend {
                source,
                identity,
                directories: NoDirectoryStore,
                observer: NoopObserver,
                limits: ReaderLimits::default(),
            },
            limits: ReaderLimits::default(),
        }
    }
}

impl<S, D, O> ArchiveReader<RangeArchiveBackend<S, D, O>> {
    pub fn with_components(
        source: S,
        identity: ArchiveIdentity,
        directories: D,
        observer: O,
        limits: ReaderLimits,
    ) -> Result<Self, ReaderConfigError> {
        validate_limits(limits)?;
        Ok(Self {
            backend: RangeArchiveBackend {
                source,
                identity,
                directories,
                observer,
                limits,
            },
            limits,
        })
    }
}

impl<B> ArchiveReader<B> {
    pub fn with_backend(backend: B, limits: ReaderLimits) -> Result<Self, ReaderConfigError> {
        validate_limits(limits)?;
        Ok(Self { backend, limits })
    }
}

impl<B> ArchiveReader<B>
where
    B: ArchiveBackend,
{
    pub async fn header(&self) -> Result<Header, B::Error> {
        Ok(self.backend.load_bootstrap().await?.header)
    }

    pub async fn metadata(&self) -> Result<Arc<Metadata>, B::Error> {
        self.backend.load_metadata().await
    }

    pub async fn lookup_with_trace(&self, tile_id: u64) -> Result<TileLookupTrace, B::Error> {
        let mut leaves = Vec::new();
        let (tile, _, archive_len) = self
            .resolve_tile_location(tile_id, Some(&mut leaves))
            .await?;
        Ok(TileLookupTrace {
            bootstrap: ArchiveRange {
                offset: 0,
                length: u32::try_from(self.limits.bootstrap_bytes).unwrap_or(u32::MAX),
            },
            leaves,
            tile,
            archive_len,
        })
    }

    /// Rejects a range that exceeds `ReaderLimits::max_range_bytes` or overflows
    /// the archive address space before any backend I/O. Tile and leaf lengths
    /// come from the archive's own directory, so an oversized range is a
    /// malformed/oversized archive rather than a legitimate request. Enforcing it
    /// here means every backend — including custom ones like Ishikari's storage
    /// adapter — observes the same bound, regardless of its own checks.
    fn check_range(&self, offset: u64, length: u32) -> Result<(), B::Error> {
        let length = length as usize;
        if length > self.limits.max_range_bytes {
            return Err(self.backend.invalid_archive(anyhow!(
                "archive range length {length} exceeds max_range_bytes {}",
                self.limits.max_range_bytes
            )));
        }
        if offset.checked_add(length as u64).is_none() {
            return Err(self.backend.invalid_archive(anyhow!(
                "archive range at offset {offset} length {length} overflows the archive address space"
            )));
        }
        Ok(())
    }

    pub async fn get_tile(&self, tile_id: u64) -> Result<Option<TileData>, B::Error> {
        let (location, header, _) = self.resolve_tile_location(tile_id, None).await?;
        let Some(location) = location else {
            return Ok(None);
        };
        self.check_range(location.offset, location.length)?;
        let bytes = self
            .backend
            .read_range(ReadRequest {
                offset: location.offset,
                length: location.length as usize,
                archive_len: Some(location.archive_len),
                kind: ReadKind::Tile,
            })
            .await?;
        Ok(Some(TileData {
            bytes,
            content_type: header.tile_type.content_type(),
            content_encoding: header.tile_compression.content_encoding(),
        }))
    }

    async fn resolve_tile_location(
        &self,
        tile_id: u64,
        mut leaves: Option<&mut Vec<ArchiveRange>>,
    ) -> Result<(Option<TileLocation>, Header, u64), B::Error> {
        let tile_id = TileId::new(tile_id).map_err(|error| self.backend.invalid_tile_id(error))?;
        let bootstrap = self.backend.load_bootstrap().await?;
        let mut walker =
            DirectoryWalker::new(bootstrap.header, tile_id, self.limits.max_directory_depth)
                .map_err(|error| self.backend.invalid_archive(error))?;
        let archive_len = bootstrap.archive_len();
        let mut directory = bootstrap.root;
        loop {
            match walker
                .step(&directory)
                .map_err(|error| self.backend.invalid_archive(error))?
            {
                DirectoryStep::Missing => {
                    return Ok((None, bootstrap.header, archive_len));
                }
                DirectoryStep::Tile(location) => {
                    return Ok((Some(location), bootstrap.header, archive_len));
                }
                DirectoryStep::Leaf(range) => {
                    self.check_range(range.offset, range.length)?;
                    if let Some(leaves) = leaves.as_mut() {
                        leaves.push(range);
                    }
                    directory = self
                        .backend
                        .load_leaf(LeafDirectoryRequest {
                            offset: range.offset,
                            length: range.length,
                            compression: bootstrap.header.internal_compression,
                            archive_len,
                        })
                        .await?;
                }
            }
        }
    }
}

impl<S, D, O> ArchiveBackend for RangeArchiveBackend<S, D, O>
where
    S: RangeSource,
    D: DirectoryStore,
    O: ReadObserver,
{
    type Error = ReaderError<S::Error>;

    async fn load_bootstrap(&self) -> Result<ArchiveBootstrap, Self::Error> {
        if let Some(bootstrap) = self.directories.get_bootstrap(&self.identity) {
            self.observer.observe(ReaderEvent::CacheHit {
                kind: ReadKind::Bootstrap,
                offset: 0,
            });
            return Ok(bootstrap);
        }

        let bytes = self
            .read_range(ReadRequest {
                offset: 0,
                length: self.limits.bootstrap_bytes,
                archive_len: None,
                kind: ReadKind::Bootstrap,
            })
            .await?;
        let bootstrap = decode_bootstrap_bytes(bytes, self.limits.max_decompressed_directory_bytes)
            .map_err(ReaderError::InvalidArchive)?;
        self.observer.observe(ReaderEvent::DirectoryDecoded {
            kind: ReadKind::Bootstrap,
            entries: bootstrap.root.entries.len(),
        });
        self.directories
            .put_bootstrap(&self.identity, bootstrap.clone());
        Ok(bootstrap)
    }

    async fn load_metadata(&self) -> Result<Arc<Metadata>, Self::Error> {
        let mut bootstrap = self.load_bootstrap().await?;
        if let Some(metadata) = bootstrap.metadata {
            self.observer.observe(ReaderEvent::CacheHit {
                kind: ReadKind::Metadata,
                offset: bootstrap.header.metadata_offset,
            });
            return Ok(metadata);
        }

        let metadata = if bootstrap.header.metadata_length == 0 {
            Metadata::default()
        } else {
            let length = usize::try_from(bootstrap.header.metadata_length)
                .context("PMTiles metadata length exceeds usize")
                .map_err(ReaderError::InvalidArchive)?;
            let request = ReadRequest {
                offset: bootstrap.header.metadata_offset,
                length,
                archive_len: Some(bootstrap.archive_len()),
                kind: ReadKind::Metadata,
            };
            let bytes = self.read_range(request).await?;
            decode_metadata_bytes(
                &bootstrap.header,
                bytes,
                self.limits.max_decompressed_metadata_bytes,
            )
            .map_err(ReaderError::InvalidArchive)?
        };
        let metadata = Arc::new(metadata);
        bootstrap.metadata = Some(Arc::clone(&metadata));
        self.directories.put_bootstrap(&self.identity, bootstrap);
        Ok(metadata)
    }

    async fn load_leaf(
        &self,
        request: LeafDirectoryRequest,
    ) -> Result<Arc<Directory>, Self::Error> {
        let key = LeafDirectoryKey {
            archive: self.identity.clone(),
            offset: request.offset,
            length: request.length,
        };
        if let Some(directory) = self.directories.get_leaf(&key) {
            self.observer.observe(ReaderEvent::CacheHit {
                kind: ReadKind::LeafDirectory,
                offset: request.offset,
            });
            return Ok(directory);
        }
        let bytes = self
            .read_range(ReadRequest {
                offset: request.offset,
                length: request.length as usize,
                archive_len: Some(request.archive_len),
                kind: ReadKind::LeafDirectory,
            })
            .await?;
        let directory = Arc::new(
            Directory::parse_with_limit(
                request.compression,
                bytes,
                self.limits.max_decompressed_directory_bytes,
            )
            .context("failed to parse PMTiles leaf directory")
            .map_err(ReaderError::InvalidArchive)?,
        );
        self.observer.observe(ReaderEvent::DirectoryDecoded {
            kind: ReadKind::LeafDirectory,
            entries: directory.entries.len(),
        });
        self.directories.put_leaf(key, Arc::clone(&directory));
        Ok(directory)
    }

    async fn read_range(&self, request: ReadRequest) -> Result<Bytes, Self::Error> {
        if request.length > self.limits.max_range_bytes {
            return Err(ReaderError::InvalidArchive(anyhow!(
                "{:?} read requests {} bytes, exceeding configured limit {}",
                request.kind,
                request.length,
                self.limits.max_range_bytes
            )));
        }
        self.observer.observe(ReaderEvent::ReadStarted(request));
        match self.source.read_range(request).await {
            Ok(bytes) => {
                if request.kind != ReadKind::Bootstrap && bytes.len() != request.length {
                    self.observer.observe(ReaderEvent::ShortRead {
                        request,
                        bytes: bytes.len(),
                    });
                    return Err(ReaderError::InvalidArchive(anyhow!(
                        "short {:?} read at offset {}: expected {} bytes, got {}",
                        request.kind,
                        request.offset,
                        request.length,
                        bytes.len()
                    )));
                }
                self.observer.observe(ReaderEvent::ReadCompleted {
                    request,
                    bytes: bytes.len(),
                });
                Ok(bytes)
            }
            Err(error) => {
                self.observer.observe(ReaderEvent::ReadFailed(request));
                Err(ReaderError::Source(error))
            }
        }
    }

    fn invalid_tile_id(&self, error: anyhow::Error) -> Self::Error {
        ReaderError::InvalidTileId(error)
    }

    fn invalid_archive(&self, error: anyhow::Error) -> Self::Error {
        ReaderError::InvalidArchive(error)
    }
}

fn validate_limits(limits: ReaderLimits) -> Result<(), ReaderConfigError> {
    if limits.bootstrap_bytes < MIN_BOOTSTRAP_BYTES {
        return Err(ReaderConfigError(format!(
            "bootstrap_bytes must be at least {MIN_BOOTSTRAP_BYTES}"
        )));
    }
    if limits.max_directory_depth == 0 {
        return Err(ReaderConfigError(
            "max_directory_depth must be greater than zero".into(),
        ));
    }
    if limits.max_range_bytes < limits.bootstrap_bytes {
        return Err(ReaderConfigError(
            "max_range_bytes must be at least bootstrap_bytes".into(),
        ));
    }
    if limits.max_decompressed_directory_bytes == 0 || limits.max_decompressed_metadata_bytes == 0 {
        return Err(ReaderConfigError(
            "decompressed-size limits must be greater than zero".into(),
        ));
    }
    Ok(())
}

/// Decodes an archive header and root directory from bootstrap bytes.
pub fn decode_bootstrap_bytes(
    body: Bytes,
    max_decompressed_directory_bytes: usize,
) -> AnyResult<ArchiveBootstrap> {
    if body.len() < HEADER_SIZE {
        bail!("PMTiles archive header is truncated");
    }
    let header =
        Header::parse(body.slice(..HEADER_SIZE)).context("failed to parse PMTiles header")?;
    let root_start =
        usize::try_from(header.root_offset).context("PMTiles root offset exceeds usize")?;
    let root_length =
        usize::try_from(header.root_length).context("PMTiles root length exceeds usize")?;
    let root_end = root_start
        .checked_add(root_length)
        .context("invalid PMTiles root directory range")?;
    if root_end > body.len() {
        bail!("PMTiles root directory must fit in the bootstrap read");
    }
    let root = Arc::new(
        Directory::parse_with_limit(
            header.internal_compression,
            body.slice(root_start..root_end),
            max_decompressed_directory_bytes,
        )
        .context("failed to parse PMTiles root directory")?,
    );
    ArchiveBootstrap::new(header, root, None)
}

/// Decompresses and parses a PMTiles metadata section.
pub fn decode_metadata_bytes(
    header: &Header,
    bytes: Bytes,
    max_decompressed_bytes: usize,
) -> AnyResult<Metadata> {
    let bytes =
        decompress_bytes_with_limit(header.internal_compression, bytes, max_decompressed_bytes)
            .context("failed to decompress PMTiles metadata")?;
    serde_json::from_slice(&bytes).context("failed to parse PMTiles metadata JSON")
}

/// Returns the checked exclusive end of the declared archive sections.
pub fn archive_len(header: &Header) -> AnyResult<u64> {
    let mut end = 0;
    for (name, offset, length) in [
        ("root directory", header.root_offset, header.root_length),
        ("metadata", header.metadata_offset, header.metadata_length),
        ("leaf directory", header.leaf_offset, header.leaf_length),
        ("tile data", header.data_offset, header.data_length),
    ] {
        let section_end = offset
            .checked_add(length)
            .with_context(|| format!("PMTiles {name} range overflows u64"))?;
        end = end.max(section_end);
    }
    Ok(end)
}

fn checked_section_offset(
    name: &str,
    section_offset: u64,
    section_length: u64,
    relative_offset: u64,
    length: u64,
) -> AnyResult<u64> {
    let relative_end = relative_offset
        .checked_add(length)
        .with_context(|| format!("PMTiles {name} entry range overflows u64"))?;
    if relative_end > section_length {
        bail!(
            "PMTiles {name} entry range {relative_offset}..{relative_end} exceeds section length {section_length}"
        );
    }
    section_offset
        .checked_add(relative_offset)
        .with_context(|| format!("PMTiles {name} absolute offset overflows u64"))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use bytes::{BufMut, Bytes, BytesMut};

    use super::*;

    #[derive(Clone)]
    struct MemorySource {
        bytes: Bytes,
        reads: Arc<Mutex<Vec<ReadRequest>>>,
    }

    impl RangeSource for MemorySource {
        type Error = io::Error;

        async fn read_range(&self, request: ReadRequest) -> Result<Bytes, Self::Error> {
            self.reads.lock().unwrap().push(request);
            let start = usize::try_from(request.offset).unwrap();
            let end = start.saturating_add(request.length).min(self.bytes.len());
            if start > end || start > self.bytes.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "range"));
            }
            Ok(self.bytes.slice(start..end))
        }
    }

    #[derive(Default)]
    struct TestStore {
        bootstraps: Mutex<HashMap<ArchiveIdentity, ArchiveBootstrap>>,
        leaves: Mutex<HashMap<LeafDirectoryKey, Arc<Directory>>>,
    }

    impl DirectoryStore for TestStore {
        fn get_bootstrap(&self, archive: &ArchiveIdentity) -> Option<ArchiveBootstrap> {
            self.bootstraps.lock().unwrap().get(archive).cloned()
        }

        fn put_bootstrap(&self, archive: &ArchiveIdentity, bootstrap: ArchiveBootstrap) {
            self.bootstraps
                .lock()
                .unwrap()
                .insert(archive.clone(), bootstrap);
        }

        fn get_leaf(&self, key: &LeafDirectoryKey) -> Option<Arc<Directory>> {
            self.leaves.lock().unwrap().get(key).cloned()
        }

        fn put_leaf(&self, key: LeafDirectoryKey, directory: Arc<Directory>) {
            self.leaves.lock().unwrap().insert(key, directory);
        }
    }

    #[derive(Default)]
    struct ParsedBackendCalls {
        bootstraps: AtomicUsize,
        metadata: AtomicUsize,
        leaves: Mutex<Vec<LeafDirectoryRequest>>,
        tile_reads: Mutex<Vec<ReadRequest>>,
    }

    struct ParsedBackend {
        bootstrap: ArchiveBootstrap,
        metadata: Arc<Metadata>,
        leaf: Option<Arc<Directory>>,
        tile: Bytes,
        calls: Arc<ParsedBackendCalls>,
    }

    impl ArchiveBackend for ParsedBackend {
        type Error = io::Error;

        async fn load_bootstrap(&self) -> Result<ArchiveBootstrap, Self::Error> {
            self.calls.bootstraps.fetch_add(1, Ordering::SeqCst);
            Ok(self.bootstrap.clone())
        }

        async fn load_metadata(&self) -> Result<Arc<Metadata>, Self::Error> {
            self.calls.metadata.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::clone(&self.metadata))
        }

        async fn load_leaf(
            &self,
            request: LeafDirectoryRequest,
        ) -> Result<Arc<Directory>, Self::Error> {
            self.calls.leaves.lock().unwrap().push(request);
            self.leaf.clone().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "test archive has no leaf directory",
                )
            })
        }

        async fn read_range(&self, request: ReadRequest) -> Result<Bytes, Self::Error> {
            self.calls.tile_reads.lock().unwrap().push(request);
            Ok(self.tile.clone())
        }

        fn invalid_tile_id(&self, error: anyhow::Error) -> Self::Error {
            io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
        }

        fn invalid_archive(&self, error: anyhow::Error) -> Self::Error {
            io::Error::new(io::ErrorKind::InvalidData, error.to_string())
        }
    }

    fn archive_bytes(root: &[u8], metadata: &[u8], leaf: &[u8], data: &[u8]) -> Bytes {
        let root_offset = HEADER_SIZE as u64;
        let metadata_offset = root_offset + root.len() as u64;
        let leaf_offset = metadata_offset + metadata.len() as u64;
        let data_offset = leaf_offset + leaf.len() as u64;
        let mut bytes = BytesMut::with_capacity(MIN_BOOTSTRAP_BYTES);
        bytes.extend_from_slice(b"PMTiles");
        bytes.put_u8(3);
        for value in [
            root_offset,
            root.len() as u64,
            metadata_offset,
            metadata.len() as u64,
            leaf_offset,
            leaf.len() as u64,
            data_offset,
            data.len() as u64,
            u64::from(!data.is_empty()),
            u64::from(!data.is_empty()),
            u64::from(!data.is_empty()),
        ] {
            bytes.put_u64_le(value);
        }
        bytes.put_u8(0); // clustered
        bytes.put_u8(1); // internal compression: none
        bytes.put_u8(1); // tile compression: none
        bytes.put_u8(1); // MVT
        bytes.put_u8(0); // min zoom
        bytes.put_u8(0); // max zoom
        for _ in 0..4 {
            bytes.put_i32_le(0);
        }
        bytes.put_u8(0);
        bytes.put_i32_le(0);
        bytes.put_i32_le(0);
        assert_eq!(bytes.len(), HEADER_SIZE);
        bytes.extend_from_slice(root);
        bytes.extend_from_slice(metadata);
        bytes.extend_from_slice(leaf);
        bytes.extend_from_slice(data);
        bytes.resize(MIN_BOOTSTRAP_BYTES, 0);
        bytes.freeze()
    }

    fn empty_archive() -> Bytes {
        archive_bytes(&[0], &[], &[], &[])
    }

    #[tokio::test]
    async fn parsed_backend_keeps_service_policy_outside_the_reader() {
        let archive = archive_bytes(&[1, 0, 1, 4, 1], &[], &[], b"tile");
        let bootstrap = decode_bootstrap_bytes(archive, DEFAULT_MAX_DECOMPRESSED_BYTES).unwrap();
        let calls = Arc::new(ParsedBackendCalls::default());
        let reader = ArchiveReader::with_backend(
            ParsedBackend {
                bootstrap,
                metadata: Arc::new(Metadata::default()),
                leaf: None,
                tile: Bytes::from_static(b"tile"),
                calls: Arc::clone(&calls),
            },
            ReaderLimits::default(),
        )
        .unwrap();

        let tile = reader.get_tile(0).await.unwrap().unwrap();
        assert_eq!(tile.bytes, Bytes::from_static(b"tile"));
        assert_eq!(calls.bootstraps.load(Ordering::SeqCst), 1);
        assert_eq!(calls.metadata.load(Ordering::SeqCst), 0);
        assert_eq!(calls.tile_reads.lock().unwrap().len(), 1);

        reader.metadata().await.unwrap();
        assert_eq!(calls.bootstraps.load(Ordering::SeqCst), 1);
        assert_eq!(calls.metadata.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn get_tile_enforces_max_range_bytes_before_backend_read() {
        // The tile entry declares length 20000 (varint `A0 9C 01`), which fits
        // the data section (so the section-length check passes) but exceeds the
        // configured `max_range_bytes`. `max_range_bytes` must be at least
        // `bootstrap_bytes` (validated), so cap it at the minimum.
        let data = vec![0u8; 20_000];
        let archive = archive_bytes(&[1, 0, 1, 0xA0, 0x9C, 0x01, 1], &[], &[], &data);
        let bootstrap = decode_bootstrap_bytes(archive, DEFAULT_MAX_DECOMPRESSED_BYTES).unwrap();
        let calls = Arc::new(ParsedBackendCalls::default());
        let reader = ArchiveReader::with_backend(
            ParsedBackend {
                bootstrap,
                metadata: Arc::new(Metadata::default()),
                leaf: None,
                tile: Bytes::from_static(b"tile"),
                calls: Arc::clone(&calls),
            },
            ReaderLimits {
                max_range_bytes: MIN_BOOTSTRAP_BYTES,
                ..ReaderLimits::default()
            },
        )
        .unwrap();

        let error = reader
            .get_tile(0)
            .await
            .err()
            .expect("oversized tile range must be rejected");
        assert!(
            error.to_string().contains("max_range_bytes"),
            "unexpected error: {error}"
        );
        // The reader rejects the oversized range before issuing the backend read,
        // so even a custom backend observes the configured limit.
        assert!(calls.tile_reads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn parsed_backend_receives_validated_leaf_ranges() {
        let root = [1, 0, 0, 5, 1];
        let leaf_bytes = Bytes::from_static(&[1, 0, 1, 4, 1]);
        let archive = archive_bytes(&root, &[], &leaf_bytes, b"leaf");
        let bootstrap = decode_bootstrap_bytes(archive, DEFAULT_MAX_DECOMPRESSED_BYTES).unwrap();
        let archive_len = bootstrap.archive_len();
        let calls = Arc::new(ParsedBackendCalls::default());
        let reader = ArchiveReader::with_backend(
            ParsedBackend {
                bootstrap,
                metadata: Arc::new(Metadata::default()),
                leaf: Some(Arc::new(
                    Directory::parse(Compression::None, leaf_bytes).unwrap(),
                )),
                tile: Bytes::from_static(b"leaf"),
                calls: Arc::clone(&calls),
            },
            ReaderLimits::default(),
        )
        .unwrap();

        let tile = reader.get_tile(0).await.unwrap().unwrap();

        assert_eq!(tile.bytes, Bytes::from_static(b"leaf"));
        let leaves = calls.leaves.lock().unwrap();
        assert_eq!(leaves.len(), 1);
        assert_eq!(leaves[0].offset, (HEADER_SIZE + root.len()) as u64);
        assert_eq!(leaves[0].length, 5);
        assert_eq!(leaves[0].compression, Compression::None);
        assert_eq!(leaves[0].archive_len, archive_len);
    }

    #[tokio::test]
    async fn injected_store_reuses_bootstrap_for_same_generation() {
        let reads = Arc::new(Mutex::new(Vec::new()));
        let reader = ArchiveReader::with_components(
            MemorySource {
                bytes: empty_archive(),
                reads: Arc::clone(&reads),
            },
            ArchiveIdentity::new("world.pmtiles", "etag-1").unwrap(),
            TestStore::default(),
            NoopObserver,
            ReaderLimits::default(),
        )
        .unwrap();

        assert_eq!(reader.header().await.unwrap().version, 3);
        assert_eq!(reader.header().await.unwrap().version, 3);
        assert_eq!(reads.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cache_identity_includes_archive_generation() {
        let store = Arc::new(TestStore::default());
        let reads = Arc::new(Mutex::new(Vec::new()));
        let first = ArchiveReader::with_components(
            MemorySource {
                bytes: empty_archive(),
                reads: Arc::clone(&reads),
            },
            ArchiveIdentity::new("world.pmtiles", "etag-1").unwrap(),
            Arc::clone(&store),
            NoopObserver,
            ReaderLimits::default(),
        )
        .unwrap();
        let second = ArchiveReader::with_components(
            MemorySource {
                bytes: empty_archive(),
                reads: Arc::clone(&reads),
            },
            ArchiveIdentity::new("world.pmtiles", "etag-2").unwrap(),
            store,
            NoopObserver,
            ReaderLimits::default(),
        )
        .unwrap();

        first.header().await.unwrap();
        second.header().await.unwrap();
        assert_eq!(reads.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn rejects_short_non_bootstrap_reads() {
        let mut archive = empty_archive().slice(..130).to_vec();
        archive[24..32].copy_from_slice(&128_u64.to_le_bytes());
        archive[32..40].copy_from_slice(&4_u64.to_le_bytes());
        let reader = ArchiveReader::new(
            MemorySource {
                bytes: Bytes::from(archive),
                reads: Arc::default(),
            },
            ArchiveIdentity::new("short.pmtiles", "etag").unwrap(),
        );

        let error = reader.metadata().await.unwrap_err();
        assert!(error.to_string().contains("expected 4 bytes, got 2"));
    }

    #[tokio::test]
    async fn reads_a_root_tile_and_reports_its_actual_access_trace() {
        // One entry: tile-id delta=0, run=1, length=4, offset=(0 + 1).
        let archive = archive_bytes(&[1, 0, 1, 4, 1], &[], &[], b"tile");
        let reads = Arc::new(Mutex::new(Vec::new()));
        let reader = ArchiveReader::with_components(
            MemorySource {
                bytes: archive,
                reads: Arc::clone(&reads),
            },
            ArchiveIdentity::new("root.pmtiles", "etag").unwrap(),
            TestStore::default(),
            NoopObserver,
            ReaderLimits::default(),
        )
        .unwrap();

        let trace = reader.lookup_with_trace(0).await.unwrap();
        assert!(trace.leaves.is_empty());
        let location = trace.tile.unwrap();
        assert_eq!(location.offset, (HEADER_SIZE + 5) as u64);
        assert_eq!(location.length, 4);

        let tile = reader.get_tile(0).await.unwrap().unwrap();
        assert_eq!(tile.bytes, Bytes::from_static(b"tile"));
        assert_eq!(tile.content_type, "application/vnd.mapbox-vector-tile");
        let requests = reads.lock().unwrap();
        assert_eq!(requests.len(), 2, "one bootstrap and one tile read");
        assert_eq!(requests[1].kind, ReadKind::Tile);
    }

    #[tokio::test]
    async fn missing_lookup_trace_retains_consulted_leaf_directories() {
        // Root points to a leaf whose only tile starts after the requested id.
        let archive = archive_bytes(&[1, 0, 0, 5, 1], &[], &[1, 1, 1, 4, 1], b"leaf");
        let reader = ArchiveReader::new(
            MemorySource {
                bytes: archive,
                reads: Arc::default(),
            },
            ArchiveIdentity::new("missing.pmtiles", "etag").unwrap(),
        );

        let trace = reader.lookup_with_trace(0).await.unwrap();

        assert!(trace.tile.is_none());
        assert_eq!(trace.leaves.len(), 1);
        assert_eq!(trace.leaves[0].length, 5);
        assert!(reader.get_tile(0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn traverses_and_caches_leaf_directories() {
        // Root entry points to a five-byte leaf; the leaf contains one tile.
        let archive = archive_bytes(&[1, 0, 0, 5, 1], &[], &[1, 0, 1, 4, 1], b"leaf");
        let reads = Arc::new(Mutex::new(Vec::new()));
        let reader = ArchiveReader::with_components(
            MemorySource {
                bytes: archive,
                reads: Arc::clone(&reads),
            },
            ArchiveIdentity::new("leaf.pmtiles", "etag").unwrap(),
            TestStore::default(),
            NoopObserver,
            ReaderLimits::default(),
        )
        .unwrap();

        let first = reader.lookup_with_trace(0).await.unwrap();
        let second = reader.lookup_with_trace(0).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(first.leaves.len(), 1);
        assert_eq!(first.leaves[0].offset, (HEADER_SIZE + 5) as u64);

        let tile = reader.get_tile(0).await.unwrap().unwrap();
        assert_eq!(tile.bytes, Bytes::from_static(b"leaf"));
        assert_eq!(
            reads.lock().unwrap().len(),
            3,
            "bootstrap and leaf should each be read once before the tile payload"
        );
    }

    #[test]
    fn directory_walker_rejects_entries_outside_the_declared_section() {
        let bootstrap = decode_bootstrap_bytes(empty_archive(), DEFAULT_MAX_DECOMPRESSED_BYTES)
            .expect("bootstrap");
        let mut walker = DirectoryWalker::new(
            bootstrap.header,
            TileId::new(0).unwrap(),
            DEFAULT_MAX_DIRECTORY_DEPTH,
        )
        .unwrap();
        let directory = Directory {
            entries: vec![crate::DirectoryEntry {
                tile_id: 0,
                offset: 0,
                length: 1,
                run_length: 1,
            }],
        };

        assert!(
            walker
                .step(&directory)
                .unwrap_err()
                .to_string()
                .contains("exceeds section length")
        );
    }

    #[test]
    fn directory_walker_bounds_cycles_by_depth() {
        let bootstrap = decode_bootstrap_bytes(
            archive_bytes(&[1, 0, 0, 5, 1], &[], &[1, 0, 1, 4, 1], b"tile"),
            DEFAULT_MAX_DECOMPRESSED_BYTES,
        )
        .expect("bootstrap");
        let mut walker =
            DirectoryWalker::new(bootstrap.header, TileId::new(0).unwrap(), 1).unwrap();

        assert!(matches!(
            walker.step(&bootstrap.root).unwrap(),
            DirectoryStep::Leaf(_)
        ));
        assert!(
            walker
                .step(&bootstrap.root)
                .unwrap_err()
                .to_string()
                .contains("depth")
        );
    }

    #[test]
    fn rejects_bootstrap_windows_smaller_than_the_pmtiles_contract() {
        let limits = ReaderLimits {
            bootstrap_bytes: MIN_BOOTSTRAP_BYTES - 1,
            ..ReaderLimits::default()
        };
        let result = ArchiveReader::with_components(
            MemorySource {
                bytes: Bytes::new(),
                reads: Arc::default(),
            },
            ArchiveIdentity::new("archive", "generation").unwrap(),
            NoDirectoryStore,
            NoopObserver,
            limits,
        );
        assert!(result.is_err());
    }

    #[test]
    fn archive_identity_requires_an_explicit_generation() {
        assert!(ArchiveIdentity::new("archive", "").is_err());
        assert!(ArchiveIdentity::new("", "etag").is_err());
    }

    #[tokio::test]
    async fn invalid_requested_tile_id_is_not_reported_as_archive_corruption() {
        let reader = ArchiveReader::new(
            MemorySource {
                bytes: empty_archive(),
                reads: Arc::default(),
            },
            ArchiveIdentity::new("archive", "etag").unwrap(),
        );
        assert!(matches!(
            reader.lookup_with_trace(u64::MAX).await,
            Err(ReaderError::InvalidTileId(_))
        ));
    }
}
