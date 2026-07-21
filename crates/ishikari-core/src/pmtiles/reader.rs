//! Distributed PMTiles orchestration over the service-independent reader core.
//!
//! PMTiles decoding, section validation, and directory traversal live in
//! `mmpf-pmtiles`; this adapter owns peer fallback, negative caching, and
//! cancellation-safe single-flight coordination keyed by `TilesetId`.

use std::{future::Future, hash::Hash, sync::Arc};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use mmpf_common::singleflight::{Flight, Follower, LeaderGuard, SingleFlight};
#[cfg(feature = "simulator-support")]
use mmpf_pmtiles::TileLookupTrace as TileAccessPlan;
use mmpf_pmtiles::{
    ArchiveBackend, ArchiveBootstrap, ArchiveReader as CoreArchiveReader, Compression,
    DEFAULT_MAX_DECOMPRESSED_BYTES, Directory, Header, LeafDirectoryRequest, MIN_BOOTSTRAP_BYTES,
    Metadata, ReadKind, ReadRequest, ReaderLimits, TileData, decode_bootstrap_bytes,
    decode_metadata_bytes,
};
use thiserror::Error;
use tracing::{debug, warn};

use crate::interned::TilesetId;

use super::cache::{ArchiveCache, LeafCacheKey};
#[cfg(any(test, feature = "simulator-support"))]
use super::cache::{DEFAULT_ARCHIVE_CACHE_MAX_BYTES, DEFAULT_LEAF_CACHE_MAX_BYTES};

/// Errors returned by PMTiles storage reads.
#[derive(Clone, Debug, Error)]
pub enum StorageError {
    #[error("archive not found")]
    NotFound,
    /// Process-wide backend work admission is saturated. Typed so service
    /// layers can shed with 503 and peer routing can try another owner.
    #[error("{0}")]
    Overloaded(String),
    /// A backend read timed out. Typed so the service layer maps it to a 504
    /// without matching on the message string.
    #[error("{0}")]
    Timeout(String),
    /// A backing object-store operation failed for a reason other than
    /// authoritative absence.
    #[error("{0}")]
    Backend(String),
    #[error("{0}")]
    Message(String),
}

#[derive(Debug, Error)]
pub(crate) enum LocalLeafError {
    #[error("invalid leaf range")]
    InvalidRange,
    #[error(transparent)]
    Reader(#[from] anyhow::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ValidatedLeafRange {
    offset: u64,
    length: usize,
}

impl ValidatedLeafRange {
    fn from_archive(
        archive: &ArchiveBootstrap,
        offset: u64,
        length: usize,
    ) -> Result<Self, InvalidLeafRange> {
        Self::new(
            archive.header.leaf_offset,
            archive.header.leaf_length,
            archive.archive_len(),
            offset,
            length,
        )
    }

    fn new(
        leaf_offset: u64,
        leaf_length: u64,
        archive_len: u64,
        offset: u64,
        length: usize,
    ) -> Result<Self, InvalidLeafRange> {
        let length_u64 = u64::try_from(length).map_err(|_| InvalidLeafRange)?;
        let end = offset.checked_add(length_u64).ok_or(InvalidLeafRange)?;
        let leaf_end = leaf_offset
            .checked_add(leaf_length)
            .ok_or(InvalidLeafRange)?;

        if length == 0
            || length > ReaderLimits::default().max_range_bytes
            || offset < leaf_offset
            || end > leaf_end
            || offset >= archive_len
            || end > archive_len
        {
            return Err(InvalidLeafRange);
        }

        Ok(Self { offset, length })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InvalidLeafRange;

/// Bootstrap bytes transferred from a peer, optionally including metadata.
pub struct BootstrapTransfer {
    pub bootstrap: Bytes,
    pub metadata: Option<Bytes>,
}

/// Storage capabilities required by the PMTiles reader.
pub trait Storage: Send + Sync {
    /// Reads a range of bytes for the given PMTiles archive.
    fn read_range<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        start: u64,
        length: usize,
        archive_len: Option<u64>,
    ) -> impl Future<Output = Result<Bytes, StorageError>> + Send + 'a;

    /// Fetches archive bootstrap bytes from a peer, optionally including metadata.
    fn fetch_bootstrap_bytes<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        include_metadata: bool,
    ) -> impl Future<Output = Result<Option<BootstrapTransfer>>> + Send + 'a;

    /// Fetches leaf bytes.
    fn fetch_leaf_bytes<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        offset: u64,
        length: usize,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send + 'a;
}

/// PMTiles archive reader backed by shared chunked range reads and index caches.
pub struct Reader<R> {
    archive_cache: ArchiveCache,
    storage: R,
    bootstrap_inflight: SingleFlight<TilesetId, ReaderFlightError>,
    metadata_inflight: SingleFlight<TilesetId, ReaderFlightError>,
    leaf_inflight: SingleFlight<LeafCacheKey, ReaderFlightError>,
}

/// One archive-bound view of Ishikari's multi-archive storage and index tier.
///
/// The shared PMTiles reader owns traversal and tile assembly. This backend
/// preserves Ishikari's existing peer-first bootstrap/leaf policy, decoded
/// caches, cancellation-safe single-flight, and backend attribution.
struct DistributedArchiveBackend<'a, S> {
    reader: &'a Arc<Reader<S>>,
    tileset_id: &'a TilesetId,
}

enum DistributedReaderError {
    ArchiveAbsent,
    Other(anyhow::Error),
}

/// Cloneable error snapshot shared with single-flight followers. Storage errors
/// remain typed because the service layer distinguishes backend, timeout, and
/// admission failures; other reader errors retain their complete printable context.
#[derive(Clone, Debug)]
struct ReaderFlightError {
    message: Arc<str>,
    storage: Option<StorageError>,
}

impl ReaderFlightError {
    fn capture(error: &anyhow::Error) -> Self {
        let storage = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<StorageError>())
            .cloned();
        let message = if storage.is_some() {
            error.to_string()
        } else {
            format!("{error:#}")
        };
        Self {
            message: Arc::from(message),
            storage,
        }
    }

    fn into_error(self) -> anyhow::Error {
        let Some(storage) = self.storage else {
            return anyhow!(self.message.to_string());
        };
        if self.message.as_ref() == storage.to_string() {
            anyhow::Error::new(storage)
        } else {
            anyhow::Error::new(storage).context(self.message.to_string())
        }
    }
}

fn complete_reader_flight<K, T>(
    guard: LeaderGuard<K, ReaderFlightError>,
    result: Result<T>,
) -> Result<T>
where
    K: Eq + Hash,
{
    if let Err(error) = &result {
        guard.complete_with_error(ReaderFlightError::capture(error));
    }
    result
}

async fn wait_reader_flight(follower: Follower<ReaderFlightError>) -> Result<()> {
    if let Some(error) = follower.wait().await {
        return Err(error.into_error());
    }
    Ok(())
}

impl<S> ArchiveBackend for DistributedArchiveBackend<'_, S>
where
    S: Storage,
{
    type Error = DistributedReaderError;

    async fn load_bootstrap(&self) -> Result<ArchiveBootstrap, Self::Error> {
        self.reader
            .load_bootstrap(self.tileset_id)
            .await
            .map_err(DistributedReaderError::Other)?
            .ok_or(DistributedReaderError::ArchiveAbsent)
    }

    async fn load_metadata(&self) -> Result<Arc<Metadata>, Self::Error> {
        self.reader
            .load_metadata(self.tileset_id)
            .await
            .map_err(DistributedReaderError::Other)?
            .ok_or(DistributedReaderError::ArchiveAbsent)
    }

    async fn load_leaf(
        &self,
        request: LeafDirectoryRequest,
    ) -> Result<Arc<Directory>, Self::Error> {
        self.reader
            .load_leaf_directory(
                self.tileset_id,
                request.offset,
                request.length as usize,
                request.compression,
                request.archive_len,
            )
            .await
            .map_err(DistributedReaderError::Other)
    }

    async fn read_range(&self, request: ReadRequest) -> Result<Bytes, Self::Error> {
        let bytes = self
            .reader
            .storage
            .read_range(
                self.tileset_id,
                request.offset,
                request.length,
                request.archive_len,
            )
            .await
            .with_context(|| match request.kind {
                ReadKind::Tile => "failed to read PMTiles tile bytes",
                ReadKind::Bootstrap => "failed to read archive bootstrap bytes",
                ReadKind::Metadata => "failed to read PMTiles metadata",
                ReadKind::LeafDirectory => "failed to read directory",
            })
            .map_err(DistributedReaderError::Other)?;

        if request.kind == ReadKind::Tile {
            tracing::debug!(
                tileset_id = %self.tileset_id,
                tile_offset = request.offset,
                tile_length = request.length,
                "resolved tile bytes"
            );
        }
        Ok(bytes)
    }

    fn invalid_tile_id(&self, error: anyhow::Error) -> Self::Error {
        DistributedReaderError::Other(error)
    }

    fn invalid_archive(&self, error: anyhow::Error) -> Self::Error {
        DistributedReaderError::Other(error)
    }
}

impl<S> Reader<S>
where
    S: Storage,
{
    #[cfg(any(test, feature = "simulator-support"))]
    pub fn new(storage: S) -> Result<Self> {
        Self::with_index_cache_capacities(
            storage,
            DEFAULT_ARCHIVE_CACHE_MAX_BYTES,
            DEFAULT_LEAF_CACHE_MAX_BYTES,
        )
    }

    /// Creates a reader with explicit archive-bootstrap and leaf-directory
    /// cache weight ceilings supplied by the composition root.
    pub fn with_index_cache_capacities(
        storage: S,
        archive_cache_max_bytes: u64,
        leaf_cache_max_bytes: u64,
    ) -> Result<Self> {
        Ok(Self {
            archive_cache: ArchiveCache::new(archive_cache_max_bytes, leaf_cache_max_bytes),
            storage,
            bootstrap_inflight: SingleFlight::default(),
            metadata_inflight: SingleFlight::default(),
            leaf_inflight: SingleFlight::default(),
        })
    }

    /// Returns a reference to the underlying storage implementation.
    pub(crate) fn storage(&self) -> &S {
        &self.storage
    }

    /// Returns weighted byte sizes for bootstrap and leaf-directory caches.
    pub(crate) fn index_cache_weighted_sizes(&self) -> (u64, u64) {
        self.archive_cache.weighted_sizes()
    }

    fn archive_reader<'a>(
        self: &'a Arc<Self>,
        tileset_id: &'a TilesetId,
    ) -> CoreArchiveReader<DistributedArchiveBackend<'a, S>> {
        CoreArchiveReader::with_backend(
            DistributedArchiveBackend {
                reader: self,
                tileset_id,
            },
            ReaderLimits::default(),
        )
        .expect("default PMTiles reader limits are valid")
    }

    /// Returns a tile by PMTiles tile id, fetching missing archive chunks as needed.
    pub(crate) async fn get_tile(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Option<TileData>> {
        match self.archive_reader(tileset_id).get_tile(tile_id).await {
            Ok(tile) => Ok(tile),
            Err(DistributedReaderError::ArchiveAbsent) => Ok(None),
            Err(DistributedReaderError::Other(error)) => Err(error),
        }
    }

    /// Returns the logical archive reads needed by the modeled simulator.
    #[cfg(feature = "simulator-support")]
    pub async fn plan_tile_access(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Option<TileAccessPlan>> {
        match self
            .archive_reader(tileset_id)
            .lookup_with_trace(tile_id)
            .await
        {
            Ok(plan) => Ok(Some(plan)),
            Err(DistributedReaderError::ArchiveAbsent) => Ok(None),
            Err(DistributedReaderError::Other(error)) => Err(error),
        }
    }

    /// Returns the parsed PMTiles header for a tileset.
    pub(crate) async fn header(self: &Arc<Self>, tileset_id: &TilesetId) -> Result<Option<Header>> {
        match self.archive_reader(tileset_id).header().await {
            Ok(header) => Ok(Some(header)),
            Err(DistributedReaderError::ArchiveAbsent) => Ok(None),
            Err(DistributedReaderError::Other(error)) => Err(error),
        }
    }

    /// Returns archive metadata if present.
    pub(crate) async fn metadata(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<Arc<Metadata>>> {
        match self.archive_reader(tileset_id).metadata().await {
            Ok(metadata) => Ok(Some(metadata)),
            Err(DistributedReaderError::ArchiveAbsent) => Ok(None),
            Err(DistributedReaderError::Other(error)) => Err(error),
        }
    }

    async fn load_metadata(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<Arc<Metadata>>> {
        loop {
            if let Some(archive) = self.archive_cache.get(tileset_id)
                && let Some(metadata) = archive.metadata
            {
                return Ok(Some(metadata));
            }
            if self.archive_cache.is_known_absent(tileset_id) {
                return Ok(None);
            }
            match self.metadata_inflight.begin(tileset_id.clone()) {
                Flight::Leader(guard) => {
                    let result = self.load_metadata_uncached(tileset_id).await;
                    return complete_reader_flight(guard, result);
                }
                Flight::Follower(follower) => {
                    wait_reader_flight(follower).await?;
                }
            }
        }
    }

    async fn load_metadata_uncached(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<Arc<Metadata>>> {
        // Metadata and header callers join the same bootstrap flight. The
        // leader asks a peer for metadata when possible; if a header-only
        // leader won the race, only the missing metadata section is read below.
        let Some(archive) = self.load_bootstrap_with_metadata(tileset_id, true).await? else {
            return Ok(None);
        };
        if let Some(metadata) = archive.metadata.clone() {
            return Ok(Some(metadata));
        }
        let metadata = Arc::new(
            self.load_metadata_from_backend(tileset_id, &archive)
                .await?,
        );
        self.archive_cache
            .put_metadata(tileset_id, metadata.clone());
        Ok(Some(metadata))
    }

    /// Loads a routed archive bootstrap, reusing a peer before falling back to backend reads.
    async fn load_bootstrap(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<ArchiveBootstrap>> {
        self.load_bootstrap_with_metadata(tileset_id, false).await
    }

    async fn load_bootstrap_with_metadata(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        include_metadata: bool,
    ) -> Result<Option<ArchiveBootstrap>> {
        loop {
            if let Some(archive) = self.archive_cache.get(tileset_id) {
                return Ok(Some(archive));
            }
            // Skip the peer forward and backend read for an archive we just found
            // absent (short TTL), so enumerating a missing tileset cannot amplify
            // into unbounded peer round-trips and object-store probes.
            if self.archive_cache.is_known_absent(tileset_id) {
                return Ok(None);
            }
            match self.bootstrap_inflight.begin(tileset_id.clone()) {
                Flight::Leader(guard) => {
                    let result = self
                        .load_bootstrap_uncached(tileset_id, include_metadata)
                        .await;
                    return complete_reader_flight(guard, result);
                }
                Flight::Follower(follower) => {
                    wait_reader_flight(follower).await?;
                }
            }
        }
    }

    async fn load_bootstrap_uncached(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        include_metadata: bool,
    ) -> Result<Option<ArchiveBootstrap>> {
        match self
            .storage
            .fetch_bootstrap_bytes(tileset_id, include_metadata)
            .await
        {
            Ok(Some(transfer)) => {
                let bootstrap = transfer.bootstrap;
                let mut archive =
                    decode_bootstrap_bytes(bootstrap.clone(), DEFAULT_MAX_DECOMPRESSED_BYTES)
                        .context("failed to decode bootstrap from peer")?;
                if let Some(metadata_bytes) = transfer.metadata {
                    archive.metadata = Some(Arc::new(
                        decode_metadata_bytes(
                            &archive.header,
                            metadata_bytes,
                            DEFAULT_MAX_DECOMPRESSED_BYTES,
                        )
                        .context("failed to decode metadata from peer")?,
                    ));
                }
                self.archive_cache
                    .put(tileset_id, archive.clone(), bootstrap);
                return Ok(Some(archive));
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    tileset_id = %tileset_id,
                    error = %error,
                    "bootstrap forward failed; falling back"
                );
            }
        }

        self.load_bootstrap_local_uncached(tileset_id).await
    }

    /// Loads or reuses the cached header/root bootstrap from local backend storage.
    pub(crate) async fn load_bootstrap_local(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<ArchiveBootstrap>> {
        loop {
            if let Some(archive) = self.archive_cache.get(tileset_id) {
                return Ok(Some(archive));
            }
            if self.archive_cache.is_known_absent(tileset_id) {
                return Ok(None);
            }
            match self.bootstrap_inflight.begin(tileset_id.clone()) {
                Flight::Leader(guard) => {
                    let result = self.load_bootstrap_local_uncached(tileset_id).await;
                    return complete_reader_flight(guard, result);
                }
                Flight::Follower(follower) => {
                    wait_reader_flight(follower).await?;
                }
            }
        }
    }

    async fn load_bootstrap_local_uncached(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<ArchiveBootstrap>> {
        let initial_bytes = match self
            .storage
            .read_range(tileset_id, 0, MIN_BOOTSTRAP_BYTES, None)
            .await
        {
            Ok(bytes) => bytes,
            // Authoritative absence (shared object storage): remember it briefly
            // so repeat/burst lookups for this missing archive don't re-probe.
            Err(StorageError::NotFound) => {
                self.archive_cache.mark_absent(tileset_id);
                return Ok(None);
            }
            Err(error) => return Err(error).context("failed to read PMTiles header"),
        };

        let archive =
            decode_bootstrap_bytes(initial_bytes.clone(), DEFAULT_MAX_DECOMPRESSED_BYTES)?;
        let header = archive.header;
        debug!(
            tileset_id = %tileset_id,
            version = header.version,
            root_offset = header.root_offset,
            root_length = header.root_length,
            metadata_offset = header.metadata_offset,
            metadata_length = header.metadata_length,
            leaf_offset = header.leaf_offset,
            leaf_length = header.leaf_length,
            data_offset = header.data_offset,
            data_length = header.data_length,
            "parsed PMTiles header"
        );
        self.archive_cache
            .put(tileset_id, archive.clone(), initial_bytes);

        Ok(Some(archive))
    }

    /// Loads local raw bootstrap bytes for internal forwarding, optionally including metadata.
    pub(crate) async fn load_bootstrap_bytes_local(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        include_metadata: bool,
    ) -> Result<Option<BootstrapTransfer>> {
        let Some(archive) = self.load_bootstrap_local(tileset_id).await? else {
            return Ok(None);
        };
        let end = archive.archive_len();
        let bootstrap_length = usize::try_from(end.min(MIN_BOOTSTRAP_BYTES as u64))
            .context("PMTiles bootstrap length exceeds usize")?;

        let bootstrap = if let Some(bytes) = self.archive_cache.get_bootstrap_bytes(tileset_id) {
            bytes
        } else {
            self.storage
                .read_range(tileset_id, 0, bootstrap_length, Some(end))
                .await
                .context("failed to read archive bootstrap bytes")?
        };

        let metadata = if include_metadata && archive.header.metadata_length > 0 {
            Some(
                self.storage
                    .read_range(
                        tileset_id,
                        archive.header.metadata_offset,
                        usize::try_from(archive.header.metadata_length)
                            .context("PMTiles metadata length exceeds usize")?,
                        Some(end),
                    )
                    .await
                    .context("failed to read PMTiles metadata")?,
            )
        } else {
            None
        };

        Ok(Some(BootstrapTransfer {
            bootstrap,
            metadata,
        }))
    }

    /// Loads a routed leaf directory from the tileset owner, falling back to local backend reads.
    async fn load_leaf_directory(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        offset: u64,
        length: usize,
        compression: Compression,
        archive_len: u64,
    ) -> Result<Arc<Directory>> {
        let leaf_key = LeafCacheKey::new(tileset_id, offset, length);
        loop {
            if let Some(directory) = self.archive_cache.get_leaf(&leaf_key) {
                return Ok(directory);
            }
            match self.leaf_inflight.begin(leaf_key.clone()) {
                Flight::Leader(guard) => {
                    let result = self
                        .load_leaf_directory_uncached(
                            tileset_id,
                            leaf_key,
                            offset,
                            length,
                            compression,
                            archive_len,
                        )
                        .await;
                    return complete_reader_flight(guard, result);
                }
                Flight::Follower(follower) => {
                    wait_reader_flight(follower).await?;
                }
            }
        }
    }

    async fn load_leaf_directory_uncached(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        leaf_key: LeafCacheKey,
        offset: u64,
        length: usize,
        compression: Compression,
        archive_len: u64,
    ) -> Result<Arc<Directory>> {
        match self
            .storage
            .fetch_leaf_bytes(tileset_id, offset, length)
            .await
        {
            Ok(Some(body)) => {
                let directory = Directory::parse(compression, body)
                    .context("failed to decode leaf directory from peer")?;
                let directory = Arc::new(directory);
                self.archive_cache
                    .put_leaf(leaf_key.clone(), directory.clone());
                return Ok(directory);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    tileset_id = %tileset_id,
                    offset = offset,
                    error = %error,
                    "leaf forward failed; falling back"
                );
            }
        }

        let directory = Arc::new(
            self.read_directory_from_backend(tileset_id, offset, length, compression, archive_len)
                .await?,
        );
        self.archive_cache.put_leaf(leaf_key, directory.clone());
        Ok(directory)
    }

    /// Loads raw PMTiles leaf bytes from local storage for internal requests.
    pub(crate) async fn load_leaf_bytes_local(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        offset: u64,
        length: usize,
    ) -> std::result::Result<Option<Bytes>, LocalLeafError> {
        let Some(archive) = self.load_bootstrap_local(tileset_id).await? else {
            return Ok(None);
        };
        let range = ValidatedLeafRange::from_archive(&archive, offset, length)
            .map_err(|_| LocalLeafError::InvalidRange)?;
        let leaf = self
            .storage
            .read_range(
                tileset_id,
                range.offset,
                range.length,
                Some(archive.archive_len()),
            )
            .await
            .context("failed to read leaf bytes")?;
        Ok(Some(leaf))
    }

    /// Reads and decodes a PMTiles directory block from local backend storage.
    async fn read_directory_from_backend(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        offset: u64,
        length: usize,
        compression: Compression,
        archive_len: u64,
    ) -> Result<Directory> {
        let bytes = self
            .storage
            .read_range(tileset_id, offset, length, Some(archive_len))
            .await
            .context("failed to read directory")?;
        Directory::parse(compression, bytes)
    }

    /// Loads and decodes the metadata section for a tileset from backend storage.
    async fn load_metadata_from_backend(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        archive: &ArchiveBootstrap,
    ) -> Result<Metadata> {
        if archive.header.metadata_length == 0 {
            return Ok(Metadata::default());
        }
        let bytes = self
            .storage
            .read_range(
                tileset_id,
                archive.header.metadata_offset,
                usize::try_from(archive.header.metadata_length)
                    .context("PMTiles metadata length exceeds usize")?,
                Some(archive.archive_len()),
            )
            .await
            .context("failed to read PMTiles metadata")?;
        decode_metadata_bytes(&archive.header, bytes, DEFAULT_MAX_DECOMPRESSED_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::Result;
    use bytes::{BufMut, Bytes, BytesMut};
    use mmpf_pmtiles::{
        DEFAULT_MAX_DECOMPRESSED_BYTES, HEADER_SIZE, MIN_BOOTSTRAP_BYTES, ReaderLimits,
    };

    use super::{
        BootstrapTransfer, InvalidLeafRange, Reader, Storage, StorageError, ValidatedLeafRange,
    };
    use crate::interned::TilesetId;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct RecordedRead {
        start: u64,
        length: usize,
        archive_len: Option<u64>,
    }

    struct RecordingStorage {
        archive: Bytes,
        peer_bootstrap: bool,
        peer_metadata: Option<Bytes>,
        bootstrap_delay: Duration,
        reads: Arc<Mutex<Vec<RecordedRead>>>,
        bootstrap_fetches: Arc<Mutex<Vec<bool>>>,
    }

    impl Storage for RecordingStorage {
        async fn read_range<'a>(
            &'a self,
            _tileset_id: &'a TilesetId,
            start: u64,
            length: usize,
            archive_len: Option<u64>,
        ) -> Result<Bytes, StorageError> {
            self.reads.lock().unwrap().push(RecordedRead {
                start,
                length,
                archive_len,
            });
            let start = usize::try_from(start)
                .map_err(|_| StorageError::Message("range start exceeds usize".into()))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| StorageError::Message("range end overflows usize".into()))?;
            if end > self.archive.len() {
                return Err(StorageError::Message("range exceeds archive".into()));
            }
            Ok(self.archive.slice(start..end))
        }

        async fn fetch_bootstrap_bytes<'a>(
            &'a self,
            _tileset_id: &'a TilesetId,
            include_metadata: bool,
        ) -> Result<Option<BootstrapTransfer>> {
            self.bootstrap_fetches
                .lock()
                .unwrap()
                .push(include_metadata);
            tokio::time::sleep(self.bootstrap_delay).await;
            Ok(self.peer_bootstrap.then(|| BootstrapTransfer {
                bootstrap: self.archive.clone(),
                metadata: include_metadata
                    .then(|| self.peer_metadata.clone())
                    .flatten(),
            }))
        }

        fn fetch_leaf_bytes<'a>(
            &'a self,
            _tileset_id: &'a TilesetId,
            _offset: u64,
            _length: usize,
        ) -> impl Future<Output = Result<Option<Bytes>>> + Send + 'a {
            std::future::ready(Ok(None))
        }
    }

    fn archive_bytes(root: &[u8], metadata: &[u8], data: &[u8]) -> Bytes {
        archive_bytes_with_leaf(root, metadata, &[], data)
    }

    fn archive_bytes_with_leaf(root: &[u8], metadata: &[u8], leaf: &[u8], data: &[u8]) -> Bytes {
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

    fn short_archive_bytes(root: &[u8], metadata: &[u8], data: &[u8]) -> Bytes {
        let logical_length = HEADER_SIZE + root.len() + metadata.len() + data.len();
        archive_bytes(root, metadata, data).slice(..logical_length)
    }

    #[tokio::test]
    async fn local_bootstrap_transfer_reuses_cached_raw_bytes() {
        let root = [1, 0, 1, 4, 1];
        let archive = short_archive_bytes(&root, &[], b"tile");
        assert!(archive.len() < MIN_BOOTSTRAP_BYTES);
        let reads = Arc::new(Mutex::new(Vec::new()));
        let bootstrap_fetches = Arc::new(Mutex::new(Vec::new()));
        let reader = Arc::new(
            Reader::new(RecordingStorage {
                archive: archive.clone(),
                peer_bootstrap: false,
                peer_metadata: None,
                bootstrap_delay: Duration::ZERO,
                reads: Arc::clone(&reads),
                bootstrap_fetches,
            })
            .unwrap(),
        );
        let tileset_id = TilesetId::try_new("short").unwrap();
        let bootstrap =
            mmpf_pmtiles::decode_bootstrap_bytes(archive.clone(), DEFAULT_MAX_DECOMPRESSED_BYTES)
                .expect("valid short bootstrap");
        reader
            .archive_cache
            .put(&tileset_id, bootstrap, archive.clone());

        let transfer = reader
            .load_bootstrap_bytes_local(&tileset_id, false)
            .await
            .expect("short bootstrap read")
            .expect("archive exists");

        assert_eq!(transfer.bootstrap, archive);
        assert!(reads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cold_local_bootstrap_transfer_reads_backend_once() {
        let root = [1, 0, 1, 4, 1];
        let archive = archive_bytes(&root, &[], b"tile");
        let reads = Arc::new(Mutex::new(Vec::new()));
        let reader = Arc::new(
            Reader::new(RecordingStorage {
                archive: archive.clone(),
                peer_bootstrap: false,
                peer_metadata: None,
                bootstrap_delay: Duration::ZERO,
                reads: Arc::clone(&reads),
                bootstrap_fetches: Arc::new(Mutex::new(Vec::new())),
            })
            .unwrap(),
        );
        let tileset_id = TilesetId::try_new("cold").unwrap();

        let transfer = reader
            .load_bootstrap_bytes_local(&tileset_id, false)
            .await
            .expect("bootstrap load")
            .expect("archive exists");

        assert_eq!(transfer.bootstrap, archive);
        assert_eq!(
            reads.lock().unwrap().as_slice(),
            &[RecordedRead {
                start: 0,
                length: MIN_BOOTSTRAP_BYTES,
                archive_len: None,
            }]
        );
    }

    #[test]
    fn leaf_range_accepts_a_range_wholly_inside_the_leaf_section() {
        assert_eq!(
            ValidatedLeafRange::new(100, 50, 200, 110, 20),
            Ok(ValidatedLeafRange {
                offset: 110,
                length: 20,
            })
        );
    }

    #[test]
    fn leaf_range_rejects_section_end_archive_end_overflow_and_oversize() {
        let oversized = ReaderLimits::default().max_range_bytes + 1;
        for result in [
            ValidatedLeafRange::new(100, 50, 200, 149, 2),
            ValidatedLeafRange::new(100, 150, 200, 199, 2),
            ValidatedLeafRange::new(0, u64::MAX, u64::MAX, u64::MAX, 1),
            ValidatedLeafRange::new(100, 50, 200, 100, 0),
            ValidatedLeafRange::new(0, oversized as u64, oversized as u64, 0, oversized),
        ] {
            assert_eq!(result, Err(InvalidLeafRange));
        }
    }

    #[tokio::test]
    async fn local_leaf_load_reuses_cached_decoded_bootstrap_without_bootstrap_read() {
        let root = [0];
        let leaf = b"leaf";
        let archive = archive_bytes_with_leaf(&root, &[], leaf, &[]);
        let decoded =
            mmpf_pmtiles::decode_bootstrap_bytes(archive.clone(), DEFAULT_MAX_DECOMPRESSED_BYTES)
                .expect("valid bootstrap");
        let leaf_offset = decoded.header.leaf_offset;
        let archive_len = decoded.archive_len();
        let reads = Arc::new(Mutex::new(Vec::new()));
        let reader = Arc::new(
            Reader::new(RecordingStorage {
                archive: archive.clone(),
                peer_bootstrap: false,
                peer_metadata: None,
                bootstrap_delay: Duration::ZERO,
                reads: Arc::clone(&reads),
                bootstrap_fetches: Arc::new(Mutex::new(Vec::new())),
            })
            .unwrap(),
        );
        let tileset_id = TilesetId::try_new("cached-leaf").unwrap();
        reader
            .archive_cache
            .put(&tileset_id, decoded, archive.clone());

        let bytes = reader
            .load_leaf_bytes_local(&tileset_id, leaf_offset, leaf.len())
            .await
            .expect("leaf load")
            .expect("archive exists");

        assert_eq!(bytes, Bytes::from_static(leaf));
        assert_eq!(
            reads.lock().unwrap().as_slice(),
            &[RecordedRead {
                start: leaf_offset,
                length: leaf.len(),
                archive_len: Some(archive_len),
            }]
        );
    }

    #[tokio::test]
    async fn shared_reader_preserves_peer_first_tile_access() {
        // One root entry: tile-id delta=0, run=1, length=4, offset=(0 + 1).
        let root = [1, 0, 1, 4, 1];
        let archive = archive_bytes(&root, &[], b"tile");
        let reads = Arc::new(Mutex::new(Vec::new()));
        let bootstrap_fetches = Arc::new(Mutex::new(Vec::new()));
        let reader = Arc::new(
            Reader::new(RecordingStorage {
                archive,
                peer_bootstrap: true,
                peer_metadata: None,
                bootstrap_delay: Duration::ZERO,
                reads: Arc::clone(&reads),
                bootstrap_fetches: Arc::clone(&bootstrap_fetches),
            })
            .unwrap(),
        );
        let tileset_id = TilesetId::try_new("peer/archive").unwrap();

        let tile = reader.get_tile(&tileset_id, 0).await.unwrap().unwrap();

        assert_eq!(tile.bytes, Bytes::from_static(b"tile"));
        assert_eq!(tile.content_type, "application/vnd.mapbox-vector-tile");
        assert_eq!(*bootstrap_fetches.lock().unwrap(), vec![false]);
        assert_eq!(
            *reads.lock().unwrap(),
            vec![RecordedRead {
                start: (HEADER_SIZE + root.len()) as u64,
                length: 4,
                archive_len: Some((HEADER_SIZE + root.len() + 4) as u64),
            }]
        );
    }

    #[tokio::test]
    async fn shared_reader_preserves_combined_peer_metadata_transfer() {
        let metadata = Bytes::from_static(br#"{"name":"world"}"#);
        let archive = archive_bytes(&[0], &metadata, &[]);
        let reads = Arc::new(Mutex::new(Vec::new()));
        let bootstrap_fetches = Arc::new(Mutex::new(Vec::new()));
        let reader = Arc::new(
            Reader::new(RecordingStorage {
                archive,
                peer_bootstrap: true,
                peer_metadata: Some(metadata),
                bootstrap_delay: Duration::ZERO,
                reads: Arc::clone(&reads),
                bootstrap_fetches: Arc::clone(&bootstrap_fetches),
            })
            .unwrap(),
        );
        let tileset_id = TilesetId::try_new("peer/metadata").unwrap();

        let metadata = reader.metadata(&tileset_id).await.unwrap().unwrap();

        assert_eq!(metadata.name.as_deref(), Some("world"));
        assert_eq!(*bootstrap_fetches.lock().unwrap(), vec![true]);
        assert!(reads.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn concurrent_header_and_metadata_share_the_bootstrap_flight() {
        let metadata_bytes = Bytes::from_static(br#"{"name":"world"}"#);
        let archive = archive_bytes(&[0], &metadata_bytes, &[]);
        let reads = Arc::new(Mutex::new(Vec::new()));
        let bootstrap_fetches = Arc::new(Mutex::new(Vec::new()));
        let reader = Arc::new(
            Reader::new(RecordingStorage {
                archive,
                peer_bootstrap: true,
                peer_metadata: Some(metadata_bytes.clone()),
                bootstrap_delay: Duration::from_millis(20),
                reads: Arc::clone(&reads),
                bootstrap_fetches: Arc::clone(&bootstrap_fetches),
            })
            .unwrap(),
        );
        let tileset_id = TilesetId::try_new("peer/concurrent-metadata").unwrap();

        // `join!` polls the header first, so its header-only peer request becomes
        // leader; metadata must join that flight and fetch only its own section.
        let (header, metadata) =
            tokio::join!(reader.header(&tileset_id), reader.metadata(&tileset_id));

        assert!(header.unwrap().is_some());
        assert_eq!(metadata.unwrap().unwrap().name.as_deref(), Some("world"));
        assert_eq!(*bootstrap_fetches.lock().unwrap(), vec![false]);
        assert_eq!(
            *reads.lock().unwrap(),
            vec![RecordedRead {
                start: (HEADER_SIZE + 1) as u64,
                length: metadata_bytes.len(),
                archive_len: Some((HEADER_SIZE + 1 + metadata_bytes.len()) as u64),
            }]
        );
    }

    struct SlowFailingStorage {
        reads: Arc<AtomicUsize>,
    }

    impl Storage for SlowFailingStorage {
        async fn read_range<'a>(
            &'a self,
            _tileset_id: &'a TilesetId,
            _start: u64,
            _length: usize,
            _archive_len: Option<u64>,
        ) -> Result<Bytes, StorageError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(StorageError::Timeout("injected timeout".into()))
        }

        fn fetch_bootstrap_bytes<'a>(
            &'a self,
            _tileset_id: &'a TilesetId,
            _include_metadata: bool,
        ) -> impl Future<Output = Result<Option<BootstrapTransfer>>> + Send + 'a {
            std::future::ready(Ok(None))
        }

        fn fetch_leaf_bytes<'a>(
            &'a self,
            _tileset_id: &'a TilesetId,
            _offset: u64,
            _length: usize,
        ) -> impl Future<Output = Result<Option<Bytes>>> + Send + 'a {
            std::future::ready(Ok(None))
        }
    }

    #[tokio::test]
    async fn concurrent_bootstrap_failures_share_one_backend_attempt() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(
            Reader::new(SlowFailingStorage {
                reads: Arc::clone(&reads),
            })
            .expect("reader"),
        );
        let tileset_id = TilesetId::try_new("failing/archive").unwrap();

        let (first, second) = tokio::join!(
            reader.load_bootstrap_local(&tileset_id),
            reader.load_bootstrap_local(&tileset_id)
        );

        let first_error = match first {
            Err(error) => error,
            Ok(_) => panic!("first request unexpectedly succeeded"),
        };
        let second_error = match second {
            Err(error) => error,
            Ok(_) => panic!("second request unexpectedly succeeded"),
        };
        assert!(format!("{first_error:#}").contains("injected timeout"));
        assert!(format!("{second_error:#}").contains("injected timeout"));
        assert!(matches!(
            first_error.downcast_ref::<StorageError>(),
            Some(StorageError::Timeout(_))
        ));
        assert!(matches!(
            second_error.downcast_ref::<StorageError>(),
            Some(StorageError::Timeout(_))
        ));
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }
}
