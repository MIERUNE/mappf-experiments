//! Stable identity for one observed object-store generation.

use std::sync::Arc;

use object_store::{GetOptions, ObjectMeta};
use thiserror::Error;

use crate::interned::TilesetId;

const MAX_VALIDATOR_BYTES: usize = 1_024;

/// Strong validator that pins all reads participating in one archive lookup.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ArchiveGeneration {
    kind: ArchiveGenerationKind,
    value: Arc<str>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ArchiveGenerationKind {
    Version,
    Etag,
}

/// Complete cache identity for one immutable observation of a logical archive.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[doc(hidden)]
pub struct ArchiveKey {
    pub(crate) tileset_id: TilesetId,
    pub(crate) generation: ArchiveGeneration,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[doc(hidden)]
pub enum ArchiveGenerationError {
    #[error("object store did not return a version or strong ETag")]
    MissingValidator,
    #[error("object-store validator is not a bounded strong HTTP value")]
    InvalidValidator,
    #[error("invalid archive-generation wire value")]
    InvalidWireValue,
}

impl ArchiveGeneration {
    pub(crate) fn from_meta(meta: &ObjectMeta) -> Result<Self, ArchiveGenerationError> {
        if let Some(version) = meta.version.as_deref() {
            return Self::new(ArchiveGenerationKind::Version, version);
        }
        if let Some(etag) = meta.e_tag.as_deref() {
            return Self::new(ArchiveGenerationKind::Etag, etag);
        }
        Err(ArchiveGenerationError::MissingValidator)
    }

    fn new(
        kind: ArchiveGenerationKind,
        value: impl Into<Arc<str>>,
    ) -> Result<Self, ArchiveGenerationError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_VALIDATOR_BYTES
            || (kind == ArchiveGenerationKind::Etag && value.starts_with("W/"))
            || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(ArchiveGenerationError::InvalidValidator);
        }
        Ok(Self { kind, value })
    }

    pub(crate) fn apply_to_get(&self, options: GetOptions) -> GetOptions {
        match self.kind {
            ArchiveGenerationKind::Version => options.with_version(Some(self.value.as_ref())),
            ArchiveGenerationKind::Etag => options.with_if_match(Some(self.value.as_ref())),
        }
    }

    #[doc(hidden)]
    pub fn to_wire(&self) -> String {
        let prefix = match self.kind {
            ArchiveGenerationKind::Version => "v:",
            ArchiveGenerationKind::Etag => "e:",
        };
        format!("{prefix}{}", self.value)
    }

    #[doc(hidden)]
    pub fn from_wire(value: &str) -> Result<Self, ArchiveGenerationError> {
        let (kind, value) = if let Some(value) = value.strip_prefix("v:") {
            (ArchiveGenerationKind::Version, value)
        } else if let Some(value) = value.strip_prefix("e:") {
            (ArchiveGenerationKind::Etag, value)
        } else {
            return Err(ArchiveGenerationError::InvalidWireValue);
        };
        Self::new(kind, value)
    }
}

impl ArchiveKey {
    #[doc(hidden)]
    pub fn new(tileset_id: &TilesetId, generation: ArchiveGeneration) -> Self {
        Self {
            tileset_id: tileset_id.clone(),
            generation,
        }
    }

    #[doc(hidden)]
    pub fn tileset_id(&self) -> &TilesetId {
        &self.tileset_id
    }
}

#[cfg(test)]
mod tests {
    use object_store::{GetOptions, ObjectMeta, path::Path};

    use super::{ArchiveGeneration, ArchiveGenerationError};

    fn metadata(version: Option<&str>, etag: Option<&str>) -> ObjectMeta {
        ObjectMeta {
            location: Path::from("archive.pmtiles"),
            last_modified: std::time::SystemTime::UNIX_EPOCH.into(),
            size: 42,
            e_tag: etag.map(str::to_owned),
            version: version.map(str::to_owned),
        }
    }

    #[test]
    fn object_version_takes_precedence_over_etag() {
        let generation =
            ArchiveGeneration::from_meta(&metadata(Some("123"), Some("etag"))).unwrap();
        assert_eq!(generation.to_wire(), "v:123");
    }

    #[test]
    fn etag_is_used_when_the_store_has_no_version() {
        let generation = ArchiveGeneration::from_meta(&metadata(None, Some("etag"))).unwrap();
        assert_eq!(generation.to_wire(), "e:etag");
        assert_eq!(ArchiveGeneration::from_wire("e:etag").unwrap(), generation);
    }

    /// A version pins with `GetOptions::version`, which the GCS backend sends as
    /// `?generation=`. Without this the read is unpinned and only the post-read
    /// validator comparison catches a replacement — correct, but after paying for
    /// the request.
    #[test]
    fn a_version_pins_the_read_by_object_version() {
        let generation =
            ArchiveGeneration::from_meta(&metadata(Some("123"), Some("etag"))).unwrap();
        let options = generation.apply_to_get(GetOptions::new());
        assert_eq!(options.version.as_deref(), Some("123"));
        assert_eq!(options.if_match, None);
    }

    /// Without an object version the read pins with `If-Match`, so a replaced
    /// object answers `412` instead of returning the new bytes.
    #[test]
    fn an_etag_pins_the_read_with_if_match() {
        let generation = ArchiveGeneration::from_meta(&metadata(None, Some("\"abc\""))).unwrap();
        let options = generation.apply_to_get(GetOptions::new());
        assert_eq!(options.if_match.as_deref(), Some("\"abc\""));
        assert_eq!(options.version, None);
    }

    /// Pinning must not disturb the range the caller asked for.
    #[test]
    fn pinning_preserves_the_requested_range() {
        let generation = ArchiveGeneration::from_meta(&metadata(Some("123"), None)).unwrap();
        let options = generation.apply_to_get(GetOptions::new().with_range(Some(10..20)));
        assert_eq!(options.version.as_deref(), Some("123"));
        assert!(options.range.is_some());
    }

    #[test]
    fn missing_or_malformed_validators_are_rejected() {
        assert_eq!(
            ArchiveGeneration::from_meta(&metadata(None, None)),
            Err(ArchiveGenerationError::MissingValidator)
        );
        assert!(ArchiveGeneration::from_wire("unknown:value").is_err());
        assert!(ArchiveGeneration::from_wire("e:").is_err());
        assert!(ArchiveGeneration::from_wire("e:W/\"weak\"").is_err());
        assert!(ArchiveGeneration::from_wire("v:contains space").is_err());
    }
}
