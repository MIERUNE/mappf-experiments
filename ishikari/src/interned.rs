//! Interned string types for shared identifiers.

use std::{fmt, ops::Deref};

use anyhow::{Result, bail};
use internment::ArcIntern;

/// Upper bound on a tileset id, so enumeration cannot bloat interned-string
/// storage, cache keys, or log lines. Generous versus real object-store keys.
const MAX_TILESET_ID_LEN: usize = 256;

/// Validated, interned tileset identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TilesetId(ArcIntern<str>);

impl TilesetId {
    /// Creates a tileset id after validating it.
    pub fn try_new(value: &str) -> Result<Self> {
        validate_tileset_id(value)?;
        Ok(Self(ArcIntern::from(value)))
    }

    /// Creates a tileset id without validation (for internal/test use).
    pub fn new_unchecked(value: &str) -> Self {
        Self(ArcIntern::from(value))
    }

    /// Returns the tileset id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for TilesetId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for TilesetId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for TilesetId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<String> for TilesetId {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        Self::try_new(&value)
    }
}

/// Validates a tileset identifier.
///
/// Accepts a flat key `id` or a single-namespace key `namespace/id`. Each
/// segment must be non-empty, use only `[A-Za-z0-9._-]`, and not be a bare
/// `.` or `..`. The key maps directly to an object-store path, so traversal
/// and empty segments are rejected.
fn validate_tileset_id(tileset_id: &str) -> Result<()> {
    if tileset_id.is_empty() {
        bail!("tileset_id must not be empty");
    }
    if tileset_id.len() > MAX_TILESET_ID_LEN {
        bail!("tileset_id must be at most {MAX_TILESET_ID_LEN} bytes");
    }
    let mut segments = tileset_id.split('/');
    let first = segments.next().unwrap_or_default();
    let second = segments.next();
    if segments.next().is_some() {
        bail!("tileset_id may contain at most one '/' separating namespace and id");
    }
    validate_tileset_segment(first)?;
    if let Some(second) = second {
        validate_tileset_segment(second)?;
        if matches!(second, "preview" | "preview.json") {
            bail!("tileset_id namespace segment may not use reserved route words");
        }
    }
    Ok(())
}

/// Validates a single `/`-delimited segment of a tileset identifier.
fn validate_tileset_segment(segment: &str) -> Result<()> {
    if segment.is_empty() {
        bail!("tileset_id segments must not be empty");
    }
    if segment == "." || segment == ".." {
        bail!("tileset_id segments must not be '.' or '..'");
    }
    if !segment
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("tileset_id contains invalid characters");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::TilesetId;

    #[test]
    fn accepts_flat_and_namespaced_keys() {
        // `preview` is reserved only as the namespaced second segment; as a flat
        // key it is reachable via `/tilesets/preview`, so it stays valid.
        for ok in ["japan", "analysis/hrnowc", "a.b-c_d", "ns/id.v1", "preview"] {
            assert!(TilesetId::try_new(ok).is_ok(), "should accept {ok}");
        }
    }

    #[test]
    fn rejects_traversal_and_malformed_keys() {
        for bad in [
            "",
            "/id",
            "ns/",
            "a/b/c",
            "../etc",
            "ns/..",
            "ns/./id",
            "ns id",
            "ns/i d",
            "ns/preview",
            "ns/preview.json",
        ] {
            assert!(TilesetId::try_new(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn rejects_overlong_ids() {
        let at_limit = "a".repeat(super::MAX_TILESET_ID_LEN);
        assert!(TilesetId::try_new(&at_limit).is_ok());
        let too_long = "a".repeat(super::MAX_TILESET_ID_LEN + 1);
        assert!(TilesetId::try_new(&too_long).is_err());
    }
}
