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

    /// Appends one validated path segment to a flat tileset id.
    ///
    /// The result is validated as a complete tileset id, so joining onto an
    /// already-namespaced id, using a reserved route word, or exceeding the
    /// length limit is rejected.
    pub fn join_segment(&self, segment: &str) -> Result<Self> {
        Self::try_new(&format!("{self}/{segment}"))
    }

    /// Returns the optional namespace before the single `/` separator.
    pub fn namespace(&self) -> Option<&str> {
        self.as_str()
            .split_once('/')
            .map(|(namespace, _)| namespace)
    }

    /// Returns the local id after the namespace, or the complete flat id.
    pub fn local_id(&self) -> &str {
        self.as_str()
            .split_once('/')
            .map_or(self.as_str(), |(_, local_id)| local_id)
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

/// Interned key used for HRW placement and generated-resource caches.
///
/// Unlike [`TilesetId`], this key may identify a synthetic namespace that has
/// no PMTiles archive or object-store path. Construction stays restricted to
/// validated tileset ids and typed derived-resource namespaces so synthetic
/// keys cannot be passed to archive APIs accidentally.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceRoutingKey(ArcIntern<str>);

impl ResourceRoutingKey {
    /// Builds the stable routing/cache namespace for a derived tileset product.
    ///
    /// The wire value remains `derived:{product}:{tileset_id}`. `product` is
    /// validated as one identifier segment so it cannot inject separators or
    /// collide with another product/tileset pairing.
    pub fn for_derived_resource(product: &str, tileset_id: &TilesetId) -> Result<Self> {
        validate_resource_segment(product)?;
        Ok(Self(ArcIntern::from(
            format!("derived:{product}:{tileset_id}").as_str(),
        )))
    }

    /// Returns the routing key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&TilesetId> for ResourceRoutingKey {
    fn from(tileset_id: &TilesetId) -> Self {
        Self(tileset_id.0.clone())
    }
}

impl From<TilesetId> for ResourceRoutingKey {
    fn from(tileset_id: TilesetId) -> Self {
        Self(tileset_id.0)
    }
}

impl AsRef<str> for ResourceRoutingKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for ResourceRoutingKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for ResourceRoutingKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
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
    if !is_identifier_segment(segment) {
        bail!("tileset_id contains invalid characters");
    }
    Ok(())
}

fn validate_resource_segment(segment: &str) -> Result<()> {
    if segment.is_empty() {
        bail!("derived resource product must not be empty");
    }
    if segment.len() > MAX_TILESET_ID_LEN {
        bail!("derived resource product must be at most {MAX_TILESET_ID_LEN} bytes");
    }
    if segment == "." || segment == ".." || !is_identifier_segment(segment) {
        bail!("derived resource product contains invalid characters");
    }
    Ok(())
}

fn is_identifier_segment(segment: &str) -> bool {
    segment
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::{ResourceRoutingKey, TilesetId};

    #[test]
    fn accepts_flat_and_namespaced_keys() {
        // `preview` is reserved only as the namespaced second segment; as a flat
        // key it is reachable via `/tilesets/preview`, so it stays valid.
        for ok in ["japan", "analysis/hrnowc", "a.b-c_d", "ns/id.v1", "preview"] {
            assert!(TilesetId::try_new(ok).is_ok(), "should accept {ok}");
        }
    }

    #[test]
    fn decomposes_flat_id() {
        let tileset = TilesetId::try_new("japan").unwrap();

        assert_eq!(tileset.namespace(), None);
        assert_eq!(tileset.local_id(), "japan");
    }

    #[test]
    fn decomposes_namespaced_id() {
        let tileset = TilesetId::try_new("analysis/hrnowc").unwrap();

        assert_eq!(tileset.namespace(), Some("analysis"));
        assert_eq!(tileset.local_id(), "hrnowc");
    }

    #[test]
    fn joins_one_validated_segment() {
        let namespace = TilesetId::try_new("mapterhorn").unwrap();
        assert_eq!(
            namespace.join_segment("6-62-23").unwrap().as_str(),
            "mapterhorn/6-62-23"
        );
        assert!(namespace.join_segment("preview").is_err());
        assert!(namespace.join_segment("nested/detail").is_err());
        assert!(
            TilesetId::try_new("already/namespaced")
                .unwrap()
                .join_segment("detail")
                .is_err()
        );
    }

    #[test]
    fn resource_routing_keys_preserve_existing_namespaces() {
        let tileset = TilesetId::try_new("mapterhorn/planet").unwrap();
        assert_eq!(
            ResourceRoutingKey::from(&tileset).as_str(),
            "mapterhorn/planet"
        );
        assert_eq!(
            ResourceRoutingKey::for_derived_resource("hillshade", &tileset)
                .unwrap()
                .as_str(),
            "derived:hillshade:mapterhorn/planet"
        );
        assert!(ResourceRoutingKey::for_derived_resource("bad:product", &tileset).is_err());
        assert!(
            ResourceRoutingKey::for_derived_resource(
                &"a".repeat(super::MAX_TILESET_ID_LEN + 1),
                &tileset,
            )
            .is_err()
        );
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
