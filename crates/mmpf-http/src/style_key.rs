//! Canonical public identity for one delivered or rendered style.

use std::fmt;

/// Maximum byte length of one style namespace segment.
pub const MAX_STYLE_NAMESPACE_BYTES: usize = 64;
/// Maximum byte length of one namespace-local style id segment.
pub const MAX_LOCAL_STYLE_ID_BYTES: usize = 128;
/// Maximum byte length of the canonical `namespace/style_id` form.
pub const MAX_STYLE_KEY_BYTES: usize = MAX_STYLE_NAMESPACE_BYTES + 1 + MAX_LOCAL_STYLE_ID_BYTES;

/// Borrowed, validated `namespace/style_id` identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StyleKey<'a> {
    namespace: &'a str,
    style_id: &'a str,
}

impl<'a> StyleKey<'a> {
    /// Parses exactly two bounded path segments.
    pub fn parse(value: &'a str) -> Result<Self, InvalidStyleKey> {
        let (namespace, style_id) = value.split_once('/').ok_or(InvalidStyleKey)?;
        if style_id.contains('/') {
            return Err(InvalidStyleKey);
        }
        Self::from_segments(namespace, style_id)
    }

    /// Validates an already separated namespace and local style id.
    pub fn from_segments(namespace: &'a str, style_id: &'a str) -> Result<Self, InvalidStyleKey> {
        validate_segment(namespace, MAX_STYLE_NAMESPACE_BYTES)?;
        validate_segment(style_id, MAX_LOCAL_STYLE_ID_BYTES)?;
        Ok(Self {
            namespace,
            style_id,
        })
    }

    pub fn namespace(self) -> &'a str {
        self.namespace
    }

    pub fn style_id(self) -> &'a str {
        self.style_id
    }
}

/// A value is not the canonical two-segment style identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidStyleKey;

impl fmt::Display for InvalidStyleKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "style key must be namespace/style_id using bounded ASCII letters, digits, '-' or '_'",
        )
    }
}

impl std::error::Error for InvalidStyleKey {}

fn validate_segment(value: &str, maximum: usize) -> Result<(), InvalidStyleKey> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(InvalidStyleKey);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_exactly_two_canonical_segments() {
        let key = StyleKey::parse("mierune/jp_mierune_streets").unwrap();

        assert_eq!(key.namespace(), "mierune");
        assert_eq!(key.style_id(), "jp_mierune_streets");
    }

    #[test]
    fn rejects_missing_extra_encoded_and_unsafe_segments() {
        for invalid in [
            "basic",
            "default/basic/extra",
            "/basic",
            "default/",
            "default/%2Fsecret",
            "default/..",
            "default/style.json",
        ] {
            assert!(StyleKey::parse(invalid).is_err(), "{invalid}");
        }
    }
}
