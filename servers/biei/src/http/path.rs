use crate::http::error::{IngressError, invalid};
use biei_core::style_catalog::StyleCatalog;
use biei_core::types::{StyleId, StyleRevision};
use mmpf_http::style_key::StyleKey;

pub(crate) struct ResolvedStyle {
    pub(crate) revision: StyleRevision,
}

pub(crate) fn resolve_style_id(components: &[&str]) -> Result<StyleId, IngressError> {
    let [namespace, style_id] = components else {
        return Err(invalid("style key must be /{namespace}/{style_id}"));
    };
    let key =
        StyleKey::from_segments(namespace, style_id).map_err(|error| invalid(error.to_string()))?;
    Ok(StyleId(format!("{}/{}", key.namespace(), key.style_id())))
}

/// Validates an already-joined style id under the same rules as a request path.
///
/// Advisory refresh hints use the same canonical identity as public paths.
pub(crate) fn resolve_style_id_str(style_id: &str) -> Result<StyleId, IngressError> {
    let key = StyleKey::parse(style_id).map_err(|error| invalid(error.to_string()))?;
    Ok(StyleId(format!("{}/{}", key.namespace(), key.style_id())))
}

pub(crate) fn resolve_style(
    catalog: &StyleCatalog,
    style_id: StyleId,
) -> Result<ResolvedStyle, IngressError> {
    let Some(version) = catalog.resolve_latest(&style_id) else {
        return Err(IngressError::UnknownStyle(style_id));
    };
    Ok(ResolvedStyle {
        revision: StyleRevision {
            id: style_id,
            version,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_canonical_style_key() {
        assert_eq!(
            resolve_style_id(&["provider", "style_variant"])
                .expect("safe style id")
                .as_str(),
            "provider/style_variant"
        );
    }

    #[test]
    fn rejects_style_id_with_url_syntax() {
        assert!(resolve_style_id(&["provider", "%2fmetadata"]).is_err());
        assert!(resolve_style_id(&["provider", "style\\host"]).is_err());
    }

    #[test]
    fn rejects_oversized_style_id() {
        let oversized = "a".repeat(mmpf_http::style_key::MAX_LOCAL_STYLE_ID_BYTES + 1);
        assert!(resolve_style_id(&["provider", &oversized]).is_err());
    }

    #[test]
    fn rejects_missing_or_extra_segments() {
        assert!(resolve_style_id(&["basic"]).is_err());
        assert!(resolve_style_id(&["default", "basic", "extra"]).is_err());
    }
}
