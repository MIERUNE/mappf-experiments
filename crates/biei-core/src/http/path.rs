use crate::http::error::{IngressError, invalid};
use crate::style_catalog::StyleCatalog;
use crate::types::{StyleId, StyleRevision};

const MAX_STYLE_ID_BYTES: usize = 512;

pub(crate) struct ResolvedStyle {
    pub(crate) revision: StyleRevision,
}

pub(crate) fn resolve_style_id(components: &[&str]) -> Result<StyleId, IngressError> {
    for component in components {
        validate_path_component(component, "style_id")?;
    }
    let style_id = components.join("/");
    if style_id.len() > MAX_STYLE_ID_BYTES {
        return Err(invalid(format!(
            "style_id must be at most {MAX_STYLE_ID_BYTES} bytes"
        )));
    }
    Ok(StyleId(style_id))
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

fn validate_path_component(value: &str, name: &str) -> Result<(), IngressError> {
    if value.is_empty() {
        return Err(invalid(format!("{name} must not be empty")));
    }
    if value.contains("..") {
        return Err(invalid(format!("{name} must not contain `..`")));
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'@')
    }) {
        return Err(invalid(format!("{name} contains an unsupported character")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_namespaced_style_id_with_revision_separator() {
        assert_eq!(
            resolve_style_id(&["provider", "style@variant"])
                .expect("safe style id")
                .as_str(),
            "provider/style@variant"
        );
    }

    #[test]
    fn rejects_style_id_with_url_syntax() {
        assert!(resolve_style_id(&["provider", "%2fmetadata"]).is_err());
        assert!(resolve_style_id(&["provider", "style\\host"]).is_err());
    }

    #[test]
    fn rejects_oversized_style_id() {
        let oversized = "a".repeat(MAX_STYLE_ID_BYTES + 1);
        assert!(resolve_style_id(&[&oversized]).is_err());
    }
}
