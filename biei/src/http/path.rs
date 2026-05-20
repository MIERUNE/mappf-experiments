use crate::http::error::{IngressError, invalid};
use crate::style_catalog::StyleCatalog;
use crate::types::{StyleId, StyleRevision};

pub(crate) struct ResolvedStyle {
    pub(crate) revision: StyleRevision,
}

pub(crate) fn resolve_style_id(components: &[&str]) -> Result<StyleId, IngressError> {
    for component in components {
        validate_path_component(component, "style_id")?;
    }
    Ok(StyleId(components.join("/")))
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
    Ok(())
}
