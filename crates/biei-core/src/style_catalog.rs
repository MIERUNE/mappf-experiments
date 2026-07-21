//! Cluster-stable `StyleId` to style definition resolution.
//!
//! Production and simulator both register explicit definitions or configure a
//! lazy URL template. Template resolution is computed on demand and is not
//! persisted in the explicit catalog, so attacker-controlled style ids cannot
//! grow the catalog indefinitely.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::types::{StyleId, StyleRevision};

fn read_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StyleDefinition {
    pub style_url: String,
    pub version: u64,
}

impl StyleDefinition {
    pub fn new(style_url: impl Into<String>, version: u64) -> Self {
        Self {
            style_url: style_url.into(),
            version,
        }
    }
}

const INITIAL_STYLE_VERSION: u64 = 1;

#[derive(Debug, Default)]
struct StyleCatalogInner {
    by_id: HashMap<StyleId, StyleDefinition>,
    /// `namespace -> template`, keyed on the first path segment of a style id.
    /// A match strips the namespace, substituting only the remaining segments.
    namespace_templates: HashMap<String, String>,
    /// Catch-all used when no namespace template matches; substitutes the whole
    /// style id (so `default` behaves like the historic single template).
    default_template: Option<String>,
}

impl StyleCatalogInner {
    /// Pick the template for `id` and the value to substitute for `{style_id}`:
    /// a namespace match strips its prefix (provider-local id), otherwise the
    /// default template receives the whole id.
    fn template_for<'a>(&'a self, id: &'a StyleId) -> Option<(&'a str, &'a str)> {
        if let Some((namespace, rest)) = id.as_str().split_once('/')
            && let Some(template) = self.namespace_templates.get(namespace)
        {
            return Some((template, rest));
        }
        self.default_template
            .as_deref()
            .map(|template| (template, id.as_str()))
    }
}

#[derive(Debug, Default)]
pub struct StyleCatalog {
    inner: RwLock<StyleCatalogInner>,
}

impl StyleCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or update the renderable style definition.
    pub fn upsert_definition(&self, style_id: StyleId, definition: StyleDefinition) {
        write_unpoisoned(&self.inner)
            .by_id
            .insert(style_id, definition);
    }

    /// Configure the default lazy `StyleId -> style.json URL` template. Unknown
    /// styles with no matching namespace template resolve on demand by replacing
    /// `{style_id}` (with the whole id) in this template. Explicit
    /// `upsert_definition` entries still take precedence.
    pub fn set_url_template(&self, template: impl Into<String>) {
        write_unpoisoned(&self.inner).default_template = Some(template.into());
    }

    /// Register a per-namespace template. A style id whose first path segment is
    /// `namespace` resolves against this template, substituting `{style_id}`
    /// with the segments after the namespace.
    pub fn add_namespace_template(
        &self,
        namespace: impl Into<String>,
        template: impl Into<String>,
    ) {
        write_unpoisoned(&self.inner)
            .namespace_templates
            .insert(namespace.into(), template.into());
    }

    pub fn resolve_latest(&self, style_id: &StyleId) -> Option<u64> {
        let inner = read_unpoisoned(&self.inner);
        inner
            .by_id
            .get(style_id)
            .map(|definition| definition.version)
            .or_else(|| inner.template_for(style_id).map(|_| INITIAL_STYLE_VERSION))
    }

    pub fn accepts_revision(&self, revision: &StyleRevision) -> bool {
        self.resolve_latest(&revision.id)
            .is_some_and(|version| version == revision.version)
    }

    pub fn definition_for_revision(&self, revision: &StyleRevision) -> Option<StyleDefinition> {
        let inner = read_unpoisoned(&self.inner);
        if let Some(definition) = inner
            .by_id
            .get(&revision.id)
            .filter(|definition| definition.version == revision.version)
            .cloned()
        {
            return Some(definition);
        }
        if revision.version == INITIAL_STYLE_VERSION {
            inner.template_for(&revision.id).map(|(template, subst)| {
                StyleDefinition::new(template.replace("{style_id}", subst), INITIAL_STYLE_VERSION)
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_definition_resolves_latest() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("voyager-gl-style".to_string());
        let definition = StyleDefinition::new(
            "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
            3,
        );

        catalog.upsert_definition(style_id.clone(), definition.clone());
        assert_eq!(catalog.resolve_latest(&style_id), Some(3));
        assert_eq!(
            catalog.definition_for_revision(&StyleRevision {
                id: style_id,
                version: 3
            }),
            Some(definition)
        );
    }

    #[test]
    fn definition_lookup_requires_matching_version() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("voyager-gl-style".to_string());
        catalog.upsert_definition(
            style_id.clone(),
            StyleDefinition::new("https://example.test/style.json", 7),
        );

        assert_eq!(
            catalog.definition_for_revision(&StyleRevision {
                id: style_id,
                version: 6
            }),
            None
        );
    }

    #[test]
    fn url_template_lazily_resolves_unknown_styles() {
        let catalog = StyleCatalog::new();
        catalog.set_url_template("http://style-provider.local/styles/{style_id}/style.json");
        let style_id = StyleId("example-basic".to_string());

        assert_eq!(catalog.resolve_latest(&style_id), Some(1));
        assert!(catalog.accepts_revision(&StyleRevision {
            id: style_id.clone(),
            version: 1,
        }));
        assert!(!catalog.accepts_revision(&StyleRevision {
            id: style_id.clone(),
            version: 0,
        }));
        assert_eq!(
            catalog.definition_for_revision(&StyleRevision {
                id: style_id.clone(),
                version: 1,
            }),
            Some(StyleDefinition::new(
                "http://style-provider.local/styles/example-basic/style.json",
                1,
            ))
        );
        assert!(
            read_unpoisoned(&catalog.inner).by_id.is_empty(),
            "template resolution must not persist attacker-controlled style ids"
        );
    }

    #[test]
    fn namespace_template_strips_prefix_and_default_keeps_whole_id() {
        let catalog = StyleCatalog::new();
        catalog.add_namespace_template(
            "gl",
            "https://basemaps.cartocdn.com/gl/{style_id}/style.json",
        );
        catalog.set_url_template("https://fallback.example/{style_id}/style.json");

        // Matched namespace: prefix stripped, only the remainder substituted.
        let matched = StyleId("gl/voyager-gl-style".to_string());
        assert_eq!(catalog.resolve_latest(&matched), Some(1));
        assert_eq!(
            catalog
                .definition_for_revision(&StyleRevision {
                    id: matched,
                    version: 1,
                })
                .expect("namespace template resolves")
                .style_url,
            "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
        );

        // Unmatched namespace falls back to the default with the whole id.
        let unmatched = StyleId("other/basic".to_string());
        assert_eq!(
            catalog
                .definition_for_revision(&StyleRevision {
                    id: unmatched,
                    version: 1,
                })
                .expect("default template resolves")
                .style_url,
            "https://fallback.example/other/basic/style.json"
        );
    }

    #[test]
    fn namespace_only_catalog_404s_unmatched() {
        let catalog = StyleCatalog::new();
        catalog.add_namespace_template(
            "gl",
            "https://basemaps.cartocdn.com/gl/{style_id}/style.json",
        );

        assert_eq!(
            catalog.resolve_latest(&StyleId("voyager-gl-style".to_string())),
            None,
            "single-segment id has no namespace and no default template"
        );
        assert_eq!(
            catalog.resolve_latest(&StyleId("unknown/foo".to_string())),
            None,
        );
    }

    #[test]
    fn explicit_definition_overrides_url_template() {
        let catalog = StyleCatalog::new();
        catalog.set_url_template("https://styles.example.com/{style_id}/style.json");
        let style_id = StyleId("voyager-gl-style".to_string());
        catalog.upsert_definition(
            style_id.clone(),
            StyleDefinition::new(
                "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
                3,
            ),
        );

        assert_eq!(catalog.resolve_latest(&style_id), Some(3));
        assert_eq!(
            catalog
                .definition_for_revision(&StyleRevision {
                    id: style_id,
                    version: 3,
                })
                .expect("explicit definition exists")
                .style_url,
            "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
        );
    }
}
