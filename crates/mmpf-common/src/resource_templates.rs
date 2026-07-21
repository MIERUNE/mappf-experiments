//! Namespaced resource-template parsing, validation, and resolution.

use std::fmt;

use url::Url;

/// Rules for recognizing the namespace before `=` in one template entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamespaceKeyPolicy {
    /// Accept any non-empty path segment without whitespace or template syntax.
    PlainSegment,
    /// Accept only ASCII letters, digits, `-`, and `_`.
    AsciiIdentifier,
}

/// Parsing policy for a `namespace=value;default=value` specification.
#[derive(Clone, Copy, Debug)]
pub struct NamespacedEntriesPolicy<'a> {
    pub config_name: &'a str,
    pub entry_name: &'a str,
    pub namespace_keys: NamespaceKeyPolicy,
}

/// Parsed namespace values plus an optional catch-all value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespacedEntries<T> {
    namespaces: Vec<(String, T)>,
    default: Option<T>,
}

impl<T> Default for NamespacedEntries<T> {
    fn default() -> Self {
        Self {
            namespaces: Vec::new(),
            default: None,
        }
    }
}

/// A selected value and the key relative to its matched namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectedEntry<'a, T> {
    value: &'a T,
    relative_key: &'a str,
}

impl<'a, T> SelectedEntry<'a, T> {
    pub fn value(&self) -> &'a T {
        self.value
    }

    pub fn relative_key(&self) -> &'a str {
        self.relative_key
    }
}

/// Invalid namespaced-entry configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespacedEntriesError(String);

impl fmt::Display for NamespacedEntriesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for NamespacedEntriesError {}

/// Service-provided validation policy for one template specification.
#[derive(Clone, Copy, Debug)]
pub struct TemplatePolicy<'a> {
    pub config_name: &'a str,
    pub placeholder: &'a str,
    pub require_placeholder: bool,
    pub placeholder_must_be_in_path: bool,
    pub allowed_schemes: &'a [&'a str],
    pub namespace_keys: NamespaceKeyPolicy,
}

/// Parsed namespace templates plus an optional catch-all template.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceTemplates {
    entries: NamespacedEntries<String>,
    placeholder: String,
    /// Mirrors `TemplatePolicy::placeholder_must_be_in_path`. When set, the
    /// path-placement guarantee is re-checked against the *resolved* URL, not
    /// only the template.
    enforce_path_placeholder: bool,
}

/// Invalid resource-template configuration.
pub type TemplateError = NamespacedEntriesError;

impl NamespacedEntries<String> {
    /// Parses `;`-separated `namespace=value`, `default=value`, and bare default
    /// entries without imposing meaning or validation on the values themselves.
    pub fn parse(
        raw: &str,
        policy: NamespacedEntriesPolicy<'_>,
    ) -> Result<Self, NamespacedEntriesError> {
        let mut out = Self::default();

        for entry in raw.split(';') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }

            // Treat `=` as a namespace separator only when its left side is a
            // valid namespace. An `=` in a bare URL query remains part of the URL.
            let (key, value, is_default) = match entry.split_once('=') {
                Some((key, value)) if valid_namespace_key(key.trim(), policy.namespace_keys) => {
                    let key = key.trim();
                    (key, value.trim(), key == "default")
                }
                _ => ("default", entry, true),
            };

            if is_default {
                if out.default.replace(value.to_string()).is_some() {
                    return Err(NamespacedEntriesError(format!(
                        "{} has multiple default {}s",
                        policy.config_name, policy.entry_name
                    )));
                }
            } else if out.namespaces.iter().any(|(namespace, _)| namespace == key) {
                return Err(NamespacedEntriesError(format!(
                    "{} has duplicate namespace {key:?}",
                    policy.config_name
                )));
            } else {
                out.namespaces.push((key.to_string(), value.to_string()));
            }
        }

        if out.namespaces.is_empty() && out.default.is_none() {
            return Err(NamespacedEntriesError(format!(
                "{} must define at least one {}",
                policy.config_name, policy.entry_name
            )));
        }

        Ok(out)
    }
}

impl<T> NamespacedEntries<T> {
    pub fn namespaces(&self) -> &[(String, T)] {
        &self.namespaces
    }

    pub fn default_value(&self) -> Option<&T> {
        self.default.as_ref()
    }

    /// Selects a namespace value, stripping the matched first segment, or falls
    /// back to the default value with the complete key.
    pub fn select<'a>(&'a self, key: &'a str) -> Option<SelectedEntry<'a, T>> {
        if let Some((namespace, rest)) = key.split_once('/')
            && let Some((_, value)) = self
                .namespaces
                .iter()
                .find(|(candidate, _)| candidate == namespace)
        {
            return Some(SelectedEntry {
                value,
                relative_key: rest,
            });
        }

        self.default.as_ref().map(|value| SelectedEntry {
            value,
            relative_key: key,
        })
    }

    /// Converts every parsed value while retaining namespace and default
    /// selection semantics.
    pub fn try_map<U, E>(
        self,
        mut map: impl FnMut(Option<&str>, T) -> Result<U, E>,
    ) -> Result<NamespacedEntries<U>, E> {
        let namespaces = self
            .namespaces
            .into_iter()
            .map(|(namespace, value)| {
                let mapped = map(Some(&namespace), value)?;
                Ok((namespace, mapped))
            })
            .collect::<Result<Vec<_>, E>>()?;
        let default = match self.default {
            Some(value) => Some(map(None, value)?),
            None => None,
        };
        Ok(NamespacedEntries {
            namespaces,
            default,
        })
    }
}

impl ResourceTemplates {
    /// Parses `;`-separated `namespace=<template>`, `default=<template>`, and
    /// bare default entries under the supplied service policy.
    pub fn parse(raw: &str, policy: TemplatePolicy<'_>) -> Result<Self, TemplateError> {
        let entries = NamespacedEntries::parse(
            raw,
            NamespacedEntriesPolicy {
                config_name: policy.config_name,
                entry_name: "template",
                namespace_keys: policy.namespace_keys,
            },
        )?;

        for (namespace, template) in entries.namespaces() {
            validate_url_template(template, namespace, policy)?;
        }
        if let Some(template) = entries.default_value() {
            validate_url_template(template, "default", policy)?;
        }

        Ok(Self {
            entries,
            placeholder: policy.placeholder.to_string(),
            enforce_path_placeholder: policy.placeholder_must_be_in_path,
        })
    }

    pub fn namespaces(&self) -> &[(String, String)] {
        self.entries.namespaces()
    }

    pub fn default_template(&self) -> Option<&str> {
        self.entries.default_value().map(String::as_str)
    }

    /// Returns whether a namespace template or the default template matches
    /// the supplied key.
    pub fn has_match(&self, key: &str) -> bool {
        self.entries.select(key).is_some()
    }

    /// Resolves a key after applying a caller-owned encoding policy to the
    /// namespace-local substitution. This keeps transport-specific encoding out
    /// of the shared template contract.
    pub fn resolve_with(&self, key: &str, encode: impl FnOnce(&str) -> String) -> Option<String> {
        let selected = self.entries.select(key)?;
        let substitution = encode(selected.relative_key());
        // Template validation only proves the placeholder sits in the template's
        // path; it says nothing about the substituted value. When the policy
        // requires the placeholder to be in the path, enforce that the resolved
        // value cannot climb out of that directory: reject a substitution that
        // still carries a `.` or `..` path segment (i.e. the caller's encoding
        // did not neutralize traversal). Callers that percent-encode `.`/`/`
        // are unaffected. This makes `placeholder_must_be_in_path` a guarantee
        // about the resolved URL, not only the template.
        if self.enforce_path_placeholder
            && selected.value().contains(&self.placeholder)
            && substitution_has_traversal_segment(&substitution)
        {
            return None;
        }
        Some(selected.value().replace(&self.placeholder, &substitution))
    }
}

/// Prefix for the stand-in used to locate a placeholder while parsing a
/// template as a URL. The complete marker is chosen not to occur in the input,
/// so literal template text cannot be mistaken for the substituted placeholder.
const PLACEHOLDER_MARKER_PREFIX: &str = "mmpf-placeholder-marker";

fn placeholder_marker(template: &str) -> String {
    let mut marker = PLACEHOLDER_MARKER_PREFIX.to_string();
    while template.contains(&marker) {
        marker.push('x');
    }
    marker
}

/// Whether an (already caller-encoded) substitution still contains a `.` or `..`
/// path segment, which would let a path-placeholder resolution traverse out of
/// its intended directory. A segment such as `style.json` is not a match — only
/// the exact traversal segments are.
fn substitution_has_traversal_segment(substitution: &str) -> bool {
    substitution
        .split('/')
        .any(|segment| segment == "." || segment == "..")
}

fn valid_namespace_key(key: &str, policy: NamespaceKeyPolicy) -> bool {
    match policy {
        NamespaceKeyPolicy::PlainSegment => {
            !key.is_empty()
                && !key.chars().any(|character| {
                    character.is_whitespace()
                        || matches!(character, ':' | '/' | ';' | '=' | '{' | '}')
                })
        }
        NamespaceKeyPolicy::AsciiIdentifier => {
            !key.is_empty()
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        }
    }
}

/// Validates one URL template under the supplied resource-template policy.
///
/// This is useful for callers with a single URL setting (rather than a
/// semicolon-separated namespaced specification) that need the same scheme and
/// placeholder-placement guarantees as [`ResourceTemplates::parse`].
pub fn validate_url_template(
    template: &str,
    label: &str,
    policy: TemplatePolicy<'_>,
) -> Result<(), TemplateError> {
    if template.is_empty() {
        return Err(NamespacedEntriesError(format!(
            "{} template for {label} must not be empty",
            policy.config_name
        )));
    }
    if policy.require_placeholder && !template.contains(policy.placeholder) {
        return Err(NamespacedEntriesError(format!(
            "{} template for {label} must contain {}",
            policy.config_name, policy.placeholder
        )));
    }

    let placeholder_marker = placeholder_marker(template);
    let sample = template.replace(policy.placeholder, &placeholder_marker);
    let parsed = Url::parse(&sample).map_err(|error| {
        NamespacedEntriesError(format!(
            "{} template for {label} is not a valid URL: {error}",
            policy.config_name
        ))
    })?;

    if !policy
        .allowed_schemes
        .iter()
        .any(|allowed| parsed.scheme() == *allowed)
    {
        return Err(NamespacedEntriesError(format!(
            "{} template URL scheme {:?} is not supported",
            policy.config_name,
            parsed.scheme()
        )));
    }

    if policy.placeholder_must_be_in_path {
        let expected = template.matches(policy.placeholder).count();
        if parsed.path().matches(&placeholder_marker).count() != expected {
            return Err(NamespacedEntriesError(format!(
                "{} template placeholder must appear only in the URL path",
                policy.config_name
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HTTP_SCHEMES: &[&str] = &["http", "https"];

    fn strict_policy() -> TemplatePolicy<'static> {
        TemplatePolicy {
            config_name: "TEST_TEMPLATES",
            placeholder: "{resource_id}",
            require_placeholder: true,
            placeholder_must_be_in_path: true,
            allowed_schemes: HTTP_SCHEMES,
            namespace_keys: NamespaceKeyPolicy::PlainSegment,
        }
    }

    #[test]
    fn generic_entries_parse_map_and_select_values() {
        let entries = NamespacedEntries::parse(
            "regional=gs://regional;default=data",
            NamespacedEntriesPolicy {
                config_name: "TEST_SOURCES",
                entry_name: "source",
                namespace_keys: NamespaceKeyPolicy::AsciiIdentifier,
            },
        )
        .unwrap()
        .try_map(|_, value| Ok::<_, std::convert::Infallible>(value.len()))
        .unwrap();

        let regional = entries.select("regional/streets").unwrap();
        assert_eq!(*regional.value(), "gs://regional".len());
        assert_eq!(regional.relative_key(), "streets");

        let fallback = entries.select("analysis/weather").unwrap();
        assert_eq!(*fallback.value(), "data".len());
        assert_eq!(fallback.relative_key(), "analysis/weather");
    }

    #[test]
    fn generic_entries_reject_duplicate_and_empty_specs() {
        let policy = NamespacedEntriesPolicy {
            config_name: "TEST_SOURCES",
            entry_name: "source",
            namespace_keys: NamespaceKeyPolicy::AsciiIdentifier,
        };
        for invalid in [
            "regional=gs://a;regional=gs://b",
            "gs://a;default=gs://b",
            "   ",
        ] {
            assert!(NamespacedEntries::parse(invalid, policy).is_err());
        }
    }

    #[test]
    fn selects_namespace_and_default_substitutions() {
        let templates = ResourceTemplates::parse(
            "regional=https://regional.test/{resource_id};default=https://default.test/{resource_id}",
            strict_policy(),
        )
        .unwrap();

        assert!(templates.has_match("regional/streets"));
        assert!(templates.has_match("analysis/weather"));

        let namespace_only = ResourceTemplates::parse(
            "regional=https://regional.test/{resource_id}",
            strict_policy(),
        )
        .unwrap();
        assert!(!namespace_only.has_match("analysis/weather"));

        assert_eq!(
            templates.resolve_with("regional/streets", str::to_owned),
            Some("https://regional.test/streets".to_string())
        );
        assert_eq!(
            templates.resolve_with("analysis/weather", str::to_owned),
            Some("https://default.test/analysis/weather".to_string())
        );
    }

    #[test]
    fn leaves_substitution_encoding_to_the_caller() {
        let templates =
            ResourceTemplates::parse("https://resources.test/{resource_id}", strict_policy())
                .unwrap();

        let resolved = templates
            .resolve_with("folder/a value", |value| value.replace(' ', "%20"))
            .unwrap();
        assert_eq!(resolved, "https://resources.test/folder/a%20value");
    }

    #[test]
    fn path_placeholder_policy_rejects_traversal_substitutions() {
        let templates = ResourceTemplates::parse(
            "default=https://tiles.test/{resource_id}/style.json",
            strict_policy(),
        )
        .unwrap();
        // An identity encoder that fails to neutralize `..`/`.` must not yield a
        // traversal URL when the policy pins the placeholder to the path.
        assert_eq!(
            templates.resolve_with("../../etc/secret", str::to_string),
            None
        );
        assert_eq!(templates.resolve_with("a/./b", str::to_string), None);
        // A legitimate multi-segment key with a dotted filename still resolves.
        assert_eq!(
            templates
                .resolve_with("region/tokyo.v2", str::to_string)
                .as_deref(),
            Some("https://tiles.test/region/tokyo.v2/style.json")
        );
    }

    #[test]
    fn non_path_placeholder_policy_leaves_traversal_to_caller() {
        let policy = TemplatePolicy {
            placeholder_must_be_in_path: false,
            ..strict_policy()
        };
        let templates =
            ResourceTemplates::parse("https://tiles.test/{resource_id}", policy).unwrap();
        // Without the path guarantee the module does not second-guess the
        // caller's encoding, preserving prior behavior.
        assert_eq!(
            templates.resolve_with("../x", str::to_string).as_deref(),
            Some("https://tiles.test/../x")
        );
    }

    #[test]
    fn keeps_query_equals_in_a_bare_default_url() {
        let templates = ResourceTemplates::parse(
            "https://resources.test/{resource_id}?token=abc",
            strict_policy(),
        )
        .unwrap();
        assert!(templates.namespaces().is_empty());
        assert_eq!(
            templates.default_template(),
            Some("https://resources.test/{resource_id}?token=abc")
        );
    }

    #[test]
    fn rejects_duplicates_empty_specs_and_invalid_placeholder_positions() {
        for invalid in [
            "  ;; ",
            "https://a.test/{resource_id};default=https://b.test/{resource_id}",
            "regional=https://a.test/{resource_id};regional=https://b.test/{resource_id}",
            "https://{resource_id}.test/resource",
            "https://resources.test/resource?id={resource_id}",
            "https://resources.test/resource#{resource_id}",
        ] {
            assert!(
                ResourceTemplates::parse(invalid, strict_policy()).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn literal_marker_text_cannot_mask_a_non_path_placeholder() {
        let invalid = format!("https://{{resource_id}}.test/{PLACEHOLDER_MARKER_PREFIX}/resource");

        assert!(ResourceTemplates::parse(&invalid, strict_policy()).is_err());
    }

    #[test]
    fn supports_optional_placeholders_and_object_store_schemes() {
        let templates = ResourceTemplates::parse(
            "shared=gs://bucket/sprite;default=s3://bucket/{resource_id}/sprite",
            TemplatePolicy {
                config_name: "OBJECT_TEMPLATES",
                placeholder: "{resource_id}",
                require_placeholder: false,
                placeholder_must_be_in_path: true,
                allowed_schemes: &["gs", "s3"],
                namespace_keys: NamespaceKeyPolicy::AsciiIdentifier,
            },
        )
        .unwrap();

        assert_eq!(
            templates
                .resolve_with("shared/..", str::to_string)
                .as_deref(),
            Some("gs://bucket/sprite")
        );
    }
}
