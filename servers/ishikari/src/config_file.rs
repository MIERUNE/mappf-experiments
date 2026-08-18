//! Optional TOML configuration document.
//!
//! **Status: partial prototype.** This is not yet the single validated
//! configuration document that `issues/refactor.md` item 115 asks for: listeners,
//! tileset sources, cache budgets, and most routing controls remain
//! flag-only. Item 115 stays open until the document can express a deployment on
//! its own.
//!
//! Flags and environment variables remain the authoritative interface. This
//! document only supplies values for settings that have **no built-in default**,
//! which is what keeps precedence unambiguous: such a setting is `None` exactly
//! when the operator did not provide it, so "flag wins, file fills the gap"
//! needs no guesswork about whether an observed value came from the command line
//! or from a default. Settings that already carry a default are deliberately
//! absent rather than silently shadowed.
//!
//! Two further exclusions are deliberate:
//!
//! - **Nothing that may carry a credential.** Provider URL templates are
//!   accepted with fixed query parameters, so a style, glyph, or sprite template
//!   can embed a token. Admitting them here would make the document itself a
//!   secret, which item 115's guardrail forbids; they stay on flags and
//!   environment variables until a secret-reference mechanism exists.
//! - **Nothing that must differ per replica.** Node identity and advertise
//!   addresses are supplied per pod (from pod metadata in Kubernetes). A
//!   configuration document is conventionally one shared object, so accepting
//!   them here would invite every replica to start with the same identity and
//!   collapse gossip membership and tile ownership onto it.
//!
//! Unknown keys are rejected. A typo must fail startup instead of leaving the
//! operator believing a setting took effect.

use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

/// Values an operator may supply from a configuration document.
///
/// Every field mirrors a flag that has no built-in default. Adding a field here
/// requires the corresponding flag to stay optional, otherwise the precedence
/// argument above no longer holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "unstable-schemas", derive(schemars::JsonSchema))]
pub(crate) struct ConfigFile {
    /// Document format version. Present so a format change reports a version
    /// mismatch rather than surfacing as an unknown-key rejection, matching the
    /// explicit versioning the auth registry and content layout already use.
    #[cfg_attr(
        feature = "unstable-schemas",
        schemars(schema_with = "schema_version_schema")
    )]
    pub(crate) schema_version: u32,
    /// Registry whose explicit anonymous grant permits credential-free access.
    /// A registry identifier, never a credential.
    pub(crate) anonymous_registry: Option<String>,
    /// Concurrent backend fetch ceiling.
    pub(crate) backend_fetch_max_inflight: Option<usize>,
    /// Composite tileset whose high zooms resolve against a detail archive.
    #[cfg_attr(
        feature = "unstable-schemas",
        schemars(
            length(min = 1, max = 256),
            regex(pattern = r"^[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)?$")
        )
    )]
    pub(crate) mapterhorn_tileset: Option<String>,
    /// Highest zoom served from the composite base archive.
    #[cfg_attr(feature = "unstable-schemas", schemars(range(min = 13, max = 30)))]
    pub(crate) mapterhorn_maxzoom: Option<u8>,
    /// CPU-bound work concurrency.
    pub(crate) cpu_work_concurrency: Option<usize>,
    /// Queued CPU-work ceiling before shedding.
    pub(crate) cpu_work_max_inflight: Option<usize>,
    /// Concurrent upstream provider body fetches (glyphs, styles, sprites).
    ///
    /// Admissible here because the flag carries no built-in default: it is `None`
    /// exactly when the operator did not set it. Its companion reserve,
    /// `ISKR_PROVIDER_ACTIVE_BODY_BUDGET_BYTES`, is deliberately absent for the
    /// opposite reason — it has a default, so the document could only shadow it.
    pub(crate) provider_fetch_concurrency: Option<usize>,
}

/// The only format this build understands.
pub(crate) const SCHEMA_VERSION: u32 = 1;

#[cfg(feature = "unstable-schemas")]
fn schema_version_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "integer",
        "const": SCHEMA_VERSION
    })
}

impl Default for ConfigFile {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            anonymous_registry: None,
            backend_fetch_max_inflight: None,
            mapterhorn_tileset: None,
            mapterhorn_maxzoom: None,
            cpu_work_concurrency: None,
            cpu_work_max_inflight: None,
            provider_fetch_concurrency: None,
        }
    }
}

/// Configuration-document failure. Reading, parsing, and version mismatch are
/// distinguished so an operator can tell a missing file from an invalid one and
/// from one written for another release.
#[derive(Debug)]
pub(crate) enum ConfigFileError {
    Read {
        path: String,
        error: std::io::Error,
    },
    Parse {
        path: String,
        error: Box<toml::de::Error>,
    },
    UnsupportedVersion {
        path: String,
        found: u32,
    },
}

impl std::fmt::Display for ConfigFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, error } => write!(f, "read config file {path}: {error}"),
            Self::Parse { path, error } => write!(f, "parse config file {path}: {error}"),
            Self::UnsupportedVersion { path, found } => write!(
                f,
                "config file {path} declares schema_version {found}, but this build \
                 supports {SCHEMA_VERSION}"
            ),
        }
    }
}

impl std::error::Error for ConfigFileError {}

impl ConfigFile {
    /// Loads and fully validates a document. An invalid document fails rather
    /// than contributing a partially applied configuration.
    pub(crate) fn load(path: &Path) -> Result<Self, ConfigFileError> {
        let path_label = path.display().to_string();
        let text = fs::read_to_string(path).map_err(|error| ConfigFileError::Read {
            path: path_label.clone(),
            error,
        })?;
        let parsed = Self::parse(&text).map_err(|error| ConfigFileError::Parse {
            path: path_label.clone(),
            error: Box::new(error),
        })?;
        if parsed.schema_version != SCHEMA_VERSION {
            return Err(ConfigFileError::UnsupportedVersion {
                path: path_label,
                found: parsed.schema_version,
            });
        }
        Ok(parsed)
    }

    fn parse(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }

    /// Applies a document value only where the flag was not supplied. The flag
    /// always wins, so a document can never override an explicit operator
    /// choice.
    pub(crate) fn fill<T>(flag: Option<T>, from_file: Option<T>) -> Option<T> {
        flag.or(from_file)
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigFile, ConfigFileError};

    #[test]
    fn parses_a_partial_document_and_defaults_the_rest() {
        let parsed = ConfigFile::parse(
            r#"
            schema_version = 1
            mapterhorn_tileset = "mapterhorn/planet"
            mapterhorn_maxzoom = 16
            "#,
        )
        .expect("partial document parses");
        assert_eq!(
            parsed.mapterhorn_tileset.as_deref(),
            Some("mapterhorn/planet")
        );
        assert_eq!(parsed.mapterhorn_maxzoom, Some(16));
        assert_eq!(parsed.cpu_work_concurrency, None);
    }

    /// The document must admit exactly the flags that carry no built-in default,
    /// which is the invariant that keeps "flag wins, file fills the gap"
    /// unambiguous. `provider_fetch_concurrency` qualifies and was missing;
    /// `provider_active_body_budget_bytes` does not, because it has a default the
    /// document could only shadow.
    ///
    /// The value below is one a document can carry *on its own*. Anything above the
    /// default concurrency also needs `ISKR_PROVIDER_ACTIVE_BODY_BUDGET_BYTES`
    /// raised, because option resolution multiplies concurrency by the largest body
    /// cap and refuses to exceed the reserve — so a larger literal here would
    /// document a file that cannot start a process by itself.
    #[test]
    fn admits_the_provider_fetch_concurrency_but_not_its_defaulted_reserve() {
        let parsed = ConfigFile::parse(
            r#"
            schema_version = 1
            provider_fetch_concurrency = 32
            "#,
        )
        .expect("document with the provider concurrency parses");
        assert_eq!(parsed.provider_fetch_concurrency, Some(32));

        let rejected =
            ConfigFile::parse("schema_version = 1\nprovider_active_body_budget_bytes = 1073741824");
        assert!(
            rejected.is_err(),
            "a defaulted setting must not be silently shadowed by the document"
        );
    }

    #[test]
    fn rejects_an_unknown_key_instead_of_ignoring_it() {
        let error =
            ConfigFile::parse("schema_version = 1\nhtp_port = 8080").expect_err("typo must fail");
        assert!(error.to_string().contains("htp_port"), "{error}");
    }

    #[test]
    fn rejects_a_wrongly_typed_value() {
        assert!(ConfigFile::parse("schema_version = 1\nmapterhorn_maxzoom = \"twelve\"").is_err());
    }

    #[test]
    fn requires_an_explicit_format_version() {
        assert!(ConfigFile::parse("").is_err());
    }

    #[test]
    fn rejects_a_document_written_for_another_format_version() {
        let error = ConfigFile::parse(&format!("schema_version = {}", super::SCHEMA_VERSION + 1))
            .map(|parsed| parsed.schema_version);
        // Parsing accepts the field; `load` is what refuses the version, so the
        // operator sees a version mismatch instead of an unknown-key error.
        assert_eq!(error.expect("parses"), super::SCHEMA_VERSION + 1);
    }

    #[test]
    fn carries_no_credential_bearing_or_per_replica_setting() {
        // These must stay on flags: provider templates may embed a token, and
        // node identity must differ per replica.
        let document = "\
            style_templates = \"x\"\n\
            glyph_url_template = \"x\"\n\
            sprite_templates = \"x\"\n\
            node_id = \"x\"\n\
            gossip_advertise_addr = \"127.0.0.1:1\"\n";
        for line in document.lines().filter(|line| !line.trim().is_empty()) {
            assert!(
                ConfigFile::parse(&format!("schema_version = 1\n{line}")).is_err(),
                "document must reject {line:?}"
            );
        }
    }

    #[test]
    fn a_flag_always_wins_over_the_document() {
        assert_eq!(ConfigFile::fill(Some(9), Some(1)), Some(9));
        assert_eq!(ConfigFile::fill(None, Some(1)), Some(1));
        assert_eq!(ConfigFile::fill::<u8>(None, None), None);
    }

    #[test]
    fn reports_a_missing_file_as_a_read_failure() {
        let error = ConfigFile::load(std::path::Path::new("/nonexistent/ishikari.toml"))
            .expect_err("missing file fails");
        assert!(matches!(error, ConfigFileError::Read { .. }), "{error}");
    }
}
