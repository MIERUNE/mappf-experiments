//! Optional TOML configuration document.
//!
//! **Status: partial prototype.** Like Ishikari's, this is not yet the single
//! validated configuration document that `issues/refactor.md` item 115 asks for:
//! listeners, worker sizing, queue limits, and the style catalog remain
//! flag-only.
//!
//! The selection rule matches Ishikari's so the two servers stay predictable.
//! Only settings with **no built-in default** appear, which keeps precedence
//! unambiguous: such a setting is `None` exactly when the operator did not
//! supply it, so "flag wins, file fills the gap" never has to guess whether an
//! observed value came from the command line or from a default.
//!
//! Three exclusions are deliberate:
//!
//! - **Nothing that may carry a credential.** Style and tileset URL templates
//!   are accepted with fixed query parameters, so they can embed a token.
//! - **Nothing that must differ per replica.** Node identity and advertise
//!   addresses come from pod metadata; a shared document carrying them would
//!   give every replica one identity.
//! - **Nothing that describes the host it happens to run on.** Core count and
//!   the debug sizing overrides are calibrated per machine shape, so a document
//!   shared across differently sized nodes would silently mis-size them.
//!
//! Unknown keys are rejected, so a typo fails startup rather than leaving the
//! operator believing a setting took effect.

use std::{fs, path::Path, path::PathBuf};

use serde::{Deserialize, Serialize};

/// Values an operator may supply from a configuration document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
#[cfg_attr(feature = "unstable-schemas", derive(schemars::JsonSchema))]
pub(crate) struct ConfigFile {
    /// Document format version, so a format change reports a version mismatch
    /// rather than an unknown-key rejection.
    #[serde(default = "default_schema_version")]
    pub(crate) schema_version: u32,
    /// Registry whose explicit anonymous grant permits credential-free access.
    /// A registry identifier, never a credential.
    pub(crate) anonymous_registry: Option<String>,
    /// Exact provider origin the delivery token may be attached to.
    pub(crate) auth_provider_origin: Option<String>,
    /// Directory for the MapLibre resource cache.
    pub(crate) maplibre_cache_path: Option<PathBuf>,
    /// Font used to draw generated pin labels.
    pub(crate) pin_label_font: Option<PathBuf>,
}

/// The only format this build understands.
pub(crate) const SCHEMA_VERSION: u32 = 1;

fn default_schema_version() -> u32 {
    SCHEMA_VERSION
}

impl Default for ConfigFile {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            anonymous_registry: None,
            auth_provider_origin: None,
            maplibre_cache_path: None,
            pin_label_font: None,
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

    /// Applies a document value only where the flag was not supplied, so a
    /// document can never override an explicit operator choice.
    pub(crate) fn fill<T>(flag: Option<T>, from_file: Option<T>) -> Option<T> {
        flag.or(from_file)
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigFile, ConfigFileError, SCHEMA_VERSION};

    #[test]
    fn parses_a_partial_document_and_defaults_the_rest() {
        let parsed = ConfigFile::parse(
            r#"
            anonymous_registry = "public"
            pin_label_font = "/fonts/NotoSans-Bold.ttf"
            "#,
        )
        .expect("partial document parses");
        assert_eq!(parsed.anonymous_registry.as_deref(), Some("public"));
        assert_eq!(
            parsed.pin_label_font.as_deref(),
            Some(std::path::Path::new("/fonts/NotoSans-Bold.ttf"))
        );
        assert_eq!(parsed.maplibre_cache_path, None);
    }

    #[test]
    fn rejects_an_unknown_key_instead_of_ignoring_it() {
        let error = ConfigFile::parse("htp_port = 8080").expect_err("typo must fail");
        assert!(error.to_string().contains("htp_port"), "{error}");
    }

    #[test]
    fn omitting_the_version_assumes_the_current_format() {
        assert_eq!(
            ConfigFile::parse("")
                .expect("empty is valid")
                .schema_version,
            SCHEMA_VERSION
        );
    }

    #[test]
    fn carries_nothing_credential_bearing_per_replica_or_host_shaped() {
        // Templates may embed a token; identity and addresses differ per pod;
        // sizing is calibrated per machine. All three stay on flags.
        for key in [
            "style_templates",
            "tileset_url_template",
            "node_id",
            "gossip_advertise_addr",
            "internal_advertise_addr",
            "cores",
            "debug_render_permits",
            "mln_body_permits",
        ] {
            assert!(
                ConfigFile::parse(&format!("{key} = \"x\"")).is_err(),
                "document must reject {key:?}"
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
        let error = ConfigFile::load(std::path::Path::new("/nonexistent/biei.toml"))
            .expect_err("missing file fails");
        assert!(matches!(error, ConfigFileError::Read { .. }), "{error}");
    }
}
