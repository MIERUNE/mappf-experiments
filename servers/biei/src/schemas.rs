//! Configuration-schema generation, behind `unstable-schemas`.
//!
//! The schema is derived from the `ConfigFile` type the server actually parses,
//! it is not a behavioural source of truth (`specs/` remains authoritative), and
//! it is absent from a served build.
//!
//! Unlike Ishikari, Biei publishes no API description here. Its public surface is
//! one fallback handler over a URL grammar parsed inside ingress, so there are no
//! per-route handlers to annotate; a hand-written description would drift from
//! the grammar with nothing to detect it. Describing Biei's delivery API needs
//! the grammar itself to become the source of truth first.

use crate::config_file::ConfigFile;

/// JSON Schema for the optional configuration document.
pub(crate) fn config_json_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(ConfigFile))
        .expect("JSON Schema is always serialisable")
}

/// Emits the schema when invoked as `biei gen-schemas [config]`.
///
/// Returns `None` for an ordinary server start. This is a build-time entry
/// point, checked before any configuration is resolved.
pub(crate) fn emit_if_requested() -> Option<anyhow::Result<()>> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("gen-schemas") {
        return None;
    }
    match args.next().as_deref() {
        Some("config") | None => {}
        Some(other) => {
            return Some(Err(anyhow::anyhow!(
                "unknown artifact {other:?}; Biei emits only `config`"
            )));
        }
    }
    Some(
        serde_json::to_string_pretty(&config_json_schema())
            .map(|text| println!("{text}"))
            .map_err(anyhow::Error::from),
    )
}

#[cfg(test)]
mod tests {
    use super::config_json_schema;

    /// The precedence rule in `config_file` only holds while every documented
    /// setting maps to a flag with no built-in default: otherwise the flag is
    /// always `Some`, the document value never applies, and the key becomes
    /// silently inert while the schema still advertises it. The field list comes
    /// from the generated schema, so this cannot drift from the struct.
    #[test]
    fn every_documented_setting_maps_to_a_flag_without_a_default() {
        let schema = config_json_schema();
        let fields = schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .expect("config schema exposes properties");
        let command = crate::cli::command();
        for field in fields.keys() {
            // Document metadata, not a setting: it has no flag by design.
            if field == "schema_version" {
                continue;
            }
            let arg = command
                .get_arguments()
                .find(|arg| arg.get_id().as_str() == field)
                .unwrap_or_else(|| panic!("config field {field:?} has no matching flag"));
            assert!(
                arg.get_default_values().is_empty(),
                "config field {field:?} maps to a flag with a built-in default, so the \
                 document value would never apply"
            );
        }
    }

    #[test]
    fn config_schema_excludes_credentials_replica_identity_and_host_sizing() {
        let schema = config_json_schema();
        let properties = schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .expect("config schema exposes properties");
        assert!(properties.contains_key("schema_version"), "{schema}");
        for excluded in [
            "style_templates",
            "tileset_url_template",
            "node_id",
            "gossip_advertise_addr",
            "internal_advertise_addr",
            "cores",
            "mln_body_permits",
        ] {
            assert!(!properties.contains_key(excluded), "{excluded}: {schema}");
        }
    }
}
