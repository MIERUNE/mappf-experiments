//! Schema and API-description generation, behind `unstable-schemas`.
//!
//! Both artifacts are derived: the configuration schema comes from the
//! `ConfigFile` type the server actually parses, and the API description comes
//! from annotations on the handlers that actually serve the routes. Neither is a
//! behavioural source of truth — `specs/` remains authoritative — and neither is
//! linked into the served binary.

use crate::config_file::ConfigFile;

/// JSON Schema for the optional configuration document.
pub(crate) fn config_json_schema() -> serde_json::Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(ConfigFile))
        .expect("JSON Schema is always serialisable");
    // The two Mapterhorn flags form one semantic setting. Keep the flat keys so
    // they continue to mirror the CLI, but make a schema-valid document provide
    // either both or neither instead of accepting a value runtime would reject
    // or silently ignore.
    schema
        .as_object_mut()
        .expect("root configuration schema is an object")
        .insert(
            "dependentRequired".to_string(),
            serde_json::json!({
                "mapterhorn_tileset": ["mapterhorn_maxzoom"],
                "mapterhorn_maxzoom": ["mapterhorn_tileset"]
            }),
        );
    schema
}

/// Response body for the TileJSON routes.
///
/// Deliberately partial: it names the fields a client needs to consume a tileset
/// and marks the object open, because the served document also carries
/// upstream-derived TileJSON keys that are not this server's contract to fix.
#[derive(utoipa::ToSchema)]
#[schema(as = TileJsonDocument)]
#[allow(dead_code, reason = "field set exists to describe the wire shape")]
pub(crate) struct TileJsonDocument {
    /// TileJSON specification version, for example `3.0.0`.
    tilejson: String,
    /// Fully-qualified tile URL templates.
    tiles: Vec<String>,
    minzoom: Option<u8>,
    maxzoom: Option<u8>,
    /// `[west, south, east, north]` in degrees.
    bounds: Option<Vec<f64>>,
    /// `[longitude, latitude, zoom]`.
    center: Option<Vec<f64>>,
}

/// Opaque bytes carried by tile, raster, and protobuf media types.
#[derive(utoipa::ToSchema)]
#[schema(as = BinaryPayload, value_type = String, format = Binary)]
#[allow(dead_code, reason = "schema-only type for opaque response bytes")]
pub(crate) struct BinaryPayload(Vec<u8>);

/// OpenAPI description of the public delivery surface.
///
/// Only routes carrying a `utoipa::path` annotation appear here. Operational
/// and cluster-internal routes are deliberately excluded: they are not part of
/// the delivery contract and must not be advertised.
#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "Ishikari",
        description = "Tile and map-resource delivery surface.",
        license(name = "MIT OR Apache-2.0")
    ),
    // Delivery credentials are optional per deployment: an unprotected
    // deployment, or one with an anonymous grant, accepts credential-free
    // requests. These alternatives expose both accepted credential transports
    // to generated clients without falsely making either one mandatory.
    security(
        (),
        ("bearer_token" = []),
        ("access_token_query" = [])
    ),
    components(schemas(
        BinaryPayload,
        TileJsonDocument,
        crate::server::DeliveryError
    )),
    modifiers(&DeliveryCredentials),
    paths(
        crate::server::tileset::tilejson::tilejson_handler,
        crate::server::tileset::tilejson::namespaced_tilejson_handler,
        crate::server::tileset::preview::preview_handler,
        crate::server::tileset::preview::namespaced_preview_handler,
        crate::server::tileset::preview::preview_style_handler,
        crate::server::tileset::preview::namespaced_preview_style_handler,
        crate::server::tileset::tile::tile_handler,
        crate::server::tileset::tile::namespaced_tile_handler,
        crate::server::tileset::terrain::derived_tilejson_handler,
        crate::server::tileset::terrain::namespaced_derived_tilejson_handler,
        crate::server::tileset::terrain::derived_tile_handler,
        crate::server::tileset::terrain::namespaced_derived_tile_handler,
        crate::server::glyph::glyph_handler,
    )
)]
pub(crate) struct DeliveryApi;

/// Publishes the two accepted delivery-credential transports. A request must
/// carry at most one of them; supplying both is rejected.
struct DeliveryCredentials;

impl utoipa::Modify for DeliveryCredentials {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::PartialSchema;
        use utoipa::openapi::{
            Content, Ref, RefOr,
            response::ResponseBuilder,
            security::{ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, SecurityScheme},
        };

        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer_token",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some("Preferred transport for a delivery key."))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "access_token_query",
            SecurityScheme::ApiKey(ApiKey::Query(ApiKeyValue::with_description(
                "access_token",
                "Fallback for clients that cannot set a header. Requires \
                 URL-log redaction and CDN cache-key review before use.",
            ))),
        );

        // Authentication, admission, and shared extraction layers can reject
        // every delivery request before it reaches a handler. Add those common
        // responses once rather than repeating an incomplete subset on every
        // path annotation.
        for path_item in openapi.paths.paths.values_mut() {
            let Some(operation) = path_item.get.as_mut() else {
                continue;
            };

            // Handler-specific errors are currently plain text. Give every
            // already-declared 4xx/5xx response an actual body schema.
            for (status, response) in &mut operation.responses.responses {
                if !(status.starts_with('4') || status.starts_with('5')) {
                    continue;
                }
                if let RefOr::T(response) = response
                    && response.content.is_empty()
                {
                    response.content.insert(
                        "text/plain".to_string(),
                        Content::new(Some(String::schema())),
                    );
                }
            }

            for (status, description, json, text) in [
                ("400", "Invalid request", true, true),
                ("401", "Missing or invalid delivery credential", true, false),
                (
                    "403",
                    "Credential is not permitted to read this resource",
                    true,
                    false,
                ),
                ("429", "Request rejected by admission control", false, true),
                ("503", "Delivery service is unavailable", true, true),
            ] {
                let response = operation
                    .responses
                    .responses
                    .entry(status.to_string())
                    .or_insert_with(|| {
                        RefOr::T(ResponseBuilder::new().description(description).build())
                    });
                let RefOr::T(response) = response else {
                    continue;
                };
                if json {
                    response
                        .content
                        .entry("application/json".to_string())
                        .or_insert_with(|| {
                            Content::new(Some(Ref::from_schema_name("DeliveryError")))
                        });
                }
                if text {
                    response
                        .content
                        .entry("text/plain".to_string())
                        .or_insert_with(|| Content::new(Some(String::schema())));
                }
            }
        }
    }
}

/// Real delivery routes that OpenAPI cannot represent without lying.
///
/// An OpenAPI path parameter matches one path segment, while Axum's wildcard
/// style path can contain arbitrarily many slash-separated segments. Omitting
/// it prevents generated clients from producing URLs that work only for
/// single-segment style keys. The catalog and prose contract remain the
/// discovery mechanism for styles.
#[cfg(test)]
const OPENAPI_UNREPRESENTABLE_PATHS: &[&str] = &["/styles/{*style_path}"];

pub(crate) fn openapi_json() -> serde_json::Value {
    use utoipa::OpenApi;
    serde_json::to_value(DeliveryApi::openapi()).expect("OpenAPI is always serialisable")
}

/// Emits an artifact when invoked as `ishikari gen-schemas [config|openapi]`.
///
/// Returns `None` for an ordinary server start so the caller proceeds normally.
/// This is a build-time entry point, deliberately checked before any
/// configuration is resolved and absent from a served build.
pub(crate) fn emit_if_requested() -> Option<anyhow::Result<()>> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("gen-schemas") {
        return None;
    }
    let value = match args.next().as_deref() {
        Some("config") => config_json_schema(),
        Some("openapi") => openapi_json(),
        None => serde_json::json!({
            "config": config_json_schema(),
            "openapi": openapi_json(),
        }),
        Some(other) => {
            return Some(Err(anyhow::anyhow!(
                "unknown artifact {other:?}; expected `config` or `openapi`"
            )));
        }
    };
    Some(
        serde_json::to_string_pretty(&value)
            .map(|text| println!("{text}"))
            .map_err(anyhow::Error::from),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{config_json_schema, openapi_json};

    /// The precedence rule in `config_file` only holds while every documented
    /// setting maps to a flag with no built-in default: otherwise the flag is
    /// always `Some`, the document value is never applied, and the key becomes
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
    fn config_schema_describes_the_parsed_document_type() {
        let schema = config_json_schema();
        let properties = schema
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .expect("config schema exposes properties");
        assert!(properties.contains_key("schema_version"), "{schema}");
        assert!(properties.contains_key("mapterhorn_tileset"), "{schema}");
        let required = schema
            .get("required")
            .and_then(serde_json::Value::as_array)
            .expect("schema has required fields");
        assert!(
            required.iter().any(|field| field == "schema_version"),
            "{schema}"
        );
        assert_eq!(
            properties["schema_version"].get("const"),
            Some(&serde_json::json!(
                super::super::config_file::SCHEMA_VERSION
            )),
            "{schema}"
        );
        assert_eq!(
            properties["mapterhorn_maxzoom"].get("minimum"),
            Some(&serde_json::json!(13)),
            "{schema}"
        );
        assert_eq!(
            properties["mapterhorn_maxzoom"].get("maximum"),
            Some(&serde_json::json!(30)),
            "{schema}"
        );
        assert_eq!(
            schema["dependentRequired"]["mapterhorn_tileset"],
            serde_json::json!(["mapterhorn_maxzoom"]),
            "{schema}"
        );
        assert_eq!(
            schema["dependentRequired"]["mapterhorn_maxzoom"],
            serde_json::json!(["mapterhorn_tileset"]),
            "{schema}"
        );
        // The document must not gain a key for a setting that already has a
        // built-in default, or file/flag precedence stops being decidable.
        assert!(!properties.contains_key("http_port"), "{schema}");
        // Nor a credential-bearing provider template, nor a per-replica
        // identity: both would break guardrails the module documents.
        for excluded in [
            "style_templates",
            "glyph_url_template",
            "sprite_templates",
            "node_id",
            "gossip_advertise_addr",
            "internal_http_advertise_addr",
        ] {
            assert!(!properties.contains_key(excluded), "{excluded}: {schema}");
        }
    }

    #[test]
    fn openapi_publishes_both_accepted_credential_transports() {
        let api = openapi_json();
        let schemes = api
            .get("components")
            .and_then(|components| components.get("securitySchemes"))
            .and_then(serde_json::Value::as_object)
            .expect("OpenAPI declares security schemes");
        assert!(schemes.contains_key("bearer_token"), "{api}");
        assert!(schemes.contains_key("access_token_query"), "{api}");

        let security = api
            .get("security")
            .and_then(serde_json::Value::as_array)
            .expect("OpenAPI declares the optional security alternatives");
        assert!(
            security.iter().any(|requirement| requirement
                .as_object()
                .is_some_and(|value| value.is_empty())),
            "credential-free deployments must remain representable: {api}"
        );
        for scheme in ["bearer_token", "access_token_query"] {
            assert!(
                security.iter().any(|requirement| requirement
                    .as_object()
                    .is_some_and(|value| value.contains_key(scheme))),
                "global security omits {scheme}: {api}"
            );
        }
    }

    #[test]
    fn openapi_matches_every_representable_public_delivery_route() {
        let api = openapi_json();
        let paths = api
            .get("paths")
            .and_then(serde_json::Value::as_object)
            .expect("OpenAPI exposes paths");

        let expected = crate::server::PUBLIC_DELIVERY_ROUTE_PATHS
            .iter()
            .filter(|path| !super::OPENAPI_UNREPRESENTABLE_PATHS.contains(path))
            .copied()
            .collect::<BTreeSet<_>>();
        let described = paths.keys().map(String::as_str).collect::<BTreeSet<_>>();
        assert_eq!(
            described, expected,
            "OpenAPI and the real public delivery router diverged"
        );
        assert!(
            !paths.contains_key("/styles/{style_path}")
                && !paths.contains_key("/styles/{*style_path}"),
            "a slash-spanning wildcard cannot be represented honestly in OpenAPI: {api}"
        );
    }

    #[test]
    fn successful_responses_declare_a_body_schema() {
        let api = openapi_json();
        let paths = api["paths"].as_object().expect("OpenAPI exposes paths");
        for (path, item) in paths {
            let response = &item["get"]["responses"]["200"];
            let content = response["content"]
                .as_object()
                .unwrap_or_else(|| panic!("{path} has no successful response content: {response}"));
            assert!(!content.is_empty(), "{path} has no successful media type");
            for (media_type, body) in content {
                assert!(
                    body.get("schema").is_some_and(|schema| !schema.is_null()),
                    "{path} response {media_type} has no schema: {body}"
                );
            }
        }
    }

    #[test]
    fn opaque_payloads_use_the_openapi_binary_schema() {
        let api = openapi_json();
        let binary = &api["components"]["schemas"]["BinaryPayload"];
        assert_eq!(binary["type"], "string", "{binary}");
        assert_eq!(binary["format"], "binary", "{binary}");

        for path in [
            "/fonts/{fontstack}/{range}",
            "/tilesets/{tileset_id}/{z}/{x}/{y}",
            "/tilesets/{tileset_id}/derived/{product}/{z}/{x}/{y}",
        ] {
            let content = api["paths"][path]["get"]["responses"]["200"]["content"]
                .as_object()
                .unwrap_or_else(|| panic!("{path} has no response content"));
            for (media_type, body) in content {
                assert_eq!(
                    body["schema"]["$ref"], "#/components/schemas/BinaryPayload",
                    "{path} response {media_type} is not modeled as opaque bytes: {body}"
                );
            }
        }
    }

    #[test]
    fn tile_responses_describe_every_served_media_type() {
        let api = openapi_json();
        for path in [
            "/tilesets/{tileset_id}/{z}/{x}/{y}",
            "/tilesets/{namespace}/{tileset_id}/{z}/{x}/{y}",
        ] {
            let content = api["paths"][path]["get"]["responses"]["200"]["content"]
                .as_object()
                .unwrap_or_else(|| panic!("{path} has no response content"));
            for media_type in [
                "application/vnd.mapbox-vector-tile",
                "application/vnd.maplibre-tile",
                "image/png",
                "image/jpeg",
                "image/webp",
                "image/avif",
                "application/octet-stream",
            ] {
                assert!(content.contains_key(media_type), "{path}: {media_type}");
            }
        }
        for path in [
            "/tilesets/{tileset_id}/derived/{product}/{z}/{x}/{y}",
            "/tilesets/{namespace}/{tileset_id}/derived/{product}/{z}/{x}/{y}",
        ] {
            let content = api["paths"][path]["get"]["responses"]["200"]["content"]
                .as_object()
                .unwrap_or_else(|| panic!("{path} has no response content"));
            for media_type in [
                "application/vnd.mapbox-vector-tile",
                "application/vnd.maplibre-tile",
                "image/webp",
                "image/jpeg",
            ] {
                assert!(content.contains_key(media_type), "{path}: {media_type}");
            }
        }
    }

    #[test]
    fn every_operation_describes_shared_rejection_contracts() {
        let api = openapi_json();
        let paths = api["paths"].as_object().expect("OpenAPI exposes paths");
        for (path, item) in paths {
            let responses = item["get"]["responses"]
                .as_object()
                .unwrap_or_else(|| panic!("{path} has no responses"));
            for status in ["400", "401", "403", "429", "503"] {
                let content = responses[status]["content"]
                    .as_object()
                    .unwrap_or_else(|| panic!("{path} response {status} has no declared content"));
                assert!(
                    !content.is_empty(),
                    "{path} response {status} has no media type"
                );
                for (media_type, body) in content {
                    assert!(
                        body.get("schema").is_some_and(|schema| !schema.is_null()),
                        "{path} response {status} {media_type} has no schema"
                    );
                }
            }
        }

        let error_schema = &api["components"]["schemas"]["DeliveryError"];
        assert!(
            error_schema["properties"]["error"].is_object(),
            "the machine-readable auth error schema is missing: {error_schema}"
        );
        let unexpected = paths
            .keys()
            .filter(|path| path.starts_with("/_internal/"))
            .collect::<Vec<_>>();
        assert!(
            unexpected.is_empty(),
            "operational and cluster-internal routes must stay undescribed: {unexpected:?}"
        );
    }
}
