use crate::http::error::{IngressError, invalid};
use crate::http::parse_util::percent_decode_str;
use crate::tileset_catalog::TilesetCatalog;
use crate::types::{AddLayer, AddLayerSource};

/// Largest decoded `addlayer` JSON we accept (bytes, after percent-decode).
/// Keeps a single request from carrying an unbounded style fragment.
pub(crate) const MAX_ADDLAYER_JSON_BYTES: usize = 4096;
/// Maximum object / array nesting depth in `addlayer` JSON. Caps recursive
/// validation cost and protects the style-spec converter from pathological
/// input.
pub(crate) const MAX_ADDLAYER_JSON_DEPTH: usize = 16;
/// Maximum `id` / `source-layer` length in bytes.
const MAX_ADDLAYER_STRING_LEN: usize = 64;
/// `id` namespace reserved for biei-managed layers; users may not place
/// addlayer ids in this prefix.
const ADDLAYER_BIEI_ID_PREFIX: &str = "__biei_";
/// Layer types accepted by the addlayer v0 path. Symbol / background /
/// raster / heatmap / fill-extrusion / hillshade are reserved for later
/// phases that need additional plumbing (icon registry, etc.).
const ADDLAYER_ALLOWED_TYPES: &[&str] = &["fill", "line", "circle"];

/// Extract `addlayer={percent-encoded JSON}` from a query string. At most
/// one `addlayer` parameter is allowed per request (static image API
/// rule). Returns `Ok(None)` if not set.
pub(crate) fn parse_addlayer_from_query(
    query: Option<&str>,
    tileset_catalog: &TilesetCatalog,
) -> Result<Option<AddLayer>, IngressError> {
    let Some(q) = query else {
        return Ok(None);
    };
    let mut found: Option<&str> = None;
    for pair in q.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if key != "addlayer" {
            continue;
        }
        if found.is_some() {
            return Err(invalid(
                "at most one addlayer parameter is allowed per request",
            ));
        }
        found = Some(value);
    }
    let Some(encoded) = found else {
        return Ok(None);
    };
    let decoded = percent_decode_str(encoded)
        .map_err(|_| invalid("addlayer must be valid percent-encoded UTF-8"))?;
    if decoded.is_empty() {
        return Err(invalid("addlayer JSON must not be empty"));
    }
    if decoded.len() > MAX_ADDLAYER_JSON_BYTES {
        return Err(invalid(format!(
            "addlayer JSON must be at most {MAX_ADDLAYER_JSON_BYTES} bytes"
        )));
    }
    let mut value: serde_json::Value = serde_json::from_str(&decoded)
        .map_err(|e| invalid(format!("addlayer JSON parse error: {e}")))?;
    let source = validate_and_rewrite_addlayer_json(&mut value, tileset_catalog)?;
    let json = serde_json::to_string(&value)
        .map_err(|e| invalid(format!("addlayer JSON serialize error: {e}")))?;
    let hash = stable_hash_u64(json.as_bytes());
    Ok(Some(AddLayer { json, hash, source }))
}

fn validate_and_rewrite_addlayer_json(
    value: &mut serde_json::Value,
    tileset_catalog: &TilesetCatalog,
) -> Result<Option<AddLayerSource>, IngressError> {
    check_json_depth(value, MAX_ADDLAYER_JSON_DEPTH)?;
    let obj = value
        .as_object_mut()
        .ok_or_else(|| invalid("addlayer must be a JSON object"))?;
    let id = obj
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("addlayer requires a string `id`"))?;
    if id.is_empty() || id.len() > MAX_ADDLAYER_STRING_LEN {
        return Err(invalid(format!(
            "addlayer `id` must be 1..={MAX_ADDLAYER_STRING_LEN} bytes"
        )));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
    {
        return Err(invalid("addlayer `id` may only contain [A-Za-z0-9-_.:]"));
    }
    if id.starts_with(ADDLAYER_BIEI_ID_PREFIX) {
        return Err(invalid(format!(
            "addlayer `id` may not start with the reserved prefix `{ADDLAYER_BIEI_ID_PREFIX}`"
        )));
    }

    let layer_type = obj
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("addlayer requires a string `type`"))?;
    if !ADDLAYER_ALLOWED_TYPES.contains(&layer_type) {
        return Err(invalid(format!(
            "addlayer `type` must be one of {:?}; got `{layer_type}`",
            ADDLAYER_ALLOWED_TYPES
        )));
    }

    let source = obj
        .get("source")
        .ok_or_else(|| invalid("addlayer requires a `source`"))?;
    let rewritten_source = match source {
        serde_json::Value::String(s) => {
            if s.is_empty() || s.len() > MAX_ADDLAYER_STRING_LEN {
                return Err(invalid(format!(
                    "addlayer `source` must be 1..={MAX_ADDLAYER_STRING_LEN} bytes"
                )));
            }
            None
        }
        serde_json::Value::Object(source_obj) => {
            Some(rewrite_addlayer_source_object(source_obj, tileset_catalog)?)
        }
        _ => return Err(invalid("addlayer `source` must be a string or object")),
    };

    if let Some(sl) = obj.get("source-layer") {
        let sl = sl
            .as_str()
            .ok_or_else(|| invalid("addlayer `source-layer` must be a string"))?;
        if sl.is_empty() || sl.len() > MAX_ADDLAYER_STRING_LEN {
            return Err(invalid(format!(
                "addlayer `source-layer` must be 1..={MAX_ADDLAYER_STRING_LEN} bytes"
            )));
        }
    }

    for key in ["minzoom", "maxzoom"] {
        if let Some(z) = obj.get(key) {
            let z = z
                .as_f64()
                .ok_or_else(|| invalid(format!("addlayer `{key}` must be a number")))?;
            if !(0.0..=24.0).contains(&z) {
                return Err(invalid(format!("addlayer `{key}` must be in [0, 24]")));
            }
        }
    }
    Ok(rewritten_source)
}

fn rewrite_addlayer_source_object(
    source_obj: &serde_json::Map<String, serde_json::Value>,
    tileset_catalog: &TilesetCatalog,
) -> Result<AddLayerSource, IngressError> {
    let source_type = source_obj
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("addlayer `source.type` must be a string"))?;
    if source_type != "vector" {
        return Err(invalid(
            "addlayer `source` objects currently support only vector sources",
        ));
    }
    let tileset_id = source_obj
        .get("url")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("addlayer `source.url` must be a tileset id string"))?;
    validate_tileset_id(tileset_id)?;

    let mut resolved = serde_json::Map::new();
    resolved.insert("type".to_string(), serde_json::json!("vector"));
    resolved.insert(
        "url".to_string(),
        serde_json::json!(tileset_catalog.resolve_url(tileset_id)),
    );
    for key in ["minzoom", "maxzoom", "attribution", "bounds", "scheme"] {
        if let Some(value) = source_obj.get(key) {
            resolved.insert(key.to_string(), value.clone());
        }
    }
    let json = serde_json::to_string(&serde_json::Value::Object(resolved))
        .map_err(|e| invalid(format!("addlayer source JSON serialize error: {e}")))?;
    Ok(AddLayerSource {
        tileset_id: tileset_id.to_string(),
        json,
    })
}

fn validate_tileset_id(value: &str) -> Result<(), IngressError> {
    if value.is_empty() || value.len() > MAX_ADDLAYER_STRING_LEN {
        return Err(invalid(format!(
            "addlayer `source.url` tileset id must be 1..={MAX_ADDLAYER_STRING_LEN} bytes"
        )));
    }
    if value.starts_with("http://") || value.starts_with("https://") {
        return Err(invalid(
            "addlayer `source.url` must be a biei tileset id, not a direct URL",
        ));
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b'/'))
    {
        return Err(invalid(
            "addlayer `source.url` tileset id contains an unsupported character",
        ));
    }
    Ok(())
}

fn check_json_depth(value: &serde_json::Value, max_depth: usize) -> Result<(), IngressError> {
    fn walk(value: &serde_json::Value, depth: usize, max: usize) -> Result<(), IngressError> {
        if depth > max {
            return Err(invalid(format!(
                "addlayer JSON nesting depth must be at most {max}"
            )));
        }
        match value {
            serde_json::Value::Object(map) => {
                for (_, v) in map {
                    walk(v, depth + 1, max)?;
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    walk(item, depth + 1, max)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    walk(value, 0, max_depth)
}

fn stable_hash_u64(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_tileset_catalog() -> TilesetCatalog {
        TilesetCatalog::new("https://tiles.example.test/{tileset_id}/tileset.json")
    }

    fn parse(query: Option<&str>) -> Result<Option<AddLayer>, IngressError> {
        parse_addlayer_from_query(query, &test_tileset_catalog())
    }

    fn encode_addlayer(json: &str) -> String {
        // Percent-encode the characters that would otherwise break the
        // outer query-string (`%`, `&`, `=`, `+`, `#`). The tests below
        // exercise both compact and expanded JSON via this helper.
        let mut out = String::new();
        for b in json.as_bytes() {
            match *b {
                b'%' => out.push_str("%25"),
                b'&' => out.push_str("%26"),
                b'=' => out.push_str("%3D"),
                b'+' => out.push_str("%2B"),
                b'#' => out.push_str("%23"),
                b' ' => out.push_str("%20"),
                _ => out.push(*b as char),
            }
        }
        out
    }

    fn addlayer_query(json: &str) -> String {
        format!("addlayer={}", encode_addlayer(json))
    }

    #[test]
    fn parses_valid_addlayer_from_query() {
        let json = r##"{"id":"my-fill","type":"fill","source":"composite","paint":{"fill-color":"#ff0000"}}"##;
        let layer = parse(Some(&addlayer_query(json)))
            .expect("valid addlayer parses")
            .expect("layer present");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&layer.json).unwrap(),
            serde_json::from_str::<serde_json::Value>(json).unwrap()
        );
        let again = parse(Some(&addlayer_query(json))).unwrap().unwrap();
        assert_eq!(layer.hash, again.hash);
    }

    #[test]
    fn addlayer_absent_returns_none() {
        assert!(parse(None).unwrap().is_none());
        assert!(parse(Some("padding=10")).unwrap().is_none());
    }

    #[test]
    fn rejects_multiple_addlayer_params() {
        let json = r#"{"id":"x","type":"fill","source":"s"}"#;
        let q = format!("{}&{}", addlayer_query(json), addlayer_query(json));
        let err = parse(Some(&q)).expect_err("multiple addlayer rejected");
        assert!(err.to_string().contains("at most one"));
    }

    #[test]
    fn rejects_oversize_addlayer_json() {
        let big = "x".repeat(MAX_ADDLAYER_JSON_BYTES + 100);
        let json = format!(r#"{{"id":"x","type":"fill","source":"s","metadata":"{big}"}}"#);
        let err = parse(Some(&addlayer_query(&json))).expect_err("oversize addlayer rejected");
        assert!(err.to_string().contains("at most"));
    }

    #[test]
    fn rejects_deeply_nested_addlayer_json() {
        let mut nested = String::from(r#"{"id":"x","type":"fill","source":"s","paint":{"a":"#);
        let depth = MAX_ADDLAYER_JSON_DEPTH + 5;
        for _ in 0..depth {
            nested.push('[');
        }
        nested.push('0');
        for _ in 0..depth {
            nested.push(']');
        }
        nested.push_str("}}");
        let err = parse(Some(&addlayer_query(&nested))).expect_err("nesting depth rejected");
        assert!(err.to_string().contains("nesting"));
    }

    #[test]
    fn rejects_addlayer_disallowed_type() {
        for ty in [
            "background",
            "raster",
            "heatmap",
            "fill-extrusion",
            "symbol",
        ] {
            let json = format!(r#"{{"id":"x","type":"{ty}","source":"s"}}"#);
            assert!(
                parse(Some(&addlayer_query(&json))).is_err(),
                "type `{ty}` should be rejected"
            );
        }
    }

    #[test]
    fn addlayer_source_url_is_resolved_to_tileset_json_url() {
        let json = r#"{"id":"x","type":"fill","source":{"type":"vector","url":"weather-tiles"}}"#;
        let layer = parse(Some(&addlayer_query(json)))
            .expect("addlayer parses")
            .expect("layer present");
        let source = layer.source.expect("source object is carried separately");
        assert_eq!(source.tileset_id, "weather-tiles");
        let source_json: serde_json::Value =
            serde_json::from_str(&source.json).expect("source JSON");
        assert_eq!(source_json["type"], serde_json::json!("vector"));
        assert_eq!(
            source_json["url"],
            serde_json::json!("https://tiles.example.test/weather-tiles/tileset.json")
        );
        let layer_json: serde_json::Value = serde_json::from_str(&layer.json).expect("layer JSON");
        assert!(layer_json["source"].is_object());
    }

    #[test]
    fn addlayer_source_url_accepts_namespaced_tileset_id() {
        let json = r#"{"id":"x","type":"fill","source":{"type":"vector","url":"analysis/hrnowc/sample","minzoom":4,"maxzoom":12,"scheme":"xyz"},"source-layer":"layer"}"#;
        let layer = parse(Some(&addlayer_query(json)))
            .expect("addlayer parses")
            .expect("layer present");
        let source = layer.source.expect("source object is carried separately");
        assert_eq!(source.tileset_id, "analysis/hrnowc/sample");

        let source_json: serde_json::Value =
            serde_json::from_str(&source.json).expect("source JSON");
        assert_eq!(
            source_json["url"],
            serde_json::json!("https://tiles.example.test/analysis/hrnowc/sample/tileset.json")
        );
        assert_eq!(source_json["minzoom"], serde_json::json!(4));
        assert_eq!(source_json["maxzoom"], serde_json::json!(12));
        assert_eq!(source_json["scheme"], serde_json::json!("xyz"));
    }

    #[test]
    fn rejects_addlayer_source_url_direct_network_url() {
        let json = r#"{"id":"x","type":"fill","source":{"type":"vector","url":"https://example.test/tiles.json"}}"#;
        let err = parse(Some(&addlayer_query(json))).expect_err("direct source URL rejected");
        assert!(err.to_string().contains("not a direct URL"));
    }

    #[test]
    fn rejects_addlayer_non_vector_source_object() {
        let json = r#"{"id":"x","type":"fill","source":{"type":"raster","url":"tiles"}}"#;
        let err =
            parse(Some(&addlayer_query(json))).expect_err("non-vector source object rejected");
        assert!(err.to_string().contains("only vector sources"));
    }

    #[test]
    fn rejects_addlayer_id_with_biei_prefix() {
        let json = r#"{"id":"__biei_user","type":"fill","source":"s"}"#;
        let err = parse(Some(&addlayer_query(json))).expect_err("biei prefix rejected");
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn rejects_addlayer_id_with_bad_charset() {
        let json = r#"{"id":"my fill","type":"fill","source":"s"}"#;
        let err = parse(Some(&addlayer_query(json))).expect_err("space in id rejected");
        assert!(err.to_string().contains("id"));
    }

    #[test]
    fn rejects_addlayer_with_missing_required_fields() {
        assert!(parse(Some(&addlayer_query(r#"{"type":"fill","source":"s"}"#))).is_err());
        assert!(parse(Some(&addlayer_query(r#"{"id":"x","source":"s"}"#))).is_err());
        assert!(parse(Some(&addlayer_query(r#"{"id":"x","type":"fill"}"#))).is_err());
    }

    #[test]
    fn rejects_addlayer_with_out_of_range_zoom() {
        let json = r#"{"id":"x","type":"fill","source":"s","minzoom":-1}"#;
        assert!(parse(Some(&addlayer_query(json))).is_err());
        let json = r#"{"id":"x","type":"fill","source":"s","maxzoom":25}"#;
        assert!(parse(Some(&addlayer_query(json))).is_err());
    }
}
