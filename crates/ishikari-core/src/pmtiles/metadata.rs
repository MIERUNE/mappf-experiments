//! PMTiles metadata decoding and TileJSON-facing metadata types.

use std::collections::BTreeMap;

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};

const KNOWN_METADATA_KEYS: &[&str] = &[
    "attribution",
    "bounds",
    "center",
    "description",
    "encoding",
    "format",
    "maxzoom",
    "minzoom",
    "name",
    "tilejson",
    "tiles",
    "tilestats",
    "vector_layers",
    "version",
];

/// Parsed PMTiles metadata document.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Metadata {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub attribution: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    /// Raster-DEM encoding (`terrarium`/`mapbox`) when a producer declares one.
    /// PMTiles headers carry no terrain hint, so this is the only self-describing
    /// source for it.
    #[serde(default)]
    pub encoding: Option<String>,
    #[serde(default)]
    pub vector_layers: Vec<VectorLayer>,
    #[serde(default)]
    pub tilestats: Option<Tilestats>,
    #[serde(default, deserialize_with = "deserialize_optional_metadata_json")]
    pub json: Option<MetadataJson>,
    #[serde(flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

/// TileJSON/vector-layer metadata for a source layer.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct VectorLayer {
    pub id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_i8")]
    pub minzoom: Option<i8>,
    #[serde(default, deserialize_with = "deserialize_optional_i8")]
    pub maxzoom: Option<i8>,
    #[serde(default)]
    pub fields: BTreeMap<String, String>,
}

/// Tile statistics document embedded in PMTiles metadata.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Tilestats {
    pub layer_count: i32,
    pub layers: Vec<TilestatsLayer>,
}

/// Tile statistics for a single vector layer.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TilestatsLayer {
    pub layer: String,
    pub count: i32,
    pub geometry: String,
    pub attribute_count: i32,
    pub attributes: Vec<TilestatsAttribute>,
}

/// Tile statistics for a single vector-layer attribute.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TilestatsAttribute {
    pub attribute: String,
    pub count: u32,
    pub r#type: String,
    #[serde(default)]
    pub values: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

/// Nested metadata blob used by some PMTiles producers.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct MetadataJson {
    #[serde(default)]
    pub encoding: Option<String>,
    #[serde(default)]
    pub vector_layers: Vec<VectorLayer>,
    #[serde(default)]
    pub tilestats: Option<Tilestats>,
    #[serde(flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

impl Metadata {
    /// Estimates the heap footprint of cached metadata.
    pub fn approx_byte_size(&self) -> usize {
        self.name
            .as_ref()
            .map_or(0, |value| std::mem::size_of::<String>() + value.len())
            + self
                .description
                .as_ref()
                .map_or(0, |value| std::mem::size_of::<String>() + value.len())
            + self
                .attribution
                .as_ref()
                .map_or(0, |value| std::mem::size_of::<String>() + value.len())
            + self
                .version
                .as_ref()
                .map_or(0, |value| std::mem::size_of::<String>() + value.len())
            + self
                .encoding
                .as_ref()
                .map_or(0, |value| std::mem::size_of::<String>() + value.len())
            + self.approx_other_size()
            + self
                .vector_layers()
                .iter()
                .map(|layer| {
                    std::mem::size_of::<VectorLayer>()
                        + std::mem::size_of::<String>()
                        + layer.id.len()
                        + layer
                            .description
                            .as_ref()
                            .map_or(0, |value| std::mem::size_of::<String>() + value.len())
                        + layer
                            .fields
                            .iter()
                            .map(|(key, value)| {
                                std::mem::size_of::<String>()
                                    + key.len()
                                    + std::mem::size_of::<String>()
                                    + value.len()
                            })
                            .sum::<usize>()
                })
                .sum::<usize>()
            + self.tilestats().map_or(0, approx_tilestats_size)
    }

    /// Returns vector layers from either the top-level or nested metadata shape.
    pub fn vector_layers(&self) -> &[VectorLayer] {
        if !self.vector_layers.is_empty() {
            self.vector_layers.as_slice()
        } else {
            self.json
                .as_ref()
                .map_or(&[], |json| json.vector_layers.as_slice())
        }
    }

    /// Returns tilestats from either the top-level or nested metadata shape.
    pub fn tilestats(&self) -> Option<&Tilestats> {
        self.tilestats
            .as_ref()
            .or_else(|| self.json.as_ref().and_then(|json| json.tilestats.as_ref()))
    }

    /// Returns the raster-DEM encoding hint from either the top-level or nested
    /// metadata shape, if a producer declared one.
    pub fn encoding(&self) -> Option<&str> {
        self.encoding
            .as_deref()
            .or_else(|| self.json.as_ref().and_then(|json| json.encoding.as_deref()))
    }

    /// Returns unknown metadata fields, including nested JSON metadata extensions.
    pub fn other(&self) -> BTreeMap<String, serde_json::Value> {
        let mut other = self
            .json
            .as_ref()
            .map_or_else(BTreeMap::new, |json| json.other.clone());
        other.extend(self.other.clone());
        for key in KNOWN_METADATA_KEYS {
            other.remove(*key);
        }
        other
    }

    /// Estimates extension fields without materializing the merged `other()`
    /// map. Top-level values override duplicate nested values, matching
    /// `other()` semantics.
    fn approx_other_size(&self) -> usize {
        let top_level = self
            .other
            .iter()
            .filter(|(key, _)| !is_known_metadata_key(key))
            .map(approx_metadata_entry_size)
            .sum::<usize>();
        let nested = self.json.as_ref().map_or(0, |json| {
            json.other
                .iter()
                .filter(|(key, _)| {
                    !is_known_metadata_key(key) && !self.other.contains_key(key.as_str())
                })
                .map(approx_metadata_entry_size)
                .sum::<usize>()
        });
        top_level + nested
    }
}

fn is_known_metadata_key(key: &str) -> bool {
    KNOWN_METADATA_KEYS.contains(&key)
}

fn approx_metadata_entry_size((key, value): (&String, &serde_json::Value)) -> usize {
    std::mem::size_of::<String>() + key.len() + approx_json_value_size(value)
}

/// Deserializes optional i8 fields that may be encoded as strings or numbers.
fn deserialize_optional_i8<'de, D>(deserializer: D) -> Result<Option<i8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    parse_optional_i8(value).map_err(serde::de::Error::custom)
}

/// Deserializes the optional nested metadata JSON blob.
fn deserialize_optional_metadata_json<'de, D>(
    deserializer: D,
) -> Result<Option<MetadataJson>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(text)) => serde_json::from_str(&text)
            .map(Some)
            .map_err(serde::de::Error::custom),
        Some(value) => serde_json::from_value(value)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

/// Parses an optional i8 from flexible JSON metadata input.
fn parse_optional_i8(value: Option<serde_json::Value>) -> anyhow::Result<Option<i8>> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(number)) => number
            .as_i64()
            .ok_or_else(|| anyhow!("invalid integer"))
            .and_then(|value| i8::try_from(value).map_err(|_| anyhow!("integer out of range")))
            .map(Some),
        Some(serde_json::Value::String(text)) => text.parse::<i8>().map(Some).map_err(Into::into),
        _ => bail!("invalid integer"),
    }
}

/// Estimates the heap footprint of a JSON value stored alongside metadata.
fn approx_json_value_size(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) => std::mem::size_of::<bool>(),
        serde_json::Value::Number(_) => std::mem::size_of::<serde_json::Number>(),
        serde_json::Value::String(text) => std::mem::size_of::<String>() + text.len(),
        serde_json::Value::Array(values) => {
            std::mem::size_of::<Vec<serde_json::Value>>()
                + values.iter().map(approx_json_value_size).sum::<usize>()
        }
        serde_json::Value::Object(values) => {
            values
                .iter()
                .map(|(key, value)| {
                    std::mem::size_of::<String>() + key.len() + approx_json_value_size(value)
                })
                .sum::<usize>()
                + std::mem::size_of::<serde_json::Map<String, serde_json::Value>>()
        }
    }
}

/// Estimates the heap footprint of embedded tilestats metadata.
fn approx_tilestats_size(tilestats: &Tilestats) -> usize {
    std::mem::size_of::<Tilestats>()
        + tilestats
            .layers
            .iter()
            .map(|layer| {
                std::mem::size_of::<TilestatsLayer>()
                    + std::mem::size_of::<String>()
                    + layer.layer.len()
                    + std::mem::size_of::<String>()
                    + layer.geometry.len()
                    + layer
                        .attributes
                        .iter()
                        .map(|attribute| {
                            std::mem::size_of::<TilestatsAttribute>()
                                + std::mem::size_of::<String>()
                                + attribute.attribute.len()
                                + std::mem::size_of::<String>()
                                + attribute.r#type.len()
                                + std::mem::size_of::<Vec<serde_json::Value>>()
                                + attribute
                                    .values
                                    .iter()
                                    .map(approx_json_value_size)
                                    .sum::<usize>()
                        })
                        .sum::<usize>()
            })
            .sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::Metadata;

    fn parse(json: &str) -> Metadata {
        serde_json::from_str(json).expect("metadata parses")
    }

    #[test]
    fn encoding_reads_top_level() {
        let meta = parse(r#"{"encoding":"terrarium"}"#);
        assert_eq!(meta.encoding(), Some("terrarium"));
        // It is surfaced as a known field, not duplicated into `other`.
        assert!(!meta.other().contains_key("encoding"));
    }

    #[test]
    fn encoding_falls_back_to_nested_json_string() {
        // PMTiles often stores extensions in a stringified `json` blob.
        let meta = parse(r#"{"json":"{\"encoding\":\"mapbox\"}"}"#);
        assert_eq!(meta.encoding(), Some("mapbox"));
    }

    #[test]
    fn encoding_top_level_wins_over_nested() {
        let meta = parse(r#"{"encoding":"terrarium","json":"{\"encoding\":\"mapbox\"}"}"#);
        assert_eq!(meta.encoding(), Some("terrarium"));
    }

    #[test]
    fn encoding_absent_is_none() {
        // The real mapterhorn metadata: attribution only, no encoding hint, so
        // the TileJSON encoding must come from the style, not the source.
        let meta = parse(
            r#"{"attribution":"<a href=\"https://mapterhorn.com/attribution\">© Mapterhorn</a>"}"#,
        );
        assert_eq!(meta.encoding(), None);
        assert_eq!(
            meta.attribution.as_deref(),
            Some("<a href=\"https://mapterhorn.com/attribution\">© Mapterhorn</a>")
        );
    }

    #[test]
    fn top_level_extension_overrides_nested_extension() {
        let meta = parse(r#"{"custom":"top","json":{"custom":"nested","nested_only":true}}"#);
        let other = meta.other();
        assert_eq!(
            other.get("custom").and_then(|value| value.as_str()),
            Some("top")
        );
        assert_eq!(
            other.get("nested_only").and_then(|value| value.as_bool()),
            Some(true)
        );

        let expected = other
            .iter()
            .map(super::approx_metadata_entry_size)
            .sum::<usize>();
        assert_eq!(meta.approx_other_size(), expected);
    }
}
