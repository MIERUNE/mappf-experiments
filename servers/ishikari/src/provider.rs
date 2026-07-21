//! Validated provider-resource URL and path contract.

use mmpf_common::resource_templates::{
    NamespaceKeyPolicy, ResourceTemplates, TemplatePolicy, validate_url_template,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct ProviderConfig {
    style_templates: ResourceTemplates,
    glyph_url_template: Option<String>,
    sprite_templates: ResourceTemplates,
}

impl ProviderConfig {
    pub(crate) fn new(
        style_templates: Option<String>,
        glyph_url_template: Option<String>,
        sprite_templates: Option<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            style_templates: style_templates
                .map(|raw| parse_resource_templates(&raw, "STYLE_TEMPLATES"))
                .transpose()?
                .unwrap_or_default(),
            glyph_url_template: glyph_url_template
                .map(|template| validate_glyph_template(&template))
                .transpose()?,
            sprite_templates: sprite_templates
                .map(|raw| parse_resource_templates(&raw, "SPRITE_TEMPLATES"))
                .transpose()?
                .unwrap_or_default(),
        })
    }

    pub(crate) fn resolve_style_url(&self, style_key: &str) -> Option<String> {
        self.style_templates
            .resolve_with(style_key, path_percent_encode_segments)
    }

    pub(crate) fn resolve_glyph_url(&self, fontstack: &str, range: &str) -> Option<String> {
        let template = self.glyph_url_template.as_ref()?;
        Some(
            template
                .replace("{fontstack}", &path_percent_encode(fontstack))
                .replace("{range}", range),
        )
    }

    pub(crate) fn resolve_sprite_url(&self, style_key: &str, suffix: &str) -> Option<String> {
        self.sprite_templates
            .resolve_with(style_key, path_percent_encode_segments)
            .map(|base| format!("{base}{suffix}"))
    }

    /// Whether a glyph upstream is configured, so glyph URLs can be served here.
    pub(crate) fn has_glyph_provider(&self) -> bool {
        self.glyph_url_template.is_some()
    }

    /// Whether a sprite upstream resolves for this style key, so its sprite URL
    /// can be served here.
    pub(crate) fn has_sprite_provider(&self, style_key: &str) -> bool {
        self.sprite_templates.has_match(style_key)
    }
}

fn parse_resource_templates(raw: &str, config_name: &str) -> Result<ResourceTemplates, String> {
    ResourceTemplates::parse(
        raw,
        TemplatePolicy {
            config_name,
            placeholder: "{style_id}",
            // A fixed URL is valid for shared sprite sheets.
            require_placeholder: false,
            placeholder_must_be_in_path: true,
            allowed_schemes: &["http", "https", "gs", "s3"],
            namespace_keys: NamespaceKeyPolicy::AsciiIdentifier,
        },
    )
    .map_err(|error| error.to_string())
}

fn validate_glyph_template(template: &str) -> Result<String, String> {
    const GLYPH_PLACEHOLDERS: [(&str, &str); 2] =
        [("{fontstack}", "Noto%20Sans"), ("{range}", "0-255")];

    for (placeholder, _) in GLYPH_PLACEHOLDERS {
        if !template.contains(placeholder) {
            return Err(format!("template must contain {placeholder}"));
        }
    }

    for (placeholder, _) in GLYPH_PLACEHOLDERS {
        let mut sample = template.to_string();
        for (other, replacement) in GLYPH_PLACEHOLDERS {
            if other != placeholder {
                sample = sample.replace(other, replacement);
            }
        }
        validate_url_template(
            &sample,
            "default",
            TemplatePolicy {
                config_name: "GLYPH_URL_TEMPLATE",
                placeholder,
                require_placeholder: true,
                placeholder_must_be_in_path: true,
                // `gs`/`s3` are read through object_store with ambient
                // credentials; `http(s)` are fetched directly.
                allowed_schemes: &["http", "https", "gs", "s3"],
                namespace_keys: NamespaceKeyPolicy::AsciiIdentifier,
            },
        )
        .map_err(|error| error.to_string())?;
    }

    Ok(template.to_string())
}

pub(crate) fn path_percent_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b',') {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }
    out
}

pub(crate) fn path_percent_encode_segments(value: &str) -> String {
    value
        .split('/')
        .map(path_percent_encode)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::ProviderConfig;

    #[test]
    fn style_templates_strip_matched_namespace() {
        let config = ProviderConfig::new(
            Some(
                "carto=https://styles.example/carto/{style_id}/style.json;default=https://styles.example/{style_id}/style.json"
                    .to_string(),
            ),
            None,
            None,
        )
        .unwrap();

        assert_eq!(
            config.resolve_style_url("carto/voyager light").as_deref(),
            Some("https://styles.example/carto/voyager%20light/style.json")
        );
        assert_eq!(
            config.resolve_style_url("demo/basic").as_deref(),
            Some("https://styles.example/demo/basic/style.json")
        );
        assert_eq!(
            config.resolve_style_url("demo/basic?x=1#frag").as_deref(),
            Some("https://styles.example/demo/basic%3Fx%3D1%23frag/style.json")
        );
    }

    #[test]
    fn glyph_template_percent_encodes_fontstack() {
        let config = ProviderConfig::new(
            None,
            Some("https://glyphs.example/{fontstack}/{range}.pbf".to_string()),
            None,
        )
        .unwrap();

        assert_eq!(
            config
                .resolve_glyph_url("Noto Sans JP,Arial", "0-255")
                .as_deref(),
            Some("https://glyphs.example/Noto%20Sans%20JP,Arial/0-255.pbf")
        );
    }

    #[test]
    fn sprite_templates_append_requested_suffix() {
        let config = ProviderConfig::new(
            None,
            None,
            Some("carto=https://sprites.example/{style_id}/sprite".to_string()),
        )
        .unwrap();

        assert_eq!(
            config
                .resolve_sprite_url("carto/voyager", "@2x.png")
                .as_deref(),
            Some("https://sprites.example/voyager/sprite@2x.png")
        );
    }

    #[test]
    fn rejects_style_and_sprite_placeholders_outside_the_path() {
        for invalid in [
            "https://{style_id}.example/resource",
            "https://resources.example/resource?id={style_id}",
            "https://resources.example/resource#{style_id}",
        ] {
            assert!(
                ProviderConfig::new(Some(invalid.to_string()), None, None).is_err(),
                "accepted style template {invalid}"
            );
            assert!(
                ProviderConfig::new(None, None, Some(invalid.to_string())).is_err(),
                "accepted sprite template {invalid}"
            );
        }
    }

    #[test]
    fn rejects_glyph_placeholders_outside_the_path() {
        for invalid in [
            "https://{fontstack}.example/fonts/{range}.pbf",
            "https://glyphs.example/fonts/{range}.pbf?font={fontstack}",
            "https://glyphs.example/fonts/{range}.pbf#{fontstack}",
            "https://{range}.example/fonts/{fontstack}.pbf",
            "https://glyphs.example/fonts/{fontstack}.pbf?range={range}",
            "https://glyphs.example/fonts/{fontstack}.pbf#{range}",
        ] {
            assert!(
                ProviderConfig::new(None, Some(invalid.to_string()), None).is_err(),
                "accepted glyph template {invalid}"
            );
        }
    }

    #[test]
    fn fixed_style_and_sprite_templates_remain_valid() {
        let config = ProviderConfig::new(
            Some("https://styles.example/shared/style.json".to_string()),
            None,
            Some("https://sprites.example/shared/sprite".to_string()),
        )
        .unwrap();

        assert_eq!(
            config.resolve_style_url("any/style").as_deref(),
            Some("https://styles.example/shared/style.json")
        );
        assert_eq!(
            config.resolve_sprite_url("any/style", ".json").as_deref(),
            Some("https://sprites.example/shared/sprite.json")
        );
    }

    #[test]
    fn accepts_object_store_schemes() {
        // `gs://` (and `s3://`) templates are read via object_store with ambient
        // credentials, so they must validate alongside `http(s)`.
        let config = ProviderConfig::new(
            Some("gs://bucket/styles/{style_id}/style.json".to_string()),
            Some("gs://bucket/fonts/{fontstack}/{range}.pbf".to_string()),
            Some("s3://bucket/sprite/sprite".to_string()),
        )
        .unwrap();

        assert_eq!(
            config.resolve_style_url("hokkaido").as_deref(),
            Some("gs://bucket/styles/hokkaido/style.json")
        );
        assert_eq!(
            config.resolve_sprite_url("hokkaido", "@2x.png").as_deref(),
            Some("s3://bucket/sprite/sprite@2x.png")
        );
    }
}
