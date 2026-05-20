//! Tile preview page support for HTTP ingress.

use crate::http::error::IngressError;
use crate::http::path::resolve_style_id;
use crate::http::response::{IngressResponse, response_from_ingress_error};
use crate::renderer::StyleAvailabilityError;
use crate::style_catalog::StyleCatalog;
use crate::types::{StyleId, StyleRevision};

pub(crate) const PREVIEW_STYLE_CHECK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(5);
const PREVIEW_HTML_TEMPLATE: &str = include_str!("preview.html");

/// Build the preview response. `check_available` confirms the style actually
/// exists at its provider. In URL-template mode `catalog.resolve_latest`
/// accepts any id, so catalog resolution alone cannot distinguish a real style
/// from a typo. Injected as a closure so tests can stub it without standing up
/// a renderer/Node.
pub(crate) async fn build_preview_response<C, Fut>(
    catalog: &StyleCatalog,
    path: &str,
    check_available: C,
) -> IngressResponse
where
    C: FnOnce(StyleRevision) -> Fut,
    Fut: std::future::Future<Output = Result<(), StyleAvailabilityError>>,
{
    let parts: Vec<_> = path
        .trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    // Accepted forms, excluding the trailing `preview` segment:
    //   /{style_id}/preview
    //   /{user}/{style}/preview
    let style_segments: &[&str] = match parts.as_slice() {
        [_style, "preview"] => &parts[..1],
        [_user, _style, "preview"] => &parts[..2],
        _ => {
            return IngressResponse::json(
                400,
                "invalid_preview_path",
                "expected /{user}/{style}/preview",
            );
        }
    };
    let style_id = match resolve_style_id(style_segments) {
        Ok(id) => id,
        Err(err) => return response_from_ingress_error(err),
    };

    let Some(version) = catalog.resolve_latest(&style_id) else {
        return response_from_ingress_error(IngressError::UnknownStyle(style_id));
    };

    let revision = StyleRevision {
        id: style_id.clone(),
        version,
    };
    match check_available(revision).await {
        Ok(()) => preview_html(&style_id),
        Err(StyleAvailabilityError::NotFound(_)) => {
            response_from_ingress_error(IngressError::UnknownStyle(style_id))
        }
        Err(StyleAvailabilityError::Unavailable(_)) => {
            IngressResponse::json(503, "style_unavailable", style_id.as_str())
        }
    }
}

fn preview_html(style_id: &StyleId) -> IngressResponse {
    let style_path = style_id.as_str();
    let html = PREVIEW_HTML_TEMPLATE
        .replace("{{style_path}}", &escape_html(style_path))
        .replace("{{style_path_json}}", &escape_json_string(style_path));
    IngressResponse::html(200, html.into_bytes())
}

/// Escape a string for safe inclusion in HTML text content / attribute values.
/// Conservative set; biei style ids are normally ASCII alphanum + `/` + `-`.
fn escape_html(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// Render a string as a JSON string literal (including the surrounding `"`),
/// suitable for direct embedding inside a `<script>` block.
fn escape_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::style_catalog::StyleDefinition;
    use crate::types::RendererError;

    fn catalog() -> StyleCatalog {
        let catalog = StyleCatalog::new();
        catalog.upsert_definition(
            StyleId("voyager-gl-style".to_string()),
            StyleDefinition::new(
                "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
                1,
            ),
        );
        catalog.upsert_definition(
            StyleId("carto/voyager-gl-style".to_string()),
            StyleDefinition::new(
                "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
                1,
            ),
        );
        catalog
    }

    fn template_catalog() -> StyleCatalog {
        let catalog = StyleCatalog::new();
        catalog.set_url_template("http://provider.test/styles/{style_id}/style.json");
        catalog
    }

    async fn style_available(_revision: StyleRevision) -> Result<(), StyleAvailabilityError> {
        Ok(())
    }

    async fn style_missing(revision: StyleRevision) -> Result<(), StyleAvailabilityError> {
        Err(StyleAvailabilityError::NotFound(
            RendererError::StyleLoadFailed {
                style_id: revision.id,
                source: "provider returned 404".to_string(),
            },
        ))
    }

    async fn style_check_unavailable(
        _revision: StyleRevision,
    ) -> Result<(), StyleAvailabilityError> {
        Err(StyleAvailabilityError::Unavailable(RendererError::Timeout))
    }

    #[tokio::test]
    async fn preview_returns_html_for_known_style() {
        let response = build_preview_response(
            &catalog(),
            "/carto/voyager-gl-style/preview",
            style_available,
        )
        .await;

        assert_eq!(response.status, 200);
        assert_eq!(
            response.content_type,
            crate::http::response::HTML_CONTENT_TYPE
        );
        assert!(
            response
                .headers
                .iter()
                .any(|(name, value)| *name == "Cache-Control" && value == "max-age=300")
        );
        let body = std::str::from_utf8(&response.body).expect("html body");
        assert!(body.contains("maplibre-gl"));
        assert!(body.contains("carto/voyager-gl-style · biei preview"));
        assert!(body.contains(r#""carto/voyager-gl-style""#));
        assert!(body.contains("/{z}/{x}/{y}@2x.webp"));
        assert!(!body.contains("@2x.png"));
    }

    #[tokio::test]
    async fn preview_supports_single_segment_style_id() {
        let response =
            build_preview_response(&catalog(), "/voyager-gl-style/preview", style_available).await;
        assert_eq!(response.status, 200);
        let body = std::str::from_utf8(&response.body).expect("html body");
        assert!(body.contains("voyager-gl-style · biei preview"));
    }

    #[tokio::test]
    async fn preview_returns_404_for_unknown_style() {
        let response =
            build_preview_response(&catalog(), "/unknown/style/preview", style_available).await;

        assert_eq!(response.status, 404);
        assert!(
            std::str::from_utf8(&response.body)
                .expect("json body")
                .contains("unknown_style")
        );
    }

    #[tokio::test]
    async fn preview_returns_404_when_template_style_missing_at_provider() {
        let response =
            build_preview_response(&template_catalog(), "/ghost-style/preview", style_missing)
                .await;

        assert_eq!(response.status, 404);
        assert!(
            std::str::from_utf8(&response.body)
                .expect("json body")
                .contains("unknown_style")
        );
    }

    #[tokio::test]
    async fn preview_serves_html_when_template_style_exists() {
        let response =
            build_preview_response(&template_catalog(), "/real-style/preview", style_available)
                .await;
        assert_eq!(response.status, 200);
        let body = std::str::from_utf8(&response.body).expect("html body");
        assert!(body.contains("real-style · biei preview"));
    }

    #[tokio::test]
    async fn preview_returns_503_when_style_check_is_transiently_unavailable() {
        let response = build_preview_response(
            &template_catalog(),
            "/flaky-style/preview",
            style_check_unavailable,
        )
        .await;
        assert_eq!(response.status, 503);
        assert!(
            std::str::from_utf8(&response.body)
                .expect("json body")
                .contains("style_unavailable")
        );
    }

    #[tokio::test]
    async fn preview_rejects_malformed_path() {
        for path in [
            "/preview",
            "/carto/voyager-gl-style/0/0/0",
            "/carto/voyager-gl-style",
            "/carto/voyager-gl-style/foo/preview",
        ] {
            let response = build_preview_response(&catalog(), path, style_available).await;
            assert_eq!(response.status, 400, "expected 400 for {path}");
            assert!(
                std::str::from_utf8(&response.body)
                    .expect("json body")
                    .contains("invalid_preview_path"),
                "expected invalid_preview_path detail for {path}"
            );
        }
    }

    #[tokio::test]
    async fn preview_rejects_path_traversal_segments() {
        let response =
            build_preview_response(&catalog(), "/../voyager-gl-style/preview", style_available)
                .await;
        assert_eq!(response.status, 400);
    }
}
