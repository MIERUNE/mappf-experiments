//! MapLibre style JSON provider endpoint.

use axum::{
    Extension,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub(crate) struct StyleQuery {
    /// `mlt` rewrites vector tileset sources to MLT (`.mlt` tiles + `encoding:
    /// mlt`); anything else leaves them as MVT. Mirrors the TileJSON endpoint.
    encoding: Option<String>,
}

use crate::provider::{ProviderConfig, path_percent_encode_segments};
use crate::server::{
    AppState, HttpError, apply_origin_vary, auth::PropagatedAccessToken, cache,
    conditional::Validators, get_origin, sprite, tileset::render_preview_html,
    upstream::ProviderResource,
};
use ishikari_core::{interned::TilesetId, storage::ProviderRequest};

const MAX_STYLE_BYTES: usize = 2 * 1024 * 1024;
const STYLE_CONTENT_TYPES: &[&str] = &["application/json", "text/json", "application/octet-stream"];

pub(crate) async fn style_handler(
    State(state): State<AppState>,
    Path(style_path): Path<String>,
    Query(query): Query<StyleQuery>,
    token: Option<Extension<PropagatedAccessToken>>,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    let token = token.as_ref().map(|Extension(token)| token);
    if let Some(style_key) = style_path.strip_suffix("/style.json") {
        return serve_style(
            state,
            style_key.to_string(),
            headers,
            query.encoding,
            token.cloned(),
        )
        .await;
    }
    if let Some(style_key) = style_path.strip_suffix("/preview") {
        return serve_style_preview(style_key, token);
    }
    if let Some(request) = sprite::parse_sprite_path(&style_path) {
        return sprite::serve_sprite(state, request, &headers).await;
    }
    Err((StatusCode::NOT_FOUND, "not found".to_string()))
}

pub(crate) async fn internal_style_handler(
    State(state): State<AppState>,
    Path(style_path): Path<String>,
) -> Result<Response, HttpError> {
    if let Some(style_key) = style_path.strip_suffix("/style.json") {
        validate_style_key(style_key)?;
        let upstream = resolve_style_url(&state, style_key)?;
        let resource = match fetch_style_bytes_local(&state, upstream).await {
            Ok(resource) => resource,
            Err(error) => return crate::server::provider::internal_provider_fetch_error(error),
        };
        state
            .metrics
            .add_internal_bytes(resource.bytes().len() as u64);
        return Ok(resource.internal_response("application/json"));
    }
    if let Some(request) = sprite::parse_sprite_path(&style_path) {
        return sprite::serve_sprite_local(state, request).await;
    }
    Err((StatusCode::NOT_FOUND, "not found".to_string()))
}

/// Serves an HTML MapLibre preview that loads this style. Same-origin, so it
/// needs no CORS, and it references the style by a relative URL so it inherits
/// the page's scheme (no mixed content).
fn serve_style_preview(
    style_key: &str,
    token: Option<&PropagatedAccessToken>,
) -> Result<Response, HttpError> {
    validate_style_key(style_key)?;
    // Default to MVT; the preview's MVT/MLT toggle switches to on-the-fly MLT.
    // The bare `style.json` (external clients, biei) is MVT regardless.
    let mut style_url = format!(
        "/styles/{}/style.json?encoding=mvt",
        path_percent_encode_segments(style_key)
    );
    if let Some(token) = token {
        style_url = token.append_to(&style_url);
    }
    let html = render_preview_html(&format!("style {style_key}"), &style_url, "", true, false);
    Ok(([(header::CACHE_CONTROL, cache::PREVIEW)], Html(html)).into_response())
}

async fn serve_style(
    state: AppState,
    style_key: String,
    headers: HeaderMap,
    encoding: Option<String>,
    token: Option<PropagatedAccessToken>,
) -> Result<Response, HttpError> {
    validate_style_key(&style_key)?;
    let upstream = resolve_style_url(&state, &style_key)?;
    let resource = route_style_bytes(&state, &style_key, &upstream).await?;
    let origin = get_origin(&headers);
    let provider = state.provider.clone();
    let transform_resource = resource.clone();
    let permit = state.admit_cpu_work("style_transform").await?;
    let (body, validators) = tokio::task::spawn_blocking(move || {
        // `spawn_blocking` cannot be cancelled once running. Keep admission in
        // the closure so a disconnected caller cannot release CPU capacity
        // while its transform still occupies a blocking worker.
        let _permit = permit;
        transform_style(
            &transform_resource,
            &origin,
            &style_key,
            &provider,
            encoding.as_deref(),
            token.as_ref(),
        )
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("style transform task failed: {error}"),
        )
    })??;
    let resource = resource.with_derived_validators(validators);
    let mut response = resource.public_response(&headers, body, "application/json");
    apply_origin_vary(response.headers_mut());
    Ok(response)
}

fn transform_style(
    resource: &ProviderResource,
    origin: &str,
    style_key: &str,
    provider: &ProviderConfig,
    encoding: Option<&str>,
    token: Option<&PropagatedAccessToken>,
) -> Result<(Vec<u8>, Validators), HttpError> {
    let decoded = resource.decoded_bytes(MAX_STYLE_BYTES, "style")?;
    let mut style: Value = serde_json::from_slice(&decoded).map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("style JSON invalid: {error}"),
        )
    })?;
    rewrite_style(&mut style, origin, style_key, provider, encoding, token);
    let body = serde_json::to_vec(&style).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("style JSON serialization failed: {error}"),
        )
    })?;
    // The body is a derived representation (origin/encoding-dependent rewrite),
    // so the upstream `ETag` does not identify it. Hash the exact bytes served
    // instead; this also changes whenever the rewrite logic itself changes.
    let validators = Validators::for_derived_body(&body);
    Ok((body, validators))
}

fn resolve_style_url(state: &AppState, style_key: &str) -> Result<String, HttpError> {
    state.provider.resolve_style_url(style_key).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            "style provider not configured".to_string(),
        )
    })
}

async fn route_style_bytes(
    state: &AppState,
    style_key: &str,
    upstream: &str,
) -> Result<ProviderResource, HttpError> {
    let request = ProviderRequest::style(style_key, upstream);
    if let Some(resource) =
        crate::server::provider::route_peer_resource(&state.resource_resolver, &request).await?
    {
        return Ok(resource);
    }
    fetch_style_bytes_local(state, request.upstream_url().to_string()).await
}

async fn fetch_style_bytes_local(
    state: &AppState,
    upstream: String,
) -> Result<ProviderResource, HttpError> {
    state
        .provider_fetcher
        .fetch_json(upstream, MAX_STYLE_BYTES, "style", STYLE_CONTENT_TYPES)
        .await
}

fn rewrite_style(
    style: &mut Value,
    base_url: &str,
    style_key: &str,
    provider: &ProviderConfig,
    encoding: Option<&str>,
    token: Option<&PropagatedAccessToken>,
) {
    let wants_mlt = encoding == Some("mlt");
    if let Some(object) = style.as_object_mut() {
        // Only point `glyphs` / `sprite` at Ishikari when a provider is actually
        // configured for them; otherwise keep the upstream style's own values so
        // clients do not hit unconfigured endpoints that 404.
        if provider.has_glyph_provider() {
            let url = format!("{base_url}/fonts/{{fontstack}}/{{range}}.pbf");
            object.insert(
                "glyphs".to_string(),
                Value::String(token.map_or(url.clone(), |token| token.append_to(&url))),
            );
        }
        if provider.has_sprite_provider(style_key) {
            let url = format!(
                "{base_url}/styles/{}/sprite",
                path_percent_encode_segments(style_key)
            );
            object.insert(
                "sprite".to_string(),
                Value::String(token.map_or(url.clone(), |token| token.append_to(&url))),
            );
        }

        if let Some(sources) = object.get_mut("sources").and_then(Value::as_object_mut) {
            for source in sources.values_mut() {
                rewrite_source_object(source, base_url, wants_mlt, token);
            }
        }
    }
}

/// Rewrites one style `source`'s `url`/`tiles` from provider-relative tileset
/// references (`/[<ns>/]<id>`) to this server's endpoints, and tags vector
/// sources with `encoding: mlt` when MLT was requested and a reference was
/// actually rewritten. External/absolute references are left untouched.
fn rewrite_source_object(
    source: &mut Value,
    base_url: &str,
    wants_mlt: bool,
    token: Option<&PropagatedAccessToken>,
) {
    let Some(source_object) = source.as_object_mut() else {
        return;
    };
    // Only vector sources can serve MLT; raster/raster-dem stay as-is.
    let mlt = wants_mlt && source_object.get("type").and_then(Value::as_str) == Some("vector");
    let mut rewrote_tileset_ref = false;
    if let Some(rewritten) = source_object
        .get("url")
        .and_then(Value::as_str)
        .and_then(|url| rewrite_tileset_ref_tilejson_url(url, base_url))
    {
        // Defer MLT selection to the TileJSON endpoint via `?encoding=mlt`.
        let mut url = if mlt {
            format!("{rewritten}?encoding=mlt")
        } else {
            rewritten
        };
        if let Some(token) = token {
            url = token.append_to(&url);
        }
        source_object.insert("url".to_string(), Value::String(url));
        rewrote_tileset_ref = true;
    }
    if let Some(tiles) = source_object.get_mut("tiles").and_then(Value::as_array_mut) {
        for tile_url in tiles {
            let Some(url) = tile_url.as_str() else {
                continue;
            };
            if let Some(rewritten) = rewrite_tileset_ref_tile_url(url, base_url) {
                let mut url = if mlt {
                    format!("{rewritten}.mlt")
                } else {
                    rewritten
                };
                if let Some(token) = token {
                    url = token.append_to(&url);
                }
                *tile_url = Value::String(url);
                rewrote_tileset_ref = true;
            }
        }
    }
    // Tell MapLibre to decode our tiles as MLT (only when we actually pointed
    // the source at a tileset we serve).
    if mlt && rewrote_tileset_ref {
        source_object.insert("encoding".to_string(), Value::String("mlt".to_string()));
    }
}

/// Extracts the tileset key from a provider-relative source reference.
///
/// A style source `url`/`tiles` that is an absolute path (`/[<namespace>/]<tileset_id>`)
/// refers to a tileset this server provides; it is rewritten to the server's tile
/// endpoint. Absolute URLs (`http(s)://…`) are external and left untouched. We do
/// NOT use a `pmtiles://`-style scheme — it would collide with the official PMTiles
/// protocol (where `pmtiles://` prefixes a real file URL).
fn strip_tileset_ref(url: &str) -> Option<&str> {
    url.strip_prefix('/')
}

fn rewrite_tileset_ref_tilejson_url(url: &str, base_url: &str) -> Option<String> {
    let tileset_key = strip_tileset_ref(url)?;
    TilesetId::try_new(tileset_key).ok()?;
    Some(format!("{base_url}/tilesets/{tileset_key}"))
}

fn rewrite_tileset_ref_tile_url(url: &str, base_url: &str) -> Option<String> {
    let rest = strip_tileset_ref(url)?;
    let tileset_key = rest.strip_suffix("/{z}/{x}/{y}").unwrap_or(rest);
    TilesetId::try_new(tileset_key).ok()?;
    Some(format!(
        "{base_url}/tilesets/{tileset_key}/{{z}}/{{x}}/{{y}}"
    ))
}

pub(crate) fn validate_style_key(style_key: &str) -> Result<(), HttpError> {
    if style_key.is_empty() || style_key.len() > 200 {
        return Err((
            StatusCode::BAD_REQUEST,
            "style_id length invalid".to_string(),
        ));
    }
    for segment in style_key.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err((
                StatusCode::BAD_REQUEST,
                "style_id segment invalid".to_string(),
            ));
        }
        if !segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err((
                StatusCode::BAD_REQUEST,
                "style_id contains invalid characters".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        rewrite_style, rewrite_tileset_ref_tile_url, rewrite_tileset_ref_tilejson_url,
        validate_style_key,
    };
    use crate::provider::ProviderConfig;

    /// A provider with both glyph and sprite upstreams configured.
    fn provider_with_glyph_and_sprite() -> ProviderConfig {
        ProviderConfig::new(
            None,
            Some("https://up.example/fonts/{fontstack}/{range}.pbf".to_string()),
            Some("default=https://up.example/{style_id}/sprite".to_string()),
        )
        .expect("valid provider config")
    }

    #[test]
    fn absolute_path_is_a_tileset_ref() {
        // `/[<namespace>/]<tileset_id>` -> the server's tile endpoint.
        assert_eq!(
            rewrite_tileset_ref_tilejson_url("/mierune/omt", "https://ish.example").as_deref(),
            Some("https://ish.example/tilesets/mierune/omt")
        );
        assert_eq!(
            rewrite_tileset_ref_tilejson_url("/mapterhorn/planet", "https://ish.example")
                .as_deref(),
            Some("https://ish.example/tilesets/mapterhorn/planet")
        );
        assert_eq!(
            rewrite_tileset_ref_tile_url("/mapterhorn/planet/{z}/{x}/{y}", "https://ish.example")
                .as_deref(),
            Some("https://ish.example/tilesets/mapterhorn/planet/{z}/{x}/{y}")
        );
        // Absolute URLs (external) and any scheme are left untouched.
        assert!(
            rewrite_tileset_ref_tilejson_url("https://x.example/t.json", "https://ish.example")
                .is_none()
        );
        assert!(
            rewrite_tileset_ref_tilejson_url("pmtiles://mierune/omt", "https://ish.example")
                .is_none()
        );
        // A scheme-less relative path (no leading `/`) is not a tileset ref.
        assert!(rewrite_tileset_ref_tilejson_url("mierune/omt", "https://ish.example").is_none());
    }

    #[test]
    fn rewrites_provider_relative_sources_and_glyphs() {
        let mut style = json!({
            "version": 8,
            "glyphs": "https://old.example/{fontstack}/{range}.pbf",
            "sources": {
                "base": { "type": "vector", "url": "/analysis/hrnowc" },
                "tiles_array": {
                    "type": "vector",
                    "tiles": [
                        "/analysis/rain/{z}/{x}/{y}",
                        "https://example.test/{z}/{x}/{y}.pbf"
                    ]
                },
                "dem": { "type": "raster-dem", "url": "/mapterhorn/planet" },
                "remote": { "type": "vector", "url": "https://example.test/tilejson.json" }
            },
            "layers": []
        });

        rewrite_style(
            &mut style,
            "https://ishikari.example",
            "carto/voyager",
            &provider_with_glyph_and_sprite(),
            None,
            None,
        );

        assert_eq!(
            style["glyphs"],
            "https://ishikari.example/fonts/{fontstack}/{range}.pbf"
        );
        assert_eq!(
            style["sprite"],
            "https://ishikari.example/styles/carto/voyager/sprite"
        );
        assert_eq!(
            style["sources"]["base"]["url"],
            "https://ishikari.example/tilesets/analysis/hrnowc"
        );
        assert_eq!(
            style["sources"]["remote"]["url"],
            "https://example.test/tilejson.json"
        );
        assert_eq!(
            style["sources"]["dem"]["url"],
            "https://ishikari.example/tilesets/mapterhorn/planet"
        );
        assert_eq!(
            style["sources"]["tiles_array"]["tiles"][0],
            "https://ishikari.example/tilesets/analysis/rain/{z}/{x}/{y}"
        );
        assert_eq!(
            style["sources"]["tiles_array"]["tiles"][1],
            "https://example.test/{z}/{x}/{y}.pbf"
        );
    }

    #[test]
    fn encoding_mlt_rewrites_only_vector_tileset_sources() {
        let mut style = json!({
            "version": 8,
            "sources": {
                "vec_url": { "type": "vector", "url": "/mierune/omt" },
                "vec_tiles": { "type": "vector", "tiles": ["/analysis/rain/{z}/{x}/{y}"] },
                "dem": { "type": "raster-dem", "url": "/mapterhorn/planet" },
                "remote": { "type": "vector", "url": "https://example.test/tj.json" }
            },
            "layers": []
        });

        rewrite_style(
            &mut style,
            "https://ish.example",
            "mierune/x",
            &provider_with_glyph_and_sprite(),
            Some("mlt"),
            None,
        );

        // Vector source via TileJSON `url`: defer to `?encoding=mlt`, mark source MLT.
        assert_eq!(
            style["sources"]["vec_url"]["url"],
            "https://ish.example/tilesets/mierune/omt?encoding=mlt"
        );
        assert_eq!(style["sources"]["vec_url"]["encoding"], "mlt");
        // Vector source via `tiles`: `.mlt` suffix, mark source MLT.
        assert_eq!(
            style["sources"]["vec_tiles"]["tiles"][0],
            "https://ish.example/tilesets/analysis/rain/{z}/{x}/{y}.mlt"
        );
        assert_eq!(style["sources"]["vec_tiles"]["encoding"], "mlt");
        // raster-dem: rewritten but never MLT.
        assert_eq!(
            style["sources"]["dem"]["url"],
            "https://ish.example/tilesets/mapterhorn/planet"
        );
        assert!(style["sources"]["dem"]["encoding"].is_null());
        // External vector source: untouched, no MLT.
        assert_eq!(
            style["sources"]["remote"]["url"],
            "https://example.test/tj.json"
        );
        assert!(style["sources"]["remote"]["encoding"].is_null());
    }

    #[test]
    fn preserves_glyphs_and_sprite_when_provider_unset() {
        let mut style = json!({
            "version": 8,
            "glyphs": "https://old.example/{fontstack}/{range}.pbf",
            "sprite": "https://old.example/sprite",
            "sources": { "base": { "type": "vector", "url": "/analysis/hrnowc" } },
            "layers": []
        });

        // No glyph/sprite provider configured.
        let provider = ProviderConfig::new(None, None, None).expect("valid provider config");
        rewrite_style(
            &mut style,
            "https://ishikari.example",
            "carto/voyager",
            &provider,
            None,
            None,
        );

        // Glyphs/sprite are left pointing at the upstream origin...
        assert_eq!(
            style["glyphs"],
            "https://old.example/{fontstack}/{range}.pbf"
        );
        assert_eq!(style["sprite"], "https://old.example/sprite");
        // ...but PMTiles sources are still rewritten to Ishikari.
        assert_eq!(
            style["sources"]["base"]["url"],
            "https://ishikari.example/tilesets/analysis/hrnowc"
        );
    }

    #[test]
    fn validates_style_key_segments() {
        assert!(validate_style_key("carto/voyager-v1").is_ok());
        assert!(validate_style_key("carto/voyager?x=1").is_err());
        assert!(validate_style_key("carto/voyager#frag").is_err());
        assert!(validate_style_key("carto/voy ager").is_err());
        assert!(validate_style_key("carto/../voyager").is_err());
    }

    #[test]
    fn rewrites_only_valid_pmtiles_tile_urls() {
        assert_eq!(
            rewrite_tileset_ref_tilejson_url("/analysis/rain", "https://i.test").as_deref(),
            Some("https://i.test/tilesets/analysis/rain")
        );
        assert!(rewrite_tileset_ref_tilejson_url("/bad?query", "https://i.test").is_none());
        assert_eq!(
            rewrite_tileset_ref_tile_url("/analysis/rain/{z}/{x}/{y}", "https://i.test").as_deref(),
            Some("https://i.test/tilesets/analysis/rain/{z}/{x}/{y}")
        );
        assert!(rewrite_tileset_ref_tile_url("/bad?query/{z}/{x}/{y}", "https://i.test").is_none());
    }
}
