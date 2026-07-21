//! MapLibre sprite JSON/PNG provider endpoint.

use axum::{
    body::Body,
    http::{HeaderMap, StatusCode},
    response::Response,
};

use ishikari_core::storage::{ProviderRequest, ProviderSpriteVariant};

use crate::server::{AppState, HttpError, style::validate_style_key, upstream::ProviderResource};

const MAX_SPRITE_JSON_BYTES: usize = 2 * 1024 * 1024;
const MAX_SPRITE_PNG_BYTES: usize = 8 * 1024 * 1024;
const SPRITE_JSON_CONTENT_TYPES: &[&str] =
    &["application/json", "text/json", "application/octet-stream"];
const SPRITE_PNG_CONTENT_TYPES: &[&str] = &["image/png", "application/octet-stream"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpriteFormat {
    Json,
    Png,
}

impl SpriteFormat {
    fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Png => "image/png",
        }
    }

    fn max_bytes(self) -> usize {
        match self {
            Self::Json => MAX_SPRITE_JSON_BYTES,
            Self::Png => MAX_SPRITE_PNG_BYTES,
        }
    }

    fn accepted_content_types(self) -> &'static [&'static str] {
        match self {
            Self::Json => SPRITE_JSON_CONTENT_TYPES,
            Self::Png => SPRITE_PNG_CONTENT_TYPES,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct SpriteRequest {
    style_key: String,
    variant: ProviderSpriteVariant,
    format: SpriteFormat,
}

pub(crate) async fn serve_sprite(
    state: AppState,
    request: SpriteRequest,
    headers: &HeaderMap,
) -> Result<Response<Body>, HttpError> {
    validate_style_key(&request.style_key)?;
    let upstream = resolve_sprite_url(&state, &request.style_key, request.variant.suffix())?;
    let resource = route_sprite_bytes(&state, &request, &upstream).await?;
    Ok(resource.public_response(
        headers,
        resource.bytes().clone(),
        request.format.content_type(),
    ))
}

pub(crate) async fn serve_sprite_local(
    state: AppState,
    request: SpriteRequest,
) -> Result<Response<Body>, HttpError> {
    validate_style_key(&request.style_key)?;
    let upstream = resolve_sprite_url(&state, &request.style_key, request.variant.suffix())?;
    let resource = match fetch_sprite_bytes_local(&state, upstream, request.format).await {
        Ok(resource) => resource,
        Err(error) => return crate::server::provider::internal_provider_fetch_error(error),
    };
    state
        .metrics
        .add_internal_bytes(resource.bytes().len() as u64);
    Ok(resource.internal_response(request.format.content_type()))
}

fn resolve_sprite_url(
    state: &AppState,
    style_key: &str,
    suffix: &str,
) -> Result<String, HttpError> {
    state
        .provider
        .resolve_sprite_url(style_key, suffix)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "sprite provider not configured".to_string(),
            )
        })
}

async fn route_sprite_bytes(
    state: &AppState,
    request: &SpriteRequest,
    upstream: &str,
) -> Result<ProviderResource, HttpError> {
    let provider_request = ProviderRequest::sprite(&request.style_key, request.variant, upstream);
    if let Some(resource) =
        crate::server::provider::route_peer_resource(&state.resource_resolver, &provider_request)
            .await?
    {
        return Ok(resource);
    }
    fetch_sprite_bytes_local(
        state,
        provider_request.upstream_url().to_string(),
        request.format,
    )
    .await
}

async fn fetch_sprite_bytes_local(
    state: &AppState,
    upstream: String,
    format: SpriteFormat,
) -> Result<ProviderResource, HttpError> {
    match format {
        SpriteFormat::Json => {
            state
                .provider_fetcher
                .fetch_json(
                    upstream,
                    format.max_bytes(),
                    "sprite",
                    format.accepted_content_types(),
                )
                .await
        }
        SpriteFormat::Png => {
            state
                .provider_fetcher
                .fetch_bytes(
                    upstream,
                    format.max_bytes(),
                    "sprite",
                    format.accepted_content_types(),
                )
                .await
        }
    }
}

pub(crate) fn parse_sprite_path(style_path: &str) -> Option<SpriteRequest> {
    for (tail, variant, format) in [
        (
            "/sprite@2x.json",
            ProviderSpriteVariant::Json2x,
            SpriteFormat::Json,
        ),
        (
            "/sprite@2x.png",
            ProviderSpriteVariant::Png2x,
            SpriteFormat::Png,
        ),
        (
            "/sprite.json",
            ProviderSpriteVariant::Json,
            SpriteFormat::Json,
        ),
        ("/sprite.png", ProviderSpriteVariant::Png, SpriteFormat::Png),
    ] {
        if let Some(style_key) = style_path.strip_suffix(tail)
            && !style_key.is_empty()
        {
            return Some(SpriteRequest {
                style_key: style_key.to_string(),
                variant,
                format,
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use ishikari_core::storage::ProviderSpriteVariant;

    use super::{SpriteFormat, SpriteRequest, parse_sprite_path};

    #[test]
    fn parses_sprite_paths_under_style_key() {
        for (path, variant, format) in [
            (
                "carto/voyager/sprite.json",
                ProviderSpriteVariant::Json,
                SpriteFormat::Json,
            ),
            (
                "carto/voyager/sprite.png",
                ProviderSpriteVariant::Png,
                SpriteFormat::Png,
            ),
            (
                "carto/voyager/sprite@2x.json",
                ProviderSpriteVariant::Json2x,
                SpriteFormat::Json,
            ),
            (
                "carto/voyager/sprite@2x.png",
                ProviderSpriteVariant::Png2x,
                SpriteFormat::Png,
            ),
        ] {
            assert_eq!(
                parse_sprite_path(path),
                Some(SpriteRequest {
                    style_key: "carto/voyager".to_string(),
                    variant,
                    format,
                })
            );
        }
        assert_eq!(parse_sprite_path("carto/voyager/style.json"), None);
        assert_eq!(parse_sprite_path("sprite.json"), None);
    }
}
