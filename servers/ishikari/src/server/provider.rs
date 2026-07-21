//! Peer routing for provider resources.

use axum::{
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use ishikari_core::storage::{
    InternalProviderNegative, PROVIDER_NEGATIVE_HEADER, ProviderRequest, ProviderRouteOutcome,
    ResourceResolver,
};

use crate::server::{HttpError, upstream::ProviderResource};

/// Converts an actual local provider-fetch 404/410 into the private marked wire
/// response. Callers must invoke this only around `ProviderFetcher` results so
/// route, configuration, and path-resolution failures remain unmarked.
pub(crate) fn internal_provider_fetch_error(error: HttpError) -> Result<Response, HttpError> {
    let marker = match error.0 {
        StatusCode::NOT_FOUND => InternalProviderNegative::NotFound,
        StatusCode::GONE => InternalProviderNegative::Gone,
        _ => return Err(error),
    };
    let mut response = error.into_response();
    response.headers_mut().insert(
        PROVIDER_NEGATIVE_HEADER,
        HeaderValue::from_static(marker.as_str()),
    );
    Ok(response)
}

/// Routes a provider resource to its owning peer and decodes the peer response.
///
/// Returns `Ok(Some(resource))` on a valid peer hit and `Ok(None)` when local
/// fallback is appropriate. Authoritative peer `NotFound`/`Gone` outcomes become
/// fixed 404/410 errors; invalid peer metadata still falls back after a warning.
/// Shared by the glyph/style/sprite handlers.
pub(crate) async fn route_peer_resource(
    resolver: &ResourceResolver,
    request: &ProviderRequest<'_>,
) -> Result<Option<ProviderResource>, HttpError> {
    let kind = request.kind().as_str();
    let outcome = resolver
        .route_provider_resource(request)
        .await
        .map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("{kind} peer route failed: {error}"),
            )
        })?;
    match outcome {
        None => Ok(None),
        Some(ProviderRouteOutcome::NotFound) => {
            Err((StatusCode::NOT_FOUND, "not found".to_string()))
        }
        Some(ProviderRouteOutcome::Gone) => Err((StatusCode::GONE, "gone".to_string())),
        Some(ProviderRouteOutcome::Resource(response)) => {
            match ProviderResource::from_peer(response) {
                Ok(resource) => Ok(Some(resource)),
                Err(error) => {
                    tracing::warn!(error, "invalid {kind} peer metadata; fetching locally");
                    Ok(None)
                }
            }
        }
    }
}
