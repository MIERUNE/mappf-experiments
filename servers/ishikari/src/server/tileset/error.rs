//! HTTP error conversion helpers for tileset handlers.

use axum::http::StatusCode;
use tracing::error;

use crate::server::HttpError;
use ishikari_core::storage::TilesetError;

/// Converts service-layer tileset errors into HTTP status codes and messages.
pub(crate) fn tileset_error_response(error: &TilesetError) -> HttpError {
    match error {
        TilesetError::Upstream(message) | TilesetError::RetryableUpstream(message) => {
            error!(error = %message, "upstream tileset request failed");
            (
                StatusCode::BAD_GATEWAY,
                "upstream tileset request failed".to_string(),
            )
        }
        TilesetError::Timeout(message) => {
            error!(error = %message, "upstream tileset request timed out");
            (
                StatusCode::GATEWAY_TIMEOUT,
                "upstream tileset request timed out".to_string(),
            )
        }
        TilesetError::Overloaded(message) => {
            error!(error = %message, "backend fetch admission saturated");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "backend fetch capacity exhausted".to_string(),
            )
        }
        TilesetError::Miss => (StatusCode::NOT_FOUND, "not found".to_string()),
        TilesetError::Internal(message) => {
            error!(error = %message, "returning internal server error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_errors_do_not_expose_internal_details() {
        let secret = "gs://private-bucket/archive.pmtiles: permission denied";
        for error in [
            TilesetError::Upstream(secret.to_string()),
            TilesetError::Internal(secret.to_string()),
            TilesetError::Timeout(secret.to_string()),
            TilesetError::Overloaded(secret.to_string()),
        ] {
            let (_, body) = tileset_error_response(&error);
            assert!(!body.contains(secret));
        }
    }

    #[test]
    fn backend_admission_overload_is_retryable_service_unavailability() {
        let (status, body) =
            tileset_error_response(&TilesetError::Overloaded("saturated".to_string()));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "backend fetch capacity exhausted");
    }
}
