//! Biei's HTTP response adapter for the shared delivery authenticator.

pub(crate) use mmpf_auth::{DeliveryAuth, RegistryCatalog};

use crate::http::response::IngressResponse;

pub(crate) fn failure_response(failure: mmpf_auth::AuthFailure) -> IngressResponse {
    match failure {
        mmpf_auth::AuthFailure::InvalidCredential => {
            IngressResponse::json(401, "invalid_token", "")
                .with_header("WWW-Authenticate", "Bearer")
        }
        mmpf_auth::AuthFailure::Forbidden => IngressResponse::json(403, "forbidden", ""),
        mmpf_auth::AuthFailure::Unavailable => {
            IngressResponse::json(503, "auth_unavailable", "").with_retry_after("5")
        }
    }
}
