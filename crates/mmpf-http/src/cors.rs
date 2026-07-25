//! Browser CORS policy for public map-resource delivery.

use std::time::Duration;

use axum::http::{
    HeaderName, Method,
    header::{
        ACCEPT, ACCEPT_RANGES, AGE, AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE,
        ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, RANGE,
    },
};
use tower_http::cors::{Any, CorsLayer};

/// CORS policy for cacheable public map resources.
///
/// Delivery uses bearer or query credentials, not cookies. Keeping credentials
/// disabled lets every deployment return the cache-friendly wildcard origin.
/// Operational and peer routes must not use this layer.
pub fn public_distribution() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::HEAD])
        .allow_headers([
            ACCEPT,
            AUTHORIZATION,
            CACHE_CONTROL,
            IF_MODIFIED_SINCE,
            IF_NONE_MATCH,
            RANGE,
        ])
        .expose_headers([
            ACCEPT_RANGES,
            AGE,
            CACHE_CONTROL,
            CONTENT_LENGTH,
            CONTENT_RANGE,
            ETAG,
            LAST_MODIFIED,
            HeaderName::from_static("x-request-id"),
        ])
        .max_age(Duration::from_secs(3600))
}
