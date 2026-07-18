pub mod adapter;
pub(crate) mod addlayer;
pub(crate) mod error;
pub(crate) mod format;
pub mod ingress;
pub mod internal;
pub(crate) mod overlay;
pub(crate) mod parse_util;
pub(crate) mod path;
pub(crate) mod preview;
pub(crate) mod query;
pub mod response;
pub(crate) mod static_image;
pub(crate) mod tile;

use axum::http::HeaderMap;

use crate::types::RequestId;

pub(crate) const REQUEST_ID_HEADER: &str = "x-request-id";
const MAX_REQUEST_ID_BYTES: usize = 128;

pub(crate) fn request_id_from_headers(headers: &HeaderMap) -> Option<RequestId> {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= MAX_REQUEST_ID_BYTES)
        .map(RequestId::from_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_header_is_non_empty_and_bounded() {
        let mut headers = HeaderMap::new();
        headers.insert(REQUEST_ID_HEADER, "request-123".parse().unwrap());
        assert_eq!(
            request_id_from_headers(&headers)
                .expect("valid request id")
                .as_str(),
            "request-123"
        );

        headers.insert(REQUEST_ID_HEADER, "".parse().unwrap());
        assert!(request_id_from_headers(&headers).is_none());

        headers.insert(
            REQUEST_ID_HEADER,
            "x".repeat(MAX_REQUEST_ID_BYTES + 1).parse().unwrap(),
        );
        assert!(request_id_from_headers(&headers).is_none());
    }
}
