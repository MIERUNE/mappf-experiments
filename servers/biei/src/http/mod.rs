pub(crate) mod adapter;
pub(crate) mod addlayer;
pub(crate) mod error;
pub(crate) mod format;
pub(crate) mod ingress;
pub(crate) mod internal;
pub(crate) mod metrics;
pub(crate) mod overlay;
pub(crate) mod parse_util;
pub(crate) mod path;
pub(crate) mod preview;
pub(crate) mod query;
pub(crate) mod response;
pub(crate) mod static_image;
pub(crate) mod tile;

use axum::http::HeaderMap;
pub(crate) use mmpf_http::request_id::HEADER as REQUEST_ID_HEADER;

use biei_core::types::RequestId;

pub(crate) fn request_id_from_headers(headers: &HeaderMap) -> Option<RequestId> {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(RequestId::from_candidate)
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
            "x".repeat(mmpf_http::request_id::MAX_LEN + 1)
                .parse()
                .unwrap(),
        );
        assert!(request_id_from_headers(&headers).is_none());

        headers.insert(REQUEST_ID_HEADER, "contains space".parse().unwrap());
        assert!(request_id_from_headers(&headers).is_none());
    }
}
