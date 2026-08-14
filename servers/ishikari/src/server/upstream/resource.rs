//! The provider resource value and the responses built from it.
//!
//! This is the representation itself: bytes plus the cache metadata that has to
//! survive peer forwarding. It knows nothing about fetching or caching, which is
//! why it separates cleanly from the rest of the module.

use std::{sync::Arc, time::Duration};

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use bytes::Bytes;

use super::FetchedProviderResource;
use crate::server::{
    HttpError, apply_accept_encoding_vary, bytes_response, conditional::Validators,
    provider_body::decode_provider_body,
};
use ishikari_core::storage::{
    InternalFetchResponse, PROVIDER_AGE_HEADER, PROVIDER_CACHE_CONTROL_HEADER,
    PROVIDER_ETAG_HEADER, PROVIDER_LAST_MODIFIED_HEADER,
};

/// Provider bytes plus the cache metadata that must survive peer forwarding.
#[derive(Clone)]
pub(crate) struct ProviderResource {
    bytes: Bytes,
    cache_control: Arc<str>,
    age_seconds: u64,
    validators: Validators,
    content_encoding: Option<Arc<str>>,
}

impl ProviderResource {
    /// Rebuilds a resource from a retained cache entry, deriving the age the
    /// client should see from how long the entry has been stored.
    pub(super) fn from_cached(
        bytes: Bytes,
        cache_control: Arc<str>,
        validators: Validators,
        content_encoding: Option<Arc<str>>,
        age_seconds: u64,
    ) -> Self {
        Self {
            bytes,
            cache_control,
            age_seconds,
            validators,
            content_encoding,
        }
    }

    pub(super) fn fetched(fetched: &FetchedProviderResource) -> Self {
        Self {
            bytes: fetched.bytes.clone(),
            cache_control: Arc::clone(&fetched.policy.response_cache_control),
            age_seconds: fetched.initial_age.as_secs(),
            validators: fetched.validators.clone(),
            content_encoding: fetched.content_encoding.clone(),
        }
    }

    pub(crate) fn from_peer(response: InternalFetchResponse) -> Result<Self, &'static str> {
        let cache_control = response
            .provider_cache_control
            .ok_or("peer provider response is missing cache policy")?;
        let age_seconds = response
            .provider_age_seconds
            .ok_or("peer provider response is missing age")?;
        Ok(Self {
            bytes: response.bytes,
            cache_control: Arc::from(cache_control),
            age_seconds,
            validators: Validators::new(
                response.provider_etag.map(Arc::from),
                response
                    .provider_last_modified
                    .as_deref()
                    .and_then(|value| httpdate::parse_http_date(value).ok()),
            ),
            content_encoding: response.content_encoding.map(Arc::from),
        })
    }

    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// The transport encoding this representation is stored in, if any.
    ///
    /// Handlers need it to answer `406` when a client excludes the only coding
    /// available: this service never cross-compresses a provider body, so an
    /// unacceptable coding has no alternative representation to fall back to.
    pub(crate) fn content_encoding(&self) -> Option<&str> {
        self.content_encoding.as_deref()
    }

    pub(crate) fn cache_control(&self) -> &str {
        &self.cache_control
    }

    pub(crate) fn age_seconds(&self) -> u64 {
        self.age_seconds
    }

    /// Builds a locally derived provider representation. Its validators apply
    /// to the transformed bytes, not to any one upstream component.
    pub(crate) fn derived(bytes: Bytes, cache_control: Arc<str>) -> Self {
        let validators = Validators::for_derived_body(&bytes);
        Self {
            bytes,
            cache_control,
            age_seconds: 0,
            validators,
            content_encoding: None,
        }
    }

    pub(crate) fn with_additional_age(mut self, elapsed: Duration) -> Self {
        self.age_seconds = self.age_seconds.saturating_add(elapsed.as_secs());
        self
    }

    /// Returns the decoded representation for server-side transformation.
    /// Byte-identical glyph/sprite responses keep their original encoding;
    /// styles must be decoded before JSON parsing and rewriting.
    pub(crate) fn decoded_bytes(
        &self,
        max_bytes: usize,
        resource: &'static str,
    ) -> Result<Bytes, HttpError> {
        decode_provider_body(
            &self.bytes,
            self.content_encoding.as_deref(),
            max_bytes,
            resource,
        )
    }

    /// Replaces the upstream validators for a derived representation whose
    /// bytes differ from the upstream body (e.g. rewritten style JSON).
    pub(crate) fn with_derived_validators(mut self, validators: Validators) -> Self {
        self.validators = validators;
        // The derived style body is serialized as an identity representation.
        self.content_encoding = None;
        self
    }

    /// Builds the public representation response, including conditional request
    /// handling and the provider's public cache and representation metadata.
    pub(crate) fn public_response(
        &self,
        request: &HeaderMap,
        body: impl Into<axum::body::Body>,
        content_type: &'static str,
    ) -> axum::response::Response {
        if self.not_modified(request) {
            return self.not_modified_response();
        }
        let mut response = bytes_response(body, content_type, None);
        self.apply_public_headers(response.headers_mut());
        response
    }

    /// Builds the cluster-internal representation response with typed provider
    /// forwarding metadata rather than downstream cache headers.
    pub(crate) fn internal_response(&self, content_type: &'static str) -> axum::response::Response {
        let mut response = bytes_response(self.bytes.clone(), content_type, None);
        self.apply_internal_headers(response.headers_mut());
        response
    }

    /// Whether a conditional request matches this representation (serve `304`).
    fn not_modified(&self, request: &HeaderMap) -> bool {
        self.validators.not_modified(request)
    }

    /// `304 Not Modified` for a matched conditional request: no body, and no
    /// representation metadata (`Content-Encoding`). It carries the cache
    /// metadata, validators, and `Vary` that a `200` would (RFC 9110 §15.4.5).
    fn not_modified_response(&self) -> axum::response::Response {
        let mut response = axum::response::Response::new(axum::body::Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        self.apply_cache_metadata(response.headers_mut());
        response
    }

    fn apply_public_headers(&self, headers: &mut HeaderMap) {
        self.apply_cache_metadata(headers);
        self.apply_content_encoding(headers);
    }

    /// `Cache-Control`, `Age`, validators, and `Vary` — the metadata shared by a
    /// `200` body response and its `304`. Excludes representation headers.
    ///
    /// `Vary` belongs here, not with `Content-Encoding`: a `304` carries no
    /// representation, but it still answers a request that was negotiated on
    /// `Accept-Encoding`, and a shared cache keys the stored `304`-refreshed entry
    /// by whatever `Vary` names. Omitting it there would let one client's revalidation
    /// re-validate an entry for a client that negotiated differently.
    fn apply_cache_metadata(&self, headers: &mut HeaderMap) {
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_bytes(self.cache_control.as_bytes())
                .expect("cache policy originated from a valid HTTP header"),
        );
        headers.insert(
            header::AGE,
            HeaderValue::from_str(&self.age_seconds.to_string()).expect("age is numeric"),
        );
        self.validators.apply(headers);
        if self.content_encoding.is_some() {
            apply_accept_encoding_vary(headers);
        }
    }

    fn apply_internal_headers(&self, headers: &mut HeaderMap) {
        headers.insert(
            PROVIDER_CACHE_CONTROL_HEADER,
            HeaderValue::from_bytes(self.cache_control.as_bytes())
                .expect("cache policy originated from a valid HTTP header"),
        );
        headers.insert(
            PROVIDER_AGE_HEADER,
            HeaderValue::from_str(&self.age_seconds.to_string()).expect("age is numeric"),
        );
        if let Some(etag) = self.validators.etag()
            && let Ok(value) = HeaderValue::from_str(etag)
        {
            headers.insert(PROVIDER_ETAG_HEADER, value);
        }
        if let Some(http_date) = self.validators.last_modified_http_date()
            && let Ok(value) = HeaderValue::from_str(&http_date)
        {
            headers.insert(PROVIDER_LAST_MODIFIED_HEADER, value);
        }
        self.apply_content_encoding(headers);
    }

    fn apply_content_encoding(&self, headers: &mut HeaderMap) {
        if let Some(encoding) = &self.content_encoding
            && let Ok(value) = HeaderValue::from_str(encoding)
        {
            headers.insert(header::CONTENT_ENCODING, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::http::{HeaderMap, StatusCode, header};
    use bytes::Bytes;

    use std::sync::Arc;

    use super::ProviderResource;
    use crate::server::conditional::Validators;
    use ishikari_core::storage::{
        InternalFetchResponse, PROVIDER_AGE_HEADER, PROVIDER_CACHE_CONTROL_HEADER,
        PROVIDER_ETAG_HEADER, PROVIDER_LAST_MODIFIED_HEADER,
    };

    #[test]
    fn provider_cache_metadata_survives_internal_and_public_headers() {
        let last_modified = std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let resource = ProviderResource {
            bytes: Bytes::from_static(b"glyph"),
            cache_control: "public, max-age=30, s-maxage=60".into(),
            age_seconds: 12,
            validators: Validators::new(Some("\"v1\"".into()), Some(last_modified)),
            content_encoding: Some("gzip".into()),
        };
        let mut internal = HeaderMap::new();
        resource.apply_internal_headers(&mut internal);
        assert_eq!(
            internal[PROVIDER_CACHE_CONTROL_HEADER],
            "public, max-age=30, s-maxage=60"
        );
        assert_eq!(internal[PROVIDER_AGE_HEADER], "12");
        assert_eq!(internal[PROVIDER_ETAG_HEADER], "\"v1\"");
        let http_date = httpdate::fmt_http_date(last_modified);
        assert_eq!(
            internal[PROVIDER_LAST_MODIFIED_HEADER].to_str().unwrap(),
            http_date
        );

        let header_string = |name: &str| {
            internal
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let peer_resource = ProviderResource::from_peer(InternalFetchResponse {
            bytes: resource.bytes().clone(),
            archive_generation: None,
            tile_source: None,
            provider_cache_control: header_string(PROVIDER_CACHE_CONTROL_HEADER),
            provider_age_seconds: header_string(PROVIDER_AGE_HEADER)
                .and_then(|value| value.parse().ok()),
            provider_etag: header_string(PROVIDER_ETAG_HEADER),
            provider_last_modified: header_string(PROVIDER_LAST_MODIFIED_HEADER),
            content_encoding: header_string(header::CONTENT_ENCODING.as_str()),
        })
        .expect("complete peer metadata");
        let mut public = HeaderMap::new();
        peer_resource.apply_public_headers(&mut public);
        assert_eq!(
            public[header::CACHE_CONTROL],
            "public, max-age=30, s-maxage=60"
        );
        assert_eq!(public[header::AGE], "12");
        assert_eq!(public[header::ETAG], "\"v1\"");
        assert_eq!(public[header::LAST_MODIFIED].to_str().unwrap(), http_date);
        assert_eq!(public[header::CONTENT_ENCODING], "gzip");

        // The forwarded validators still answer conditional requests.
        let mut conditional = HeaderMap::new();
        conditional.insert(header::IF_NONE_MATCH, "\"v1\"".parse().unwrap());
        assert!(peer_resource.not_modified(&conditional));
    }

    #[test]
    fn not_modified_response_omits_representation_metadata() {
        let resource = ProviderResource {
            bytes: Bytes::from_static(b"gzipped"),
            cache_control: "public, max-age=30".into(),
            age_seconds: 7,
            validators: Validators::new(Some("\"v1\"".into()), None),
            content_encoding: Some("gzip".into()),
        };

        // The 200 carries the representation's Content-Encoding.
        let mut ok = HeaderMap::new();
        resource.apply_public_headers(&mut ok);
        assert_eq!(ok[header::CONTENT_ENCODING], "gzip");

        // The 304 carries cache metadata and validators, but not the
        // representation's Content-Encoding (RFC 9110 §15.4.5).
        let response = resource.not_modified_response();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        let headers = response.headers();
        assert_eq!(headers[header::CACHE_CONTROL], "public, max-age=30");
        assert_eq!(headers[header::AGE], "7");
        assert_eq!(headers[header::ETAG], "\"v1\"");
        assert!(headers.get(header::CONTENT_ENCODING).is_none());

        // Vary is cache metadata, not representation metadata, so it must survive on
        // the 304. Without it a shared cache revalidating on behalf of one client
        // would refresh an entry it cannot tell apart from a differently negotiated
        // one.
        assert!(
            headers
                .get_all(header::VARY)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .any(|value| value.to_ascii_lowercase().contains("accept-encoding")),
            "304 must repeat the 200's Vary"
        );
    }

    /// An identity representation negotiates nothing, so neither its `200` nor its
    /// `304` should claim to vary.
    #[test]
    fn an_identity_not_modified_response_declares_no_encoding_vary() {
        let resource = ProviderResource {
            bytes: Bytes::from_static(b"plain"),
            cache_control: "public, max-age=30".into(),
            age_seconds: 0,
            validators: Validators::new(Some("\"v1\"".into()), None),
            content_encoding: None,
        };

        let response = resource.not_modified_response();
        assert!(
            !response
                .headers()
                .get_all(header::VARY)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .any(|value| value.to_ascii_lowercase().contains("accept-encoding"))
        );
    }

    #[test]
    fn peer_without_provider_metadata_is_rejected() {
        let result = ProviderResource::from_peer(InternalFetchResponse {
            bytes: Bytes::from_static(b"missing metadata"),
            archive_generation: None,
            tile_source: None,
            provider_cache_control: None,
            provider_age_seconds: None,
            provider_etag: None,
            provider_last_modified: None,
            content_encoding: None,
        });
        let Err(error) = result else {
            panic!("missing peer metadata must fail closed");
        };
        assert_eq!(error, "peer provider response is missing cache policy");
    }

    /// The deployment-sensitive contract: a provider body stored as a gzip stream
    /// must reach the caller still compressed, labelled with the resource's own
    /// content type, and declared as varying by `Accept-Encoding`.
    ///
    /// Getting any part of this wrong is silent. Dropping the encoding header hands
    /// the caller gzip bytes labelled protobuf; dropping `Vary` lets a shared cache
    /// serve them to a client that asked for identity.
    #[test]
    fn a_gzip_stored_body_is_forwarded_compressed_and_declared() {
        let compressed = Bytes::from_static(&[0x1f, 0x8b, 0x08, 0x00]);
        let resource = ProviderResource::from_cached(
            compressed.clone(),
            "public, max-age=86400".into(),
            Validators::new(None, None),
            Some(Arc::from("gzip")),
            0,
        );

        assert_eq!(resource.content_encoding(), Some("gzip"));

        let response = resource.public_response(
            &HeaderMap::new(),
            compressed.clone(),
            "application/x-protobuf",
        );

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_ENCODING).unwrap(),
            "gzip"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-protobuf"
        );
        let vary: Vec<_> = response
            .headers()
            .get_all(header::VARY)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect();
        assert!(
            vary.iter()
                .any(|value| value.to_ascii_lowercase().contains("accept-encoding")),
            "Vary must list Accept-Encoding, got {vary:?}"
        );
    }

    /// An identity body must not gain either header, so the common path stays
    /// byte-for-byte what it was before compressed storage existed.
    #[test]
    fn an_identity_body_declares_no_encoding_and_no_encoding_vary() {
        let resource = ProviderResource::from_cached(
            Bytes::from_static(b"plain"),
            "public, max-age=86400".into(),
            Validators::new(None, None),
            None,
            0,
        );

        let response = resource.public_response(
            &HeaderMap::new(),
            Bytes::from_static(b"plain"),
            "application/x-protobuf",
        );

        assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
        assert!(
            !response
                .headers()
                .get_all(header::VARY)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .any(|value| value.to_ascii_lowercase().contains("accept-encoding"))
        );
    }
}
