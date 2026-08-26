//! Origin transport and representation validation for provider resources.

use std::{
    borrow::Cow,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use bytes::BytesMut;
use ishikari_core::storage::ObjectStoreRegistry;
use object_store::{Attribute, Error as ObjectStoreError, GetOptions};
use reqwest::Client;
use url::Url;

use crate::server::{
    HttpError,
    conditional::Validators,
    provider_body::{BodyValidation, validate_body, validate_content_type},
    provider_cache_policy::{
        UpstreamCacheControl, cache_policy, cache_policy_with_freshness,
        negative_cache_policy_values, negative_cache_policy_with_freshness,
        revalidated_cache_policy,
    },
};

use super::cache::{CachedProviderFetch, ProviderFetchCacheKey, ProviderFetcher};
use super::{
    CachedProviderRepresentation, FetchedProviderNegative, FetchedProviderResource,
    PROVIDER_FETCH_TIMEOUT, ProviderOriginOutcome,
};

pub(super) async fn fetch_limited_bytes_uncached(
    fetcher: &ProviderFetcher,
    url: &str,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
    body_validation: BodyValidation,
    revalidate: Option<&CachedProviderRepresentation>,
) -> Result<ProviderOriginOutcome, HttpError> {
    let fetch = async {
        // The one deadline covers queueing, headers, and the complete body. A
        // request cannot consume 15 seconds for each phase independently.
        let _admission = fetcher.cache.admit_fetch(resource).await?;
        let parsed = Url::parse(url).map_err(|_| provider_invalid_url(resource))?;
        let fetched = match parsed.scheme() {
            // object_store's HTTP adapter intentionally normalizes metadata and
            // exposes only one Cache-Control field value. Fetch HTTP directly
            // so Age/Date, repeated Cache-Control, and Content-Encoding survive.
            "http" | "https" => {
                fetch_http_provider(
                    &fetcher.cache.http_client,
                    parsed,
                    max_bytes,
                    resource,
                    accepted_content_types,
                    revalidate,
                )
                .await?
            }
            _ => {
                fetch_object_store_provider(
                    &fetcher.object_store_registry,
                    &parsed,
                    max_bytes,
                    resource,
                    accepted_content_types,
                    revalidate,
                )
                .await?
            }
        };
        if let ProviderOriginOutcome::Modified(fetched) = &fetched {
            validate_body(
                &fetched.bytes,
                fetched.content_encoding.as_deref(),
                body_validation,
                max_bytes,
                resource,
            )?;
        }
        Ok(fetched)
    };
    tokio::time::timeout(PROVIDER_FETCH_TIMEOUT, fetch)
        .await
        .map_err(|_| {
            (
                StatusCode::GATEWAY_TIMEOUT,
                format!("{resource} upstream timed out"),
            )
        })?
}

async fn fetch_http_provider(
    client: &Client,
    url: Url,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
    revalidate: Option<&CachedProviderRepresentation>,
) -> Result<ProviderOriginOutcome, HttpError> {
    let request_started = Instant::now();
    let mut request = client.get(url.clone());
    if let Some(cached) = revalidate {
        if let Some(etag) = cached.validators.etag() {
            request = request.header(header::IF_NONE_MATCH, etag);
        } else if let Some(last_modified) = cached.validators.last_modified_http_date() {
            request = request.header(header::IF_MODIFIED_SINCE, last_modified);
        }
    }
    let mut response = request.send().await.map_err(|error| {
        provider_bad_gateway(resource, "GET failed", &url, reqwest_error_kind(&error))
    })?;
    let status = response.status();
    let headers = std::mem::take(response.headers_mut());
    if status == StatusCode::NOT_MODIFIED {
        let cached = revalidate.ok_or_else(|| {
            (
                StatusCode::BAD_GATEWAY,
                format!("{resource} upstream returned an unsolicited 304"),
            )
        })?;
        return Ok(ProviderOriginOutcome::NotModified(
            revalidated_provider_resource(
                cached,
                resource,
                Some(&headers),
                request_started.elapsed(),
            ),
        ));
    }
    if matches!(status, StatusCode::NOT_FOUND | StatusCode::GONE) {
        let (policy, has_explicit_freshness) =
            negative_cache_policy_with_freshness(upstream_cache_control(&headers));
        let initial_age = if has_explicit_freshness {
            corrected_initial_age(&headers, SystemTime::now(), request_started.elapsed())
        } else {
            Duration::ZERO
        };
        return Ok(ProviderOriginOutcome::Negative(FetchedProviderNegative {
            status,
            policy,
            initial_age,
        }));
    }
    require_complete_provider_status(status, resource)?;
    if response
        .content_length()
        .is_some_and(|size| size > max_bytes as u64)
    {
        return Err(provider_body_too_large(resource));
    }

    validate_content_type(
        header_value(&headers, header::CONTENT_TYPE),
        accepted_content_types,
        resource,
    )?;
    let (policy, has_explicit_freshness) =
        cache_policy_with_freshness(resource, upstream_cache_control(&headers));
    // Age accounting is only meaningful against an upstream-declared lifetime.
    // When the upstream sets no explicit freshness, Ishikari applies its own
    // default TTL, and charging the transported `Age`/`Date` against that
    // invented lifetime would wrongly evict (a CDN-fronted body sending
    // `Age: 900` but no `Cache-Control` would never cache). Match the
    // object-store path and start the clock at fetch time in that case.
    let validators = Validators::new(
        header_value(&headers, header::ETAG).map(Arc::from),
        header_value(&headers, header::LAST_MODIFIED)
            .and_then(|value| httpdate::parse_http_date(value).ok()),
    );
    let content_encoding = joined_header_values(&headers, header::CONTENT_ENCODING)
        .filter(|value| !value.trim().eq_ignore_ascii_case("identity"))
        .map(|value| Arc::from(value.as_ref()))
        // Apply the same storage convention the object-store path uses, so a
        // provider is described identically whichever transport reached it. An
        // HTTP provider that serves gzip bytes under a compressed content type
        // and no `Content-Encoding` would otherwise be forwarded as raw gzip
        // labelled protobuf: the caller would try to parse a gzip stream.
        .or_else(|| gzip_encoding_from_content_type(header_value(&headers, header::CONTENT_TYPE)));
    let mut body = BytesMut::with_capacity(
        response.content_length().unwrap_or(0).min(max_bytes as u64) as usize,
    );
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        provider_bad_gateway(resource, "body failed", &url, reqwest_error_kind(&error))
    })? {
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(provider_body_too_large(resource));
        }
        body.extend_from_slice(&chunk);
    }
    // Include body transfer time because the entry cannot be served or stored
    // until the complete bounded representation has arrived.
    let initial_age = if has_explicit_freshness {
        corrected_initial_age(&headers, SystemTime::now(), request_started.elapsed())
    } else {
        Duration::ZERO
    };

    Ok(ProviderOriginOutcome::Modified(FetchedProviderResource {
        bytes: body.freeze(),
        policy,
        validators,
        content_encoding,
        initial_age,
    }))
}

pub(super) fn require_complete_provider_status(
    status: StatusCode,
    resource: &'static str,
) -> Result<(), HttpError> {
    if status == StatusCode::OK {
        return Ok(());
    }
    Err((
        StatusCode::BAD_GATEWAY,
        format!("{resource} upstream returned {status}"),
    ))
}

async fn fetch_object_store_provider(
    registry: &ObjectStoreRegistry,
    url: &Url,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
    revalidate: Option<&CachedProviderRepresentation>,
) -> Result<ProviderOriginOutcome, HttpError> {
    // `gs://` and `s3://` authenticate with ambient credentials. The registry
    // reuses connection pools and credentials per bucket.
    let (store, path) = registry
        .resolve(url)
        .map_err(|_| provider_bad_gateway(resource, "store init failed", url, "object-store"))?;
    let mut options = GetOptions::new();
    if let Some(cached) = revalidate {
        if let Some(etag) = cached.validators.etag() {
            options = options.with_if_none_match(Some(etag));
        } else if let Some(last_modified) = cached.validators.last_modified() {
            options = options.with_if_modified_since(Some(last_modified));
        }
    }
    let result = match store.get_opts(&path, options).await {
        Ok(result) => result,
        Err(ObjectStoreError::NotModified { .. }) if revalidate.is_some() => {
            return Ok(ProviderOriginOutcome::NotModified(
                revalidated_provider_resource(
                    revalidate.expect("checked above"),
                    resource,
                    None,
                    Duration::ZERO,
                ),
            ));
        }
        Err(ObjectStoreError::NotFound { .. }) => {
            return Ok(ProviderOriginOutcome::Negative(FetchedProviderNegative {
                status: StatusCode::NOT_FOUND,
                policy: negative_cache_policy_values(std::iter::empty()),
                initial_age: Duration::ZERO,
            }));
        }
        Err(_other) => {
            return Err(provider_bad_gateway(
                resource,
                "GET failed",
                url,
                "object-store",
            ));
        }
    };
    if result.meta.size > max_bytes as u64 {
        return Err(provider_body_too_large(resource));
    }
    validate_content_type(
        result
            .attributes
            .get(&Attribute::ContentType)
            .map(std::convert::AsRef::as_ref),
        accepted_content_types,
        resource,
    )?;
    let policy = cache_policy(
        resource,
        result
            .attributes
            .get(&Attribute::CacheControl)
            .map(std::convert::AsRef::as_ref),
    );
    let last_modified = SystemTime::from(result.meta.last_modified);
    let validators = Validators::new(
        result.meta.e_tag.as_deref().map(Arc::from),
        (last_modified != UNIX_EPOCH).then_some(last_modified),
    );
    let stored_content_type = result
        .attributes
        .get(&Attribute::ContentType)
        .map(std::convert::AsRef::as_ref);
    let content_encoding = result
        .attributes
        .get(&Attribute::ContentEncoding)
        .map(std::convert::AsRef::as_ref)
        .filter(|value| !value.trim().eq_ignore_ascii_case("identity"))
        .map(Arc::from)
        // An object stored *as* a compressed archive declares that through its
        // content type, not through `Content-Encoding`.
        //
        // This matters because the two are not interchangeable on GCS. Setting
        // `Content-Encoding: gzip` enables decompressive transcoding: a client
        // that does not ask for gzip receives the object decompressed **and
        // without `Content-Length`**, which `object_store` rejects outright
        // (`header_meta` requires that header), so the fetch fails with a 502.
        // `object_store` has no way to send `Accept-Encoding: gzip` — its reqwest
        // build disables the feature — so the header form is unusable here.
        //
        // Storing the gzip bytes with a compressed content type instead keeps the
        // transfer compressed end to end: GCS returns the bytes verbatim with a
        // correct `Content-Length`, and declaring the encoding here lets
        // `decode_provider_body` decompress when the server needs to read the
        // body, while a byte-identical response forwards it to the caller with
        // `Content-Encoding: gzip` for transparent client-side decoding.
        .or_else(|| gzip_encoding_from_content_type(stored_content_type));
    let body = result
        .bytes()
        .await
        .map_err(|_| provider_bad_gateway(resource, "body failed", url, "object-store"))?;
    if body.len() > max_bytes {
        return Err(provider_body_too_large(resource));
    }
    Ok(ProviderOriginOutcome::Modified(FetchedProviderResource {
        bytes: body,
        policy,
        validators,
        content_encoding,
        initial_age: Duration::ZERO,
    }))
}

/// Recognises a content type that means "the stored bytes are a gzip stream".
///
/// Only an exact archive type counts. A `+gzip` structured suffix is deliberately
/// accepted too, while anything else — including a plain protobuf or octet-stream
/// type — is left alone, so an object that merely *contains* compressed data is
/// never double-decoded.
fn gzip_encoding_from_content_type(content_type: Option<&str>) -> Option<Arc<str>> {
    let value = content_type?;
    let essence = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let is_gzip = matches!(essence.as_str(), "application/gzip" | "application/x-gzip")
        || essence.ends_with("+gzip");
    is_gzip.then(|| Arc::from("gzip"))
}

/// The `BAD_GATEWAY` shed used when an upstream body exceeds the resource cap.
fn provider_body_too_large(resource: &str) -> HttpError {
    (
        StatusCode::BAD_GATEWAY,
        format!("{resource} body too large"),
    )
}

/// A client-safe `BAD_GATEWAY` for an upstream transport failure. The raw
/// transport error is deliberately reduced to a bounded category before this
/// point because reqwest and object-store errors can embed credentialed URLs.
fn provider_bad_gateway(
    resource: &'static str,
    what: &'static str,
    source: &Url,
    failure_kind: &'static str,
) -> HttpError {
    let diagnostic = provider_failure_diagnostic(resource, what, Some(source), failure_kind);
    tracing::warn!(%diagnostic, "provider upstream failure");
    (
        StatusCode::BAD_GATEWAY,
        format!("{resource} upstream {what}"),
    )
}

fn provider_invalid_url(resource: &'static str) -> HttpError {
    let diagnostic = provider_failure_diagnostic(resource, "URL invalid", None, "invalid-url");
    tracing::warn!(%diagnostic, "provider upstream failure");
    (
        StatusCode::BAD_GATEWAY,
        format!("{resource} upstream URL invalid"),
    )
}

fn provider_failure_diagnostic(
    resource: &'static str,
    what: &'static str,
    source: Option<&Url>,
    failure_kind: &'static str,
) -> String {
    let source = source.map_or_else(
        || "<invalid-url>".to_string(),
        |url| format!("{}://<redacted>", url.scheme()),
    );
    format!("{resource} upstream {what}; source={source}; kind={failure_kind}")
}

fn reqwest_error_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_body() {
        "body"
    } else if error.is_decode() {
        "decode"
    } else if error.is_builder() {
        "builder"
    } else {
        "request"
    }
}

pub(super) fn revalidated_provider_resource(
    cached: &CachedProviderRepresentation,
    resource: &'static str,
    headers: Option<&HeaderMap>,
    response_delay: Duration,
) -> FetchedProviderResource {
    let upstream = UpstreamCacheControl::from_fields(
        headers
            .into_iter()
            .flat_map(|headers| headers.get_all(header::CACHE_CONTROL).iter())
            .map(HeaderValue::as_bytes),
    );
    let policy = revalidated_cache_policy(resource, upstream, cached.cache_control.as_ref());
    let validators = Validators::new(
        headers
            .and_then(|headers| header_value(headers, header::ETAG))
            .map(Arc::from)
            .or_else(|| cached.validators.etag_arc()),
        headers
            .and_then(|headers| header_value(headers, header::LAST_MODIFIED))
            .and_then(|value| httpdate::parse_http_date(value).ok())
            .or_else(|| cached.validators.last_modified()),
    );
    let content_encoding =
        match headers.and_then(|headers| joined_header_values(headers, header::CONTENT_ENCODING)) {
            Some(value) if value.trim().eq_ignore_ascii_case("identity") => None,
            Some(value) => Some(Arc::from(value.trim())),
            None => cached.content_encoding.clone(),
        };
    let initial_age = headers.map_or(Duration::ZERO, |headers| {
        corrected_initial_age(headers, SystemTime::now(), response_delay)
    });
    FetchedProviderResource {
        bytes: cached.bytes.clone(),
        policy,
        validators,
        content_encoding,
        initial_age,
    }
}

fn upstream_cache_control(headers: &HeaderMap) -> UpstreamCacheControl<'_> {
    UpstreamCacheControl::from_fields(
        headers
            .get_all(header::CACHE_CONTROL)
            .iter()
            .map(HeaderValue::as_bytes),
    )
}

fn header_value(headers: &HeaderMap, name: header::HeaderName) -> Option<&str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn joined_header_values(headers: &HeaderMap, name: header::HeaderName) -> Option<Cow<'_, str>> {
    let mut values = headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok());
    let first = values.next()?;
    let Some(second) = values.next() else {
        return Some(Cow::Borrowed(first));
    };
    let mut joined = String::with_capacity(first.len() + 2 + second.len());
    joined.push_str(first);
    joined.push_str(", ");
    joined.push_str(second);
    for value in values {
        joined.push_str(", ");
        joined.push_str(value);
    }
    Some(Cow::Owned(joined))
}

pub(super) fn corrected_initial_age(
    headers: &HeaderMap,
    response_received: SystemTime,
    response_delay: Duration,
) -> Duration {
    let age_value = headers
        .get_all(header::AGE)
        .iter()
        .filter_map(|value| value.to_str().ok()?.trim().parse::<u64>().ok())
        .max()
        .map(Duration::from_secs)
        .unwrap_or_default();
    let apparent_age = header_value(headers, header::DATE)
        .and_then(|value| httpdate::parse_http_date(value).ok())
        .and_then(|date| response_received.duration_since(date).ok())
        .unwrap_or_default();
    apparent_age.max(age_value.saturating_add(response_delay))
}

pub(super) fn provider_fetch_cache_weight(
    key: &ProviderFetchCacheKey,
    value: &CachedProviderFetch,
) -> u32 {
    let value_size = match value {
        CachedProviderFetch::Found { bytes, .. } => bytes.len(),
        CachedProviderFetch::Negative { .. } => 0,
    };
    let total = std::mem::size_of_val(key)
        .saturating_add(key.url.len())
        .saturating_add(value_size);
    total.min(u32::MAX as usize) as u32
}

#[cfg(test)]
mod tests {

    #[test]
    fn gzip_content_types_declare_the_stored_encoding() {
        for value in [
            "application/gzip",
            "application/x-gzip",
            "APPLICATION/GZIP",
            "application/gzip; charset=binary",
            "application/vnd.mapbox-vector-tile+gzip",
        ] {
            assert_eq!(
                gzip_encoding_from_content_type(Some(value)).as_deref(),
                Some("gzip"),
                "{value} should declare gzip"
            );
        }

        // A plain protobuf or octet-stream body is not compressed. Treating it as
        // gzip would make the server try to inflate raw glyph bytes.
        for value in [
            "application/x-protobuf",
            "application/octet-stream",
            "application/json",
            "",
        ] {
            assert!(
                gzip_encoding_from_content_type(Some(value)).is_none(),
                "{value} must not declare gzip"
            );
        }
        assert!(gzip_encoding_from_content_type(None).is_none());
    }
    use super::{
        fetch_http_provider, gzip_encoding_from_content_type, provider_failure_diagnostic,
    };
    use reqwest::Client;
    use url::Url;

    #[tokio::test]
    async fn transport_failure_does_not_disclose_credentialed_source_url() {
        let url = Url::parse(
            "http://alice:super-secret@127.0.0.1:0/private/style.json?token=signed-secret#fragment",
        )
        .unwrap();
        let client = Client::builder().no_proxy().build().unwrap();

        let error = match fetch_http_provider(&client, url.clone(), 1024, "style", &[], None).await
        {
            Ok(_) => panic!("closed local port must fail"),
            Err(error) => error,
        };

        assert_eq!(error.0, axum::http::StatusCode::BAD_GATEWAY);
        assert_eq!(error.1, "style upstream GET failed");

        let diagnostic = provider_failure_diagnostic("style", "GET failed", Some(&url), "connect");
        assert_eq!(
            diagnostic,
            "style upstream GET failed; source=http://<redacted>; kind=connect"
        );
        for sensitive in [
            "alice",
            "super-secret",
            "127.0.0.1",
            "private",
            "signed-secret",
            "fragment",
        ] {
            assert!(
                !error.1.contains(sensitive),
                "public error leaked {sensitive:?}"
            );
            assert!(
                !diagnostic.contains(sensitive),
                "internal diagnostic leaked {sensitive:?}"
            );
        }
    }
}
