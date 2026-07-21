//! Origin transport and representation validation for provider resources.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::http::{HeaderMap, StatusCode, header};
use bytes::BytesMut;
use ishikari_core::storage::ObjectStoreRegistry;
use mmpf_common::singleflight::Flight;
use object_store::{Attribute, Error as ObjectStoreError, GetOptions};
use reqwest::Client;
use url::Url;

use crate::server::{
    HttpError,
    conditional::Validators,
    provider_body::{BodyValidation, validate_body, validate_content_type},
    provider_cache_policy::{
        cache_policy, cache_policy_values, cache_policy_with_freshness_values,
        negative_cache_policy_values, negative_cache_policy_with_freshness_values,
    },
};

use super::{
    CachedProviderFetch, CachedProviderRepresentation, FetchedProviderNegative,
    FetchedProviderResource, Freshness, PROVIDER_FETCH_TIMEOUT, ProviderFetchCacheKey,
    ProviderFetcher, ProviderFlightOutcome, ProviderOriginOutcome, ProviderResource,
    record_cached_provider_fetch, spawn_stale_revalidation, store_leader_result,
};

pub(super) async fn fetch_limited_bytes_with_validation(
    fetcher: &ProviderFetcher,
    url: String,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &'static [&'static str],
    body_validation: BodyValidation,
) -> Result<ProviderResource, HttpError> {
    let url: Arc<str> = Arc::from(url);
    let key = ProviderFetchCacheKey::new(
        resource,
        Arc::clone(&url),
        accepted_content_types,
        body_validation,
    );
    let mut recorded_miss = false;
    let mut joined_singleflight = false;
    loop {
        if let Some((entry, freshness)) = fetcher.cache.get(&key) {
            // A follower already recorded the request as a miss plus a join.
            // Reading the leader's freshly inserted value is not an independent
            // cache hit and must not inflate cache-hit-ratio dashboards.
            record_cached_provider_fetch(
                &fetcher.metrics,
                resource,
                &entry,
                freshness,
                joined_singleflight,
            );
            if freshness == Freshness::Stale {
                spawn_stale_revalidation(
                    fetcher,
                    key.clone(),
                    Arc::clone(&url),
                    max_bytes,
                    resource,
                    accepted_content_types,
                    body_validation,
                );
            }
            return entry.into_result();
        }
        if !recorded_miss {
            fetcher
                .metrics
                .record_provider_resource_cache(resource, "miss");
            recorded_miss = true;
        }

        match fetcher.cache.begin_fetch(key.clone()) {
            Flight::Leader(guard) => {
                // Another leader may have installed a replacement after our
                // initial miss but before this election. Re-check under flight
                // ownership so an expired observation cannot trigger a serial
                // duplicate origin fetch.
                if fetcher.cache.get(&key).is_some() {
                    drop(guard);
                    continue;
                }
                let result = fetch_limited_bytes_uncached(
                    fetcher,
                    &url,
                    max_bytes,
                    resource,
                    accepted_content_types,
                    body_validation,
                    None,
                )
                .await;
                return store_leader_result(fetcher, &key, resource, result, guard);
            }
            Flight::Follower(follower) => {
                // Request-scoped: an uncacheable success stores nothing, so a
                // follower can wake, miss, and follow the next leader. Those
                // internal wait cycles are one joined request, not several.
                if !joined_singleflight {
                    fetcher
                        .metrics
                        .record_provider_resource_cache(resource, "singleflight_join");
                    joined_singleflight = true;
                }
                if let Some(outcome) = follower.wait().await {
                    return match outcome {
                        ProviderFlightOutcome::Error(error) => Err(error),
                        ProviderFlightOutcome::Resource(resource) => Ok(resource),
                    };
                }
            }
        }
    }
}

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
        let (policy, has_explicit_freshness) = negative_cache_policy_with_freshness_values(
            headers
                .get_all(header::CACHE_CONTROL)
                .iter()
                .filter_map(|value| value.to_str().ok()),
        );
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
        header_value(&headers, header::CONTENT_TYPE).as_deref(),
        accepted_content_types,
        resource,
    )?;
    let (policy, has_explicit_freshness) = cache_policy_with_freshness_values(
        resource,
        headers
            .get_all(header::CACHE_CONTROL)
            .iter()
            .filter_map(|value| value.to_str().ok()),
    );
    // Age accounting is only meaningful against an upstream-declared lifetime.
    // When the upstream sets no explicit freshness, Ishikari applies its own
    // default TTL, and charging the transported `Age`/`Date` against that
    // invented lifetime would wrongly evict (a CDN-fronted body sending
    // `Age: 900` but no `Cache-Control` would never cache). Match the
    // object-store path and start the clock at fetch time in that case.
    let validators = Validators::new(
        header_value(&headers, header::ETAG).map(Arc::from),
        header_value(&headers, header::LAST_MODIFIED)
            .and_then(|value| httpdate::parse_http_date(&value).ok()),
    );
    let content_encoding = joined_header_values(&headers, header::CONTENT_ENCODING)
        .filter(|value| !value.trim().eq_ignore_ascii_case("identity"))
        .map(Arc::from);
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
            .map(|value| value.as_ref()),
        accepted_content_types,
        resource,
    )?;
    let policy = cache_policy(
        resource,
        result
            .attributes
            .get(&Attribute::CacheControl)
            .map(|value| value.as_ref()),
    );
    let last_modified = SystemTime::from(result.meta.last_modified);
    let validators = Validators::new(
        result.meta.e_tag.as_deref().map(Arc::from),
        (last_modified != UNIX_EPOCH).then_some(last_modified),
    );
    let content_encoding = result
        .attributes
        .get(&Attribute::ContentEncoding)
        .map(|value| value.as_ref())
        .filter(|value| !value.trim().eq_ignore_ascii_case("identity"))
        .map(Arc::from);
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
    let cache_control_values = headers
        .map(|headers| {
            headers
                .get_all(header::CACHE_CONTROL)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let policy = if cache_control_values.is_empty() {
        cache_policy(resource, Some(cached.cache_control.as_ref()))
    } else {
        cache_policy_values(resource, cache_control_values.iter().copied())
    };
    let validators = Validators::new(
        headers
            .and_then(|headers| header_value(headers, header::ETAG))
            .map(Arc::from)
            .or_else(|| cached.validators.etag_arc()),
        headers
            .and_then(|headers| header_value(headers, header::LAST_MODIFIED))
            .and_then(|value| httpdate::parse_http_date(&value).ok())
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

fn header_value(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn joined_header_values(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    let values: Vec<&str> = headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect();
    (!values.is_empty()).then(|| values.join(", "))
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
        .and_then(|value| httpdate::parse_http_date(&value).ok())
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
    use super::{fetch_http_provider, provider_failure_diagnostic};
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
