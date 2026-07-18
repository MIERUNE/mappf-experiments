//! HTTP-to-MapLibre response mapping and cache policy.

use std::time::{Duration, SystemTime};

use maplibre_native::file_source::{
    ErrorReason, ResourceKind, ResourceRequest, Response, StoragePolicy,
};
use reqwest::header::{AGE, CACHE_CONTROL, DATE, ETAG, EXPIRES, LAST_MODIFIED, RETRY_AFTER};

use crate::renderer::http_fetch::reqwest_error_label;

// A 304 is allowed to omit freshness headers. The bridge stores only an
// absolute expiry, not the original freshness lifetime, so give a successfully
// revalidated entry a short bounded lifetime instead of revalidating it on
// every subsequent resource lookup.
const REVALIDATED_FALLBACK_TTL: Duration = Duration::from_secs(60);

// RFC 9111 §4.2.2 heuristic freshness: a cacheable response with no explicit
// expiry must not be fresh forever (that would serve a stale glyph/tile on
// every render). Fresh for a fraction of its age since `Last-Modified`, clamped,
// or a short default; after that it becomes a strictly-revalidated `Revalidate`.
const HEURISTIC_FRESHNESS_DIVISOR: u32 = 10;
const MIN_HEURISTIC_FRESHNESS: Duration = Duration::from_secs(60);
const MAX_HEURISTIC_FRESHNESS: Duration = Duration::from_secs(3600);
const DEFAULT_HEURISTIC_FRESHNESS: Duration = Duration::from_secs(300);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CachePolicy {
    Store,
    Remove,
    Unchanged,
}

pub(super) fn cache_policy_for_response(
    storage_policy: StoragePolicy,
    headers: &reqwest::header::HeaderMap,
) -> CachePolicy {
    if !matches!(storage_policy, StoragePolicy::Permanent) {
        CachePolicy::Unchanged
    } else if has_cache_directive(headers, "no-store") || has_cache_directive(headers, "private") {
        CachePolicy::Remove
    } else {
        CachePolicy::Store
    }
}

#[derive(Clone, Copy)]
pub(super) struct RetryDirective {
    pub(super) reason: &'static str,
    pub(super) delay: Option<Duration>,
}

pub(super) fn retry_directive(
    status: u16,
    headers: &reqwest::header::HeaderMap,
) -> Option<RetryDirective> {
    if status == 408 {
        return Some(RetryDirective {
            reason: "request_timeout",
            delay: None,
        });
    }
    if status == 429 {
        return Some(RetryDirective {
            reason: "rate_limit",
            delay: header_str(headers, RETRY_AFTER)
                .and_then(parse_retry_after)
                .and_then(duration_until),
        });
    }
    (500..=599).contains(&status).then_some(RetryDirective {
        reason: "server",
        delay: None,
    })
}

pub(super) fn negative_cache_ttl(
    status: u16,
    kind: ResourceKind,
    storage_policy: StoragePolicy,
    headers: &reqwest::header::HeaderMap,
    maximum: Duration,
) -> Option<Duration> {
    if !matches!(status, 404 | 410)
        || cache_policy_for_response(storage_policy, headers) != CachePolicy::Store
        || has_cache_directive(headers, "no-cache")
    {
        return None;
    }

    // Explicit upstream freshness on the 404/410, if any.
    let explicit = header_str(headers, CACHE_CONTROL)
        .as_deref()
        .and_then(parse_shared_max_age)
        .map(|freshness| freshness.saturating_sub(response_current_age(headers)))
        .or_else(|| {
            header_date(headers, EXPIRES)
                .and_then(|expires| expires.duration_since(SystemTime::now()).ok())
        });

    let ttl = match explicit {
        // Honor an explicit upstream freshness lifetime for any resource kind,
        // still bounded by `maximum` (a missing resource may reappear).
        Some(freshness) => freshness.min(maximum),
        // No explicit freshness: only fabricate a TTL for tiles. A 404 tile is
        // a normal "empty tile" that providers routinely return without cache
        // headers, and caching it shields the provider from empty-area spray.
        // For required resources (glyphs / sprites / style / source / image) a
        // fabricated TTL would turn a transient upstream 404 — e.g. a rolling
        // provider deploy — into a guaranteed broken-render window until the
        // entry expires, so we do not negative-cache them without explicit
        // upstream intent.
        None if kind == ResourceKind::Tile => maximum,
        None => return None,
    };
    (!ttl.is_zero()).then_some(ttl)
}

#[derive(Clone, Copy, Default)]
pub(super) struct PriorResponse<'a> {
    pub(super) data: Option<&'a [u8]>,
    pub(super) etag: Option<&'a str>,
    pub(super) modified: Option<SystemTime>,
    pub(super) expires: Option<SystemTime>,
    pub(super) must_revalidate: bool,
}

pub(super) fn prior_response_with_cache<'a>(
    request: &'a ResourceRequest,
    cached: Option<&'a Response>,
) -> PriorResponse<'a> {
    PriorResponse {
        data: request
            .prior_data
            .as_deref()
            .or_else(|| cached.and_then(|response| response.data.as_deref())),
        etag: request
            .prior_etag
            .as_deref()
            .or_else(|| cached.and_then(|response| response.etag.as_deref())),
        modified: request
            .prior_modified
            .or_else(|| cached.and_then(|response| response.modified)),
        expires: request
            .prior_expires
            .or_else(|| cached.and_then(|response| response.expires)),
        must_revalidate: cached.is_some_and(|response| response.must_revalidate),
    }
}

pub(super) fn response_from_reqwest_error(error: &reqwest::Error) -> Response {
    // reqwest reports both connect and total-deadline expiry as timeouts;
    // mbgl's taxonomy folds transport-level failures into `Connection`.
    Response::error(ErrorReason::Connection, reqwest_error_message(error))
}

fn reqwest_error_message(error: &reqwest::Error) -> &'static str {
    match reqwest_error_label(error) {
        "timeout" => "resource request timed out",
        "connect" => "resource connection failed",
        "redirect" => "resource redirect failed",
        "body" | "decode" => "resource response body failed",
        _ => "resource request failed",
    }
}

/// Maps an upstream HTTP response onto mbgl's `Response` shape.
pub(super) fn response_from_http(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: Vec<u8>,
    kind: ResourceKind,
    prior: PriorResponse<'_>,
) -> Response {
    match status {
        200 | 206 => with_cache_metadata(Response::data(body), headers, PriorResponse::default()),
        204 => Response::no_content(),
        304 => with_cache_metadata(Response::not_modified(), headers, prior),
        404 | 410 if kind == ResourceKind::Tile => Response::no_content(),
        404 | 410 => Response::error(ErrorReason::NotFound, format!("HTTP {status}")),
        429 => {
            let mut response = Response::error(ErrorReason::RateLimit, "HTTP 429");
            if let Some(retry_after) = header_str(headers, RETRY_AFTER).and_then(parse_retry_after)
            {
                response = response.with_retry_after(retry_after);
            }
            response
        }
        500..=599 => Response::error(ErrorReason::Server, format!("HTTP {status}")),
        other => Response::error(ErrorReason::Other, format!("HTTP {other}")),
    }
}

/// Materialize a 304 response for the process-wide Rust cache. MLN receives
/// the original bodyless response and merges it with its prior representation.
pub(super) fn materialize_not_modified(
    response: &Response,
    prior: PriorResponse<'_>,
) -> Option<Response> {
    let data = prior.data?;
    let mut materialized = response.clone();
    materialized.not_modified = false;
    materialized.data = Some(data.to_vec());
    Some(materialized)
}

fn with_cache_metadata(
    mut response: Response,
    headers: &reqwest::header::HeaderMap,
    prior: PriorResponse<'_>,
) -> Response {
    if let Some(etag) = header_str(headers, ETAG).or_else(|| prior.etag.map(str::to_owned)) {
        response = response.with_etag(etag);
    }
    if let Some(modified) = header_date(headers, LAST_MODIFIED).or(prior.modified) {
        response = response.with_modified(modified);
    }

    let cache_control = header_str(headers, CACHE_CONTROL);
    let requires_validation = cache_control
        .as_deref()
        .map_or(prior.must_revalidate, |value| {
            has_cache_directive_value(value, "no-cache")
                || has_cache_directive_value(value, "must-revalidate")
                // RFC 9111 gives s-maxage the semantics of
                // proxy-revalidate for shared caches.
                || parse_cache_duration(value, "s-maxage").is_some()
        });
    let now = SystemTime::now();
    let no_cache = cache_control
        .as_deref()
        .is_some_and(|value| has_cache_directive_value(value, "no-cache"))
        || (cache_control.is_none()
            && prior.must_revalidate
            && prior.expires == Some(SystemTime::UNIX_EPOCH));
    let expires = if requires_validation && no_cache {
        Some(SystemTime::UNIX_EPOCH)
    } else {
        cache_control
            .as_deref()
            .and_then(parse_shared_max_age)
            .map(|max_age| max_age.saturating_sub(response_current_age(headers)))
            .and_then(|max_age| now.checked_add(max_age))
            .or_else(|| header_date(headers, EXPIRES))
            .or_else(|| match prior.expires {
                Some(expires) if prior.data.is_some() && expires <= now => {
                    now.checked_add(REVALIDATED_FALLBACK_TTL)
                }
                expires => expires,
            })
            // No explicit or inherited freshness: bound it heuristically rather
            // than leaving `expires = None`, which `cache::lookup` would treat
            // as fresh forever. Never for a response that requires validation
            // (`must-revalidate`/`s-maxage`): fabricating a freshness window
            // would defeat `cache::lookup`'s `must_revalidate && no-expiry`
            // rule that forces revalidation on every lookup.
            .or_else(|| {
                if requires_validation {
                    None
                } else {
                    heuristic_expires(headers, now)
                }
            })
    };
    if let Some(expires) = expires {
        response = response.with_expires(expires);
    }
    if requires_validation {
        response = response.with_must_revalidate(true);
    }
    response
}

/// Heuristic expiry for a response with no explicit freshness (see the
/// `HEURISTIC_FRESHNESS_*` constants).
fn heuristic_expires(headers: &reqwest::header::HeaderMap, now: SystemTime) -> Option<SystemTime> {
    let ttl = header_date(headers, LAST_MODIFIED)
        .and_then(|modified| now.duration_since(modified).ok())
        .map(|age| {
            (age / HEURISTIC_FRESHNESS_DIVISOR)
                .clamp(MIN_HEURISTIC_FRESHNESS, MAX_HEURISTIC_FRESHNESS)
        })
        .unwrap_or(DEFAULT_HEURISTIC_FRESHNESS);
    now.checked_add(ttl)
}

fn header_str(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn header_date(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Option<SystemTime> {
    header_str(headers, name).and_then(|value| httpdate::parse_http_date(&value).ok())
}

pub(super) fn parse_max_age(cache_control: &str) -> Option<Duration> {
    parse_cache_duration(cache_control, "max-age")
}

fn parse_shared_max_age(cache_control: &str) -> Option<Duration> {
    parse_cache_duration(cache_control, "s-maxage").or_else(|| parse_max_age(cache_control))
}

fn parse_cache_duration(cache_control: &str, expected: &str) -> Option<Duration> {
    cache_control.split(',').find_map(|directive| {
        let directive = directive.trim();
        let (name, value) = directive.split_once('=')?;
        if !name.trim().eq_ignore_ascii_case(expected) {
            return None;
        }
        value
            .trim()
            .trim_matches('"')
            .parse::<u64>()
            .ok()
            .map(Duration::from_secs)
    })
}

fn response_current_age(headers: &reqwest::header::HeaderMap) -> Duration {
    let age = header_str(headers, AGE)
        .and_then(|age| age.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_default();
    let apparent_age = header_date(headers, DATE)
        .and_then(|date| SystemTime::now().duration_since(date).ok())
        .unwrap_or_default();
    age.max(apparent_age)
}

pub(super) fn has_cache_directive(headers: &reqwest::header::HeaderMap, expected: &str) -> bool {
    header_str(headers, CACHE_CONTROL)
        .is_some_and(|value| has_cache_directive_value(&value, expected))
}

fn has_cache_directive_value(cache_control: &str, expected: &str) -> bool {
    cache_control.split(',').any(|directive| {
        directive
            .trim()
            .split_once('=')
            .map_or(directive.trim(), |(name, _)| name.trim())
            .eq_ignore_ascii_case(expected)
    })
}

pub(super) fn parse_retry_after(value: String) -> Option<SystemTime> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .and_then(|seconds| SystemTime::now().checked_add(Duration::from_secs(seconds)))
        .or_else(|| httpdate::parse_http_date(value.trim()).ok())
}

fn duration_until(deadline: SystemTime) -> Option<Duration> {
    deadline.duration_since(SystemTime::now()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn unknown_freshness_gets_bounded_heuristic_expiry_not_forever() {
        // No Cache-Control, no Expires, no Last-Modified, no prior freshness:
        // the response must still gain a bounded expiry so `cache::lookup`
        // eventually revalidates instead of serving it as a permanent Hit.
        let response = with_cache_metadata(
            Response::data(vec![1, 2, 3]),
            &HeaderMap::new(),
            PriorResponse::default(),
        );
        let now = SystemTime::now();
        let expires = response
            .expires
            .expect("unknown-freshness response must not be treated as fresh forever");
        assert!(expires > now, "heuristic expiry is in the future");
        assert!(
            expires <= now + DEFAULT_HEURISTIC_FRESHNESS + Duration::from_secs(5),
            "with no Last-Modified the heuristic falls back to the short default"
        );
    }

    #[test]
    fn heuristic_freshness_scales_with_last_modified_and_clamps_to_max() {
        // Modified ~100h ago → 10% = ~10h, clamped to MAX_HEURISTIC_FRESHNESS.
        let modified = SystemTime::now() - Duration::from_secs(360_000);
        let mut headers = HeaderMap::new();
        headers.insert(
            LAST_MODIFIED,
            HeaderValue::from_str(&httpdate::fmt_http_date(modified)).expect("date header"),
        );
        let response =
            with_cache_metadata(Response::data(vec![1]), &headers, PriorResponse::default());
        let now = SystemTime::now();
        let expires = response.expires.expect("bounded heuristic expiry");
        assert!(expires >= now + MIN_HEURISTIC_FRESHNESS);
        assert!(
            expires <= now + MAX_HEURISTIC_FRESHNESS + Duration::from_secs(5),
            "long-unmodified resources are still revalidated within the cap"
        );
    }

    #[test]
    fn explicit_max_age_is_not_overridden_by_heuristic() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CACHE_CONTROL,
            HeaderValue::from_static("max-age=30"),
        );
        let response =
            with_cache_metadata(Response::data(vec![1]), &headers, PriorResponse::default());
        let now = SystemTime::now();
        let expires = response.expires.expect("explicit max-age expiry");
        // ~30s, well under the heuristic floor, proving the explicit directive wins.
        assert!(expires <= now + Duration::from_secs(30) + Duration::from_secs(5));
    }

    #[test]
    fn must_revalidate_without_expiry_is_not_given_heuristic_freshness() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CACHE_CONTROL,
            HeaderValue::from_static("must-revalidate"),
        );
        let response =
            with_cache_metadata(Response::data(vec![1]), &headers, PriorResponse::default());
        assert!(response.must_revalidate);
        // Leaving `expires = None` keeps `cache::lookup`'s
        // `must_revalidate && no-expiry` rule forcing revalidation; a fabricated
        // heuristic window would serve it stale instead.
        assert_eq!(
            response.expires, None,
            "must-revalidate without explicit freshness must not receive heuristic freshness"
        );
    }
}
