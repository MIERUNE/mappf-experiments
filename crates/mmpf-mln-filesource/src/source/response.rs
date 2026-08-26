//! HTTP-to-MapLibre response mapping and cache policy.

use std::time::{Duration, SystemTime};

use maplibre_native::file_source::{
    ErrorReason, ResourceKind, ResourceRequest, Response, StoragePolicy,
};
#[cfg(test)]
use mmpf_http::cache_control::conservative_delta_seconds;
use mmpf_http::cache_control::{ParsedCacheControl, parse_values};
#[cfg(test)]
use mmpf_http::cache_control::{conservative_delta_seconds_values, directives};
use reqwest::header::{AGE, CACHE_CONTROL, DATE, ETAG, EXPIRES, LAST_MODIFIED, RETRY_AFTER};

use crate::http::reqwest_error_label;

// A 304 is allowed to omit freshness headers. The bridge stores only an
// absolute expiry, not the original freshness lifetime, so give a successfully
// revalidated entry a short bounded lifetime instead of revalidating it on
// every subsequent resource lookup.
const REVALIDATED_FALLBACK_TTL: Duration = Duration::from_mins(1);

// RFC 9111 §4.2.2 heuristic freshness: a cacheable response with no explicit
// expiry must not be fresh forever (that would serve a stale glyph/tile on
// every render). Fresh for a fraction of its age since `Last-Modified`, clamped,
// or a short default; after that it becomes a strictly-revalidated `Revalidate`.
const HEURISTIC_FRESHNESS_DIVISOR: u32 = 10;
const MIN_HEURISTIC_FRESHNESS: Duration = Duration::from_mins(1);
const MAX_HEURISTIC_FRESHNESS: Duration = Duration::from_hours(1);
const DEFAULT_HEURISTIC_FRESHNESS: Duration = Duration::from_mins(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CachePolicy {
    Store { freshness: CachedFreshness },
    Remove,
    Unchanged,
}

/// Effective freshness policy at store time, retained beside the cached
/// representation. A sparse 304 inherits absent caching fields from the stored
/// response (RFC 9111 §4.3.4), which is only possible if the effective
/// directives — not just their derived timestamps — survive the store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct CachedFreshness {
    /// Effective shared freshness lifetime (`s-maxage` falling back to
    /// `max-age`), when declared.
    pub(super) lifetime: Option<Duration>,
    /// RFC 5861 stale-while-revalidate grant, when declared and not cancelled.
    pub(super) stale_while_revalidate: Option<Duration>,
    /// Absolute end of the stale-while-revalidate window, anchored on the
    /// origin's clock (transported `Age` charged against the whole retention).
    pub(super) stale_until: Option<SystemTime>,
}

impl CachedFreshness {
    /// Re-derive the window after a validation that restated nothing: the
    /// inherited directives re-anchor at validation time, minus the 304's own
    /// transported age.
    pub(super) fn reanchored(self, current_age: Duration) -> Self {
        let stale_until = self.stale_while_revalidate.and_then(|grant| {
            let lifetime = self.lifetime.unwrap_or_default();
            SystemTime::now().checked_add((lifetime + grant).saturating_sub(current_age))
        });
        Self {
            stale_until,
            ..self
        }
    }
}

pub(super) fn cache_policy_for_response(
    storage_policy: StoragePolicy,
    headers: &reqwest::header::HeaderMap,
) -> CachePolicy {
    let control = parsed_cache_control(headers);
    if !matches!(storage_policy, StoragePolicy::Permanent) {
        CachePolicy::Unchanged
    } else if control
        .as_ref()
        .is_some_and(|control| control.no_store || control.private)
    {
        CachePolicy::Remove
    } else {
        CachePolicy::Store {
            freshness: freshness_for_response(headers, control.as_ref()),
        }
    }
}

/// A 304 may omit caching headers entirely. RFC 9111 §4.3.4 then retains the
/// stored fields, so the stored effective policy is inherited and re-anchored
/// at validation time (a successful validation resets the response's age). A
/// present `Cache-Control` — or `Expires`, for origins that restate freshness
/// without one — replaces the stored policy and is evaluated normally,
/// including removing the grant when the restatement omits it. Inheriting
/// past an `Expires`-only restatement would keep the entry's expiry and its
/// stale-while-revalidate window on two different policies.
pub(super) fn cache_policy_for_not_modified(
    storage_policy: StoragePolicy,
    headers: &reqwest::header::HeaderMap,
    prior_freshness: CachedFreshness,
) -> CachePolicy {
    let policy = cache_policy_for_response(storage_policy, headers);
    if headers.contains_key(CACHE_CONTROL) || headers.contains_key(EXPIRES) {
        policy
    } else {
        match policy {
            CachePolicy::Store { .. } => CachePolicy::Store {
                freshness: prior_freshness.reanchored(response_current_age(headers)),
            },
            other => other,
        }
    }
}

/// Derives the effective freshness policy — lifetime, grant, and the absolute
/// end of the stale-while-revalidate window — in one place. The window anchors
/// on the origin's own clock: `now + (lifetime + grant) - current_age`.
/// Deriving it from an expiry already saturated to `now` would hand a response
/// that arrived mid-window a fresh full grant, letting chained caches extend
/// the origin's permitted stale lifetime. Without an explicit lifetime there is
/// nothing to anchor the window on except an absolute `Expires`.
fn freshness_for_response(
    headers: &reqwest::header::HeaderMap,
    control: Option<&ParsedCacheControl>,
) -> CachedFreshness {
    let lifetime = shared_max_age(control);
    let grant = control.and_then(stale_while_revalidate_grant);
    let stale_until = grant.and_then(|grant| {
        if let Some(lifetime) = lifetime {
            let remaining = lifetime
                .saturating_add(grant)
                .saturating_sub(response_current_age(headers));
            return SystemTime::now().checked_add(remaining);
        }
        header_date(headers, EXPIRES)?.checked_add(grant)
    });
    CachedFreshness {
        lifetime,
        stale_while_revalidate: grant,
        stale_until,
    }
}

/// RFC 5861 grant: the window after expiry in which a shared cache may serve
/// the stale representation while revalidating in the background. An explicit
/// `must-revalidate`/`proxy-revalidate`/`no-cache` prohibits serving stale and
/// overrides it. `s-maxage` alone does not: origins pair `s-maxage` with
/// `stale-while-revalidate` precisely to allow this (RFC 9111's implied
/// proxy-revalidate yields to the explicit grant).
fn stale_while_revalidate_grant(control: &ParsedCacheControl) -> Option<Duration> {
    if control.must_revalidate || control.proxy_revalidate || control.no_cache {
        return None;
    }
    control.stale_while_revalidate.map(Duration::from_secs)
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
    let control = parsed_cache_control(headers);
    if !matches!(status, 404 | 410)
        || !matches!(storage_policy, StoragePolicy::Permanent)
        || control
            .as_ref()
            .is_some_and(|control| control.no_store || control.private || control.no_cache)
    {
        return None;
    }

    // Explicit upstream freshness on the 404/410, if any.
    let explicit = shared_max_age(control.as_ref())
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
    /// Body the native side withheld for revalidation. Its presence means the
    /// consumer behind this request has NOT received the representation, so a
    /// 304 must be materialized before native sees it.
    pub(super) native_data: Option<&'a [u8]>,
    /// Body known only to the process-wide Rust cache. Native consumers of a
    /// background refresh already hold their copy, so a 304 backed only by
    /// this stays bodyless for native — re-sending the body would make
    /// sprite/tile consumers re-parse an unchanged representation.
    pub(super) cache_data: Option<&'a [u8]>,
    pub(super) etag: Option<&'a str>,
    pub(super) modified: Option<SystemTime>,
    pub(super) expires: Option<SystemTime>,
    pub(super) must_revalidate: bool,
    /// Effective freshness policy stored beside the cached representation. A
    /// sparse 304 inherits it because the corresponding fields were not
    /// replaced (RFC 9111 §4.3.4).
    pub(super) freshness: CachedFreshness,
}

impl<'a> PriorResponse<'a> {
    /// The representation this process holds, regardless of who else has it.
    pub(super) fn body(&self) -> Option<&'a [u8]> {
        self.native_data.or(self.cache_data)
    }
}

pub(super) fn prior_response_with_cache<'a>(
    request: &'a ResourceRequest,
    cached: Option<&'a Response>,
    cached_freshness: CachedFreshness,
) -> PriorResponse<'a> {
    PriorResponse {
        native_data: request.prior_data.as_deref(),
        cache_data: cached.and_then(|response| response.data.as_deref()),
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
        freshness: cached_freshness,
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

/// Merge the representation this process holds into a 304. The result always
/// feeds the process-wide Rust cache; it also becomes the native response when
/// the prior body came from native itself (withheld for revalidation), which is
/// the same merge the stock `OnlineFileSource` performs before consumers see a
/// 304.
pub(super) fn materialize_not_modified(
    response: &Response,
    prior: PriorResponse<'_>,
) -> Option<Response> {
    let data = prior.body()?;
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
    if let Some(etag) = header_str(headers, ETAG)
        .map(str::to_owned)
        .or_else(|| prior.etag.map(str::to_owned))
    {
        response = response.with_etag(etag);
    }
    if let Some(modified) = header_date(headers, LAST_MODIFIED).or(prior.modified) {
        response = response.with_modified(modified);
    }

    let control = parsed_cache_control(headers);
    let requires_validation = if let Some(control) = &control {
        control.no_cache
            || control.must_revalidate
            // RFC 9111 gives s-maxage the semantics of proxy-revalidate for
            // shared caches.
            || control.s_maxage.is_some()
    } else {
        prior.must_revalidate
    };
    let now = SystemTime::now();
    let no_cache = control.as_ref().is_some_and(|control| control.no_cache)
        || (control.is_none()
            && prior.must_revalidate
            && prior.expires == Some(SystemTime::UNIX_EPOCH));
    let expires = if requires_validation && no_cache {
        Some(SystemTime::UNIX_EPOCH)
    } else {
        shared_max_age(control.as_ref())
            .map(|max_age| max_age.saturating_sub(response_current_age(headers)))
            .and_then(|max_age| now.checked_add(max_age))
            .or_else(|| header_date(headers, EXPIRES))
            .or_else(|| match prior.expires {
                Some(expires) if prior.body().is_some() && expires <= now => {
                    // A sparse revalidation inherits the stored lifetime
                    // (RFC 9111 §4.3.4) re-anchored at validation time; the
                    // bounded fallback only covers a prior that never declared
                    // one.
                    let lifetime = prior.freshness.lifetime.unwrap_or(REVALIDATED_FALLBACK_TTL);
                    now.checked_add(lifetime.saturating_sub(response_current_age(headers)))
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
        .map_or(DEFAULT_HEURISTIC_FRESHNESS, |age| {
            (age / HEURISTIC_FRESHNESS_DIVISOR)
                .clamp(MIN_HEURISTIC_FRESHNESS, MAX_HEURISTIC_FRESHNESS)
        });
    now.checked_add(ttl)
}

fn header_str(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Option<&str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn header_date(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Option<SystemTime> {
    header_str(headers, name).and_then(|value| httpdate::parse_http_date(value).ok())
}

#[cfg(test)]
pub(super) fn parse_max_age(cache_control: &str) -> Option<Duration> {
    parse_cache_duration(cache_control, "max-age")
}

fn parsed_cache_control(headers: &reqwest::header::HeaderMap) -> Option<ParsedCacheControl> {
    if unreadable_cache_control(headers) {
        return Some(ParsedCacheControl {
            no_store: true,
            ..ParsedCacheControl::default()
        });
    }
    parse_values(cache_control_values(headers))
}

fn shared_max_age(control: Option<&ParsedCacheControl>) -> Option<Duration> {
    control
        .and_then(|control| control.s_maxage.or(control.max_age))
        .map(Duration::from_secs)
}

#[cfg(test)]
fn parse_cache_duration(cache_control: &str, expected: &str) -> Option<Duration> {
    conservative_delta_seconds(cache_control, expected).map(Duration::from_secs)
}

#[cfg(test)]
fn parse_cache_duration_headers(
    headers: &reqwest::header::HeaderMap,
    expected: &str,
) -> Option<Duration> {
    conservative_delta_seconds_values(cache_control_values(headers), expected)
        .map(Duration::from_secs)
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

#[cfg(test)]
pub(super) fn has_cache_directive(headers: &reqwest::header::HeaderMap, expected: &str) -> bool {
    cache_control_values(headers)
        .flat_map(directives)
        .any(|directive| directive.name_eq(expected))
}

fn cache_control_values(headers: &reqwest::header::HeaderMap) -> impl Iterator<Item = &str> {
    // `HeaderValue::to_str` rejects obs-text (0x80–0xFF), which reaches here
    // because `HeaderValue` accepts it on construction. Falling from
    // "present" to "absent" would silently upgrade a `no-store` to heuristic
    // caching, so accept any UTF-8 and let `unreadable_cache_control` fail
    // the rest closed.
    headers
        .get_all(CACHE_CONTROL)
        .iter()
        .filter_map(|value| std::str::from_utf8(value.as_bytes()).ok())
}

/// A physically present `Cache-Control` field that cannot be decoded must be
/// treated as the most conservative directive, never as absent.
fn unreadable_cache_control(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get_all(CACHE_CONTROL)
        .iter()
        .any(|value| std::str::from_utf8(value.as_bytes()).is_err())
}

pub(super) fn parse_retry_after(value: &str) -> Option<SystemTime> {
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
        let modified = SystemTime::now() - Duration::from_hours(100);
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
    fn cache_control_uses_every_physical_header_field() {
        let mut headers = HeaderMap::new();
        headers.append(
            reqwest::header::CACHE_CONTROL,
            HeaderValue::from_static(r#"extension="unterminated"#),
        );
        headers.append(
            reqwest::header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        );
        assert_eq!(
            cache_policy_for_response(StoragePolicy::Permanent, &headers),
            CachePolicy::Remove
        );
    }

    #[test]
    fn freshness_is_conservative_across_physical_header_fields() {
        let mut headers = HeaderMap::new();
        headers.append(
            reqwest::header::CACHE_CONTROL,
            HeaderValue::from_static("max-age=120"),
        );
        headers.append(
            reqwest::header::CACHE_CONTROL,
            HeaderValue::from_static("MAX-AGE=0"),
        );
        assert_eq!(
            parse_cache_duration_headers(&headers, "max-age"),
            Some(Duration::ZERO)
        );
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
