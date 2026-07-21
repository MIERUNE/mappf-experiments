//! Shared HTTP response and request-origin helpers.

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};

use super::conditional::Validators;

/// Builds a `200 OK` response carrying `body` with the given content type and an
/// optional `Cache-Control`. Shared by the glyph / sprite / internal handlers so
/// the status/header boilerplate lives in one place.
pub(crate) fn bytes_response(
    body: impl Into<Body>,
    content_type: &'static str,
    cache_control: Option<&'static str>,
) -> Response {
    let mut out = Response::new(body.into());
    *out.status_mut() = StatusCode::OK;
    out.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Some(cache_control) = cache_control {
        out.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(cache_control),
        );
    }
    out
}

/// Marks a generated document whose absolute URLs depend on request origin
/// metadata supplied by the client or trusted reverse proxy.
pub(crate) fn apply_origin_vary(headers: &mut HeaderMap) {
    headers.insert(
        header::VARY,
        HeaderValue::from_static("Origin, X-Forwarded-Proto"),
    );
}

/// Serves a derived JSON document (TileJSON and similar origin-dependent bodies)
/// validated by a strong ETag over the exact bytes served: answers conditional
/// requests with `304 Not Modified`, otherwise `200` with the body. Both branches
/// carry `cache_control` and the origin `Vary`, matching the derived-representation
/// caching contract shared by the base and derived tileset endpoints.
pub(crate) fn derived_json_response(
    body: Vec<u8>,
    headers: &HeaderMap,
    cache_control: &'static str,
) -> Response {
    let validators = Validators::for_derived_body(&body);
    if validators.not_modified(headers) {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(cache_control),
        );
        validators.apply(response.headers_mut());
        apply_origin_vary(response.headers_mut());
        return response;
    }
    let mut response = bytes_response(body, "application/json", Some(cache_control));
    validators.apply(response.headers_mut());
    apply_origin_vary(response.headers_mut());
    response
}

pub(crate) fn get_origin(headers: &HeaderMap) -> String {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty());
    let origin_parts = origin.and_then(split_origin);
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .or_else(|| origin_parts.map(|(origin_scheme, _)| origin_scheme))
        // Reflect only real web schemes. A spoofed `X-Forwarded-Proto` such as
        // `https://attacker/x?` would otherwise be interpolated as the scheme and
        // point emitted glyph/sprite/tile URLs off-origin.
        .filter(|value| is_reflectable_scheme(value))
        .unwrap_or("http");
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| is_reflectable_host(value))
        .or_else(|| {
            origin_parts
                .map(|(_, origin_host)| origin_host)
                .filter(|value| is_reflectable_host(value))
        })
        .unwrap_or("127.0.0.1:8080");
    format!("{scheme}://{host}")
}

/// Whether a client-supplied `Host`/`Origin` host is safe to interpolate into
/// emitted URLs (TileJSON `tiles`, style `glyphs`/`sprite`/source URLs). A spoofed
/// `Host` is otherwise reflected verbatim — a header-injection / reflected-URL
/// vector — so restrict it to the characters a real authority can contain.
fn is_reflectable_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 255
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b':' | b'_'))
}

/// Whether a client-supplied forwarded scheme is safe to reflect into emitted
/// URLs. Only `http`/`https`; anything else falls back to the default.
fn is_reflectable_scheme(scheme: &str) -> bool {
    scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
}

/// Splits an Origin header into scheme and host components.
fn split_origin(origin: &str) -> Option<(&str, &str)> {
    let (scheme, rest) = origin.split_once("://")?;
    let host = rest.split('/').next()?;
    if scheme.is_empty() || host.is_empty() {
        return None;
    }
    Some((scheme, host))
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};

    use super::{get_origin, is_reflectable_host};

    #[test]
    fn rejects_hosts_with_injection_chars() {
        assert!(is_reflectable_host("ishikari-demo.mierune.dev"));
        assert!(is_reflectable_host("127.0.0.1:8080"));
        assert!(!is_reflectable_host("evil.test/path"));
        assert!(!is_reflectable_host("evil.test foo"));
        assert!(!is_reflectable_host(""));
    }

    #[test]
    fn get_origin_does_not_reflect_a_spoofed_host() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("good.example:8080"));
        assert_eq!(get_origin(&headers), "http://good.example:8080");

        // A `Host` carrying a path separator is dropped, not reflected verbatim.
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("a.test/evil"));
        assert_eq!(get_origin(&headers), "http://127.0.0.1:8080");
    }

    #[test]
    fn get_origin_rejects_spoofed_forwarded_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("good.example"));
        // A forwarded-proto that smuggles an authority is not reflected as the
        // scheme; it falls back to the default `http`.
        headers.insert(
            "x-forwarded-proto",
            HeaderValue::from_static("https://attacker.example/x?"),
        );
        assert_eq!(get_origin(&headers), "http://good.example");

        // A legitimate forwarded scheme is honored.
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert_eq!(get_origin(&headers), "https://good.example");
    }
}
