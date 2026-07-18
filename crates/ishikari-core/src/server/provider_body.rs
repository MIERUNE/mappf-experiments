//! Bounded provider representation decoding and validation.

use std::io::Read;

use axum::http::StatusCode;
use bytes::Bytes;
use serde::de::IgnoredAny;

use super::HttpError;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum BodyValidation {
    Bytes,
    Json,
}

pub(super) fn validate_body(
    body: &Bytes,
    content_encoding: Option<&str>,
    validation: BodyValidation,
    max_bytes: usize,
    resource: &'static str,
) -> Result<(), HttpError> {
    if validation == BodyValidation::Json {
        let decoded = decode_provider_body(body, content_encoding, max_bytes, resource)?;
        serde_json::from_slice::<IgnoredAny>(&decoded).map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("{resource} JSON invalid: {error}"),
            )
        })?;
    }
    Ok(())
}

/// Returns a decoded representation while bounding both identity and compressed
/// bodies by the resource-specific limit.
pub(super) fn decode_provider_body(
    body: &Bytes,
    content_encoding: Option<&str>,
    max_bytes: usize,
    resource: &'static str,
) -> Result<Bytes, HttpError> {
    let Some(encoding) = content_encoding
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("identity"))
    else {
        if body.len() > max_bytes {
            return Err((
                StatusCode::BAD_GATEWAY,
                format!("{resource} body too large"),
            ));
        }
        return Ok(body.clone());
    };
    if !encoding.eq_ignore_ascii_case("gzip") && !encoding.eq_ignore_ascii_case("x-gzip") {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("{resource} upstream content-encoding unsupported: {encoding}"),
        ));
    }
    let decoder = flate2::read::GzDecoder::new(body.as_ref());
    let mut limited = decoder.take(max_bytes.saturating_add(1) as u64);
    let mut decoded = Vec::with_capacity(body.len().min(max_bytes));
    limited.read_to_end(&mut decoded).map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("{resource} upstream gzip invalid: {error}"),
        )
    })?;
    if decoded.len() > max_bytes {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("{resource} decoded body too large"),
        ));
    }
    Ok(Bytes::from(decoded))
}

pub(super) fn validate_content_type(
    content_type: Option<&str>,
    accepted_content_types: &[&str],
    resource: &'static str,
) -> Result<(), HttpError> {
    if accepted_content_types.is_empty() {
        return Ok(());
    }
    // No content-type from the backend (some object stores omit it): accept, the
    // resource handler still pins the response content-type itself.
    let Some(content_type) = content_type else {
        return Ok(());
    };
    if content_type_matches(content_type, accepted_content_types) {
        return Ok(());
    }
    Err((
        StatusCode::BAD_GATEWAY,
        format!("{resource} upstream content-type unsupported: {content_type}"),
    ))
}

fn content_type_matches(value: &str, accepted: &[&str]) -> bool {
    let media_type = value
        .split_once(';')
        .map_or(value, |(media_type, _)| media_type)
        .trim();
    accepted
        .iter()
        .any(|candidate| media_type.eq_ignore_ascii_case(candidate))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{BodyValidation, content_type_matches, decode_provider_body, validate_body};

    #[test]
    fn content_type_match_ignores_parameters_and_case() {
        assert!(content_type_matches(
            "Application/JSON; charset=utf-8",
            &["application/json"]
        ));
        assert!(content_type_matches(
            "application/octet-stream",
            &["image/png", "application/octet-stream"]
        ));
        assert!(!content_type_matches("text/html", &["application/json"]));
    }

    #[test]
    fn decode_provider_body_never_panics_and_bounds_output() {
        // Arbitrary non-gzip bytes under an identity/`None` encoding round-trip
        // up to the limit and are rejected past it.
        assert!(
            decode_provider_body(
                &Bytes::from_static(b"\x00\xff\x1f\x8b junk"),
                None,
                1024,
                "style",
            )
            .is_ok()
        );
        assert!(decode_provider_body(&Bytes::from(vec![0u8; 2048]), None, 1024, "style").is_err());
        assert!(
            decode_provider_body(
                &Bytes::from_static(b"anything"),
                Some("identity"),
                1024,
                "style",
            )
            .is_ok()
        );

        // Truncated / non-gzip payloads under a gzip encoding error, not panic.
        assert!(
            decode_provider_body(
                &Bytes::from_static(b"not gzip"),
                Some("gzip"),
                1024,
                "style",
            )
            .is_err()
        );
        assert!(
            decode_provider_body(
                &Bytes::from_static(&[0x1f, 0x8b, 0x08]),
                Some("gzip"),
                1024,
                "style",
            )
            .is_err()
        );

        // Unsupported encodings are rejected rather than silently served.
        assert!(
            decode_provider_body(&Bytes::from_static(b"x"), Some("br"), 1024, "style").is_err()
        );

        // A gzip bomb cannot exceed the decoded bound.
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        std::io::Write::write_all(&mut encoder, &vec![0u8; 1_000_000]).unwrap();
        let bomb = encoder.finish().unwrap();
        assert!(decode_provider_body(&Bytes::from(bomb), Some("gzip"), 4096, "style").is_err());
    }

    #[test]
    fn json_validation_runs_after_bounded_decoding() {
        assert!(
            validate_body(
                &Bytes::from_static(br#"{"version":8}"#),
                None,
                BodyValidation::Json,
                1024,
                "style",
            )
            .is_ok()
        );
        assert!(
            validate_body(
                &Bytes::from_static(b"not-json"),
                None,
                BodyValidation::Json,
                1024,
                "style",
            )
            .is_err()
        );
        assert!(
            validate_body(
                &Bytes::from_static(b"opaque"),
                None,
                BodyValidation::Bytes,
                1024,
                "sprite",
            )
            .is_ok()
        );
    }
}
