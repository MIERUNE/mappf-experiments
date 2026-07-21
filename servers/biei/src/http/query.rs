use crate::http::error::{IngressError, invalid};
use biei_core::types::Padding;

/// Extract `before_layer=<id>` from a query string. Per the static image grammar this is
/// a request-level parameter (applies to all overlays in the URL). Other
/// query parameters are accepted but ignored. Returns `Ok(None)` if not set.
pub(crate) fn parse_before_layer_from_query(
    query: Option<&str>,
) -> Result<Option<String>, IngressError> {
    let Some(q) = query else {
        return Ok(None);
    };
    for pair in q.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if key != "before_layer" {
            continue;
        }
        validate_before_layer(Some(value))?;
        return Ok(Some(value.to_string()));
    }
    Ok(None)
}

pub(crate) fn validate_before_layer(before_layer: Option<&str>) -> Result<(), IngressError> {
    let Some(value) = before_layer else {
        return Ok(());
    };
    if value.is_empty() || value.len() > 64 {
        return Err(invalid("before_layer must be 1..=64 characters"));
    }
    // Whitelist of style-spec-typical layer-id characters. Keeps mbgl's FFI
    // surface clean and rejects anything that could reach logs or downstream
    // string interpolation through the typed forwarded-request path.
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(invalid("before_layer contains an unsupported character"));
    }
    Ok(())
}

/// Largest padding value accepted. Pixels — keeps a single side from
/// consuming the whole renderable area for any reasonable image size.
pub(crate) const MAX_PADDING: u16 = 1024;

pub(crate) fn validate_padding(padding: Padding) -> Result<(), IngressError> {
    for (name, value) in [
        ("padding top", padding.top),
        ("padding right", padding.right),
        ("padding bottom", padding.bottom),
        ("padding left", padding.left),
    ] {
        if value > MAX_PADDING {
            return Err(invalid(format!("{name} must be in [0, {MAX_PADDING}]")));
        }
    }
    Ok(())
}

/// Extract `padding=...` from a query string. Accepts:
/// - `padding=N` — uniform on all sides.
/// - `padding=top,right,bottom,left` — CSS-style 4-value form.
///
/// Other arities (2, 3) are rejected for v0 to avoid the
/// shorthand-ambiguity footgun. Bbox / Auto positioning use this padding
/// when fitting the viewport; `Center` ignores it. Returns `None` when
/// the parameter is absent so positioning-specific defaults can apply.
pub(crate) fn parse_padding_from_query(
    query: Option<&str>,
) -> Result<Option<Padding>, IngressError> {
    let Some(q) = query else {
        return Ok(None);
    };
    for pair in q.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if key != "padding" {
            continue;
        }
        return parse_padding_value(value).map(Some);
    }
    Ok(None)
}

fn parse_padding_value(value: &str) -> Result<Padding, IngressError> {
    let parts: Vec<&str> = value.split(',').collect();
    let sides = match parts.as_slice() {
        [v] => {
            let n = parse_padding_side(v, "padding")?;
            [n, n, n, n]
        }
        [t, r, b, l] => [
            parse_padding_side(t, "padding top")?,
            parse_padding_side(r, "padding right")?,
            parse_padding_side(b, "padding bottom")?,
            parse_padding_side(l, "padding left")?,
        ],
        _ => {
            return Err(invalid("padding must be `N` or `top,right,bottom,left`"));
        }
    };
    let padding = Padding {
        top: sides[0],
        right: sides[1],
        bottom: sides[2],
        left: sides[3],
    };
    validate_padding(padding)?;
    Ok(padding)
}

fn parse_padding_side(value: &str, name: &str) -> Result<u16, IngressError> {
    let n = value
        .parse::<u16>()
        .map_err(|_| invalid(format!("{name} must be a non-negative integer")))?;
    if n > MAX_PADDING {
        return Err(invalid(format!("{name} must be in [0, {MAX_PADDING}]")));
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_padding_uniform_from_query() {
        assert_eq!(
            parse_padding_from_query(Some("padding=20")).expect("uniform"),
            Some(Padding::all(20))
        );
    }

    #[test]
    fn parses_padding_four_sides_from_query() {
        assert_eq!(
            parse_padding_from_query(Some("padding=1,2,3,4")).expect("4 sides"),
            Some(Padding {
                top: 1,
                right: 2,
                bottom: 3,
                left: 4,
            })
        );
    }

    #[test]
    fn padding_returns_none_when_absent() {
        assert_eq!(parse_padding_from_query(None).unwrap(), None);
        assert_eq!(parse_padding_from_query(Some("foo=bar")).unwrap(), None);
    }

    #[test]
    fn rejects_padding_with_unsupported_arity() {
        let err = parse_padding_from_query(Some("padding=10,20")).expect_err("2-value rejected");
        assert!(err.to_string().contains("padding"));
        let err = parse_padding_from_query(Some("padding=10,20,30")).expect_err("3-value rejected");
        assert!(err.to_string().contains("padding"));
    }

    #[test]
    fn rejects_padding_above_max() {
        let err = parse_padding_from_query(Some("padding=99999"))
            .expect_err("padding above MAX_PADDING rejected");
        assert!(err.to_string().contains("padding"));
    }

    #[test]
    fn parses_before_layer_from_query_when_present() {
        let parsed =
            parse_before_layer_from_query(Some("before_layer=road-label")).expect("valid layer id");
        assert_eq!(parsed.as_deref(), Some("road-label"));
    }

    #[test]
    fn parse_before_layer_returns_none_for_absent_or_unrelated_query() {
        assert_eq!(parse_before_layer_from_query(None), Ok(None));
        assert_eq!(parse_before_layer_from_query(Some("")), Ok(None));
        assert_eq!(parse_before_layer_from_query(Some("foo=bar")), Ok(None));
    }

    #[test]
    fn parse_before_layer_rejects_invalid_characters() {
        let err = parse_before_layer_from_query(Some("before_layer=foo/bar"))
            .expect_err("slash is not in the whitelist");
        assert!(matches!(err, IngressError::InvalidRequest(_)));
    }

    #[test]
    fn parse_before_layer_rejects_overly_long_value() {
        let long = format!("before_layer={}", "a".repeat(65));
        assert!(matches!(
            parse_before_layer_from_query(Some(&long)),
            Err(IngressError::InvalidRequest(_))
        ));
    }
}
