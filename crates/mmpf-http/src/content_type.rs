//! Parsing helpers for `Content-Type` header values.

/// Compares the media type in a `Content-Type` value with an expected media
/// type, ignoring ASCII case, surrounding whitespace, and parameters.
pub fn media_type_eq(value: &str, expected: &str) -> bool {
    value
        .split_once(';')
        .map_or(value, |(media_type, _)| media_type)
        .trim()
        .eq_ignore_ascii_case(expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compares_media_types_without_parameters_or_case() {
        assert!(media_type_eq("application/json", "application/json"));
        assert!(media_type_eq(
            " Application/JSON; charset=utf-8 ",
            "application/json"
        ));
        assert!(!media_type_eq("text/html", "application/json"));
    }
}
