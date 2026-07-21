//! Allocation-free tokenization of `Cache-Control` directives.

/// One comma-separated `Cache-Control` directive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Directive<'a> {
    name: &'a str,
    value: Option<&'a str>,
}

impl<'a> Directive<'a> {
    /// Returns the directive argument, already whitespace-trimmed. A complete
    /// surrounding quoted-string is stripped; malformed quotes are preserved
    /// so policy code cannot mistake a fragment for a valid value.
    pub fn value(&self) -> Option<&'a str> {
        self.value
    }

    pub fn name_eq(&self, expected: &str) -> bool {
        self.name.eq_ignore_ascii_case(expected)
    }

    /// Parses this directive's value as HTTP delta-seconds. Callers decide
    /// whether a missing or malformed value is ignored or treated as zero.
    pub fn delta_seconds(&self) -> Option<u64> {
        self.value.and_then(parse_delta_seconds)
    }
}

/// Iterates over non-empty directives. Commas inside a quoted-string do not
/// split the field value, and an unterminated quote consumes the remainder
/// rather than manufacturing directives from its contents.
pub fn directives(value: &str) -> Directives<'_> {
    Directives { remaining: value }
}

pub struct Directives<'a> {
    remaining: &'a str,
}

/// Conservatively parsed directives relevant to HTTP cache policy.
///
/// Duplicate delta-seconds directives retain their smallest value. A present
/// but malformed value contributes zero, so reordering fields can never extend
/// freshness. Unknown extension directives are intentionally ignored.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ParsedCacheControl {
    pub no_store: bool,
    pub no_cache: bool,
    pub private: bool,
    pub must_revalidate: bool,
    pub proxy_revalidate: bool,
    pub no_transform: bool,
    pub immutable: bool,
    pub max_age: Option<u64>,
    pub s_maxage: Option<u64>,
    pub stale_while_revalidate: Option<u64>,
}

/// Parses every physical `Cache-Control` field in one pass. Returns `None`
/// when the header is absent.
pub fn parse_values<'a>(values: impl IntoIterator<Item = &'a str>) -> Option<ParsedCacheControl> {
    let mut control = ParsedCacheControl::default();
    let mut present = false;
    for value in values {
        present = true;
        for directive in directives(value) {
            if directive.name_eq("no-store") {
                control.no_store = true;
            } else if directive.name_eq("no-cache") {
                control.no_cache = true;
            } else if directive.name_eq("private") {
                control.private = true;
            } else if directive.name_eq("must-revalidate") {
                control.must_revalidate = true;
            } else if directive.name_eq("proxy-revalidate") {
                control.proxy_revalidate = true;
            } else if directive.name_eq("no-transform") {
                control.no_transform = true;
            } else if directive.name_eq("immutable") {
                control.immutable = true;
            } else if directive.name_eq("max-age") {
                merge_conservative(&mut control.max_age, directive.delta_seconds());
            } else if directive.name_eq("s-maxage") {
                merge_conservative(&mut control.s_maxage, directive.delta_seconds());
            } else if directive.name_eq("stale-while-revalidate") {
                merge_conservative(
                    &mut control.stale_while_revalidate,
                    directive.delta_seconds(),
                );
            }
        }
    }
    present.then_some(control)
}

/// Parses one `Cache-Control` field value.
pub fn parse(value: &str) -> ParsedCacheControl {
    parse_values(std::iter::once(value)).expect("one cache-control value is present")
}

fn merge_conservative(target: &mut Option<u64>, parsed: Option<u64>) {
    // A named directive with an absent or malformed value is conservative zero.
    let candidate = parsed.unwrap_or(0);
    *target = Some(target.map_or(candidate, |current| current.min(candidate)));
}

impl<'a> Iterator for Directives<'a> {
    type Item = Directive<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.remaining.is_empty() {
                return None;
            }

            let bytes = self.remaining.as_bytes();
            let mut quoted = false;
            let mut escaped = false;
            let mut boundary = bytes.len();
            for (index, byte) in bytes.iter().copied().enumerate() {
                if escaped {
                    escaped = false;
                    continue;
                }
                match byte {
                    b'\\' if quoted => escaped = true,
                    b'"' => quoted = !quoted,
                    b',' if !quoted => {
                        boundary = index;
                        break;
                    }
                    _ => {}
                }
            }

            let token = self.remaining[..boundary].trim();
            self.remaining = if boundary < bytes.len() {
                &self.remaining[boundary + 1..]
            } else {
                ""
            };
            if token.is_empty() {
                continue;
            }

            let (name, value) = match token.split_once('=') {
                Some((name, value)) => (name.trim(), Some(normalize_value(value.trim()))),
                None => (token, None),
            };
            return Some(Directive { name, value });
        }
    }
}

fn normalize_value(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() < 2 || bytes.first() != Some(&b'"') || bytes.last() != Some(&b'"') {
        return value;
    }

    let mut escaped = false;
    for byte in bytes[1..bytes.len() - 1].iter().copied() {
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return value;
        }
    }
    if escaped {
        value
    } else {
        &value[1..value.len() - 1]
    }
}

/// Returns the most conservative value of a repeated delta-seconds directive.
///
/// Reordering duplicate freshness fields must never extend a cache lifetime.
/// An explicitly present but missing or malformed value therefore contributes
/// zero, while a value larger than `u64` is saturated.
pub fn conservative_delta_seconds(value: &str, expected: &str) -> Option<u64> {
    conservative_delta_seconds_values(std::iter::once(value), expected)
}

/// Returns the conservative delta-seconds value across every physical header
/// field. Each field is tokenized independently so a malformed quote in one
/// field cannot hide a directive in the next field.
pub fn conservative_delta_seconds_values<'a>(
    values: impl IntoIterator<Item = &'a str>,
    expected: &str,
) -> Option<u64> {
    values
        .into_iter()
        .flat_map(directives)
        .filter(|directive| directive.name_eq(expected))
        .map(|directive| directive.value().and_then(parse_delta_seconds).unwrap_or(0))
        .min()
}

fn parse_delta_seconds(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(value.bytes().fold(0_u64, |seconds, byte| {
        seconds
            .saturating_mul(10)
            .saturating_add(u64::from(byte - b'0'))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_names_values_whitespace_and_quotes() {
        let parsed: Vec<_> =
            directives(" public, max-age=60, s-maxage=\"120\", must-revalidate ").collect();
        assert_eq!(
            parsed,
            vec![
                Directive {
                    name: "public",
                    value: None,
                },
                Directive {
                    name: "max-age",
                    value: Some("60"),
                },
                Directive {
                    name: "s-maxage",
                    value: Some("120"),
                },
                Directive {
                    name: "must-revalidate",
                    value: None,
                },
            ]
        );
    }

    #[test]
    fn preserves_duplicates_and_malformed_values_for_policy_layers() {
        let parsed: Vec<_> = directives("max-age=60, MAX-AGE=bad, no-cache, ,").collect();
        assert_eq!(parsed.len(), 3);
        assert!(parsed[0].name_eq("max-age"));
        assert!(parsed[1].name_eq("max-age"));
        assert_eq!(parsed[1].value(), Some("bad"));
        assert!(parsed[2].name_eq("NO-CACHE"));
    }

    #[test]
    fn quoted_commas_never_create_synthetic_directives() {
        let parsed: Vec<_> = directives(r#"extension="private,no-store", max-age=60"#).collect();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].value(), Some("private,no-store"));
        assert!(parsed[1].name_eq("max-age"));

        let malformed: Vec<_> = directives(r#"max-age="60,no-store"#).collect();
        assert_eq!(malformed.len(), 1);
        assert_eq!(
            conservative_delta_seconds(r#"max-age="60,no-store"#, "max-age"),
            Some(0)
        );
    }

    #[test]
    fn delta_seconds_require_complete_ascii_digits() {
        for value in [
            "max-age=+60",
            "max-age=-1",
            "max-age=60x",
            r#"max-age="60" trailing"#,
        ] {
            assert_eq!(conservative_delta_seconds(value, "max-age"), Some(0));
        }
        assert_eq!(
            conservative_delta_seconds(r#"max-age="60""#, "max-age"),
            Some(60)
        );
    }

    #[test]
    fn physical_fields_are_evaluated_independently() {
        assert_eq!(
            conservative_delta_seconds_values(
                [r#"extension="unterminated"#, "max-age=30"],
                "max-age"
            ),
            Some(30)
        );
    }

    #[test]
    fn delta_seconds_are_conservative_and_order_independent() {
        for value in ["max-age=604800, max-age=0", "max-age=0, max-age=604800"] {
            assert_eq!(conservative_delta_seconds(value, "max-age"), Some(0));
        }
        assert_eq!(
            conservative_delta_seconds("MAX-AGE=30, max-age=bad", "max-age"),
            Some(0)
        );
        assert_eq!(conservative_delta_seconds("public", "max-age"), None);
        assert_eq!(
            conservative_delta_seconds("max-age=999999999999999999999999999999999999", "max-age"),
            Some(u64::MAX)
        );
    }

    #[test]
    fn summary_parses_policy_and_freshness_in_one_pass() {
        let control = parse_values([
            "public, max-age=120, stale-while-revalidate=60",
            "MAX-AGE=30, must-revalidate, no-transform",
        ])
        .expect("cache-control present");

        assert_eq!(control.max_age, Some(30));
        assert_eq!(control.stale_while_revalidate, Some(60));
        assert!(control.must_revalidate);
        assert!(control.no_transform);
        assert!(!control.no_store);
    }

    #[test]
    fn summary_treats_malformed_duplicate_freshness_as_zero() {
        let control = parse("s-maxage=60, S-MAXAGE=invalid");
        assert_eq!(control.s_maxage, Some(0));
    }
}
