//! Percent-decoding (RFC 3986 §2.1) shared across the delivery servers.

use std::error::Error;
use std::fmt;

/// Error returned when percent-decoding fails: a truncated `%XX` escape, a
/// non-hexadecimal digit, or bytes that are not valid UTF-8.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PercentDecodeError;

impl fmt::Display for PercentDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid percent-encoding")
    }
}

impl Error for PercentDecodeError {}

/// Decodes `%XX` escapes.
///
/// A literal `+` is passed through unchanged; use [`percent_decode_form`] for
/// the form-encoding-tolerant variant.
///
/// # Errors
///
/// Returns [`PercentDecodeError`] for a truncated escape, a non-hexadecimal
/// digit, or bytes that are not valid UTF-8.
pub fn percent_decode(value: &str) -> Result<String, PercentDecodeError> {
    decode(value, false)
}

/// Like [`percent_decode`], but also maps a literal `+` byte to a space, the
/// `application/x-www-form-urlencoded` convention tolerated for query-string
/// ergonomics. `%2B` still decodes to a literal `+`.
///
/// # Errors
///
/// Returns [`PercentDecodeError`] for a truncated escape, a non-hexadecimal
/// digit, or bytes that are not valid UTF-8.
pub fn percent_decode_form(value: &str) -> Result<String, PercentDecodeError> {
    decode(value, true)
}

fn decode(value: &str, plus_as_space: bool) -> Result<String, PercentDecodeError> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%' {
            let hi = *bytes.get(index + 1).ok_or(PercentDecodeError)?;
            let lo = *bytes.get(index + 2).ok_or(PercentDecodeError)?;
            let decoded = nibble(hi)
                .and_then(|hi| nibble(lo).map(|lo| (hi << 4) | lo))
                .ok_or(PercentDecodeError)?;
            out.push(decoded);
            index += 3;
        } else if plus_as_space && byte == b'+' {
            out.push(b' ');
            index += 1;
        } else {
            out.push(byte);
            index += 1;
        }
    }
    String::from_utf8(out).map_err(|_| PercentDecodeError)
}

const fn nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(10 + byte - b'a'),
        b'A'..=b'F' => Some(10 + byte - b'A'),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{percent_decode, percent_decode_form};

    #[test]
    fn decodes_percent_escapes() {
        assert_eq!(percent_decode("%66oo").unwrap(), "foo");
        assert_eq!(percent_decode("%20%2B%20").unwrap(), " + ");
        assert_eq!(percent_decode("plain").unwrap(), "plain");
        assert_eq!(percent_decode("").unwrap(), "");
    }

    #[test]
    fn strict_variant_keeps_plus() {
        assert_eq!(percent_decode("a+b").unwrap(), "a+b");
    }

    #[test]
    fn form_variant_maps_plus_to_space() {
        assert_eq!(percent_decode_form("a+b").unwrap(), "a b");
        // An escaped plus is never reinterpreted as a space.
        assert_eq!(percent_decode_form("%2B").unwrap(), "+");
        assert_eq!(percent_decode_form("a+b%2Bc").unwrap(), "a b+c");
    }

    #[test]
    fn rejects_malformed_input() {
        assert!(percent_decode("%").is_err());
        assert!(percent_decode("%0").is_err());
        assert!(percent_decode("%zz").is_err());
        assert!(percent_decode("ok%").is_err());
        assert!(percent_decode_form("%1").is_err());
    }

    #[test]
    fn rejects_invalid_utf8() {
        assert!(percent_decode("%ff").is_err());
        assert!(percent_decode_form("%c3").is_err());
    }
}
