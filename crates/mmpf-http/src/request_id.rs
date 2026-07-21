//! Shared request-correlation ID value and inbound validation policy.

use serde::{Deserialize, Deserializer, Serialize};

/// Canonical lowercase HTTP header name.
pub const HEADER: &str = "x-request-id";

/// Maximum accepted length of a client-supplied request ID.
pub const MAX_LEN: usize = 128;

/// A string violated the bounded request-ID contract: non-empty, at most
/// [`MAX_LEN`] bytes, and RFC 7230 token characters only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestIdError;

impl std::fmt::Display for RequestIdError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "request id must be 1..={MAX_LEN} RFC 7230 token characters"
        )
    }
}

impl std::error::Error for RequestIdError {}

/// End-to-end request correlation ID propagated across service hops.
///
/// `Deserialize` is validating (see the manual impl below): a value arriving on
/// the internal wire that violates the contract is rejected during decoding,
/// so it can never reach a tracing field or a forwarded response header.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Serialize)]
pub struct RequestId(String);

impl RequestId {
    /// Generates a random 128-bit lowercase hexadecimal request ID.
    pub fn new_random() -> Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let bytes: [u8; 16] = rand::random();
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        Self(out)
    }

    /// Constructs an ID, enforcing the bounded HTTP-token contract. Prefer this
    /// (or [`Self::from_candidate`]) for any externally-influenced value.
    pub fn try_new(value: impl Into<String>) -> Result<Self, RequestIdError> {
        let value = value.into();
        if is_valid(&value) {
            Ok(Self(value))
        } else {
            Err(RequestIdError)
        }
    }

    /// Constructs an ID from trusted internal data (e.g. a static test fixture)
    /// without inbound validation. Do not use on values that cross a trust
    /// boundary — use [`Self::try_new`] or [`Self::from_candidate`] there.
    pub fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Accepts an inbound value only when it is a bounded HTTP token.
    pub fn from_candidate(value: &str) -> Option<Self> {
        // Validate the borrowed value before allocating, so a rejected inbound
        // id costs nothing on the per-request accept path.
        is_valid(value).then(|| Self(value.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new_random()
    }
}

impl AsRef<str> for RequestId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RequestId {
    /// Validates on the way in, so a peer cannot smuggle an out-of-contract ID
    /// (e.g. one carrying CR/LF) onto the internal wire and into logs or headers.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

/// Accepts a valid inbound value or generates a new request ID.
pub fn accept_or_generate(candidate: Option<&str>) -> RequestId {
    candidate
        .and_then(RequestId::from_candidate)
        .unwrap_or_default()
}

fn is_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_LEN
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_bounded_http_tokens() {
        let id = RequestId::from_candidate("trace_01.a-b").unwrap();
        assert_eq!(id.as_str(), "trace_01.a-b");
        assert!(RequestId::from_candidate(&"a".repeat(MAX_LEN)).is_some());
    }

    #[test]
    fn rejects_empty_oversized_and_unsafe_values() {
        for value in ["", "contains space", "contains\ttab", "non-ascii-あ"] {
            assert!(
                RequestId::from_candidate(value).is_none(),
                "accepted {value:?}"
            );
        }
        assert!(RequestId::from_candidate(&"a".repeat(MAX_LEN + 1)).is_none());
    }

    #[test]
    fn generated_ids_are_fixed_width_lowercase_hex() {
        let id = accept_or_generate(None);
        assert_eq!(id.as_str().len(), 32);
        assert!(
            id.as_str()
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
    }

    #[test]
    fn try_new_enforces_the_same_contract_as_from_candidate() {
        assert_eq!(
            RequestId::try_new("trace_01.a-b").unwrap().as_str(),
            "trace_01.a-b"
        );
        for value in ["", "contains space", "non-ascii-あ"] {
            assert!(RequestId::try_new(value).is_err(), "accepted {value:?}");
        }
        assert!(RequestId::try_new("a".repeat(MAX_LEN + 1)).is_err());
    }

    #[test]
    fn deserialize_validates_and_rejects_out_of_contract_values() {
        use serde::de::IntoDeserializer;
        use serde::de::value::{Error as ValueError, StrDeserializer};

        let valid: StrDeserializer<ValueError> = "valid-id-01".into_deserializer();
        assert_eq!(
            RequestId::deserialize(valid).unwrap().as_str(),
            "valid-id-01"
        );

        // A CR/LF-bearing value (header/log injection vector) supplied on the
        // internal wire must be rejected during decoding, not accepted verbatim.
        let injected: StrDeserializer<ValueError> = "bad\r\nvalue".into_deserializer();
        assert!(RequestId::deserialize(injected).is_err());
    }
}
