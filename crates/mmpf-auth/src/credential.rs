use std::borrow::Cow;

use anyhow::bail;
use http::HeaderMap;
use sha2::{Digest, Sha256};

use crate::catalog::validate_registry_id;

const MAX_CREDENTIAL_BYTES: usize = 4096;
const DIGEST_DOMAIN: &[u8] = b"mmpf-object-store-auth-v1\0";
const CACHE_PARTITION_DOMAIN: &[u8] = b"mmpf-delivery-cache-partition-v1\0";
const ANONYMOUS_CACHE_PARTITION_DOMAIN: &[u8] = b"mmpf-delivery-anonymous-cache-partition-v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthFailure {
    InvalidCredential,
    Forbidden,
    Unavailable,
}

pub(super) struct PresentedCredential<'a> {
    pub(super) value: Cow<'a, str>,
    pub(super) from_query: bool,
}

pub(super) fn delivery_token<'a>(
    headers: &'a HeaderMap,
    query: Option<&'a str>,
) -> Result<Option<PresentedCredential<'a>>, AuthFailure> {
    let bearer = bearer_token(headers)?;
    let query = access_token_from_query(query)?;
    match (bearer, query) {
        (Some(_), Some(_)) => Err(AuthFailure::InvalidCredential),
        (Some(token), None) => Ok(Some(PresentedCredential {
            value: Cow::Borrowed(token),
            from_query: false,
        })),
        (None, Some(token)) => Ok(Some(PresentedCredential {
            value: token,
            from_query: true,
        })),
        (None, None) => Ok(None),
    }
}

fn bearer_token(headers: &HeaderMap) -> Result<Option<&str>, AuthFailure> {
    let mut values = headers.get_all(http::header::AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AuthFailure::InvalidCredential);
    }
    let value = value.to_str().map_err(|_| AuthFailure::InvalidCredential)?;
    let (scheme, token) = value
        .split_once(' ')
        .ok_or(AuthFailure::InvalidCredential)?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.contains(char::is_whitespace)
    {
        return Err(AuthFailure::InvalidCredential);
    }
    Ok(Some(token))
}

fn access_token_from_query(query: Option<&str>) -> Result<Option<Cow<'_, str>>, AuthFailure> {
    let Some(query) = query else {
        return Ok(None);
    };
    let mut token = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if key != "access_token" {
            continue;
        }
        if token.replace(value).is_some() {
            return Err(AuthFailure::InvalidCredential);
        }
    }
    Ok(token)
}

pub(super) fn parse_token_envelope(token: &str) -> Result<(&str, &str), AuthFailure> {
    let (registry_id, credential) = token
        .split_once('.')
        .ok_or(AuthFailure::InvalidCredential)?;
    validate_registry_id(registry_id).map_err(|_| AuthFailure::InvalidCredential)?;
    if credential.is_empty()
        || credential.len() > MAX_CREDENTIAL_BYTES
        || credential
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(AuthFailure::InvalidCredential);
    }
    Ok((registry_id, credential))
}

pub(super) fn credential_digest(registry_id: &str, credential: &str) -> [u8; 32] {
    namespaced_digest(DIGEST_DOMAIN, registry_id, credential)
}

pub(super) fn credential_cache_partition(
    registry_id: &str,
    credential: &str,
    registry_revision: u64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CACHE_PARTITION_DOMAIN);
    hasher.update(registry_revision.to_be_bytes());
    hasher.update((registry_id.len() as u64).to_be_bytes());
    hasher.update(registry_id.as_bytes());
    hasher.update((credential.len() as u64).to_be_bytes());
    hasher.update(credential.as_bytes());
    hasher.finalize().into()
}

pub(super) fn anonymous_cache_partition(registry_id: &str, registry_revision: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(ANONYMOUS_CACHE_PARTITION_DOMAIN);
    hasher.update(registry_revision.to_be_bytes());
    hasher.update((registry_id.len() as u64).to_be_bytes());
    hasher.update(registry_id.as_bytes());
    hasher.finalize().into()
}

fn namespaced_digest(domain: &[u8], registry_id: &str, credential: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((registry_id.len() as u64).to_be_bytes());
    hasher.update(registry_id.as_bytes());
    hasher.update((credential.len() as u64).to_be_bytes());
    hasher.update(credential.as_bytes());
    hasher.finalize().into()
}

/// Encodes the verifier digest stored in a registry snapshot for one opaque
/// credential suffix. Registry tooling can use this without reimplementing the
/// domain-separated hash contract.
pub fn credential_sha256(registry_id: &str, credential: &str) -> String {
    encode_sha256_bytes(credential_digest(registry_id, credential))
}

pub(super) fn decode_sha256(value: &str) -> anyhow::Result<[u8; 32]> {
    if value.len() != 64 {
        bail!("credential_sha256 must be 64 lowercase hexadecimal characters");
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let high = decode_lower_hex(pair[0])?;
        let low = decode_lower_hex(pair[1])?;
        digest[index] = (high << 4) | low;
    }
    Ok(digest)
}

fn decode_lower_hex(byte: u8) -> anyhow::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => bail!("credential_sha256 must use lowercase hexadecimal"),
    }
}

fn encode_sha256_bytes(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    #[test]
    fn token_suffix_is_opaque_and_split_only_once() {
        let (registry, credential) = parse_token_envelope("corp.aaa.bbb.ccc").unwrap();
        assert_eq!(registry, "corp");
        assert_eq!(credential, "aaa.bbb.ccc");
    }

    #[test]
    fn anonymous_cache_partition_is_revision_scoped_and_domain_separated() {
        assert_ne!(
            anonymous_cache_partition("public", 1),
            anonymous_cache_partition("public", 2)
        );
        assert_ne!(
            anonymous_cache_partition("public", 1),
            credential_cache_partition("public", "anonymous", 1)
        );
    }

    #[test]
    fn query_tokens_are_decoded_once_and_cannot_be_mixed_or_repeated() {
        assert_eq!(
            access_token_from_query(Some("x=1&access_token=public.a%2Bb.c"))
                .unwrap()
                .as_deref(),
            Some("public.a+b.c")
        );
        assert!(matches!(
            access_token_from_query(Some("access_token=one&access_token=two")),
            Err(AuthFailure::InvalidCredential)
        ));
        assert!(matches!(
            delivery_token(&headers("public.header"), Some("access_token=public.query")),
            Err(AuthFailure::InvalidCredential)
        ));
    }

    #[test]
    fn duplicate_authorization_headers_are_rejected() {
        let mut headers = HeaderMap::new();
        headers.append(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer public.one"),
        );
        headers.append(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer public.two"),
        );
        assert!(matches!(
            bearer_token(&headers),
            Err(AuthFailure::InvalidCredential)
        ));
    }
}
