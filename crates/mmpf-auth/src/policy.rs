use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, bail};
use http::HeaderMap;
use serde::Deserialize;
use url::Url;

use crate::credential::{AuthFailure, decode_sha256};

const MAX_CREDENTIALS_PER_REGISTRY: usize = 100_000;
const MAX_PRINCIPAL_ID_BYTES: usize = 256;
const MAX_NAMESPACES_PER_CREDENTIAL: usize = 1024;
const MAX_ORIGINS_PER_CREDENTIAL: usize = 128;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash)]
pub enum DeliveryAction {
    #[serde(rename = "render.static")]
    RenderStatic,
    #[serde(rename = "read")]
    Read,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrySnapshotWire {
    schema_version: u32,
    registry_id: String,
    revision: u64,
    #[serde(default)]
    anonymous: Option<AnonymousGrantWire>,
    credentials: Vec<CredentialGrantWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AnonymousGrantWire {
    enabled: bool,
    namespaces: Vec<String>,
    actions: Vec<DeliveryAction>,
    #[serde(default)]
    allowed_origins: Vec<String>,
    allow_missing_origin: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialGrantWire {
    credential_sha256: String,
    principal_id: String,
    enabled: bool,
    namespaces: Vec<String>,
    actions: Vec<DeliveryAction>,
    #[serde(default)]
    allowed_origins: Vec<String>,
    allow_missing_origin: bool,
}

pub(super) struct RegistrySnapshot {
    pub(super) revision: u64,
    pub(super) anonymous: Option<AuthorizationGrant>,
    pub(super) credentials: HashMap<[u8; 32], CredentialGrant>,
}

pub(super) struct CredentialGrant {
    pub(super) authorization: AuthorizationGrant,
}

pub(super) struct AuthorizationGrant {
    pub(super) principal_id: String,
    pub(super) enabled: bool,
    pub(super) namespaces: Arc<[String]>,
    actions: HashSet<DeliveryAction>,
    allowed_origins: Vec<String>,
    allow_missing_origin: bool,
}

impl RegistrySnapshot {
    pub(super) fn parse(expected_registry_id: &str, body: &[u8]) -> anyhow::Result<Self> {
        let wire: RegistrySnapshotWire =
            serde_json::from_slice(body).context("parse auth registry JSON")?;
        if wire.schema_version != 1 {
            bail!("unsupported auth registry schema_version");
        }
        if wire.registry_id != expected_registry_id {
            bail!("auth registry id does not match configured registry");
        }
        if wire.credentials.len() > MAX_CREDENTIALS_PER_REGISTRY {
            bail!("auth registry has too many credentials");
        }
        let anonymous = wire
            .anonymous
            .map(|grant| {
                normalize_grant(
                    "anonymous".to_string(),
                    grant.enabled,
                    grant.namespaces,
                    grant.actions,
                    grant.allowed_origins,
                    grant.allow_missing_origin,
                )
            })
            .transpose()?;
        let mut credentials = HashMap::with_capacity(wire.credentials.len());
        for grant in wire.credentials {
            let digest = decode_sha256(&grant.credential_sha256)?;
            if credentials.contains_key(&digest) {
                bail!("auth registry contains a duplicate credential digest");
            }
            credentials.insert(
                digest,
                CredentialGrant {
                    authorization: normalize_grant(
                        grant.principal_id,
                        grant.enabled,
                        grant.namespaces,
                        grant.actions,
                        grant.allowed_origins,
                        grant.allow_missing_origin,
                    )?,
                },
            );
        }
        Ok(Self {
            revision: wire.revision,
            anonymous,
            credentials,
        })
    }
}

fn normalize_grant(
    principal_id: String,
    enabled: bool,
    mut namespaces: Vec<String>,
    actions: Vec<DeliveryAction>,
    allowed_origins: Vec<String>,
    allow_missing_origin: bool,
) -> anyhow::Result<AuthorizationGrant> {
    validate_bounded_label("principal_id", &principal_id, MAX_PRINCIPAL_ID_BYTES)?;
    if namespaces.is_empty() || namespaces.len() > MAX_NAMESPACES_PER_CREDENTIAL {
        bail!("credential namespaces must be non-empty and bounded");
    }
    for namespace in &namespaces {
        if namespace != "*" {
            validate_bounded_label("namespace", namespace, 256)?;
        }
    }
    namespaces.sort_unstable();
    namespaces.dedup();
    if namespaces
        .binary_search_by(|value| value.as_str().cmp("*"))
        .is_ok()
    {
        namespaces.clear();
        namespaces.push("*".to_string());
    }
    let actions: HashSet<_> = actions.into_iter().collect();
    if actions.is_empty() {
        bail!("credential actions must not be empty");
    }
    if allowed_origins.len() > MAX_ORIGINS_PER_CREDENTIAL {
        bail!("credential has too many allowed origins");
    }
    let allowed_origins = allowed_origins
        .iter()
        .map(|origin| normalize_declared_origin(origin))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(AuthorizationGrant {
        principal_id,
        enabled,
        namespaces: namespaces.into(),
        actions,
        allowed_origins,
        allow_missing_origin,
    })
}

pub(super) fn authorize_grant(
    headers: &HeaderMap,
    grant: &AuthorizationGrant,
    namespace: Option<&str>,
    action: DeliveryAction,
) -> Result<(), AuthFailure> {
    if namespace.is_some_and(|namespace| {
        grant.namespaces.first().is_none_or(|first| first != "*")
            && grant
                .namespaces
                .binary_search_by(|allowed| allowed.as_str().cmp(namespace))
                .is_err()
    }) || !grant.actions.contains(&action)
    {
        return Err(AuthFailure::Forbidden);
    }
    authorize_origin(headers, grant)
}

fn validate_bounded_label(name: &str, value: &str, max_bytes: usize) -> anyhow::Result<()> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        bail!("{name} must be non-empty, bounded, and contain no control characters");
    }
    Ok(())
}

fn authorize_origin(headers: &HeaderMap, grant: &AuthorizationGrant) -> Result<(), AuthFailure> {
    if grant.allowed_origins.is_empty() {
        return Ok(());
    }
    let origin = single_header(headers, http::header::ORIGIN)?
        .map(normalize_declared_origin)
        .transpose()
        .map_err(|_| AuthFailure::Forbidden)?;
    let origin = match origin {
        Some(origin) => Some(origin),
        None => single_header(headers, http::header::REFERER)?
            .map(normalize_referer_origin)
            .transpose()
            .map_err(|_| AuthFailure::Forbidden)?,
    };
    match origin {
        Some(origin)
            if grant
                .allowed_origins
                .iter()
                .any(|allowed| allowed == &origin) =>
        {
            Ok(())
        }
        None if grant.allow_missing_origin => Ok(()),
        _ => Err(AuthFailure::Forbidden),
    }
}

fn single_header(
    headers: &HeaderMap,
    name: http::header::HeaderName,
) -> Result<Option<&str>, AuthFailure> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AuthFailure::Forbidden);
    }
    value.to_str().map(Some).map_err(|_| AuthFailure::Forbidden)
}

fn normalize_declared_origin(raw: &str) -> anyhow::Result<String> {
    let url = Url::parse(raw).context("parse origin")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("origin must be HTTP(S)");
    }
    let origin = url.origin().ascii_serialization();
    if origin == "null" {
        bail!("opaque origins are not supported");
    }
    Ok(origin)
}

fn normalize_referer_origin(raw: &str) -> anyhow::Result<String> {
    let url = Url::parse(raw).context("parse referer")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        bail!("referer must be HTTP(S)");
    }
    let origin = url.origin().ascii_serialization();
    if origin == "null" {
        bail!("opaque origins are not supported");
    }
    Ok(origin)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::credential_sha256;
    use http::{HeaderName, HeaderValue};

    #[test]
    fn malformed_snapshots_are_rejected_as_a_whole() {
        let duplicate = "07".repeat(32);
        let body = serde_json::json!({
            "schema_version": 1,
            "registry_id": "public",
            "revision": 1,
            "credentials": [
                {"credential_sha256": duplicate, "principal_id": "a", "enabled": true, "namespaces": ["a"], "actions": ["render.static"], "allow_missing_origin": true},
                {"credential_sha256": duplicate, "principal_id": "b", "enabled": true, "namespaces": ["b"], "actions": ["render.static"], "allow_missing_origin": true}
            ]
        });
        assert!(RegistrySnapshot::parse("public", body.to_string().as_bytes()).is_err());
    }

    #[test]
    fn registry_load_normalizes_namespace_grants_once() {
        let body = serde_json::json!({
            "schema_version": 1,
            "registry_id": "public",
            "revision": 1,
            "credentials": [{
                "credential_sha256": credential_sha256("public", "reader"),
                "principal_id": "reader",
                "enabled": true,
                "namespaces": ["terrain", "*", "basemap", "basemap"],
                "actions": ["read"],
                "allow_missing_origin": true
            }]
        });
        let snapshot = RegistrySnapshot::parse("public", body.to_string().as_bytes()).unwrap();
        let grant = snapshot
            .credentials
            .values()
            .next()
            .expect("the snapshot contains the credential");

        assert_eq!(grant.authorization.namespaces.as_ref(), &["*".to_string()]);
    }

    #[test]
    fn duplicate_origin_headers_are_rejected() {
        let mut headers = HeaderMap::new();
        let name = HeaderName::from_static("origin");
        headers.append(name.clone(), HeaderValue::from_static("https://a.example"));
        headers.append(name.clone(), HeaderValue::from_static("https://b.example"));

        assert!(matches!(
            single_header(&headers, name),
            Err(AuthFailure::Forbidden)
        ));
    }
}
