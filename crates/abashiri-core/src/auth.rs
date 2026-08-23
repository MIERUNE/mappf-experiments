//! Object-storage-backed authentication for the Abashiri management plane.
//!
//! This registry is intentionally distinct from `mmpf-auth`: a management
//! credential authorizes account-scoped publishing actions and is never a
//! Biei or Ishikari delivery credential. Requests authenticate from one
//! validated in-memory snapshot; object storage is consulted only on cold load
//! or after the bounded refresh interval.

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context as _, bail, ensure};
use http::{HeaderMap, header};
use object_store::{ObjectStore, ObjectStoreExt as _, parse_url_opts, path::Path as ObjectPath};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::{
    sync::{Mutex, RwLock},
    time::{Instant, timeout},
};
use url::Url;

use crate::mutation::{AccountId, Actor, ActorKind, digest_hex};

const CURRENT_OBJECT: &str = "current.json";
const SCHEMA_VERSION: u32 = 1;
const MAX_SNAPSHOT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_CREDENTIALS: usize = 10_000;
const MAX_ACCOUNTS_PER_CREDENTIAL: usize = 256;
const MIN_CREDENTIAL_BYTES: usize = 32;
const MAX_CREDENTIAL_BYTES: usize = 4_096;
const REFRESH_INTERVAL: Duration = Duration::from_mins(1);
const REFRESH_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
const CREDENTIAL_DOMAIN: &[u8] = b"abashiri-object-store-credential-v1\0";

/// Management action granted by an Abashiri authentication registry.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ManagementAction {
    #[serde(rename = "operations.read")]
    OperationsRead,
    #[serde(rename = "style.read")]
    StyleRead,
    #[serde(rename = "style.publish")]
    StylePublish,
}

impl ManagementAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OperationsRead => "operations.read",
            Self::StyleRead => "style.read",
            Self::StylePublish => "style.publish",
        }
    }
}

/// One authenticated management principal and its complete grant snapshot.
#[derive(Clone, Debug)]
pub struct AuthenticatedManagement {
    actor: Actor,
    accounts: Arc<[AccountId]>,
    actions: Arc<[ManagementAction]>,
    registry_revision: u64,
}

impl AuthenticatedManagement {
    pub fn actor(&self) -> &Actor {
        &self.actor
    }

    pub fn accounts(&self) -> &[AccountId] {
        &self.accounts
    }

    pub fn actions(&self) -> &[ManagementAction] {
        &self.actions
    }

    pub fn registry_revision(&self) -> u64 {
        self.registry_revision
    }

    pub fn authorize(
        &self,
        account: &AccountId,
        action: ManagementAction,
    ) -> Result<(), ManagementAuthFailure> {
        let account_allowed = self
            .accounts
            .binary_search_by(|candidate| candidate.as_str().cmp(account.as_str()))
            .is_ok();
        if account_allowed && self.authorize_action(action).is_ok() {
            Ok(())
        } else {
            Err(ManagementAuthFailure::Forbidden)
        }
    }

    /// Authorizes an action whose resource is not account-scoped.
    pub fn authorize_action(&self, action: ManagementAction) -> Result<(), ManagementAuthFailure> {
        if self.actions.binary_search(&action).is_ok() {
            Ok(())
        } else {
            Err(ManagementAuthFailure::Forbidden)
        }
    }
}

/// Bounded public failure categories suitable for HTTP mapping.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ManagementAuthFailure {
    #[error("invalid management credential")]
    InvalidCredential,
    #[error("management credential is not authorized")]
    Forbidden,
    #[error("management authentication is unavailable")]
    Unavailable,
}

/// Object-store registry reader and in-process verifier.
#[derive(Clone)]
pub struct ObjectStoreManagementAuth {
    inner: Arc<AuthInner>,
}

struct AuthInner {
    store: Arc<dyn ObjectStore>,
    current: ObjectPath,
    cached: RwLock<Option<CachedSnapshot>>,
    refresh_retry_after: RwLock<Option<Instant>>,
    refresh: Mutex<()>,
}

struct CachedSnapshot {
    snapshot: Arc<RegistrySnapshot>,
    body_sha256: [u8; 32],
    refresh_after: Instant,
}

impl ObjectStoreManagementAuth {
    pub fn from_url<I, K, V>(root: &Url, options: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        validate_registry_root(root)?;
        let current_url = root
            .join(CURRENT_OBJECT)
            .context("resolve management auth current.json")?;
        let (store, current) =
            parse_url_opts(&current_url, options).context("configure management auth store")?;
        Ok(Self::from_object_store(store.into(), current))
    }

    pub fn from_object_store(store: Arc<dyn ObjectStore>, current: ObjectPath) -> Self {
        Self {
            inner: Arc::new(AuthInner {
                store,
                current,
                cached: RwLock::new(None),
                refresh_retry_after: RwLock::new(None),
                refresh: Mutex::new(()),
            }),
        }
    }

    /// Loads and validates the initial registry before the server becomes ready.
    pub async fn prime(&self) -> anyhow::Result<()> {
        self.snapshot().await.map(|_| ())
    }

    /// Verifies exactly one `Authorization: Bearer` management credential.
    pub async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedManagement, ManagementAuthFailure> {
        let credential = bearer_credential(headers)?;
        let digest =
            credential_digest(credential).map_err(|_| ManagementAuthFailure::InvalidCredential)?;
        let snapshot = self
            .snapshot()
            .await
            .map_err(|_| ManagementAuthFailure::Unavailable)?;
        let grant = snapshot
            .credentials
            .get(&digest)
            .filter(|grant| grant.enabled)
            .ok_or(ManagementAuthFailure::InvalidCredential)?;
        Ok(AuthenticatedManagement {
            actor: grant.actor.clone(),
            accounts: Arc::clone(&grant.accounts),
            actions: Arc::clone(&grant.actions),
            registry_revision: snapshot.revision,
        })
    }

    async fn snapshot(&self) -> anyhow::Result<Arc<RegistrySnapshot>> {
        let now = Instant::now();
        if let Some(snapshot) = fresh_snapshot(&self.inner.cached, now).await {
            return Ok(snapshot);
        }
        ensure!(
            !refresh_is_deferred(&self.inner.refresh_retry_after, now).await,
            "management auth registry refresh is cooling down"
        );

        let _refresh = self.inner.refresh.lock().await;
        let now = Instant::now();
        if let Some(snapshot) = fresh_snapshot(&self.inner.cached, now).await {
            return Ok(snapshot);
        }
        ensure!(
            !refresh_is_deferred(&self.inner.refresh_retry_after, now).await,
            "management auth registry refresh is cooling down"
        );

        let (candidate, body_sha256) = match self.load_candidate().await {
            Ok(candidate) => candidate,
            Err(error) => {
                *self.inner.refresh_retry_after.write().await =
                    Some(Instant::now() + REFRESH_FAILURE_COOLDOWN);
                return Err(error);
            }
        };

        let mut cached = self.inner.cached.write().await;
        if let Some(previous) = cached.as_ref() {
            let valid_successor = if candidate.revision < previous.snapshot.revision {
                Err(anyhow::anyhow!(
                    "management auth registry revision rolled back"
                ))
            } else if candidate.revision == previous.snapshot.revision
                && body_sha256 != previous.body_sha256
            {
                Err(anyhow::anyhow!(
                    "management auth registry changed without a revision increase"
                ))
            } else {
                Ok(())
            };
            if let Err(error) = valid_successor {
                drop(cached);
                *self.inner.refresh_retry_after.write().await =
                    Some(Instant::now() + REFRESH_FAILURE_COOLDOWN);
                return Err(error);
            }
        }
        *cached = Some(CachedSnapshot {
            snapshot: Arc::clone(&candidate),
            body_sha256,
            refresh_after: Instant::now() + REFRESH_INTERVAL,
        });
        *self.inner.refresh_retry_after.write().await = None;
        Ok(candidate)
    }

    async fn load_candidate(&self) -> anyhow::Result<(Arc<RegistrySnapshot>, [u8; 32])> {
        let result = timeout(
            crate::store_policy::OPERATION_TIMEOUT,
            self.inner.store.get(&self.inner.current),
        )
        .await
        .context("management auth registry read timed out")?
        .context("read management auth registry")?;
        ensure!(
            result.meta.size <= MAX_SNAPSHOT_BYTES,
            "management auth registry exceeds {MAX_SNAPSHOT_BYTES} bytes"
        );
        let body = timeout(crate::store_policy::OPERATION_TIMEOUT, result.bytes())
            .await
            .context("management auth registry body timed out")?
            .context("collect management auth registry")?;
        ensure!(
            body.len() as u64 <= MAX_SNAPSHOT_BYTES,
            "management auth registry exceeds {MAX_SNAPSHOT_BYTES} bytes"
        );
        let candidate = Arc::new(RegistrySnapshot::parse(&body)?);
        let body_sha256: [u8; 32] = Sha256::digest(&body).into();
        Ok((candidate, body_sha256))
    }
}

async fn fresh_snapshot(
    cached: &RwLock<Option<CachedSnapshot>>,
    now: Instant,
) -> Option<Arc<RegistrySnapshot>> {
    let cached = cached.read().await;
    cached
        .as_ref()
        .filter(|cached| now < cached.refresh_after)
        .map(|cached| Arc::clone(&cached.snapshot))
}

async fn refresh_is_deferred(retry_after: &RwLock<Option<Instant>>, now: Instant) -> bool {
    retry_after
        .read()
        .await
        .is_some_and(|retry_after| now < retry_after)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistrySnapshotWire {
    schema_version: u32,
    revision: u64,
    credentials: Vec<CredentialGrantWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialGrantWire {
    credential_sha256: String,
    enabled: bool,
    actor: ActorWire,
    accounts: Vec<String>,
    actions: Vec<ManagementAction>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActorWire {
    kind: ActorKind,
    issuer: String,
    subject: String,
}

struct RegistrySnapshot {
    revision: u64,
    credentials: HashMap<[u8; 32], CredentialGrant>,
}

struct CredentialGrant {
    enabled: bool,
    actor: Actor,
    accounts: Arc<[AccountId]>,
    actions: Arc<[ManagementAction]>,
}

impl RegistrySnapshot {
    fn parse(body: &[u8]) -> anyhow::Result<Self> {
        let wire: RegistrySnapshotWire =
            serde_json::from_slice(body).context("parse management auth registry JSON")?;
        ensure!(
            wire.schema_version == SCHEMA_VERSION,
            "unsupported management auth registry schema_version"
        );
        ensure!(
            wire.revision > 0,
            "management auth revision must be positive"
        );
        ensure!(
            wire.credentials.len() <= MAX_CREDENTIALS,
            "management auth registry has too many credentials"
        );

        let mut credentials = HashMap::with_capacity(wire.credentials.len());
        for grant in wire.credentials {
            let digest = decode_sha256(&grant.credential_sha256)?;
            ensure!(
                !credentials.contains_key(&digest),
                "management auth registry contains a duplicate credential digest"
            );
            ensure!(
                !grant.actions.is_empty(),
                "management credential actions must not be empty"
            );
            ensure!(
                grant.accounts.len() <= MAX_ACCOUNTS_PER_CREDENTIAL,
                "management credential accounts must be bounded"
            );
            let requires_account = grant
                .actions
                .iter()
                .any(|action| !matches!(action, ManagementAction::OperationsRead));
            ensure!(
                !requires_account || !grant.accounts.is_empty(),
                "account-scoped management actions require at least one account"
            );
            ensure!(
                grant.actor.kind == ActorKind::Workload,
                "object-store management credentials must represent workloads"
            );

            let actor = Actor::try_new(grant.actor.kind, grant.actor.issuer, grant.actor.subject)
                .context("validate management actor")?;
            let mut accounts = grant
                .accounts
                .into_iter()
                .map(AccountId::try_new)
                .collect::<Result<Vec<_>, _>>()
                .context("validate management account grant")?;
            accounts.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
            accounts.dedup();
            let mut actions = grant.actions;
            actions.sort_unstable();
            actions.dedup();
            credentials.insert(
                digest,
                CredentialGrant {
                    enabled: grant.enabled,
                    actor,
                    accounts: accounts.into(),
                    actions: actions.into(),
                },
            );
        }
        Ok(Self {
            revision: wire.revision,
            credentials,
        })
    }
}

/// Produces the digest stored in a management authentication registry.
pub fn credential_sha256(credential: &str) -> anyhow::Result<String> {
    validate_credential(credential)?;
    Ok(digest_hex(CREDENTIAL_DOMAIN, credential.as_bytes()))
}

fn credential_digest(credential: &str) -> anyhow::Result<[u8; 32]> {
    validate_credential(credential)?;
    let mut hasher = Sha256::new();
    hasher.update(CREDENTIAL_DOMAIN);
    hasher.update(credential.as_bytes());
    Ok(hasher.finalize().into())
}

fn validate_credential(credential: &str) -> anyhow::Result<()> {
    ensure!(
        (MIN_CREDENTIAL_BYTES..=MAX_CREDENTIAL_BYTES).contains(&credential.len()),
        "management credential must contain {MIN_CREDENTIAL_BYTES}..={MAX_CREDENTIAL_BYTES} bytes"
    );
    ensure!(
        credential.bytes().all(|byte| (0x21..=0x7e).contains(&byte)),
        "management credential must contain visible ASCII without spaces"
    );
    Ok(())
}

fn bearer_credential(headers: &HeaderMap) -> Result<&str, ManagementAuthFailure> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values
        .next()
        .ok_or(ManagementAuthFailure::InvalidCredential)?;
    if values.next().is_some() {
        return Err(ManagementAuthFailure::InvalidCredential);
    }
    let value = value
        .to_str()
        .map_err(|_| ManagementAuthFailure::InvalidCredential)?;
    let (scheme, credential) = value
        .split_once(' ')
        .ok_or(ManagementAuthFailure::InvalidCredential)?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || credential.contains(char::is_whitespace)
        || validate_credential(credential).is_err()
    {
        return Err(ManagementAuthFailure::InvalidCredential);
    }
    Ok(credential)
}

fn validate_registry_root(root: &Url) -> anyhow::Result<()> {
    if !matches!(root.scheme(), "file" | "memory" | "gs" | "s3") {
        bail!("management auth root must use file, memory, gs, or s3");
    }
    ensure!(
        !root.cannot_be_a_base() && root.path().ends_with('/'),
        "management auth root must be a directory URL ending in `/`"
    );
    crate::store_policy::ensure_location_only(root, "management auth root")?;
    Ok(())
}

fn decode_sha256(value: &str) -> anyhow::Result<[u8; 32]> {
    ensure!(
        value.len() == 64,
        "management credential digest must contain 64 hexadecimal characters"
    );
    let mut digest = [0_u8; 32];
    for (index, output) in digest.iter_mut().enumerate() {
        let offset = index * 2;
        *output = u8::from_str_radix(&value[offset..offset + 2], 16)
            .context("decode management credential digest")?;
    }
    Ok(digest)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use serde_json::json;

    use super::*;

    const TOKEN: &str = "test-management-token-with-32-bytes";

    async fn configured_auth(enabled: bool) -> (ObjectStoreManagementAuth, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        let body = json!({
            "schema_version": 1,
            "revision": 1,
            "credentials": [{
                "credential_sha256": credential_sha256(TOKEN).unwrap(),
                "enabled": enabled,
                "actor": {
                    "kind": "workload",
                    "issuer": "test",
                    "subject": "publisher"
                },
                "accounts": ["beta", "alpha", "alpha"],
                "actions": ["style.publish"]
            }]
        });
        store
            .put(
                &ObjectPath::from("auth/current.json"),
                Bytes::from(serde_json::to_vec(&body).unwrap()).into(),
            )
            .await
            .unwrap();
        let auth = ObjectStoreManagementAuth::from_object_store(
            store.clone(),
            ObjectPath::from("auth/current.json"),
        );
        (auth, store)
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        headers
    }

    #[tokio::test]
    async fn authenticates_and_authorizes_from_one_cached_snapshot() {
        let (auth, store) = configured_auth(true).await;
        let principal = auth.authenticate(&bearer(TOKEN)).await.unwrap();
        assert_eq!(principal.actor().kind(), ActorKind::Workload);
        assert_eq!(principal.actor().subject(), "publisher");
        assert_eq!(
            principal
                .accounts()
                .iter()
                .map(AccountId::as_str)
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        principal
            .authorize(
                &AccountId::try_new("alpha").unwrap(),
                ManagementAction::StylePublish,
            )
            .unwrap();
        assert_eq!(
            principal.authorize_action(ManagementAction::OperationsRead),
            Err(ManagementAuthFailure::Forbidden)
        );
        assert_eq!(
            principal.authorize(
                &AccountId::try_new("other").unwrap(),
                ManagementAction::StylePublish,
            ),
            Err(ManagementAuthFailure::Forbidden)
        );

        store
            .delete(&ObjectPath::from("auth/current.json"))
            .await
            .unwrap();
        assert!(auth.authenticate(&bearer(TOKEN)).await.is_ok());
    }

    #[tokio::test]
    async fn expired_snapshot_is_not_used_when_refresh_fails() {
        let (auth, store) = configured_auth(true).await;
        auth.prime().await.unwrap();
        {
            let mut cached = auth.inner.cached.write().await;
            cached.as_mut().unwrap().refresh_after = Instant::now();
        }
        store
            .delete(&ObjectPath::from("auth/current.json"))
            .await
            .unwrap();

        assert!(matches!(
            auth.authenticate(&bearer(TOKEN)).await,
            Err(ManagementAuthFailure::Unavailable)
        ));
    }

    #[tokio::test]
    async fn missing_invalid_and_disabled_credentials_fail_closed() {
        let (auth, _) = configured_auth(true).await;
        assert!(matches!(
            auth.authenticate(&HeaderMap::new()).await,
            Err(ManagementAuthFailure::InvalidCredential)
        ));
        assert!(matches!(
            auth.authenticate(&bearer("wrong-management-token-with-32-bytes"))
                .await,
            Err(ManagementAuthFailure::InvalidCredential)
        ));

        let (disabled, _) = configured_auth(false).await;
        assert!(matches!(
            disabled.authenticate(&bearer(TOKEN)).await,
            Err(ManagementAuthFailure::InvalidCredential)
        ));
    }

    #[tokio::test]
    async fn registry_rejects_changed_content_without_revision_increase() {
        let (auth, store) = configured_auth(true).await;
        auth.prime().await.unwrap();
        {
            let mut cached = auth.inner.cached.write().await;
            cached.as_mut().unwrap().refresh_after = Instant::now();
        }
        let changed = json!({
            "schema_version": 1,
            "revision": 1,
            "credentials": []
        });
        store
            .put(
                &ObjectPath::from("auth/current.json"),
                Bytes::from(serde_json::to_vec(&changed).unwrap()).into(),
            )
            .await
            .unwrap();
        assert!(
            format!("{:#}", auth.prime().await.unwrap_err())
                .contains("changed without a revision increase")
        );
        assert!(
            auth.inner
                .refresh_retry_after
                .read()
                .await
                .is_some_and(|retry_after| retry_after > Instant::now())
        );
    }

    #[test]
    fn object_store_credentials_cannot_impersonate_human_actors() {
        let body = json!({
            "schema_version": 1,
            "revision": 1,
            "credentials": [{
                "credential_sha256": credential_sha256(TOKEN).unwrap(),
                "enabled": true,
                "actor": {
                    "kind": "human",
                    "issuer": "test",
                    "subject": "person"
                },
                "accounts": ["example"],
                "actions": ["style.publish"]
            }]
        });
        let error = RegistrySnapshot::parse(&serde_json::to_vec(&body).unwrap())
            .err()
            .unwrap();
        assert!(error.to_string().contains("must represent workloads"));
    }

    #[test]
    fn global_operations_action_does_not_require_a_fake_account() {
        let body = json!({
            "schema_version": 1,
            "revision": 1,
            "credentials": [{
                "credential_sha256": credential_sha256(TOKEN).unwrap(),
                "enabled": true,
                "actor": {
                    "kind": "workload",
                    "issuer": "test",
                    "subject": "observer"
                },
                "accounts": [],
                "actions": ["operations.read"]
            }]
        });
        assert!(RegistrySnapshot::parse(&serde_json::to_vec(&body).unwrap()).is_ok());

        let mut account_scoped = body;
        account_scoped["credentials"][0]["actions"] = json!(["style.read"]);
        assert!(
            RegistrySnapshot::parse(&serde_json::to_vec(&account_scoped).unwrap())
                .err()
                .unwrap()
                .to_string()
                .contains("require at least one account")
        );
    }

    #[test]
    fn credential_digest_is_domain_separated_and_raw_secret_is_absent() {
        let digest = credential_sha256(TOKEN).unwrap();
        assert_eq!(digest.len(), 64);
        assert!(!digest.contains(TOKEN));
        assert_ne!(digest, digest_hex(b"", TOKEN.as_bytes()));
    }
}
