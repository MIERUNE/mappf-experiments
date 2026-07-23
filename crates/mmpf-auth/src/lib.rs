//! Optional delivery-plane authentication shared by MMPF delivery servers.
//!
//! The public token envelope deliberately owns only registry selection. The
//! suffix is opaque here: each registry adapter decides whether it represents
//! a random secret, a JWT, or another credential format.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, bail};
use http::HeaderMap;
use mmpf_common::singleflight::{Flight, SingleFlight};
use mmpf_common::sync::lock_unpoisoned;
use moka::sync::Cache;
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectStoreError, GetOptions, ObjectStore, parse_url_opts};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use url::Url;

const CURRENT_OBJECT: &str = "current.json";
const MAX_REGISTRIES: usize = 128;
const MAX_REGISTRY_ID_BYTES: usize = 64;
const MAX_CREDENTIAL_BYTES: usize = 4096;
const MAX_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024;
const AUTH_CACHE_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CREDENTIALS_PER_REGISTRY: usize = 100_000;
const MAX_CONCURRENT_REGISTRY_LOADS: usize = 8;
const MAX_PRINCIPAL_ID_BYTES: usize = 256;
const MAX_NAMESPACES_PER_CREDENTIAL: usize = 1024;
const MAX_ORIGINS_PER_CREDENTIAL: usize = 128;
const REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const REFRESH_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
const OBJECT_STORE_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const DIGEST_DOMAIN: &[u8] = b"mmpf-object-store-auth-v1\0";
const CACHE_PARTITION_DOMAIN: &[u8] = b"mmpf-delivery-cache-partition-v1\0";

#[derive(Clone, PartialEq, Eq)]
pub struct RegistryCatalog {
    entries: Arc<BTreeMap<String, RegistryConfig>>,
}

#[derive(Clone, PartialEq, Eq)]
struct RegistryConfig {
    current_url: Url,
}

impl RegistryCatalog {
    pub fn empty() -> Self {
        Self {
            entries: Arc::new(BTreeMap::new()),
        }
    }

    /// Parses `registry_id=auth-root;...`. An empty string disables auth.
    pub fn parse(spec: &str) -> anyhow::Result<Self> {
        if spec.trim().is_empty() {
            return Ok(Self::empty());
        }

        let mut entries = BTreeMap::new();
        for raw_entry in spec.split(';') {
            let (raw_id, raw_root) = raw_entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("auth registry entry must be registry_id=URL"))?;
            let registry_id = raw_id.trim();
            validate_registry_id(registry_id)?;
            let auth_root = parse_auth_root(raw_root.trim())?;
            let current_url = auth_root
                .join(CURRENT_OBJECT)
                .context("resolve auth registry current.json")?;
            if entries
                .insert(registry_id.to_string(), RegistryConfig { current_url })
                .is_some()
            {
                bail!("duplicate auth registry id {registry_id:?}");
            }
            if entries.len() > MAX_REGISTRIES {
                bail!("too many auth registries; maximum is {MAX_REGISTRIES}");
            }
        }
        Ok(Self {
            entries: Arc::new(entries),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn get(&self, registry_id: &str) -> Option<&RegistryConfig> {
        self.entries.get(registry_id)
    }
}

impl fmt::Debug for RegistryCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistryCatalog")
            .field("registry_ids", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn parse_auth_root(raw: &str) -> anyhow::Result<Url> {
    let url = Url::parse(raw).context("parse auth registry URL")?;
    if !matches!(
        url.scheme(),
        "file" | "memory" | "gs" | "s3" | "http" | "https"
    ) {
        bail!(
            "auth registry URL scheme {:?} is not supported",
            url.scheme()
        );
    }
    if url.cannot_be_a_base() || !url.path().ends_with('/') {
        bail!("auth registry URL must be an absolute directory URL ending in `/`");
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("auth registry URL must not contain credentials, query, or fragment");
    }
    Ok(url)
}

fn validate_registry_id(registry_id: &str) -> anyhow::Result<()> {
    if registry_id.is_empty() || registry_id.len() > MAX_REGISTRY_ID_BYTES {
        bail!("auth registry id must contain 1..={MAX_REGISTRY_ID_BYTES} bytes");
    }
    if !registry_id.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
    }) {
        bail!("auth registry id must use lowercase ASCII letters, digits, `-`, or `_`");
    }
    Ok(())
}

#[derive(Clone)]
pub struct DeliveryAuth {
    inner: Arc<DeliveryAuthInner>,
}

struct DeliveryAuthInner {
    catalog: RegistryCatalog,
    stores: ObjectStores,
    cache: Cache<String, CachedRegistry>,
    cold_retry_after: Mutex<HashMap<String, Instant>>,
    installed_revisions: Mutex<HashMap<String, InstalledRevision>>,
    refreshes: SingleFlight<String, AuthUnavailable>,
    refresh_permits: Arc<Semaphore>,
}

#[derive(Clone)]
struct CachedRegistry {
    snapshot: Arc<RegistrySnapshot>,
    etag: Option<String>,
    refresh_after: Instant,
    source_bytes: u32,
}

#[derive(Clone, Copy)]
struct InstalledRevision {
    revision: u64,
    body_sha256: [u8; 32],
}

impl DeliveryAuth {
    pub fn new<I, K, V>(catalog: RegistryCatalog, object_store_options: I) -> Option<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        (!catalog.is_empty()).then(|| Self {
            inner: Arc::new(DeliveryAuthInner {
                catalog,
                stores: ObjectStores::new(object_store_options),
                cache: Cache::builder()
                    .max_capacity(AUTH_CACHE_CAPACITY_BYTES)
                    .weigher(|_registry_id: &String, cached: &CachedRegistry| cached.source_bytes)
                    .build(),
                cold_retry_after: Mutex::new(HashMap::new()),
                installed_revisions: Mutex::new(HashMap::new()),
                refreshes: SingleFlight::default(),
                refresh_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_REGISTRY_LOADS)),
            }),
        })
    }

    pub async fn authorize_static(
        &self,
        headers: &HeaderMap,
        query: Option<&str>,
        namespace: &str,
    ) -> Result<AuthorizedDelivery, AuthFailure> {
        self.authorize(
            headers,
            query,
            Some(namespace),
            DeliveryAction::RenderStatic,
        )
        .await
    }

    /// Authenticates one delivery request against a configured registry.
    ///
    /// `namespace = None` is reserved for globally shared resources such as
    /// glyph ranges. Such resources still require the requested action, but do
    /// not pretend to belong to the style that happened to reference them.
    pub async fn authorize(
        &self,
        headers: &HeaderMap,
        query: Option<&str>,
        namespace: Option<&str>,
        action: DeliveryAction,
    ) -> Result<AuthorizedDelivery, AuthFailure> {
        let presented = delivery_token(headers, query)?;
        let propagate_access_token = presented.from_query;
        let (registry_id, credential) = parse_token_envelope(presented.value.as_ref())?;
        let Some(config) = self.inner.catalog.get(registry_id) else {
            // Registry selection is bounded local configuration. Unknown IDs
            // must never turn into attacker-selected object-store reads.
            return Err(AuthFailure::InvalidCredential);
        };
        let snapshot = self
            .snapshot(registry_id, config)
            .await
            .map_err(|_| AuthFailure::Unavailable)?;
        let digest = credential_digest(registry_id, credential);
        let Some(grant) = snapshot
            .credentials
            .get(&digest)
            .filter(|grant| grant.enabled && constant_time_eq(&grant.credential_sha256, &digest))
        else {
            return Err(AuthFailure::InvalidCredential);
        };

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
        authorize_origin(headers, grant)?;
        Ok(AuthorizedDelivery {
            principal_id: grant.principal_id.clone(),
            registry_id: registry_id.to_string(),
            readable_namespaces: Arc::clone(&grant.namespaces),
            cache_partition: credential_cache_partition(registry_id, credential, snapshot.revision),
            presented_token: Arc::from(presented.value.as_ref()),
            propagate_access_token,
        })
    }

    async fn snapshot(
        &self,
        registry_id: &str,
        config: &RegistryConfig,
    ) -> Result<Arc<RegistrySnapshot>, AuthUnavailable> {
        loop {
            let now = Instant::now();
            if let Some(cached) = self.inner.cache.get(registry_id)
                && cached.refresh_after > now
            {
                return Ok(cached.snapshot);
            }
            if !self.inner.cache.contains_key(registry_id)
                && lock_unpoisoned(&self.inner.cold_retry_after)
                    .get(registry_id)
                    .is_some_and(|retry_after| *retry_after > now)
            {
                return Err(AuthUnavailable);
            }

            match self.inner.refreshes.begin(registry_id.to_string()) {
                Flight::Leader(leader) => match self.refresh(registry_id, config).await {
                    Ok(snapshot) => return Ok(snapshot),
                    Err(error) => {
                        if let Some(stale) = self.defer_failed_refresh(registry_id) {
                            tracing::warn!(
                                registry_id,
                                "auth registry refresh failed; using last known good snapshot"
                            );
                            return Ok(stale);
                        }
                        leader.complete_with_error(error.clone());
                        return Err(error);
                    }
                },
                Flight::Follower(follower) => {
                    if let Some(error) = follower.wait().await {
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn refresh(
        &self,
        registry_id: &str,
        config: &RegistryConfig,
    ) -> Result<Arc<RegistrySnapshot>, AuthUnavailable> {
        let _permit = self
            .inner
            .refresh_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| AuthUnavailable)?;
        let previous = self.inner.cache.get(registry_id);
        let (store, path) = self
            .inner
            .stores
            .resolve(&config.current_url)
            .map_err(|_| AuthUnavailable)?;
        let mut options = GetOptions::new();
        if let Some(etag) = previous.as_ref().and_then(|cached| cached.etag.as_ref()) {
            options = options.with_if_none_match(Some(etag));
        }
        let result = match tokio::time::timeout(
            OBJECT_STORE_OPERATION_TIMEOUT,
            store.get_opts(&path, options),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(ObjectStoreError::NotModified { .. })) if previous.is_some() => {
                let mut cached = previous.ok_or(AuthUnavailable)?;
                cached.refresh_after = Instant::now() + REFRESH_INTERVAL;
                let snapshot = cached.snapshot.clone();
                self.inner.cache.insert(registry_id.to_string(), cached);
                return Ok(snapshot);
            }
            Ok(Err(_)) | Err(_) => return Err(AuthUnavailable),
        };
        if result.meta.size > MAX_SNAPSHOT_BYTES {
            return Err(AuthUnavailable);
        }
        let etag = result.meta.e_tag.clone();
        let body = tokio::time::timeout(OBJECT_STORE_OPERATION_TIMEOUT, result.bytes())
            .await
            .map_err(|_| AuthUnavailable)?
            .map_err(|_| AuthUnavailable)?;
        if body.len() as u64 > MAX_SNAPSHOT_BYTES {
            return Err(AuthUnavailable);
        }
        let snapshot = RegistrySnapshot::parse(registry_id, &body).map_err(|_error| {
            // Parser diagnostics can contain attacker-controlled registry
            // strings. Keep the operational event bounded and secret-free.
            tracing::warn!(registry_id, "rejected invalid auth registry snapshot");
            AuthUnavailable
        })?;
        let body_sha256: [u8; 32] = Sha256::digest(&body).into();
        {
            let installed = lock_unpoisoned(&self.inner.installed_revisions);
            if let Some(previous) = installed.get(registry_id)
                && snapshot.revision < previous.revision
            {
                tracing::warn!(
                    registry_id,
                    previous_revision = previous.revision,
                    candidate_revision = snapshot.revision,
                    "rejected auth registry revision rollback"
                );
                return Err(AuthUnavailable);
            }
            if let Some(previous) = installed.get(registry_id)
                && snapshot.revision == previous.revision
                && body_sha256 != previous.body_sha256
            {
                tracing::warn!(
                    registry_id,
                    revision = snapshot.revision,
                    "rejected changed auth snapshot without a revision increase"
                );
                return Err(AuthUnavailable);
            }
        }

        let snapshot = Arc::new(snapshot);
        lock_unpoisoned(&self.inner.installed_revisions).insert(
            registry_id.to_string(),
            InstalledRevision {
                revision: snapshot.revision,
                body_sha256,
            },
        );
        lock_unpoisoned(&self.inner.cold_retry_after).remove(registry_id);
        self.inner.cache.insert(
            registry_id.to_string(),
            CachedRegistry {
                snapshot: snapshot.clone(),
                etag,
                refresh_after: Instant::now() + REFRESH_INTERVAL,
                source_bytes: body.len() as u32,
            },
        );
        Ok(snapshot)
    }

    fn defer_failed_refresh(&self, registry_id: &str) -> Option<Arc<RegistrySnapshot>> {
        if let Some(mut cached) = self.inner.cache.get(registry_id) {
            cached.refresh_after = Instant::now() + REFRESH_FAILURE_COOLDOWN;
            let snapshot = cached.snapshot.clone();
            self.inner.cache.insert(registry_id.to_string(), cached);
            return Some(snapshot);
        }
        lock_unpoisoned(&self.inner.cold_retry_after).insert(
            registry_id.to_string(),
            Instant::now() + REFRESH_FAILURE_COOLDOWN,
        );
        None
    }
}

pub struct AuthorizedDelivery {
    pub principal_id: String,
    pub registry_id: String,
    readable_namespaces: Arc<[String]>,
    cache_partition: [u8; 32],
    presented_token: Arc<str>,
    propagate_access_token: bool,
}

impl AuthorizedDelivery {
    /// Returns the normalized namespace grant set captured from the same
    /// registry revision that authenticated this request.
    pub fn readable_namespaces(&self) -> &[String] {
        &self.readable_namespaces
    }

    /// Shares the immutable normalized grant set without cloning every label.
    pub fn shared_readable_namespaces(&self) -> Arc<[String]> {
        Arc::clone(&self.readable_namespaces)
    }

    /// Returns a domain-separated, one-way partition for credential-sensitive
    /// in-process caches. It is stable across nodes at one registry revision,
    /// changes on policy revision, and is neither the raw credential nor the
    /// verifier digest stored in the registry.
    pub fn cache_partition(&self) -> [u8; 32] {
        self.cache_partition
    }

    /// Returns the verified credential exactly as presented at the delivery
    /// boundary. A backend may forward it only to an explicitly configured
    /// trusted provider; it must never be logged or used as a cache key.
    pub fn backend_bearer_token(&self) -> &str {
        &self.presented_token
    }

    /// Returns the verified query credential when the caller used
    /// `access_token`. Header credentials are never copied into generated URLs.
    pub fn propagated_access_token(&self) -> Option<&str> {
        self.propagate_access_token
            .then_some(self.presented_token.as_ref())
    }

    /// Shares the verified query credential without copying its secret bytes.
    /// Header credentials are never converted into a URL credential.
    pub fn shared_propagated_access_token(&self) -> Option<Arc<str>> {
        self.propagate_access_token
            .then(|| Arc::clone(&self.presented_token))
    }
}

#[derive(Clone, Debug)]
struct AuthUnavailable;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthFailure {
    InvalidCredential,
    Forbidden,
    Unavailable,
}

fn delivery_token<'a>(
    headers: &'a HeaderMap,
    query: Option<&'a str>,
) -> Result<PresentedCredential<'a>, AuthFailure> {
    let bearer = bearer_token(headers)?;
    let query = access_token_from_query(query)?;
    match (bearer, query) {
        (Some(_), Some(_)) => Err(AuthFailure::InvalidCredential),
        (Some(token), None) => Ok(PresentedCredential {
            value: Cow::Borrowed(token),
            from_query: false,
        }),
        (None, Some(token)) => Ok(PresentedCredential {
            value: token,
            from_query: true,
        }),
        (None, None) => Err(AuthFailure::InvalidCredential),
    }
}

struct PresentedCredential<'a> {
    value: Cow<'a, str>,
    from_query: bool,
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

fn parse_token_envelope(token: &str) -> Result<(&str, &str), AuthFailure> {
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
    credentials: Vec<CredentialGrantWire>,
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

struct RegistrySnapshot {
    revision: u64,
    credentials: HashMap<[u8; 32], CredentialGrant>,
}

struct CredentialGrant {
    credential_sha256: [u8; 32],
    principal_id: String,
    enabled: bool,
    namespaces: Arc<[String]>,
    actions: HashSet<DeliveryAction>,
    allowed_origins: Vec<String>,
    allow_missing_origin: bool,
}

impl RegistrySnapshot {
    fn parse(expected_registry_id: &str, body: &[u8]) -> anyhow::Result<Self> {
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
        let mut credentials = HashMap::with_capacity(wire.credentials.len());
        for mut grant in wire.credentials {
            let digest = decode_sha256(&grant.credential_sha256)?;
            if credentials.contains_key(&digest) {
                bail!("auth registry contains a duplicate credential digest");
            }
            validate_bounded_label("principal_id", &grant.principal_id, MAX_PRINCIPAL_ID_BYTES)?;
            if grant.namespaces.is_empty() || grant.namespaces.len() > MAX_NAMESPACES_PER_CREDENTIAL
            {
                bail!("credential namespaces must be non-empty and bounded");
            }
            for namespace in &grant.namespaces {
                if namespace != "*" {
                    validate_bounded_label("namespace", namespace, 256)?;
                }
            }
            grant.namespaces.sort_unstable();
            grant.namespaces.dedup();
            if grant
                .namespaces
                .binary_search_by(|value| value.as_str().cmp("*"))
                .is_ok()
            {
                grant.namespaces.clear();
                grant.namespaces.push("*".to_string());
            }
            let actions: HashSet<_> = grant.actions.into_iter().collect();
            if actions.is_empty() {
                bail!("credential actions must not be empty");
            }
            if grant.allowed_origins.len() > MAX_ORIGINS_PER_CREDENTIAL {
                bail!("credential has too many allowed origins");
            }
            let allowed_origins = grant
                .allowed_origins
                .iter()
                .map(|origin| normalize_declared_origin(origin))
                .collect::<anyhow::Result<Vec<_>>>()?;
            credentials.insert(
                digest,
                CredentialGrant {
                    credential_sha256: digest,
                    principal_id: grant.principal_id,
                    enabled: grant.enabled,
                    namespaces: grant.namespaces.into(),
                    actions,
                    allowed_origins,
                    allow_missing_origin: grant.allow_missing_origin,
                },
            );
        }
        Ok(Self {
            revision: wire.revision,
            credentials,
        })
    }
}

fn validate_bounded_label(name: &str, value: &str, max_bytes: usize) -> anyhow::Result<()> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        bail!("{name} must be non-empty, bounded, and contain no control characters");
    }
    Ok(())
}

fn authorize_origin(headers: &HeaderMap, grant: &CredentialGrant) -> Result<(), AuthFailure> {
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

fn credential_digest(registry_id: &str, credential: &str) -> [u8; 32] {
    namespaced_digest(DIGEST_DOMAIN, registry_id, credential)
}

fn credential_cache_partition(
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

fn decode_sha256(value: &str) -> anyhow::Result<[u8; 32]> {
    if value.len() != 64 {
        bail!("credential_sha256 must be 64 lowercase hexadecimal characters");
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
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

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
fn encode_sha256(digest: [u8; 32]) -> String {
    encode_sha256_bytes(digest)
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

struct ObjectStores {
    options: Arc<[(String, String)]>,
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl ObjectStores {
    fn new<I, K, V>(options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            options: options
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect::<Vec<_>>()
                .into(),
            stores: Mutex::new(HashMap::new()),
        }
    }

    fn resolve(&self, url: &Url) -> anyhow::Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let key = format!("{}://{}", url.scheme(), url.authority());
        let store = {
            let mut stores = lock_unpoisoned(&self.stores);
            if let Some(store) = stores.get(&key) {
                store.clone()
            } else {
                let allow_http = (url.scheme() == "http")
                    .then_some(("allow_http".to_string(), "true".to_string()));
                let options = self.options.iter().cloned().chain(allow_http);
                let (store, _) = parse_url_opts(url, options)
                    .map_err(|_| anyhow::anyhow!("failed to configure auth object store"))?;
                let store: Arc<dyn ObjectStore> = store.into();
                stores.insert(key, store.clone());
                store
            }
        };
        let path = ObjectPath::from_url_path(url.path())
            .map_err(|_| anyhow::anyhow!("invalid auth object path"))?;
        Ok((store, path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderName, HeaderValue};
    use object_store::{ObjectStoreExt, PutPayload};

    fn headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    fn snapshot_json(registry_id: &str, credential: &str) -> Vec<u8> {
        snapshot_json_at_revision(registry_id, credential, 1)
    }

    fn snapshot_json_at_revision(registry_id: &str, credential: &str, revision: u64) -> Vec<u8> {
        serde_json::json!({
            "schema_version": 1,
            "registry_id": registry_id,
            "revision": revision,
            "credentials": [{
                "credential_sha256": encode_sha256(credential_digest(registry_id, credential)),
                "principal_id": "demo-browser",
                "enabled": true,
                "namespaces": ["demo"],
                "actions": ["render.static"],
                "allowed_origins": ["https://maps.example"],
                "allow_missing_origin": false
            }]
        })
        .to_string()
        .into_bytes()
    }

    async fn configured_auth(registry_id: &str, credential: &str) -> DeliveryAuth {
        let catalog =
            RegistryCatalog::parse(&format!("{registry_id}=memory:///auth/{registry_id}/"))
                .unwrap();
        let auth = DeliveryAuth::new(catalog, std::iter::empty::<(String, String)>()).unwrap();
        put_snapshot(&auth, registry_id, snapshot_json(registry_id, credential)).await;
        auth
    }

    async fn put_snapshot(auth: &DeliveryAuth, registry_id: &str, body: Vec<u8>) {
        let config = auth.inner.catalog.get(registry_id).unwrap();
        let (store, path) = auth.inner.stores.resolve(&config.current_url).unwrap();
        store.put(&path, PutPayload::from(body)).await.unwrap();
    }

    #[test]
    fn token_suffix_is_opaque_and_split_only_once() {
        let (registry, credential) = parse_token_envelope("corp.aaa.bbb.ccc").unwrap();
        assert_eq!(registry, "corp");
        assert_eq!(credential, "aaa.bbb.ccc");
    }

    #[test]
    fn registry_catalog_rejects_ambiguous_or_secret_urls() {
        assert!(RegistryCatalog::parse("A=gs://bucket/auth/").is_err());
        assert!(RegistryCatalog::parse("a=gs://bucket/auth").is_err());
        assert!(RegistryCatalog::parse("a=https://user:secret@example/auth/").is_err());
        assert!(RegistryCatalog::parse("a=gs://bucket/a/;a=gs://bucket/b/").is_err());
    }

    #[tokio::test]
    async fn object_store_registry_authorizes_opaque_credentials() {
        let auth = configured_auth("public", "eyJhbGciOi.fake.jwt").await;
        let mut headers = headers("public.eyJhbGciOi.fake.jwt");
        headers.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://maps.example"),
        );

        let authorized = auth.authorize_static(&headers, None, "demo").await.unwrap();

        assert_eq!(authorized.registry_id, "public");
        assert_eq!(authorized.principal_id, "demo-browser");
        assert_eq!(authorized.readable_namespaces(), &["demo".to_string()]);
        assert_eq!(
            authorized.backend_bearer_token(),
            "public.eyJhbGciOi.fake.jwt"
        );
        assert_eq!(authorized.propagated_access_token(), None);
        let header_cache_partition = authorized.cache_partition();
        assert_ne!(
            header_cache_partition,
            credential_digest("public", "eyJhbGciOi.fake.jwt"),
            "cache partition must not expose the verifier digest stored in current.json"
        );

        let mut query_headers = HeaderMap::new();
        query_headers.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://maps.example"),
        );
        let authorized = auth
            .authorize_static(
                &query_headers,
                Some("access_token=public.eyJhbGciOi.fake.jwt"),
                "demo",
            )
            .await
            .expect("query token");
        assert_eq!(authorized.principal_id, "demo-browser");
        assert_eq!(
            authorized.backend_bearer_token(),
            "public.eyJhbGciOi.fake.jwt"
        );
        assert_eq!(
            authorized.propagated_access_token(),
            Some("public.eyJhbGciOi.fake.jwt")
        );
        assert_eq!(
            authorized.cache_partition(),
            header_cache_partition,
            "transport choice must not change cache isolation identity"
        );
    }

    #[tokio::test]
    async fn read_action_can_authorize_scoped_and_global_resources() {
        let catalog = RegistryCatalog::parse("public=memory:///auth/public/").unwrap();
        let auth = DeliveryAuth::new(catalog, std::iter::empty::<(String, String)>()).unwrap();
        let body = serde_json::json!({
            "schema_version": 1,
            "registry_id": "public",
            "revision": 1,
            "credentials": [{
                "credential_sha256": credential_sha256("public", "reader"),
                "principal_id": "reader",
                "enabled": true,
                "namespaces": ["demo"],
                "actions": ["read"],
                "allow_missing_origin": true
            }]
        });
        put_snapshot(&auth, "public", body.to_string().into_bytes()).await;

        let request_headers = headers("public.reader");
        auth.authorize(&request_headers, None, Some("demo"), DeliveryAction::Read)
            .await
            .expect("matching namespace");
        auth.authorize(&request_headers, None, None, DeliveryAction::Read)
            .await
            .expect("global shared resource");
        assert!(matches!(
            auth.authorize_static(&request_headers, None, "demo").await,
            Err(AuthFailure::Forbidden)
        ));
    }

    #[tokio::test]
    async fn registry_load_normalizes_namespace_grants_once() {
        let catalog = RegistryCatalog::parse("public=memory:///auth/public/").unwrap();
        let auth = DeliveryAuth::new(catalog, std::iter::empty::<(String, String)>()).unwrap();
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
        put_snapshot(&auth, "public", body.to_string().into_bytes()).await;

        let authorized = auth
            .authorize(
                &headers("public.reader"),
                None,
                Some("anything"),
                DeliveryAction::Read,
            )
            .await
            .unwrap();

        assert_eq!(authorized.readable_namespaces(), &["*".to_string()]);
    }

    #[tokio::test]
    async fn wrong_credential_namespace_and_origin_are_rejected() {
        let auth = configured_auth("public", "secret").await;
        let mut valid = headers("public.secret");
        valid.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://maps.example"),
        );
        assert!(matches!(
            auth.authorize_static(&headers("public.wrong"), None, "demo")
                .await,
            Err(AuthFailure::InvalidCredential)
        ));
        assert!(matches!(
            auth.authorize_static(&valid, None, "other").await,
            Err(AuthFailure::Forbidden)
        ));
        valid.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://evil.example"),
        );
        assert!(matches!(
            auth.authorize_static(&valid, None, "demo").await,
            Err(AuthFailure::Forbidden)
        ));
    }

    #[tokio::test]
    async fn unknown_registry_rejects_before_store_resolution() {
        let catalog = RegistryCatalog::parse("known=memory:///auth/known/").unwrap();
        let auth = DeliveryAuth::new(catalog, std::iter::empty::<(String, String)>()).unwrap();

        assert!(matches!(
            auth.authorize_static(&headers("unknown.anything"), None, "demo")
                .await,
            Err(AuthFailure::InvalidCredential)
        ));
        assert!(lock_unpoisoned(&auth.inner.stores.stores).is_empty());
    }

    #[tokio::test]
    async fn failed_refresh_keeps_the_last_known_good_snapshot() {
        let auth = configured_auth("public", "secret").await;
        let mut request_headers = headers("public.secret");
        request_headers.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://maps.example"),
        );
        auth.authorize_static(&request_headers, None, "demo")
            .await
            .expect("initial snapshot");

        put_snapshot(&auth, "public", b"not-json".to_vec()).await;
        let mut cached = auth.inner.cache.get("public").expect("cached snapshot");
        cached.refresh_after = Instant::now();
        auth.inner.cache.insert("public".to_string(), cached);

        auth.authorize_static(&request_headers, None, "demo")
            .await
            .expect("last-known-good snapshot remains usable");
    }

    #[tokio::test]
    async fn cache_partition_changes_when_registry_policy_revision_advances() {
        let auth = configured_auth("public", "secret").await;
        let mut request_headers = headers("public.secret");
        request_headers.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://maps.example"),
        );
        let initial = auth
            .authorize_static(&request_headers, None, "demo")
            .await
            .expect("initial snapshot")
            .cache_partition();

        put_snapshot(
            &auth,
            "public",
            snapshot_json_at_revision("public", "secret", 2),
        )
        .await;
        let mut cached = auth.inner.cache.get("public").expect("cached snapshot");
        cached.refresh_after = Instant::now();
        auth.inner.cache.insert("public".to_string(), cached);

        let refreshed = auth
            .authorize_static(&request_headers, None, "demo")
            .await
            .expect("newer snapshot")
            .cache_partition();

        assert_ne!(initial, refreshed);
    }

    #[tokio::test]
    async fn cache_eviction_does_not_forget_the_installed_revision() {
        let auth = configured_auth("public", "original").await;
        let mut request_headers = headers("public.original");
        request_headers.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://maps.example"),
        );
        auth.authorize_static(&request_headers, None, "demo")
            .await
            .expect("initial snapshot");

        auth.inner.cache.invalidate("public");
        put_snapshot(
            &auth,
            "public",
            snapshot_json_at_revision("public", "replacement", 1),
        )
        .await;

        assert!(matches!(
            auth.authorize_static(&headers("public.replacement"), None, "demo")
                .await,
            Err(AuthFailure::Unavailable)
        ));
    }

    #[test]
    fn malformed_snapshots_are_rejected_as_a_whole() {
        let duplicate = encode_sha256([7; 32]);
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
    fn duplicate_security_headers_are_rejected() {
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

        let name = HeaderName::from_static("origin");
        headers.append(name.clone(), HeaderValue::from_static("https://a.example"));
        headers.append(name.clone(), HeaderValue::from_static("https://b.example"));
        assert!(matches!(
            single_header(&headers, name),
            Err(AuthFailure::Forbidden)
        ));
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
}
