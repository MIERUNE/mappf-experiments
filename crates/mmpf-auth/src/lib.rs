//! Optional delivery-plane authentication shared by MMPF delivery servers.
//!
//! The public token envelope deliberately owns only registry selection. The
//! suffix is opaque here: each registry adapter decides whether it represents
//! a random secret, a JWT, or another credential format.
//! A service may also select one configured registry whose explicit anonymous
//! grant applies only when the request contains no credential at all.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::bail;
use http::HeaderMap;
use mmpf_common::singleflight::{Flight, SingleFlight};
use mmpf_common::sync::lock_unpoisoned;
use moka::sync::Cache;
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectStoreError, GetOptions, ObjectStore, parse_url_opts};
use prometheus::proto::MetricFamily;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio::time::Instant;
use url::Url;

mod catalog;
mod credential;
mod metrics;
mod policy;

pub use catalog::RegistryCatalog;
pub use credential::{AuthFailure, credential_sha256};
pub use policy::DeliveryAction;

use catalog::{RegistryConfig, validate_registry_id};
use credential::{
    anonymous_cache_partition, credential_cache_partition, credential_digest, delivery_token,
    parse_token_envelope,
};
use metrics::AuthMetrics;
use policy::{RegistrySnapshot, authorize_grant};

const MAX_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024;
const AUTH_CACHE_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_CONCURRENT_REGISTRY_LOADS: usize = 8;
const REFRESH_INTERVAL: Duration = Duration::from_mins(1);
const REFRESH_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
const OBJECT_STORE_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct DeliveryAuth {
    inner: Arc<DeliveryAuthInner>,
}

struct DeliveryAuthInner {
    catalog: RegistryCatalog,
    anonymous_registry_id: Option<String>,
    stores: ObjectStores,
    cache: Cache<String, CachedRegistry>,
    cold_retry_after: Mutex<HashMap<String, Instant>>,
    installed_revisions: Mutex<HashMap<String, InstalledRevision>>,
    refreshes: SingleFlight<String, AuthUnavailable>,
    refresh_permits: Arc<Semaphore>,
    metrics: AuthMetrics,
}

#[derive(Clone)]
struct CachedRegistry {
    snapshot: Arc<RegistrySnapshot>,
    etag: Option<String>,
    refresh_after: Instant,
    source_bytes: u32,
    /// When the backend last confirmed this snapshot current — a fetched body
    /// or a `304`. Deliberately distinct from `refresh_after`, which only says
    /// when the next attempt is due: a failing refresh keeps moving that
    /// deadline while this stands still, and the gap is the revocation lag a
    /// prolonged outage can accumulate.
    validated_at: Instant,
    /// First failure of the current failing streak, cleared by the next
    /// successful validation. `None` means refresh is healthy.
    refresh_failing_since: Option<Instant>,
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
        Self::new_with_anonymous_registry(catalog, None, object_store_options)
            .expect("no anonymous registry selection is always valid")
    }

    /// Builds delivery authentication with an optional registry whose
    /// explicitly configured anonymous grant applies when no credential is
    /// presented.
    ///
    /// Missing credentials may use this policy. Malformed, mixed, unknown, or
    /// otherwise invalid credentials never fall back to anonymous access.
    pub fn new_with_anonymous_registry<I, K, V>(
        catalog: RegistryCatalog,
        anonymous_registry_id: Option<String>,
        object_store_options: I,
    ) -> anyhow::Result<Option<Self>>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        if let Some(registry_id) = anonymous_registry_id.as_deref() {
            validate_registry_id(registry_id)?;
            if catalog.get(registry_id).is_none() {
                bail!("anonymous auth registry {registry_id:?} is not configured");
            }
        }
        if catalog.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            inner: Arc::new(DeliveryAuthInner {
                catalog,
                anonymous_registry_id,
                stores: ObjectStores::new(object_store_options),
                cache: Cache::builder()
                    .max_capacity(AUTH_CACHE_CAPACITY_BYTES)
                    .weigher(|_registry_id: &String, cached: &CachedRegistry| cached.source_bytes)
                    .build(),
                cold_retry_after: Mutex::new(HashMap::new()),
                installed_revisions: Mutex::new(HashMap::new()),
                refreshes: SingleFlight::default(),
                refresh_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_REGISTRY_LOADS)),
                metrics: AuthMetrics::new(),
            }),
        }))
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
        let Some(presented) = delivery_token(headers, query)? else {
            return self.authorize_anonymous(headers, namespace, action).await;
        };
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
            .filter(|grant| grant.authorization.enabled)
        else {
            return Err(AuthFailure::InvalidCredential);
        };

        authorize_grant(headers, &grant.authorization, namespace, action)?;
        Ok(AuthorizedDelivery {
            principal_id: grant.authorization.principal_id.clone(),
            registry_id: registry_id.to_string(),
            readable_namespaces: Arc::clone(&grant.authorization.namespaces),
            cache_partition: credential_cache_partition(registry_id, credential, snapshot.revision),
            presented_token: Some(Arc::from(presented.value.as_ref())),
            propagate_access_token,
        })
    }

    async fn authorize_anonymous(
        &self,
        headers: &HeaderMap,
        namespace: Option<&str>,
        action: DeliveryAction,
    ) -> Result<AuthorizedDelivery, AuthFailure> {
        let Some(registry_id) = self.inner.anonymous_registry_id.as_deref() else {
            return Err(AuthFailure::InvalidCredential);
        };
        let config = self
            .inner
            .catalog
            .get(registry_id)
            .expect("anonymous registry selection is validated at construction");
        let snapshot = self
            .snapshot(registry_id, config)
            .await
            .map_err(|_| AuthFailure::Unavailable)?;
        let Some(grant) = snapshot.anonymous.as_ref().filter(|grant| grant.enabled) else {
            return Err(AuthFailure::InvalidCredential);
        };
        authorize_grant(headers, grant, namespace, action)?;
        Ok(AuthorizedDelivery {
            principal_id: grant.principal_id.clone(),
            registry_id: registry_id.to_string(),
            readable_namespaces: Arc::clone(&grant.namespaces),
            cache_partition: anonymous_cache_partition(registry_id, snapshot.revision),
            presented_token: None,
            propagate_access_token: false,
        })
    }

    async fn snapshot(
        &self,
        registry_id: &str,
        config: &RegistryConfig,
    ) -> Result<Arc<RegistrySnapshot>, AuthUnavailable> {
        loop {
            let now = Instant::now();
            match self.inner.cache.get(registry_id) {
                Some(cached) if cached.refresh_after > now => return Ok(cached.snapshot),
                Some(_) => {}
                None if lock_unpoisoned(&self.inner.cold_retry_after)
                    .get(registry_id)
                    .is_some_and(|retry_after| *retry_after > now) =>
                {
                    return Err(AuthUnavailable);
                }
                None => {}
            }

            match self.inner.refreshes.begin(registry_id.to_string()) {
                Flight::Leader(leader) => match self.refresh(registry_id, config).await {
                    Ok(snapshot) => return Ok(snapshot),
                    Err(error) => {
                        self.inner.metrics.record_refresh(registry_id, "failure");
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
                let now = Instant::now();
                cached.refresh_after = now + REFRESH_INTERVAL;
                // A `304` is a successful validation: the entry is confirmed
                // current, so its age restarts and any failing streak ends.
                cached.validated_at = now;
                cached.refresh_failing_since = None;
                let snapshot = cached.snapshot.clone();
                self.inner.cache.insert(registry_id.to_string(), cached);
                self.inner
                    .metrics
                    .record_refresh(registry_id, "not_modified");
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
        let now = Instant::now();
        self.inner.cache.insert(
            registry_id.to_string(),
            CachedRegistry {
                snapshot: snapshot.clone(),
                etag,
                refresh_after: now + REFRESH_INTERVAL,
                source_bytes: body.len() as u32,
                validated_at: now,
                refresh_failing_since: None,
            },
        );
        self.inner.metrics.record_refresh(registry_id, "success");
        Ok(snapshot)
    }

    fn defer_failed_refresh(&self, registry_id: &str) -> Option<Arc<RegistrySnapshot>> {
        if let Some(mut cached) = self.inner.cache.get(registry_id) {
            let now = Instant::now();
            cached.refresh_after = now + REFRESH_FAILURE_COOLDOWN;
            // Keep the streak's first failure: the streak length, not the last
            // attempt, is how long this grant set has outlived its validation.
            cached.refresh_failing_since.get_or_insert(now);
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

    /// Registry freshness families, sampled at scrape time because snapshot age
    /// is only meaningful relative to the moment it is read.
    ///
    /// `registry_id` is a bounded label: ids come from validated configuration,
    /// never from a request, and the catalog validation caps their count.
    pub fn gather_metrics(&self) -> Vec<MetricFamily> {
        let metrics = &self.inner.metrics;
        let now = Instant::now();
        let seconds_since = |since| now.saturating_duration_since(since).as_secs_f64();
        for registry_id in self.inner.catalog.registry_ids() {
            // Every configured registry reports, loaded or not: an absent series
            // is indistinguishable from a healthy one, so a registry that never
            // loaded — and therefore fails every request closed — would
            // otherwise be invisible to alerting. Its timings read zero because
            // an unloaded snapshot has no age, which is why `snapshot_loaded`
            // has to be part of any staleness alert.
            let cached = self.inner.cache.get(registry_id);
            metrics.observe_snapshot(
                registry_id,
                cached.is_some(),
                cached
                    .as_ref()
                    .map_or(0.0, |cached| seconds_since(cached.validated_at)),
                cached.as_ref().map_or(0.0, |cached| {
                    cached.refresh_failing_since.map_or(0.0, seconds_since)
                }),
                cached.as_ref().map_or(0, |cached| cached.snapshot.revision),
            );
        }
        metrics.gather()
    }
}

pub struct AuthorizedDelivery {
    pub principal_id: String,
    pub registry_id: String,
    readable_namespaces: Arc<[String]>,
    cache_partition: [u8; 32],
    presented_token: Option<Arc<str>>,
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
    pub fn backend_bearer_token(&self) -> Option<&str> {
        self.presented_token.as_deref()
    }

    /// Returns the verified query credential when the caller used
    /// `access_token`. Header credentials are never copied into generated URLs.
    pub fn propagated_access_token(&self) -> Option<&str> {
        if self.propagate_access_token {
            self.presented_token.as_deref()
        } else {
            None
        }
    }

    /// Shares the verified query credential without copying its secret bytes.
    /// Header credentials are never converted into a URL credential.
    pub fn shared_propagated_access_token(&self) -> Option<Arc<str>> {
        if self.propagate_access_token {
            self.presented_token.as_ref().map(Arc::clone)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
struct AuthUnavailable;

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
                    .then(|| ("allow_http".to_string(), "true".to_string()));
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
    use http::HeaderValue;
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
                "credential_sha256": credential_sha256(registry_id, credential),
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

    async fn configured_anonymous_auth(revision: u64) -> DeliveryAuth {
        let catalog = RegistryCatalog::parse("public=memory:///auth/public/").unwrap();
        let auth = DeliveryAuth::new_with_anonymous_registry(
            catalog,
            Some("public".to_string()),
            std::iter::empty::<(String, String)>(),
        )
        .unwrap()
        .unwrap();
        let snapshot = serde_json::json!({
            "schema_version": 1,
            "registry_id": "public",
            "revision": revision,
            "anonymous": {
                "enabled": true,
                "namespaces": ["mierune", "carto", "mapterhorn"],
                "actions": ["read", "render.static"],
                "allowed_origins": [],
                "allow_missing_origin": true
            },
            "credentials": [{
                "credential_sha256": credential_sha256("public", "private"),
                "principal_id": "private-user",
                "enabled": true,
                "namespaces": ["private"],
                "actions": ["read", "render.static"],
                "allowed_origins": [],
                "allow_missing_origin": true
            }]
        })
        .to_string()
        .into_bytes();
        put_snapshot(&auth, "public", snapshot).await;
        auth
    }

    async fn put_snapshot(auth: &DeliveryAuth, registry_id: &str, body: Vec<u8>) {
        let config = auth.inner.catalog.get(registry_id).unwrap();
        let (store, path) = auth.inner.stores.resolve(&config.current_url).unwrap();
        store.put(&path, PutPayload::from(body)).await.unwrap();
    }

    fn labelled_sample(
        families: &[MetricFamily],
        name: &str,
        labels: &[(&str, &str)],
    ) -> Option<f64> {
        let family = families.iter().find(|family| family.name() == name)?;
        let metric = family.get_metric().iter().find(|metric| {
            labels.iter().all(|(expected_name, expected_value)| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == *expected_name && label.value() == *expected_value)
            })
        })?;
        Some(match family.get_field_type() {
            prometheus::proto::MetricType::COUNTER => metric.get_counter().value(),
            _ => metric.get_gauge().value(),
        })
    }

    /// Reads one per-registry gauge or counter sample.
    fn sample(families: &[MetricFamily], name: &str, registry_id: &str) -> Option<f64> {
        labelled_sample(families, name, &[("registry_id", registry_id)])
    }

    fn headers_from_allowed_origin(token: &str) -> HeaderMap {
        let mut headers = headers(token);
        headers.insert(
            http::header::ORIGIN,
            HeaderValue::from_static("https://maps.example"),
        );
        headers
    }

    async fn delete_snapshot(auth: &DeliveryAuth, registry_id: &str) {
        let config = auth.inner.catalog.get(registry_id).unwrap();
        let (store, path) = auth.inner.stores.resolve(&config.current_url).unwrap();
        store.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn a_configured_registry_that_never_loaded_reports_itself_unloaded() {
        let catalog = RegistryCatalog::parse("cold=memory:///auth/cold/").unwrap();
        let auth = DeliveryAuth::new(catalog, std::iter::empty::<(String, String)>()).unwrap();

        let families = auth.gather_metrics();
        assert_eq!(
            sample(&families, "mmpf_auth_registry_snapshot_loaded", "cold"),
            Some(0.0),
            "a registry with no snapshot fails every request closed and must be visible"
        );
    }

    #[tokio::test]
    async fn a_validated_snapshot_reports_no_unvalidated_time() {
        let auth = configured_auth("corp", "secret").await;
        auth.authorize_static(&headers_from_allowed_origin("corp.secret"), None, "demo")
            .await
            .expect("the snapshot authorizes this credential");

        let families = auth.gather_metrics();
        assert_eq!(
            sample(&families, "mmpf_auth_registry_snapshot_loaded", "corp"),
            Some(1.0)
        );
        assert_eq!(
            sample(&families, "mmpf_auth_registry_unvalidated_seconds", "corp"),
            Some(0.0),
            "a freshly validated snapshot has no unvalidated window"
        );
        assert_eq!(
            sample(&families, "mmpf_auth_registry_revision", "corp"),
            Some(1.0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_refresh_ages_the_snapshot_it_keeps_serving() {
        let auth = configured_auth("corp", "secret").await;
        auth.authorize_static(&headers_from_allowed_origin("corp.secret"), None, "demo")
            .await
            .expect("the first authorization loads the snapshot");

        // The backend loses the object: refresh now fails while the loaded
        // snapshot stays authoritative (the read tier serves last known good).
        delete_snapshot(&auth, "corp").await;
        tokio::time::advance(REFRESH_INTERVAL + Duration::from_secs(1)).await;
        auth.authorize_static(&headers_from_allowed_origin("corp.secret"), None, "demo")
            .await
            .expect("a failed refresh must not revoke a live grant");

        tokio::time::advance(Duration::from_secs(10)).await;
        let families = auth.gather_metrics();
        let first_streak = sample(&families, "mmpf_auth_registry_unvalidated_seconds", "corp")
            .expect("the failing streak is reported");
        assert!(
            first_streak >= 10.0,
            "a snapshot served past a failed refresh is no longer validated, got {first_streak}"
        );
        assert!(
            sample(&families, "mmpf_auth_registry_snapshot_age_seconds", "corp")
                .is_some_and(|age| age >= 71.0),
            "age accrues from the last successful validation, not the last attempt"
        );

        // A second failure must extend the same streak rather than restart it:
        // the accumulated revocation lag is the whole outage, not one attempt.
        tokio::time::advance(REFRESH_FAILURE_COOLDOWN + Duration::from_secs(1)).await;
        auth.authorize_static(&headers_from_allowed_origin("corp.secret"), None, "demo")
            .await
            .expect("still served from last known good");
        let families = auth.gather_metrics();
        let second_streak = sample(&families, "mmpf_auth_registry_unvalidated_seconds", "corp")
            .expect("the failing streak is reported");
        assert!(
            second_streak > first_streak,
            "the streak must accumulate across attempts, got {first_streak} then {second_streak}"
        );
        assert!(
            labelled_sample(
                &families,
                "mmpf_auth_registry_refresh_total",
                &[("registry_id", "corp"), ("outcome", "failure")],
            )
            .is_some_and(|n| n >= 1.0),
            "refresh failures must be counted"
        );
        assert_eq!(
            sample(&families, "mmpf_auth_registry_snapshot_loaded", "corp"),
            Some(1.0),
            "the entry is still serving, which is exactly why its age matters"
        );
    }

    #[test]
    fn anonymous_registry_selection_must_name_a_configured_registry() {
        let catalog = RegistryCatalog::parse("public=memory:///auth/public/").unwrap();
        assert!(
            DeliveryAuth::new_with_anonymous_registry(
                catalog,
                Some("missing".to_string()),
                std::iter::empty::<(String, String)>(),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn missing_credentials_use_only_the_explicit_anonymous_grant() {
        let auth = configured_anonymous_auth(7).await;
        let no_headers = HeaderMap::new();

        let authorized = auth
            .authorize_static(&no_headers, None, "mierune")
            .await
            .expect("configured public namespace");
        assert_eq!(authorized.principal_id, "anonymous");
        assert_eq!(authorized.registry_id, "public");
        assert_eq!(
            authorized.readable_namespaces(),
            &[
                "carto".to_string(),
                "mapterhorn".to_string(),
                "mierune".to_string()
            ]
        );
        assert_eq!(authorized.backend_bearer_token(), None);
        assert_eq!(authorized.propagated_access_token(), None);

        auth.authorize(&no_headers, None, Some("carto"), DeliveryAction::Read)
            .await
            .expect("anonymous read grant");
        assert!(matches!(
            auth.authorize_static(&no_headers, None, "private").await,
            Err(AuthFailure::Forbidden)
        ));
    }

    #[tokio::test]
    async fn invalid_credentials_never_fall_back_to_anonymous_access() {
        let auth = configured_anonymous_auth(1).await;

        assert!(matches!(
            auth.authorize_static(&headers("public.wrong"), None, "mierune")
                .await,
            Err(AuthFailure::InvalidCredential)
        ));
        assert!(matches!(
            auth.authorize_static(&HeaderMap::new(), Some("access_token=malformed"), "mierune")
                .await,
            Err(AuthFailure::InvalidCredential)
        ));
    }

    #[tokio::test]
    async fn missing_credentials_still_fail_without_an_anonymous_selection() {
        let auth = configured_auth("public", "secret").await;
        assert!(matches!(
            auth.authorize_static(&HeaderMap::new(), None, "demo").await,
            Err(AuthFailure::InvalidCredential)
        ));
    }

    #[tokio::test]
    async fn anonymous_selection_does_not_imply_access_without_a_snapshot_grant() {
        let catalog = RegistryCatalog::parse("public=memory:///auth/public/").unwrap();
        let auth = DeliveryAuth::new_with_anonymous_registry(
            catalog,
            Some("public".to_string()),
            std::iter::empty::<(String, String)>(),
        )
        .unwrap()
        .unwrap();
        put_snapshot(&auth, "public", snapshot_json("public", "secret")).await;

        assert!(matches!(
            auth.authorize_static(&HeaderMap::new(), None, "demo").await,
            Err(AuthFailure::InvalidCredential)
        ));
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
            Some("public.eyJhbGciOi.fake.jwt")
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
            Some("public.eyJhbGciOi.fake.jwt")
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
}
