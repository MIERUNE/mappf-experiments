//! Style preparation, resource caching, and per-key fetch coordination.

use std::{hash::Hash, sync::Arc, time::Duration};

use async_trait::async_trait;
use biei_core::{
    style_catalog::StyleCatalog,
    types::{
        AddLayerSource, CredentialCachePartition, InternalTask, ProfilePreparationError,
        RenderAuthorization, SourceHash, StyleId, StyleRevision,
    },
};
use mmpf_common::singleflight::{Flight, SingleFlight};
use moka::sync::Cache;
use tokio::time::Instant;

use crate::renderer::{PreparedProfile, ProfilePreparer, StyleAvailabilityError};

use super::profile_fetch::{
    addlayer_source_from_task, addlayer_source_hash_from_task, fetch_style_json_with_auth,
    fetch_tileset_json_with_auth, rewrite_tileset_source_json, source_url_from_addlayer_source,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct StyleCacheKey {
    revision: StyleRevision,
    credential: Option<CredentialCachePartition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TilesetCacheKey {
    url: String,
    credential: Option<CredentialCachePartition>,
}

fn credential_partition(
    authorization: Option<&RenderAuthorization>,
) -> Option<CredentialCachePartition> {
    authorization.map(|authorization| authorization.cache_partition)
}

pub(crate) struct MapLibreProfilePreparer {
    style_catalog: Arc<StyleCatalog>,
    http_client: reqwest::Client,
    url_policy: mmpf_mln_filesource::policy::ResourceUrlPolicy,
    auth_provider_origin: Option<url::Url>,
    fetch_permits: Arc<tokio::sync::Semaphore>,
    style_json_cache: Cache<StyleCacheKey, Arc<str>>,
    style_error_cache: Cache<StyleCacheKey, ProfilePreparationError>,
    tileset_json_cache: Cache<TilesetCacheKey, Arc<str>>,
    tileset_error_cache: Cache<TilesetCacheKey, ProfilePreparationError>,
    inflight_style_loads: SingleFlight<StyleCacheKey, ProfileFetchError>,
    inflight_tileset_loads: SingleFlight<TilesetCacheKey, ProfileFetchError>,
}

const STYLE_JSON_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const STYLE_JSON_CACHE_IDLE_TTL: Duration = Duration::from_secs(60 * 60);
const TILESET_JSON_CACHE_MAX_BYTES: u64 = 32 * 1024 * 1024;
const TILESET_JSON_CACHE_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
// Absolute freshness bound. Idle TTL alone lets continuous traffic renew a hot
// entry forever, so an upstream style/TileJSON edit would never become visible.
// `time_to_live` caps an entry's age from insertion regardless of access, so a
// hot entry is refetched after at most this age (the idle TTL still evicts cold
// entries sooner for capacity). Refetch flows through the same single-flight
// path, and a failed refetch fails/negative-caches rather than silently serving
// the stale value.
const STYLE_JSON_CACHE_MAX_AGE: Duration = Duration::from_secs(60 * 60);
const TILESET_JSON_CACHE_MAX_AGE: Duration = Duration::from_secs(60 * 60);
const JSON_NEGATIVE_CACHE_MAX_ENTRIES: u64 = 4096;
// Short on purpose: the negative cache only needs to absorb repeated requests
// for the same definitively-bad style or TileJSON within a burst (§7.5 spray
// defense). A longer TTL would delay a freshly-registered/fixed resource from
// becoming servable. Transient failures (5xx, connection/read errors,
// timeouts) are not cached here at all — see `ProfileFetchError`.
const JSON_NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(5);

pub(super) fn is_permanent_profile_http_status(status: reqwest::StatusCode) -> bool {
    status.is_client_error()
        && status != reqwest::StatusCode::REQUEST_TIMEOUT
        && status != reqwest::StatusCode::TOO_MANY_REQUESTS
}

impl MapLibreProfilePreparer {
    #[cfg(test)]
    pub(crate) fn new(
        style_catalog: Arc<StyleCatalog>,
        max_concurrent_fetches: usize,
        private_hosts: Vec<String>,
    ) -> anyhow::Result<Self> {
        Self::new_with_auth_provider_origin(
            style_catalog,
            max_concurrent_fetches,
            private_hosts,
            None,
        )
    }

    pub(crate) fn new_with_auth_provider_origin(
        style_catalog: Arc<StyleCatalog>,
        max_concurrent_fetches: usize,
        private_hosts: Vec<String>,
        auth_provider_origin: Option<url::Url>,
    ) -> anyhow::Result<Self> {
        let url_policy = mmpf_mln_filesource::policy::ResourceUrlPolicy::new(private_hosts);
        Ok(Self {
            style_catalog,
            http_client: mmpf_mln_filesource::build_profile_http_client(
                url_policy.clone(),
                crate::renderer::RESOURCE_USER_AGENT,
            )?,
            url_policy,
            auth_provider_origin,
            fetch_permits: Arc::new(tokio::sync::Semaphore::new(max_concurrent_fetches.max(1))),
            style_json_cache: style_json_cache(),
            style_error_cache: error_cache(),
            tileset_json_cache: tileset_json_cache(),
            tileset_error_cache: error_cache(),
            inflight_style_loads: SingleFlight::default(),
            inflight_tileset_loads: SingleFlight::default(),
        })
    }

    async fn resolve_style(
        &self,
        style: &StyleRevision,
        authorization: Option<&RenderAuthorization>,
        deadline: Instant,
    ) -> Result<PreparedProfile, ProfilePreparationError> {
        self.resolve_style_fetch(style, authorization, deadline)
            .await
            .map_err(|failure| failure.error)
    }

    /// Single-flight JSON load shared by style and tileset resolution: serve a
    /// cache hit, join an in-flight load, honor the negative cache, or become the
    /// loader (await `fetch`, store the result, wake waiters). `lookup` stays
    /// per-resource because the style and tileset caches probe positive vs error
    /// entries in the opposite order. `fetch` is a lazy future — it only runs on
    /// the loader path and is otherwise dropped un-awaited.
    async fn single_flight_load<K>(
        &self,
        key: K,
        caches: JsonCaches<'_, K>,
        deadline: Instant,
        lookup: impl Fn() -> Option<Result<Arc<str>, ProfileFetchError>>,
        fetch: impl std::future::Future<Output = Result<Arc<str>, ProfileFetchError>>,
    ) -> Result<Arc<str>, ProfileFetchError>
    where
        K: Eq + Hash + Clone + Send + Sync + 'static,
    {
        let mut fetch = Some(fetch);
        loop {
            if let Some(cached) = lookup() {
                return cached;
            }
            match caches.inflight.begin(key.clone()) {
                Flight::Leader(guard) => {
                    let result = fetch
                        .take()
                        .expect("JSON fetch future is polled by one leader")
                        .await;
                    match &result {
                        Ok(json) => {
                            caches.json.insert(key, json.clone());
                            drop(guard);
                        }
                        Err(failure) => {
                            // Only definitive failures are negative-cached;
                            // transient failures remain retryable by later calls.
                            if failure.negative_cacheable {
                                caches.error.insert(key, failure.error.clone());
                            }
                            if failure.is_attempt_wide() {
                                guard.complete_with_error(failure.clone());
                            } else {
                                // The elected caller exhausted only its own budget.
                                // Releasing the flight without an outcome wakes
                                // followers so one can retry under its own deadline.
                                drop(guard);
                            }
                        }
                    }
                    return result;
                }
                Flight::Follower(follower) => {
                    if let Some(failure) = tokio::time::timeout_at(deadline, follower.wait())
                        .await
                        .map_err(|_| ProfileFetchError::caller_deadline())?
                    {
                        return Err(failure);
                    }
                }
            }
        }
    }

    async fn resolve_style_fetch(
        &self,
        style: &StyleRevision,
        authorization: Option<&RenderAuthorization>,
        deadline: Instant,
    ) -> Result<PreparedProfile, ProfileFetchError> {
        let key = StyleCacheKey {
            revision: style.clone(),
            credential: credential_partition(authorization),
        };
        let style_json = self
            .single_flight_load(
                key.clone(),
                JsonCaches {
                    json: &self.style_json_cache,
                    error: &self.style_error_cache,
                    inflight: &self.inflight_style_loads,
                },
                deadline,
                || self.lookup_style_cache(&key),
                self.fetch_uncached_style(style, authorization, deadline),
            )
            .await?;
        Ok(PreparedProfile {
            revision: style.clone(),
            authorization_partition: credential_partition(authorization),
            style_json,
            addlayer_source: None,
        })
    }

    async fn resolve_addlayer_source(
        &self,
        task: &InternalTask,
    ) -> Result<Option<AddLayerSource>, ProfilePreparationError> {
        let Some(source) = addlayer_source_from_task(task) else {
            return Ok(None);
        };
        let source_hash = addlayer_source_hash_from_task(task).unwrap_or(0);
        let source_json = self
            .resolve_tileset_source_json(
                &task.style.id,
                source,
                task.authorization.as_ref(),
                task.deadline,
            )
            .await
            .map_err(|err| source_unavailable_from(err, source_hash))?;
        Ok(Some(AddLayerSource {
            tileset_id: source.tileset_id.clone(),
            json: source_json,
        }))
    }

    async fn resolve_tileset_source_json(
        &self,
        style_id: &StyleId,
        source: &AddLayerSource,
        authorization: Option<&RenderAuthorization>,
        deadline: Instant,
    ) -> Result<String, ProfilePreparationError> {
        let tileset_url = source_url_from_addlayer_source(style_id, source)?;
        let tilejson = self
            .resolve_tileset_json(style_id, &tileset_url, authorization, deadline)
            .await?;
        rewrite_tileset_source_json(style_id, source, &tileset_url, &tilejson)
    }

    async fn resolve_tileset_json(
        &self,
        style_id: &StyleId,
        tileset_url: &str,
        authorization: Option<&RenderAuthorization>,
        deadline: Instant,
    ) -> Result<Arc<str>, ProfilePreparationError> {
        let key = TilesetCacheKey {
            url: tileset_url.to_string(),
            credential: credential_partition(authorization),
        };
        self.single_flight_load(
            key.clone(),
            JsonCaches {
                json: &self.tileset_json_cache,
                error: &self.tileset_error_cache,
                inflight: &self.inflight_tileset_loads,
            },
            deadline,
            || self.lookup_tileset_cache(&key),
            self.fetch_uncached_tileset(style_id, tileset_url, authorization, deadline),
        )
        .await
        .map_err(|failure| failure.error)
    }

    fn lookup_style_cache(
        &self,
        key: &StyleCacheKey,
    ) -> Option<Result<Arc<str>, ProfileFetchError>> {
        if let Some(err) = self.style_error_cache.get(key) {
            return Some(Err(ProfileFetchError::permanent(err)));
        }
        self.style_json_cache.get(key).map(Ok)
    }

    fn lookup_tileset_cache(
        &self,
        key: &TilesetCacheKey,
    ) -> Option<Result<Arc<str>, ProfileFetchError>> {
        if let Some(tilejson) = self.tileset_json_cache.get(key) {
            return Some(Ok(tilejson));
        }
        self.tileset_error_cache
            .get(key)
            .map(ProfileFetchError::permanent)
            .map(Err)
    }

    async fn fetch_uncached_style(
        &self,
        style: &StyleRevision,
        authorization: Option<&RenderAuthorization>,
        deadline: Instant,
    ) -> Result<Arc<str>, ProfileFetchError> {
        let _permit = tokio::time::timeout_at(deadline, self.fetch_permits.acquire())
            .await
            .map_err(|_| ProfileFetchError::caller_deadline())?
            .map_err(|_| {
                ProfileFetchError::transient(ProfilePreparationError::infrastructure(
                    "profile fetch semaphore closed",
                ))
            })?;
        let definition = self
            .style_catalog
            .definition_for_revision(style)
            .ok_or_else(|| {
                ProfileFetchError::transient(ProfilePreparationError::infrastructure(format!(
                    "style definition for {}@{} is not registered",
                    style.id.as_str(),
                    style.version
                )))
            })?;
        Ok(Arc::from(
            fetch_style_json_with_auth(
                &self.http_client,
                &self.url_policy,
                &style.id,
                &definition.style_url,
                authorization.map(|authorization| &authorization.provider_bearer_token),
                self.auth_provider_origin.as_ref(),
                deadline,
            )
            .await?,
        ))
    }

    async fn fetch_uncached_tileset(
        &self,
        style_id: &StyleId,
        tileset_url: &str,
        authorization: Option<&RenderAuthorization>,
        deadline: Instant,
    ) -> Result<Arc<str>, ProfileFetchError> {
        let _permit = tokio::time::timeout_at(deadline, self.fetch_permits.acquire())
            .await
            .map_err(|_| ProfileFetchError::caller_deadline())?
            .map_err(|_| {
                ProfileFetchError::transient(ProfilePreparationError::infrastructure(
                    "profile fetch semaphore closed",
                ))
            })?;
        Ok(Arc::from(
            fetch_tileset_json_with_auth(
                &self.http_client,
                &self.url_policy,
                style_id,
                tileset_url,
                authorization.map(|authorization| &authorization.provider_bearer_token),
                self.auth_provider_origin.as_ref(),
                deadline,
            )
            .await?,
        ))
    }
}

/// The positive, negative, and in-flight maps for one JSON resource type,
/// bundled so single-flight loading takes one argument instead of three.
struct JsonCaches<'a, K: Eq + Hash> {
    json: &'a Cache<K, Arc<str>>,
    error: &'a Cache<K, ProfilePreparationError>,
    inflight: &'a SingleFlight<K, ProfileFetchError>,
}

fn style_json_cache() -> Cache<StyleCacheKey, Arc<str>> {
    Cache::builder()
        .max_capacity(STYLE_JSON_CACHE_MAX_BYTES)
        .time_to_idle(STYLE_JSON_CACHE_IDLE_TTL)
        .time_to_live(STYLE_JSON_CACHE_MAX_AGE)
        .weigher(|_key: &StyleCacheKey, style_json: &Arc<str>| {
            style_json.len().clamp(1, u32::MAX as usize) as u32
        })
        .build()
}

fn tileset_json_cache() -> Cache<TilesetCacheKey, Arc<str>> {
    Cache::builder()
        .max_capacity(TILESET_JSON_CACHE_MAX_BYTES)
        .time_to_idle(TILESET_JSON_CACHE_IDLE_TTL)
        .time_to_live(TILESET_JSON_CACHE_MAX_AGE)
        .weigher(|_key: &TilesetCacheKey, tilejson: &Arc<str>| {
            tilejson.len().clamp(1, u32::MAX as usize) as u32
        })
        .build()
}

fn error_cache<K>() -> Cache<K, ProfilePreparationError>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    Cache::builder()
        .max_capacity(JSON_NEGATIVE_CACHE_MAX_ENTRIES)
        .time_to_live(JSON_NEGATIVE_CACHE_TTL)
        .build()
}

/// A failed style or TileJSON fetch with its caching and sharing policy.
///
/// Permanent/content failures (4xx, parse, oversize, bad encoding, unknown
/// resource) reproduce on an immediate retry, so caching them briefly is the
/// §7.5 spray defense. Transient attempt-wide failures (5xx, connection/read
/// errors) are shared with current followers but not cached. Caller-local
/// deadline failures are neither cached nor shared because another caller may
/// still have enough budget to retry successfully.
#[derive(Clone)]
pub(super) struct ProfileFetchError {
    error: ProfilePreparationError,
    negative_cacheable: bool,
    scope: ProfileFetchFailureScope,
}

#[derive(Clone, Copy)]
enum ProfileFetchFailureScope {
    Attempt,
    Caller,
}

/// A style-availability error for `style_id` with a formatted reason.
pub(super) fn style_load_failed(
    style_id: &StyleId,
    source: impl Into<String>,
) -> ProfilePreparationError {
    ProfilePreparationError::style_unavailable(style_id, source)
}

/// Re-label a tileset/addlayer resolution error as a *source* failure. Shared
/// JSON helpers initially operate in style context, but a failed user-provided
/// addlayer source belongs to `SourceUnavailable`. Caller deadlines and
/// infrastructure failures remain unchanged.
fn source_unavailable_from(
    err: ProfilePreparationError,
    source_hash: SourceHash,
) -> ProfilePreparationError {
    err.into_source(source_hash)
}

impl ProfileFetchError {
    pub(super) fn permanent(error: ProfilePreparationError) -> Self {
        Self {
            error,
            negative_cacheable: true,
            scope: ProfileFetchFailureScope::Attempt,
        }
    }

    pub(super) fn transient(error: ProfilePreparationError) -> Self {
        Self {
            error,
            negative_cacheable: false,
            scope: ProfileFetchFailureScope::Attempt,
        }
    }

    pub(super) fn caller_deadline() -> Self {
        Self {
            error: ProfilePreparationError::CallerDeadlineExceeded,
            negative_cacheable: false,
            scope: ProfileFetchFailureScope::Caller,
        }
    }

    fn is_attempt_wide(&self) -> bool {
        matches!(self.scope, ProfileFetchFailureScope::Attempt)
    }

    /// Retryable style availability failure for `style_id`.
    pub(super) fn transient_load(style_id: &StyleId, source: impl Into<String>) -> Self {
        Self::transient(style_load_failed(style_id, source))
    }

    /// Negative-cacheable invalid style/source content in the current context.
    pub(super) fn permanent_invalid(style_id: &StyleId, source: impl Into<String>) -> Self {
        Self::permanent(ProfilePreparationError::invalid_style(style_id, source))
    }

    fn into_availability_error(self) -> StyleAvailabilityError {
        if self.negative_cacheable {
            StyleAvailabilityError::NotFound(self.error)
        } else {
            StyleAvailabilityError::Unavailable(self.error)
        }
    }

    #[cfg(test)]
    pub(super) fn error(&self) -> &ProfilePreparationError {
        &self.error
    }

    #[cfg(test)]
    pub(super) fn is_negative_cacheable(&self) -> bool {
        self.negative_cacheable
    }
}

#[async_trait]
impl ProfilePreparer for MapLibreProfilePreparer {
    async fn prepare_profile(
        &self,
        task: &InternalTask,
    ) -> Result<Option<PreparedProfile>, ProfilePreparationError> {
        let mut prepared = self
            .resolve_style(&task.style, task.authorization.as_ref(), task.deadline)
            .await?;
        prepared.addlayer_source = self.resolve_addlayer_source(task).await?;
        Ok(Some(prepared))
    }

    async fn ensure_style_available(
        &self,
        revision: &StyleRevision,
        deadline: Instant,
    ) -> Result<(), StyleAvailabilityError> {
        // Reuses the cache / single-flight / negative-cache path; the fetched
        // bytes are dropped — we only need to know the provider has the style.
        self.resolve_style_fetch(revision, None, deadline)
            .await
            .map(|_| ())
            .map_err(ProfileFetchError::into_availability_error)
    }

    fn mark_style_load_failed(
        &self,
        revision: &StyleRevision,
        authorization: Option<&RenderAuthorization>,
    ) {
        // A provider may repair invalid style JSON without changing the lazy
        // template revision. Do not keep feeding MLN the rejected positive
        // cache entry after the short negative-cache window expires.
        let key = StyleCacheKey {
            revision: revision.clone(),
            credential: credential_partition(authorization),
        };
        self.style_json_cache.invalidate(&key);
        self.style_error_cache.insert(
            key,
            style_load_failed(&revision.id, "MapLibre rejected the prepared style"),
        );
    }
}

#[cfg(test)]
impl MapLibreProfilePreparer {
    pub(super) fn for_tests(style_catalog: Arc<StyleCatalog>) -> Self {
        Self {
            style_catalog,
            http_client: reqwest::Client::new(),
            url_policy: mmpf_mln_filesource::policy::ResourceUrlPolicy::new(vec![
                "127.0.0.1".to_owned(),
                "localhost".to_owned(),
            ]),
            auth_provider_origin: None,
            fetch_permits: Arc::new(tokio::sync::Semaphore::new(16)),
            style_json_cache: style_json_cache(),
            style_error_cache: error_cache(),
            tileset_json_cache: tileset_json_cache(),
            tileset_error_cache: error_cache(),
            inflight_style_loads: SingleFlight::default(),
            inflight_tileset_loads: SingleFlight::default(),
        }
    }

    pub(super) fn has_cached_style(&self, revision: &StyleRevision) -> bool {
        self.style_json_cache.contains_key(&StyleCacheKey {
            revision: revision.clone(),
            credential: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use biei_core::types::FailureKind;

    #[test]
    fn addlayer_source_failure_reports_as_source_not_style() {
        let style_err = style_load_failed(&StyleId("carto/voyager".to_string()), "tileset GET 404");
        let converted = source_unavailable_from(style_err, 42);
        assert!(matches!(
            converted,
            ProfilePreparationError::SourceUnavailable { hash: 42, .. }
        ));
        assert_eq!(converted.failure_kind(), FailureKind::SourceUnavailable);

        assert!(matches!(
            source_unavailable_from(ProfilePreparationError::CallerDeadlineExceeded, 42),
            ProfilePreparationError::CallerDeadlineExceeded
        ));
    }
}
