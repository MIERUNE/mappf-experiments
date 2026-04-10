//! Shared bounded upstream fetch helpers for provider resources.

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::http::StatusCode;
use bytes::Bytes;
use moka::sync::Cache;
use object_store::{Attribute, Error as ObjectStoreError, ObjectStoreExt};
use tokio::sync::watch;
use url::Url;

use crate::server::{AppState, HttpError};
use crate::storage::ObjectStoreRegistry;

const PROVIDER_RESOURCE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const STYLE_POSITIVE_TTL: Duration = Duration::from_secs(300);
const GLYPH_SPRITE_POSITIVE_TTL: Duration = Duration::from_secs(86400);
const NEGATIVE_TTL: Duration = Duration::from_secs(30);
/// Bounded so a slow or hung upstream cannot pin request tasks indefinitely
/// (mirrors the tile backend fetch timeout).
const PROVIDER_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub(crate) struct ProviderFetchCache {
    entries: Cache<ProviderFetchCacheKey, CachedProviderFetch>,
    inflight: Arc<Mutex<HashMap<ProviderFetchCacheKey, watch::Sender<bool>>>>,
}

impl ProviderFetchCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(PROVIDER_RESOURCE_CACHE_MAX_BYTES)
                .weigher(provider_fetch_cache_weight)
                .build(),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get(&self, key: &ProviderFetchCacheKey) -> Option<CachedProviderFetch> {
        let entry = self.entries.get(key)?;
        if entry.is_fresh() {
            Some(entry)
        } else {
            self.entries.invalidate(key);
            None
        }
    }

    fn put_found(&self, key: ProviderFetchCacheKey, bytes: Bytes) {
        self.entries.insert(
            key.clone(),
            CachedProviderFetch::Found {
                bytes,
                expires_at: Instant::now() + positive_ttl(key.resource),
            },
        );
    }

    fn put_not_found(&self, key: ProviderFetchCacheKey) {
        self.entries.insert(
            key,
            CachedProviderFetch::NotFound {
                expires_at: Instant::now() + NEGATIVE_TTL,
            },
        );
    }

    fn begin_fetch(&self, key: ProviderFetchCacheKey) -> FetchFlight {
        let mut inflight = self
            .inflight
            .lock()
            .expect("provider fetch inflight mutex poisoned");
        if let Some(sender) = inflight.get(&key) {
            return FetchFlight::Follower(sender.subscribe());
        }

        let (sender, _) = watch::channel(false);
        inflight.insert(key.clone(), sender);
        FetchFlight::Leader(FetchLeaderGuard {
            inflight: Arc::clone(&self.inflight),
            key,
        })
    }

    pub(crate) fn weighted_size(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.weighted_size()
    }
}

enum FetchFlight {
    Leader(FetchLeaderGuard),
    Follower(watch::Receiver<bool>),
}

struct FetchLeaderGuard {
    inflight: Arc<Mutex<HashMap<ProviderFetchCacheKey, watch::Sender<bool>>>>,
    key: ProviderFetchCacheKey,
}

impl Drop for FetchLeaderGuard {
    fn drop(&mut self) {
        let sender = self
            .inflight
            .lock()
            .expect("provider fetch inflight mutex poisoned")
            .remove(&self.key);
        if let Some(sender) = sender {
            let _ = sender.send(true);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ProviderFetchCacheKey {
    resource: &'static str,
    validation: Arc<str>,
    url: Arc<str>,
}

impl ProviderFetchCacheKey {
    fn new(resource: &'static str, url: &str, accepted_content_types: &[&str]) -> Self {
        Self {
            resource,
            validation: Arc::from(validation_key(accepted_content_types)),
            url: Arc::from(url),
        }
    }
}

#[derive(Clone)]
enum CachedProviderFetch {
    Found { bytes: Bytes, expires_at: Instant },
    NotFound { expires_at: Instant },
}

impl CachedProviderFetch {
    fn is_fresh(&self) -> bool {
        match self {
            Self::Found { expires_at, .. } | Self::NotFound { expires_at } => {
                Instant::now() < *expires_at
            }
        }
    }

    fn into_result(self) -> Result<Bytes, HttpError> {
        match self {
            Self::Found { bytes, .. } => Ok(bytes),
            Self::NotFound { .. } => Err((StatusCode::NOT_FOUND, "not found".to_string())),
        }
    }
}

pub(crate) async fn fetch_limited_bytes(
    state: &AppState,
    url: String,
    max_bytes: usize,
    resource: &'static str,
) -> Result<Bytes, HttpError> {
    fetch_limited_bytes_with_content_type(state, url, max_bytes, resource, &[]).await
}

pub(crate) async fn fetch_limited_bytes_with_content_type(
    state: &AppState,
    url: String,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
) -> Result<Bytes, HttpError> {
    let key = ProviderFetchCacheKey::new(resource, &url, accepted_content_types);
    loop {
        if let Some(entry) = state.provider_fetch_cache.get(&key) {
            return entry.into_result();
        }

        match state.provider_fetch_cache.begin_fetch(key.clone()) {
            FetchFlight::Leader(_guard) => {
                let result = fetch_limited_bytes_uncached(
                    &state.object_store_registry,
                    &url,
                    max_bytes,
                    resource,
                    accepted_content_types,
                )
                .await;
                match &result {
                    Ok(bytes) => state
                        .provider_fetch_cache
                        .put_found(key.clone(), bytes.clone()),
                    Err((StatusCode::NOT_FOUND, _)) => {
                        state.provider_fetch_cache.put_not_found(key.clone());
                    }
                    Err(_) => {}
                }
                return result;
            }
            FetchFlight::Follower(mut receiver) => {
                let _ = receiver.changed().await;
            }
        }
    }
}

async fn fetch_limited_bytes_uncached(
    registry: &ObjectStoreRegistry,
    url: &str,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
) -> Result<Bytes, HttpError> {
    let parsed = Url::parse(url).map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("{resource} upstream URL invalid: {error}"),
        )
    })?;
    // Same object_store backend as tile reads: `gs://` (and `s3://`) authenticate
    // with the ambient credentials (Workload Identity on GKE), while `http(s)://`
    // stays anonymous. The registry reuses one store (connection pool + cached
    // credentials) per bucket/host instead of rebuilding it per fetch.
    let (store, path) = registry.resolve(&parsed).map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("{resource} upstream store init failed: {error}"),
        )
    })?;

    let result = tokio::time::timeout(PROVIDER_FETCH_TIMEOUT, store.get(&path))
        .await
        .map_err(|_| {
            (
                StatusCode::GATEWAY_TIMEOUT,
                format!("{resource} upstream timed out"),
            )
        })?
        .map_err(|error| match error {
            ObjectStoreError::NotFound { .. } => (StatusCode::NOT_FOUND, "not found".to_string()),
            other => (
                StatusCode::BAD_GATEWAY,
                format!("upstream GET failed: {other}"),
            ),
        })?;

    if result.meta.size > max_bytes as u64 {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("{resource} body too large"),
        ));
    }

    let content_type = result
        .attributes
        .get(&Attribute::ContentType)
        .map(|value| value.as_ref().to_string());
    validate_content_type(content_type.as_deref(), accepted_content_types, resource)?;

    let body = tokio::time::timeout(PROVIDER_FETCH_TIMEOUT, result.bytes())
        .await
        .map_err(|_| {
            (
                StatusCode::GATEWAY_TIMEOUT,
                format!("{resource} upstream timed out"),
            )
        })?
        .map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("upstream body failed: {error}"),
            )
        })?;

    if body.len() > max_bytes {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("{resource} body too large"),
        ));
    }
    Ok(body)
}

fn positive_ttl(resource: &'static str) -> Duration {
    match resource {
        "glyph" | "sprite" => GLYPH_SPRITE_POSITIVE_TTL,
        _ => STYLE_POSITIVE_TTL,
    }
}

fn provider_fetch_cache_weight(key: &ProviderFetchCacheKey, value: &CachedProviderFetch) -> u32 {
    let value_size = match value {
        CachedProviderFetch::Found { bytes, .. } => bytes.len(),
        CachedProviderFetch::NotFound { .. } => 0,
    };
    let total = std::mem::size_of_val(key)
        .saturating_add(key.url.len())
        .saturating_add(key.validation.len())
        .saturating_add(value_size);
    total.min(u32::MAX as usize) as u32
}

fn validation_key(accepted_content_types: &[&str]) -> String {
    if accepted_content_types.is_empty() {
        return "*".to_string();
    }
    accepted_content_types.join("|")
}

fn validate_content_type(
    content_type: Option<&str>,
    accepted_content_types: &[&str],
    resource: &'static str,
) -> Result<(), HttpError> {
    if accepted_content_types.is_empty() {
        return Ok(());
    }
    // No content-type from the backend (some object stores omit it): accept, the
    // resource handler still pins the response content-type itself.
    let Some(content_type) = content_type else {
        return Ok(());
    };
    if content_type_matches(content_type, accepted_content_types) {
        return Ok(());
    }
    Err((
        StatusCode::BAD_GATEWAY,
        format!("{resource} upstream content-type unsupported: {content_type}"),
    ))
}

fn content_type_matches(value: &str, accepted: &[&str]) -> bool {
    let media_type = value
        .split_once(';')
        .map_or(value, |(media_type, _)| media_type)
        .trim();
    accepted
        .iter()
        .any(|candidate| media_type.eq_ignore_ascii_case(candidate))
}

#[cfg(test)]
mod tests {
    use super::{
        FetchFlight, ProviderFetchCache, ProviderFetchCacheKey, content_type_matches, positive_ttl,
    };
    use std::time::Duration;

    #[test]
    fn content_type_match_ignores_parameters_and_case() {
        assert!(content_type_matches(
            "Application/JSON; charset=utf-8",
            &["application/json"]
        ));
        assert!(content_type_matches(
            "application/octet-stream",
            &["image/png", "application/octet-stream"]
        ));
        assert!(!content_type_matches("text/html", &["application/json"]));
    }

    #[test]
    fn provider_cache_uses_longer_ttl_for_heavy_resources() {
        assert_eq!(positive_ttl("style"), Duration::from_secs(300));
        assert_eq!(positive_ttl("glyph"), Duration::from_secs(86400));
        assert_eq!(positive_ttl("sprite"), Duration::from_secs(86400));
    }

    #[test]
    fn provider_cache_key_includes_validation_class() {
        let png =
            ProviderFetchCacheKey::new("sprite", "https://assets.example/sprite", &["image/png"]);
        let json = ProviderFetchCacheKey::new(
            "sprite",
            "https://assets.example/sprite",
            &["application/json"],
        );

        assert_ne!(png, json);
    }

    #[test]
    fn provider_fetch_leader_guard_cleans_inflight_on_drop() {
        let cache = ProviderFetchCache::new();
        let key = ProviderFetchCacheKey::new("style", "https://styles.example/base", &[]);

        let leader = match cache.begin_fetch(key.clone()) {
            FetchFlight::Leader(guard) => guard,
            FetchFlight::Follower(_) => panic!("first fetch should lead"),
        };
        assert_eq!(
            cache
                .inflight
                .lock()
                .expect("provider fetch inflight mutex poisoned")
                .len(),
            1
        );

        drop(leader);

        assert_eq!(
            cache
                .inflight
                .lock()
                .expect("provider fetch inflight mutex poisoned")
                .len(),
            0
        );
        match cache.begin_fetch(key) {
            FetchFlight::Leader(_) => {}
            FetchFlight::Follower(_) => panic!("new fetch should lead after guard drop"),
        }
    }

    #[tokio::test]
    async fn follower_is_woken_and_slot_freed_when_leader_is_cancelled() {
        let cache = ProviderFetchCache::new();
        let key = ProviderFetchCacheKey::new("style", "https://styles.example/base", &[]);

        let leader = match cache.begin_fetch(key.clone()) {
            FetchFlight::Leader(guard) => guard,
            FetchFlight::Follower(_) => panic!("first fetch should lead"),
        };
        let mut follower = match cache.begin_fetch(key.clone()) {
            FetchFlight::Follower(receiver) => receiver,
            FetchFlight::Leader(_) => panic!("second fetch should follow"),
        };

        // Simulate the leader's request task being cancelled: the guard is
        // dropped without the cache being populated.
        drop(leader);

        // The follower must be woken, not left hanging.
        let changed = tokio::time::timeout(Duration::from_secs(1), follower.changed())
            .await
            .expect("follower should be woken when the leader is cancelled");
        assert!(
            changed.is_ok(),
            "leader cancellation should notify followers before the channel closes"
        );

        // The inflight slot is freed, so the next request re-leads and retries.
        match cache.begin_fetch(key) {
            FetchFlight::Leader(_) => {}
            FetchFlight::Follower(_) => {
                panic!("slot should be free to re-lead after cancellation")
            }
        }
    }
}
