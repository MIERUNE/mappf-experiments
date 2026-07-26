//! Provider representation cache: freshness evaluation, single-flight, and
//! background stale revalidation.
//!
//! Holds the decision of *whether to go to origin*; `fetch` performs the
//! transport and `resource` owns the value that results.

use std::{
    sync::{Arc, atomic::AtomicUsize},
    time::{Duration, Instant},
};

use axum::http::StatusCode;
use bytes::Bytes;
use moka::sync::Cache;
use reqwest::Client;
use tokio::{sync::Semaphore, time::Instant as TokioInstant};

use super::fetch::{fetch_limited_bytes_uncached, provider_fetch_cache_weight};
use super::{
    CachedProviderRepresentation, FetchedProviderNegative, FetchedProviderResource,
    PROVIDER_FETCH_CONCURRENCY, PROVIDER_FETCH_MAX_INFLIGHT,
    PROVIDER_STALE_REVALIDATION_FAILURE_COOLDOWN, PROVIDER_STALE_REVALIDATION_FAILURE_MAX_KEYS,
    ProviderFetchPermit, ProviderFetchSlot, ProviderFlightOutcome, ProviderOriginOutcome,
    ProviderResource, provider_http_client, provider_negative_error,
};
use crate::server::provider_body::BodyValidation;
use crate::server::{HttpError, conditional::Validators};
use ishikari_core::metrics::NodeMetrics;
use ishikari_core::storage::ObjectStoreRegistry;
use mmpf_common::singleflight::{Flight, LeaderGuard, SingleFlight};

#[derive(Clone)]
pub(super) struct ProviderFetchCache {
    entries: Cache<ProviderFetchCacheKey, CachedProviderFetch>,
    failed_revalidations: Cache<ProviderFetchCacheKey, TokioInstant>,
    inflight: SingleFlight<ProviderFetchCacheKey, ProviderFlightOutcome>,
    pub(super) http_client: Client,
    fetch_semaphore: Arc<Semaphore>,
    fetch_inflight: Arc<AtomicUsize>,
}

impl ProviderFetchCache {
    fn new(max_capacity_bytes: u64) -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(max_capacity_bytes)
                .weigher(provider_fetch_cache_weight)
                .build(),
            failed_revalidations: Cache::builder()
                .max_capacity(PROVIDER_STALE_REVALIDATION_FAILURE_MAX_KEYS)
                .build(),
            inflight: SingleFlight::default(),
            http_client: provider_http_client(),
            fetch_semaphore: Arc::new(Semaphore::new(PROVIDER_FETCH_CONCURRENCY)),
            fetch_inflight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns the cached entry with its freshness. Fully expired entries are
    /// reported as a miss but are not invalidated here: invalidating a cloned
    /// observation could race a concurrent successful replacement and delete
    /// the new value. The byte-bounded cache may retain the unreachable expired
    /// value until replacement or eviction.
    fn get(&self, key: &ProviderFetchCacheKey) -> Option<(CachedProviderFetch, Freshness)> {
        let entry = self.entries.get(key)?;
        match entry.freshness() {
            Freshness::Expired => None,
            freshness => Some((entry, freshness)),
        }
    }

    fn stale_representation(
        &self,
        key: &ProviderFetchCacheKey,
    ) -> Option<CachedProviderRepresentation> {
        let (entry, Freshness::Stale) = self.get(key)? else {
            return None;
        };
        entry.representation()
    }

    fn put_found(&self, key: ProviderFetchCacheKey, fetched: &FetchedProviderResource) -> bool {
        // Modified and 304 responses both arrive here. Either is a successful
        // revalidation and resets any prior failed-refresh cooldown.
        self.failed_revalidations.invalidate(&key);
        if !fetched.policy.store {
            // A successful refresh can tighten an existing stale entry to
            // `no-store`/`private`/`no-cache`. Remove that old body promptly.
            self.invalidate(&key);
            return false;
        }
        let stored_at = Instant::now();
        let fresh_remaining = fetched.policy.fresh.saturating_sub(fetched.initial_age);
        let retention_remaining = fetched
            .policy
            .fresh
            .saturating_add(fetched.policy.swr)
            .saturating_sub(fetched.initial_age);
        if retention_remaining.is_zero() {
            self.invalidate(&key);
            return false;
        }
        let fresh_until = stored_at + fresh_remaining;
        self.entries.insert(
            key,
            CachedProviderFetch::Found {
                bytes: fetched.bytes.clone(),
                cache_control: Arc::clone(&fetched.policy.response_cache_control),
                validators: fetched.validators.clone(),
                content_encoding: fetched.content_encoding.clone(),
                age_at_insert: fetched.initial_age,
                stored_at,
                fresh_until,
                stale_until: stored_at + retention_remaining,
            },
        );
        true
    }

    fn put_negative(&self, key: ProviderFetchCacheKey, negative: &FetchedProviderNegative) -> bool {
        // A terminal origin response is also a successful revalidation attempt.
        self.failed_revalidations.invalidate(&key);
        if !negative.policy.store {
            self.invalidate(&key);
            return false;
        }
        let fresh = negative.policy.fresh.saturating_sub(negative.initial_age);
        if fresh.is_zero() {
            self.invalidate(&key);
            return false;
        }
        let fresh_until = Instant::now() + fresh;
        self.entries.insert(
            key,
            CachedProviderFetch::Negative {
                status: negative.status,
                fresh_until,
                stale_until: fresh_until,
            },
        );
        true
    }

    fn begin_fetch(
        &self,
        key: ProviderFetchCacheKey,
    ) -> Flight<ProviderFetchCacheKey, ProviderFlightOutcome> {
        self.inflight.begin(key)
    }

    /// Whether a stale hit may attempt background revalidation. This is never
    /// consulted by the blocking miss path, so the cooldown cannot extend stale
    /// serving or suppress a fetch after `stale_until`.
    fn stale_revalidation_allowed(&self, key: &ProviderFetchCacheKey) -> bool {
        let Some(retry_at) = self.failed_revalidations.get(key) else {
            return true;
        };
        if TokioInstant::now() < retry_at {
            return false;
        }
        self.failed_revalidations.invalidate(key);
        true
    }

    fn mark_stale_revalidation_failure(&self, key: &ProviderFetchCacheKey) {
        match self.entries.get(key).map(|entry| entry.freshness()) {
            Some(Freshness::Stale) => self.failed_revalidations.insert(
                key.clone(),
                TokioInstant::now() + PROVIDER_STALE_REVALIDATION_FAILURE_COOLDOWN,
            ),
            Some(Freshness::Expired) => self.invalidate(key),
            Some(Freshness::Fresh) | None => self.failed_revalidations.invalidate(key),
        }
    }

    fn invalidate(&self, key: &ProviderFetchCacheKey) {
        self.entries.invalidate(key);
        self.failed_revalidations.invalidate(key);
    }

    pub(super) async fn admit_fetch(
        &self,
        resource: &'static str,
    ) -> Result<ProviderFetchPermit, HttpError> {
        let slot =
            ProviderFetchSlot::try_reserve(&self.fetch_inflight, PROVIDER_FETCH_MAX_INFLIGHT)
                .ok_or_else(|| {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("{resource} upstream fetch queue full"),
                    )
                })?;
        let permit = Arc::clone(&self.fetch_semaphore)
            .acquire_owned()
            .await
            .map_err(|_| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("{resource} upstream fetch unavailable"),
                )
            })?;
        Ok(ProviderFetchPermit {
            _permit: permit,
            _slot: slot,
        })
    }

    fn weighted_size(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.weighted_size()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct ProviderFetchCacheKey {
    resource: &'static str,
    accepted_content_types: &'static [&'static str],
    body_validation: BodyValidation,
    pub(super) url: Arc<str>,
}

impl ProviderFetchCacheKey {
    fn new(
        resource: &'static str,
        url: impl Into<Arc<str>>,
        accepted_content_types: &'static [&'static str],
        body_validation: BodyValidation,
    ) -> Self {
        Self {
            resource,
            accepted_content_types,
            body_validation,
            url: url.into(),
        }
    }
}

/// Freshness of a cached entry relative to its window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Freshness {
    /// Serve directly.
    Fresh,
    /// Past `fresh_until` but within the SWR window: serve, revalidate in the
    /// background. Only reachable for `Found` (negative entries have no SWR).
    Stale,
    /// Past the SWR window: treat as a miss.
    Expired,
}

#[derive(Clone)]
pub(super) enum CachedProviderFetch {
    Found {
        bytes: Bytes,
        cache_control: Arc<str>,
        validators: Validators,
        content_encoding: Option<Arc<str>>,
        age_at_insert: Duration,
        stored_at: Instant,
        fresh_until: Instant,
        stale_until: Instant,
    },
    Negative {
        status: StatusCode,
        fresh_until: Instant,
        stale_until: Instant,
    },
}

impl CachedProviderFetch {
    fn freshness(&self) -> Freshness {
        let (fresh_until, stale_until) = match self {
            Self::Found {
                fresh_until,
                stale_until,
                ..
            }
            | Self::Negative {
                fresh_until,
                stale_until,
                ..
            } => (fresh_until, stale_until),
        };
        let now = Instant::now();
        if now < *fresh_until {
            Freshness::Fresh
        } else if now < *stale_until {
            Freshness::Stale
        } else {
            Freshness::Expired
        }
    }

    fn into_result(self) -> Result<ProviderResource, HttpError> {
        match self {
            Self::Found {
                bytes,
                cache_control,
                validators,
                content_encoding,
                age_at_insert,
                stored_at,
                ..
            } => Ok(ProviderResource::from_cached(
                bytes,
                cache_control,
                validators,
                content_encoding,
                Instant::now()
                    .saturating_duration_since(stored_at)
                    .saturating_add(age_at_insert)
                    .as_secs(),
            )),
            Self::Negative { status, .. } => Err(provider_negative_error(status)),
        }
    }

    fn cache_outcome(&self, freshness: Freshness) -> &'static str {
        match (self, freshness) {
            (Self::Found { .. }, Freshness::Stale) => "stale_hit",
            (Self::Found { .. }, _) => "hit",
            (Self::Negative { .. }, _) => "negative_hit",
        }
    }

    fn representation(&self) -> Option<CachedProviderRepresentation> {
        match self {
            Self::Found {
                bytes,
                cache_control,
                validators,
                content_encoding,
                ..
            } => Some(CachedProviderRepresentation {
                bytes: bytes.clone(),
                cache_control: Arc::clone(cache_control),
                validators: validators.clone(),
                content_encoding: content_encoding.clone(),
            }),
            Self::Negative { .. } => None,
        }
    }
}

fn record_cached_provider_fetch(
    metrics: &NodeMetrics,
    resource: &'static str,
    entry: &CachedProviderFetch,
    freshness: Freshness,
    joined_singleflight: bool,
) {
    if !joined_singleflight {
        metrics.record_provider_resource_cache(resource, entry.cache_outcome(freshness));
    }
}

/// Best-effort background revalidation of a stale-but-serveable entry. The
/// single-flight election makes only one refresh run per key; concurrent stale
/// hits return the prior body immediately without stacking backend reads. The
/// entry is checked again after leader election so a delayed stale observation
/// cannot revalidate a newer fresh replacement.
fn spawn_stale_revalidation(
    fetcher: &ProviderFetcher,
    key: ProviderFetchCacheKey,
    url: Arc<str>,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &'static [&'static str],
    body_validation: BodyValidation,
) {
    if !fetcher.cache.stale_revalidation_allowed(&key) {
        return;
    }
    let Flight::Leader(guard) = fetcher.cache.begin_fetch(key.clone()) else {
        // A refresh (or a blocking fetch) is already in flight for this key.
        return;
    };
    if !fetcher.cache.stale_revalidation_allowed(&key) {
        drop(guard);
        return;
    }
    let Some(stale) = fetcher.cache.stale_representation(&key) else {
        drop(guard);
        return;
    };
    let fetcher = fetcher.clone();
    tokio::spawn(async move {
        let result = fetch_limited_bytes_uncached(
            &fetcher,
            &url,
            max_bytes,
            resource,
            accepted_content_types,
            body_validation,
            Some(&stale),
        )
        .await;
        // Install failure state before completing the flight so the next stale
        // hit cannot race the guard release and immediately retry.
        if result.is_err() {
            fetcher.cache.mark_stale_revalidation_failure(&key);
        }
        // The refreshed body (or error) reaches later requests through the cache
        // and the single-flight guard; this task only drives the revalidation.
        let _ = store_leader_result(&fetcher, &key, resource, result, guard);
    });
}

/// Applies a leader (foreground or background) fetch outcome to the cache,
/// records the insert metric, and shares a transient error with followers.
fn store_leader_result(
    fetcher: &ProviderFetcher,
    key: &ProviderFetchCacheKey,
    resource: &'static str,
    result: Result<ProviderOriginOutcome, HttpError>,
    guard: LeaderGuard<ProviderFetchCacheKey, ProviderFlightOutcome>,
) -> Result<ProviderResource, HttpError> {
    match result {
        Ok(ProviderOriginOutcome::Negative(negative)) => {
            let error = provider_negative_error(negative.status);
            let stored = fetcher.cache.put_negative(key.clone(), &negative);
            fetcher.metrics.record_provider_resource_cache(
                resource,
                if stored {
                    "negative_insert"
                } else {
                    "negative_uncacheable"
                },
            );
            guard.complete_with_error(ProviderFlightOutcome::Error(error.clone()));
            Err(error)
        }
        Ok(origin) => {
            let (fetched, stored_outcome) = match origin {
                ProviderOriginOutcome::Modified(fetched) => (fetched, "insert"),
                ProviderOriginOutcome::NotModified(fetched) => (fetched, "revalidated"),
                ProviderOriginOutcome::Negative(_) => unreachable!("handled above"),
            };
            let response = ProviderResource::fetched(&fetched);
            let stored = fetcher.cache.put_found(key.clone(), &fetched);
            // An uncacheable response was fetched successfully but intentionally
            // not retained. This can also happen when a 304 tightens policy.
            let outcome = if stored {
                stored_outcome
            } else {
                "uncacheable"
            };
            fetcher
                .metrics
                .record_provider_resource_cache(resource, outcome);
            guard.complete_with(ProviderFlightOutcome::Resource(response.clone()));
            Ok(response)
        }
        Err(error) => {
            fetcher
                .metrics
                .record_provider_resource_cache(resource, "error");
            guard.complete_with_error(ProviderFlightOutcome::Error(error.clone()));
            Err(error)
        }
    }
}

/// Owns Ishikari's local provider-fetch capability: cache and single-flight
/// state, admission, provider metrics, and shared object-store clients.
#[derive(Clone)]
pub(crate) struct ProviderFetcher {
    pub(super) cache: ProviderFetchCache,
    metrics: NodeMetrics,
    pub(super) object_store_registry: Arc<ObjectStoreRegistry>,
}

impl ProviderFetcher {
    pub(crate) fn new(
        metrics: NodeMetrics,
        object_store_registry: Arc<ObjectStoreRegistry>,
        cache_max_bytes: u64,
    ) -> Self {
        Self {
            cache: ProviderFetchCache::new(cache_max_bytes),
            metrics,
            object_store_registry,
        }
    }

    pub(crate) async fn fetch_bytes(
        &self,
        url: String,
        max_bytes: usize,
        resource: &'static str,
        accepted_content_types: &'static [&'static str],
    ) -> Result<ProviderResource, HttpError> {
        fetch_limited_bytes_with_validation(
            self,
            url,
            max_bytes,
            resource,
            accepted_content_types,
            BodyValidation::Bytes,
        )
        .await
    }

    pub(crate) async fn fetch_json(
        &self,
        url: String,
        max_bytes: usize,
        resource: &'static str,
        accepted_content_types: &'static [&'static str],
    ) -> Result<ProviderResource, HttpError> {
        fetch_limited_bytes_with_validation(
            self,
            url,
            max_bytes,
            resource,
            accepted_content_types,
            BodyValidation::Json,
        )
        .await
    }

    pub(crate) fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }
}

async fn fetch_limited_bytes_with_validation(
    fetcher: &ProviderFetcher,
    url: String,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &'static [&'static str],
    body_validation: BodyValidation,
) -> Result<ProviderResource, HttpError> {
    let url: Arc<str> = Arc::from(url);
    let key = ProviderFetchCacheKey::new(
        resource,
        Arc::clone(&url),
        accepted_content_types,
        body_validation,
    );
    let mut recorded_miss = false;
    let mut joined_singleflight = false;
    loop {
        if let Some((entry, freshness)) = fetcher.cache.get(&key) {
            // A follower already recorded the request as a miss plus a join.
            // Reading the leader's freshly inserted value is not an independent
            // cache hit and must not inflate cache-hit-ratio dashboards.
            record_cached_provider_fetch(
                &fetcher.metrics,
                resource,
                &entry,
                freshness,
                joined_singleflight,
            );
            if freshness == Freshness::Stale {
                spawn_stale_revalidation(
                    fetcher,
                    key.clone(),
                    Arc::clone(&url),
                    max_bytes,
                    resource,
                    accepted_content_types,
                    body_validation,
                );
            }
            return entry.into_result();
        }
        if !recorded_miss {
            fetcher
                .metrics
                .record_provider_resource_cache(resource, "miss");
            recorded_miss = true;
        }

        match fetcher.cache.begin_fetch(key.clone()) {
            Flight::Leader(guard) => {
                // Another leader may have installed a replacement after our
                // initial miss but before this election. Re-check under flight
                // ownership so an expired observation cannot trigger a serial
                // duplicate origin fetch.
                if fetcher.cache.get(&key).is_some() {
                    drop(guard);
                    continue;
                }
                let result = fetch_limited_bytes_uncached(
                    fetcher,
                    &url,
                    max_bytes,
                    resource,
                    accepted_content_types,
                    body_validation,
                    None,
                )
                .await;
                return store_leader_result(fetcher, &key, resource, result, guard);
            }
            Flight::Follower(follower) => {
                // Request-scoped: an uncacheable success stores nothing, so a
                // follower can wake, miss, and follow the next leader. Those
                // internal wait cycles are one joined request, not several.
                if !joined_singleflight {
                    fetcher
                        .metrics
                        .record_provider_resource_cache(resource, "singleflight_join");
                    joined_singleflight = true;
                }
                if let Some(outcome) = follower.wait().await {
                    return match outcome {
                        ProviderFlightOutcome::Error(error) => Err(error),
                        ProviderFlightOutcome::Resource(resource) => Ok(resource),
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::fetch::{
        corrected_initial_age, require_complete_provider_status, revalidated_provider_resource,
    };
    use super::{
        BodyValidation, CachedProviderFetch, FetchedProviderNegative, FetchedProviderResource,
        Freshness, PROVIDER_STALE_REVALIDATION_FAILURE_COOLDOWN, ProviderFetchCache,
        ProviderFetchCacheKey, ProviderFetchSlot, ProviderFetcher, ProviderFlightOutcome,
        ProviderOriginOutcome, Validators, record_cached_provider_fetch, store_leader_result,
    };
    use crate::server::provider_cache_policy::{NegativeCachePolicy, cache_policy};
    use axum::http::{HeaderMap, StatusCode, header};
    use bytes::Bytes;
    use ishikari_core::metrics::NodeMetrics;
    use ishikari_core::storage::ObjectStoreRegistry;
    use mmpf_common::singleflight::Flight;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime},
    };

    const TEST_PROVIDER_CACHE_MAX_BYTES: u64 = 1024 * 1024;

    #[test]
    fn only_complete_http_200_provider_representations_are_accepted() {
        assert!(require_complete_provider_status(StatusCode::OK, "glyph").is_ok());
        for status in [
            StatusCode::NO_CONTENT,
            StatusCode::PARTIAL_CONTENT,
            StatusCode::IM_USED,
        ] {
            let error = require_complete_provider_status(status, "glyph")
                .expect_err("non-200 success must not become a complete representation");
            assert_eq!(error.0, StatusCode::BAD_GATEWAY);
            assert!(error.1.contains(status.as_str()));
        }
    }

    fn stale_found(stale_for: Duration) -> CachedProviderFetch {
        let now = std::time::Instant::now();
        CachedProviderFetch::Found {
            bytes: Bytes::from_static(b"stale"),
            cache_control: "public, max-age=0, stale-while-revalidate=60".into(),
            validators: Validators::default(),
            content_encoding: None,
            age_at_insert: Duration::ZERO,
            stored_at: now,
            fresh_until: now,
            stale_until: now + stale_for,
        }
    }

    fn provider_key(url: &str) -> ProviderFetchCacheKey {
        ProviderFetchCacheKey::new("style", url, &["application/json"], BodyValidation::Json)
    }

    #[test]
    fn provider_fetch_slots_are_bounded_and_released_on_drop() {
        let inflight = Arc::new(AtomicUsize::new(0));
        let slot = ProviderFetchSlot::try_reserve(&inflight, 1).expect("first slot");
        assert!(ProviderFetchSlot::try_reserve(&inflight, 1).is_none());
        assert_eq!(inflight.load(Ordering::Relaxed), 1);
        drop(slot);
        assert!(ProviderFetchSlot::try_reserve(&inflight, 1).is_some());
    }

    #[test]
    fn corrected_age_uses_the_largest_origin_or_apparent_age() {
        let now = SystemTime::now();
        let mut headers = HeaderMap::new();
        headers.insert(header::AGE, "20".parse().unwrap());
        headers.insert(
            header::DATE,
            httpdate::fmt_http_date(now - Duration::from_secs(40))
                .parse()
                .unwrap(),
        );
        let age = corrected_initial_age(&headers, now, Duration::from_secs(5));
        assert!(age >= Duration::from_secs(40));
    }

    #[test]
    fn not_modified_reuses_body_and_refreshes_origin_metadata() {
        let cached = CachedProviderFetch::Found {
            bytes: Bytes::from_static(b"validated-style"),
            cache_control: "public, max-age=0, s-maxage=0, stale-while-revalidate=60".into(),
            validators: Validators::new(Some("\"v1\"".into()), None),
            content_encoding: Some("gzip".into()),
            age_at_insert: Duration::from_secs(40),
            stored_at: std::time::Instant::now(),
            fresh_until: std::time::Instant::now(),
            stale_until: std::time::Instant::now() + Duration::from_secs(60),
        }
        .representation()
        .expect("found representation");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CACHE_CONTROL,
            "public, max-age=120, stale-while-revalidate=30"
                .parse()
                .unwrap(),
        );
        headers.insert(header::ETAG, "\"v2\"".parse().unwrap());

        let refreshed = revalidated_provider_resource(
            &cached,
            "style",
            Some(&headers),
            Duration::from_millis(10),
        );

        assert_eq!(refreshed.bytes.as_ref(), b"validated-style");
        assert_eq!(refreshed.policy.fresh, Duration::from_secs(120));
        assert_eq!(refreshed.policy.swr, Duration::from_secs(30));
        assert_eq!(refreshed.validators.etag(), Some("\"v2\""));
        assert_eq!(refreshed.content_encoding.as_deref(), Some("gzip"));
        assert!(refreshed.initial_age < Duration::from_secs(1));

        headers.insert(header::CONTENT_ENCODING, "identity".parse().unwrap());
        let identity =
            revalidated_provider_resource(&cached, "style", Some(&headers), Duration::ZERO);
        assert_eq!(identity.content_encoding, None);
    }

    #[test]
    fn provider_cache_key_includes_validation_class() {
        let png = ProviderFetchCacheKey::new(
            "sprite",
            "https://assets.example/sprite",
            &["image/png"],
            BodyValidation::Bytes,
        );
        let json = ProviderFetchCacheKey::new(
            "sprite",
            "https://assets.example/sprite",
            &["application/json"],
            BodyValidation::Json,
        );

        assert_ne!(png, json);
    }

    #[test]
    fn singleflight_joiner_does_not_record_a_cache_hit() {
        let metrics = NodeMetrics::new();
        let stored_at = std::time::Instant::now();
        let fresh_until = stored_at + Duration::from_secs(60);
        let entry = CachedProviderFetch::Found {
            bytes: Bytes::from_static(b"style"),
            cache_control: "public, max-age=60".into(),
            validators: Validators::default(),
            content_encoding: None,
            age_at_insert: Duration::ZERO,
            stored_at,
            fresh_until,
            stale_until: fresh_until,
        };

        record_cached_provider_fetch(&metrics, "style", &entry, Freshness::Fresh, true);
        assert!(!metrics.encode().contains(
            "ishikari_provider_resource_cache_total{outcome=\"hit\",resource=\"style\"}"
        ));

        record_cached_provider_fetch(&metrics, "style", &entry, Freshness::Fresh, false);
        assert!(metrics.encode().contains(
            "ishikari_provider_resource_cache_total{outcome=\"hit\",resource=\"style\"} 1"
        ));
    }

    #[test]
    fn uncacheable_refresh_invalidates_an_existing_stale_body() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = ProviderFetchCacheKey::new(
            "style",
            "https://example/style.json",
            &[],
            BodyValidation::Json,
        );
        let old = FetchedProviderResource {
            bytes: Bytes::from_static(b"old"),
            policy: cache_policy("style", Some("max-age=60, stale-while-revalidate=600")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };
        cache.put_found(key.clone(), &old);
        assert!(cache.get(&key).is_some());

        let new = FetchedProviderResource {
            bytes: Bytes::from_static(b"new"),
            policy: cache_policy("style", Some("no-store")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };
        cache.put_found(key.clone(), &new);
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn negative_cache_preserves_status_and_origin_bypass() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = ProviderFetchCacheKey::new(
            "style",
            "https://example/missing.json",
            &["application/json"],
            BodyValidation::Json,
        );
        let gone = FetchedProviderNegative {
            status: StatusCode::GONE,
            policy: NegativeCachePolicy {
                store: true,
                fresh: Duration::from_secs(10),
            },
            initial_age: Duration::ZERO,
        };
        assert!(cache.put_negative(key.clone(), &gone));
        let result = cache.get(&key).expect("negative entry").0.into_result();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("negative entry unexpectedly returned a body"),
        };
        assert_eq!(error, (StatusCode::GONE, "gone".to_string()));

        let no_store = FetchedProviderNegative {
            status: StatusCode::NOT_FOUND,
            policy: NegativeCachePolicy {
                store: false,
                fresh: Duration::ZERO,
            },
            initial_age: Duration::ZERO,
        };
        assert!(!cache.put_negative(key.clone(), &no_store));
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn upstream_age_reduces_local_freshness_and_is_emitted() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = ProviderFetchCacheKey::new(
            "style",
            "https://example/aged-style.json",
            &["application/json"],
            BodyValidation::Json,
        );
        let fetched = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=60")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::from_secs(45),
        };
        assert!(cache.put_found(key.clone(), &fetched));
        let (entry, freshness) = cache.get(&key).expect("aged entry");
        assert_eq!(freshness, Freshness::Fresh);
        let resource = entry.into_result().expect("resource");
        assert!(resource.age_seconds() >= 45);

        let already_expired = FetchedProviderResource {
            initial_age: Duration::from_secs(60),
            ..fetched
        };
        assert!(!cache.put_found(key.clone(), &already_expired));
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn upstream_age_also_reduces_default_freshness() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = ProviderFetchCacheKey::new(
            "style",
            "https://example/defaulted-style.json",
            &["application/json"],
            BodyValidation::Json,
        );
        let fetched = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", None),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::from_secs(300),
        };

        assert!(!cache.put_found(key.clone(), &fetched));
        assert!(cache.get(&key).is_none());
    }

    #[tokio::test]
    async fn current_followers_receive_stored_result_after_immediate_cache_eviction() {
        let fetcher = ProviderFetcher::new(
            NodeMetrics::new(),
            Arc::new(ObjectStoreRegistry::without_options()),
            TEST_PROVIDER_CACHE_MAX_BYTES,
        );
        let key = provider_key("https://example/flight.json");
        let Flight::Leader(guard) = fetcher.cache.begin_fetch(key.clone()) else {
            panic!("first caller must lead");
        };
        let Flight::Follower(follower) = fetcher.cache.begin_fetch(key.clone()) else {
            panic!("second caller must follow");
        };
        let fetched = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=60")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };

        let leader_resource = store_leader_result(
            &fetcher,
            &key,
            "style",
            Ok(ProviderOriginOutcome::Modified(fetched)),
            guard,
        )
        .expect("leader response");
        fetcher.cache.invalidate(&key);

        let outcome = follower.wait().await.expect("published leader result");
        let ProviderFlightOutcome::Resource(follower_resource) = outcome else {
            panic!("follower must receive the leader representation");
        };
        assert_eq!(follower_resource.bytes(), leader_resource.bytes());
    }

    #[tokio::test(start_paused = true)]
    async fn failed_stale_revalidation_is_suppressed_until_cooldown_elapses() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = provider_key("https://example/stale.json");
        cache
            .entries
            .insert(key.clone(), stale_found(Duration::from_secs(60)));

        cache.mark_stale_revalidation_failure(&key);
        assert!(!cache.stale_revalidation_allowed(&key));

        tokio::time::advance(
            PROVIDER_STALE_REVALIDATION_FAILURE_COOLDOWN.saturating_sub(Duration::from_millis(1)),
        )
        .await;
        assert!(!cache.stale_revalidation_allowed(&key));

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(cache.stale_revalidation_allowed(&key));
        assert!(cache.failed_revalidations.get(&key).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn failed_revalidation_cooldowns_are_per_key_and_success_clears_them() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let first = provider_key("https://example/first.json");
        let second = provider_key("https://example/second.json");
        cache
            .entries
            .insert(first.clone(), stale_found(Duration::from_secs(60)));
        cache
            .entries
            .insert(second.clone(), stale_found(Duration::from_secs(60)));

        cache.mark_stale_revalidation_failure(&first);
        assert!(!cache.stale_revalidation_allowed(&first));
        assert!(cache.stale_revalidation_allowed(&second));

        cache.mark_stale_revalidation_failure(&second);
        let refreshed = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=60, stale-while-revalidate=60")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };
        // Modified and 304 outcomes share this successful insertion path.
        assert!(cache.put_found(first.clone(), &refreshed));
        assert!(cache.stale_revalidation_allowed(&first));
        assert!(!cache.stale_revalidation_allowed(&second));
    }

    #[tokio::test(start_paused = true)]
    async fn hard_expiry_does_not_suppress_blocking_fetch() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = provider_key("https://example/expired.json");
        cache
            .entries
            .insert(key.clone(), stale_found(Duration::from_secs(60)));
        cache.mark_stale_revalidation_failure(&key);
        assert!(!cache.stale_revalidation_allowed(&key));

        let now = std::time::Instant::now();
        let two_seconds_ago = now
            .checked_sub(Duration::from_secs(2))
            .expect("test instant supports a two-second lookback");
        let one_second_ago = now
            .checked_sub(Duration::from_secs(1))
            .expect("test instant supports a one-second lookback");
        cache.entries.insert(
            key.clone(),
            CachedProviderFetch::Found {
                bytes: Bytes::from_static(b"expired"),
                cache_control: "public, max-age=0, stale-while-revalidate=60".into(),
                validators: Validators::default(),
                content_encoding: None,
                age_at_insert: Duration::ZERO,
                stored_at: two_seconds_ago,
                fresh_until: two_seconds_ago,
                stale_until: one_second_ago,
            },
        );

        assert!(cache.get(&key).is_none());
        // Cooldown state is independently bounded and is intentionally not
        // consulted by the blocking miss path.
        assert!(cache.failed_revalidations.get(&key).is_some());
        assert!(matches!(cache.begin_fetch(key), Flight::Leader(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_stale_observation_cannot_revalidate_a_fresh_replacement() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = provider_key("https://example/replaced.json");
        cache
            .entries
            .insert(key.clone(), stale_found(Duration::from_secs(60)));
        assert!(cache.stale_representation(&key).is_some());

        let refreshed = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=60, stale-while-revalidate=60")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };
        assert!(cache.put_found(key.clone(), &refreshed));

        assert!(matches!(cache.get(&key), Some((_, Freshness::Fresh))));
        assert!(cache.stale_representation(&key).is_none());
    }

    #[test]
    fn stale_entry_reports_stale_then_expired() {
        let now = std::time::Instant::now();
        let three_seconds_ago = now
            .checked_sub(Duration::from_secs(3))
            .expect("test instant supports a three-second lookback");
        let two_seconds_ago = now
            .checked_sub(Duration::from_secs(2))
            .expect("test instant supports a two-second lookback");
        let one_second_ago = now
            .checked_sub(Duration::from_secs(1))
            .expect("test instant supports a one-second lookback");
        let entry = CachedProviderFetch::Found {
            bytes: Bytes::from_static(b"x"),
            cache_control: "public, max-age=60".into(),
            validators: Validators::default(),
            content_encoding: None,
            age_at_insert: Duration::ZERO,
            stored_at: two_seconds_ago,
            fresh_until: one_second_ago,
            stale_until: now + Duration::from_secs(60),
        };
        assert_eq!(entry.freshness(), Freshness::Stale);
        assert_eq!(entry.cache_outcome(Freshness::Stale), "stale_hit");

        let expired = CachedProviderFetch::Found {
            bytes: Bytes::from_static(b"x"),
            cache_control: "public, max-age=60".into(),
            validators: Validators::default(),
            content_encoding: None,
            age_at_insert: Duration::ZERO,
            stored_at: three_seconds_ago,
            fresh_until: two_seconds_ago,
            stale_until: one_second_ago,
        };
        assert_eq!(expired.freshness(), Freshness::Expired);
    }
}
