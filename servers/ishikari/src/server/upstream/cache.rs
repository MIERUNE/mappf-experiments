//! Provider representation cache: freshness evaluation, single-flight, and
//! background stale revalidation.
//!
//! Holds the decision of *whether to go to origin*; `fetch` performs the
//! transport and `resource` owns the value that results.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::http::StatusCode;
use bytes::Bytes;
use moka::sync::Cache;
use reqwest::Client;
use tokio::{sync::Semaphore, time::Instant as TokioInstant};

use super::fetch::{fetch_limited_bytes_uncached, provider_fetch_cache_weight};
use super::{
    CachedProviderRepresentation, FetchFence, FetchedProviderNegative, FetchedProviderResource,
    PROVIDER_FETCH_CONCURRENCY, PROVIDER_FETCH_MAX_INFLIGHT, PROVIDER_INVALIDATION_FENCE_RETENTION,
    PROVIDER_STALE_REVALIDATION_FAILURE_COOLDOWN, PROVIDER_STALE_REVALIDATION_FAILURE_MAX_KEYS,
    ProviderFetchPermit, ProviderFetchSlot, ProviderFlightOutcome, ProviderFlightResult,
    ProviderOriginOutcome, ProviderResource, provider_http_client, provider_negative_error,
};
use crate::server::provider_body::BodyValidation;
use crate::server::{HttpError, conditional::Validators};
use ishikari_core::metrics::NodeMetrics;
use ishikari_core::storage::ObjectStoreRegistry;
use mmpf_common::singleflight::{Flight, LeaderGuard, SingleFlight};

/// Why a completed fetch was or was not retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderCacheStore {
    Stored,
    /// Fetched successfully, but the response policy forbids retention.
    Uncacheable,
    /// A refresh hint advanced the fence while the fetch was in flight, so these
    /// bytes were already unreachable before they arrived.
    Superseded,
}

impl ProviderCacheStore {
    /// Production code distinguishes all three outcomes for its metric label, so
    /// this collapse to a boolean exists only for assertions.
    #[cfg(test)]
    fn is_stored(self) -> bool {
        self == Self::Stored
    }
}

#[derive(Clone)]
pub(super) struct ProviderFetchCache {
    entries: Cache<ProviderFetchCacheKey, CachedProviderFetch>,
    /// Bounded per-key generation fences for explicit management refreshes. A
    /// refresh removes the cached entry outright and a pre-hint fetch completing
    /// afterwards is refused rather than stored, so this map only has to outlive
    /// the fetches that captured a value — see
    /// [`PROVIDER_INVALIDATION_FENCE_RETENTION`]. Because it is bounded, losing an
    /// entry must never be mistaken for "never refreshed": see `refreshes`.
    invalidation_epochs: Cache<ProviderFetchCacheKey, u64>,
    /// Process-wide count of refreshes, which no eviction or expiry can lower.
    /// This is what makes an evicted per-key fence fail closed instead of open;
    /// [`FetchFence`] documents the reasoning.
    refreshes: Arc<AtomicU64>,
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
            invalidation_epochs: Cache::builder()
                .max_capacity(PROVIDER_STALE_REVALIDATION_FAILURE_MAX_KEYS)
                .time_to_live(PROVIDER_INVALIDATION_FENCE_RETENTION)
                .build(),
            failed_revalidations: Cache::builder()
                .max_capacity(PROVIDER_STALE_REVALIDATION_FAILURE_MAX_KEYS)
                .build(),
            refreshes: Arc::new(AtomicU64::new(0)),
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
        if entry.invalidation_epoch() != self.invalidation_epoch(key) {
            return None;
        }
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

    fn put_found(
        &self,
        key: ProviderFetchCacheKey,
        fetched: &FetchedProviderResource,
        fence: FetchFence,
    ) -> ProviderCacheStore {
        // Modified and 304 responses both arrive here. Either is a successful
        // revalidation and resets any prior failed-refresh cooldown.
        self.failed_revalidations.invalidate(&key);
        if self.is_superseded(&key, fence) {
            self.invalidate(&key);
            return ProviderCacheStore::Superseded;
        }
        if !fetched.policy.store {
            // A successful refresh can tighten an existing stale entry to
            // `no-store`/`private`/`no-cache`. Remove that old body promptly.
            self.invalidate(&key);
            return ProviderCacheStore::Uncacheable;
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
            return ProviderCacheStore::Uncacheable;
        }
        let fresh_until = stored_at + fresh_remaining;
        self.entries.insert(
            key,
            CachedProviderFetch::Found {
                invalidation_epoch: fence.stored_epoch(),
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
        ProviderCacheStore::Stored
    }

    fn put_negative(
        &self,
        key: ProviderFetchCacheKey,
        negative: &FetchedProviderNegative,
        fence: FetchFence,
    ) -> ProviderCacheStore {
        // A terminal origin response is also a successful revalidation attempt.
        self.failed_revalidations.invalidate(&key);
        if self.is_superseded(&key, fence) {
            self.invalidate(&key);
            return ProviderCacheStore::Superseded;
        }
        if !negative.policy.store {
            self.invalidate(&key);
            return ProviderCacheStore::Uncacheable;
        }
        let fresh = negative.policy.fresh.saturating_sub(negative.initial_age);
        if fresh.is_zero() {
            self.invalidate(&key);
            return ProviderCacheStore::Uncacheable;
        }
        let fresh_until = Instant::now() + fresh;
        self.entries.insert(
            key,
            CachedProviderFetch::Negative {
                invalidation_epoch: fence.stored_epoch(),
                status: negative.status,
                fresh_until,
                stale_until: fresh_until,
            },
        );
        ProviderCacheStore::Stored
    }

    /// Whether a refresh hint advanced the fence while this fetch was running.
    ///
    /// Such bytes must not be stored at all. Storing them and relying on the
    /// read-side epoch comparison in [`get`](Self::get) would make the fence
    /// depend on `invalidation_epochs` retaining the key forever: that map is
    /// bounded, and once it drops the key the epoch reads back as zero, so a
    /// stored pre-hint entry whose own epoch was zero becomes reachable again.
    ///
    /// Refusing the store is necessary but not sufficient, because the per-key
    /// entry can be *capacity*-evicted mid-flight — which no expiry setting can
    /// prevent. So the per-key comparison is only trusted while the entry is
    /// actually present; an absent entry is trusted only when the process-wide
    /// refresh count proves no refresh happened at all during the flight.
    fn is_superseded(&self, key: &ProviderFetchCacheKey, fence: FetchFence) -> bool {
        if self.refreshes.load(Ordering::Acquire) == fence.refreshes {
            // Nothing was refreshed anywhere while this fetch ran, so the per-key
            // map cannot have lost an entry that matters here. This is the
            // overwhelmingly common path and costs one atomic load.
            return false;
        }
        match self.invalidation_epochs.get(key) {
            Some(current) => Some(current) != fence.key_epoch,
            // A refresh happened while this fetch ran and this key now has no
            // fence entry. Absence is indistinguishable from an entry a refresh
            // set and eviction then dropped, so refuse rather than risk
            // republishing superseded bytes. The cost is one extra fetch, and
            // only for a fetch that overlapped an actual refresh.
            None => true,
        }
    }

    /// Captures the refresh fence immediately before provider I/O begins.
    fn fetch_fence(&self, key: &ProviderFetchCacheKey) -> FetchFence {
        FetchFence {
            key_epoch: self.invalidation_epochs.get(key),
            refreshes: self.refreshes.load(Ordering::Acquire),
        }
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

    fn invalidate_for_refresh(&self, key: &ProviderFetchCacheKey) {
        // Raise the process-wide count first, so a fetch completing concurrently
        // can never see the pre-refresh count together with post-refresh per-key
        // state. Dropping the entry last is what makes the interleaving safe
        // either way: a store that slips through in between is removed here.
        self.refreshes.fetch_add(1, Ordering::AcqRel);
        let next = self.invalidation_epoch(key).wrapping_add(1);
        self.invalidation_epochs.insert(key.clone(), next);
        self.invalidate(key);
    }

    fn invalidation_epoch(&self, key: &ProviderFetchCacheKey) -> u64 {
        self.invalidation_epochs.get(key).unwrap_or_default()
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
        invalidation_epoch: u64,
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
        invalidation_epoch: u64,
        status: StatusCode,
        fresh_until: Instant,
        stale_until: Instant,
    },
}

impl CachedProviderFetch {
    fn invalidation_epoch(&self) -> u64 {
        match self {
            Self::Found {
                invalidation_epoch, ..
            }
            | Self::Negative {
                invalidation_epoch, ..
            } => *invalidation_epoch,
        }
    }

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
    let fence = fetcher.cache.fetch_fence(&key);
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
        let _ = store_leader_result(&fetcher, &key, resource, result, fence, guard);
    });
}

/// Applies a leader (foreground or background) fetch outcome to the cache,
/// records the insert metric, and shares a transient error with followers.
fn store_leader_result(
    fetcher: &ProviderFetcher,
    key: &ProviderFetchCacheKey,
    resource: &'static str,
    result: Result<ProviderOriginOutcome, HttpError>,
    fence: FetchFence,
    guard: LeaderGuard<ProviderFetchCacheKey, ProviderFlightOutcome>,
) -> Result<ProviderResource, HttpError> {
    match result {
        Ok(ProviderOriginOutcome::Negative(negative)) => {
            let error = provider_negative_error(negative.status);
            let stored = fetcher.cache.put_negative(key.clone(), &negative, fence);
            fetcher.metrics.record_provider_resource_cache(
                resource,
                match stored {
                    ProviderCacheStore::Stored => "negative_insert",
                    ProviderCacheStore::Uncacheable => "negative_uncacheable",
                    ProviderCacheStore::Superseded => "negative_superseded_by_refresh",
                },
            );
            guard.complete_with_error(ProviderFlightOutcome::error(fence, error.clone()));
            Err(error)
        }
        Ok(origin) => {
            let (fetched, stored_outcome) = match origin {
                ProviderOriginOutcome::Modified(fetched) => (fetched, "insert"),
                ProviderOriginOutcome::NotModified(fetched) => (fetched, "revalidated"),
                ProviderOriginOutcome::Negative(_) => unreachable!("handled above"),
            };
            let response = ProviderResource::fetched(&fetched);
            let stored = fetcher.cache.put_found(key.clone(), &fetched, fence);
            // An uncacheable response was fetched successfully but intentionally
            // not retained. This can also happen when a 304 tightens policy.
            let outcome = match stored {
                ProviderCacheStore::Stored => stored_outcome,
                ProviderCacheStore::Uncacheable => "uncacheable",
                ProviderCacheStore::Superseded => "superseded_by_refresh",
            };
            fetcher
                .metrics
                .record_provider_resource_cache(resource, outcome);
            guard.complete_with(ProviderFlightOutcome::resource(fence, response.clone()));
            Ok(response)
        }
        Err(error) => {
            fetcher
                .metrics
                .record_provider_resource_cache(resource, "error");
            guard.complete_with_error(ProviderFlightOutcome::error(fence, error.clone()));
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

    pub(crate) fn invalidate_json(
        &self,
        url: String,
        resource: &'static str,
        accepted_content_types: &'static [&'static str],
    ) {
        let key = ProviderFetchCacheKey::new(
            resource,
            Arc::<str>::from(url),
            accepted_content_types,
            BodyValidation::Json,
        );
        self.cache.invalidate_for_refresh(&key);
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
                let fence = fetcher.cache.fetch_fence(&key);
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
                return store_leader_result(fetcher, &key, resource, result, fence, guard);
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
                    // A refresh hint may have advanced the fence while this
                    // flight was in progress. Publication to followers bypasses
                    // the cache, so the fence has to be re-checked here or the
                    // hint would be ignored for every joined request. A stale
                    // outcome re-enters the loop, which re-reads the cache and
                    // either leads a fresh fetch or joins the next flight.
                    if !fetcher.cache.is_superseded(&key, outcome.fence) {
                        return match outcome.result {
                            ProviderFlightResult::Error(error) => Err(error),
                            ProviderFlightResult::Resource(resource) => Ok(resource),
                        };
                    }
                    fetcher.metrics.record_provider_resource_cache(
                        resource,
                        "singleflight_refetch_after_hint",
                    );
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
        Freshness, PROVIDER_STALE_REVALIDATION_FAILURE_COOLDOWN, ProviderCacheStore,
        ProviderFetchCache, ProviderFetchCacheKey, ProviderFetchSlot, ProviderFetcher,
        ProviderFlightResult, ProviderOriginOutcome, Validators, record_cached_provider_fetch,
        store_leader_result,
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
            invalidation_epoch: 0,
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
            invalidation_epoch: 0,
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
            invalidation_epoch: 0,
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
        cache.put_found(key.clone(), &old, cache.fetch_fence(&key));
        assert!(cache.get(&key).is_some());

        let new = FetchedProviderResource {
            bytes: Bytes::from_static(b"new"),
            policy: cache_policy("style", Some("no-store")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };
        cache.put_found(key.clone(), &new, cache.fetch_fence(&key));
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
        assert!(
            cache
                .put_negative(key.clone(), &gone, cache.fetch_fence(&key))
                .is_stored()
        );
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
        assert!(
            !cache
                .put_negative(key.clone(), &no_store, cache.fetch_fence(&key))
                .is_stored()
        );
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
        assert!(
            cache
                .put_found(key.clone(), &fetched, cache.fetch_fence(&key))
                .is_stored()
        );
        let (entry, freshness) = cache.get(&key).expect("aged entry");
        assert_eq!(freshness, Freshness::Fresh);
        let resource = entry.into_result().expect("resource");
        assert!(resource.age_seconds() >= 45);

        let already_expired = FetchedProviderResource {
            initial_age: Duration::from_secs(60),
            ..fetched
        };
        assert!(
            !cache
                .put_found(key.clone(), &already_expired, cache.fetch_fence(&key))
                .is_stored()
        );
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

        assert!(
            !cache
                .put_found(key.clone(), &fetched, cache.fetch_fence(&key))
                .is_stored()
        );
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
            fetcher.cache.fetch_fence(&key),
            guard,
        )
        .expect("leader response");
        fetcher.cache.invalidate(&key);

        let outcome = follower.wait().await.expect("published leader result");
        assert!(
            !fetcher.cache.is_superseded(&key, outcome.fence),
            "plain eviction must not advance the fence, so the outcome stays usable"
        );
        let ProviderFlightResult::Resource(follower_resource) = outcome.result else {
            panic!("follower must receive the leader representation");
        };
        assert_eq!(follower_resource.bytes(), leader_resource.bytes());
    }

    /// A refresh hint that lands while a fetch is in flight must also reach the
    /// requests that joined that fetch. The fence filters at *lookup*, but
    /// publication to followers bypasses lookup entirely, so the epoch has to
    /// travel on the outcome — otherwise a hint is silently ignored for exactly
    /// the concurrent requests it is most likely to overlap.
    #[tokio::test]
    async fn a_refresh_hint_during_a_flight_is_detectable_by_the_follower() {
        let fetcher = ProviderFetcher::new(
            NodeMetrics::new(),
            Arc::new(ObjectStoreRegistry::without_options()),
            TEST_PROVIDER_CACHE_MAX_BYTES,
        );
        let key = provider_key("https://example/hinted-during-flight.json");
        let Flight::Leader(guard) = fetcher.cache.begin_fetch(key.clone()) else {
            panic!("first caller must lead");
        };
        let Flight::Follower(follower) = fetcher.cache.begin_fetch(key.clone()) else {
            panic!("second caller must follow");
        };
        let fetched = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=3600")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };

        let fetch_start_fence = fetcher.cache.fetch_fence(&key);
        // The hint lands while the leader is still fetching.
        fetcher.cache.invalidate_for_refresh(&key);

        store_leader_result(
            &fetcher,
            &key,
            "style",
            Ok(ProviderOriginOutcome::Modified(fetched)),
            fetch_start_fence,
            guard,
        )
        .expect("the leader still returns the response it fetched");
        assert!(
            fetcher.cache.get(&key).is_none(),
            "the pre-hint completion must stay unreachable by lookup"
        );

        let outcome = follower.wait().await.expect("published leader result");
        assert_eq!(outcome.fence, fetch_start_fence);
        assert!(
            fetcher.cache.is_superseded(&key, outcome.fence),
            "the follower must be able to tell that its outcome predates the hint"
        );
    }

    #[test]
    fn explicit_json_invalidation_removes_only_the_matching_representation() {
        let fetcher = ProviderFetcher::new(
            NodeMetrics::new(),
            Arc::new(ObjectStoreRegistry::without_options()),
            TEST_PROVIDER_CACHE_MAX_BYTES,
        );
        let url = "https://example/style.json";
        let json_key =
            ProviderFetchCacheKey::new("style", url, &["application/json"], BodyValidation::Json);
        let bytes_key =
            ProviderFetchCacheKey::new("sprite", url, &["application/json"], BodyValidation::Bytes);
        let fetched = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=60")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };
        assert!(
            fetcher
                .cache
                .put_found(
                    json_key.clone(),
                    &fetched,
                    fetcher.cache.fetch_fence(&json_key)
                )
                .is_stored()
        );
        assert!(
            fetcher
                .cache
                .put_found(
                    bytes_key.clone(),
                    &fetched,
                    fetcher.cache.fetch_fence(&bytes_key)
                )
                .is_stored()
        );

        fetcher.invalidate_json(url.to_string(), "style", &["application/json"]);

        assert!(fetcher.cache.get(&json_key).is_none());
        assert!(fetcher.cache.get(&bytes_key).is_some());
    }

    #[test]
    fn refresh_fence_rejects_a_pre_hint_inflight_completion() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = provider_key("https://example/raced-style.json");
        let fetched = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=3600")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };
        let fetch_start_fence = cache.fetch_fence(&key);

        cache.invalidate_for_refresh(&key);
        assert_eq!(
            cache.put_found(key.clone(), &fetched, fetch_start_fence),
            ProviderCacheStore::Superseded,
            "a completion from before the hint must be refused, not stored and filtered"
        );
        assert!(
            cache.get(&key).is_none(),
            "completion from before the hint must remain unreachable"
        );

        let retry_fence = cache.fetch_fence(&key);
        assert!(
            cache
                .put_found(key.clone(), &fetched, retry_fence)
                .is_stored()
        );
        assert!(cache.get(&key).is_some());
    }

    /// The fence is bounded and expiring, so it must not be the only thing
    /// keeping superseded bytes unreachable. Losing a fence entry may cost an
    /// extra fetch; it must never make pre-hint content visible again.
    #[test]
    fn a_fence_entry_evicted_mid_flight_still_refuses_the_stale_completion() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = provider_key("https://example/forgotten-fence.json");
        let fetched = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=3600")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };

        // A fetch begins on a key that has never been refreshed, so it captures
        // *no* per-key epoch at all.
        let fetch_start_fence = cache.fetch_fence(&key);
        assert_eq!(fetch_start_fence.key_epoch, None);

        cache.invalidate_for_refresh(&key);

        // The fence entry is dropped while the fetch is still running. Capacity
        // eviction can do this at any moment and no expiry setting prevents it,
        // so the per-key epoch now reads back as absent — indistinguishable from
        // the "never refreshed" state the fetch captured.
        cache.invalidation_epochs.invalidate(&key);
        cache.invalidation_epochs.run_pending_tasks();
        assert_eq!(cache.invalidation_epochs.get(&key), None);
        assert_eq!(
            cache.invalidation_epoch(&key),
            fetch_start_fence.stored_epoch()
        );

        // Only the process-wide refresh count still records that a refresh
        // happened, and it is what must make this fail closed.
        assert_eq!(
            cache.put_found(key.clone(), &fetched, fetch_start_fence),
            ProviderCacheStore::Superseded,
            "an evicted fence must not let a pre-hint completion be stored"
        );
        assert!(
            cache.get(&key).is_none(),
            "pre-hint content must stay unreachable after the fence is forgotten"
        );
    }

    /// The fail-closed rule above must not tax ordinary traffic: with no refresh
    /// anywhere, a completion is stored regardless of per-key fence state.
    #[test]
    fn without_any_refresh_a_completion_is_stored_even_with_no_fence_entry() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = provider_key("https://example/never-refreshed.json");
        let fetched = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=3600")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };
        let fence = cache.fetch_fence(&key);
        assert_eq!(fence.key_epoch, None);
        assert!(cache.put_found(key.clone(), &fetched, fence).is_stored());
        assert!(cache.get(&key).is_some());
    }

    /// A refresh of an unrelated key advances the process-wide count. That alone
    /// must not supersede a fetch whose own key still matches its captured epoch,
    /// or every hint would discard concurrent work across the whole cache.
    #[test]
    fn a_refresh_of_another_key_does_not_supersede_a_matching_fence() {
        let cache = ProviderFetchCache::new(TEST_PROVIDER_CACHE_MAX_BYTES);
        let key = provider_key("https://example/mine.json");
        let other = provider_key("https://example/theirs.json");
        let fetched = FetchedProviderResource {
            bytes: Bytes::from_static(br#"{"version":8}"#),
            policy: cache_policy("style", Some("max-age=3600")),
            validators: Validators::default(),
            content_encoding: None,
            initial_age: Duration::ZERO,
        };

        // Give this key a fence entry, so its state is present rather than absent.
        cache.invalidate_for_refresh(&key);
        let fence = cache.fetch_fence(&key);
        assert!(fence.key_epoch.is_some());

        cache.invalidate_for_refresh(&other);

        assert!(
            cache.put_found(key.clone(), &fetched, fence).is_stored(),
            "another key's refresh must not discard this fetch"
        );
        assert!(cache.get(&key).is_some());
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
        assert!(
            cache
                .put_found(first.clone(), &refreshed, cache.fetch_fence(&first))
                .is_stored()
        );
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
                invalidation_epoch: 0,
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
        assert!(
            cache
                .put_found(key.clone(), &refreshed, cache.fetch_fence(&key))
                .is_stored()
        );

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
            invalidation_epoch: 0,
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
            invalidation_epoch: 0,
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
