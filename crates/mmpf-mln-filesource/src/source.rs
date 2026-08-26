//! Rust-side network `FileSource` for MapLibre Native.
//!
//! Replaces mbgl's default network leaf (`FileSourceType::Network`) so that
//! style-internal resources (tiles / glyphs / sprites / TileJSON) are fetched
//! by a tokio + reqwest stack with explicit timeouts, bounded admission,
//! and Prometheus metrics. A process-wide weighted Rust cache also
//! replaces `FileSourceType::Database`, removing mbgl's fixed ambient-cache
//! limit while preserving the default ResourceLoader's cache-first waterfall.
//! Cross-renderer cold misses are coalesced here because MainResourceLoader
//! itself tracks each request independently.
//!
//! Registration is process-global and must happen before the first renderer is
//! constructed; the application is responsible for registering at startup.
//! Cancellation: when mbgl drops a request (e.g. the render was abandoned),
//! the crate's tokio adapter aborts the fetch task; the permit and in-flight
//! gauge are released by RAII guards.

mod cache;
mod health;
mod metrics;
mod response;
mod retry;
mod singleflight;

use std::collections::{HashMap, hash_map::DefaultHasher};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use maplibre_native::file_source::{
    ErrorReason, FileSourceType, Priority, ResourceKind, ResourceRequest, Response,
    TokioFileSource, register_tokio_file_source,
};
use moka::sync::Cache;
use reqwest::header::{IF_MODIFIED_SINCE, IF_NONE_MATCH, RANGE};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::policy::{FilteringResolver, ResourceUrlPolicy};
pub use health::{ProviderHealthTracker, ProviderRetryGuard};
pub use metrics::gather_metrics;
#[cfg(test)]
use metrics::usage_label;
use metrics::{
    BodyInflightGuard, DeferredRefreshGuard, InflightGuard, RequestObservation,
    UpstreamAttemptObservation, fs_metrics, kind_label, mark_metrics_started, note_caller_thread,
    outcome_label, priority_label,
};
#[cfg(test)]
use response::CachedFreshness;
use response::{
    CachePolicy, PriorResponse, RetryDirective, cache_policy_for_not_modified,
    cache_policy_for_response, materialize_not_modified, negative_cache_ttl,
    prior_response_with_cache, response_from_http, response_from_reqwest_error, retry_directive,
};
#[cfg(test)]
use response::{has_cache_directive, parse_max_age, parse_retry_after};
#[cfg(test)]
use retry::{MAX_ATTEMPTS, RETRY_BACKOFF};
use retry::{
    MAX_RETRY_DELAY, NetworkAttemptBudget, REQUEST_TIMEOUT, RETRY_WINDOW, request_timeout_response,
    retry_delay, retry_fits_budget,
};
use singleflight::{
    FLIGHT_SHARDS, Flight, FlightKey, FlightLeader, FlightMap, FlightRequestSemantics,
};

use maplibre_native::file_source::Usage;

use crate::http::{redacted_query_keys, redacted_url_str, reqwest_error_label};
use mmpf_common::sync::lock_unpoisoned;

/// TCP connect timeout for upstream resource fetches.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Minimum concurrent upstream fetches for regular-priority
/// (render-blocking) requests.
const DEFAULT_REGULAR_PERMITS: usize = 64;
/// Background refreshes must not consume all render-blocking network slots.
const LOW_PRIORITY_PERMITS: usize = 8;
/// Minimum concurrent body downloads. Body buffering is the expensive part of
/// a fetch: one worst-case body slot is reserved before reading so
/// request-count admission cannot multiply the per-resource cap into multi-GiB
/// transient memory. Production expands this with the node's render permits;
/// a fixed low cap would serialize in-render I/O on larger nodes.
const DEFAULT_BODY_PERMITS: usize = 24;
/// Concurrent body downloads allowed per executing render. A style commonly
/// needs several tiles and glyph ranges in parallel, so two body slots per
/// render leaves the native renderer unnecessarily network-bound on cold
/// requests.
const BODY_PERMITS_PER_RENDER: usize = 4;
const MIB: u64 = 1024 * 1024;
/// A missing resource may appear later (dynamic tiles and rolling provider
/// updates), so negative entries are deliberately short lived.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(15);
/// Bounds attacker-controlled or broken-style URL cardinality.
const NEGATIVE_CACHE_CAPACITY: u64 = 4_096;
/// A failed speculative refresh must not turn render rate into provider retry
/// rate. Foreground revalidation remains unaffected.
const REFRESH_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
/// A usable Database hit has already satisfied its requester, so the paired
/// background Network callback may wait for expiry. Cap that wait so an
/// upstream's long freshness lifetime cannot retain a Tokio task for hours.
const MAX_REFRESH_DEFERRAL: Duration = Duration::from_mins(5);
/// A normal cache miss is not provider-failure evidence. Promote only an
/// attempt that has spent this long in actual network I/O (after admission),
/// which still precedes the default render SLA and the per-attempt timeout.
const SLOW_PROVIDER_ATTEMPT_THRESHOLD: Duration = Duration::from_secs(1);

struct NetworkIoObservation<'a> {
    provider_health: &'a ProviderHealthTracker,
    metrics: Option<&'a mut UpstreamAttemptObservation>,
    enabled: bool,
    elapsed: Duration,
}

impl<'a> NetworkIoObservation<'a> {
    fn new(
        provider_health: &'a ProviderHealthTracker,
        metrics: &'a mut UpstreamAttemptObservation,
        enabled: bool,
    ) -> Self {
        Self {
            provider_health,
            metrics: Some(metrics),
            enabled,
            elapsed: Duration::ZERO,
        }
    }

    #[cfg(test)]
    fn without_metrics(provider_health: &'a ProviderHealthTracker, enabled: bool) -> Self {
        Self {
            provider_health,
            metrics: None,
            enabled,
            elapsed: Duration::ZERO,
        }
    }

    async fn run<F>(
        &mut self,
        budget: &mut NetworkAttemptBudget,
        future: F,
    ) -> Result<F::Output, tokio::time::error::Elapsed>
    where
        F: Future,
    {
        let threshold_remaining = SLOW_PROVIDER_ATTEMPT_THRESHOLD.saturating_sub(self.elapsed);
        let Self {
            provider_health,
            metrics,
            enabled,
            elapsed,
        } = self;
        // This guard spans only `NetworkAttemptBudget::run`. Its Drop also
        // covers cancellation, while body-permit and retry waits occur outside.
        let _timing = NetworkOperationTiming::new(elapsed, metrics.as_deref_mut());
        let operation = budget.run(future);
        tokio::pin!(operation);

        if !*enabled {
            return operation.await;
        }
        // Evidence belongs to this network future only. It must be dropped
        // before response-body permit waits or CPU work between chunks.
        let mut slow_evidence = None;
        let output = if threshold_remaining.is_zero() {
            slow_evidence = Some(provider_health.begin_slow_attempt());
            operation.await
        } else {
            tokio::select! {
                output = &mut operation => output,
                () = tokio::time::sleep(threshold_remaining) => {
                    slow_evidence = Some(provider_health.begin_slow_attempt());
                    operation.await
                }
            }
        };
        drop(slow_evidence);
        output
    }

    #[cfg(test)]
    fn elapsed(&self) -> Duration {
        self.elapsed
    }
}

struct NetworkOperationTiming<'a> {
    started: tokio::time::Instant,
    elapsed: &'a mut Duration,
    metrics: Option<&'a mut UpstreamAttemptObservation>,
}

impl<'a> NetworkOperationTiming<'a> {
    fn new(elapsed: &'a mut Duration, metrics: Option<&'a mut UpstreamAttemptObservation>) -> Self {
        Self {
            started: tokio::time::Instant::now(),
            elapsed,
            metrics,
        }
    }
}

impl Drop for NetworkOperationTiming<'_> {
    fn drop(&mut self) {
        let duration = self.started.elapsed();
        *self.elapsed = self.elapsed.saturating_add(duration);
        if let Some(metrics) = &mut self.metrics {
            metrics.add_network_duration(duration);
        }
    }
}

fn max_resource_bytes(kind: ResourceKind) -> u64 {
    if kind == ResourceKind::Glyphs
        || kind == ResourceKind::SpriteJSON
        || kind == ResourceKind::Source
        || kind == ResourceKind::Style
    {
        4 * MIB
    } else if kind == ResourceKind::Tile
        || kind == ResourceKind::SpriteImage
        || kind == ResourceKind::Image
    {
        16 * MIB
    } else {
        8 * MIB
    }
}

/// Concurrency sizing for the network FileSource, resolved by the caller from
/// the node's render permits.
#[derive(Clone, Copy, Debug)]
pub struct FileSourceIoPermits {
    /// Concurrent upstream fetches for regular-priority requests.
    pub regular: usize,
    /// Concurrent response-body downloads (each reserves one worst-case-body
    /// memory slot). The effective in-render I/O parallelism of the node.
    pub body: usize,
}

impl Default for FileSourceIoPermits {
    fn default() -> Self {
        Self {
            regular: DEFAULT_REGULAR_PERMITS,
            body: DEFAULT_BODY_PERMITS,
        }
    }
}

impl FileSourceIoPermits {
    /// Default sizing from the node's execution (render) permits: every
    /// executing render can stream four subresource bodies concurrently, with
    /// floors so small nodes never serialize I/O. Worst-case transient body
    /// memory is `body × 16 MiB` (`max_resource_bytes`); realistic bodies are
    /// far smaller.
    pub fn for_render_permits(render_permits: usize) -> Self {
        let body = render_permits
            .saturating_mul(BODY_PERMITS_PER_RENDER)
            .max(DEFAULT_BODY_PERMITS);
        Self {
            regular: body.saturating_mul(2).max(DEFAULT_REGULAR_PERMITS),
            body,
        }
    }

    fn clamped(self) -> Self {
        Self {
            // regular must admit at least as many requests as bodies can
            // stream, or the regular lane silently becomes the binding cap.
            regular: self.regular.max(self.body).max(1),
            body: self.body.max(1),
        }
    }
}

/// Network leaf: plain `reqwest` GETs with timeouts and bounded admission.
struct NetworkFileSource {
    client: reqwest::Client,
    url_policy: ResourceUrlPolicy,
    regular: Semaphore,
    low_priority: Semaphore,
    bodies: Semaphore,
    negative_cache: Cache<SharedResourceRequestKey, NegativeCacheEntry>,
    refresh_failure_cooldown: Cache<SharedResourceRequestKey, ()>,
    resource_cache: cache::ResourceCache,
    inflight: Box<[FlightMap]>,
    provider_health: ProviderHealthTracker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConditionalValidator<'a> {
    Etag(&'a str),
    Modified(SystemTime),
}

#[derive(Clone)]
struct NegativeCacheEntry {
    response: Response,
    expires_at: Instant,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResourceRequestKey {
    pub(crate) url: String,
    pub(crate) kind: &'static str,
    pub(crate) range: Option<(u64, u64)>,
    pub(crate) tile: Option<ResourceTileKey>,
}

pub(crate) type SharedResourceRequestKey = Arc<ResourceRequestKey>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResourceTileKey {
    pub(crate) url_template: String,
    pub(crate) pixel_ratio: u8,
    pub(crate) x: i32,
    pub(crate) y: i32,
    pub(crate) z: i8,
}

impl ResourceRequestKey {
    pub(crate) fn from_request(request: &ResourceRequest) -> Self {
        Self {
            url: request.url.clone(),
            kind: kind_label(request.kind),
            range: request
                .data_range
                .as_ref()
                .map(|range| (*range.start(), *range.end())),
            tile: request.tile.as_ref().map(|tile| ResourceTileKey {
                url_template: tile.url_template.clone(),
                pixel_ratio: tile.pixel_ratio,
                x: tile.x,
                y: tile.y,
                z: tile.z,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_key(url: &str, kind: ResourceKind) -> Self {
        Self {
            url: url.to_owned(),
            kind: kind_label(kind),
            range: None,
            tile: None,
        }
    }
}

impl NetworkFileSource {
    fn new(
        resource_cache: cache::ResourceCache,
        private_hosts: Vec<String>,
        io_permits: FileSourceIoPermits,
        provider_health: ProviderHealthTracker,
        user_agent: &str,
    ) -> anyhow::Result<Self> {
        let io_permits = io_permits.clamped();
        let url_policy = ResourceUrlPolicy::new(private_hosts);
        let client = build_filtered_http_client(url_policy.clone(), user_agent)?;
        Ok(Self {
            client,
            url_policy,
            regular: Semaphore::new(io_permits.regular),
            low_priority: Semaphore::new(LOW_PRIORITY_PERMITS),
            bodies: Semaphore::new(io_permits.body),
            negative_cache: Cache::builder()
                .max_capacity(NEGATIVE_CACHE_CAPACITY)
                .time_to_live(NEGATIVE_CACHE_TTL)
                .build(),
            refresh_failure_cooldown: Cache::builder()
                .max_capacity(NEGATIVE_CACHE_CAPACITY)
                .time_to_live(REFRESH_FAILURE_COOLDOWN)
                .build(),
            resource_cache,
            inflight: (0..FLIGHT_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            provider_health,
        })
    }

    /// Inputs the shared-cache race decision needs, separated from
    /// `ResourceRequest` so this branch can be exercised directly.
    ///
    /// `ResourceRequest` is `#[non_exhaustive]` with only a crate-private
    /// constructor upstream, so no test can build one. Without this split the only
    /// testable surface was the classification helpers — and a test over those
    /// stays green even if the early return below is deleted, which is exactly the
    /// production failure it is supposed to guard.
    async fn resolve_shared_cache_race(
        &self,
        negative_key: &Arc<ResourceRequestKey>,
        inputs: SharedCacheRaceInputs,
        observation: &mut RequestObservation,
    ) -> Option<Response> {
        if inputs.consults_shared_cache
            && let Some(cached) = self.resource_cache.fresh_response(negative_key)
        {
            let native_database_state =
                native_database_state(inputs.prior_data_present, inputs.prior_metadata_present);
            let expires = match resolve_fresh_cache_race(
                &cached.response,
                native_database_state,
                inputs.is_low_priority && inputs.is_online,
            ) {
                FreshCacheResolution::Serve(response) => {
                    fs_metrics()
                        .fresh_cache_race_total
                        .with_label_values(&[inputs.kind, native_database_state.label()])
                        .inc();
                    observation.complete(&response);
                    return Some(response);
                }
                FreshCacheResolution::Defer(expires) => expires,
            };

            fs_metrics()
                .refresh_deferred_total
                .with_label_values(&[inputs.kind])
                .inc();
            let _deferred = DeferredRefreshGuard::new(inputs.kind);
            let deferral = refresh_deferral(expires, inputs.minimum_update_interval);
            // Measure the actual park rather than trusting `deferral.wait`: the
            // sleep can be cancelled or run long, and the reported duration must
            // exclude what really happened, not what was planned.
            let parked_since = Instant::now();
            tokio::time::sleep(deferral.wait).await;
            observation.add_parked(parked_since.elapsed());

            if let Some(response) = complete_deferred_refresh(
                &deferral,
                self.resource_cache
                    .fresh_response(negative_key)
                    .and_then(|entry| entry.response.expires),
            ) {
                observation.complete(&response);
                return Some(response);
            }
        }
        None
    }

    fn flight_shard(&self, key: &FlightKey) -> &FlightMap {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        &self.inflight[(hasher.finish() as usize) & (FLIGHT_SHARDS - 1)]
    }

    async fn acquire(&self, request: &ResourceRequest) -> SemaphorePermit<'_> {
        let semaphore = if request.priority == Priority::Low {
            &self.low_priority
        } else {
            &self.regular
        };
        semaphore
            .acquire()
            .await
            .expect("file source semaphore is never closed")
    }

    async fn fetch_coalesced(
        &self,
        request: &ResourceRequest,
        resource_key: SharedResourceRequestKey,
    ) -> Response {
        let key = Arc::new(FlightKey {
            resource: Arc::clone(&resource_key),
            persistent: uses_shared_cache(request.storage_policy),
            priority: priority_label(request.priority),
            semantics: FlightRequestSemantics::from_request(request),
        });
        let flights = self.flight_shard(&key);
        loop {
            let (flight, is_leader) = {
                let mut entries = lock_unpoisoned(flights);
                match entries.get(&key) {
                    Some(flight) => (Arc::clone(flight), false),
                    None => {
                        let flight = Arc::new(Flight::new());
                        entries.insert(Arc::clone(&key), Arc::clone(&flight));
                        (flight, true)
                    }
                }
            };

            if is_leader {
                fs_metrics()
                    .singleflight_total
                    .with_label_values(&[kind_label(request.kind), "leader"])
                    .inc();
                let leader = FlightLeader {
                    flights,
                    key: Arc::clone(&key),
                    flight,
                    completed: false,
                };
                // Admission permits are taken per attempt inside
                // `fetch_with_retries` so retry backoff never parks lane
                // capacity.
                let response = self.fetch_with_retries(request, &resource_key).await;
                return leader.complete(response);
            }
            fs_metrics()
                .singleflight_total
                .with_label_values(&[kind_label(request.kind), "waiter"])
                .inc();
            if let Some(response) = flight.wait().await {
                return response;
            }
            fs_metrics()
                .singleflight_total
                .with_label_values(&[kind_label(request.kind), "restart"])
                .inc();
        }
    }

    fn resolve_refresh_failure_cooldown(
        &self,
        resource_key: &SharedResourceRequestKey,
        inputs: SharedCacheRaceInputs,
        observation: &mut RequestObservation,
    ) -> Option<Response> {
        if !inputs.is_background_refresh()
            || !self
                .resource_cache
                .is_stale_while_revalidating(resource_key)
            || self.refresh_failure_cooldown.get(resource_key).is_none()
        {
            return None;
        }

        // Database already delivered the stale representation. Completing the
        // paired Network callback as unchanged avoids another provider attempt
        // without withholding bytes from the current render.
        let response = Response::not_modified();
        observation.complete(&response);
        observation.outcome = "refresh_cooldown";
        Some(response)
    }

    fn finish_fetch_sequence(
        &self,
        request: &ResourceRequest,
        resource_key: &SharedResourceRequestKey,
        attempt: FetchAttempt,
    ) -> Response {
        let inputs = SharedCacheRaceInputs::from_request(request);
        let response = self.finish_fetch(resource_key, attempt);
        self.update_refresh_failure_cooldown(resource_key, inputs, &response);
        response
    }

    fn update_refresh_failure_cooldown(
        &self,
        resource_key: &SharedResourceRequestKey,
        inputs: SharedCacheRaceInputs,
        response: &Response,
    ) {
        if inputs.is_background_refresh() {
            if response.error.is_some()
                && self
                    .resource_cache
                    .is_stale_while_revalidating(resource_key)
            {
                self.refresh_failure_cooldown
                    .insert(Arc::clone(resource_key), ());
            } else if response.error.is_none() {
                self.refresh_failure_cooldown.invalidate(resource_key);
            }
        }
    }

    /// Performs at most one short retry for transient failures (transport,
    /// 5xx, 408/429), then returns the final error to MapLibre. Still renders
    /// complete on required-resource errors; keeping the callback pending past
    /// the renderer SLA would only turn an upstream failure into a wedged slot.
    async fn fetch_with_retries(
        &self,
        request: &ResourceRequest,
        resource_key: &SharedResourceRequestKey,
    ) -> Response {
        let lane = priority_label(request.priority);
        let retry_started = tokio::time::Instant::now();
        let mut attempt_index = 0usize;
        let mut retry_evidence = None;
        // A first attempt is promoted to provisional external evidence only
        // after it is actually on the network and remains slow. Admission
        // wait and ordinary fast traffic are not provider-failure evidence.
        loop {
            let attempt = {
                // Hold an admission permit only while network I/O can happen;
                // backoff sleeps must not park lane capacity.
                let admission_started = std::time::Instant::now();
                let _permit = self.acquire(request).await;
                fs_metrics()
                    .admission_wait_seconds
                    .with_label_values(&[kind_label(request.kind), lane])
                    .observe(admission_started.elapsed().as_secs_f64());
                let _inflight = InflightGuard::new(lane);
                self.fetch_once(
                    request,
                    resource_key,
                    attempt_index == 0 && tracks_provider_health(request.priority),
                )
                .await
            };
            if let Some(ttl) = attempt.negative_cache_ttl
                && uses_shared_cache(request.storage_policy)
            {
                self.negative_cache.insert(
                    Arc::clone(resource_key),
                    NegativeCacheEntry {
                        response: attempt.response.clone(),
                        expires_at: Instant::now() + ttl,
                    },
                );
                fs_metrics()
                    .negative_cache_total
                    .with_label_values(&[kind_label(request.kind), "insert"])
                    .inc();
            }

            let Some(retry) = attempt.retry else {
                return self.finish_fetch_sequence(request, resource_key, attempt);
            };

            if tracks_provider_health(request.priority) {
                retry_evidence.get_or_insert_with(|| self.provider_health.begin_retry());
            }

            let delay = retry
                .delay
                .unwrap_or_else(|| retry_delay(&request.url, attempt_index));
            if !retry_fits_budget(attempt_index + 1, retry_started.elapsed(), delay) {
                return self.finish_fetch_sequence(request, resource_key, attempt);
            }
            fs_metrics()
                .retries_total
                .with_label_values(&[kind_label(request.kind), retry.reason])
                .inc();
            tokio::time::sleep(delay).await;
            attempt_index += 1;
        }
    }

    fn finish_fetch(
        &self,
        resource_key: &SharedResourceRequestKey,
        attempt: FetchAttempt,
    ) -> Response {
        let FetchAttempt {
            response,
            cache_policy,
            cache_response,
            ..
        } = attempt;
        match cache_policy {
            CachePolicy::Store { freshness } => {
                // A 304 path already owns a separate materialized cache value;
                // move it instead of cloning its potentially large body again.
                let cached = cache_response.unwrap_or_else(|| response.clone());
                self.resource_cache
                    .store(Arc::clone(resource_key), cached, freshness);
            }
            CachePolicy::Remove => self.resource_cache.invalidate(resource_key),
            CachePolicy::Unchanged => {}
        }
        response
    }

    async fn fetch_once(
        &self,
        request: &ResourceRequest,
        resource_key: &SharedResourceRequestKey,
        track_provider_health: bool,
    ) -> FetchAttempt {
        let mut metrics = UpstreamAttemptObservation::new(request);
        let attempt = {
            let mut network_io = NetworkIoObservation::new(
                &self.provider_health,
                &mut metrics,
                track_provider_health,
            );
            self.fetch_once_inner(request, resource_key, &mut network_io)
                .await
        };
        let outcome = attempt
            .retry
            .as_ref()
            .map_or_else(|| outcome_label(&attempt.response), |retry| retry.reason);
        metrics.outcome = outcome;
        attempt
    }

    async fn fetch_once_inner(
        &self,
        request: &ResourceRequest,
        resource_key: &SharedResourceRequestKey,
        network_io: &mut NetworkIoObservation<'_>,
    ) -> FetchAttempt {
        let mut network_budget = NetworkAttemptBudget::new();
        // MLN's background revalidation deliberately omits `prior_data` from
        // the cache-enabled Network request because the consumer has already
        // received the cached body. Retain that body in Rust so a 304 can
        // refresh the shared cache while crossing the native bridge as a
        // bodyless notModified. NetworkOnly must not import that body, though
        // its caller-supplied validators remain valid without one.
        let cached =
            may_consult_shared_cache(request.storage_policy, request.loading_methods.has_cache())
                .then(|| self.resource_cache.lookup_shared(resource_key))
                .flatten();
        let prior = prior_response_with_cache(
            request,
            cached.as_ref().map(|entry| &entry.response),
            cached
                .as_ref()
                .map(|entry| entry.freshness())
                .unwrap_or_default(),
        );
        let mut builder = self.client.get(&request.url);
        if let Some(range) = &request.data_range {
            builder = builder.header(RANGE, format!("bytes={}-{}", range.start(), range.end()));
        }
        match conditional_validator(prior) {
            Some(ConditionalValidator::Etag(etag)) => {
                builder = builder.header(IF_NONE_MATCH, etag);
            }
            Some(ConditionalValidator::Modified(modified)) => {
                builder = builder.header(IF_MODIFIED_SINCE, httpdate::fmt_http_date(modified));
            }
            None => {}
        }

        let mut response = match network_io.run(&mut network_budget, builder.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                tracing::debug!(
                    kind = kind_label(request.kind),
                    error_kind = reqwest_error_label(&error),
                    resource_url = redacted_url_str(&request.url),
                    "resource request transport failed"
                );
                return FetchAttempt::retryable(
                    response_from_reqwest_error(&error),
                    "transport",
                    None,
                );
            }
            Err(_) => {
                tracing::debug!(
                    kind = kind_label(request.kind),
                    resource_url = redacted_url_str(&request.url),
                    "resource request timed out"
                );
                return FetchAttempt::retryable(request_timeout_response(), "timeout", None);
            }
        };
        let status = response.status().as_u16();
        let headers = std::mem::take(response.headers_mut());
        if status == 304 {
            return not_modified_attempt(request.kind, request.storage_policy, &headers, prior);
        }
        if status != 200 && status != 206 {
            tracing::debug!(
                kind = kind_label(request.kind),
                status,
                resource_url = redacted_url_str(&request.url),
                // Names only, never values: a provider refusal is usually either
                // "wrong resource" or "no credential travelled", and the second
                // is indistinguishable from the first once the query is dropped.
                query_keys = redacted_query_keys(&request.url),
                "resource provider returned a non-success status"
            );
            let mapped = response_from_http(status, &headers, Vec::new(), request.kind, prior);
            if let Some(retry) = retry_directive(status, &headers) {
                return FetchAttempt {
                    response: mapped,
                    cache_response: None,
                    retry: Some(retry),
                    negative_cache_ttl: None,
                    cache_policy: CachePolicy::Unchanged,
                };
            }
            return FetchAttempt {
                response: mapped,
                cache_response: None,
                retry: None,
                negative_cache_ttl: negative_cache_ttl(
                    status,
                    request.kind,
                    request.storage_policy,
                    &headers,
                    NEGATIVE_CACHE_TTL,
                ),
                cache_policy: CachePolicy::Unchanged,
            };
        }

        let max_resource_bytes = max_resource_bytes(request.kind);
        if let Some(length) = response.content_length()
            && length > max_resource_bytes
        {
            return FetchAttempt::done(Response::error(
                ErrorReason::Other,
                format!("resource body too large: {length} bytes"),
            ));
        }
        let body_wait_started = std::time::Instant::now();
        let _body_permit = self
            .bodies
            .acquire()
            .await
            .expect("file source body semaphore is never closed");
        fs_metrics()
            .body_wait_seconds
            .with_label_values(&[kind_label(request.kind)])
            .observe(body_wait_started.elapsed().as_secs_f64());
        let _body_inflight = BodyInflightGuard::new(request.kind);
        let mut body = Vec::with_capacity(
            response
                .content_length()
                .unwrap_or_default()
                .min(max_resource_bytes) as usize,
        );
        loop {
            match network_io.run(&mut network_budget, response.chunk()).await {
                Ok(Ok(Some(chunk))) => {
                    let Some(new_len) = body.len().checked_add(chunk.len()) else {
                        return FetchAttempt::done(Response::error(
                            ErrorReason::Other,
                            "resource body too large",
                        ));
                    };
                    if new_len > max_resource_bytes as usize {
                        return FetchAttempt::done(Response::error(
                            ErrorReason::Other,
                            format!("resource body exceeds {max_resource_bytes} bytes"),
                        ));
                    }
                    body.extend_from_slice(&chunk);
                }
                Ok(Ok(None)) => break,
                Ok(Err(error)) => {
                    tracing::debug!(
                        kind = kind_label(request.kind),
                        error_kind = reqwest_error_label(&error),
                        resource_url = redacted_url_str(&request.url),
                        "resource response body failed"
                    );
                    return FetchAttempt::retryable(
                        response_from_reqwest_error(&error),
                        "transport",
                        None,
                    );
                }
                Err(_) => {
                    tracing::debug!(
                        kind = kind_label(request.kind),
                        resource_url = redacted_url_str(&request.url),
                        "resource response body timed out"
                    );
                    return FetchAttempt::retryable(request_timeout_response(), "timeout", None);
                }
            }
        }
        FetchAttempt::done_with_cache(
            response_from_http(
                status,
                &headers,
                body,
                request.kind,
                PriorResponse::default(),
            ),
            cache_policy_for_response(request.storage_policy, &headers),
        )
    }
}

pub fn provider_health() -> ProviderHealthTracker {
    static HEALTH: OnceLock<ProviderHealthTracker> = OnceLock::new();
    HEALTH.get_or_init(ProviderHealthTracker::new).clone()
}

fn build_filtered_http_client(
    url_policy: ResourceUrlPolicy,
    user_agent: &str,
) -> anyhow::Result<reqwest::Client> {
    let redirect_policy = url_policy.clone();
    Ok(reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .user_agent(user_agent)
        // Keep address filtering authoritative; an environment proxy could
        // otherwise resolve blocked destinations outside this process.
        .no_proxy()
        .dns_resolver(FilteringResolver::new(url_policy))
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= 10 {
                attempt.error("too many resource redirects")
            } else if !credentialed_redirect_stays_on_origin(attempt.previous(), attempt.url()) {
                attempt.error("credential-bearing resource redirect changes origin")
            } else if redirect_policy.permits_url_without_dns(attempt.url()) {
                attempt.follow()
            } else {
                attempt.error("resource redirect target is blocked")
            }
        }))
        .build()?)
}

fn credentialed_redirect_stays_on_origin(previous: &[url::Url], target: &url::Url) -> bool {
    previous
        .iter()
        .find(|url| url.query_pairs().any(|(key, _)| key == "access_token"))
        .is_none_or(|credentialed| credentialed.origin() == target.origin())
}

/// Build the same address- and redirect-filtered client for the profile
/// preparer's style/TileJSON fetches. Those requests happen before MapLibre's
/// FileSource waterfall but must enforce the identical SSRF boundary.
pub fn build_profile_http_client(
    url_policy: ResourceUrlPolicy,
    user_agent: &str,
) -> anyhow::Result<reqwest::Client> {
    build_filtered_http_client(url_policy, user_agent)
}

struct FetchAttempt {
    response: Response,
    retry: Option<RetryDirective>,
    negative_cache_ttl: Option<Duration>,
    cache_policy: CachePolicy,
    cache_response: Option<Response>,
}

impl FetchAttempt {
    fn done(response: Response) -> Self {
        Self {
            response,
            retry: None,
            negative_cache_ttl: None,
            cache_policy: CachePolicy::Unchanged,
            cache_response: None,
        }
    }

    fn done_with_cache(response: Response, cache_policy: CachePolicy) -> Self {
        Self {
            response,
            retry: None,
            negative_cache_ttl: None,
            cache_policy,
            cache_response: None,
        }
    }

    fn done_with_cache_response(
        response: Response,
        cache_policy: CachePolicy,
        cache_response: Response,
    ) -> Self {
        Self {
            response,
            retry: None,
            negative_cache_ttl: None,
            cache_policy,
            cache_response: Some(cache_response),
        }
    }

    fn retryable(response: Response, reason: &'static str, delay: Option<Duration>) -> Self {
        Self {
            response,
            retry: Some(RetryDirective { reason, delay }),
            negative_cache_ttl: None,
            cache_policy: CachePolicy::Unchanged,
            cache_response: None,
        }
    }
}

fn not_modified_attempt(
    kind: ResourceKind,
    storage_policy: maplibre_native::file_source::StoragePolicy,
    headers: &reqwest::header::HeaderMap,
    prior: PriorResponse<'_>,
) -> FetchAttempt {
    let not_modified = response_from_http(304, headers, Vec::new(), kind, prior);
    let Some(materialized) = materialize_not_modified(&not_modified, prior) else {
        return if prior.etag.is_some() || prior.modified.is_some() {
            // A validator without a prior body means native delivered the
            // representation to its consumer and holds this request only for
            // revalidation. A bodyless 304 is the correct answer there: the
            // consumer treats `notModified` as "keep what you have".
            FetchAttempt::done(not_modified)
        } else {
            FetchAttempt::done(Response::error(
                ErrorReason::Other,
                "HTTP 304 received without a prior validator",
            ))
        };
    };
    let cache_policy = cache_policy_for_not_modified(storage_policy, headers, prior.freshness);
    if prior.native_data.is_some() {
        // A prior body on the request means native withheld the representation
        // for revalidation — the consumer does NOT have it. The stock
        // OnlineFileSource merges `priorData` into the 304 before anyone
        // downstream sees it (`online_file_source.cpp`:
        // `if (response.notModified && resource.priorData)`), and
        // glyph/sprite/tile consumers rely on that: they treat `notModified`
        // as a no-op without completing their load. This source replaced that
        // layer, so it must perform the same merge; returning the bodyless 304
        // here parks the consumer's load forever.
        return FetchAttempt::done_with_cache_response(
            materialized.clone(),
            cache_policy,
            materialized,
        );
    }
    // The body exists only in the process-wide Rust cache: native delivered
    // its copy to the consumer already (a background refresh). A bodyless 304
    // is the correct native answer — re-sending the body would make consumers
    // re-parse an unchanged representation — while the Rust cache still takes
    // the materialized entry to refresh its own metadata.
    FetchAttempt::done_with_cache_response(not_modified, cache_policy, materialized)
}

impl TokioFileSource for NetworkFileSource {
    fn can_request(&self, request: &ResourceRequest) -> bool {
        note_caller_thread();
        request.loading_methods.has_network()
            && url::Url::parse(&request.url)
                .is_ok_and(|url| self.url_policy.permits_url_without_dns(&url))
    }

    async fn request(&self, request: ResourceRequest) -> Response {
        let mut observation = RequestObservation::new(&request);
        let negative_key = Arc::new(ResourceRequestKey::from_request(&request));
        // MainResourceLoader normally serves a usable Database response and
        // keeps the paired Network request alive as a background refresh. A
        // concurrent renderer can, however, populate the shared cache after
        // this request's Database lookup missed. That request has no prior
        // representation and must receive the cached body immediately. A stale
        // Database hit also needs that body: `priorData` means native withheld the
        // stale representation for revalidation. Only a metadata-only prior state
        // proves that Database already delivered a usable body and is safe to park.
        // NetworkOnly remains an explicit refresh and bypasses this path.
        if let Some(response) = self
            .resolve_shared_cache_race(
                &negative_key,
                SharedCacheRaceInputs::from_request(&request),
                &mut observation,
            )
            .await
        {
            return response;
        }
        if uses_shared_cache(request.storage_policy)
            && request.loading_methods.has_cache()
            && let Some(entry) = self.negative_cache.get(&negative_key)
        {
            if entry.expires_at > Instant::now() {
                fs_metrics()
                    .negative_cache_total
                    .with_label_values(&[kind_label(request.kind), "hit"])
                    .inc();
                observation.complete(&entry.response);
                // A negative hit is a distinct operational fact, so it keeps its own
                // outcome rather than the response's generic label.
                observation.outcome = "negative_cache_hit";
                return entry.response;
            }
            self.negative_cache.invalidate(&negative_key);
        }

        if let Some(response) = self.resolve_refresh_failure_cooldown(
            &negative_key,
            SharedCacheRaceInputs::from_request(&request),
            &mut observation,
        ) {
            return response;
        }

        // NetworkAttemptBudget applies REQUEST_TIMEOUT to each actual HTTP
        // attempt. Do not include semaphore/single-flight admission time: a
        // cold burst must not turn queued requests into synthetic network
        // timeouts before they ever reach the provider.
        let response = self
            .fetch_coalesced(&request, Arc::clone(&negative_key))
            .await;
        observation.complete(&response);
        response
    }
}

#[derive(Clone, Copy, Debug)]
struct SharedCacheRaceInputs {
    consults_shared_cache: bool,
    prior_data_present: bool,
    prior_metadata_present: bool,
    kind: &'static str,
    /// Typed rather than derived from the metrics label: policy must not depend on
    /// the spelling of a monitoring string. An unexpected spelling would fail safe
    /// (serve immediately), but the coupling is still wrong.
    is_low_priority: bool,
    /// `Low` alone does not mean "body already delivered". `offline_download.cpp`
    /// also creates low-priority requests, and those are awaiting bytes. Biei only
    /// renders online today, so that path is unreachable here — but this type is a
    /// reusable library, and the condition costs nothing.
    is_online: bool,
    minimum_update_interval: Duration,
}

impl SharedCacheRaceInputs {
    fn from_request(request: &ResourceRequest) -> Self {
        Self {
            consults_shared_cache: uses_shared_cache(request.storage_policy)
                && request.loading_methods.has_cache(),
            prior_data_present: request.prior_data.is_some(),
            prior_metadata_present: request.prior_etag.is_some()
                || request.prior_modified.is_some()
                || request.prior_expires.is_some(),
            kind: kind_label(request.kind),
            is_low_priority: request.priority == Priority::Low,
            is_online: request.usage == Usage::Online,
            minimum_update_interval: request.minimum_update_interval,
        }
    }

    fn is_background_refresh(self) -> bool {
        self.consults_shared_cache
            && !self.prior_data_present
            && self.prior_metadata_present
            && self.is_low_priority
            && self.is_online
    }
}

enum FreshCacheResolution {
    Serve(Response),
    Defer(SystemTime),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeDatabaseState {
    /// This request's Database lookup missed, so no representation was delivered.
    Miss,
    /// Database delivered a usable representation before starting Network.
    Delivered,
    /// Database found an unusable representation and withheld it for revalidation.
    WithheldForRevalidation,
}

impl NativeDatabaseState {
    /// Bounded label so a reproduction can tell a legitimate background refresh
    /// from a callback that was still blocking a render.
    fn label(self) -> &'static str {
        match self {
            Self::Miss => "miss",
            Self::Delivered => "delivered",
            Self::WithheldForRevalidation => "withheld",
        }
    }
}

fn native_database_state(
    prior_data_present: bool,
    prior_metadata_present: bool,
) -> NativeDatabaseState {
    // MainResourceLoader sets `priorData` only when the Database response was not
    // usable and therefore was not delivered to the requester. A usable response
    // is delivered immediately and only its validators/freshness metadata are
    // copied into the paired low-priority Network request.
    if prior_data_present {
        NativeDatabaseState::WithheldForRevalidation
    } else if prior_metadata_present {
        NativeDatabaseState::Delivered
    } else {
        NativeDatabaseState::Miss
    }
}

fn resolve_fresh_cache_race(
    cached: &Response,
    native_database_state: NativeDatabaseState,
    is_background_refresh: bool,
) -> FreshCacheResolution {
    // Parking requires every signal to agree. `Delivered` is inferred from field
    // presence; mbgl demotes the paired request to `Low` on exactly the same branch
    // that delivers the body, so a request still carrying `regular` priority was not
    // produced by that branch and something is waiting on it. `Low` on its own is
    // not sufficient either — `offline_download.cpp` sets it on requests that are
    // awaiting bytes — so the caller also requires online usage.
    //
    // Fully cold processes never expose the difference: they hold nothing to
    // revalidate, so every `Delivered` classification there really is the
    // background refresh. A long-lived process revalidating aged entries is where
    // the two signals can disagree, and parking such a request stalls the render
    // behind it for up to `MAX_REFRESH_DEFERRAL`.
    if native_database_state == NativeDatabaseState::Delivered && is_background_refresh {
        return FreshCacheResolution::Defer(
            cached
                .expires
                .expect("fresh cache responses have an explicit expiry"),
        );
    }
    FreshCacheResolution::Serve(cached.clone())
}

#[derive(Debug, PartialEq, Eq)]
struct RefreshDeferral {
    wait: Duration,
    capped: bool,
}

fn refresh_deferral(expires: SystemTime, minimum_update_interval: Duration) -> RefreshDeferral {
    let requested = expires
        .duration_since(SystemTime::now())
        .unwrap_or_default()
        .max(minimum_update_interval);
    RefreshDeferral {
        wait: requested.min(MAX_REFRESH_DEFERRAL),
        capped: requested > MAX_REFRESH_DEFERRAL,
    }
}

fn complete_deferred_refresh(
    deferral: &RefreshDeferral,
    current_expires: Option<SystemTime>,
) -> Option<Response> {
    if !deferral.capped && current_expires.is_none() {
        return None;
    }
    let mut response = Response::not_modified();
    response.expires = current_expires;
    Some(response)
}

fn uses_shared_cache(storage_policy: maplibre_native::file_source::StoragePolicy) -> bool {
    matches!(
        storage_policy,
        maplibre_native::file_source::StoragePolicy::Permanent
    )
}

fn tracks_provider_health(priority: Priority) -> bool {
    priority != Priority::Low
}

fn may_consult_shared_cache(
    storage_policy: maplibre_native::file_source::StoragePolicy,
    cache_loading_allowed: bool,
) -> bool {
    cache_loading_allowed && uses_shared_cache(storage_policy)
}

fn conditional_validator(prior: PriorResponse<'_>) -> Option<ConditionalValidator<'_>> {
    prior
        .etag
        .map(ConditionalValidator::Etag)
        .or_else(|| prior.modified.map(ConditionalValidator::Modified))
}

/// Registers the Rust network and database file sources with MapLibre Native.
/// Idempotent; must run inside the long-lived tokio runtime and before the
/// first renderer is constructed (re-registration does not update cached mbgl
/// file sources).
pub fn register_file_sources(
    resource_cache_capacity_bytes: u64,
    private_hosts: Vec<String>,
    io_permits: FileSourceIoPermits,
    user_agent: &str,
) -> anyhow::Result<()> {
    let io_permits = io_permits.clamped();
    static REGISTRATION: OnceLock<Result<(), String>> = OnceLock::new();
    REGISTRATION
        .get_or_init(|| {
            let resource_cache = cache::ResourceCache::new(resource_cache_capacity_bytes);
            let source = NetworkFileSource::new(
                resource_cache.clone(),
                private_hosts.clone(),
                io_permits,
                provider_health(),
                user_agent,
            )
            .map_err(|error| error.to_string())?;
            register_tokio_file_source(FileSourceType::Network, source);
            register_tokio_file_source(
                FileSourceType::Database,
                cache::DatabaseFileSource::new(resource_cache),
            );
            mark_metrics_started();
            Ok(())
        })
        .clone()
        .map_err(anyhow::Error::msg)?;
    tracing::info!(
        connect_timeout_ms = CONNECT_TIMEOUT.as_millis() as u64,
        request_timeout_ms = REQUEST_TIMEOUT.as_millis() as u64,
        regular_permits = io_permits.regular,
        low_priority_permits = LOW_PRIORITY_PERMITS,
        body_permits = io_permits.body,
        max_retry_delay_ms = MAX_RETRY_DELAY.as_millis() as u64,
        retry_window_ms = RETRY_WINDOW.as_millis() as u64,
        negative_cache_ttl_ms = NEGATIVE_CACHE_TTL.as_millis() as u64,
        negative_cache_capacity = NEGATIVE_CACHE_CAPACITY,
        resource_cache_capacity_bytes,
        private_resource_hosts = private_hosts.len(),
        "registered Rust network and database file sources for MapLibre Native"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
