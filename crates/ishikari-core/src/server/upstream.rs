//! Shared bounded upstream fetch helpers for provider resources.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::http::StatusCode;
use axum::http::{HeaderMap, HeaderValue, header};
use bytes::{Bytes, BytesMut};
use moka::sync::Cache;
use object_store::{Attribute, Error as ObjectStoreError, GetOptions};
use reqwest::{Client, redirect};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::{
    http_client::representation_preserving_builder,
    metrics::NodeMetrics,
    server::{
        AppState, HttpError,
        conditional::Validators,
        provider_body::{
            BodyValidation, decode_provider_body, validate_body, validate_content_type,
        },
        provider_cache_policy::{CachePolicy, cache_policy, has_explicit_freshness},
    },
    singleflight::{Flight, LeaderGuard, SingleFlight},
    storage::{
        InternalFetchResponse, ObjectStoreRegistry, PROVIDER_AGE_HEADER,
        PROVIDER_CACHE_CONTROL_HEADER, PROVIDER_ETAG_HEADER, PROVIDER_LAST_MODIFIED_HEADER,
    },
};

const PROVIDER_RESOURCE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Provider resources are much larger than PMTiles index reads. Bound active
/// bodies process-wide so many distinct URLs cannot bypass per-key
/// single-flight and consume unbounded memory.
const PROVIDER_FETCH_CONCURRENCY: usize = 16;
const PROVIDER_FETCH_MAX_INFLIGHT: usize = 128;
const NEGATIVE_TTL: Duration = Duration::from_secs(30);
/// Bounded so a slow or hung upstream cannot pin request tasks indefinitely
/// (mirrors the tile backend fetch timeout).
const PROVIDER_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
enum ProviderFlightOutcome {
    Error(HttpError),
    /// A successful representation that intentionally was not retained in the
    /// shared cache. Current followers may still reuse it.
    Uncached(ProviderResource),
}

struct FetchedProviderResource {
    bytes: Bytes,
    policy: CachePolicy,
    validators: Validators,
    content_encoding: Option<Arc<str>>,
    initial_age: Duration,
}

/// Result of an origin request. A conditional hit carries a rebuilt cache entry
/// around the previously validated body, so it follows the same insertion path
/// without downloading or re-validating the representation bytes.
enum ProviderOriginOutcome {
    Modified(FetchedProviderResource),
    NotModified(FetchedProviderResource),
}

#[derive(Clone)]
struct CachedProviderRepresentation {
    bytes: Bytes,
    cache_control: Arc<str>,
    validators: Validators,
    content_encoding: Option<Arc<str>>,
}

struct ProviderFetchSlot {
    inflight: Arc<AtomicUsize>,
}

impl ProviderFetchSlot {
    fn try_reserve(inflight: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        let previous = inflight.fetch_add(1, Ordering::Relaxed);
        if previous >= max {
            inflight.fetch_sub(1, Ordering::Relaxed);
            None
        } else {
            Some(Self {
                inflight: Arc::clone(inflight),
            })
        }
    }
}

impl Drop for ProviderFetchSlot {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

struct ProviderFetchPermit {
    _permit: OwnedSemaphorePermit,
    _slot: ProviderFetchSlot,
}

/// HTTP client for direct provider fetches. Redirects are disabled: provider
/// upstreams answer directly, and following a redirect would let a compromised
/// or open-redirecting upstream steer the fetch at cluster-internal or
/// link-local addresses (e.g. cloud metadata) that the internal-listener
/// isolation otherwise fences off. The per-request deadline still bounds the
/// whole fetch, but a connect timeout fails a black-hole host faster.
///
/// `Content-Encoding` is preserved as representation metadata and decoded
/// explicitly, so transparent transfer decompression must stay off. Disable it
/// on the client rather than relying on Cargo feature isolation: workspace-wide
/// builds also compile Biei, which intentionally enables some of these features.
fn provider_http_client() -> Client {
    representation_preserving_builder()
        .redirect(redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("provider HTTP client builds")
}

/// Provider bytes plus the cache metadata that must survive peer forwarding.
#[derive(Clone)]
pub(crate) struct ProviderResource {
    bytes: Bytes,
    cache_control: Arc<str>,
    age_seconds: u64,
    validators: Validators,
    content_encoding: Option<Arc<str>>,
}

impl ProviderResource {
    fn fetched(fetched: &FetchedProviderResource) -> Self {
        Self {
            bytes: fetched.bytes.clone(),
            cache_control: Arc::clone(&fetched.policy.response_cache_control),
            age_seconds: fetched.initial_age.as_secs(),
            validators: fetched.validators.clone(),
            content_encoding: fetched.content_encoding.clone(),
        }
    }

    pub(crate) fn from_peer(response: InternalFetchResponse) -> Self {
        Self {
            bytes: response.bytes,
            // Older peers do not send provider metadata. Avoid making their
            // response cacheable under a potentially incompatible policy.
            cache_control: Arc::from(
                response
                    .provider_cache_control
                    .unwrap_or_else(|| "no-cache".to_string()),
            ),
            age_seconds: response.provider_age_seconds.unwrap_or(0),
            validators: Validators::new(
                response.provider_etag.map(Arc::from),
                response
                    .provider_last_modified
                    .as_deref()
                    .and_then(|value| httpdate::parse_http_date(value).ok()),
            ),
            content_encoding: response.content_encoding.map(Arc::from),
        }
    }

    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the decoded representation for server-side transformation.
    /// Byte-identical glyph/sprite responses keep their original encoding;
    /// styles must be decoded before JSON parsing and rewriting.
    pub(crate) fn decoded_bytes(
        &self,
        max_bytes: usize,
        resource: &'static str,
    ) -> Result<Bytes, HttpError> {
        decode_provider_body(
            &self.bytes,
            self.content_encoding.as_deref(),
            max_bytes,
            resource,
        )
    }

    /// Replaces the upstream validators for a derived representation whose
    /// bytes differ from the upstream body (e.g. rewritten style JSON).
    pub(crate) fn with_derived_validators(mut self, validators: Validators) -> Self {
        self.validators = validators;
        // The derived style body is serialized as an identity representation.
        self.content_encoding = None;
        self
    }

    /// Whether a conditional request matches this representation (serve `304`).
    pub(crate) fn not_modified(&self, request: &HeaderMap) -> bool {
        self.validators.not_modified(request)
    }

    /// `304 Not Modified` for a matched conditional request: no body, and no
    /// representation metadata (`Content-Encoding`). It carries the cache
    /// metadata and validators that a `200` would (RFC 9110 §15.4.5).
    pub(crate) fn not_modified_response(&self) -> axum::response::Response {
        let mut response = axum::response::Response::new(axum::body::Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        self.apply_cache_metadata(response.headers_mut());
        response
    }

    pub(crate) fn apply_public_headers(&self, headers: &mut HeaderMap) {
        self.apply_cache_metadata(headers);
        self.apply_content_encoding(headers);
    }

    /// `Cache-Control`, `Age`, and validators — the metadata shared by a `200`
    /// body response and its `304`. Excludes representation headers.
    fn apply_cache_metadata(&self, headers: &mut HeaderMap) {
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_bytes(self.cache_control.as_bytes())
                .expect("cache policy originated from a valid HTTP header"),
        );
        headers.insert(
            header::AGE,
            HeaderValue::from_str(&self.age_seconds.to_string()).expect("age is numeric"),
        );
        self.validators.apply(headers);
    }

    pub(crate) fn apply_internal_headers(&self, headers: &mut HeaderMap) {
        headers.insert(
            PROVIDER_CACHE_CONTROL_HEADER,
            HeaderValue::from_bytes(self.cache_control.as_bytes())
                .expect("cache policy originated from a valid HTTP header"),
        );
        headers.insert(
            PROVIDER_AGE_HEADER,
            HeaderValue::from_str(&self.age_seconds.to_string()).expect("age is numeric"),
        );
        if let Some(etag) = self.validators.etag()
            && let Ok(value) = HeaderValue::from_str(etag)
        {
            headers.insert(PROVIDER_ETAG_HEADER, value);
        }
        if let Some(http_date) = self.validators.last_modified_http_date()
            && let Ok(value) = HeaderValue::from_str(&http_date)
        {
            headers.insert(PROVIDER_LAST_MODIFIED_HEADER, value);
        }
        self.apply_content_encoding(headers);
    }

    fn apply_content_encoding(&self, headers: &mut HeaderMap) {
        if let Some(encoding) = &self.content_encoding
            && let Ok(value) = HeaderValue::from_str(encoding)
        {
            headers.insert(header::CONTENT_ENCODING, value);
        }
    }
}

#[derive(Clone)]
pub(crate) struct ProviderFetchCache {
    entries: Cache<ProviderFetchCacheKey, CachedProviderFetch>,
    inflight: SingleFlight<ProviderFetchCacheKey, ProviderFlightOutcome>,
    http_client: Client,
    fetch_semaphore: Arc<Semaphore>,
    fetch_inflight: Arc<AtomicUsize>,
}

impl ProviderFetchCache {
    pub(crate) fn new() -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(PROVIDER_RESOURCE_CACHE_MAX_BYTES)
                .weigher(provider_fetch_cache_weight)
                .build(),
            inflight: SingleFlight::default(),
            http_client: provider_http_client(),
            fetch_semaphore: Arc::new(Semaphore::new(PROVIDER_FETCH_CONCURRENCY)),
            fetch_inflight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns the cached entry with its freshness. Fully expired entries are
    /// dropped and reported as a miss so a background refresh cannot resurrect
    /// bytes past their stale window.
    fn get(&self, key: &ProviderFetchCacheKey) -> Option<(CachedProviderFetch, Freshness)> {
        let entry = self.entries.get(key)?;
        match entry.freshness() {
            Freshness::Expired => {
                self.entries.invalidate(key);
                None
            }
            freshness => Some((entry, freshness)),
        }
    }

    fn put_found(&self, key: ProviderFetchCacheKey, fetched: &FetchedProviderResource) -> bool {
        if !fetched.policy.store {
            // A successful refresh can tighten an existing stale entry to
            // `no-store`/`private`/`no-cache`. Remove that old body promptly.
            self.entries.invalidate(&key);
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
            self.entries.invalidate(&key);
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

    fn put_not_found(&self, key: ProviderFetchCacheKey) {
        let fresh_until = Instant::now() + NEGATIVE_TTL;
        self.entries.insert(
            key,
            CachedProviderFetch::NotFound {
                fresh_until,
                stale_until: fresh_until,
            },
        );
    }

    fn begin_fetch(
        &self,
        key: ProviderFetchCacheKey,
    ) -> Flight<ProviderFetchCacheKey, ProviderFlightOutcome> {
        self.inflight.begin(key)
    }

    async fn admit_fetch(&self, resource: &'static str) -> Result<ProviderFetchPermit, HttpError> {
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

    pub(crate) fn weighted_size(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.weighted_size()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ProviderFetchCacheKey {
    resource: &'static str,
    validation: Arc<str>,
    body_validation: BodyValidation,
    url: Arc<str>,
}

impl ProviderFetchCacheKey {
    fn new(
        resource: &'static str,
        url: &str,
        accepted_content_types: &[&str],
        body_validation: BodyValidation,
    ) -> Self {
        Self {
            resource,
            validation: Arc::from(validation_key(accepted_content_types)),
            body_validation,
            url: Arc::from(url),
        }
    }
}

/// Freshness of a cached entry relative to its window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Freshness {
    /// Serve directly.
    Fresh,
    /// Past `fresh_until` but within the SWR window: serve, revalidate in the
    /// background. Only reachable for `Found` (negative entries have no SWR).
    Stale,
    /// Past the SWR window: treat as a miss.
    Expired,
}

#[derive(Clone)]
enum CachedProviderFetch {
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
    NotFound {
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
            | Self::NotFound {
                fresh_until,
                stale_until,
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
            } => Ok(ProviderResource {
                bytes,
                cache_control,
                age_seconds: Instant::now()
                    .saturating_duration_since(stored_at)
                    .saturating_add(age_at_insert)
                    .as_secs(),
                validators,
                content_encoding,
            }),
            Self::NotFound { .. } => Err((StatusCode::NOT_FOUND, "not found".to_string())),
        }
    }

    fn cache_outcome(&self, freshness: Freshness) -> &'static str {
        match (self, freshness) {
            (Self::Found { .. }, Freshness::Stale) => "stale_hit",
            (Self::Found { .. }, _) => "hit",
            (Self::NotFound { .. }, _) => "negative_hit",
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
            Self::NotFound { .. } => None,
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
/// hits return the prior body immediately without stacking backend reads.
fn spawn_stale_revalidation(
    state: &AppState,
    key: ProviderFetchCacheKey,
    url: String,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
    body_validation: BodyValidation,
) {
    let Some(stale) = state
        .provider_fetch_cache
        .entries
        .get(&key)
        .and_then(|entry| entry.representation())
    else {
        return;
    };
    let Flight::Leader(guard) = state.provider_fetch_cache.begin_fetch(key.clone()) else {
        // A refresh (or a blocking fetch) is already in flight for this key.
        return;
    };
    let state = state.clone();
    let accepted: Vec<String> = accepted_content_types
        .iter()
        .map(|value| (*value).to_string())
        .collect();
    tokio::spawn(async move {
        let accepted: Vec<&str> = accepted.iter().map(String::as_str).collect();
        let result = fetch_limited_bytes_uncached(
            &state,
            &url,
            max_bytes,
            resource,
            &accepted,
            body_validation,
            Some(&stale),
        )
        .await;
        // The refreshed body (or error) reaches later requests through the cache
        // and the single-flight guard; this task only drives the revalidation.
        let _ = store_leader_result(&state, &key, resource, result, guard);
    });
}

/// Applies a leader (foreground or background) fetch outcome to the cache,
/// records the insert metric, and shares a transient error with followers.
fn store_leader_result(
    state: &AppState,
    key: &ProviderFetchCacheKey,
    resource: &'static str,
    result: Result<ProviderOriginOutcome, HttpError>,
    guard: LeaderGuard<ProviderFetchCacheKey, ProviderFlightOutcome>,
) -> Result<ProviderResource, HttpError> {
    match result {
        Ok(origin) => {
            let (fetched, stored_outcome) = match origin {
                ProviderOriginOutcome::Modified(fetched) => (fetched, "insert"),
                ProviderOriginOutcome::NotModified(fetched) => (fetched, "revalidated"),
            };
            let response = ProviderResource::fetched(&fetched);
            let stored = state.provider_fetch_cache.put_found(key.clone(), &fetched);
            // An uncacheable response was fetched successfully but intentionally
            // not retained. This can also happen when a 304 tightens policy.
            let outcome = if stored {
                stored_outcome
            } else {
                "uncacheable"
            };
            state
                .metrics
                .record_provider_resource_cache(resource, outcome);
            if !stored {
                guard.complete_with(ProviderFlightOutcome::Uncached(response.clone()));
            }
            Ok(response)
        }
        Err(error @ (StatusCode::NOT_FOUND, _)) => {
            state.provider_fetch_cache.put_not_found(key.clone());
            state
                .metrics
                .record_provider_resource_cache(resource, "negative_insert");
            Err(error)
        }
        Err(error) => {
            state
                .metrics
                .record_provider_resource_cache(resource, "error");
            guard.complete_with_error(ProviderFlightOutcome::Error(error.clone()));
            Err(error)
        }
    }
}

pub(crate) async fn fetch_limited_bytes_with_content_type(
    state: &AppState,
    url: String,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
) -> Result<ProviderResource, HttpError> {
    fetch_limited_bytes_with_validation(
        state,
        url,
        max_bytes,
        resource,
        accepted_content_types,
        BodyValidation::Bytes,
    )
    .await
}

pub(crate) async fn fetch_limited_json(
    state: &AppState,
    url: String,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
) -> Result<ProviderResource, HttpError> {
    fetch_limited_bytes_with_validation(
        state,
        url,
        max_bytes,
        resource,
        accepted_content_types,
        BodyValidation::Json,
    )
    .await
}

async fn fetch_limited_bytes_with_validation(
    state: &AppState,
    url: String,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
    body_validation: BodyValidation,
) -> Result<ProviderResource, HttpError> {
    let key = ProviderFetchCacheKey::new(resource, &url, accepted_content_types, body_validation);
    let mut recorded_miss = false;
    let mut joined_singleflight = false;
    loop {
        if let Some((entry, freshness)) = state.provider_fetch_cache.get(&key) {
            // A follower already recorded the request as a miss plus a join.
            // Reading the leader's freshly inserted value is not an independent
            // cache hit and must not inflate cache-hit-ratio dashboards.
            record_cached_provider_fetch(
                &state.metrics,
                resource,
                &entry,
                freshness,
                joined_singleflight,
            );
            if freshness == Freshness::Stale {
                spawn_stale_revalidation(
                    state,
                    key.clone(),
                    url.clone(),
                    max_bytes,
                    resource,
                    accepted_content_types,
                    body_validation,
                );
            }
            return entry.into_result();
        }
        if !recorded_miss {
            state
                .metrics
                .record_provider_resource_cache(resource, "miss");
            recorded_miss = true;
        }

        match state.provider_fetch_cache.begin_fetch(key.clone()) {
            Flight::Leader(guard) => {
                let result = fetch_limited_bytes_uncached(
                    state,
                    &url,
                    max_bytes,
                    resource,
                    accepted_content_types,
                    body_validation,
                    None,
                )
                .await;
                return store_leader_result(state, &key, resource, result, guard);
            }
            Flight::Follower(follower) => {
                // Request-scoped: an uncacheable success stores nothing, so a
                // follower can wake, miss, and follow the next leader. Those
                // internal wait cycles are one joined request, not several.
                if !joined_singleflight {
                    state
                        .metrics
                        .record_provider_resource_cache(resource, "singleflight_join");
                    joined_singleflight = true;
                }
                if let Some(outcome) = follower.wait().await {
                    return match outcome {
                        ProviderFlightOutcome::Error(error) => Err(error),
                        ProviderFlightOutcome::Uncached(resource) => Ok(resource),
                    };
                }
            }
        }
    }
}

async fn fetch_limited_bytes_uncached(
    state: &AppState,
    url: &str,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
    body_validation: BodyValidation,
    revalidate: Option<&CachedProviderRepresentation>,
) -> Result<ProviderOriginOutcome, HttpError> {
    let fetch = async {
        // The one deadline covers queueing, headers, and the complete body. A
        // request cannot consume 15 seconds for each phase independently.
        let _admission = state.provider_fetch_cache.admit_fetch(resource).await?;
        let parsed = Url::parse(url).map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("{resource} upstream URL invalid: {error}"),
            )
        })?;
        let fetched = match parsed.scheme() {
            // object_store's HTTP adapter intentionally normalizes metadata and
            // exposes only one Cache-Control field value. Fetch HTTP directly
            // so Age/Date, repeated Cache-Control, and Content-Encoding survive.
            "http" | "https" => {
                fetch_http_provider(
                    &state.provider_fetch_cache.http_client,
                    parsed,
                    max_bytes,
                    resource,
                    accepted_content_types,
                    revalidate,
                )
                .await?
            }
            _ => {
                fetch_object_store_provider(
                    &state.object_store_registry,
                    &parsed,
                    max_bytes,
                    resource,
                    accepted_content_types,
                    revalidate,
                )
                .await?
            }
        };
        if let ProviderOriginOutcome::Modified(fetched) = &fetched {
            validate_body(
                &fetched.bytes,
                fetched.content_encoding.as_deref(),
                body_validation,
                max_bytes,
                resource,
            )?;
        }
        Ok(fetched)
    };
    tokio::time::timeout(PROVIDER_FETCH_TIMEOUT, fetch)
        .await
        .map_err(|_| {
            (
                StatusCode::GATEWAY_TIMEOUT,
                format!("{resource} upstream timed out"),
            )
        })?
}

async fn fetch_http_provider(
    client: &Client,
    url: Url,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
    revalidate: Option<&CachedProviderRepresentation>,
) -> Result<ProviderOriginOutcome, HttpError> {
    let request_started = Instant::now();
    let mut request = client.get(url);
    if let Some(cached) = revalidate {
        if let Some(etag) = cached.validators.etag() {
            request = request.header(header::IF_NONE_MATCH, etag);
        } else if let Some(last_modified) = cached.validators.last_modified_http_date() {
            request = request.header(header::IF_MODIFIED_SINCE, last_modified);
        }
    }
    let mut response = request.send().await.map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("{resource} upstream GET failed: {error}"),
        )
    })?;
    let status = response.status();
    let headers = response.headers().clone();
    if status == StatusCode::NOT_MODIFIED {
        let cached = revalidate.ok_or_else(|| {
            (
                StatusCode::BAD_GATEWAY,
                format!("{resource} upstream returned an unsolicited 304"),
            )
        })?;
        return Ok(ProviderOriginOutcome::NotModified(
            revalidated_provider_resource(
                cached,
                resource,
                Some(&headers),
                request_started.elapsed(),
            ),
        ));
    }
    if status == StatusCode::NOT_FOUND {
        return Err((StatusCode::NOT_FOUND, "not found".to_string()));
    }
    if !status.is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("{resource} upstream returned {status}"),
        ));
    }
    if response
        .content_length()
        .is_some_and(|size| size > max_bytes as u64)
    {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("{resource} body too large"),
        ));
    }

    validate_content_type(
        header_value(&headers, header::CONTENT_TYPE).as_deref(),
        accepted_content_types,
        resource,
    )?;
    let cache_control = joined_header_values(&headers, header::CACHE_CONTROL);
    let policy = cache_policy(resource, cache_control.as_deref());
    // Age accounting is only meaningful against an upstream-declared lifetime.
    // When the upstream sets no explicit freshness, Ishikari applies its own
    // default TTL, and charging the transported `Age`/`Date` against that
    // invented lifetime would wrongly evict (a CDN-fronted body sending
    // `Age: 900` but no `Cache-Control` would never cache). Match the
    // object-store path and start the clock at fetch time in that case.
    let has_explicit_freshness = has_explicit_freshness(cache_control.as_deref());
    let validators = Validators::new(
        header_value(&headers, header::ETAG).map(Arc::from),
        header_value(&headers, header::LAST_MODIFIED)
            .and_then(|value| httpdate::parse_http_date(&value).ok()),
    );
    let content_encoding = joined_header_values(&headers, header::CONTENT_ENCODING)
        .filter(|value| !value.trim().eq_ignore_ascii_case("identity"))
        .map(Arc::from);
    let mut body = BytesMut::with_capacity(
        response.content_length().unwrap_or(0).min(max_bytes as u64) as usize,
    );
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("{resource} upstream body failed: {error}"),
        )
    })? {
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err((
                StatusCode::BAD_GATEWAY,
                format!("{resource} body too large"),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    // Include body transfer time because the entry cannot be served or stored
    // until the complete bounded representation has arrived.
    let initial_age = if has_explicit_freshness {
        corrected_initial_age(&headers, SystemTime::now(), request_started.elapsed())
    } else {
        Duration::ZERO
    };

    Ok(ProviderOriginOutcome::Modified(FetchedProviderResource {
        bytes: body.freeze(),
        policy,
        validators,
        content_encoding,
        initial_age,
    }))
}

async fn fetch_object_store_provider(
    registry: &ObjectStoreRegistry,
    url: &Url,
    max_bytes: usize,
    resource: &'static str,
    accepted_content_types: &[&str],
    revalidate: Option<&CachedProviderRepresentation>,
) -> Result<ProviderOriginOutcome, HttpError> {
    // `gs://` and `s3://` authenticate with ambient credentials. The registry
    // reuses connection pools and credentials per bucket.
    let (store, path) = registry.resolve(url).map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("{resource} upstream store init failed: {error}"),
        )
    })?;
    let mut options = GetOptions::new();
    if let Some(cached) = revalidate {
        if let Some(etag) = cached.validators.etag() {
            options = options.with_if_none_match(Some(etag));
        } else if let Some(last_modified) = cached.validators.last_modified() {
            options = options.with_if_modified_since(Some(last_modified));
        }
    }
    let result = match store.get_opts(&path, options).await {
        Ok(result) => result,
        Err(ObjectStoreError::NotModified { .. }) if revalidate.is_some() => {
            return Ok(ProviderOriginOutcome::NotModified(
                revalidated_provider_resource(
                    revalidate.expect("checked above"),
                    resource,
                    None,
                    Duration::ZERO,
                ),
            ));
        }
        Err(ObjectStoreError::NotFound { .. }) => {
            return Err((StatusCode::NOT_FOUND, "not found".to_string()));
        }
        Err(other) => {
            return Err((
                StatusCode::BAD_GATEWAY,
                format!("{resource} upstream GET failed: {other}"),
            ));
        }
    };
    if result.meta.size > max_bytes as u64 {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("{resource} body too large"),
        ));
    }
    validate_content_type(
        result
            .attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref()),
        accepted_content_types,
        resource,
    )?;
    let policy = cache_policy(
        resource,
        result
            .attributes
            .get(&Attribute::CacheControl)
            .map(|value| value.as_ref()),
    );
    let last_modified = SystemTime::from(result.meta.last_modified);
    let validators = Validators::new(
        result.meta.e_tag.as_deref().map(Arc::from),
        (last_modified != UNIX_EPOCH).then_some(last_modified),
    );
    let content_encoding = result
        .attributes
        .get(&Attribute::ContentEncoding)
        .map(|value| value.as_ref())
        .filter(|value| !value.trim().eq_ignore_ascii_case("identity"))
        .map(Arc::from);
    let body = result.bytes().await.map_err(|error| {
        (
            StatusCode::BAD_GATEWAY,
            format!("{resource} upstream body failed: {error}"),
        )
    })?;
    if body.len() > max_bytes {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("{resource} body too large"),
        ));
    }
    Ok(ProviderOriginOutcome::Modified(FetchedProviderResource {
        bytes: body,
        policy,
        validators,
        content_encoding,
        initial_age: Duration::ZERO,
    }))
}

fn revalidated_provider_resource(
    cached: &CachedProviderRepresentation,
    resource: &'static str,
    headers: Option<&HeaderMap>,
    response_delay: Duration,
) -> FetchedProviderResource {
    let cache_control = headers
        .and_then(|headers| joined_header_values(headers, header::CACHE_CONTROL))
        .unwrap_or_else(|| cached.cache_control.to_string());
    let policy = cache_policy(resource, Some(&cache_control));
    let validators = Validators::new(
        headers
            .and_then(|headers| header_value(headers, header::ETAG))
            .map(Arc::from)
            .or_else(|| cached.validators.etag_arc()),
        headers
            .and_then(|headers| header_value(headers, header::LAST_MODIFIED))
            .and_then(|value| httpdate::parse_http_date(&value).ok())
            .or_else(|| cached.validators.last_modified()),
    );
    let content_encoding =
        match headers.and_then(|headers| joined_header_values(headers, header::CONTENT_ENCODING)) {
            Some(value) if value.trim().eq_ignore_ascii_case("identity") => None,
            Some(value) => Some(Arc::from(value.trim())),
            None => cached.content_encoding.clone(),
        };
    let initial_age = headers.map_or(Duration::ZERO, |headers| {
        corrected_initial_age(headers, SystemTime::now(), response_delay)
    });
    FetchedProviderResource {
        bytes: cached.bytes.clone(),
        policy,
        validators,
        content_encoding,
        initial_age,
    }
}

fn header_value(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn joined_header_values(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    let values: Vec<&str> = headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect();
    (!values.is_empty()).then(|| values.join(", "))
}

fn corrected_initial_age(
    headers: &HeaderMap,
    response_received: SystemTime,
    response_delay: Duration,
) -> Duration {
    let age_value = headers
        .get_all(header::AGE)
        .iter()
        .filter_map(|value| value.to_str().ok()?.trim().parse::<u64>().ok())
        .max()
        .map(Duration::from_secs)
        .unwrap_or_default();
    let apparent_age = header_value(headers, header::DATE)
        .and_then(|value| httpdate::parse_http_date(&value).ok())
        .and_then(|date| response_received.duration_since(date).ok())
        .unwrap_or_default();
    apparent_age.max(age_value.saturating_add(response_delay))
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

#[cfg(test)]
mod tests {
    use super::{
        BodyValidation, CachedProviderFetch, FetchedProviderResource, Freshness,
        ProviderFetchCache, ProviderFetchCacheKey, ProviderFetchSlot, ProviderResource, Validators,
        cache_policy, corrected_initial_age, record_cached_provider_fetch,
        revalidated_provider_resource,
    };
    use crate::metrics::NodeMetrics;
    use crate::storage::{
        InternalFetchResponse, PROVIDER_AGE_HEADER, PROVIDER_CACHE_CONTROL_HEADER,
        PROVIDER_ETAG_HEADER, PROVIDER_LAST_MODIFIED_HEADER,
    };
    use axum::http::{HeaderMap, StatusCode, header};
    use bytes::Bytes;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime},
    };

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
    fn provider_cache_metadata_survives_internal_and_public_headers() {
        let last_modified = std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let resource = ProviderResource {
            bytes: Bytes::from_static(b"glyph"),
            cache_control: "public, max-age=30, s-maxage=60".into(),
            age_seconds: 12,
            validators: Validators::new(Some("\"v1\"".into()), Some(last_modified)),
            content_encoding: Some("gzip".into()),
        };
        let mut internal = HeaderMap::new();
        resource.apply_internal_headers(&mut internal);
        assert_eq!(
            internal[PROVIDER_CACHE_CONTROL_HEADER],
            "public, max-age=30, s-maxage=60"
        );
        assert_eq!(internal[PROVIDER_AGE_HEADER], "12");
        assert_eq!(internal[PROVIDER_ETAG_HEADER], "\"v1\"");
        let http_date = httpdate::fmt_http_date(last_modified);
        assert_eq!(
            internal[PROVIDER_LAST_MODIFIED_HEADER].to_str().unwrap(),
            http_date
        );

        let header_string = |name: &str| {
            internal
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let peer_resource = ProviderResource::from_peer(InternalFetchResponse {
            bytes: resource.bytes().clone(),
            tile_source: None,
            provider_cache_control: header_string(PROVIDER_CACHE_CONTROL_HEADER),
            provider_age_seconds: header_string(PROVIDER_AGE_HEADER)
                .and_then(|value| value.parse().ok()),
            provider_etag: header_string(PROVIDER_ETAG_HEADER),
            provider_last_modified: header_string(PROVIDER_LAST_MODIFIED_HEADER),
            content_encoding: header_string(header::CONTENT_ENCODING.as_str()),
        });
        let mut public = HeaderMap::new();
        peer_resource.apply_public_headers(&mut public);
        assert_eq!(
            public[header::CACHE_CONTROL],
            "public, max-age=30, s-maxage=60"
        );
        assert_eq!(public[header::AGE], "12");
        assert_eq!(public[header::ETAG], "\"v1\"");
        assert_eq!(public[header::LAST_MODIFIED].to_str().unwrap(), http_date);
        assert_eq!(public[header::CONTENT_ENCODING], "gzip");

        // The forwarded validators still answer conditional requests.
        let mut conditional = HeaderMap::new();
        conditional.insert(header::IF_NONE_MATCH, "\"v1\"".parse().unwrap());
        assert!(peer_resource.not_modified(&conditional));
    }

    #[test]
    fn not_modified_response_omits_representation_metadata() {
        let resource = ProviderResource {
            bytes: Bytes::from_static(b"gzipped"),
            cache_control: "public, max-age=30".into(),
            age_seconds: 7,
            validators: Validators::new(Some("\"v1\"".into()), None),
            content_encoding: Some("gzip".into()),
        };

        // The 200 carries the representation's Content-Encoding.
        let mut ok = HeaderMap::new();
        resource.apply_public_headers(&mut ok);
        assert_eq!(ok[header::CONTENT_ENCODING], "gzip");

        // The 304 carries cache metadata and validators, but not the
        // representation's Content-Encoding (RFC 9110 §15.4.5).
        let response = resource.not_modified_response();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        let headers = response.headers();
        assert_eq!(headers[header::CACHE_CONTROL], "public, max-age=30");
        assert_eq!(headers[header::AGE], "7");
        assert_eq!(headers[header::ETAG], "\"v1\"");
        assert!(headers.get(header::CONTENT_ENCODING).is_none());
    }

    #[test]
    fn peer_without_provider_metadata_is_not_publicly_cacheable() {
        let resource = ProviderResource::from_peer(InternalFetchResponse {
            bytes: Bytes::from_static(b"old peer"),
            tile_source: None,
            provider_cache_control: None,
            provider_age_seconds: None,
            provider_etag: None,
            provider_last_modified: None,
            content_encoding: None,
        });
        let mut headers = HeaderMap::new();
        resource.apply_public_headers(&mut headers);
        assert_eq!(headers[header::CACHE_CONTROL], "no-cache");
        assert_eq!(headers[header::AGE], "0");
        assert!(headers.get(header::ETAG).is_none());
        assert!(headers.get(header::LAST_MODIFIED).is_none());
    }

    #[test]
    fn uncacheable_refresh_invalidates_an_existing_stale_body() {
        let cache = ProviderFetchCache::new();
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
    fn upstream_age_reduces_local_freshness_and_is_emitted() {
        let cache = ProviderFetchCache::new();
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
        assert!(resource.age_seconds >= 45);

        let already_expired = FetchedProviderResource {
            initial_age: Duration::from_secs(60),
            ..fetched
        };
        assert!(!cache.put_found(key.clone(), &already_expired));
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn upstream_age_also_reduces_default_freshness() {
        let cache = ProviderFetchCache::new();
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

    #[test]
    fn stale_entry_reports_stale_then_expired() {
        let now = std::time::Instant::now();
        let entry = CachedProviderFetch::Found {
            bytes: Bytes::from_static(b"x"),
            cache_control: "public, max-age=60".into(),
            validators: Validators::default(),
            content_encoding: None,
            age_at_insert: Duration::ZERO,
            stored_at: now - Duration::from_secs(2),
            fresh_until: now - Duration::from_secs(1),
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
            stored_at: now - Duration::from_secs(3),
            fresh_until: now - Duration::from_secs(2),
            stale_until: now - Duration::from_secs(1),
        };
        assert_eq!(expired.freshness(), Freshness::Expired);
    }
}
