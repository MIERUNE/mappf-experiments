//! Shared bounded upstream fetch helpers for provider resources.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::http::StatusCode;
use bytes::Bytes;
use reqwest::{Client, redirect};
use tokio::sync::OwnedSemaphorePermit;

use crate::http_client::representation_preserving_builder;
use crate::server::{
    HttpError,
    conditional::Validators,
    provider_cache_policy::{CachePolicy, NegativeCachePolicy},
};

mod fetch;

mod cache;
mod resource;

pub(crate) use cache::ProviderFetcher;
pub(crate) use resource::ProviderResource;

/// Provider resources are much larger than PMTiles index reads. Bound active
/// bodies process-wide so many distinct URLs cannot bypass per-key
/// single-flight and consume unbounded memory.
pub(super) const PROVIDER_FETCH_CONCURRENCY: usize = 16;
pub(super) const PROVIDER_FETCH_MAX_INFLIGHT: usize = 128;
/// Bounded so a slow or hung upstream cannot pin request tasks indefinitely
/// (mirrors the tile backend fetch timeout).
pub(super) const PROVIDER_FETCH_TIMEOUT: Duration = Duration::from_secs(15);
/// Failed stale revalidations carry only `HttpError` (`StatusCode` plus a
/// message), so origin `Retry-After` metadata is not available here without a
/// broader transport error redesign. Use a small fixed delay to prevent hot
/// stale keys from retrying at request rate.
pub(super) const PROVIDER_STALE_REVALIDATION_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
/// Failure state is auxiliary to the byte-bounded representation cache. Bound
/// it independently so cache eviction cannot leave an unbounded key set.
pub(super) const PROVIDER_STALE_REVALIDATION_FAILURE_MAX_KEYS: u64 = 4_096;

#[derive(Clone)]
pub(super) enum ProviderFlightOutcome {
    Error(HttpError),
    /// The leader's completed representation. Current followers reuse this
    /// directly; cache retention and eviction are independent concerns.
    Resource(ProviderResource),
}

pub(super) struct FetchedProviderResource {
    bytes: Bytes,
    policy: CachePolicy,
    validators: Validators,
    content_encoding: Option<Arc<str>>,
    initial_age: Duration,
}

pub(super) struct FetchedProviderNegative {
    status: StatusCode,
    policy: NegativeCachePolicy,
    initial_age: Duration,
}

/// Result of an origin request. A conditional hit carries a rebuilt cache entry
/// around the previously validated body, so it follows the same insertion path
/// without downloading or re-validating the representation bytes.
pub(super) enum ProviderOriginOutcome {
    Modified(FetchedProviderResource),
    NotModified(FetchedProviderResource),
    Negative(FetchedProviderNegative),
}

#[derive(Clone)]
pub(super) struct CachedProviderRepresentation {
    bytes: Bytes,
    cache_control: Arc<str>,
    validators: Validators,
    content_encoding: Option<Arc<str>>,
}

pub(super) struct ProviderFetchSlot {
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

pub(super) struct ProviderFetchPermit {
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
pub(super) fn provider_http_client() -> Client {
    representation_preserving_builder()
        .redirect(redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("provider HTTP client builds")
}

pub(super) fn provider_negative_error(status: StatusCode) -> HttpError {
    (
        status,
        if status == StatusCode::GONE {
            "gone"
        } else {
            "not found"
        }
        .to_string(),
    )
}

// Origin transport and body validation live in the fetch module; this module
// owns cache, freshness, admission, and single-flight policy.
