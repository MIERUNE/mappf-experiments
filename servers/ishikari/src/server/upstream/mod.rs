//! Shared bounded upstream fetch helpers for provider resources.

use std::{sync::Arc, time::Duration};

use axum::http::StatusCode;
use bytes::Bytes;
use reqwest::{Client, redirect};
use tokio::sync::OwnedSemaphorePermit;

use crate::http_client::representation_preserving_builder;
use crate::server::{
    HttpError,
    conditional::Validators,
    inflight::InflightSlot,
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
///
/// This is a memory bound, not a throughput knob. One semaphore covers every
/// provider resource, so the worst case is `concurrency * MAX_PROVIDER_BODY_BYTES`
/// — the *largest* cap, which is a sprite PNG, not the 1 MiB glyph the default was
/// raised for. That product is validated against
/// `ISKR_PROVIDER_ACTIVE_BODY_BUDGET_BYTES` at startup, so raising the concurrency
/// requires raising the budget and therefore acknowledging the memory.
///
/// The value itself is chosen for the glyph workload: 1 MiB bodies, ~150 ranges per
/// cold CJK render, so a low value turns one render into many sequential waves.
/// Charging every resource the sprite cap makes the budget conservative rather than
/// wrong; a sprite lane with its own lower concurrency would let the budget drop to
/// roughly the glyph-dominated figure (see issues/ishikari-todo.md).
///
/// The default is sized for the demo's dominant cost. A CJK basemap needs on the
/// order of 150 glyph ranges for one cold render, so 16 forced roughly ten
/// sequential waves; at ~0.4 s per wave that is most of a four-second cold
/// render. Override with `ISKR_PROVIDER_FETCH_CONCURRENCY` when a deployment's
/// memory budget or upstream differs.
pub(crate) const DEFAULT_PROVIDER_FETCH_CONCURRENCY: usize = 64;

/// The largest single provider body any handler accepts, and therefore the
/// multiplier for worst-case transient body memory across the shared fetch
/// semaphore. Sprite PNGs are the largest; glyphs are 1 MiB and styles 2 MiB.
///
/// The assertions below fail the build if a handler ever raises its own cap past
/// this, so the budget arithmetic in `options` cannot silently go stale.
pub(crate) const MAX_PROVIDER_BODY_BYTES: usize = 8 * 1024 * 1024;

const _: () = assert!(crate::server::glyph::MAX_GLYPH_BYTES <= MAX_PROVIDER_BODY_BYTES);
const _: () = assert!(crate::server::style::MAX_STYLE_BYTES <= MAX_PROVIDER_BODY_BYTES);
const _: () = assert!(crate::server::sprite::MAX_SPRITE_JSON_BYTES <= MAX_PROVIDER_BODY_BYTES);
const _: () = assert!(crate::server::sprite::MAX_SPRITE_PNG_BYTES <= MAX_PROVIDER_BODY_BYTES);
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
/// How long a refresh fence entry is retained.
///
/// A fence value is only consulted by fetches that captured it before their I/O,
/// and [`PROVIDER_FETCH_TIMEOUT`] is a single deadline covering a fetch's whole
/// lifetime, so nothing can still be holding a value older than that. Retaining
/// well beyond it keeps ordinary operation clear of the boundary; expiring at all
/// is what stops a hint-heavy deployment from pinning one entry per refreshed key
/// for the life of the process. Losing an entry early is safe but not free: a
/// fetch in flight across the expiry sees a fence mismatch and is refetched
/// rather than stored, so this must stay comfortably above the fetch timeout.
pub(super) const PROVIDER_INVALIDATION_FENCE_RETENTION: Duration = Duration::from_mins(10);

/// The refresh fence a fetch captured immediately before its I/O.
///
/// `key_epoch` is `None` when the key had no fence entry at all, which is
/// deliberately *not* the same as `Some(0)`. The per-key fence map is bounded, so
/// an absent entry is ambiguous: it may mean "never refreshed", or it may mean a
/// refresh set an entry that capacity eviction then dropped while this fetch was
/// still running. Treating absence as epoch zero is what lets a pre-hint fetch
/// match and republish bytes the refresh had already made unreachable.
///
/// `refreshes` resolves the ambiguity. It counts refreshes process-wide, lives in
/// an atomic that nothing can evict or lower, and is captured with the epoch. An
/// unchanged value proves no refresh happened *anywhere* during the flight, so the
/// per-key map cannot have lost anything relevant. Once it has changed, an absent
/// per-key entry is treated as superseded rather than trusted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FetchFence {
    pub(super) key_epoch: Option<u64>,
    pub(super) refreshes: u64,
}

impl FetchFence {
    /// The epoch value to record on a stored entry for the read-side comparison
    /// in the cache's own lookup path.
    pub(super) fn stored_epoch(self) -> u64 {
        self.key_epoch.unwrap_or_default()
    }
}

/// A completed flight, tagged with the refresh fence the leader fetched under.
///
/// The fence is part of the outcome because a follower may join *after* a refresh
/// hint advanced the epoch and invalidated the cache. Publication to followers
/// bypasses the cache entirely, so without the fence a follower would receive
/// pre-hint bytes that the fence had already made unreachable by lookup —
/// defeating the refresh contract for exactly the concurrent requests a refresh
/// is most likely to overlap.
#[derive(Clone)]
pub(super) struct ProviderFlightOutcome {
    pub(super) fence: FetchFence,
    pub(super) result: ProviderFlightResult,
}

#[derive(Clone)]
pub(super) enum ProviderFlightResult {
    Error(HttpError),
    /// The leader's completed representation. A follower reuses it only while the
    /// fence it was fetched under is still current.
    Resource(ProviderResource),
}

impl ProviderFlightOutcome {
    pub(super) fn resource(fence: FetchFence, resource: ProviderResource) -> Self {
        Self {
            fence,
            result: ProviderFlightResult::Resource(resource),
        }
    }

    pub(super) fn error(fence: FetchFence, error: HttpError) -> Self {
        Self {
            fence,
            result: ProviderFlightResult::Error(error),
        }
    }
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

pub(super) struct ProviderFetchPermit {
    _permit: OwnedSemaphorePermit,
    _slot: InflightSlot,
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
