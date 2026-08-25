//! Process-wide MapLibre resource cache used as the `Database` FileSource.

use std::sync::{Arc, OnceLock};
#[cfg(test)]
use std::time::Duration;
use std::time::SystemTime;

use maplibre_native::file_source::{
    ErrorReason, ResourceRequest, Response, StoragePolicy, TokioFileSource,
};
use moka::notification::RemovalCause;
use moka::sync::Cache;
use prometheus::{IntCounterVec, IntGauge, Opts, Registry};

use super::{ResourceRequestKey, SharedResourceRequestKey, kind_label};

const ENTRY_OVERHEAD_BYTES: usize = 128;

/// A cached response plus the origin's RFC 5861 grant, which lives beside the
/// response because MapLibre Native's `Response` cannot express it.
pub(super) struct CachedResource {
    pub(super) response: Response,
    /// End of the `stale-while-revalidate` window; absent = no grant.
    stale_until: Option<SystemTime>,
}

impl CachedResource {
    pub(super) fn stale_until(&self) -> Option<SystemTime> {
        self.stale_until
    }
}

#[derive(Clone)]
pub(super) struct ResourceCache {
    cache: Cache<SharedResourceRequestKey, Arc<CachedResource>>,
}

impl ResourceCache {
    pub(super) fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .weigher(
                |key: &SharedResourceRequestKey, entry: &Arc<CachedResource>| {
                    resource_weight(key, &entry.response).clamp(1, u32::MAX as usize) as u32
                },
            )
            .eviction_listener(|key, _response, cause| {
                cache_metrics()
                    .operations_total
                    .with_label_values(&[key.kind, removal_operation(cause)])
                    .inc();
            })
            .build();
        Self { cache }
    }

    pub(super) fn lookup_shared(
        &self,
        key: &SharedResourceRequestKey,
    ) -> Option<Arc<CachedResource>> {
        self.cache.get(key)
    }

    /// Return a response whose explicit freshness lifetime has not elapsed.
    /// The Network source uses this to close a race where another renderer
    /// populates Database after this renderer's cache lookup missed.
    pub(super) fn fresh_response(
        &self,
        key: &SharedResourceRequestKey,
    ) -> Option<Arc<CachedResource>> {
        let entry = self.lookup_shared(key)?;
        let expires = entry.response.expires?;
        (expires > SystemTime::now()).then_some(entry)
    }

    pub(super) fn is_stale_while_revalidating(&self, key: &SharedResourceRequestKey) -> bool {
        let Some(entry) = self.lookup_shared(key) else {
            return false;
        };
        let now = SystemTime::now();
        entry.response.expires.is_some_and(|expires| expires <= now)
            && entry
                .stale_until
                .is_some_and(|stale_until| now < stale_until)
    }

    fn lookup(&self, key: &SharedResourceRequestKey) -> CacheLookup {
        let Some(entry) = self.lookup_shared(key) else {
            return CacheLookup::Miss;
        };
        let response = entry.response.clone();
        let now = SystemTime::now();
        let expired = response.expires.is_some_and(|expires| expires <= now);
        if expired {
            if entry
                .stale_until
                .is_some_and(|stale_until| now < stale_until)
            {
                return CacheLookup::ServeStaleWhileRevalidating(response);
            }
            return CacheLookup::Revalidate(response);
        }
        if response.must_revalidate && response.expires.is_none() {
            return CacheLookup::Revalidate(response);
        }
        CacheLookup::Hit(response)
    }

    pub(super) fn store(
        &self,
        key: SharedResourceRequestKey,
        response: Response,
        stale_until: Option<SystemTime>,
    ) -> bool {
        if !is_cacheable_response(&response) {
            cache_metrics()
                .operations_total
                .with_label_values(&[key.kind, "skip"])
                .inc();
            return false;
        }
        let kind = key.kind;
        self.cache.insert(
            key,
            Arc::new(CachedResource {
                response,
                stale_until,
            }),
        );
        cache_metrics()
            .operations_total
            .with_label_values(&[kind, "insert"])
            .inc();
        self.update_size_metrics();
        true
    }

    pub(super) fn invalidate(&self, key: &SharedResourceRequestKey) {
        self.cache.invalidate(key);
        cache_metrics()
            .operations_total
            .with_label_values(&[key.kind, "remove"])
            .inc();
        self.update_size_metrics();
    }

    fn update_size_metrics(&self) {
        let metrics = cache_metrics();
        metrics
            .weight_bytes
            .set(self.cache.weighted_size().min(i64::MAX as u64) as i64);
        metrics
            .entries
            .set(self.cache.entry_count().min(i64::MAX as u64) as i64);
    }
}

enum CacheLookup {
    Hit(Response),
    /// Expired, but inside the origin's stale-while-revalidate window.
    ServeStaleWhileRevalidating(Response),
    Revalidate(Response),
    Miss,
}

pub(super) struct DatabaseFileSource {
    cache: ResourceCache,
}

impl DatabaseFileSource {
    pub(super) fn new(cache: ResourceCache) -> Self {
        Self { cache }
    }
}

impl TokioFileSource for DatabaseFileSource {
    fn can_request(&self, request: &ResourceRequest) -> bool {
        request.loading_methods.has_cache()
            && !request.url.starts_with("asset://")
            && !request.url.starts_with("file://")
    }

    async fn request(&self, request: ResourceRequest) -> Response {
        let kind = kind_label(request.kind);
        if !matches!(request.storage_policy, StoragePolicy::Permanent) {
            cache_metrics()
                .operations_total
                .with_label_values(&[kind, "bypass"])
                .inc();
            return cache_miss();
        }

        let key = Arc::new(ResourceRequestKey::from_request(&request));
        let response = self.cache.lookup(&key);
        let (operation, response) = match response {
            CacheLookup::Hit(response) => ("hit", response),
            CacheLookup::ServeStaleWhileRevalidating(mut response) => {
                // The origin granted RFC 5861 stale-while-revalidate. Serve as
                // usable-stale so MainResourceLoader delivers the body to its
                // consumer and pairs a background revalidation, instead of
                // withholding it and blocking the render on the round trip.
                response.must_revalidate = false;
                ("serve_stale", response)
            }
            CacheLookup::Revalidate(mut response) => {
                // MainResourceLoader copies this stale body and its validators
                // into the following Network request. Force strict revalidation
                // even when the provider only supplied an expiry: outside an
                // explicit stale-while-revalidate grant, biei never serves an
                // expired resource while refreshing it.
                response.must_revalidate = true;
                ("revalidate", response)
            }
            CacheLookup::Miss => ("miss", cache_miss()),
        };
        cache_metrics()
            .operations_total
            .with_label_values(&[kind, operation])
            .inc();
        self.cache.update_size_metrics();
        response
    }

    // Network responses are stored directly by `NetworkFileSource` before
    // crossing the C++ bridge. The default no-op `forward` avoids an extra FFI
    // round trip and lets a bodyless 304 update the materialized Rust entry.
}

fn cache_miss() -> Response {
    let mut response = Response::error(ErrorReason::NotFound, "resource cache miss");
    response.no_content = true;
    response
}

fn is_cacheable_response(response: &Response) -> bool {
    response.error.is_none()
        && !response.no_content
        && !response.not_modified
        && response.data.is_some()
}

fn resource_weight(key: &ResourceRequestKey, response: &Response) -> usize {
    let key_bytes = key.url.len()
        + key
            .tile
            .as_ref()
            .map_or(0, |tile| tile.url_template.len() + 16);
    let response_bytes =
        response.data.as_ref().map_or(0, Vec::len) + response.etag.as_ref().map_or(0, String::len);
    ENTRY_OVERHEAD_BYTES
        .saturating_add(key_bytes)
        .saturating_add(response_bytes)
}

fn removal_operation(cause: RemovalCause) -> &'static str {
    match cause {
        RemovalCause::Expired => "evict_expired",
        RemovalCause::Explicit => "remove_observed",
        RemovalCause::Replaced => "replace",
        RemovalCause::Size => "evict_size",
    }
}

struct CacheMetrics {
    registry: Registry,
    operations_total: IntCounterVec,
    weight_bytes: IntGauge,
    entries: IntGauge,
}

fn cache_metrics() -> &'static CacheMetrics {
    static METRICS: OnceLock<CacheMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let registry = Registry::new();
        let operations_total = IntCounterVec::new(
            Opts::new(
                "mmpf_mln_resource_cache_total",
                "Process-wide MapLibre resource cache operations.",
            ),
            &["kind", "operation"],
        )
        .expect("valid resource cache counter");
        let weight_bytes = IntGauge::new(
            "mmpf_mln_resource_cache_weight_bytes",
            "Approximate weighted size of the process-wide MapLibre resource cache.",
        )
        .expect("valid resource cache weight gauge");
        let entries = IntGauge::new(
            "mmpf_mln_resource_cache_entries",
            "Approximate entry count of the process-wide MapLibre resource cache.",
        )
        .expect("valid resource cache entry gauge");
        for collector in [
            Box::new(operations_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(weight_bytes.clone()),
            Box::new(entries.clone()),
        ] {
            registry
                .register(collector)
                .expect("register resource cache metric");
        }
        CacheMetrics {
            registry,
            operations_total,
            weight_bytes,
            entries,
        }
    })
}

pub(super) fn gather_metrics() -> Vec<prometheus::proto::MetricFamily> {
    cache_metrics().registry.gather()
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplibre_native::file_source::ResourceKind;

    #[test]
    fn stores_successful_bodies_and_returns_cache_miss_shape() {
        let cache = ResourceCache::new(1024);
        let shared = cache.clone();
        let key = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/a",
            ResourceKind::Tile,
        ));
        let response = Response::data(b"tile".to_vec()).with_etag("v1");

        assert!(cache.store(key.clone(), response, None));
        let CacheLookup::Hit(cached) = shared.lookup(&key) else {
            panic!("shared cached response");
        };
        assert_eq!(cached.data.as_deref(), Some(b"tile".as_slice()));
        assert_eq!(cached.etag.as_deref(), Some("v1"));

        let miss = cache_miss();
        assert!(miss.no_content);
        assert_eq!(
            miss.error.expect("miss error").reason,
            ErrorReason::NotFound
        );
    }

    #[test]
    fn skips_errors_empty_responses_and_not_modified_markers() {
        assert!(!is_cacheable_response(&Response::error(
            ErrorReason::Server,
            "failed",
        )));
        assert!(!is_cacheable_response(&Response::no_content()));
        assert!(!is_cacheable_response(&Response::not_modified()));
        assert!(is_cacheable_response(&Response::data(Vec::new())));
    }

    #[test]
    fn cache_weight_includes_key_metadata_and_body() {
        let key = ResourceRequestKey::test_key("https://resource.test/a", ResourceKind::Image);
        let response = Response::data(vec![0; 256]).with_etag("etag");
        assert!(resource_weight(&key, &response) >= ENTRY_OVERHEAD_BYTES + 256 + 4);
    }

    #[test]
    fn cache_removal_metrics_use_bounded_labels() {
        assert_eq!(removal_operation(RemovalCause::Expired), "evict_expired");
        assert_eq!(removal_operation(RemovalCause::Explicit), "remove_observed");
        assert_eq!(removal_operation(RemovalCause::Replaced), "replace");
        assert_eq!(removal_operation(RemovalCause::Size), "evict_size");
    }

    #[test]
    fn an_expired_entry_inside_the_grant_serves_stale_while_revalidating() {
        let cache = ResourceCache::new(1024);
        let key = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/swr",
            ResourceKind::Tile,
        ));
        cache.store(
            key.clone(),
            Response::data(b"stale".to_vec())
                .with_expires(SystemTime::now() - Duration::from_secs(1))
                .with_must_revalidate(true),
            Some(SystemTime::now() + Duration::from_secs(59)),
        );

        assert!(matches!(
            cache.lookup(&key),
            CacheLookup::ServeStaleWhileRevalidating(_)
        ));
    }

    #[test]
    fn an_expired_entry_past_the_grant_requires_revalidation() {
        let cache = ResourceCache::new(1024);
        let key = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/swr-expired",
            ResourceKind::Tile,
        ));
        cache.store(
            key.clone(),
            Response::data(b"stale".to_vec())
                .with_expires(SystemTime::now() - Duration::from_secs(120)),
            Some(SystemTime::now() - Duration::from_secs(60)),
        );

        assert!(matches!(cache.lookup(&key), CacheLookup::Revalidate(_)));
    }

    #[test]
    fn a_fresh_entry_ignores_the_grant() {
        let cache = ResourceCache::new(1024);
        let key = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/swr-fresh",
            ResourceKind::Tile,
        ));
        cache.store(
            key.clone(),
            Response::data(b"fresh".to_vec())
                .with_expires(SystemTime::now() + Duration::from_secs(60)),
            Some(SystemTime::now() + Duration::from_secs(120)),
        );

        assert!(matches!(cache.lookup(&key), CacheLookup::Hit(_)));
    }

    #[test]
    fn expired_must_revalidate_entry_is_not_served_stale() {
        let cache = ResourceCache::new(1024);
        let key = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/a",
            ResourceKind::Tile,
        ));
        cache.store(
            key.clone(),
            Response::data(b"stale".to_vec())
                .with_expires(SystemTime::UNIX_EPOCH)
                .with_must_revalidate(true),
            None,
        );

        assert!(matches!(cache.lookup(&key), CacheLookup::Revalidate(_)));
        assert_eq!(
            cache
                .lookup_shared(&key)
                .and_then(|entry| entry.response.data.clone()),
            Some(b"stale".to_vec()),
            "validators and prior bytes remain available to the network source"
        );
    }

    #[test]
    fn expired_entry_revalidates_without_must_revalidate_directive() {
        let cache = ResourceCache::new(1024);
        let key = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/a",
            ResourceKind::Tile,
        ));
        cache.store(
            key.clone(),
            Response::data(b"stale".to_vec()).with_expires(SystemTime::UNIX_EPOCH),
            None,
        );

        assert!(matches!(cache.lookup(&key), CacheLookup::Revalidate(_)));
    }

    #[test]
    fn fresh_response_only_returns_usable_unexpired_responses() {
        let cache = ResourceCache::new(4096);
        let fresh = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/fresh",
            ResourceKind::Glyphs,
        ));
        let expired = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/expired",
            ResourceKind::Glyphs,
        ));
        cache.store(
            fresh.clone(),
            Response::data(b"fresh".to_vec())
                .with_expires(SystemTime::now() + std::time::Duration::from_mins(1)),
            None,
        );
        cache.store(
            expired.clone(),
            Response::data(b"expired".to_vec())
                .with_expires(SystemTime::now() - std::time::Duration::from_secs(1)),
            None,
        );

        assert_eq!(
            cache
                .fresh_response(&fresh)
                .and_then(|entry| entry.response.data.clone())
                .as_deref(),
            Some(b"fresh".as_slice())
        );
        assert!(cache.fresh_response(&expired).is_none());
    }

    #[test]
    fn response_without_freshness_lifetime_is_served_then_revalidated() {
        let cache = ResourceCache::new(4096);
        let key = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/no-expiration",
            ResourceKind::Glyphs,
        ));
        cache.store(key.clone(), Response::data(b"cached".to_vec()), None);

        assert!(matches!(cache.lookup(&key), CacheLookup::Hit(_)));
        assert!(
            cache.fresh_response(&key).is_none(),
            "without an explicit freshness lifetime, Network must revalidate"
        );
    }
}
