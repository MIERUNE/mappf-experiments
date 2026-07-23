//! Unit and regression tests for the Rust FileSource integration.

use super::*;
use std::time::SystemTime;

use reqwest::header::{
    AGE, CACHE_CONTROL, DATE, ETAG, EXPIRES, HeaderMap, HeaderValue, LAST_MODIFIED, RETRY_AFTER,
};

#[test]
fn body_permit_wait_and_inflight_metrics_are_registered() {
    fs_metrics()
        .body_wait_seconds
        .with_label_values(&["tile"])
        .observe(0.001);
    let guard = BodyInflightGuard::new(ResourceKind::Tile);
    let names: Vec<_> = fs_metrics()
        .registry
        .gather()
        .into_iter()
        .map(|family| family.name().to_string())
        .collect();

    assert!(
        names
            .iter()
            .any(|name| name == "mmpf_mln_resource_body_wait_seconds")
    );
    assert!(
        names
            .iter()
            .any(|name| name == "mmpf_mln_resource_bodies_inflight")
    );
    assert!(
        names
            .iter()
            .any(|name| name == "mmpf_mln_resource_retry_sequences_inflight")
    );
    assert!(
        names
            .iter()
            .any(|name| name == "mmpf_mln_resource_slow_attempts_inflight")
    );
    drop(guard);
}

fn map_response(status: u16, headers: &HeaderMap, body: &[u8], kind: ResourceKind) -> Response {
    response_from_http(
        status,
        headers,
        body.to_vec(),
        kind,
        PriorResponse::default(),
    )
}

#[tokio::test(start_paused = true)]
async fn network_attempt_budget_excludes_admission_wait() {
    let mut budget = NetworkAttemptBudget {
        remaining: Duration::from_millis(100),
    };

    // Time outside `run` represents semaphore/single-flight admission and
    // must not consume the network attempt budget.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(budget.remaining, Duration::from_millis(100));

    budget
        .run(tokio::time::sleep(Duration::from_millis(40)))
        .await
        .expect("first network operation fits");
    assert_eq!(budget.remaining, Duration::from_millis(60));

    assert!(
        budget
            .run(tokio::time::sleep(Duration::from_millis(61)))
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn provider_evidence_and_duration_count_only_network_pending_time() {
    let health = ProviderHealthTracker::new();
    let mut observation = NetworkIoObservation::without_metrics(&health, true);
    let mut budget = NetworkAttemptBudget::new();
    observation
        .run(
            &mut budget,
            tokio::time::sleep(SLOW_PROVIDER_ATTEMPT_THRESHOLD / 2),
        )
        .await
        .expect("fast network operation");
    assert!(!health.has_external_evidence());

    // This represents a saturated response-body semaphore. It must affect the
    // dedicated body-wait metric, not upstream duration or provider health.
    tokio::time::sleep(SLOW_PROVIDER_ATTEMPT_THRESHOLD * 10).await;
    assert!(!health.has_external_evidence());
    assert_eq!(observation.elapsed(), SLOW_PROVIDER_ATTEMPT_THRESHOLD / 2);
    observation
        .run(
            &mut budget,
            tokio::time::sleep(SLOW_PROVIDER_ATTEMPT_THRESHOLD * 3 / 4),
        )
        .await
        .expect("cumulatively slow network operation");
    assert!(
        !health.has_external_evidence(),
        "provisional evidence must end when network polling ends"
    );
    tokio::time::sleep(SLOW_PROVIDER_ATTEMPT_THRESHOLD * 2).await;
    assert!(
        !health.has_external_evidence(),
        "local work after a slow network operation must not inherit provider evidence"
    );

    let slow = tokio::spawn({
        let health = health.clone();
        async move {
            let mut observation = NetworkIoObservation::without_metrics(&health, true);
            let mut budget = NetworkAttemptBudget::new();
            observation
                .run(
                    &mut budget,
                    tokio::time::sleep(SLOW_PROVIDER_ATTEMPT_THRESHOLD * 10),
                )
                .await
        }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(SLOW_PROVIDER_ATTEMPT_THRESHOLD.saturating_sub(Duration::from_millis(1)))
        .await;
    assert!(!health.has_external_evidence());
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(health.has_external_evidence());

    slow.abort();
    let _ = slow.await;
    assert!(
        !health.has_external_evidence(),
        "cancelling the fetch must release provisional evidence"
    );
}

#[test]
fn request_metadata_uses_bounded_native_labels() {
    assert_eq!(priority_label(Priority::Regular), "regular");
    assert_eq!(priority_label(Priority::Low), "low");
    assert_eq!(usage_label(Usage::Online), "online");
    assert_eq!(usage_label(Usage::Offline), "offline");
}

#[test]
fn background_refresh_retry_is_not_render_failure_evidence() {
    assert!(tracks_provider_health(Priority::Regular));
    assert!(!tracks_provider_health(Priority::Low));
}

#[test]
fn singleflight_does_not_mix_regular_and_background_refreshes() {
    let resource = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/tile",
        ResourceKind::Tile,
    ));
    let regular = FlightKey {
        resource: resource.clone(),
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics::default(),
    };
    let low = FlightKey {
        resource,
        persistent: true,
        priority: "low",
        semantics: FlightRequestSemantics::default(),
    };

    assert_ne!(regular, low);
}

#[test]
fn singleflight_does_not_mix_network_only_and_cache_revalidation() {
    let resource = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/tile",
        ResourceKind::Tile,
    ));
    let network_only = FlightKey {
        resource: resource.clone(),
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics {
            cache_allowed: false,
            ..FlightRequestSemantics::default()
        },
    };
    let cache_revalidation = FlightKey {
        resource,
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics {
            cache_allowed: true,
            ..FlightRequestSemantics::default()
        },
    };

    assert_ne!(network_only, cache_revalidation);
}

#[test]
fn singleflight_does_not_mix_different_validators() {
    let resource = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/tile",
        ResourceKind::Tile,
    ));
    let v1 = FlightKey {
        resource: resource.clone(),
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics {
            prior_etag: Some("\"v1\"".to_string()),
            ..FlightRequestSemantics::default()
        },
    };
    let v2 = FlightKey {
        resource,
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics {
            prior_etag: Some("\"v2\"".to_string()),
            ..FlightRequestSemantics::default()
        },
    };

    assert_ne!(v1, v2);
}

#[test]
fn credential_bearing_urls_partition_shared_cache_and_singleflight_identity() {
    let broad = Arc::new(ResourceRequestKey::test_key(
        "https://ishikari.test/tilesets/base/0/0/0?access_token=public.broad",
        ResourceKind::Tile,
    ));
    let weaker = Arc::new(ResourceRequestKey::test_key(
        "https://ishikari.test/tilesets/base/0/0/0?access_token=public.style-only",
        ResourceKind::Tile,
    ));
    assert_ne!(
        broad, weaker,
        "the complete credential-bearing URL must remain part of resource identity"
    );

    let cache = cache::ResourceCache::new(4096);
    assert!(cache.store(
        broad.clone(),
        Response::data(b"authorized-for-broad-token".to_vec())
    ));
    assert!(cache.lookup_shared(&broad).is_some());
    assert!(
        cache.lookup_shared(&weaker).is_none(),
        "a response fetched with one token must not satisfy another token's request"
    );

    let broad_flight = FlightKey {
        resource: broad,
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics::default(),
    };
    let weaker_flight = FlightKey {
        resource: weaker,
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics::default(),
    };
    assert_ne!(
        broad_flight, weaker_flight,
        "single-flight coalescing must preserve the same credential boundary"
    );
}

#[test]
fn network_only_does_not_consult_the_shared_cache() {
    assert!(uses_shared_cache(
        maplibre_native::file_source::StoragePolicy::Permanent
    ));
    assert!(may_consult_shared_cache(
        maplibre_native::file_source::StoragePolicy::Permanent,
        true,
    ));
    assert!(
        !may_consult_shared_cache(
            maplibre_native::file_source::StoragePolicy::Permanent,
            false,
        ),
        "NetworkOnly must bypass the process-wide Database cache"
    );
}

#[test]
fn validator_does_not_require_a_prior_body() {
    let validator = conditional_validator(PriorResponse {
        etag: Some("\"v1\""),
        ..PriorResponse::default()
    });

    assert_eq!(validator, Some(ConditionalValidator::Etag("\"v1\"")));

    let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    assert_eq!(
        conditional_validator(PriorResponse {
            modified: Some(modified),
            ..PriorResponse::default()
        }),
        Some(ConditionalValidator::Modified(modified))
    );
}

#[test]
fn validator_only_not_modified_stays_bodyless() {
    let attempt = not_modified_attempt(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &HeaderMap::new(),
        PriorResponse {
            etag: Some("\"v1\""),
            ..PriorResponse::default()
        },
    );

    assert!(attempt.response.error.is_none());
    assert!(attempt.response.not_modified);
    assert!(attempt.response.data.is_none());
    assert_eq!(attempt.response.etag.as_deref(), Some("\"v1\""));
}

#[test]
fn not_modified_with_prior_body_stays_bodyless_for_native() {
    let attempt = not_modified_attempt(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &HeaderMap::new(),
        PriorResponse {
            data: Some(b"cached"),
            etag: Some("\"v1\""),
            ..PriorResponse::default()
        },
    );

    assert!(attempt.response.not_modified);
    assert!(attempt.response.data.is_none());
    let cached = attempt
        .cache_response
        .expect("Rust cache receives materialized response");
    assert!(!cached.not_modified);
    assert_eq!(cached.data.as_deref(), Some(b"cached".as_slice()));
}

#[test]
fn not_modified_without_body_or_validator_is_rejected() {
    let attempt = not_modified_attempt(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &HeaderMap::new(),
        PriorResponse::default(),
    );

    assert_eq!(
        attempt
            .response
            .error
            .expect("unconditional 304 is invalid")
            .reason,
        ErrorReason::Other
    );
}

#[test]
fn refresh_wait_honors_expiry_and_minimum_update_interval() {
    let expiry_wait = refresh_deferral(SystemTime::now() + Duration::from_secs(60), Duration::ZERO);
    assert!(!expiry_wait.capped);
    let expiry_wait = expiry_wait.wait;
    assert!(expiry_wait > Duration::from_secs(59));
    assert!(expiry_wait <= Duration::from_secs(60));

    assert_eq!(
        refresh_deferral(
            SystemTime::now() + Duration::from_secs(1),
            Duration::from_secs(30),
        ),
        RefreshDeferral {
            wait: Duration::from_secs(30),
            capped: false,
        }
    );
}

#[test]
fn refresh_wait_is_bounded_for_long_lived_fresh_entries() {
    let deferral = refresh_deferral(
        SystemTime::now() + Duration::from_secs(24 * 60 * 60),
        Duration::ZERO,
    );

    assert_eq!(deferral.wait, MAX_REFRESH_DEFERRAL);
    assert!(deferral.capped);
}

#[test]
fn capped_refresh_completes_without_fetch_even_after_cache_eviction() {
    let capped = RefreshDeferral {
        wait: MAX_REFRESH_DEFERRAL,
        capped: true,
    };
    let response = complete_deferred_refresh(&capped, None)
        .expect("a capped background refresh must complete without a fetch");

    assert!(response.not_modified);
    assert!(response.data.is_none());

    let expired = RefreshDeferral {
        wait: Duration::from_secs(1),
        capped: false,
    };
    assert!(complete_deferred_refresh(&expired, None).is_none());
}

#[test]
fn maps_200_with_cache_metadata() {
    let mut headers = HeaderMap::new();
    headers.insert(ETAG, HeaderValue::from_static("\"abc\""));
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=3600, must-revalidate"),
    );

    let response = map_response(200, &headers, b"tile", ResourceKind::Tile);

    assert!(response.error.is_none());
    assert_eq!(response.data.as_deref(), Some(b"tile".as_slice()));
    assert_eq!(response.etag.as_deref(), Some("\"abc\""));
    assert!(response.must_revalidate);
    let expires = response.expires.expect("expires derived from max-age");
    let lower = SystemTime::now() + Duration::from_secs(3000);
    assert!(expires > lower, "expires should be ~1h out");
}

#[test]
fn replacement_response_does_not_inherit_old_validators_or_freshness() {
    let old_expiry = SystemTime::now() + Duration::from_secs(3600);
    let response = response_from_http(
        200,
        &HeaderMap::new(),
        b"replacement".to_vec(),
        ResourceKind::Tile,
        PriorResponse {
            etag: Some("\"old\""),
            modified: Some(SystemTime::UNIX_EPOCH),
            expires: Some(old_expiry),
            ..PriorResponse::default()
        },
    );

    assert_eq!(response.data.as_deref(), Some(b"replacement".as_slice()));
    assert_eq!(response.etag, None);
    assert_eq!(response.modified, None);
    // The replacement carries no explicit freshness, so it gets bounded
    // heuristic freshness rather than inheriting the prior entry's hour-long
    // expiry (and rather than being cached forever).
    let expires = response.expires.expect("bounded heuristic freshness");
    assert!(
        expires < old_expiry,
        "replacement must not inherit the prior's longer freshness"
    );
}

#[test]
fn no_cache_requires_validation_and_no_store_is_detected() {
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache, no-store"),
    );

    let response = map_response(200, &headers, b"private", ResourceKind::Tile);
    assert!(response.must_revalidate);
    assert_eq!(response.expires, Some(SystemTime::UNIX_EPOCH));
    assert!(has_cache_directive(&headers, "no-store"));
    assert_eq!(
        cache_policy_for_response(
            maplibre_native::file_source::StoragePolicy::Permanent,
            &headers,
        ),
        CachePolicy::Remove
    );
    assert_eq!(
        cache_policy_for_response(
            maplibre_native::file_source::StoragePolicy::Volatile,
            &HeaderMap::new(),
        ),
        CachePolicy::Unchanged
    );
}

#[test]
fn shared_cache_rejects_private_responses() {
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=3600"),
    );

    assert_eq!(
        cache_policy_for_response(
            maplibre_native::file_source::StoragePolicy::Permanent,
            &headers,
        ),
        CachePolicy::Remove
    );
}

#[test]
fn not_modified_response_retains_required_revalidation() {
    let response = response_from_http(
        304,
        &HeaderMap::new(),
        Vec::new(),
        ResourceKind::Tile,
        PriorResponse {
            data: Some(b"cached"),
            expires: Some(SystemTime::UNIX_EPOCH),
            must_revalidate: true,
            ..PriorResponse::default()
        },
    );

    assert!(response.not_modified);
    assert_eq!(response.data, None);
    assert!(response.must_revalidate);
    assert_eq!(response.expires, Some(SystemTime::UNIX_EPOCH));
}

#[test]
fn not_modified_response_gets_a_bounded_freshness_window() {
    let response = response_from_http(
        304,
        &HeaderMap::new(),
        Vec::new(),
        ResourceKind::Tile,
        PriorResponse {
            data: Some(b"cached"),
            expires: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
            ..PriorResponse::default()
        },
    );

    assert!(
        response
            .expires
            .is_some_and(|expires| expires > SystemTime::now())
    );
}

#[test]
fn extreme_cache_durations_do_not_panic_or_overflow() {
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("max-age=18446744073709551615"),
    );
    let response = map_response(200, &headers, b"data", ResourceKind::Tile);
    // An un-representable max-age cannot produce an absolute expiry; it must not
    // panic or overflow, and now degrades to bounded heuristic freshness rather
    // than being treated as fresh forever.
    let now = SystemTime::now();
    let expires = response
        .expires
        .expect("bounded heuristic expiry, no overflow");
    assert!(expires > now && expires <= now + Duration::from_secs(3600));

    assert_eq!(parse_retry_after("18446744073709551615"), None);
}

#[test]
fn maps_missing_tile_to_no_content_and_other_resource_to_not_found() {
    let headers = HeaderMap::new();
    let tile_missing = map_response(404, &headers, &[], ResourceKind::Tile);
    assert!(tile_missing.no_content);
    assert_eq!(outcome_label(&tile_missing), "no_content");

    let not_found = map_response(410, &headers, &[], ResourceKind::Glyphs);
    assert_eq!(
        not_found.error.expect("410 is an error").reason,
        ErrorReason::NotFound
    );
}

#[test]
fn retries_only_transient_statuses_and_bounds_backoff() {
    let headers = HeaderMap::new();
    let server = retry_directive(503, &headers).expect("5xx is retryable");
    assert_eq!(server.reason, "server");
    assert_eq!(server.delay, None);
    assert!(retry_directive(404, &headers).is_none());
    assert_eq!(
        retry_directive(408, &headers)
            .expect("408 is retryable")
            .reason,
        "request_timeout"
    );

    let mut limited_headers = HeaderMap::new();
    limited_headers.insert(RETRY_AFTER, HeaderValue::from_static("30"));
    let limited = retry_directive(429, &limited_headers).expect("429 is retryable");
    assert_eq!(limited.reason, "rate_limit");
    // The server-requested delay is honored (the retry loop clamps it to
    // MAX_RETRY_DELAY instead of turning it into a final error).
    assert!(
        limited
            .delay
            .is_some_and(|delay| delay >= Duration::from_secs(25))
    );

    for (index, base) in RETRY_BACKOFF.into_iter().enumerate() {
        let delay = retry_delay("https://resource.test/tile", index);
        assert!(delay >= base);
        assert!(delay < base + Duration::from_millis(50));
    }
}

#[test]
fn negative_cache_honors_upstream_freshness() {
    let permanent = maplibre_native::file_source::StoragePolicy::Permanent;
    let headers = HeaderMap::new();
    // No explicit upstream freshness: only tiles get the fabricated
    // heuristic (empty tiles are a normal 404); required resources do not.
    assert_eq!(
        negative_cache_ttl(
            404,
            ResourceKind::Tile,
            permanent,
            &headers,
            NEGATIVE_CACHE_TTL
        ),
        Some(NEGATIVE_CACHE_TTL)
    );
    assert_eq!(
        negative_cache_ttl(
            410,
            ResourceKind::Tile,
            permanent,
            &headers,
            NEGATIVE_CACHE_TTL
        ),
        Some(NEGATIVE_CACHE_TTL)
    );
    for status in [400, 401, 403, 408, 429, 500, 503] {
        assert_eq!(
            negative_cache_ttl(
                status,
                ResourceKind::Tile,
                permanent,
                &headers,
                NEGATIVE_CACHE_TTL
            ),
            None
        );
    }

    let mut private = HeaderMap::new();
    private.insert(CACHE_CONTROL, HeaderValue::from_static("private"));
    assert_eq!(
        negative_cache_ttl(
            404,
            ResourceKind::Tile,
            permanent,
            &private,
            NEGATIVE_CACHE_TTL
        ),
        None
    );
    assert_eq!(
        negative_cache_ttl(
            404,
            ResourceKind::Tile,
            maplibre_native::file_source::StoragePolicy::Volatile,
            &headers,
            NEGATIVE_CACHE_TTL,
        ),
        None
    );

    let mut no_cache = HeaderMap::new();
    no_cache.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    assert_eq!(
        negative_cache_ttl(
            404,
            ResourceKind::Tile,
            permanent,
            &no_cache,
            NEGATIVE_CACHE_TTL
        ),
        None
    );

    let mut immediately_stale = HeaderMap::new();
    immediately_stale.insert(CACHE_CONTROL, HeaderValue::from_static("max-age=0"));
    assert_eq!(
        negative_cache_ttl(
            404,
            ResourceKind::Tile,
            permanent,
            &immediately_stale,
            NEGATIVE_CACHE_TTL
        ),
        None
    );

    let mut bounded = HeaderMap::new();
    bounded.insert(CACHE_CONTROL, HeaderValue::from_static("s-maxage=10"));
    bounded.insert(AGE, HeaderValue::from_static("4"));
    // Explicit upstream freshness is honored for tiles and required
    // resources alike (capped at NEGATIVE_CACHE_TTL).
    assert_eq!(
        negative_cache_ttl(
            404,
            ResourceKind::Tile,
            permanent,
            &bounded,
            NEGATIVE_CACHE_TTL
        ),
        Some(Duration::from_secs(6))
    );
    assert_eq!(
        negative_cache_ttl(
            404,
            ResourceKind::Glyphs,
            permanent,
            &bounded,
            NEGATIVE_CACHE_TTL
        ),
        Some(Duration::from_secs(6))
    );
}

#[test]
fn required_resource_404_is_not_negative_cached_without_explicit_freshness() {
    // A transient upstream 404 for a required resource (rolling provider
    // deploy) must not be fabricated into a broken-render window: without
    // explicit upstream freshness these kinds are re-fetched every time,
    // so recovery is immediate once the provider heals.
    let permanent = maplibre_native::file_source::StoragePolicy::Permanent;
    let headers = HeaderMap::new();
    for kind in [
        ResourceKind::Glyphs,
        ResourceKind::SpriteImage,
        ResourceKind::SpriteJSON,
        ResourceKind::Style,
        ResourceKind::Source,
        ResourceKind::Image,
    ] {
        assert_eq!(
            negative_cache_ttl(404, kind, permanent, &headers, NEGATIVE_CACHE_TTL),
            None,
            "{kind:?} 404 must not be negative-cached without explicit upstream freshness"
        );
    }
}

#[test]
fn maps_partial_content_and_server_error() {
    let headers = HeaderMap::new();
    let partial = map_response(206, &headers, b"part", ResourceKind::Tile);
    assert_eq!(partial.data.as_deref(), Some(b"part".as_slice()));

    let server = map_response(503, &headers, &[], ResourceKind::Tile);
    assert_eq!(
        server.error.expect("503 is an error").reason,
        ErrorReason::Server
    );
}

#[test]
fn maps_special_statuses() {
    let headers = HeaderMap::new();
    assert!(map_response(204, &headers, &[], ResourceKind::Image).no_content);

    let not_modified = response_from_http(
        304,
        &headers,
        Vec::new(),
        ResourceKind::Tile,
        PriorResponse {
            data: Some(b"cached"),
            etag: Some("\"old\""),
            ..PriorResponse::default()
        },
    );
    assert!(not_modified.not_modified);
    assert_eq!(not_modified.data, None);
    assert_eq!(not_modified.etag.as_deref(), Some("\"old\""));

    let materialized = materialize_not_modified(
        &not_modified,
        PriorResponse {
            data: Some(b"cached"),
            ..PriorResponse::default()
        },
    )
    .expect("prior body materializes a 304");
    assert!(!materialized.not_modified);
    assert_eq!(materialized.data.as_deref(), Some(b"cached".as_slice()));
    assert_eq!(materialized.etag.as_deref(), Some("\"old\""));
    assert!(materialize_not_modified(&not_modified, PriorResponse::default()).is_none());

    let mut headers = HeaderMap::new();
    headers.insert(RETRY_AFTER, HeaderValue::from_static("30"));
    let limited = map_response(429, &headers, &[], ResourceKind::Tile);
    let error = limited.error.expect("429 is an error");
    assert_eq!(error.reason, ErrorReason::RateLimit);
    assert!(error.retry_after.is_some());

    let teapot = map_response(418, &headers, &[], ResourceKind::Tile);
    assert_eq!(
        teapot.error.expect("4xx is an error").reason,
        ErrorReason::Other
    );
}

#[test]
fn parses_max_age_directive() {
    assert_eq!(
        parse_max_age("public, max-age=3600"),
        Some(Duration::from_secs(3600))
    );
    assert_eq!(
        parse_max_age("max-age=60, stale-while-revalidate=120"),
        Some(Duration::from_secs(60))
    );
    assert_eq!(parse_max_age("no-store"), None);
    assert_eq!(parse_max_age("max-age=abc"), Some(Duration::ZERO));
    assert_eq!(
        parse_max_age("PUBLIC, MAX-AGE=\"90\""),
        Some(Duration::from_secs(90))
    );
}

#[test]
fn duplicate_freshness_directives_are_order_independent_and_conservative() {
    for value in ["max-age=604800, max-age=0", "max-age=0, max-age=604800"] {
        assert_eq!(parse_max_age(value), Some(Duration::ZERO));
    }
    assert_eq!(
        parse_max_age("max-age=604800, max-age=invalid"),
        Some(Duration::ZERO)
    );
}

#[test]
fn shared_cache_freshness_prefers_s_maxage_and_subtracts_age() {
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=600, s-maxage=120"),
    );
    headers.insert(AGE, HeaderValue::from_static("90"));

    let before = SystemTime::now();
    let response = map_response(200, &headers, b"data", ResourceKind::Tile);
    let remaining = response
        .expires
        .expect("freshness produces expiry")
        .duration_since(before)
        .expect("expiry is not in the past");

    assert!(remaining >= Duration::from_secs(29));
    assert!(remaining <= Duration::from_secs(31));
    assert!(
        response.must_revalidate,
        "s-maxage implies proxy-revalidate in a shared cache"
    );
}

#[test]
fn shared_cache_freshness_accounts_for_apparent_age_from_date() {
    let mut headers = HeaderMap::new();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-age=120"));
    headers.insert(
        DATE,
        HeaderValue::from_str(&httpdate::fmt_http_date(
            SystemTime::now() - Duration::from_secs(90),
        ))
        .expect("valid date"),
    );

    let before = SystemTime::now();
    let response = map_response(200, &headers, b"data", ResourceKind::Tile);
    let remaining = response
        .expires
        .expect("freshness produces expiry")
        .duration_since(before)
        .expect("expiry is not in the past");

    assert!(remaining <= Duration::from_secs(31));
    assert!(remaining >= Duration::from_secs(28));
}

#[test]
fn response_older_than_shared_max_age_expires_immediately() {
    let mut headers = HeaderMap::new();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-age=60"));
    headers.insert(AGE, HeaderValue::from_static("120"));

    let before = SystemTime::now();
    let response = map_response(200, &headers, b"data", ResourceKind::Tile);
    let expires = response.expires.expect("expired response has timestamp");

    assert!(expires >= before);
    assert!(expires <= SystemTime::now() + Duration::from_millis(10));
}

#[test]
fn resource_body_caps_are_kind_specific() {
    assert_eq!(max_resource_bytes(ResourceKind::Glyphs), 4 * MIB);
    assert_eq!(max_resource_bytes(ResourceKind::Tile), 16 * MIB);
    assert_eq!(max_resource_bytes(ResourceKind::Unknown), 8 * MIB);
}

#[tokio::test]
async fn flight_waiter_observes_completion_without_lost_wakeup() {
    let flight = Arc::new(Flight::new());
    let waiter = tokio::spawn({
        let flight = Arc::clone(&flight);
        async move { flight.wait().await }
    });
    flight.complete(Response::data(b"shared".to_vec()));

    let response = waiter
        .await
        .expect("waiter task")
        .expect("completed flight");
    assert_eq!(response.data.as_deref(), Some(b"shared".as_slice()));
}

#[tokio::test]
async fn cancelled_flight_leader_wakes_waiters_and_removes_entry() {
    let key = Arc::new(FlightKey {
        resource: Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/tile",
            ResourceKind::Tile,
        )),
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics::default(),
    });
    let flight = Arc::new(Flight::new());
    let flights = Mutex::new(HashMap::from([(key.clone(), Arc::clone(&flight))]));
    let waiter = tokio::spawn({
        let flight = Arc::clone(&flight);
        async move { flight.wait().await }
    });

    drop(FlightLeader {
        flights: &flights,
        key,
        flight,
        completed: false,
    });

    assert!(waiter.await.expect("waiter task").is_none());
    assert!(lock_unpoisoned(&flights).is_empty());
}

#[tokio::test]
async fn completed_flight_leader_wakes_waiters_and_removes_entry() {
    let key = Arc::new(FlightKey {
        resource: Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/tile",
            ResourceKind::Tile,
        )),
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics::default(),
    });
    let flight = Arc::new(Flight::new());
    let flights = Mutex::new(HashMap::from([(key.clone(), Arc::clone(&flight))]));
    let waiter = tokio::spawn({
        let flight = Arc::clone(&flight);
        async move { flight.wait().await }
    });

    let response = FlightLeader {
        flights: &flights,
        key,
        flight,
        completed: false,
    }
    .complete(Response::data(b"shared".to_vec()));

    assert_eq!(response.data.as_deref(), Some(b"shared".as_slice()));
    assert_eq!(
        waiter
            .await
            .expect("waiter task")
            .expect("completed flight")
            .data
            .as_deref(),
        Some(b"shared".as_slice())
    );
    assert!(lock_unpoisoned(&flights).is_empty());
}

#[test]
fn volatile_requests_bypass_negative_cache() {
    assert!(uses_shared_cache(
        maplibre_native::file_source::StoragePolicy::Permanent
    ));
    assert!(!uses_shared_cache(
        maplibre_native::file_source::StoragePolicy::Volatile
    ));
}

#[test]
fn maps_http_dates_to_cache_metadata() {
    let modified = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let expires = modified + Duration::from_secs(3600);
    let mut headers = HeaderMap::new();
    headers.insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&httpdate::fmt_http_date(modified)).expect("valid date"),
    );
    headers.insert(
        EXPIRES,
        HeaderValue::from_str(&httpdate::fmt_http_date(expires)).expect("valid date"),
    );

    let response = map_response(200, &headers, b"data", ResourceKind::Image);
    assert_eq!(response.modified, Some(modified));
    assert_eq!(response.expires, Some(expires));
}

#[test]
fn credentialed_redirect_chain_cannot_change_origin() {
    let credentialed =
        url::Url::parse("https://ishikari.test/style.json?access_token=public.secret").unwrap();
    let same_origin = url::Url::parse("https://ishikari.test/canonical/style.json").unwrap();
    let other_origin = url::Url::parse("https://objects.example/style.json").unwrap();

    assert!(credentialed_redirect_stays_on_origin(
        std::slice::from_ref(&credentialed),
        &same_origin
    ));
    assert!(!credentialed_redirect_stays_on_origin(
        std::slice::from_ref(&credentialed),
        &other_origin
    ));

    let uncredentialed = url::Url::parse("https://public.test/style.json").unwrap();
    assert!(credentialed_redirect_stays_on_origin(
        std::slice::from_ref(&uncredentialed),
        &other_origin
    ));

    // The original credential-bearing URL remains authoritative after an
    // intermediate same-origin redirect drops the query string.
    assert!(!credentialed_redirect_stays_on_origin(
        &[credentialed, same_origin],
        &other_origin
    ));
}
