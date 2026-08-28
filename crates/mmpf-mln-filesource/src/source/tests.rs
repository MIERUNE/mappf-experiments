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
fn a_withheld_body_request_never_shares_a_flight_with_a_bodyless_refresh() {
    // Waiters receive the leader's response verbatim. A request whose native
    // side withheld the body needs a materialized 304 (its consumer holds
    // nothing), while a background refresh must stay bodyless. Sharing one
    // flight across that difference would hand one of them the wrong shape —
    // the wedge only ever reproduced under concurrency because of exactly this
    // coincidence — so `has_prior_data` must partition the flight key.
    let resource = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/tile",
        ResourceKind::Tile,
    ));
    let withheld_body = FlightKey {
        resource: resource.clone(),
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics {
            has_prior_data: true,
            prior_etag: Some("\"v1\"".to_string()),
            ..FlightRequestSemantics::default()
        },
    };
    let bodyless_refresh = FlightKey {
        resource,
        persistent: true,
        priority: "regular",
        semantics: FlightRequestSemantics {
            has_prior_data: false,
            prior_etag: Some("\"v1\"".to_string()),
            ..FlightRequestSemantics::default()
        },
    };

    assert_ne!(withheld_body, bodyless_refresh);
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
        Response::data(b"authorized-for-broad-token".to_vec()),
        CachedFreshness::default(),
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
fn a_304_for_a_withheld_prior_body_reaches_native_with_that_body() {
    // A prior body on the request means native withheld the representation for
    // revalidation, so its consumer (glyph/sprite/tile loader) has nothing and
    // treats `notModified` as a no-op without completing the load. The stock
    // OnlineFileSource merges `priorData` into the 304 before consumers see
    // it; this source replaced that layer and must do the same, or the load —
    // and any still render behind it — waits forever.
    let attempt = not_modified_attempt(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &HeaderMap::new(),
        PriorResponse {
            native_data: Some(b"cached"),
            etag: Some("\"v1\""),
            ..PriorResponse::default()
        },
    );

    assert!(!attempt.response.not_modified);
    assert_eq!(attempt.response.data.as_deref(), Some(b"cached".as_slice()));
    let cached = attempt
        .cache_response
        .expect("Rust cache receives materialized response");
    assert!(!cached.not_modified);
    assert_eq!(cached.data.as_deref(), Some(b"cached".as_slice()));
}

#[test]
fn a_304_backed_only_by_the_shared_cache_stays_bodyless_for_native() {
    // No native prior body means native delivered the representation to its
    // consumer and holds this request only as a background refresh. Re-sending
    // the body would make sprite/tile consumers re-parse an unchanged
    // representation; only the process-wide Rust cache takes the materialized
    // entry.
    let attempt = not_modified_attempt(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &HeaderMap::new(),
        PriorResponse {
            cache_data: Some(b"cached"),
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
fn a_sparse_304_inherits_and_reanchors_the_stored_policy() {
    // RFC 9111 §4.3.4: a 304 that restates nothing inherits the stored
    // directives, and the successful validation resets the response's age —
    // the window re-anchors at validation time instead of freezing at the
    // original boundary.
    let stored = CachedFreshness {
        lifetime: Some(Duration::from_secs(3600)),
        stale_while_revalidate: Some(Duration::from_secs(86400)),
        stale_until: Some(SystemTime::now() + Duration::from_secs(30)),
    };
    let attempt = not_modified_attempt(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &HeaderMap::new(),
        PriorResponse {
            cache_data: Some(b"cached"),
            etag: Some("\"v1\""),
            expires: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
            freshness: stored,
            ..PriorResponse::default()
        },
    );

    let CachePolicy::Store { freshness } = attempt.cache_policy else {
        panic!("a sparse 304 keeps storing");
    };
    assert_eq!(freshness.lifetime, stored.lifetime);
    assert_eq!(
        freshness.stale_while_revalidate,
        stored.stale_while_revalidate
    );
    let floor = SystemTime::now() + Duration::from_secs(3600 + 86400 - 2);
    assert!(
        freshness.stale_until.is_some_and(|until| until > floor),
        "the inherited window re-anchors at validation time"
    );
    // The materialized entry's freshness also inherits the stored lifetime
    // instead of the invented fallback TTL.
    let cached = attempt.cache_response.expect("materialized entry");
    let expires_floor = SystemTime::now() + Duration::from_secs(3600 - 2);
    assert!(
        cached
            .expires
            .is_some_and(|expires| expires > expires_floor),
        "a sparse revalidation restores the stored lifetime, not one minute"
    );
}

#[test]
fn cache_control_on_a_304_replaces_the_stored_swr_grant() {
    let mut headers = HeaderMap::new();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-age=60"));
    let attempt = not_modified_attempt(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &headers,
        PriorResponse {
            cache_data: Some(b"cached"),
            etag: Some("\"v1\""),
            freshness: CachedFreshness {
                lifetime: Some(Duration::from_secs(3600)),
                stale_while_revalidate: Some(Duration::from_secs(86400)),
                stale_until: Some(SystemTime::now() + Duration::from_hours(12)),
            },
            ..PriorResponse::default()
        },
    );

    let CachePolicy::Store { freshness } = attempt.cache_policy else {
        panic!("a restated policy keeps storing");
    };
    assert_eq!(
        freshness.stale_until, None,
        "a present Cache-Control field replaces the stored field, dropping the grant"
    );
    assert_eq!(freshness.lifetime, Some(Duration::from_secs(60)));
}

#[test]
fn an_expires_only_304_replaces_the_stored_policy_and_drops_the_grant() {
    // An origin that restates freshness with a bare short `Expires` has
    // withdrawn the earlier long grant; inheriting it would let the entry's
    // stale-while-revalidate window outlive the restated expiry.
    let expires = SystemTime::now() + Duration::from_secs(60);
    let mut headers = HeaderMap::new();
    headers.insert(
        EXPIRES,
        HeaderValue::from_str(&httpdate::fmt_http_date(expires)).expect("valid date"),
    );
    let attempt = not_modified_attempt(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &headers,
        PriorResponse {
            cache_data: Some(b"cached"),
            etag: Some("\"v1\""),
            freshness: CachedFreshness {
                lifetime: Some(Duration::from_secs(3600)),
                stale_while_revalidate: Some(Duration::from_secs(86400)),
                stale_until: Some(SystemTime::now() + Duration::from_hours(12)),
            },
            ..PriorResponse::default()
        },
    );

    let CachePolicy::Store { freshness } = attempt.cache_policy else {
        panic!("a restated policy keeps storing");
    };
    assert_eq!(
        freshness.stale_until, None,
        "an Expires-only restatement drops the stored stale-while-revalidate grant"
    );
    assert_eq!(freshness.stale_while_revalidate, None);
    assert_eq!(
        freshness.lifetime, None,
        "the restatement declared no relative lifetime"
    );
    let cached = attempt.cache_response.expect("materialized entry");
    let ceiling = SystemTime::now() + Duration::from_secs(62);
    assert!(
        cached.expires.is_some_and(|entry| entry <= ceiling),
        "the materialized entry expires at the restated Expires, not the stored lifetime"
    );
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
fn fresh_cache_race_serves_body_unless_database_already_delivered_one() {
    let expires = SystemTime::now() + Duration::from_hours(24);
    let cached = Response::data(b"cached".to_vec()).with_expires(expires);

    assert_eq!(
        native_database_state(false, false),
        NativeDatabaseState::Miss
    );
    assert_eq!(
        native_database_state(true, true),
        NativeDatabaseState::WithheldForRevalidation,
        "priorData means the stale body was withheld, not delivered"
    );
    assert_eq!(
        native_database_state(false, true),
        NativeDatabaseState::Delivered
    );

    // Labels must stay bounded and distinct so a reproduction can separate a
    // legitimate background refresh from a callback still blocking a render.
    assert_eq!(NativeDatabaseState::Miss.label(), "miss");
    assert_eq!(NativeDatabaseState::Delivered.label(), "delivered");
    assert_eq!(
        NativeDatabaseState::WithheldForRevalidation.label(),
        "withheld"
    );

    for state in [
        NativeDatabaseState::Miss,
        NativeDatabaseState::WithheldForRevalidation,
    ] {
        match resolve_fresh_cache_race(&cached, state, true) {
            FreshCacheResolution::Serve(response) => {
                assert_eq!(response.data.as_deref(), Some(b"cached".as_slice()));
                assert_eq!(response.expires, Some(expires));
            }
            FreshCacheResolution::Defer(_) => {
                panic!("a requester without a delivered body needs the fresh cache body now")
            }
        }
    }

    match resolve_fresh_cache_race(&cached, NativeDatabaseState::Delivered, true) {
        FreshCacheResolution::Defer(observed) => assert_eq!(observed, expires),
        FreshCacheResolution::Serve(_) => {
            panic!("a usable Database hit must not receive the body twice")
        }
    }
}

#[test]
fn refresh_wait_honors_expiry_and_minimum_update_interval() {
    let expiry_wait = refresh_deferral(SystemTime::now() + Duration::from_mins(1), Duration::ZERO);
    assert!(!expiry_wait.capped);
    let expiry_wait = expiry_wait.wait;
    assert!(expiry_wait > Duration::from_secs(59));
    assert!(expiry_wait <= Duration::from_mins(1));

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
    let deferral = refresh_deferral(SystemTime::now() + Duration::from_hours(24), Duration::ZERO);

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
    let lower = SystemTime::now() + Duration::from_mins(50);
    assert!(expires > lower, "expires should be ~1h out");
}

#[test]
fn replacement_response_does_not_inherit_old_validators_or_freshness() {
    let old_expiry = SystemTime::now() + Duration::from_hours(1);
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
fn the_swr_grant_survives_s_maxage_but_not_must_revalidate() {
    // Ishikari's actual style/TileJSON header: RFC 9111's s-maxage-implied
    // proxy-revalidate yields to the origin's explicit RFC 5861 grant.
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(
            "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400",
        ),
    );
    let floor = SystemTime::now() + Duration::from_secs(3600 + 86400 - 2);
    let policy = cache_policy_for_response(
        maplibre_native::file_source::StoragePolicy::Permanent,
        &headers,
    );
    let CachePolicy::Store { freshness } = policy else {
        panic!("an swr grant with an explicit lifetime records a window");
    };
    assert!(
        freshness.stale_until.is_some_and(|until| until > floor),
        "window anchors on lifetime + grant"
    );

    // An explicit must-revalidate prohibits serving stale and wins.
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("must-revalidate, stale-while-revalidate=86400"),
    );
    let CachePolicy::Store { freshness } = cache_policy_for_response(
        maplibre_native::file_source::StoragePolicy::Permanent,
        &headers,
    ) else {
        panic!("must-revalidate still stores");
    };
    assert_eq!(freshness.stale_until, None);
    assert_eq!(freshness.stale_while_revalidate, None);
}

#[test]
fn a_transported_age_shrinks_the_swr_window_instead_of_restarting_it() {
    // s-maxage=3600, swr=86400, Age=43200: the response is already twelve
    // hours into its retention. The correct remainder is 3600 + 86400 - 43200
    // = 46800s; deriving the window from an expiry saturated to `now` would
    // hand back the full 86400s and extend the origin's permitted stale
    // lifetime by eleven hours.
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("s-maxage=3600, stale-while-revalidate=86400"),
    );
    headers.insert(reqwest::header::AGE, HeaderValue::from_static("43200"));
    let now = SystemTime::now();
    let CachePolicy::Store { freshness } = cache_policy_for_response(
        maplibre_native::file_source::StoragePolicy::Permanent,
        &headers,
    ) else {
        panic!("a mid-window response still records its remaining window");
    };
    let stale_until = freshness
        .stale_until
        .expect("a mid-window response still records its remaining window");
    let remaining = stale_until
        .duration_since(now)
        .expect("remaining window is in the future");
    assert!(
        remaining <= Duration::from_secs(46_800 + 2),
        "the transported age must be charged against the whole retention"
    );
    assert!(
        remaining > Duration::from_secs(46_800 - 60),
        "the remainder is lifetime + grant - age, not the full grant"
    );
}

#[test]
fn an_unreadable_cache_control_field_fails_closed() {
    // HTAB is valid HTTP optional whitespace but `HeaderValue::to_str`
    // rejects it; the field must not silently degrade to "absent".
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_bytes(b"no-store,\tmax-age=0").expect("HTAB is a valid header byte"),
    );
    assert_eq!(
        cache_policy_for_response(
            maplibre_native::file_source::StoragePolicy::Permanent,
            &headers,
        ),
        CachePolicy::Remove,
        "a tab-separated no-store must still be honored"
    );

    // A present-but-undecodable field is conservative, never heuristic.
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_bytes(&[0x6d, 0x61, 0x78, 0xff]).expect("arbitrary header bytes"),
    );
    assert_eq!(
        cache_policy_for_response(
            maplibre_native::file_source::StoragePolicy::Permanent,
            &headers,
        ),
        CachePolicy::Remove,
        "an unreadable Cache-Control must fail closed"
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
            cache_data: Some(b"cached"),
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
            cache_data: Some(b"cached"),
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
    assert!(expires > now && expires <= now + Duration::from_hours(1));

    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(
            "max-age=18446744073709551615, stale-while-revalidate=18446744073709551615",
        ),
    );
    assert!(matches!(
        cache_policy_for_response(
            maplibre_native::file_source::StoragePolicy::Permanent,
            &headers,
        ),
        CachePolicy::Store { .. }
    ));

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
    // The response preserves Retry-After, but the render retry budget below
    // rejects a delay longer than the complete sequence budget.
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
fn render_blocking_retries_are_short_and_bounded() {
    assert!(REQUEST_TIMEOUT < RETRY_WINDOW);
    assert_eq!(MAX_ATTEMPTS, 2);
    assert_eq!(MAX_RETRY_DELAY, Duration::from_secs(3));

    assert!(retry_fits_budget(1, Duration::ZERO, Duration::from_secs(3)));
    assert!(
        !retry_fits_budget(1, Duration::from_secs(2), Duration::from_secs(2)),
        "an already-slow attempt cannot also consume a long Retry-After"
    );
    assert!(
        !retry_fits_budget(2, Duration::ZERO, Duration::ZERO),
        "a second failed attempt must be final"
    );
    assert!(
        !retry_fits_budget(1, Duration::ZERO, Duration::from_secs(30)),
        "a long Retry-After must not park a native render"
    );
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
            cache_data: Some(b"cached"),
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
            cache_data: Some(b"cached"),
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
        Some(Duration::from_hours(1))
    );
    assert_eq!(
        parse_max_age("max-age=60, stale-while-revalidate=120"),
        Some(Duration::from_mins(1))
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
    let expires = modified + Duration::from_hours(1);
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

/// Drives the shared-cache race branch on a real source and a real cache, so the
/// early return that serves the body is itself covered.
///
/// The classification helpers are tested separately, but a test over those alone
/// stays green if the early return is deleted — which is the production failure:
/// a requester still awaiting bytes parks for up to five minutes and then gets a
/// bodyless 304.
#[tokio::test(start_paused = true)]
async fn a_peer_cache_fill_is_served_to_every_requester_still_awaiting_bytes() {
    fn inputs(prior_data_present: bool, prior_metadata_present: bool) -> SharedCacheRaceInputs {
        background_inputs(prior_data_present, prior_metadata_present, true)
    }

    // (label, prior_data, prior_metadata) as MainResourceLoader would set them.
    for (label, prior_data, prior_metadata) in [
        ("database miss", false, false),
        ("stale body withheld for revalidation", true, true),
    ] {
        let cache = cache::ResourceCache::new(1024 * 1024);
        let source = NetworkFileSource::new(
            cache.clone(),
            vec!["resource.test".to_string()],
            FileSourceIoPermits::default(),
            ProviderHealthTracker::default(),
            "test-agent",
        )
        .expect("file source builds");

        let key = Arc::new(ResourceRequestKey::test_key(
            "https://resource.test/glyphs",
            ResourceKind::Glyphs,
        ));
        // A concurrent renderer lands the body after this request's Database
        // lookup was already decided.
        assert!(
            cache.store(
                key.clone(),
                Response::data(b"filled-by-peer".to_vec())
                    .with_expires(SystemTime::now() + Duration::from_hours(24)),
                CachedFreshness::default(),
            ),
            "{label}: the peer fill is cacheable"
        );

        let mut observation = RequestObservation::for_test("glyphs");
        let response = source
            .resolve_shared_cache_race(&key, inputs(prior_data, prior_metadata), &mut observation)
            .await
            .unwrap_or_else(|| panic!("{label}: the race branch must resolve, not fall through"));

        assert_eq!(
            response.data.as_deref(),
            Some(b"filled-by-peer".as_slice()),
            "{label}: a requester still awaiting bytes must receive the body"
        );
        assert!(
            !response.not_modified,
            "{label}: a bodyless 304 leaves the render without this resource"
        );
    }

    // The one case that may park: Database already delivered a usable body.
    let cache = cache::ResourceCache::new(1024 * 1024);
    let source = NetworkFileSource::new(
        cache.clone(),
        vec!["resource.test".to_string()],
        FileSourceIoPermits::default(),
        ProviderHealthTracker::default(),
        "test-agent",
    )
    .expect("file source builds");
    let key = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/glyphs",
        ResourceKind::Glyphs,
    ));
    assert!(
        cache.store(
            key.clone(),
            Response::data(b"filled-by-peer".to_vec())
                .with_expires(SystemTime::now() + Duration::from_secs(30)),
            CachedFreshness::default(),
        )
    );
    let mut observation = RequestObservation::for_test("glyphs");
    let response = source
        .resolve_shared_cache_race(&key, inputs(false, true), &mut observation)
        .await
        .expect("a delivered requester still resolves, after parking");
    assert!(
        response.not_modified,
        "a requester that already holds the body gets a 304, not a second copy"
    );
}

/// A `Delivered` classification is not enough on its own: mbgl demotes the paired
/// background refresh to `Low` on the very branch that delivers the body, so a
/// request still carrying `regular` priority was not produced by that branch and
/// something is waiting on it.
///
/// Parking such a request stalls the render behind it for up to five minutes. A
/// fully cold process cannot expose this — it holds nothing to revalidate — which
/// is why the cold reproduction passed while long-lived pods kept timing out.
#[test]
fn a_regular_priority_request_is_never_parked_even_when_classified_delivered() {
    let expires = SystemTime::now() + Duration::from_hours(24);
    let cached = Response::data(b"cached".to_vec()).with_expires(expires);

    match resolve_fresh_cache_race(&cached, NativeDatabaseState::Delivered, false) {
        FreshCacheResolution::Serve(response) => {
            assert_eq!(response.data.as_deref(), Some(b"cached".as_slice()));
        }
        FreshCacheResolution::Defer(_) => {
            panic!("a request that was not demoted to background must not be parked")
        }
    }

    // The genuine background refresh still parks.
    match resolve_fresh_cache_race(&cached, NativeDatabaseState::Delivered, true) {
        FreshCacheResolution::Defer(observed) => assert_eq!(observed, expires),
        FreshCacheResolution::Serve(_) => panic!("the demoted refresh may still park"),
    }
}

/// Inputs as the production call site builds them, with the background-refresh
/// signal under test control.
fn background_inputs(
    prior_data_present: bool,
    prior_metadata_present: bool,
    is_background_refresh: bool,
) -> SharedCacheRaceInputs {
    SharedCacheRaceInputs {
        consults_shared_cache: true,
        prior_data_present,
        prior_metadata_present,
        kind: "glyphs",
        is_low_priority: is_background_refresh,
        is_online: true,
        minimum_update_interval: Duration::from_secs(0),
    }
}

#[test]
fn failed_background_refreshes_are_cooled_down_per_resource() {
    let cache = cache::ResourceCache::new(1024 * 1024);
    let source = NetworkFileSource::new(
        cache.clone(),
        vec!["resource.test".to_string()],
        FileSourceIoPermits::default(),
        ProviderHealthTracker::default(),
        "test-agent",
    )
    .expect("file source builds");
    let key = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/glyphs",
        ResourceKind::Glyphs,
    ));
    assert!(cache.store(
        key.clone(),
        Response::data(b"stale".to_vec()).with_expires(SystemTime::now() - Duration::from_secs(1)),
        CachedFreshness {
            stale_until: Some(SystemTime::now() + Duration::from_secs(60)),
            ..CachedFreshness::default()
        },
    ));
    let background = background_inputs(false, true, true);

    source.update_refresh_failure_cooldown(
        &key,
        background,
        &Response::error(ErrorReason::Server, "provider unavailable"),
    );
    let mut observation = RequestObservation::for_test("glyphs");
    let suppressed = source
        .resolve_refresh_failure_cooldown(&key, background, &mut observation)
        .expect("the next speculative refresh is suppressed");
    assert!(suppressed.not_modified);
    assert_eq!(observation.outcome, "refresh_cooldown");

    source.update_refresh_failure_cooldown(&key, background, &Response::data(b"fresh".to_vec()));
    assert!(
        source
            .resolve_refresh_failure_cooldown(&key, background, &mut observation)
            .is_none(),
        "a successful refresh clears the cooldown"
    );
}

#[test]
fn refresh_cooldown_never_suppresses_foreground_or_out_of_window_revalidation() {
    let cache = cache::ResourceCache::new(1024 * 1024);
    let source = NetworkFileSource::new(
        cache.clone(),
        vec!["resource.test".to_string()],
        FileSourceIoPermits::default(),
        ProviderHealthTracker::default(),
        "test-agent",
    )
    .expect("file source builds");
    let key = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/glyphs",
        ResourceKind::Glyphs,
    ));
    assert!(
        cache.store(
            key.clone(),
            Response::data(b"stale".to_vec())
                .with_expires(SystemTime::now() - Duration::from_secs(120)),
            CachedFreshness {
                stale_until: Some(SystemTime::now() - Duration::from_secs(60)),
                ..CachedFreshness::default()
            },
        )
    );
    let background = background_inputs(false, true, true);
    source.refresh_failure_cooldown.insert(key.clone(), ());

    let mut observation = RequestObservation::for_test("glyphs");
    assert!(
        source
            .resolve_refresh_failure_cooldown(&key, background, &mut observation)
            .is_none(),
        "strict revalidation after the SWR window must still reach the provider"
    );

    let mut foreground = background;
    foreground.is_low_priority = false;
    assert!(
        source
            .resolve_refresh_failure_cooldown(&key, foreground, &mut observation)
            .is_none(),
        "foreground revalidation is never suppressed"
    );
}

/// Pins the *production wiring*, not just the decision function.
///
/// The pure-function test would stay green if the call site were changed to pass
/// `true` unconditionally, which is exactly the regression that reintroduces the
/// stall. Driving `resolve_shared_cache_race` with regular priority proves the
/// real path forwards the background-refresh signal.
///
/// Paused time makes the assertion sharp: a park would sleep until expiry, so
/// returning at all means no park happened.
#[tokio::test(start_paused = true)]
async fn the_call_site_forwards_the_background_refresh_signal() {
    let cache = cache::ResourceCache::new(1024 * 1024);
    let source = NetworkFileSource::new(
        cache.clone(),
        vec!["resource.test".to_string()],
        FileSourceIoPermits::default(),
        ProviderHealthTracker::default(),
        "test-agent",
    )
    .expect("file source builds");
    let key = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/glyphs",
        ResourceKind::Glyphs,
    ));
    assert!(
        cache.store(
            key.clone(),
            Response::data(b"cached".to_vec())
                .with_expires(SystemTime::now() + Duration::from_hours(24)),
            CachedFreshness::default(),
        )
    );

    // Classified `Delivered` (metadata, no prior body) but *not* demoted to
    // background: something is waiting on it.
    let mut observation = RequestObservation::for_test("glyphs");
    let response = source
        .resolve_shared_cache_race(
            &key,
            background_inputs(false, true, false),
            &mut observation,
        )
        .await
        .expect("a request awaiting bytes must resolve immediately");
    assert_eq!(
        response.data.as_deref(),
        Some(b"cached".as_slice()),
        "a regular-priority request must receive the body, not a park"
    );

    // The genuine background refresh still parks, so the gate is not simply off.
    let mut observation = RequestObservation::for_test("glyphs");
    let parked = source
        .resolve_shared_cache_race(&key, background_inputs(false, true, true), &mut observation)
        .await
        .expect("the demoted refresh resolves after parking");
    assert!(
        parked.not_modified,
        "the background refresh still revalidates rather than re-delivering"
    );
}

/// `Low` alone does not mean "body already delivered": `offline_download.cpp` sets
/// low priority on requests that are still awaiting bytes. Biei renders online
/// today so that path is unreachable here, but this type is a reusable library.
#[tokio::test(start_paused = true)]
async fn offline_usage_is_never_parked_even_at_low_priority() {
    let cache = cache::ResourceCache::new(1024 * 1024);
    let source = NetworkFileSource::new(
        cache.clone(),
        vec!["resource.test".to_string()],
        FileSourceIoPermits::default(),
        ProviderHealthTracker::default(),
        "test-agent",
    )
    .expect("file source builds");
    let key = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/glyphs",
        ResourceKind::Glyphs,
    ));
    assert!(
        cache.store(
            key.clone(),
            Response::data(b"cached".to_vec())
                .with_expires(SystemTime::now() + Duration::from_hours(24)),
            CachedFreshness::default(),
        )
    );

    let mut offline = background_inputs(false, true, true);
    offline.is_online = false;
    let mut observation = RequestObservation::for_test("glyphs");
    let response = source
        .resolve_shared_cache_race(&key, offline, &mut observation)
        .await
        .expect("offline low-priority work must resolve immediately");
    assert_eq!(
        response.data.as_deref(),
        Some(b"cached".as_slice()),
        "an offline download awaiting bytes must not be parked"
    );
}

/// Ishikari's own tile policy, which a stored zero-byte tile carries on its 204.
fn empty_tile_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(
            "public, max-age=3600, s-maxage=86400, stale-while-revalidate=604800",
        ),
    );
    headers
}

#[test]
fn an_empty_tile_is_retained_under_the_provider_freshness() {
    // Without this the 204 was neither stored as a body nor negative-cached,
    // so every render re-fetched every empty tile despite the provider
    // declaring a day of shared freshness.
    let headers = empty_tile_headers();
    let CachePolicy::Store { freshness } = empty_representation_cache_policy(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &headers,
    ) else {
        panic!("an empty tile is retained");
    };
    assert_eq!(freshness.lifetime, Some(Duration::from_secs(86_400)));
    assert_eq!(
        freshness.stale_while_revalidate,
        Some(Duration::from_secs(604_800))
    );

    let response = map_response(204, &headers, &[], ResourceKind::Tile);
    assert!(response.no_content, "native still sees an empty tile");
    assert!(response.data.is_none());
    assert!(
        response
            .expires
            .is_some_and(|expires| expires > SystemTime::now()),
        "the entry carries the provider's expiry, not a fabricated one"
    );
}

#[tokio::test]
async fn a_fresh_empty_tile_is_reused_after_mln_falls_through_to_network() {
    let headers = empty_tile_headers();
    let cache = cache::ResourceCache::new(4096);
    let source = NetworkFileSource::new(
        cache.clone(),
        vec!["resource.test".to_string()],
        FileSourceIoPermits::default(),
        ProviderHealthTracker::default(),
        "test-agent",
    )
    .expect("file source builds");
    let key = Arc::new(ResourceRequestKey::test_key(
        "https://resource.test/empty.pbf",
        ResourceKind::Tile,
    ));
    let CachePolicy::Store { freshness } = empty_representation_cache_policy(
        ResourceKind::Tile,
        maplibre_native::file_source::StoragePolicy::Permanent,
        &headers,
    ) else {
        panic!("an empty tile is retained");
    };

    assert!(
        cache.store(
            Arc::clone(&key),
            map_response(204, &headers, &[], ResourceKind::Tile),
            freshness,
        ),
        "an error-free empty representation is storable"
    );

    // MainResourceLoader falls through to Network when Database returns
    // `noContent`. The Network adapter must close that gap from the shared
    // cache instead of issuing another HTTP request.
    let mut observation = RequestObservation::for_test("tile");
    let response = source
        .resolve_shared_cache_race(
            &key,
            SharedCacheRaceInputs {
                consults_shared_cache: true,
                prior_data_present: false,
                prior_metadata_present: false,
                kind: "tile",
                is_low_priority: false,
                is_online: true,
                minimum_update_interval: Duration::ZERO,
            },
            &mut observation,
        )
        .await
        .expect("the fresh empty result must resolve before provider I/O");
    assert!(response.no_content);
}

#[test]
fn an_empty_tile_the_provider_refuses_to_share_is_not_retained() {
    let mut headers = HeaderMap::new();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    assert_eq!(
        empty_representation_cache_policy(
            ResourceKind::Tile,
            maplibre_native::file_source::StoragePolicy::Permanent,
            &headers,
        ),
        CachePolicy::Remove
    );
}

#[test]
fn an_empty_required_resource_or_volatile_request_is_never_retained() {
    let headers = empty_tile_headers();
    // A required resource answering 204 is a provider fault, not an empty tile.
    for kind in [
        ResourceKind::Glyphs,
        ResourceKind::SpriteJSON,
        ResourceKind::Style,
        ResourceKind::Source,
    ] {
        assert_eq!(
            empty_representation_cache_policy(
                kind,
                maplibre_native::file_source::StoragePolicy::Permanent,
                &headers,
            ),
            CachePolicy::Unchanged,
            "{kind:?}"
        );
    }
    assert_eq!(
        empty_representation_cache_policy(
            ResourceKind::Tile,
            maplibre_native::file_source::StoragePolicy::Volatile,
            &headers,
        ),
        CachePolicy::Unchanged,
        "a volatile request never populates the shared cache"
    );
}

#[test]
fn an_undeclared_empty_tile_is_capped_at_the_negative_cache_bound() {
    // Nothing declared, so the body heuristic would grant minutes. The same
    // "this area is empty" fact arriving as a 404 is capped at 15s because an
    // empty tile can fill in, and a 204 must not be retained longer just for
    // having a different status.
    let response = empty_representation(ResourceKind::Tile, &HeaderMap::new());
    let remaining = response
        .expires
        .expect("an empty tile still carries a bounded expiry")
        .duration_since(SystemTime::now())
        .expect("the expiry is in the future");
    assert!(
        remaining <= NEGATIVE_CACHE_TTL,
        "undeclared empty-tile freshness must not exceed the negative-cache bound, got {remaining:?}"
    );
}
