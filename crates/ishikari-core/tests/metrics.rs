use std::time::Duration;

use ishikari_core::metrics::{NodeMetrics, NodeMetricsSnapshot};

fn node_metrics_snapshot(value: u64) -> NodeMetricsSnapshot {
    NodeMetricsSnapshot {
        peer_forward_successes: value,
        peer_forward_not_found: value,
        peer_forward_retryable: value,
        peer_forward_fatal: value,
        peer_forward_backoff_skips: value,
        peer_tile_fetches: value,
        peer_bootstrap_fetches: value,
        peer_leaf_fetches: value,
        peer_provider_fetches: value,
        peer_tile_duplicate_inflight: value,
        peer_bootstrap_duplicate_inflight: value,
        peer_leaf_duplicate_inflight: value,
        peer_provider_duplicate_inflight: value,
        internal_tile_requests: value,
        internal_bootstrap_requests: value,
        internal_leaf_requests: value,
        internal_provider_requests: value,
        backend_fetches: value,
        backend_fetch_successes: value,
        backend_fetch_not_found: value,
        backend_fetch_errors: value,
        backend_fetch_timeouts: value,
        backend_fetched_chunks: value,
        chunk_cache_hits: value,
        chunk_cache_misses: value,
        chunk_cache_post_fetch_hits: value,
        chunk_fetch_queued: value,
        chunk_fetch_joined_pending: value,
        chunk_fetch_joined_inflight: value,
        chunk_dispatch_immediate: value,
        chunk_dispatch_window: value,
        chunk_dispatch_pending_chunks: value,
        chunk_waiters_released: value,
    }
}

#[test]
fn merges_every_node_metric_counter() {
    let mut total = node_metrics_snapshot(1);

    total.merge(&node_metrics_snapshot(2));

    assert_eq!(total, node_metrics_snapshot(3));
}

#[test]
fn exposes_http_request_duration_by_bounded_route_and_status_class() {
    let metrics = NodeMetrics::new();

    metrics.record_http(
        "/tilesets/{tileset_id}/{z}/{x}/{y}",
        "200",
        Duration::from_millis(125),
    );
    metrics.record_http(
        "/tilesets/{tileset_id}/{z}/{x}/{y}",
        "404",
        Duration::from_millis(25),
    );

    let encoded = metrics.encode();
    assert!(encoded.contains(
        "ishikari_http_requests_total{endpoint=\"/tilesets/{tileset_id}/{z}/{x}/{y}\",status=\"200\"} 1"
    ));
    assert!(encoded.contains(
        "ishikari_http_request_duration_seconds_count{endpoint=\"/tilesets/{tileset_id}/{z}/{x}/{y}\",status_class=\"2xx\"} 1"
    ));
    assert!(encoded.contains(
        "ishikari_http_request_duration_seconds_sum{endpoint=\"/tilesets/{tileset_id}/{z}/{x}/{y}\",status_class=\"2xx\"} 0.125"
    ));
    assert!(encoded.contains(
        "ishikari_http_request_duration_seconds_count{endpoint=\"/tilesets/{tileset_id}/{z}/{x}/{y}\",status_class=\"4xx\"} 1"
    ));
}

#[test]
fn records_metrics_scrape_count_without_self_observing_duration() {
    let metrics = NodeMetrics::new();

    metrics.record_http_request("/_internal/metrics", "200");

    let encoded = metrics.encode();
    assert!(encoded.contains(
        "ishikari_http_requests_total{endpoint=\"/_internal/metrics\",status=\"200\"} 1"
    ));
    assert!(
        !encoded.contains(
            "ishikari_http_request_duration_seconds_count{endpoint=\"/_internal/metrics\""
        )
    );
}

#[test]
fn exposes_cpu_work_admission_queue_and_state() {
    let metrics = NodeMetrics::new();

    metrics.record_cpu_work_admission("dem_decode", "accepted");
    metrics.record_cpu_work_admission("terrain_generate", "shed");
    metrics.record_cpu_work_queue_duration("dem_decode", Duration::from_millis(20));
    metrics.set_cpu_work(3, 2, 2, 8);

    let encoded = metrics.encode();
    assert!(
        encoded.contains(
            "ishikari_cpu_work_admission_total{outcome=\"accepted\",work=\"dem_decode\"} 1"
        )
    );
    assert!(encoded.contains(
        "ishikari_cpu_work_admission_total{outcome=\"shed\",work=\"terrain_generate\"} 1"
    ));
    assert!(
        encoded.contains("ishikari_cpu_work_queue_duration_seconds_count{work=\"dem_decode\"} 1")
    );
    assert!(encoded.contains("ishikari_cpu_work{state=\"inflight\"} 3"));
    assert!(encoded.contains("ishikari_cpu_work{state=\"running\"} 2"));
    assert!(encoded.contains("ishikari_cpu_work{state=\"concurrency\"} 2"));
    assert!(encoded.contains("ishikari_cpu_work{state=\"max_inflight\"} 8"));
}

#[test]
fn exposes_derived_terrain_cost_by_bounded_product() {
    let metrics = NodeMetrics::new();

    metrics.record_terrain_generation(
        "hillshade",
        Duration::from_millis(400),
        Duration::from_millis(125),
        9,
        65_536,
    );

    let encoded = metrics.encode();
    assert!(
        encoded.contains("ishikari_terrain_source_duration_seconds_sum{product=\"hillshade\"} 0.4")
    );
    assert!(
        encoded.contains(
            "ishikari_terrain_generation_duration_seconds_sum{product=\"hillshade\"} 0.125"
        )
    );
    assert!(encoded.contains("ishikari_terrain_source_tiles_sum{product=\"hillshade\"} 9"));
    assert!(
        encoded.contains("ishikari_terrain_output_size_bytes_sum{product=\"hillshade\"} 65536")
    );
}

#[test]
fn exposes_tile_serving_and_cache_metrics() {
    let metrics = NodeMetrics::new();

    metrics.record_tile_served("cache");
    metrics.record_tile_served("miss");
    metrics.record_tile_cache("hit");
    metrics.record_tile_cache("miss");
    metrics.record_tile_cache("insert");
    metrics.record_tile_cache("negative");

    let encoded = metrics.encode();

    assert!(encoded.contains("ishikari_tiles_served_total{source=\"cache\"} 1"));
    assert!(encoded.contains("ishikari_tiles_served_total{source=\"miss\"} 1"));
    assert!(encoded.contains("ishikari_tile_cache_total{outcome=\"hit\"} 1"));
    assert!(encoded.contains("ishikari_tile_cache_total{outcome=\"miss\"} 1"));
    assert!(encoded.contains("ishikari_tile_cache_total{outcome=\"insert\"} 1"));
    assert!(encoded.contains("ishikari_tile_cache_total{outcome=\"negative\"} 1"));
}

#[test]
fn exposes_bounded_peer_forward_outcomes() {
    let metrics = NodeMetrics::new();

    metrics.record_peer_forward("success");
    metrics.record_peer_forward("retryable");
    metrics.record_peer_forward("backoff");
    metrics.record_peer_forward("backoff");

    let encoded = metrics.encode();
    assert!(encoded.contains("ishikari_peer_forward_total{outcome=\"success\"} 1"));
    assert!(encoded.contains("ishikari_peer_forward_total{outcome=\"retryable\"} 1"));
    assert!(encoded.contains("ishikari_peer_forward_total{outcome=\"backoff\"} 2"));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.peer_forward_successes, 1);
    assert_eq!(snapshot.peer_forward_retryable, 1);
    assert_eq!(snapshot.peer_forward_backoff_skips, 2);
}

#[test]
fn exposes_peer_fetches_by_resource() {
    let metrics = NodeMetrics::new();

    metrics.record_peer_fetch("bootstrap", "success");
    metrics.record_peer_fetch("leaf", "retryable");

    let encoded = metrics.encode();
    assert!(
        encoded.contains("ishikari_peer_fetch_total{outcome=\"success\",resource=\"bootstrap\"} 1")
    );
    assert!(
        encoded.contains("ishikari_peer_fetch_total{outcome=\"retryable\",resource=\"leaf\"} 1")
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.peer_bootstrap_fetches, 1);
    assert_eq!(snapshot.peer_leaf_fetches, 1);
}

#[test]
fn exposes_identical_peer_fetch_overlap() {
    let metrics = NodeMetrics::new();

    metrics.record_peer_fetch_duplicate_inflight("tile");
    metrics.record_peer_fetch_duplicate_inflight("tile");

    let encoded = metrics.encode();
    assert!(encoded.contains("ishikari_peer_fetch_duplicate_inflight_total{resource=\"tile\"} 2"));
    assert_eq!(metrics.snapshot().peer_tile_duplicate_inflight, 2);
}

#[test]
fn exposes_internal_resources_served_by_resource() {
    let metrics = NodeMetrics::new();

    metrics.record_internal_resource_request("bootstrap", "success");
    metrics.record_internal_resource_request("leaf", "not_found");

    let encoded = metrics.encode();
    assert!(encoded.contains(
        "ishikari_internal_resource_requests_total{outcome=\"success\",resource=\"bootstrap\"} 1"
    ));
    assert!(encoded.contains(
        "ishikari_internal_resource_requests_total{outcome=\"not_found\",resource=\"leaf\"} 1"
    ));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.internal_bootstrap_requests, 1);
    assert_eq!(snapshot.internal_leaf_requests, 1);
}

#[test]
fn exposes_provider_resource_cache_activity() {
    let metrics = NodeMetrics::new();

    metrics.record_provider_resource_cache("style", "miss");
    metrics.record_provider_resource_cache("style", "insert");
    metrics.record_provider_resource_cache("glyph", "singleflight_join");
    metrics.record_provider_resource_cache("sprite", "negative_hit");

    let encoded = metrics.encode();
    assert!(
        encoded.contains(
            "ishikari_provider_resource_cache_total{outcome=\"miss\",resource=\"style\"} 1"
        )
    );
    assert!(encoded.contains(
        "ishikari_provider_resource_cache_total{outcome=\"insert\",resource=\"style\"} 1"
    ));
    assert!(encoded.contains(
        "ishikari_provider_resource_cache_total{outcome=\"singleflight_join\",resource=\"glyph\"} 1"
    ));
    assert!(encoded.contains(
        "ishikari_provider_resource_cache_total{outcome=\"negative_hit\",resource=\"sprite\"} 1"
    ));
}

#[test]
fn records_actual_backend_bytes_once_at_fetch_completion() {
    let metrics = NodeMetrics::new();

    metrics.record_backend_fetch("success", Duration::from_millis(10), 1, 10);
    metrics.record_backend_fetch("timeout", Duration::from_millis(20), 2, 0);
    metrics.record_backend_fetch("error", Duration::from_millis(25), 2, 4);
    metrics.record_backend_fetch("success", Duration::from_millis(30), 3, 15);

    let first_scrape = metrics.encode();
    let second_scrape = metrics.encode();

    assert!(first_scrape.contains("ishikari_backend_fetch_bytes_total 29"));
    assert!(second_scrape.contains("ishikari_backend_fetch_bytes_total 29"));
}

#[test]
fn exposes_backend_fetch_histograms_and_chunk_config() {
    let metrics = NodeMetrics::new();

    metrics.set_chunk_config(1_048_576, 8);
    metrics.set_backend_fetch_concurrency_limit(32);
    metrics.set_backend_fetch_max_inflight(128);
    metrics.adjust_backend_fetch_concurrency("waiting", 1);
    metrics.adjust_backend_fetch_concurrency("waiting", -1);
    metrics.adjust_backend_fetch_concurrency("active", 1);
    metrics.record_backend_fetch_queue(Duration::from_millis(125));
    metrics.set_chunk_fetch_merge_window(Duration::from_millis(10));
    metrics.record_backend_fetch("success", Duration::from_millis(250), 4, 4_194_304);
    metrics.record_chunk_fetch_dispatch("window", Duration::from_millis(10), 6);
    metrics.record_chunk_fetch_group_waiters("success", 12);
    metrics.record_chunk_fetch_group_waiters("shed", 3);
    metrics.record_chunk_cache("hit");
    metrics.record_chunk_cache("miss");
    metrics.record_chunk_fetch_wait("queued");
    metrics.record_chunk_fetch_wait("joined_inflight");
    metrics.record_cancelled_chunk_fetch_waiters(2);

    let encoded = metrics.encode();

    assert!(encoded.contains("ishikari_chunk_size_bytes 1048576"));
    assert!(encoded.contains("ishikari_max_fetch_chunks 8"));
    assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"active\"} 1"));
    assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"limit\"} 32"));
    assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"max_inflight\"} 128"));
    assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"waiting\"} 0"));
    assert!(encoded.contains("ishikari_backend_fetch_queue_duration_seconds_count 1"));
    assert!(encoded.contains("ishikari_backend_fetch_queue_duration_seconds_sum 0.125"));
    assert!(encoded.contains("ishikari_chunk_fetch_merge_window_seconds 0.01"));
    assert!(
        encoded.contains("ishikari_backend_fetch_duration_seconds_count{outcome=\"success\"} 1")
    );
    assert!(encoded.contains("ishikari_backend_fetch_bytes_total 4194304"));
    assert!(
        encoded.contains("ishikari_backend_fetch_duration_seconds_sum{outcome=\"success\"} 0.25")
    );
    assert!(encoded.contains("ishikari_backend_fetch_size_bytes_count{outcome=\"success\"} 1"));
    assert!(encoded.contains("ishikari_backend_fetch_size_bytes_sum{outcome=\"success\"} 4194304"));
    assert!(encoded.contains("ishikari_backend_fetch_chunks_count{outcome=\"success\"} 1"));
    assert!(encoded.contains("ishikari_backend_fetch_chunks_sum{outcome=\"success\"} 4"));
    assert!(encoded.contains("ishikari_chunk_fetch_queue_delay_seconds_count{flush=\"window\"} 1"));
    assert!(encoded.contains("ishikari_chunk_fetch_pending_chunks_sum{flush=\"window\"} 6"));
    assert!(encoded.contains("ishikari_chunk_fetch_group_waiters_sum{outcome=\"success\"} 12"));
    assert!(encoded.contains("ishikari_chunk_fetch_group_waiters_sum{outcome=\"shed\"} 3"));
    assert!(encoded.contains("ishikari_chunk_cache_total{outcome=\"hit\"} 1"));
    assert!(encoded.contains("ishikari_chunk_cache_total{outcome=\"miss\"} 1"));
    assert!(encoded.contains("ishikari_chunk_fetch_wait_total{outcome=\"queued\"} 1"));
    assert!(encoded.contains("ishikari_chunk_fetch_wait_total{outcome=\"joined_inflight\"} 1"));
    assert!(encoded.contains("ishikari_chunk_fetch_wait_total{outcome=\"cancelled\"} 2"));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.backend_fetches, 1);
    assert_eq!(snapshot.backend_fetch_successes, 1);
    assert_eq!(snapshot.backend_fetched_chunks, 4);
    assert_eq!(snapshot.chunk_cache_hits, 1);
    assert_eq!(snapshot.chunk_cache_misses, 1);
    assert_eq!(snapshot.chunk_fetch_queued, 1);
    assert_eq!(snapshot.chunk_fetch_joined_inflight, 1);
    assert_eq!(snapshot.chunk_dispatch_window, 1);
    assert_eq!(snapshot.chunk_dispatch_pending_chunks, 6);
    assert_eq!(snapshot.chunk_waiters_released, 15);

    let histograms = metrics.histogram_snapshot();
    assert_eq!(histograms.backend_fetch_duration_seconds.count, 1);
    assert_eq!(histograms.backend_fetch_duration_seconds.sum, 0.25);
    assert_eq!(histograms.backend_fetch_queue_duration_seconds.count, 1);
    assert_eq!(histograms.backend_fetch_queue_duration_seconds.sum, 0.125);
    assert_eq!(histograms.backend_fetch_size_bytes.sum, 4_194_304.0);
    assert_eq!(histograms.backend_fetch_chunks.sum, 4.0);
    assert_eq!(histograms.queue_delay_window_seconds.count, 1);
    assert_eq!(histograms.pending_chunks_window.sum, 6.0);
    assert_eq!(histograms.group_waiters.sum, 15.0);
}
