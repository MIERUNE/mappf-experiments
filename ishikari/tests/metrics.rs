use std::time::Duration;

use ishikari::metrics::NodeMetrics;

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
fn syncs_backend_fetch_bytes_as_monotonic_counter() {
    let metrics = NodeMetrics::new();

    metrics.sync_backend_fetch_bytes(10);
    metrics.sync_backend_fetch_bytes(25);
    metrics.sync_backend_fetch_bytes(20);

    let encoded = metrics.encode();

    assert!(encoded.contains("ishikari_backend_fetch_bytes_total 25"));
}

#[test]
fn exposes_backend_fetch_histograms_and_chunk_config() {
    let metrics = NodeMetrics::new();

    metrics.set_chunk_config(1_048_576, 8);
    metrics.set_chunk_fetch_merge_window(Duration::from_millis(10));
    metrics.record_backend_fetch("success", Duration::from_millis(250), 4, 4_194_304);
    metrics.record_chunk_fetch_dispatch("window", Duration::from_millis(10), 6);
    metrics.record_chunk_fetch_group_waiters("success", 12);
    metrics.record_chunk_cache("hit");
    metrics.record_chunk_cache("miss");
    metrics.record_chunk_fetch_wait("queued");
    metrics.record_chunk_fetch_wait("joined_inflight");

    let encoded = metrics.encode();

    assert!(encoded.contains("ishikari_chunk_size_bytes 1048576"));
    assert!(encoded.contains("ishikari_max_fetch_chunks 8"));
    assert!(encoded.contains("ishikari_chunk_fetch_merge_window_seconds 0.01"));
    assert!(
        encoded.contains("ishikari_backend_fetch_duration_seconds_count{outcome=\"success\"} 1")
    );
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
    assert!(encoded.contains("ishikari_chunk_cache_total{outcome=\"hit\"} 1"));
    assert!(encoded.contains("ishikari_chunk_cache_total{outcome=\"miss\"} 1"));
    assert!(encoded.contains("ishikari_chunk_fetch_wait_total{outcome=\"queued\"} 1"));
    assert!(encoded.contains("ishikari_chunk_fetch_wait_total{outcome=\"joined_inflight\"} 1"));

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
    assert_eq!(snapshot.chunk_waiters_released, 12);

    let histograms = metrics.histogram_snapshot();
    assert_eq!(histograms.backend_fetch_duration_seconds.count, 1);
    assert_eq!(histograms.backend_fetch_duration_seconds.sum, 0.25);
    assert_eq!(histograms.backend_fetch_size_bytes.sum, 4_194_304.0);
    assert_eq!(histograms.backend_fetch_chunks.sum, 4.0);
    assert_eq!(histograms.queue_delay_window_seconds.count, 1);
    assert_eq!(histograms.pending_chunks_window.sum, 6.0);
    assert_eq!(histograms.group_waiters.sum, 12.0);
}
