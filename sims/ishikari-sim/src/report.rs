use std::collections::BTreeMap;

use ishikari_core::{
    metrics::{HistogramSnapshot, NodeHistogramSnapshot, NodeMetricsSnapshot},
    storage::TileSource,
};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(usize)]
pub(crate) enum SourceCategory {
    SelfCache,
    SelfBackend,
    Miss,
    PeerCache,
    PeerBackend,
}

impl SourceCategory {
    const ALL: [Self; 5] = [
        Self::SelfCache,
        Self::SelfBackend,
        Self::Miss,
        Self::PeerCache,
        Self::PeerBackend,
    ];

    pub(crate) fn from_tile_source(source: TileSource) -> Self {
        match source {
            TileSource::SelfTileCache | TileSource::SelfChunkCache => Self::SelfCache,
            TileSource::SelfBackend => Self::SelfBackend,
            TileSource::NegativeCache | TileSource::SelfMiss | TileSource::PeerMiss => Self::Miss,
            TileSource::PeerCache => Self::PeerCache,
            TileSource::PeerBackend => Self::PeerBackend,
        }
    }

    pub(crate) const fn report_label(self) -> &'static str {
        match self {
            Self::SelfCache => "self_cache",
            Self::SelfBackend => "self_backend",
            Self::Miss => "miss",
            Self::PeerCache => "peer_cache",
            Self::PeerBackend => "peer_backend",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SourceCounts([u64; SourceCategory::ALL.len()]);

impl SourceCounts {
    pub(crate) fn increment(&mut self, source: TileSource) {
        self.0[SourceCategory::from_tile_source(source) as usize] += 1;
    }

    pub(crate) fn to_report_map(&self) -> BTreeMap<String, u64> {
        SourceCategory::ALL
            .into_iter()
            .filter_map(|source| {
                let count = self.0[source as usize];
                (count != 0).then(|| (source.report_label().to_owned(), count))
            })
            .collect()
    }
}

#[derive(Debug, Default, Serialize)]
pub struct SimReport {
    pub requests: u64,
    pub found: u64,
    pub not_found: u64,
    pub served_bytes: u64,
    pub by_source: BTreeMap<String, u64>,
    pub peer_requests: u64,
    pub peer_bytes: u64,
    pub peer_unavailable_requests: u64,
    pub gossip_messages: u64,
    pub gossip_bytes: u64,
    pub backend_bytes: u64,
    pub tile_cache_bytes: u64,
    pub chunk_cache_bytes: u64,
    pub l1_cache_hits: u64,
    pub l1_cache_hit_rate: f64,
    pub cache_hit_rate: f64,
    pub peer_forward_rate: f64,
    pub read_amplification: f64,
    pub node_request_load: NodeRequestLoadReport,
    pub metrics: NodeMetricsSnapshot,
    pub scheduler: SchedulerReport,
    pub nodes: Vec<NodeReport>,
}

impl SimReport {
    pub(crate) fn finalize_derived_metrics(&mut self) {
        if self.requests > 0 {
            self.l1_cache_hit_rate = self.l1_cache_hits as f64 / self.requests as f64;
            self.cache_hit_rate = (self.source_count("self_cache")
                + self.source_count("peer_cache")) as f64
                / self.requests as f64;
            self.peer_forward_rate = (self.source_count("peer_cache")
                + self.source_count("peer_backend")) as f64
                / self.requests as f64;
        }
        if self.served_bytes > 0 {
            self.read_amplification = self.backend_bytes as f64 / self.served_bytes as f64;
        }
    }

    fn source_count(&self, source: &str) -> u64 {
        self.by_source.get(source).copied().unwrap_or(0)
    }

    pub(crate) fn set_histograms(&mut self, histograms: &NodeHistogramSnapshot) {
        self.scheduler = SchedulerReport::from_histograms(histograms);
    }

    pub(crate) fn set_node_request_load(&mut self) {
        self.node_request_load = NodeRequestLoadReport::from_nodes(&self.nodes);
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NodeRequestLoadReport {
    pub participating_nodes: usize,
    pub mean_requests_per_node: f64,
    pub max_requests: u64,
    pub max_to_mean: f64,
    pub coefficient_of_variation: f64,
}

impl NodeRequestLoadReport {
    fn from_nodes(nodes: &[NodeReport]) -> Self {
        if nodes.is_empty() {
            return Self::default();
        }
        let count = nodes.len();
        let total = nodes.iter().map(|node| node.requests).sum::<u64>();
        let mean = total as f64 / count as f64;
        let max_requests = nodes.iter().map(|node| node.requests).max().unwrap_or(0);
        let variance = nodes
            .iter()
            .map(|node| {
                let deviation = node.requests as f64 - mean;
                deviation * deviation
            })
            .sum::<f64>()
            / count as f64;
        Self {
            participating_nodes: count,
            mean_requests_per_node: mean,
            max_requests,
            max_to_mean: if mean > 0.0 {
                max_requests as f64 / mean
            } else {
                0.0
            },
            coefficient_of_variation: if mean > 0.0 {
                variance.sqrt() / mean
            } else {
                0.0
            },
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct NodeReport {
    pub id: String,
    pub active: bool,
    pub requests: u64,
    pub served_bytes: u64,
    pub by_source: BTreeMap<String, u64>,
    pub backend_bytes: u64,
    pub tile_cache_bytes: u64,
    pub chunk_cache_bytes: u64,
    pub metrics: NodeMetricsSnapshot,
    pub scheduler: SchedulerReport,
    #[serde(skip)]
    pub(crate) histograms: NodeHistogramSnapshot,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SchedulerReport {
    pub backend_fetch_duration_ms: DistributionSummary,
    pub backend_fetch_queue_duration_ms: DistributionSummary,
    pub backend_fetch_size_bytes: DistributionSummary,
    pub backend_fetch_chunks: DistributionSummary,
    pub queue_delay_immediate_ms: DistributionSummary,
    pub queue_delay_window_ms: DistributionSummary,
    pub pending_chunks_immediate: DistributionSummary,
    pub pending_chunks_window: DistributionSummary,
    pub group_waiters: DistributionSummary,
}

impl SchedulerReport {
    pub(crate) fn from_histograms(histograms: &NodeHistogramSnapshot) -> Self {
        Self {
            backend_fetch_duration_ms: DistributionSummary::from_continuous_histogram(
                &histograms.backend_fetch_duration_seconds,
                1_000.0,
            ),
            backend_fetch_queue_duration_ms: DistributionSummary::from_continuous_histogram(
                &histograms.backend_fetch_queue_duration_seconds,
                1_000.0,
            ),
            backend_fetch_size_bytes: DistributionSummary::from_continuous_histogram(
                &histograms.backend_fetch_size_bytes,
                1.0,
            ),
            backend_fetch_chunks: DistributionSummary::from_discrete_histogram(
                &histograms.backend_fetch_chunks,
                1.0,
            ),
            queue_delay_immediate_ms: DistributionSummary::from_continuous_histogram(
                &histograms.queue_delay_immediate_seconds,
                1_000.0,
            ),
            queue_delay_window_ms: DistributionSummary::from_continuous_histogram(
                &histograms.queue_delay_window_seconds,
                1_000.0,
            ),
            pending_chunks_immediate: DistributionSummary::from_discrete_histogram(
                &histograms.pending_chunks_immediate,
                1.0,
            ),
            pending_chunks_window: DistributionSummary::from_discrete_histogram(
                &histograms.pending_chunks_window,
                1.0,
            ),
            group_waiters: DistributionSummary::from_discrete_histogram(
                &histograms.group_waiters,
                1.0,
            ),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DistributionSummary {
    pub count: u64,
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub overflow_count: u64,
}

impl DistributionSummary {
    fn from_continuous_histogram(histogram: &HistogramSnapshot, scale: f64) -> Self {
        Self::from_histogram(histogram, scale, true)
    }

    fn from_discrete_histogram(histogram: &HistogramSnapshot, scale: f64) -> Self {
        Self::from_histogram(histogram, scale, false)
    }

    fn from_histogram(histogram: &HistogramSnapshot, scale: f64, interpolate: bool) -> Self {
        if histogram.count == 0 {
            return Self::default();
        }
        let covered = histogram
            .buckets
            .last()
            .map_or(0, |bucket| bucket.cumulative_count);
        Self {
            count: histogram.count,
            mean: histogram.sum / histogram.count as f64 * scale,
            p50: histogram_quantile(histogram, 0.50, interpolate) * scale,
            p95: histogram_quantile(histogram, 0.95, interpolate) * scale,
            p99: histogram_quantile(histogram, 0.99, interpolate) * scale,
            overflow_count: histogram.count.saturating_sub(covered),
        }
    }
}

fn histogram_quantile(histogram: &HistogramSnapshot, quantile: f64, interpolate: bool) -> f64 {
    if histogram.count == 0 || histogram.buckets.is_empty() {
        return 0.0;
    }
    let rank = (histogram.count as f64 * quantile).ceil() as u64;
    let mut previous_count = 0;
    let mut previous_bound = 0.0;
    for bucket in &histogram.buckets {
        if bucket.cumulative_count >= rank {
            if !interpolate {
                return bucket.upper_bound;
            }
            let bucket_count = bucket.cumulative_count.saturating_sub(previous_count);
            if bucket_count == 0 {
                return bucket.upper_bound;
            }
            let position = rank.saturating_sub(previous_count) as f64 / bucket_count as f64;
            return previous_bound + (bucket.upper_bound - previous_bound) * position;
        }
        previous_count = bucket.cumulative_count;
        previous_bound = bucket.upper_bound;
    }
    previous_bound
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ClusterObservation {
    pub requests: u64,
    pub active_nodes: usize,
    pub virtual_elapsed_ms: Option<u64>,
    pub gossip_messages: u64,
    pub gossip_bytes: u64,
    pub membership_converged_nodes: usize,
    pub membership_stale_nodes: usize,
    pub membership_missing_peer_refs: usize,
    pub membership_extra_peer_refs: usize,
    pub membership_min_peer_count: usize,
    pub membership_max_peer_count: usize,
    pub cache_hits: u64,
    pub by_source: BTreeMap<String, u64>,
    pub node_requests: BTreeMap<String, u64>,
    pub peer_requests: u64,
    pub peer_unavailable_requests: u64,
    pub peer_retryable_failures: u64,
    pub peer_backoff_skips: u64,
    pub backend_fetches: u64,
    pub backend_bytes: u64,
    pub served_bytes: u64,
    pub tile_cache_bytes: u64,
    pub chunk_cache_bytes: u64,
}

#[cfg(test)]
mod tests {
    use ishikari_core::metrics::{HistogramBucketSnapshot, HistogramSnapshot};

    use super::{DistributionSummary, NodeReport, SimReport};

    #[test]
    fn derives_rates_from_common_report_counters() {
        let mut report = SimReport {
            requests: 10,
            served_bytes: 1_000,
            backend_bytes: 1_500,
            l1_cache_hits: 4,
            by_source: [
                ("self_cache".to_string(), 5),
                ("peer_cache".to_string(), 2),
                ("peer_backend".to_string(), 1),
            ]
            .into_iter()
            .collect(),
            ..SimReport::default()
        };

        report.finalize_derived_metrics();

        assert_eq!(report.l1_cache_hit_rate, 0.4);
        assert_eq!(report.cache_hit_rate, 0.7);
        assert_eq!(report.peer_forward_rate, 0.3);
        assert_eq!(report.read_amplification, 1.5);
    }

    #[test]
    fn summarizes_histogram_buckets() {
        let histogram = HistogramSnapshot {
            count: 100,
            sum: 250.0,
            buckets: vec![
                HistogramBucketSnapshot {
                    upper_bound: 1.0,
                    cumulative_count: 50,
                },
                HistogramBucketSnapshot {
                    upper_bound: 4.0,
                    cumulative_count: 95,
                },
                HistogramBucketSnapshot {
                    upper_bound: 8.0,
                    cumulative_count: 99,
                },
            ],
        };
        let summary = DistributionSummary::from_continuous_histogram(&histogram, 1.0);
        assert_eq!(summary.count, 100);
        assert_eq!(summary.mean, 2.5);
        assert_eq!(summary.p50, 1.0);
        assert_eq!(summary.p95, 4.0);
        assert_eq!(summary.p99, 8.0);
        assert_eq!(summary.overflow_count, 1);
    }

    #[test]
    fn discrete_histogram_quantiles_use_bucket_bounds() {
        let histogram = HistogramSnapshot {
            count: 10,
            sum: 17.0,
            buckets: vec![
                HistogramBucketSnapshot {
                    upper_bound: 1.0,
                    cumulative_count: 7,
                },
                HistogramBucketSnapshot {
                    upper_bound: 2.0,
                    cumulative_count: 9,
                },
                HistogramBucketSnapshot {
                    upper_bound: 4.0,
                    cumulative_count: 10,
                },
            ],
        };

        let summary = DistributionSummary::from_discrete_histogram(&histogram, 1.0);
        assert_eq!(summary.p50, 1.0);
        assert_eq!(summary.p95, 4.0);
        assert_eq!(summary.p99, 4.0);
    }

    #[test]
    fn summarizes_request_load_skew() {
        let mut report = SimReport {
            nodes: [10, 20, 30]
                .into_iter()
                .enumerate()
                .map(|(index, requests)| NodeReport {
                    id: format!("node-{index}"),
                    requests,
                    ..NodeReport::default()
                })
                .collect(),
            ..SimReport::default()
        };

        report.set_node_request_load();

        assert_eq!(report.node_request_load.participating_nodes, 3);
        assert_eq!(report.node_request_load.mean_requests_per_node, 20.0);
        assert_eq!(report.node_request_load.max_requests, 30);
        assert_eq!(report.node_request_load.max_to_mean, 1.5);
        assert!(
            (report.node_request_load.coefficient_of_variation - 0.408_248_290_463_863).abs()
                < 1e-12
        );
    }
}
