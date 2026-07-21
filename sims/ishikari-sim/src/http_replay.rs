use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use ishikari_core::storage::TilesetId;
use mmpf_pmtiles::{TileCoord, TileId};
use reqwest::{Client, StatusCode, Url, header};
use serde::Serialize;
use tokio::task::JoinSet;

use crate::{TraceEntry, read_trace, viewport_batch_ranges};

const HTTP_REPLAY_SCHEMA_VERSION: u32 = 1;
const MAX_FAILURE_SAMPLES: usize = 20;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Debug)]
pub enum HttpReplayTarget {
    DirectNodes { node_urls: Vec<Url> },
    Gateway { gateway_url: Url },
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpExecutionMode {
    Serial,
    ViewportBatches,
}

#[derive(Clone, Debug)]
pub struct HttpReplayConfig {
    pub trace_path: PathBuf,
    pub target: HttpReplayTarget,
    pub mode: HttpExecutionMode,
    pub metrics_urls: Vec<Url>,
    pub request_timeout: Duration,
}

#[derive(Debug, Serialize)]
pub struct HttpReplayReport {
    schema_version: u32,
    kind: &'static str,
    runner_version: &'static str,
    trace: TraceFingerprint,
    execution: HttpExecutionReport,
    target: HttpTargetReport,
    result: HttpReplayResult,
    prometheus: PrometheusCapture,
}

impl HttpReplayReport {
    pub fn is_success(&self) -> bool {
        self.result.transport_errors == 0
            && self.result.unexpected_statuses == 0
            && !matches!(self.prometheus, PrometheusCapture::Failed { .. })
    }
}

#[derive(Debug, Serialize)]
struct TraceFingerprint {
    path: PathBuf,
    requests: usize,
    bytes: u64,
    fnv1a64: String,
}

#[derive(Debug, Serialize)]
struct HttpExecutionReport {
    mode: HttpExecutionMode,
    request_timeout_ms: u128,
    redirects: bool,
    retries: u8,
    cache_control_no_cache: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum HttpTargetReport {
    DirectNodes { node_urls: Vec<String> },
    Gateway { gateway_url: String },
}

#[derive(Debug, Serialize)]
struct HttpReplayResult {
    attempted: usize,
    responses: usize,
    transport_errors: usize,
    unexpected_statuses: usize,
    status_counts: BTreeMap<u16, u64>,
    response_body_bytes: u64,
    elapsed_ms: f64,
    throughput_rps: f64,
    latency_ms: HttpLatencySummary,
    failure_samples: Vec<HttpFailureSample>,
}

#[derive(Debug, Default, Serialize)]
struct HttpLatencySummary {
    count: usize,
    mean: f64,
    p50: f64,
    p90: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

#[derive(Debug, Serialize)]
struct HttpFailureSample {
    trace_index: usize,
    step: u64,
    user: usize,
    ordinal: usize,
    url: String,
    category: &'static str,
    detail: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PrometheusCapture {
    Disabled,
    Complete {
        nodes: Vec<PrometheusNodeReport>,
        aggregate: Box<ComparableMetrics>,
    },
    Failed {
        error: String,
    },
}

#[derive(Debug, Serialize)]
struct PrometheusNodeReport {
    target_index: usize,
    metrics_url: String,
    result: ComparableMetrics,
}

#[derive(Clone, Debug, Default, Serialize)]
struct ComparableMetrics {
    requests: u64,
    found: u64,
    not_found: u64,
    served_bytes: u64,
    by_source: BTreeMap<String, u64>,
    peer_requests: u64,
    peer_bytes: u64,
    backend_bytes: u64,
    l1_cache_hits: u64,
    l1_cache_hit_rate: f64,
    cache_hit_rate: f64,
    peer_forward_rate: f64,
    read_amplification: f64,
    backend_fetches: u64,
    backend_fetch_outcomes: BTreeMap<String, u64>,
    backend_fetched_chunks: u64,
    chunk_cache: BTreeMap<String, u64>,
    chunk_fetch_wait: BTreeMap<String, u64>,
}

/// Reads a per-label counter delta for each value into a map keyed by that value.
fn counter_delta_map(
    before: &MetricSnapshot,
    after: &MetricSnapshot,
    metric: &str,
    label: &str,
    values: &[&str],
) -> Result<BTreeMap<String, u64>> {
    values
        .iter()
        .copied()
        .map(|value| {
            Ok((
                value.to_string(),
                counter_delta(before, after, metric, &[(label, value)])?,
            ))
        })
        .collect()
}

impl ComparableMetrics {
    fn from_delta(before: &MetricSnapshot, after: &MetricSnapshot) -> Result<Self> {
        let mut result = Self {
            by_source: counter_delta_map(
                before,
                after,
                "ishikari_tiles_served_total",
                "source",
                &[
                    "self_cache",
                    "self_backend",
                    "peer_cache",
                    "peer_backend",
                    "miss",
                ],
            )?,
            ..Default::default()
        };
        result.requests = result.by_source.values().sum();
        result.not_found = result.by_source.get("miss").copied().unwrap_or_default();
        result.found = result.requests.saturating_sub(result.not_found);
        result.served_bytes =
            counter_delta(before, after, "ishikari_external_egress_bytes_total", &[])?;
        result.peer_bytes =
            counter_delta(before, after, "ishikari_internal_egress_bytes_total", &[])?;
        result.backend_bytes =
            counter_delta(before, after, "ishikari_backend_fetch_bytes_total", &[])?;
        result.l1_cache_hits = counter_delta(
            before,
            after,
            "ishikari_tile_cache_total",
            &[("outcome", "hit")],
        )?;
        result.peer_requests =
            sum_counter_family_delta(before, after, "ishikari_peer_fetch_total")?;

        for outcome in ["success", "not_found", "error", "timeout"] {
            let count = counter_delta(
                before,
                after,
                "ishikari_backend_fetch_duration_seconds_count",
                &[("outcome", outcome)],
            )?;
            result.backend_fetches += count;
            result
                .backend_fetch_outcomes
                .insert(outcome.to_string(), count);
        }
        result.backend_fetched_chunks = counter_delta(
            before,
            after,
            "ishikari_backend_fetch_chunks_sum",
            &[("outcome", "success")],
        )?;
        result.chunk_cache = counter_delta_map(
            before,
            after,
            "ishikari_chunk_cache_total",
            "outcome",
            &["hit", "miss", "post_fetch_hit"],
        )?;
        result.chunk_fetch_wait = counter_delta_map(
            before,
            after,
            "ishikari_chunk_fetch_wait_total",
            "outcome",
            &["queued", "joined_pending", "joined_inflight"],
        )?;
        result.finalize_rates();
        Ok(result)
    }

    fn add_assign(&mut self, other: &Self) {
        self.requests += other.requests;
        self.found += other.found;
        self.not_found += other.not_found;
        self.served_bytes += other.served_bytes;
        self.peer_requests += other.peer_requests;
        self.peer_bytes += other.peer_bytes;
        self.backend_bytes += other.backend_bytes;
        self.l1_cache_hits += other.l1_cache_hits;
        self.backend_fetches += other.backend_fetches;
        self.backend_fetched_chunks += other.backend_fetched_chunks;
        add_map(&mut self.by_source, &other.by_source);
        add_map(
            &mut self.backend_fetch_outcomes,
            &other.backend_fetch_outcomes,
        );
        add_map(&mut self.chunk_cache, &other.chunk_cache);
        add_map(&mut self.chunk_fetch_wait, &other.chunk_fetch_wait);
    }

    fn finalize_rates(&mut self) {
        if self.requests > 0 {
            let self_cache = self
                .by_source
                .get("self_cache")
                .copied()
                .unwrap_or_default();
            let peer_cache = self
                .by_source
                .get("peer_cache")
                .copied()
                .unwrap_or_default();
            let peer_backend = self
                .by_source
                .get("peer_backend")
                .copied()
                .unwrap_or_default();
            self.l1_cache_hit_rate = self.l1_cache_hits as f64 / self.requests as f64;
            self.cache_hit_rate = (self_cache + peer_cache) as f64 / self.requests as f64;
            self.peer_forward_rate = (peer_cache + peer_backend) as f64 / self.requests as f64;
        }
        if self.served_bytes > 0 {
            self.read_amplification = self.backend_bytes as f64 / self.served_bytes as f64;
        }
    }
}

fn add_map(target: &mut BTreeMap<String, u64>, other: &BTreeMap<String, u64>) {
    for (key, value) in other {
        *target.entry(key.clone()).or_default() += value;
    }
}

#[derive(Clone)]
struct PlannedHttpRequest {
    trace_index: usize,
    entry: TraceEntry,
    url: Url,
}

struct HttpRequestOutcome {
    plan: PlannedHttpRequest,
    latency: Duration,
    status: Option<StatusCode>,
    body_bytes: u64,
    error_category: Option<&'static str>,
    error_detail: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SeriesKey {
    name: String,
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct MetricSnapshot {
    samples: BTreeMap<SeriesKey, f64>,
}

/// Replays one existing trace against public Ishikari HTTP endpoints and
/// optionally captures per-node Prometheus counter deltas for calibration.
pub async fn run_http_replay(config: HttpReplayConfig) -> Result<HttpReplayReport> {
    validate_config(&config)?;
    let trace_file = File::open(&config.trace_path)
        .with_context(|| format!("open HTTP replay trace {}", config.trace_path.display()))?;
    let entries = read_trace(BufReader::new(trace_file))?;
    ensure!(!entries.is_empty(), "HTTP replay trace must not be empty");
    let plans = plan_requests(&entries, &config.target)?;
    let trace = fingerprint_trace(&config.trace_path, entries.len())?;

    let client = Client::builder()
        .timeout(config.request_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build HTTP replay client")?;
    let before_metrics = if config.metrics_urls.is_empty() {
        None
    } else {
        Some(scrape_metrics(&client, &config.metrics_urls).await?)
    };

    let started_at = Instant::now();
    let outcomes = execute_plans(&client, &plans, &entries, config.mode).await?;
    let elapsed = started_at.elapsed();
    let result = summarize_http_outcomes(outcomes, elapsed);

    let prometheus = match before_metrics {
        None => PrometheusCapture::Disabled,
        Some(before) => match scrape_metrics(&client, &config.metrics_urls).await {
            Ok(after) => match build_prometheus_report(&config.metrics_urls, &before, &after) {
                Ok((nodes, aggregate)) => PrometheusCapture::Complete {
                    nodes,
                    aggregate: Box::new(aggregate),
                },
                Err(error) => PrometheusCapture::Failed {
                    error: format!("derive Prometheus deltas: {error:#}"),
                },
            },
            Err(error) => PrometheusCapture::Failed {
                error: format!("post-replay Prometheus scrape: {error:#}"),
            },
        },
    };

    Ok(HttpReplayReport {
        schema_version: HTTP_REPLAY_SCHEMA_VERSION,
        kind: "ishikari_http_replay",
        runner_version: env!("CARGO_PKG_VERSION"),
        trace,
        execution: HttpExecutionReport {
            mode: config.mode,
            request_timeout_ms: config.request_timeout.as_millis(),
            redirects: false,
            retries: 0,
            cache_control_no_cache: true,
        },
        target: target_report(&config.target),
        result,
        prometheus,
    })
}

fn validate_config(config: &HttpReplayConfig) -> Result<()> {
    ensure!(
        !config.request_timeout.is_zero(),
        "HTTP replay request timeout must be positive"
    );
    let public_urls = match &config.target {
        HttpReplayTarget::DirectNodes { node_urls } => {
            ensure!(!node_urls.is_empty(), "direct replay requires node URLs");
            if !config.metrics_urls.is_empty() {
                ensure!(
                    config.metrics_urls.len() == node_urls.len(),
                    "direct replay requires one metrics URL per node URL"
                );
            }
            node_urls
        }
        HttpReplayTarget::Gateway { gateway_url } => std::slice::from_ref(gateway_url),
    };
    validate_unique_urls("public target", public_urls)?;
    validate_unique_urls("metrics", &config.metrics_urls)?;
    for url in public_urls {
        validate_public_base_url(url)?;
    }
    for url in &config.metrics_urls {
        validate_http_url(url, false)?;
    }
    Ok(())
}

fn validate_unique_urls(kind: &str, urls: &[Url]) -> Result<()> {
    let mut seen = HashSet::with_capacity(urls.len());
    ensure!(
        urls.iter().all(|url| seen.insert(url.as_str())),
        "duplicate {kind} URL"
    );
    Ok(())
}

fn validate_public_base_url(url: &Url) -> Result<()> {
    validate_http_url(url, true)
}

fn validate_http_url(url: &Url, require_root_path: bool) -> Result<()> {
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "URL scheme must be http or https: {url}"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "URL must not contain credentials"
    );
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "URL must not contain a query or fragment: {url}"
    );
    if require_root_path {
        ensure!(
            url.path() == "/",
            "public target URL must have a root path: {url}"
        );
    }
    Ok(())
}

fn plan_requests(
    entries: &[TraceEntry],
    target: &HttpReplayTarget,
) -> Result<Vec<PlannedHttpRequest>> {
    entries
        .iter()
        .enumerate()
        .map(|(trace_index, entry)| {
            let tileset = TilesetId::try_new(&entry.tileset)
                .with_context(|| format!("trace request {trace_index} has invalid tileset"))?;
            let coordinate = TileCoord::new(entry.z, entry.x, entry.y)
                .with_context(|| format!("trace request {trace_index} has invalid coordinate"))?;
            let _ = TileId::from(coordinate);
            let base = match target {
                HttpReplayTarget::Gateway { gateway_url } => gateway_url,
                HttpReplayTarget::DirectNodes { node_urls } => {
                    let node = entry.entry_node.with_context(|| {
                        format!("trace request {trace_index} has no direct entry_node")
                    })?;
                    node_urls.get(node).with_context(|| {
                        format!(
                            "trace request {trace_index} entry_node {node} exceeds {} direct targets",
                            node_urls.len()
                        )
                    })?
                }
            };
            let path = format!(
                "tilesets/{tileset}/{}/{}/{}",
                entry.z, entry.x, entry.y
            );
            let url = base
                .join(&path)
                .with_context(|| format!("build tile URL for trace request {trace_index}"))?;
            Ok(PlannedHttpRequest {
                trace_index,
                entry: entry.clone(),
                url,
            })
        })
        .collect()
}

async fn execute_plans(
    client: &Client,
    plans: &[PlannedHttpRequest],
    entries: &[TraceEntry],
    mode: HttpExecutionMode,
) -> Result<Vec<HttpRequestOutcome>> {
    match mode {
        HttpExecutionMode::Serial => {
            let mut outcomes = Vec::with_capacity(plans.len());
            for plan in plans {
                outcomes.push(execute_request(client.clone(), plan.clone()).await);
            }
            Ok(outcomes)
        }
        HttpExecutionMode::ViewportBatches => {
            let mut outcomes = Vec::with_capacity(plans.len());
            for range in viewport_batch_ranges(entries)? {
                let mut tasks = JoinSet::new();
                for plan in &plans[range] {
                    tasks.spawn(execute_request(client.clone(), plan.clone()));
                }
                while let Some(outcome) = tasks.join_next().await {
                    outcomes.push(outcome.context("HTTP replay request task failed")?);
                }
            }
            Ok(outcomes)
        }
    }
}

async fn execute_request(client: Client, plan: PlannedHttpRequest) -> HttpRequestOutcome {
    let started_at = Instant::now();
    let response = client
        .get(plan.url.clone())
        .header(header::CACHE_CONTROL, "no-cache")
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            return HttpRequestOutcome {
                plan,
                latency: started_at.elapsed(),
                status: None,
                body_bytes: 0,
                error_category: Some(request_error_category(&error)),
                error_detail: Some(error.to_string()),
            };
        }
    };
    let status = response.status();
    match response.bytes().await {
        Ok(body) => HttpRequestOutcome {
            plan,
            latency: started_at.elapsed(),
            status: Some(status),
            body_bytes: body.len() as u64,
            error_category: None,
            error_detail: None,
        },
        Err(error) => HttpRequestOutcome {
            plan,
            latency: started_at.elapsed(),
            status: Some(status),
            body_bytes: 0,
            error_category: Some("body"),
            error_detail: Some(error.to_string()),
        },
    }
}

fn request_error_category(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_body() {
        "body"
    } else {
        "request"
    }
}

fn summarize_http_outcomes(
    mut outcomes: Vec<HttpRequestOutcome>,
    elapsed: Duration,
) -> HttpReplayResult {
    let mut result = HttpReplayResult {
        attempted: outcomes.len(),
        responses: 0,
        transport_errors: 0,
        unexpected_statuses: 0,
        status_counts: BTreeMap::new(),
        response_body_bytes: 0,
        elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
        throughput_rps: if elapsed.is_zero() {
            0.0
        } else {
            outcomes.len() as f64 / elapsed.as_secs_f64()
        },
        latency_ms: HttpLatencySummary::default(),
        failure_samples: Vec::new(),
    };
    // JoinSet yields viewport requests in completion order. Aggregate results do
    // not depend on that order, but the bounded diagnostic sample must be
    // reproducible for the same trace.
    outcomes.sort_unstable_by_key(|outcome| outcome.plan.trace_index);
    let mut latencies = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        latencies.push(outcome.latency);
        if let Some(status) = outcome.status {
            result.responses += 1;
            *result.status_counts.entry(status.as_u16()).or_default() += 1;
            result.response_body_bytes += outcome.body_bytes;
            if !matches!(status, StatusCode::OK | StatusCode::NOT_FOUND) {
                result.unexpected_statuses += 1;
                push_failure(
                    &mut result.failure_samples,
                    &outcome,
                    "status",
                    status.to_string(),
                );
            }
        }
        if let Some(category) = outcome.error_category {
            result.transport_errors += 1;
            push_failure(
                &mut result.failure_samples,
                &outcome,
                category,
                outcome
                    .error_detail
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string()),
            );
        }
    }
    result.latency_ms = summarize_latencies(latencies);
    result
}

fn push_failure(
    failures: &mut Vec<HttpFailureSample>,
    outcome: &HttpRequestOutcome,
    category: &'static str,
    detail: String,
) {
    if failures.len() >= MAX_FAILURE_SAMPLES {
        return;
    }
    failures.push(HttpFailureSample {
        trace_index: outcome.plan.trace_index,
        step: outcome.plan.entry.step,
        user: outcome.plan.entry.user,
        ordinal: outcome.plan.entry.ordinal,
        url: outcome.plan.url.to_string(),
        category,
        detail,
    });
}

fn summarize_latencies(mut values: Vec<Duration>) -> HttpLatencySummary {
    if values.is_empty() {
        return HttpLatencySummary::default();
    }
    values.sort_unstable();
    let sum = values.iter().map(Duration::as_secs_f64).sum::<f64>() * 1_000.0;
    HttpLatencySummary {
        count: values.len(),
        mean: sum / values.len() as f64,
        p50: percentile_ms(&values, 0.50),
        p90: percentile_ms(&values, 0.90),
        p95: percentile_ms(&values, 0.95),
        p99: percentile_ms(&values, 0.99),
        max: values
            .last()
            .map_or(0.0, |value| value.as_secs_f64() * 1_000.0),
    }
}

fn percentile_ms(values: &[Duration], quantile: f64) -> f64 {
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values[index].as_secs_f64() * 1_000.0
}

async fn scrape_metrics(client: &Client, urls: &[Url]) -> Result<Vec<MetricSnapshot>> {
    let mut tasks = JoinSet::new();
    for (index, url) in urls.iter().cloned().enumerate() {
        let client = client.clone();
        tasks.spawn(async move {
            let response = client
                .get(url.clone())
                .send()
                .await
                .with_context(|| format!("scrape metrics {url}"))?;
            ensure!(
                response.status() == StatusCode::OK,
                "metrics endpoint {url} returned {}",
                response.status()
            );
            let body = response
                .text()
                .await
                .with_context(|| format!("read metrics body {url}"))?;
            Ok::<_, anyhow::Error>((index, parse_metrics(&body)?))
        });
    }
    let mut snapshots = vec![None; urls.len()];
    while let Some(result) = tasks.join_next().await {
        let (index, snapshot) = result.context("metrics scrape task failed")??;
        snapshots[index] = Some(snapshot);
    }
    snapshots
        .into_iter()
        .enumerate()
        .map(|(index, snapshot)| {
            snapshot.with_context(|| format!("metrics scrape {index} produced no result"))
        })
        .collect()
}

fn build_prometheus_report(
    urls: &[Url],
    before: &[MetricSnapshot],
    after: &[MetricSnapshot],
) -> Result<(Vec<PrometheusNodeReport>, ComparableMetrics)> {
    ensure!(
        before.len() == after.len() && before.len() == urls.len(),
        "Prometheus scrape cardinality changed"
    );
    let mut nodes = Vec::with_capacity(urls.len());
    let mut aggregate = ComparableMetrics::default();
    for (index, ((before, after), url)) in before.iter().zip(after).zip(urls).enumerate() {
        let result = ComparableMetrics::from_delta(before, after)
            .with_context(|| format!("metrics target {index} ({url})"))?;
        aggregate.add_assign(&result);
        nodes.push(PrometheusNodeReport {
            target_index: index,
            metrics_url: url.to_string(),
            result,
        });
    }
    aggregate.finalize_rates();
    Ok((nodes, aggregate))
}

fn parse_metrics(input: &str) -> Result<MetricSnapshot> {
    let mut snapshot = MetricSnapshot::default();
    for (line_index, line) in input.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (series, value) = split_sample(line)
            .with_context(|| format!("parse Prometheus line {}", line_index + 1))?;
        if !series.name.starts_with("ishikari_") {
            continue;
        }
        ensure!(
            snapshot.samples.insert(series, value).is_none(),
            "duplicate Prometheus series on line {}",
            line_index + 1
        );
    }
    Ok(snapshot)
}

fn split_sample(line: &str) -> Result<(SeriesKey, f64)> {
    let mut quoted = false;
    let mut escaped = false;
    let split = line
        .char_indices()
        .find_map(|(index, character)| {
            if escaped {
                escaped = false;
                return None;
            }
            if quoted && character == '\\' {
                escaped = true;
                return None;
            }
            if character == '"' {
                quoted = !quoted;
                return None;
            }
            (!quoted && character.is_ascii_whitespace()).then_some(index)
        })
        .context("Prometheus sample has no value")?;
    let series = &line[..split];
    let value = line[split..]
        .split_ascii_whitespace()
        .next()
        .context("Prometheus sample has no value")?
        .parse::<f64>()
        .context("Prometheus sample value is invalid")?;
    let (name, labels) = match series.find('{') {
        Some(open) => {
            ensure!(series.ends_with('}'), "Prometheus labels are not closed");
            (
                &series[..open],
                parse_labels(&series[open + 1..series.len() - 1])?,
            )
        }
        None => (series, BTreeMap::new()),
    };
    ensure!(!name.is_empty(), "Prometheus metric name is empty");
    Ok((
        SeriesKey {
            name: name.to_string(),
            labels,
        },
        value,
    ))
}

fn parse_labels(input: &str) -> Result<BTreeMap<String, String>> {
    let bytes = input.as_bytes();
    let mut labels = BTreeMap::new();
    let mut index = 0;
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let key_start = index;
        while index < bytes.len() && bytes[index] != b'=' {
            index += 1;
        }
        ensure!(
            index > key_start && index < bytes.len(),
            "invalid Prometheus label key"
        );
        let key = input[key_start..index].trim();
        index += 1;
        ensure!(
            index < bytes.len() && bytes[index] == b'"',
            "label {key} is not quoted"
        );
        index += 1;
        let mut value = String::new();
        let mut closed = false;
        while index < bytes.len() {
            match bytes[index] {
                b'"' => {
                    index += 1;
                    closed = true;
                    break;
                }
                b'\\' => {
                    index += 1;
                    ensure!(index < bytes.len(), "unterminated label escape");
                    value.push(match bytes[index] {
                        b'n' => '\n',
                        b'\\' => '\\',
                        b'"' => '"',
                        other => other as char,
                    });
                    index += 1;
                }
                byte => {
                    value.push(byte as char);
                    index += 1;
                }
            }
        }
        ensure!(closed, "label {key} is not closed");
        ensure!(
            labels.insert(key.to_string(), value).is_none(),
            "duplicate Prometheus label {key}"
        );
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index < bytes.len() {
            ensure!(
                bytes[index] == b',',
                "expected comma between Prometheus labels"
            );
            index += 1;
        }
    }
    Ok(labels)
}

fn counter_delta(
    before: &MetricSnapshot,
    after: &MetricSnapshot,
    name: &str,
    labels: &[(&str, &str)],
) -> Result<u64> {
    let labels = labels
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();
    let key = SeriesKey {
        name: name.to_string(),
        labels,
    };
    counter_delta_for_key(before, after, &key)
}

fn sum_counter_family_delta(
    before: &MetricSnapshot,
    after: &MetricSnapshot,
    name: &str,
) -> Result<u64> {
    let keys = before
        .samples
        .keys()
        .chain(after.samples.keys())
        .filter(|key| key.name == name)
        .cloned()
        .collect::<BTreeSet<_>>();
    keys.iter().try_fold(0_u64, |total, key| {
        Ok(total + counter_delta_for_key(before, after, key)?)
    })
}

fn counter_delta_for_key(
    before: &MetricSnapshot,
    after: &MetricSnapshot,
    key: &SeriesKey,
) -> Result<u64> {
    let before_value = before.samples.get(key).copied().unwrap_or(0.0);
    let Some(after_value) = after.samples.get(key).copied() else {
        if before.samples.contains_key(key) {
            bail!("counter series disappeared: {}{:?}", key.name, key.labels);
        }
        return Ok(0);
    };
    ensure!(
        before_value.is_finite() && after_value.is_finite(),
        "counter is not finite: {}{:?}",
        key.name,
        key.labels
    );
    ensure!(
        after_value >= before_value,
        "counter reset: {}{:?} before={before_value} after={after_value}",
        key.name,
        key.labels
    );
    let delta = after_value - before_value;
    let rounded = delta.round();
    ensure!(
        (delta - rounded).abs() < 1e-6 && rounded <= u64::MAX as f64,
        "counter delta is not an integer: {}{:?} delta={delta}",
        key.name,
        key.labels
    );
    Ok(rounded as u64)
}

fn target_report(target: &HttpReplayTarget) -> HttpTargetReport {
    match target {
        HttpReplayTarget::DirectNodes { node_urls } => HttpTargetReport::DirectNodes {
            node_urls: node_urls.iter().map(ToString::to_string).collect(),
        },
        HttpReplayTarget::Gateway { gateway_url } => HttpTargetReport::Gateway {
            gateway_url: gateway_url.to_string(),
        },
    }
}

fn fingerprint_trace(path: &Path, requests: usize) -> Result<TraceFingerprint> {
    let mut file =
        File::open(path).with_context(|| format!("open {} for hashing", path.display()))?;
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut hash = FNV_OFFSET_BASIS;
    let mut bytes = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            break;
        }
        bytes = bytes.saturating_add(read as u64);
        for byte in &buffer[..read] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    Ok(TraceFingerprint {
        path: path.to_path_buf(),
        requests,
        bytes,
        fnv1a64: format!("fnv1a64:{hash:016x}"),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::{Router, routing::get};

    use super::*;

    fn entry(index: usize, entry_node: Option<usize>) -> TraceEntry {
        TraceEntry {
            step: 0,
            user: 0,
            ordinal: index,
            tileset: "japan".to_string(),
            z: 0,
            x: 0,
            y: 0,
            entry_node,
        }
    }

    fn failed_outcome(trace_index: usize) -> HttpRequestOutcome {
        HttpRequestOutcome {
            plan: PlannedHttpRequest {
                trace_index,
                entry: entry(trace_index, None),
                url: Url::parse(&format!(
                    "https://gateway.example/tilesets/japan/0/{trace_index}/0"
                ))
                .unwrap(),
            },
            latency: Duration::from_millis(trace_index as u64 + 1),
            status: Some(StatusCode::INTERNAL_SERVER_ERROR),
            body_bytes: 0,
            error_category: None,
            error_detail: None,
        }
    }

    #[test]
    fn failure_samples_are_deterministic_and_bounded_by_trace_order() {
        let forward = summarize_http_outcomes(
            (0..25).map(failed_outcome).collect(),
            Duration::from_secs(1),
        );
        let reverse = summarize_http_outcomes(
            (0..25).rev().map(failed_outcome).collect(),
            Duration::from_secs(1),
        );

        let sampled_indices = |result: &HttpReplayResult| {
            result
                .failure_samples
                .iter()
                .map(|sample| sample.trace_index)
                .collect::<Vec<_>>()
        };
        assert_eq!(sampled_indices(&forward), (0..20).collect::<Vec<_>>());
        assert_eq!(sampled_indices(&reverse), sampled_indices(&forward));
        assert_eq!(forward.unexpected_statuses, 25);
        assert_eq!(reverse.unexpected_statuses, forward.unexpected_statuses);
        assert_eq!(reverse.status_counts, forward.status_counts);
        assert_eq!(reverse.latency_ms.count, forward.latency_ms.count);
        assert_eq!(reverse.latency_ms.mean, forward.latency_ms.mean);
        assert_eq!(reverse.latency_ms.p99, forward.latency_ms.p99);
    }

    #[test]
    fn direct_replay_requires_valid_entry_node_and_gateway_ignores_it() {
        let direct = HttpReplayTarget::DirectNodes {
            node_urls: vec![Url::parse("http://node-0.example/").unwrap()],
        };
        assert!(plan_requests(&[entry(0, None)], &direct).is_err());
        assert!(plan_requests(&[entry(0, Some(1))], &direct).is_err());

        let gateway = HttpReplayTarget::Gateway {
            gateway_url: Url::parse("https://gateway.example/").unwrap(),
        };
        let plans = plan_requests(&[entry(0, None)], &gateway).expect("gateway plan");
        assert_eq!(
            plans[0].url.as_str(),
            "https://gateway.example/tilesets/japan/0/0/0"
        );
    }

    #[test]
    fn prometheus_delta_maps_comparable_fields_and_rejects_resets() {
        let before = parse_metrics(
            r#"
ishikari_tiles_served_total{source="self_cache"} 2
ishikari_external_egress_bytes_total 100
ishikari_backend_fetch_bytes_total 50
ishikari_backend_fetch_duration_seconds_count{outcome="success"} 1
ishikari_backend_fetch_chunks_sum{outcome="success"} 2
ishikari_peer_fetch_total{resource="tile",outcome="success"} 3
"#,
        )
        .unwrap();
        let after = parse_metrics(
            r#"
ishikari_tiles_served_total{source="self_cache"} 5
ishikari_tiles_served_total{source="peer_cache"} 1
ishikari_external_egress_bytes_total 220
ishikari_backend_fetch_bytes_total 90
ishikari_backend_fetch_duration_seconds_count{outcome="success"} 3
ishikari_backend_fetch_chunks_sum{outcome="success"} 7
ishikari_peer_fetch_total{outcome="success",resource="tile"} 5
"#,
        )
        .unwrap();
        let metrics = ComparableMetrics::from_delta(&before, &after).unwrap();
        assert_eq!(metrics.requests, 4);
        assert_eq!(metrics.served_bytes, 120);
        assert_eq!(metrics.backend_bytes, 40);
        assert_eq!(metrics.backend_fetches, 2);
        assert_eq!(metrics.backend_fetched_chunks, 5);
        assert_eq!(metrics.peer_requests, 2);
        assert_eq!(metrics.cache_hit_rate, 1.0);

        let reset = parse_metrics("ishikari_external_egress_bytes_total 99\n").unwrap();
        let error = counter_delta(&before, &reset, "ishikari_external_egress_bytes_total", &[])
            .expect_err("counter reset must fail");
        assert!(error.to_string().contains("counter reset"));
    }

    #[tokio::test]
    async fn gateway_replay_executes_a_trace_and_reports_http_results() {
        let requests = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .route(
                "/tilesets/japan/0/0/0",
                get({
                    let requests = Arc::clone(&requests);
                    move || {
                        let requests = Arc::clone(&requests);
                        async move {
                            requests.fetch_add(1, Ordering::Relaxed);
                            "tile"
                        }
                    }
                }),
            )
            .route(
                "/metrics",
                get({
                    let requests = Arc::clone(&requests);
                    move || {
                        let requests = Arc::clone(&requests);
                        async move {
                            let count = requests.load(Ordering::Relaxed);
                            format!(
                                "ishikari_tiles_served_total{{source=\"self_cache\"}} {count}\nishikari_external_egress_bytes_total {}\n",
                                count * 4
                            )
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let trace_path = std::env::temp_dir().join(format!(
            "ishikari-http-replay-{}-{suffix}.jsonl",
            std::process::id()
        ));
        let mut second_entry = entry(0, None);
        second_entry.step = 1;
        let trace = [entry(0, Some(99)), second_entry]
            .into_iter()
            .map(|entry| serde_json::to_string(&entry).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&trace_path, format!("{trace}\n")).unwrap();

        let report = run_http_replay(HttpReplayConfig {
            trace_path: trace_path.clone(),
            target: HttpReplayTarget::Gateway {
                gateway_url: Url::parse(&format!("http://{address}/")).unwrap(),
            },
            mode: HttpExecutionMode::ViewportBatches,
            metrics_urls: vec![Url::parse(&format!("http://{address}/metrics")).unwrap()],
            request_timeout: Duration::from_secs(5),
        })
        .await
        .expect("HTTP replay");

        assert!(report.is_success());
        assert_eq!(report.result.responses, 2);
        assert_eq!(report.result.status_counts.get(&200), Some(&2));
        assert_eq!(requests.load(Ordering::Relaxed), 2);
        let PrometheusCapture::Complete { nodes, aggregate } = report.prometheus else {
            panic!("expected complete Prometheus capture");
        };
        assert_eq!(nodes.len(), 1);
        assert_eq!(aggregate.requests, 2);
        assert_eq!(aggregate.served_bytes, 8);
        assert_eq!(aggregate.cache_hit_rate, 1.0);
        let _ = std::fs::remove_file(trace_path);
    }
}
