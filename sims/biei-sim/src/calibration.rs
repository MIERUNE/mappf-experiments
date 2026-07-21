//! Immutable production calibration profiles exported from Prometheus.
//!
//! M12a deliberately ends at a provenance-bearing JSON snapshot. The
//! provisional M12b importer reads that file; simulation runs never query a
//! live Prometheus endpoint.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};

pub const CALIBRATION_PROFILE_SCHEMA_VERSION: u32 = 1;
pub const CALIBRATION_PROFILE_KIND: &str = "biei-production-calibration-profile";
const MAX_PROMETHEUS_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct CalibrationExportOptions {
    pub prometheus_url: String,
    pub start_unix_seconds: u64,
    pub end_unix_seconds: u64,
    pub match_labels: BTreeMap<String, String>,
    pub bearer_token: Option<String>,
    pub timeout: Duration,
    pub provenance: CalibrationProvenance,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CalibrationProvenance {
    pub deployment_revision: String,
    pub architecture: String,
    pub hardware_profile: String,
    pub cpu_cores_per_node: usize,
    pub renderer_slots_per_node: usize,
    pub execution_permits_per_node: usize,
    pub native_render_permits_per_node: usize,
    /// Maximum public-request concurrency used to capture this window. CPU
    /// reference profiles require `Some(1)` so service-wall measurements do
    /// not already contain scheduler contention that the simulator reapplies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_concurrency: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CalibrationProfile {
    pub schema_version: u32,
    pub kind: String,
    pub exporter: String,
    pub exporter_version: String,
    pub exported_at_unix_seconds: u64,
    pub collection: CalibrationCollection,
    pub provenance: CalibrationProvenance,
    pub histograms: Vec<CalibrationHistogram>,
    pub warnings: Vec<String>,
}

impl CalibrationProfile {
    /// Calibration snapshots are immutable evidence: refuse to overwrite an
    /// existing path even when the contents happen to be identical.
    pub fn write_new_json(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("create new calibration profile {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, self).context("serialize calibration profile")?;
        writer
            .write_all(b"\n")
            .context("finish calibration profile")?;
        writer.flush().context("flush calibration profile")
    }

    pub fn series_count(&self) -> usize {
        self.histograms
            .iter()
            .map(|histogram| histogram.series.len())
            .sum()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CalibrationCollection {
    pub prometheus_url: String,
    pub start_unix_seconds: u64,
    pub end_unix_seconds: u64,
    pub window_seconds: u64,
    pub evaluation_unix_seconds: u64,
    pub match_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CalibrationHistogram {
    pub metric: String,
    pub unit: String,
    pub query: String,
    pub series: Vec<CalibrationSeries>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CalibrationSeries {
    pub labels: BTreeMap<String, String>,
    /// The `+Inf` cumulative bucket over the selected collection window.
    pub sample_count: f64,
    /// Non-cumulative counts, ordered by upper bound with `+Inf` last.
    pub buckets: Vec<CalibrationBucket>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CalibrationBucket {
    /// `None` represents Prometheus' `le="+Inf"` bucket.
    pub upper_bound_seconds: Option<f64>,
    pub count: f64,
}

#[derive(Clone, Copy, Debug)]
struct MetricSpec {
    metric: &'static str,
    group_labels: &'static [&'static str],
    ingress_only: bool,
}

const METRIC_SPECS: &[MetricSpec] = &[
    MetricSpec {
        metric: "biei_render_duration_seconds",
        group_labels: &["render_mode", "scale", "format", "size", "state"],
        ingress_only: true,
    },
    // A positive sample count means the successful render histogram is
    // right-censored by timeouts and must not be treated as a complete service
    // distribution when calibrating production defaults. A present zero series
    // proves the current instrumentation was exposed during the window.
    MetricSpec {
        metric: "biei_render_timeout_lower_bound_seconds",
        group_labels: &[],
        ingress_only: true,
    },
    MetricSpec {
        metric: "biei_style_setup_duration_seconds",
        group_labels: &["render_mode", "scale", "state"],
        ingress_only: true,
    },
    MetricSpec {
        metric: "biei_source_setup_duration_seconds",
        group_labels: &["render_mode", "scale"],
        ingress_only: true,
    },
    MetricSpec {
        metric: "biei_profile_prepare_duration_seconds",
        group_labels: &["outcome"],
        ingress_only: false,
    },
    // In-render fetch activity over the same window. The importer uses this
    // to verify a capture was resource-cache-warm: regular-lane upstream
    // attempts during the window mean warm render walls still contained
    // provider I/O, so they must not be read as CPU service demand.
    MetricSpec {
        metric: "mmpf_mln_resource_upstream_attempt_duration_seconds",
        group_labels: &["kind", "priority"],
        ingress_only: false,
    },
];

pub async fn export_calibration_profile(
    options: CalibrationExportOptions,
) -> Result<CalibrationProfile> {
    validate_export_options(&options)?;
    let window_seconds = options.end_unix_seconds - options.start_unix_seconds;
    let endpoint = prometheus_query_endpoint(&options.prometheus_url)?;
    let client = PrometheusClient::new(endpoint.clone(), options.bearer_token, options.timeout)?;
    let mut histograms = Vec::with_capacity(METRIC_SPECS.len());
    let mut warnings = Vec::new();

    for spec in METRIC_SPECS {
        let query = build_histogram_query(spec, &options.match_labels, window_seconds)?;
        let result = client.query(&query, options.end_unix_seconds).await?;
        warnings.extend(
            result
                .warnings
                .into_iter()
                .map(|warning| format!("{}: {warning}", spec.metric)),
        );
        let series = histogram_series(spec, result.samples)?;
        if series.is_empty() {
            warnings.push(format!(
                "{} returned no series for the selected window",
                spec.metric
            ));
        }
        histograms.push(CalibrationHistogram {
            metric: spec.metric.to_owned(),
            unit: "seconds".to_owned(),
            query,
            series,
        });
    }

    if histograms
        .iter()
        .all(|histogram| histogram.series.is_empty())
    {
        bail!("calibration window returned no usable histogram series");
    }

    let exported_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs();
    Ok(CalibrationProfile {
        schema_version: CALIBRATION_PROFILE_SCHEMA_VERSION,
        kind: CALIBRATION_PROFILE_KIND.to_owned(),
        exporter: "biei-sim".to_owned(),
        exporter_version: env!("CARGO_PKG_VERSION").to_owned(),
        exported_at_unix_seconds,
        collection: CalibrationCollection {
            prometheus_url: endpoint.to_string(),
            start_unix_seconds: options.start_unix_seconds,
            end_unix_seconds: options.end_unix_seconds,
            window_seconds,
            evaluation_unix_seconds: options.end_unix_seconds,
            match_labels: options.match_labels,
        },
        provenance: options.provenance,
        histograms,
        warnings,
    })
}

pub fn parse_match_labels<I, S>(values: I) -> Result<BTreeMap<String, String>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut parsed = BTreeMap::new();
    for value in values {
        let (name, value) = parse_match_label(value.as_ref())?;
        if parsed.insert(name.clone(), value).is_some() {
            bail!("duplicate Prometheus match label {name:?}");
        }
    }
    Ok(parsed)
}

fn parse_match_label(value: &str) -> Result<(String, String)> {
    let Some((name, value)) = value.split_once('=') else {
        bail!("Prometheus match label must use NAME=VALUE syntax: {value:?}");
    };
    if !valid_label_name(name) {
        bail!("invalid Prometheus label name in matcher: {name:?}");
    }
    Ok((name.to_owned(), value.to_owned()))
}

pub fn read_bearer_token(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref();
    let token = fs::read_to_string(path)
        .with_context(|| format!("read Prometheus bearer token {}", path.display()))?;
    let token = token.trim();
    if token.is_empty() {
        bail!("Prometheus bearer token file is empty: {}", path.display());
    }
    Ok(token.to_owned())
}

fn validate_export_options(options: &CalibrationExportOptions) -> Result<()> {
    if options.start_unix_seconds >= options.end_unix_seconds {
        bail!("calibration start must be earlier than end");
    }
    if options.timeout.is_zero() {
        bail!("Prometheus request timeout must be non-zero");
    }
    for unsafe_label in ["pod", "instance"] {
        if options.match_labels.contains_key(unsafe_label) {
            bail!(
                "calibration matcher {unsafe_label:?} is not allowed: ingress outcomes and forwarded FileSource work may occur on different pods; export a cluster-wide deployment window"
            );
        }
    }
    let provenance = &options.provenance;
    for (label, value) in [
        (
            "deployment revision",
            provenance.deployment_revision.as_str(),
        ),
        ("architecture", provenance.architecture.as_str()),
        ("hardware profile", provenance.hardware_profile.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("calibration {label} must not be empty");
        }
    }
    for (label, value) in [
        ("CPU cores per node", provenance.cpu_cores_per_node),
        (
            "renderer slots per node",
            provenance.renderer_slots_per_node,
        ),
        (
            "execution permits per node",
            provenance.execution_permits_per_node,
        ),
        (
            "native-render permits per node",
            provenance.native_render_permits_per_node,
        ),
    ] {
        if value == 0 {
            bail!("calibration {label} must be non-zero");
        }
    }
    if provenance.execution_permits_per_node > provenance.renderer_slots_per_node {
        bail!("execution permits per node must not exceed renderer slots per node");
    }
    if provenance.native_render_permits_per_node > provenance.renderer_slots_per_node {
        bail!("native-render permits per node must not exceed renderer slots per node");
    }
    if provenance.capture_concurrency == Some(0) {
        bail!("calibration capture concurrency must be non-zero when recorded");
    }
    Ok(())
}

fn prometheus_query_endpoint(base: &str) -> Result<Url> {
    let mut url = Url::parse(base).context("parse Prometheus URL")?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("Prometheus URL must use http or https");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("Prometheus URL must not contain credentials; use a bearer-token file");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("Prometheus URL must not contain a query or fragment");
    }
    let path = url.path().trim_end_matches('/');
    let endpoint_path = if path.ends_with("/api/v1/query") {
        path.to_owned()
    } else if path.ends_with("/api/v1") {
        format!("{path}/query")
    } else {
        format!("{path}/api/v1/query")
    };
    url.set_path(&endpoint_path);
    Ok(url)
}

fn build_histogram_query(
    spec: &MetricSpec,
    match_labels: &BTreeMap<String, String>,
    window_seconds: u64,
) -> Result<String> {
    let mut matchers = match_labels.clone();
    if matchers.contains_key("le") {
        bail!("the exporter owns the Prometheus histogram label \"le\"");
    }
    if spec.ingress_only {
        if matchers.contains_key("scope") {
            bail!("the exporter owns the calibration matcher scope=ingress");
        }
        matchers.insert("scope".to_owned(), "ingress".to_owned());
    }
    for name in matchers.keys() {
        if !valid_label_name(name) {
            bail!("invalid Prometheus label name in matcher: {name:?}");
        }
    }

    let selector = if matchers.is_empty() {
        String::new()
    } else {
        let parts = matchers
            .iter()
            .map(|(name, value)| {
                let quoted =
                    serde_json::to_string(value).expect("string serialization cannot fail");
                format!("{name}={quoted}")
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("{{{parts}}}")
    };
    let group_labels = std::iter::once("le")
        .chain(spec.group_labels.iter().copied())
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!(
        "sum by ({group_labels}) (increase({}_bucket{selector}[{window_seconds}s]))",
        spec.metric
    ))
}

fn valid_label_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

struct PrometheusClient {
    endpoint: Url,
    client: Client,
    bearer_token: Option<String>,
}

impl PrometheusClient {
    fn new(endpoint: Url, bearer_token: Option<String>, timeout: Duration) -> Result<Self> {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .context("build Prometheus HTTP client")?;
        Ok(Self {
            endpoint,
            client,
            bearer_token,
        })
    }

    async fn query(&self, query: &str, evaluation_unix_seconds: u64) -> Result<QueryResult> {
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("query", query)
            .append_pair("time", &evaluation_unix_seconds.to_string());
        let mut request = self.client.get(url);
        if let Some(token) = &self.bearer_token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.context("query Prometheus")?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_PROMETHEUS_RESPONSE_BYTES as u64)
        {
            bail!("Prometheus response exceeds 16 MiB");
        }
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .context("read Prometheus response body")?;
        if bytes.len() > MAX_PROMETHEUS_RESPONSE_BYTES {
            bail!("Prometheus response exceeds 16 MiB");
        }
        decode_prometheus_response(status.as_u16(), &bytes)
    }
}

#[derive(Debug)]
struct QueryResult {
    samples: Vec<PrometheusSample>,
    warnings: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PrometheusApiResponse {
    status: String,
    #[serde(default)]
    data: Option<PrometheusData>,
    #[serde(default, rename = "errorType")]
    error_type: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PrometheusData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PrometheusSample>,
}

#[derive(Debug, Deserialize)]
struct PrometheusSample {
    metric: BTreeMap<String, String>,
    value: (f64, String),
}

type SeriesLabels = BTreeMap<String, String>;
type CumulativeBuckets = Vec<(Option<f64>, f64)>;

fn decode_prometheus_response(status_code: u16, bytes: &[u8]) -> Result<QueryResult> {
    let response: PrometheusApiResponse = serde_json::from_slice(bytes)
        .with_context(|| format!("decode Prometheus response (HTTP {status_code})"))?;
    if status_code >= 400 || response.status != "success" {
        bail!(
            "Prometheus query failed (HTTP {status_code}, type={}): {}",
            response.error_type.as_deref().unwrap_or("unknown"),
            response.error.as_deref().unwrap_or("unknown error")
        );
    }
    let data = response
        .data
        .context("successful Prometheus response has no data")?;
    if data.result_type != "vector" {
        bail!(
            "Prometheus instant query returned {:?}, expected vector",
            data.result_type
        );
    }
    Ok(QueryResult {
        samples: data.result,
        warnings: response.warnings,
    })
}

fn histogram_series(
    spec: &MetricSpec,
    samples: Vec<PrometheusSample>,
) -> Result<Vec<CalibrationSeries>> {
    let mut groups: BTreeMap<SeriesLabels, CumulativeBuckets> = BTreeMap::new();
    for sample in samples {
        let mut labels = sample.metric;
        let le = labels
            .remove("le")
            .with_context(|| format!("{} sample is missing le label", spec.metric))?;
        for expected in spec.group_labels {
            if !labels.contains_key(*expected) {
                bail!("{} sample is missing group label {expected:?}", spec.metric);
            }
        }
        if labels.len() != spec.group_labels.len() {
            bail!(
                "{} query returned unexpected labels: {:?}",
                spec.metric,
                labels.keys().collect::<Vec<_>>()
            );
        }
        let upper_bound = if le == "+Inf" {
            None
        } else {
            let value = le
                .parse::<f64>()
                .with_context(|| format!("{} has invalid le={le:?}", spec.metric))?;
            if !value.is_finite() || value < 0.0 {
                bail!("{} has invalid finite bucket bound {value}", spec.metric);
            }
            Some(value)
        };
        let count = sample
            .value
            .1
            .parse::<f64>()
            .with_context(|| format!("{} has invalid sample value", spec.metric))?;
        if !count.is_finite() || count < 0.0 {
            bail!("{} has invalid bucket count {count}", spec.metric);
        }
        groups.entry(labels).or_default().push((upper_bound, count));
    }

    groups
        .into_iter()
        .map(|(labels, mut cumulative)| {
            cumulative.sort_by(|(left, _), (right, _)| match (left, right) {
                (Some(left), Some(right)) => left.total_cmp(right),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            });
            for pair in cumulative.windows(2) {
                if pair[0].0 == pair[1].0 {
                    bail!(
                        "{} returned duplicate histogram bound {:?}",
                        spec.metric,
                        pair[0].0
                    );
                }
            }
            if cumulative.last().is_none_or(|(bound, _)| bound.is_some()) {
                bail!("{} histogram series is missing +Inf", spec.metric);
            }
            let counts = cumulative
                .iter()
                .map(|(_, count)| *count)
                .collect::<Vec<_>>();
            let disjoint = disjoint_counts(&counts)
                .with_context(|| format!("{} labels={labels:?}", spec.metric))?;
            let sample_count = *counts.last().expect("non-empty after +Inf validation");
            let buckets = cumulative
                .into_iter()
                .zip(disjoint)
                .map(|((upper_bound_seconds, _), count)| CalibrationBucket {
                    upper_bound_seconds,
                    count,
                })
                .collect();
            Ok(CalibrationSeries {
                labels,
                sample_count,
                buckets,
            })
        })
        .collect()
}

fn disjoint_counts(cumulative: &[f64]) -> Result<Vec<f64>> {
    let mut previous = 0.0_f64;
    cumulative
        .iter()
        .enumerate()
        .map(|(index, current)| {
            if !current.is_finite() || *current < 0.0 {
                bail!("cumulative bucket {index} has invalid count {current}");
            }
            let tolerance = previous.abs().max(current.abs()).max(1.0) * 1e-9;
            if *current + tolerance < previous {
                bail!("cumulative histogram decreases at bucket {index}: {previous} -> {current}");
            }
            let count = (*current - previous).max(0.0);
            previous = *current;
            Ok(count)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{
        CALIBRATION_PROFILE_KIND, CALIBRATION_PROFILE_SCHEMA_VERSION, CalibrationCollection,
        CalibrationExportOptions, CalibrationProfile, CalibrationProvenance, METRIC_SPECS,
        PrometheusSample, build_histogram_query, decode_prometheus_response, disjoint_counts,
        export_calibration_profile, histogram_series, parse_match_label, parse_match_labels,
        prometheus_query_endpoint, validate_export_options,
    };

    #[test]
    fn cumulative_histogram_buckets_become_disjoint_counts() {
        assert_eq!(
            disjoint_counts(&[5.0, 8.0, 10.0]).expect("valid cumulative buckets"),
            vec![5.0, 3.0, 2.0]
        );
    }

    #[test]
    fn decreasing_cumulative_histogram_is_rejected() {
        let error = disjoint_counts(&[5.0, 4.0]).expect_err("non-monotonic histogram");
        assert!(error.to_string().contains("decreases"));
    }

    #[test]
    fn render_query_is_time_bounded_aggregated_and_ingress_only() {
        let labels = BTreeMap::from([
            ("container".to_owned(), "biei".to_owned()),
            ("namespace".to_owned(), "map-demo".to_owned()),
        ]);
        let query =
            build_histogram_query(&METRIC_SPECS[0], &labels, 900).expect("render histogram query");

        assert_eq!(
            query,
            "sum by (le,render_mode,scale,format,size,state) (increase(biei_render_duration_seconds_bucket{container=\"biei\",namespace=\"map-demo\",scope=\"ingress\"}[900s]))"
        );
    }

    #[test]
    fn matcher_values_are_escaped_and_owned_labels_are_rejected() {
        let labels = BTreeMap::from([("namespace".to_owned(), "a\"b\\c".to_owned())]);
        let query = build_histogram_query(&METRIC_SPECS[3], &labels, 60)
            .expect("escaped profile-prepare query");
        assert!(query.contains("namespace=\"a\\\"b\\\\c\""));

        let owned = BTreeMap::from([("scope".to_owned(), "forwarded".to_owned())]);
        assert!(build_histogram_query(&METRIC_SPECS[0], &owned, 60).is_err());
    }

    #[test]
    fn prometheus_vector_becomes_disjoint_profile_series() {
        let labels = BTreeMap::from([
            ("render_mode".to_owned(), "static".to_owned()),
            ("scale".to_owned(), "2x".to_owned()),
            ("format".to_owned(), "webp".to_owned()),
            ("size".to_owned(), "le_1024px".to_owned()),
            ("state".to_owned(), "warm".to_owned()),
        ]);
        let samples = [("0.1", "5"), ("0.5", "8"), ("+Inf", "10")]
            .into_iter()
            .map(|(le, value)| {
                let mut metric = labels.clone();
                metric.insert("le".to_owned(), le.to_owned());
                PrometheusSample {
                    metric,
                    value: (1_700_000_000.0, value.to_owned()),
                }
            })
            .collect();

        let series = histogram_series(&METRIC_SPECS[0], samples).expect("profile series");
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].sample_count, 10.0);
        assert_eq!(
            series[0]
                .buckets
                .iter()
                .map(|bucket| bucket.count)
                .collect::<Vec<_>>(),
            vec![5.0, 3.0, 2.0]
        );
        assert_eq!(series[0].buckets[2].upper_bound_seconds, None);
    }

    #[test]
    fn prometheus_api_errors_remain_actionable() {
        let error = decode_prometheus_response(
            422,
            br#"{"status":"error","errorType":"bad_data","error":"invalid query"}"#,
        )
        .expect_err("API error");
        assert!(error.to_string().contains("bad_data"));
        assert!(error.to_string().contains("invalid query"));
    }

    #[test]
    fn prometheus_endpoint_accepts_standard_and_google_managed_roots() {
        assert_eq!(
            prometheus_query_endpoint("http://localhost:9090")
                .expect("local endpoint")
                .as_str(),
            "http://localhost:9090/api/v1/query"
        );
        assert_eq!(
            prometheus_query_endpoint(
                "https://monitoring.googleapis.com/v1/projects/p/location/global/prometheus"
            )
            .expect("managed endpoint")
            .as_str(),
            "https://monitoring.googleapis.com/v1/projects/p/location/global/prometheus/api/v1/query"
        );
    }

    #[test]
    fn match_label_requires_prometheus_name_value_syntax() {
        assert_eq!(
            parse_match_label("namespace=map-demo").expect("matcher"),
            ("namespace".to_owned(), "map-demo".to_owned())
        );
        assert!(parse_match_label("namespace").is_err());
        assert!(parse_match_label("9namespace=map-demo").is_err());
    }

    #[test]
    fn match_labels_reject_duplicates_after_parsing() {
        let labels =
            parse_match_labels(["namespace=map-demo", "container=biei"]).expect("distinct labels");
        assert_eq!(
            labels.get("namespace").map(String::as_str),
            Some("map-demo")
        );
        assert_eq!(labels.get("container").map(String::as_str), Some("biei"));

        let error = parse_match_labels(["namespace=map-demo", "namespace=other"])
            .expect_err("duplicate label must fail");
        assert!(
            error
                .to_string()
                .contains("duplicate Prometheus match label")
        );
    }

    #[test]
    fn exporter_rejects_pod_scoped_matchers_that_split_forwarded_work() {
        let options = CalibrationExportOptions {
            prometheus_url: "http://prometheus.test".to_owned(),
            start_unix_seconds: 1,
            end_unix_seconds: 2,
            match_labels: BTreeMap::from([("pod".to_owned(), "biei-0".to_owned())]),
            bearer_token: None,
            timeout: Duration::from_secs(1),
            provenance: CalibrationProvenance {
                deployment_revision: "test".to_owned(),
                architecture: "x86_64".to_owned(),
                hardware_profile: "test-node".to_owned(),
                cpu_cores_per_node: 2,
                renderer_slots_per_node: 2,
                execution_permits_per_node: 2,
                native_render_permits_per_node: 2,
                capture_concurrency: Some(1),
                notes: None,
            },
        };

        let error = validate_export_options(&options)
            .expect_err("pod-scoped calibration must not mix ingress and forwarded work");
        assert!(error.to_string().contains("cluster-wide deployment window"));
    }

    #[test]
    fn calibration_snapshot_schema_roundtrips_and_refuses_overwrite() {
        let profile = CalibrationProfile {
            schema_version: CALIBRATION_PROFILE_SCHEMA_VERSION,
            kind: CALIBRATION_PROFILE_KIND.to_owned(),
            exporter: "biei-sim".to_owned(),
            exporter_version: "test".to_owned(),
            exported_at_unix_seconds: 1_700_000_100,
            collection: CalibrationCollection {
                prometheus_url: "https://prometheus.test/api/v1/query".to_owned(),
                start_unix_seconds: 1_700_000_000,
                end_unix_seconds: 1_700_000_100,
                window_seconds: 100,
                evaluation_unix_seconds: 1_700_000_100,
                match_labels: BTreeMap::from([("namespace".to_owned(), "map-demo".to_owned())]),
            },
            provenance: CalibrationProvenance {
                deployment_revision: "deadbeef".to_owned(),
                architecture: "x86_64".to_owned(),
                hardware_profile: "test-node".to_owned(),
                cpu_cores_per_node: 2,
                renderer_slots_per_node: 3,
                execution_permits_per_node: 2,
                native_render_permits_per_node: 2,
                capture_concurrency: Some(1),
                notes: None,
            },
            histograms: Vec::new(),
            warnings: vec!["fixture".to_owned()],
        };
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "biei-calibration-{}-{unique}.json",
            std::process::id()
        ));

        profile.write_new_json(&path).expect("first snapshot write");
        let decoded: CalibrationProfile = serde_json::from_str(
            &fs::read_to_string(&path).expect("read written calibration profile"),
        )
        .expect("schema roundtrip");
        assert_eq!(decoded.schema_version, CALIBRATION_PROFILE_SCHEMA_VERSION);
        assert_eq!(decoded.kind, CALIBRATION_PROFILE_KIND);
        assert!(profile.write_new_json(&path).is_err());

        fs::remove_file(path).expect("remove test snapshot");
    }

    #[tokio::test]
    async fn exporter_queries_prometheus_and_preserves_empty_optional_families() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let address = listener.local_addr().expect("mock address");
        let server = tokio::spawn(async move {
            for _ in 0..METRIC_SPECS.len() {
                let (mut stream, _) = listener.accept().await.expect("accept query");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).await.expect("read query");
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let body = if request.contains("biei_render_duration_seconds_bucket") {
                    render_vector_response()
                } else {
                    r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#.to_owned()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
            }
        });

        let profile = export_calibration_profile(CalibrationExportOptions {
            prometheus_url: format!("http://{address}"),
            start_unix_seconds: 1_700_000_000,
            end_unix_seconds: 1_700_000_900,
            match_labels: BTreeMap::from([("namespace".to_owned(), "map-demo".to_owned())]),
            bearer_token: None,
            timeout: Duration::from_secs(2),
            provenance: CalibrationProvenance {
                deployment_revision: "deadbeef".to_owned(),
                architecture: "x86_64".to_owned(),
                hardware_profile: "mock-node".to_owned(),
                cpu_cores_per_node: 2,
                renderer_slots_per_node: 3,
                execution_permits_per_node: 2,
                native_render_permits_per_node: 2,
                capture_concurrency: Some(1),
                notes: None,
            },
        })
        .await
        .expect("export profile");
        server.await.expect("mock server");

        assert_eq!(profile.collection.window_seconds, 900);
        assert_eq!(profile.histograms.len(), METRIC_SPECS.len());
        assert_eq!(profile.series_count(), 1);
        // Every optional family is empty in the mock; only the required
        // render family returns data.
        assert_eq!(profile.warnings.len(), METRIC_SPECS.len() - 1);
        assert_eq!(profile.histograms[0].series[0].sample_count, 10.0);
    }

    #[tokio::test]
    async fn exporter_accepts_partial_families_but_rejects_an_empty_window() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let address = listener.local_addr().expect("mock address");
        let server = tokio::spawn(async move {
            for _ in 0..METRIC_SPECS.len() {
                let (mut stream, _) = listener.accept().await.expect("accept query");
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await.expect("read query");
                let body = r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("write response");
            }
        });

        let result = export_calibration_profile(CalibrationExportOptions {
            prometheus_url: format!("http://{address}"),
            start_unix_seconds: 1_700_000_000,
            end_unix_seconds: 1_700_000_900,
            match_labels: BTreeMap::new(),
            bearer_token: None,
            timeout: Duration::from_secs(2),
            provenance: CalibrationProvenance {
                deployment_revision: "deadbeef".to_owned(),
                architecture: "x86_64".to_owned(),
                hardware_profile: "mock-node".to_owned(),
                cpu_cores_per_node: 2,
                renderer_slots_per_node: 3,
                execution_permits_per_node: 2,
                native_render_permits_per_node: 2,
                capture_concurrency: Some(1),
                notes: None,
            },
        })
        .await;
        server.await.expect("mock server");

        let error = result.expect_err("empty calibration window must fail");
        assert!(error.to_string().contains("no usable histogram series"));
    }

    fn render_vector_response() -> String {
        let mut result = Vec::new();
        for (le, value) in [("0.1", "5"), ("0.5", "8"), ("+Inf", "10")] {
            result.push(serde_json::json!({
                "metric": {
                    "le": le,
                    "render_mode": "static",
                    "scale": "2x",
                    "format": "webp",
                    "size": "le_1024px",
                    "state": "warm"
                },
                "value": [1_700_000_900.0, value]
            }));
        }
        serde_json::json!({
            "status": "success",
            "data": { "resultType": "vector", "result": result }
        })
        .to_string()
    }
}
