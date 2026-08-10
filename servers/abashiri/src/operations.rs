//! Bounded polling of delivery-service operational snapshots.

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, ensure};
use futures_util::future::join_all;
use mmpf_http::operational::{
    INTERNAL_STATUS_PATH, OPERATIONAL_SNAPSHOT_SCHEMA_VERSION, OperationalSnapshot,
};
use reqwest::{Client, redirect::Policy};
use serde::Serialize;
use serde_json::Value;
use tokio::{sync::Mutex, time::Instant};
use url::Url;

const MAX_ENDPOINTS: usize = 8;
const MAX_SOURCE_ID_BYTES: usize = 64;
const MAX_RESPONSE_BYTES: usize = 128 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const FRESHNESS: Duration = Duration::from_secs(2);
const MAX_STALE_AGE: Duration = Duration::from_secs(5 * 60);

/// Named delivery-service endpoint, parsed from `<source-id>=<url>`.
#[derive(Clone, Debug)]
pub(crate) struct OperationalStatusEndpoint {
    source_id: String,
    url: Url,
}

impl FromStr for OperationalStatusEndpoint {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (source_id, url) = value
            .split_once('=')
            .context("operational status endpoint must use <source-id>=<url>")?;
        ensure!(
            !source_id.is_empty()
                && source_id.len() <= MAX_SOURCE_ID_BYTES
                && source_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
            "operational status source id must be a bounded ASCII identifier"
        );
        let url = Url::parse(url).context("parse operational status endpoint URL")?;
        ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == INTERNAL_STATUS_PATH,
            "operational status endpoint must be an http(s) URL ending exactly in {INTERNAL_STATUS_PATH} without credentials, query, or fragment"
        );
        Ok(Self {
            source_id: source_id.to_owned(),
            url,
        })
    }
}

#[derive(Clone)]
pub(crate) struct OperationalStatusClient {
    inner: Arc<OperationalStatusInner>,
}

struct OperationalStatusInner {
    client: Client,
    endpoints: Arc<[OperationalStatusEndpoint]>,
    freshness: Duration,
    max_stale_age: Duration,
    cache: Mutex<OperationalCache>,
}

#[derive(Default)]
struct OperationalCache {
    current: Option<OperationalOverview>,
    refresh_after: Option<Instant>,
    last_good: HashMap<String, LastGood>,
}

struct LastGood {
    fetched_at: Instant,
    snapshot: OperationalSnapshot<Value>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct OperationalOverview {
    observed_at_unix_ms: u64,
    complete: bool,
    sources: Vec<OperationalSource>,
}

#[derive(Clone, Debug, Serialize)]
struct OperationalSource {
    source_id: String,
    #[serde(flatten)]
    state: OperationalSourceState,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum OperationalSourceState {
    Fresh {
        snapshot: OperationalSnapshot<Value>,
    },
    Stale {
        stale_for_ms: u64,
        snapshot: OperationalSnapshot<Value>,
    },
    Unavailable,
}

impl OperationalStatusClient {
    pub(crate) fn new(endpoints: Vec<OperationalStatusEndpoint>) -> anyhow::Result<Self> {
        Self::new_with_timing(endpoints, FRESHNESS, MAX_STALE_AGE)
    }

    fn new_with_timing(
        endpoints: Vec<OperationalStatusEndpoint>,
        freshness: Duration,
        max_stale_age: Duration,
    ) -> anyhow::Result<Self> {
        ensure!(
            !endpoints.is_empty() && endpoints.len() <= MAX_ENDPOINTS,
            "operational status endpoints must contain 1..={MAX_ENDPOINTS} entries"
        );
        let mut source_ids = HashSet::with_capacity(endpoints.len());
        let mut urls = HashSet::with_capacity(endpoints.len());
        for endpoint in &endpoints {
            ensure!(
                source_ids.insert(endpoint.source_id.clone()),
                "operational status source ids must be unique"
            );
            ensure!(
                urls.insert(endpoint.url.as_str().to_owned()),
                "operational status endpoint URLs must be unique"
            );
        }
        let client = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(Policy::none())
            .use_rustls_tls()
            .build()
            .context("build operational status HTTP client")?;
        Ok(Self {
            inner: Arc::new(OperationalStatusInner {
                client,
                endpoints: endpoints.into(),
                freshness,
                max_stale_age,
                cache: Mutex::new(OperationalCache::default()),
            }),
        })
    }

    /// Returns one coalesced observation. A failed source retains only a
    /// bounded, explicitly stale last-known-good snapshot.
    pub(crate) async fn overview(&self) -> OperationalOverview {
        let mut cache = self.inner.cache.lock().await;
        let now = Instant::now();
        if cache
            .refresh_after
            .is_some_and(|refresh_after| now < refresh_after)
            && let Some(current) = &cache.current
        {
            return current.clone();
        }

        let results = join_all(
            self.inner
                .endpoints
                .iter()
                .map(|endpoint| fetch_snapshot(&self.inner.client, endpoint)),
        )
        .await;
        let fetched_at = Instant::now();
        let mut sources = Vec::with_capacity(results.len());
        for (endpoint, result) in self.inner.endpoints.iter().zip(results) {
            let state = match result {
                Ok(snapshot) => {
                    cache.last_good.insert(
                        endpoint.source_id.clone(),
                        LastGood {
                            fetched_at,
                            snapshot: snapshot.clone(),
                        },
                    );
                    OperationalSourceState::Fresh { snapshot }
                }
                Err(error) => {
                    tracing::warn!(
                        source_id = %endpoint.source_id,
                        error = %format_args!("{error:#}"),
                        "delivery operational status poll failed"
                    );
                    match cache.last_good.get(&endpoint.source_id) {
                        Some(previous)
                            if fetched_at.duration_since(previous.fetched_at)
                                <= self.inner.max_stale_age =>
                        {
                            OperationalSourceState::Stale {
                                stale_for_ms: duration_ms(
                                    fetched_at.duration_since(previous.fetched_at),
                                ),
                                snapshot: previous.snapshot.clone(),
                            }
                        }
                        _ => OperationalSourceState::Unavailable,
                    }
                }
            };
            sources.push(OperationalSource {
                source_id: endpoint.source_id.clone(),
                state,
            });
        }
        cache.last_good.retain(|_, previous| {
            fetched_at.duration_since(previous.fetched_at) <= self.inner.max_stale_age
        });
        let overview = OperationalOverview {
            observed_at_unix_ms: unix_time_ms(),
            complete: sources
                .iter()
                .all(|source| matches!(source.state, OperationalSourceState::Fresh { .. })),
            sources,
        };
        cache.current = Some(overview.clone());
        cache.refresh_after = Some(fetched_at + self.inner.freshness);
        overview
    }
}

async fn fetch_snapshot(
    client: &Client,
    endpoint: &OperationalStatusEndpoint,
) -> anyhow::Result<OperationalSnapshot<Value>> {
    let mut response = client
        .get(endpoint.url.clone())
        .send()
        .await
        .with_context(|| format!("request status from {}", endpoint.source_id))?;
    ensure!(
        response.status().is_success(),
        "status endpoint returned {}",
        response.status()
    );
    if let Some(length) = response.content_length() {
        ensure!(
            length <= MAX_RESPONSE_BYTES as u64,
            "status response exceeds {MAX_RESPONSE_BYTES} bytes"
        );
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.context("read status response")? {
        ensure!(
            body.len().saturating_add(chunk.len()) <= MAX_RESPONSE_BYTES,
            "status response exceeds {MAX_RESPONSE_BYTES} bytes"
        );
        body.extend_from_slice(&chunk);
    }
    let snapshot: OperationalSnapshot<Value> =
        serde_json::from_slice(&body).context("parse status response JSON")?;
    ensure!(
        snapshot.schema_version == OPERATIONAL_SNAPSHOT_SCHEMA_VERSION,
        "unsupported operational snapshot schema version"
    );
    ensure!(
        !snapshot.service.is_empty()
            && snapshot.service.len() <= 32
            && snapshot
                .service
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "invalid operational snapshot service"
    );
    ensure!(
        !snapshot.observer_node_id.is_empty() && snapshot.observer_node_id.len() <= 256,
        "invalid operational snapshot observer"
    );
    ensure!(
        snapshot.status.is_object(),
        "invalid operational status payload"
    );
    Ok(snapshot)
}

fn unix_time_ms() -> u64 {
    duration_ms(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default(),
    )
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use tokio::{io::AsyncWriteExt as _, net::TcpListener, time::sleep};

    use super::*;

    fn endpoint(source: &str, address: std::net::SocketAddr) -> OperationalStatusEndpoint {
        format!("{source}=http://{address}{INTERNAL_STATUS_PATH}")
            .parse()
            .unwrap()
    }

    async fn sequential_server(
        responses: Vec<&'static str>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            for response in &responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let _ = crate::test_http::read_http_request(&mut stream).await;
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            responses.len()
        });
        (address, task)
    }

    #[test]
    fn endpoint_requires_a_name_and_the_exact_internal_path() {
        for invalid in [
            "http://biei:9090/_internal/operations/v1/status",
            "biei=http://biei:9090/",
            "bad/name=http://biei:9090/_internal/operations/v1/status",
            "biei=http://user@biei:9090/_internal/operations/v1/status",
        ] {
            assert!(invalid.parse::<OperationalStatusEndpoint>().is_err());
        }
    }

    #[tokio::test]
    async fn coalesces_fresh_reads_and_marks_a_failed_refresh_stale() {
        let body = r#"{"schema_version":1,"service":"biei","observer_node_id":"node-a","observed_at_unix_ms":1,"status":{"ready":true}}"#;
        let ok = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let responses: Vec<&'static str> = vec![
            Box::leak(ok.into_boxed_str()),
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ];
        let (address, server) = sequential_server(responses).await;
        let client = OperationalStatusClient::new_with_timing(
            vec![endpoint("renderer", address)],
            Duration::from_millis(10),
            Duration::from_secs(1),
        )
        .unwrap();

        let first = client.overview().await;
        assert!(first.complete);
        let cached = client.overview().await;
        assert!(cached.complete);
        sleep(Duration::from_millis(20)).await;
        let stale = client.overview().await;
        assert!(!stale.complete);
        let value = serde_json::to_value(stale).unwrap();
        assert_eq!(value["sources"][0]["state"], "stale");
        assert_eq!(server.await.unwrap(), 2);
    }
}
