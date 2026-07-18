//! Controlled low-volume production exercises for calibration captures.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, Url};

const MAX_CONCURRENCY: usize = 1_024;

#[derive(Clone, Debug)]
pub struct CalibrationExerciseOptions {
    pub urls: Vec<String>,
    pub warmup_requests_per_url: usize,
    pub requests_per_url: usize,
    pub concurrency: usize,
    pub timeout: Duration,
    /// Allow Prometheus to scrape once after warmup and once after the
    /// measured batch. Set this to at least the configured scrape interval.
    pub scrape_settle: Duration,
}

#[derive(Debug)]
pub struct CalibrationExerciseSummary {
    pub start_unix_seconds: u64,
    pub end_unix_seconds: u64,
    pub requests: usize,
    pub status_counts: BTreeMap<u16, usize>,
}

impl CalibrationExerciseSummary {
    pub fn successful(&self) -> usize {
        self.status_counts
            .iter()
            .filter(|(status, _)| (200..300).contains(*status))
            .map(|(_, count)| *count)
            .sum()
    }
}

pub async fn run_calibration_exercise(
    options: CalibrationExerciseOptions,
) -> Result<CalibrationExerciseSummary> {
    validate_options(&options)?;
    let urls = Arc::new(
        options
            .urls
            .iter()
            .map(|value| {
                let url =
                    Url::parse(value).with_context(|| format!("parse exercise URL {value:?}"))?;
                ensure!(
                    matches!(url.scheme(), "http" | "https"),
                    "exercise URL must use http or https: {value:?}"
                );
                Ok(url)
            })
            .collect::<Result<Vec<_>>>()?,
    );
    let client = Client::builder()
        .timeout(options.timeout)
        .build()
        .context("build calibration exercise client")?;

    let warmup = options
        .warmup_requests_per_url
        .checked_mul(urls.len())
        .context("warmup request count overflow")?;
    if warmup > 0 {
        run_batch(
            client.clone(),
            Arc::clone(&urls),
            warmup,
            options.concurrency,
        )
        .await
        .context("calibration warmup failed")?;
    }
    tokio::time::sleep(options.scrape_settle).await;

    let requests = options
        .requests_per_url
        .checked_mul(urls.len())
        .context("measured request count overflow")?;
    let start_unix_seconds = unix_seconds()?;
    let status_counts = run_batch(client, urls, requests, options.concurrency).await?;
    tokio::time::sleep(options.scrape_settle).await;
    let end_unix_seconds = unix_seconds()?.max(start_unix_seconds + 1);

    Ok(CalibrationExerciseSummary {
        start_unix_seconds,
        end_unix_seconds,
        requests,
        status_counts,
    })
}

fn validate_options(options: &CalibrationExerciseOptions) -> Result<()> {
    ensure!(!options.urls.is_empty(), "at least one --url is required");
    ensure!(
        options.requests_per_url > 0,
        "requests per URL must be non-zero"
    );
    ensure!(
        (1..=MAX_CONCURRENCY).contains(&options.concurrency),
        "concurrency must be in 1..={MAX_CONCURRENCY}"
    );
    ensure!(
        !options.timeout.is_zero(),
        "request timeout must be non-zero"
    );
    Ok(())
}

async fn run_batch(
    client: Client,
    urls: Arc<Vec<Url>>,
    requests: usize,
    concurrency: usize,
) -> Result<BTreeMap<u16, usize>> {
    let next = Arc::new(AtomicUsize::new(0));
    let worker_count = concurrency.min(requests.max(1));
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..worker_count {
        let client = client.clone();
        let urls = Arc::clone(&urls);
        let next = Arc::clone(&next);
        workers.spawn(async move {
            let mut statuses = BTreeMap::<u16, usize>::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= requests {
                    break;
                }
                let url = &urls[index % urls.len()];
                let response = client
                    .get(url.clone())
                    .send()
                    .await
                    .with_context(|| format!("request {url}"))?;
                let status = response.status().as_u16();
                response
                    .bytes()
                    .await
                    .with_context(|| format!("read response body from {url}"))?;
                *statuses.entry(status).or_default() += 1;
            }
            Ok::<_, anyhow::Error>(statuses)
        });
    }

    let mut combined = BTreeMap::new();
    while let Some(result) = workers.join_next().await {
        let statuses = result.context("calibration exercise worker panicked")??;
        for (status, count) in statuses {
            *combined.entry(status).or_default() += count;
        }
    }
    if combined.values().sum::<usize>() != requests {
        bail!("calibration exercise completed an unexpected number of requests");
    }
    Ok(combined)
}

fn unix_seconds() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{CalibrationExerciseOptions, run_calibration_exercise};

    #[tokio::test]
    async fn exercise_warms_then_reports_only_measured_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            for _ in 0..6 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request).await.expect("read");
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await
                    .expect("respond");
            }
        });

        let summary = run_calibration_exercise(CalibrationExerciseOptions {
            urls: vec![format!("http://{address}/tile")],
            warmup_requests_per_url: 2,
            requests_per_url: 4,
            concurrency: 2,
            timeout: Duration::from_secs(2),
            scrape_settle: Duration::ZERO,
        })
        .await
        .expect("exercise");
        server.await.expect("server");

        assert_eq!(summary.requests, 4);
        assert_eq!(summary.successful(), 4);
        assert_eq!(summary.status_counts.get(&200), Some(&4));
        assert!(summary.end_unix_seconds > summary.start_unix_seconds);
    }
}
