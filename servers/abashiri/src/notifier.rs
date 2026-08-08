//! Bounded, post-commit delivery of advisory style-refresh hints.

use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, ensure};
use futures_util::future::join_all;
use mmpf_cluster::StyleRefreshHint;
use reqwest::{Client, redirect::Policy};
use url::Url;

const MAX_ENDPOINTS: usize = 8;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const REFRESH_PATH: &str = "/_internal/refresh/style";

#[derive(Clone)]
pub(crate) struct StyleRefreshNotifier {
    client: Client,
    endpoints: Arc<[Url]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RefreshDelivery {
    pub(crate) delivered: usize,
    pub(crate) total: usize,
}

impl RefreshDelivery {
    pub(crate) fn complete(self) -> bool {
        self.delivered == self.total
    }
}

impl StyleRefreshNotifier {
    pub(crate) fn new(endpoints: Vec<Url>) -> anyhow::Result<Self> {
        ensure!(
            !endpoints.is_empty() && endpoints.len() <= MAX_ENDPOINTS,
            "style refresh endpoints must contain 1..={MAX_ENDPOINTS} URLs"
        );
        for endpoint in &endpoints {
            ensure!(
                matches!(endpoint.scheme(), "http" | "https")
                    && endpoint.username().is_empty()
                    && endpoint.password().is_none()
                    && endpoint.query().is_none()
                    && endpoint.fragment().is_none()
                    && endpoint.path() == REFRESH_PATH,
                "style refresh endpoint must be an http(s) URL ending exactly in {REFRESH_PATH} without credentials, query, or fragment"
            );
        }
        let client = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(Policy::none())
            .use_rustls_tls()
            .build()
            .context("build style refresh HTTP client")?;
        Ok(Self {
            client,
            endpoints: endpoints.into(),
        })
    }

    /// Delivers the same idempotent hint to every configured service in parallel.
    pub(crate) async fn notify(&self, hint: &StyleRefreshHint) -> RefreshDelivery {
        let responses = join_all(
            self.endpoints
                .iter()
                .map(|endpoint| self.client.post(endpoint.clone()).json(hint).send()),
        )
        .await;
        let delivered = responses
            .into_iter()
            .filter(|response| {
                response
                    .as_ref()
                    .is_ok_and(|response| response.status().is_success())
            })
            .count();
        RefreshDelivery {
            delivered,
            total: self.endpoints.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::{io::AsyncWriteExt as _, net::TcpListener};

    use super::*;

    async fn endpoint(status: &str) -> (Url, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = crate::test_http::read_http_request(&mut stream).await;
            stream
                .write_all(
                    format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            String::from_utf8(request).unwrap()
        });
        (
            Url::parse(&format!("http://{address}{REFRESH_PATH}")).unwrap(),
            server,
        )
    }

    #[tokio::test]
    async fn delivers_in_parallel_and_reports_partial_failure() {
        let (accepted, accepted_request) = endpoint("202 Accepted").await;
        let (failed, failed_request) = endpoint("503 Service Unavailable").await;
        let notifier = StyleRefreshNotifier::new(vec![accepted, failed]).unwrap();
        let hint = StyleRefreshHint::new("mutation-42", "demo/basic").unwrap();

        let delivery = notifier.notify(&hint).await;
        assert_eq!(
            delivery,
            RefreshDelivery {
                delivered: 1,
                total: 2
            }
        );
        let accepted_request = accepted_request.await.unwrap();
        let failed_request = failed_request.await.unwrap();
        for request in [accepted_request, failed_request] {
            assert!(request.starts_with("POST /_internal/refresh/style HTTP/1.1\r\n"));
            assert!(request.contains(r#""hint_id":"mutation-42""#));
            assert!(request.contains(r#""style_id":"demo/basic""#));
            assert!(!request.to_ascii_lowercase().contains("authorization:"));
        }
    }

    #[test]
    fn accepts_only_the_fixed_internal_receiver_path() {
        for invalid in [
            "https://example.test/",
            "https://user@example.test/_internal/refresh/style",
            "https://example.test/_internal/refresh/style?token=x",
        ] {
            assert!(StyleRefreshNotifier::new(vec![Url::parse(invalid).unwrap()]).is_err());
        }
    }
}
