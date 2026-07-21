//! Concrete reqwest-based internal peer transport for production.
//!
//! `ishikari-core` defines the [`InternalTransport`] seam and injects it into
//! the peer backend; the reqwest client (and its TLS/HTTP stack) lives here in
//! the server binary so the core and simulator do not depend on reqwest.

use std::time::Duration;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use reqwest::{Client, Response, StatusCode, header};

use ishikari_core::storage::{
    FetchFuture, InternalFetchResponse, InternalProviderNegative, InternalTileSource,
    InternalTransport, PROVIDER_AGE_HEADER, PROVIDER_CACHE_CONTROL_HEADER, PROVIDER_ETAG_HEADER,
    PROVIDER_LAST_MODIFIED_HEADER, PROVIDER_NEGATIVE_HEADER, Peer, PeerFetchError,
    TILE_SOURCE_HEADER, internal_peer_request_timeout, internal_response_body_limit,
};

use crate::{http_client::representation_preserving_builder, request_id};

const INTERNAL_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

/// Peer transport backed by a representation-preserving reqwest client.
#[derive(Clone)]
pub(crate) struct HttpInternalTransport {
    http_client: Client,
}

impl HttpInternalTransport {
    /// Builds the internal transport with a representation-preserving client.
    ///
    /// A peer forwards provider bodies with their `Content-Encoding` intact as
    /// representation metadata. Keep transparent decompression disabled even
    /// when a workspace-wide build enables those reqwest features for Biei.
    pub(crate) fn new() -> Result<Self> {
        let http_client = representation_preserving_builder()
            .connect_timeout(INTERNAL_HTTP_CONNECT_TIMEOUT)
            .use_rustls_tls()
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self { http_client })
    }
}

impl InternalTransport for HttpInternalTransport {
    fn fetch<'a>(&'a self, peer: &'a Peer, path: &'a str) -> FetchFuture<'a> {
        Box::pin(async move {
            let url = format!("http://{}{}", peer.addr, path);
            let mut request = self
                .http_client
                .get(url)
                .timeout(internal_peer_request_timeout(path));
            if let Some(id) = request_id::current() {
                request = request.header(request_id::HEADER, id.as_str());
            }
            let mut response = request.send().await.map_err(|error| {
                if error.is_connect() || error.is_timeout() {
                    PeerFetchError::Retryable(error.to_string())
                } else {
                    PeerFetchError::Fatal(error.to_string())
                }
            })?;

            let status = response.status();
            let headers = response.headers();
            match authoritative_provider_negative(path, status, headers) {
                Some(InternalProviderNegative::NotFound) => {
                    return Err(PeerFetchError::ProviderNotFound);
                }
                Some(InternalProviderNegative::Gone) => {
                    return Err(PeerFetchError::ProviderGone);
                }
                None => {}
            }
            if status == StatusCode::NOT_FOUND {
                return Err(PeerFetchError::NotFound);
            }
            if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                return Err(PeerFetchError::Retryable(format!("peer returned {status}")));
            }
            if !status.is_success() {
                return Err(PeerFetchError::Fatal(format!(
                    "peer returned unexpected status {status}"
                )));
            }

            let str_header = |name: &str| headers.get(name).and_then(|value| value.to_str().ok());
            let owned_header = |name: &str| str_header(name).map(str::to_owned);
            let tile_source = str_header(TILE_SOURCE_HEADER).and_then(InternalTileSource::parse);
            let provider_cache_control = owned_header(PROVIDER_CACHE_CONTROL_HEADER);
            let provider_age_seconds =
                str_header(PROVIDER_AGE_HEADER).and_then(|value| value.parse().ok());
            let provider_etag = owned_header(PROVIDER_ETAG_HEADER);
            let provider_last_modified = owned_header(PROVIDER_LAST_MODIFIED_HEADER);
            let content_encoding = owned_header(header::CONTENT_ENCODING.as_str());
            let bytes =
                read_bounded_body(&mut response, internal_response_body_limit(path)).await?;
            Ok(InternalFetchResponse {
                bytes,
                tile_source,
                provider_cache_control,
                provider_age_seconds,
                provider_etag,
                provider_last_modified,
                content_encoding,
            })
        })
    }
}

async fn read_bounded_body(response: &mut Response, limit: usize) -> Result<Bytes, PeerFetchError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(PeerFetchError::Fatal(format!(
            "peer response body exceeds the {limit}-byte route limit"
        )));
    }
    let initial_capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or(0)
        .min(limit);
    let mut body = BytesMut::with_capacity(initial_capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| PeerFetchError::Fatal(error.to_string()))?
    {
        let Some(next_len) = body.len().checked_add(chunk.len()) else {
            return Err(PeerFetchError::Fatal(
                "peer response body length overflowed usize".to_string(),
            ));
        };
        if next_len > limit {
            return Err(PeerFetchError::Fatal(format!(
                "peer response body exceeds the {limit}-byte route limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn authoritative_provider_negative(
    path: &str,
    status: StatusCode,
    headers: &header::HeaderMap,
) -> Option<InternalProviderNegative> {
    if !path.starts_with("/_internal/provider/") {
        return None;
    }
    let marker = headers
        .get(PROVIDER_NEGATIVE_HEADER)?
        .to_str()
        .ok()
        .and_then(InternalProviderNegative::parse)?;
    match (status, marker) {
        (StatusCode::NOT_FOUND, InternalProviderNegative::NotFound) => Some(marker),
        (StatusCode::GONE, InternalProviderNegative::Gone) => Some(marker),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ishikari_core::storage::{
        InternalProviderNegative, PROVIDER_NEGATIVE_HEADER, internal_peer_request_timeout,
    };
    use reqwest::{
        StatusCode,
        header::{HeaderMap, HeaderValue},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{authoritative_provider_negative, read_bounded_body};

    async fn raw_response(bytes: &'static [u8]) -> reqwest::Response {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test peer");
        let address = listener.local_addr().expect("test peer address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept test request");
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.expect("read test request");
            stream.write_all(bytes).await.expect("write test response");
            stream.shutdown().await.expect("shutdown test response");
        });
        let response = reqwest::Client::new()
            .get(format!("http://{address}/"))
            .send()
            .await
            .expect("receive test response");
        server.await.expect("test peer task");
        response
    }

    #[tokio::test]
    async fn bounded_body_rejects_oversized_content_length_before_collection() {
        let mut response = raw_response(
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabcdef",
        )
        .await;

        let error = read_bounded_body(&mut response, 5)
            .await
            .expect_err("declared oversized body must fail");
        assert!(error.to_string().contains("5-byte route limit"));
    }

    #[tokio::test]
    async fn bounded_body_accepts_exact_declared_limit() {
        let mut response =
            raw_response(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nabcde")
                .await;

        let body = read_bounded_body(&mut response, 5)
            .await
            .expect("body at the route limit must pass");
        assert_eq!(body.as_ref(), b"abcde");
    }

    #[tokio::test]
    async fn bounded_body_rejects_chunked_transfer_at_limit_plus_one() {
        let mut response = raw_response(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n",
        )
        .await;

        let error = read_bounded_body(&mut response, 5)
            .await
            .expect_err("chunked oversized body must fail");
        assert!(error.to_string().contains("5-byte route limit"));
    }

    #[test]
    fn provider_negative_requires_exact_path_status_and_marker() {
        let provider_path = "/_internal/provider/styles/missing/style.json";
        for (status, marker) in [
            (StatusCode::NOT_FOUND, InternalProviderNegative::NotFound),
            (StatusCode::GONE, InternalProviderNegative::Gone),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                PROVIDER_NEGATIVE_HEADER,
                HeaderValue::from_static(marker.as_str()),
            );
            assert_eq!(
                authoritative_provider_negative(provider_path, status, &headers),
                Some(marker)
            );
            assert_eq!(
                authoritative_provider_negative("/_internal/derived/demo", status, &headers),
                None
            );
        }

        let bare = HeaderMap::new();
        assert_eq!(
            authoritative_provider_negative(provider_path, StatusCode::NOT_FOUND, &bare),
            None
        );
        assert_eq!(
            authoritative_provider_negative(provider_path, StatusCode::GONE, &bare),
            None
        );

        for (status, value) in [
            (StatusCode::NOT_FOUND, "gone"),
            (StatusCode::GONE, "not-found"),
            (StatusCode::NOT_FOUND, "unknown"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(PROVIDER_NEGATIVE_HEADER, HeaderValue::from_static(value));
            assert_eq!(
                authoritative_provider_negative(provider_path, status, &headers),
                None
            );
        }
    }

    #[test]
    fn derived_fetches_have_a_longer_peer_timeout() {
        assert_eq!(
            internal_peer_request_timeout(
                "/_internal/derived/mapterhorn%2Fplanet/hillshade/8/226/100"
            ),
            Duration::from_secs(30)
        );
        assert_eq!(
            internal_peer_request_timeout("/_internal/tiles/mierune%2Fomt/700"),
            Duration::from_secs(10)
        );
        assert_eq!(
            internal_peer_request_timeout("/_internal/provider/fonts/Test/0-255.pbf"),
            Duration::from_secs(20)
        );
    }
}
