//! Shared HTTP safety helpers for renderer resource fetches.

use std::fmt;

use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BodyReadError {
    Timeout,
    Transport(&'static str),
    TooLarge { limit: usize },
}

impl fmt::Display for BodyReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => formatter.write_str("response body read timed out"),
            Self::Transport(kind) => write!(formatter, "response body read failed ({kind})"),
            Self::TooLarge { limit } => write!(formatter, "response body exceeds {limit} bytes"),
        }
    }
}

/// Buffers an HTTP body without allowing chunked transfer encoding to bypass
/// the caller's memory cap. The same absolute deadline covers every chunk.
pub(crate) async fn read_bounded_body(
    mut response: reqwest::Response,
    limit: usize,
    deadline: Instant,
) -> Result<Vec<u8>, BodyReadError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(BodyReadError::TooLarge { limit });
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(limit as u64) as usize,
    );
    loop {
        let chunk = tokio::time::timeout_at(deadline, response.chunk())
            .await
            .map_err(|_| BodyReadError::Timeout)?
            .map_err(|error| BodyReadError::Transport(reqwest_error_label(&error)))?;
        let Some(chunk) = chunk else {
            return Ok(body);
        };
        let new_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or(BodyReadError::TooLarge { limit })?;
        if new_len > limit {
            return Err(BodyReadError::TooLarge { limit });
        }
        body.extend_from_slice(&chunk);
    }
}

pub(crate) fn reqwest_error_label(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_redirect() {
        "redirect"
    } else if error.is_body() {
        "body"
    } else if error.is_decode() {
        "decode"
    } else {
        "request"
    }
}

pub(crate) fn redacted_url(url: &url::Url) -> String {
    let mut redacted = url.clone();
    let _ = redacted.set_username("");
    let _ = redacted.set_password(None);
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted.to_string()
}

pub(crate) fn redacted_url_str(raw: &str) -> String {
    url::Url::parse(raw)
        .map(|url| redacted_url(&url))
        .unwrap_or_else(|_| "invalid resource URL".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    async fn spawn_raw_http_server(
        chunks: Vec<&'static [u8]>,
        chunk_delay: std::time::Duration,
    ) -> (url::Url, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server binds");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("test request connects");
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await
                .expect("response headers write");
            socket.flush().await.expect("response headers flush");
            for chunk in chunks {
                if !chunk_delay.is_zero() {
                    tokio::time::sleep(chunk_delay).await;
                }
                socket
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await
                    .expect("chunk length writes");
                socket.write_all(chunk).await.expect("chunk writes");
                socket.write_all(b"\r\n").await.expect("chunk terminates");
            }
            socket
                .write_all(b"0\r\n\r\n")
                .await
                .expect("response terminates");
            socket.flush().await.expect("response flushes");
            socket.shutdown().await.expect("response socket shuts down");
        });
        (
            url::Url::parse(&format!("http://{address}/resource")).expect("test URL"),
            server,
        )
    }

    #[test]
    fn redacts_credentials_query_and_fragment() {
        let url = url::Url::parse(
            "https://user:password@example.test/style.json?access_token=secret#fragment",
        )
        .expect("valid URL");

        let redacted = redacted_url(&url);

        assert_eq!(redacted, "https://example.test/style.json");
        assert!(!redacted.contains("password"));
        assert!(!redacted.contains("secret"));
    }

    #[test]
    fn malformed_url_does_not_echo_credentials() {
        let secret = "do-not-log-this-password";

        let redacted = redacted_url_str(&format!("http://user:{secret}@[invalid"));

        assert_eq!(redacted, "invalid resource URL");
        assert!(!redacted.contains(secret));
    }

    #[tokio::test]
    async fn chunked_body_cannot_bypass_size_limit() {
        let (url, server) = spawn_raw_http_server(
            vec![b"abcd".as_slice(), b"efgh".as_slice()],
            std::time::Duration::ZERO,
        )
        .await;
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("response headers");

        let error = read_bounded_body(
            response,
            6,
            Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .expect_err("eight-byte chunked body exceeds six-byte limit");

        server.await.expect("test server finishes");
        assert_eq!(error, BodyReadError::TooLarge { limit: 6 });
    }

    #[tokio::test]
    async fn body_reader_uses_absolute_deadline() {
        let (url, server) = spawn_raw_http_server(
            vec![b"late".as_slice()],
            std::time::Duration::from_millis(100),
        )
        .await;
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("response headers");

        let error = read_bounded_body(
            response,
            16,
            Instant::now() + std::time::Duration::from_millis(10),
        )
        .await
        .expect_err("delayed body exceeds deadline");

        assert_eq!(error, BodyReadError::Timeout);
        server.abort();
    }
}
