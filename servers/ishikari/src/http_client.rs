//! HTTP client invariants shared by Ishikari's representation-preserving hops.

use reqwest::{Client, ClientBuilder};

/// Starts a client builder that never asks reqwest to transparently decode a
/// response body.
///
/// Biei intentionally enables some transfer-decompression features on the same
/// reqwest package. Cargo unifies those features in workspace-wide builds, so
/// Ishikari must enforce byte preservation at runtime rather than relying on a
/// feature being absent.
pub(crate) fn representation_preserving_builder() -> ClientBuilder {
    Client::builder()
        .no_gzip()
        .no_deflate()
        .no_brotli()
        .no_zstd()
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::representation_preserving_builder;

    #[tokio::test]
    async fn workspace_features_cannot_enable_transfer_decompression() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test origin");
        let address = listener.local_addr().expect("test origin address");
        let origin = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept request");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).await.expect("read request");
                assert!(read > 0, "client closed before completing headers");
                request.extend_from_slice(&buffer[..read]);
            }
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                      Content-Length: 2\r\nConnection: close\r\n\r\n{}",
                )
                .await
                .expect("write response");
            String::from_utf8(request).expect("HTTP request is ASCII")
        });

        let response = representation_preserving_builder()
            .build()
            .expect("build client")
            .get(format!("http://{address}/style.json"))
            .send()
            .await
            .expect("fetch test representation");
        assert!(response.status().is_success());

        let request = origin.await.expect("origin task").to_ascii_lowercase();
        assert!(
            !request.contains("\r\naccept-encoding:"),
            "representation-preserving client advertised decoding: {request}"
        );
    }
}
