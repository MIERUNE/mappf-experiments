//! Style and TileJSON fetching, validation, and URL resolution.

use tokio::time::Instant;

use biei_core::types::{
    AddLayerSource, InternalTask, ProfilePreparationError, ProviderBearerToken, RenderRequest,
    SourceHash, StyleId,
};
use mmpf_mln_filesource::http::{
    BodyReadError, read_bounded_body, redacted_url, redacted_url_str, reqwest_error_label,
};

use super::profile::{ProfileFetchError, is_permanent_profile_http_status, style_load_failed};

const MAX_STYLE_JSON_BYTES: usize = 2 * 1024 * 1024;
const MAX_TILESET_JSON_BYTES: usize = 1024 * 1024;

pub(super) fn addlayer_source_from_task(task: &InternalTask) -> Option<&AddLayerSource> {
    match &task.request {
        RenderRequest::StaticImage {
            addlayer: Some(addlayer),
            ..
        } => addlayer.source.as_ref(),
        _ => None,
    }
}

/// The addlayer's stable hash, used only to identify a failed source in a
/// `SourceFetchFailed` error (diagnostic, never a metric label). `None` when the
/// task carries no addlayer source.
pub(super) fn addlayer_source_hash_from_task(task: &InternalTask) -> Option<SourceHash> {
    match &task.request {
        RenderRequest::StaticImage {
            addlayer: Some(addlayer),
            ..
        } => addlayer.source.as_ref().map(|_| addlayer.hash),
        _ => None,
    }
}

pub(super) fn source_url_from_addlayer_source(
    style_id: &StyleId,
    source: &AddLayerSource,
) -> Result<String, ProfilePreparationError> {
    let value: serde_json::Value = serde_json::from_str(&source.json).map_err(|err| {
        ProfilePreparationError::invalid_style(
            style_id,
            format!("addlayer source JSON parse failed: {err}"),
        )
    })?;
    let url = value
        .as_object()
        .and_then(|obj| obj.get("url"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            ProfilePreparationError::invalid_style(
                style_id,
                "addlayer source JSON is missing TileJSON URL",
            )
        })?;
    Ok(url.to_string())
}

#[cfg(test)]
pub(super) async fn fetch_tileset_json(
    client: &reqwest::Client,
    url_policy: &mmpf_mln_filesource::policy::ResourceUrlPolicy,
    style_id: &StyleId,
    tileset_url: &str,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    fetch_tileset_json_with_auth(
        client,
        url_policy,
        style_id,
        tileset_url,
        None,
        None,
        deadline,
    )
    .await
}

pub(super) async fn fetch_tileset_json_with_auth(
    client: &reqwest::Client,
    url_policy: &mmpf_mln_filesource::policy::ResourceUrlPolicy,
    style_id: &StyleId,
    tileset_url: &str,
    provider_token: Option<&ProviderBearerToken>,
    auth_provider_origin: Option<&url::Url>,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    let safe_input = redacted_url_str(tileset_url);
    let mut url = url::Url::parse(tileset_url).map_err(|err| {
        ProfileFetchError::permanent_invalid(
            style_id,
            format!("tileset URL parse failed for {safe_input}: {err}"),
        )
    })?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(ProfileFetchError::permanent_invalid(
            style_id,
            format!("unsupported tileset URL scheme: {}", url.scheme()),
        ));
    }
    if !url_policy.permits_url_without_dns(&url) {
        return Err(ProfileFetchError::permanent_invalid(
            style_id,
            format!("blocked tileset URL destination: {safe_input}"),
        ));
    }
    attach_provider_token(
        style_id,
        &mut url,
        provider_token,
        auth_provider_origin,
        "tileset",
    )?;
    let safe_url = redacted_url(&url);
    let response = tokio::time::timeout_at(deadline, client.get(url.clone()).send())
        .await
        .map_err(|_| ProfileFetchError::caller_deadline())?
        .map_err(|err| {
            let error_kind = reqwest_error_label(&err);
            tracing::debug!(
                style_id = style_id.as_str(),
                resource_url = safe_url,
                error_kind,
                "TileJSON request failed"
            );
            ProfileFetchError::transient_load(
                style_id,
                format!("tileset GET failed for {safe_url} ({error_kind})"),
            )
        })?;
    let status = response.status();
    if !status.is_success() {
        tracing::debug!(
            style_id = style_id.as_str(),
            resource_url = safe_url,
            %status,
            "TileJSON provider returned a non-success status"
        );
        let error = style_load_failed(
            style_id,
            format!("tileset GET failed for {safe_url}: HTTP status code {status}"),
        );
        return Err(if is_permanent_profile_http_status(status) {
            ProfileFetchError::permanent(error)
        } else {
            ProfileFetchError::transient(error)
        });
    }
    let bytes = read_bounded_body(response, MAX_TILESET_JSON_BYTES, deadline)
        .await
        .map_err(|err| match err {
            BodyReadError::Timeout => ProfileFetchError::caller_deadline(),
            BodyReadError::Transport(_) => ProfileFetchError::transient_load(
                style_id,
                format!("tileset body read failed for {safe_url}: {err}"),
            ),
            BodyReadError::TooLarge { .. } => {
                ProfileFetchError::permanent_invalid(style_id, err.to_string())
            }
        })?;
    let json = String::from_utf8(bytes).map_err(|err| {
        ProfileFetchError::permanent_invalid(style_id, format!("tileset JSON is not UTF-8: {err}"))
    })?;
    validate_tileset_json(style_id, &json)?;
    Ok(json)
}

fn validate_tileset_json(style_id: &StyleId, json: &str) -> Result<(), ProfileFetchError> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|err| {
        ProfileFetchError::permanent_invalid(style_id, format!("tileset JSON parse failed: {err}"))
    })?;
    let tiles = value
        .as_object()
        .and_then(|object| object.get("tiles"))
        .and_then(serde_json::Value::as_array)
        .filter(|tiles| !tiles.is_empty())
        .ok_or_else(|| {
            ProfileFetchError::permanent_invalid(
                style_id,
                "tileset JSON must contain a non-empty `tiles` array",
            )
        })?;
    if tiles.iter().any(|tile| !tile.is_string()) {
        return Err(ProfileFetchError::permanent_invalid(
            style_id,
            "tileset JSON contains a non-string tile URL",
        ));
    }
    Ok(())
}

pub(super) fn rewrite_tileset_source_json(
    style_id: &StyleId,
    source: &AddLayerSource,
    tileset_url: &str,
    tilejson: &str,
) -> Result<String, ProfilePreparationError> {
    let original: serde_json::Value = serde_json::from_str(&source.json).map_err(|err| {
        style_load_failed(
            style_id,
            format!("addlayer source JSON parse failed: {err}"),
        )
    })?;
    let original = original
        .as_object()
        .ok_or_else(|| style_load_failed(style_id, "addlayer source JSON must be an object"))?;
    let tilejson_value: serde_json::Value = serde_json::from_str(tilejson).map_err(|err| {
        style_load_failed(
            style_id,
            format!("tileset JSON parse failed for {}: {err}", source.tileset_id),
        )
    })?;
    let tilejson_obj = tilejson_value.as_object().ok_or_else(|| {
        style_load_failed(
            style_id,
            format!("tileset JSON for {} must be an object", source.tileset_id),
        )
    })?;
    let base = url::Url::parse(tileset_url).map_err(|err| {
        style_load_failed(
            style_id,
            format!(
                "tileset URL parse failed for {}: {err}",
                redacted_url_str(tileset_url)
            ),
        )
    })?;
    let tile_urls = tilejson_obj
        .get("tiles")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            style_load_failed(
                style_id,
                format!("tileset JSON for {} is missing `tiles`", source.tileset_id),
            )
        })?;
    if tile_urls.is_empty() {
        return Err(style_load_failed(
            style_id,
            format!("tileset JSON for {} has no tile URLs", source.tileset_id),
        ));
    }
    let mut tiles = Vec::with_capacity(tile_urls.len());
    for tile in tile_urls {
        let tile = tile.as_str().ok_or_else(|| {
            style_load_failed(
                style_id,
                format!(
                    "tileset JSON for {} has non-string tile URL",
                    source.tileset_id
                ),
            )
        })?;
        let resolved = resolve_tile_url(style_id, &base, tile)?;
        tiles.push(serde_json::Value::String(resolved));
    }

    let mut resolved = serde_json::Map::new();
    resolved.insert("type".to_string(), serde_json::json!("vector"));
    resolved.insert("tiles".to_string(), serde_json::Value::Array(tiles));
    for key in ["minzoom", "maxzoom", "attribution", "bounds", "scheme"] {
        if let Some(value) = tilejson_obj.get(key) {
            resolved.insert(key.to_string(), value.clone());
        }
    }
    for key in ["minzoom", "maxzoom", "attribution", "bounds", "scheme"] {
        if let Some(value) = original.get(key) {
            resolved.insert(key.to_string(), value.clone());
        }
    }
    serde_json::to_string(&serde_json::Value::Object(resolved)).map_err(|err| {
        style_load_failed(
            style_id,
            format!("tileset source JSON serialize failed: {err}"),
        )
    })
}

pub(super) fn resolve_tile_url(
    style_id: &StyleId,
    base: &url::Url,
    tile: &str,
) -> Result<String, ProfilePreparationError> {
    let protected_tile = protect_tile_template_placeholders(tile);
    let url = match url::Url::parse(&protected_tile) {
        Ok(url) => url,
        Err(_) => base.join(&protected_tile).map_err(|err| {
            style_load_failed(style_id, format!("relative tile URL resolve failed: {err}"))
        })?,
    };
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(style_load_failed(
            style_id,
            format!("unsupported tile URL scheme: {}", url.scheme()),
        ));
    }
    Ok(unprotect_tile_template_placeholders(url.as_str()))
}

const TILE_Z_PLACEHOLDER: &str = "__BIEI_TILE_Z__";
const TILE_X_PLACEHOLDER: &str = "__BIEI_TILE_X__";
const TILE_Y_PLACEHOLDER: &str = "__BIEI_TILE_Y__";

fn protect_tile_template_placeholders(tile: &str) -> String {
    tile.replace("{z}", TILE_Z_PLACEHOLDER)
        .replace("{x}", TILE_X_PLACEHOLDER)
        .replace("{y}", TILE_Y_PLACEHOLDER)
}

fn unprotect_tile_template_placeholders(url: &str) -> String {
    url.replace(TILE_Z_PLACEHOLDER, "{z}")
        .replace(TILE_X_PLACEHOLDER, "{x}")
        .replace(TILE_Y_PLACEHOLDER, "{y}")
}

#[cfg(test)]
pub(super) async fn fetch_style_json(
    client: &reqwest::Client,
    url_policy: &mmpf_mln_filesource::policy::ResourceUrlPolicy,
    style_id: &StyleId,
    style_url: &str,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    fetch_style_json_with_auth(
        client, url_policy, style_id, style_url, None, None, deadline,
    )
    .await
}

pub(super) async fn fetch_style_json_with_auth(
    client: &reqwest::Client,
    url_policy: &mmpf_mln_filesource::policy::ResourceUrlPolicy,
    style_id: &StyleId,
    style_url: &str,
    provider_token: Option<&ProviderBearerToken>,
    auth_provider_origin: Option<&url::Url>,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    let json = match url::Url::parse(style_url) {
        Ok(mut url) if url.scheme() == "http" || url.scheme() == "https" => {
            attach_provider_token(
                style_id,
                &mut url,
                provider_token,
                auth_provider_origin,
                "style",
            )?;
            fetch_http_style_json(client, url_policy, style_id, url, deadline).await?
        }
        Ok(url) if url.scheme() == "file" => {
            let path = url.to_file_path().map_err(|_| {
                ProfileFetchError::permanent_invalid(
                    style_id,
                    format!("style file URL is not a local path: {style_url}"),
                )
            })?;
            read_style_json_file(style_id, &path, deadline).await?
        }
        Ok(url) => {
            return Err(ProfileFetchError::permanent_invalid(
                style_id,
                format!("unsupported style URL scheme: {}", url.scheme()),
            ));
        }
        Err(_) => read_style_json_file(style_id, std::path::Path::new(style_url), deadline).await?,
    };

    // TODO: this keeps error taxonomy under biei's control, but MapLibre
    // Native parses the same JSON again in load_style_from_json. Revisit if
    // cold profile setup cost becomes visible in production profiles.
    serde_json::from_str::<serde_json::Value>(&json).map_err(|err| {
        ProfileFetchError::permanent_invalid(style_id, format!("style JSON parse failed: {err}"))
    })?;
    Ok(json)
}

fn attach_provider_token(
    style_id: &StyleId,
    url: &mut url::Url,
    provider_token: Option<&ProviderBearerToken>,
    auth_provider_origin: Option<&url::Url>,
    resource: &str,
) -> Result<(), ProfileFetchError> {
    let (Some(provider_token), Some(auth_provider_origin)) = (provider_token, auth_provider_origin)
    else {
        return Ok(());
    };
    if url.origin() != auth_provider_origin.origin() {
        return Ok(());
    }
    if url.query_pairs().any(|(key, _)| key == "access_token") {
        return Err(ProfileFetchError::permanent_invalid(
            style_id,
            format!(
                "{resource} provider URL must not contain access_token when delivery auth forwarding is enabled"
            ),
        ));
    }
    url.query_pairs_mut()
        .append_pair("access_token", provider_token.as_str());
    Ok(())
}

async fn fetch_http_style_json(
    client: &reqwest::Client,
    url_policy: &mmpf_mln_filesource::policy::ResourceUrlPolicy,
    style_id: &biei_core::types::StyleId,
    style_url: url::Url,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    let safe_url = redacted_url(&style_url);
    if !url_policy.permits_url_without_dns(&style_url) {
        return Err(ProfileFetchError::permanent_invalid(
            style_id,
            format!("blocked style URL destination: {safe_url}"),
        ));
    }
    let response = tokio::time::timeout_at(deadline, client.get(style_url.clone()).send())
        .await
        .map_err(|_| ProfileFetchError::caller_deadline())?
        .map_err(|err| {
            // Connection/DNS/send failure: the upstream may come back at once.
            let error_kind = reqwest_error_label(&err);
            tracing::debug!(
                style_id = style_id.as_str(),
                resource_url = safe_url,
                error_kind,
                "style request failed"
            );
            ProfileFetchError::transient_load(
                style_id,
                format!("style GET failed for {safe_url} ({error_kind})"),
            )
        })?;

    let status = response.status();
    if !status.is_success() {
        tracing::debug!(
            style_id = style_id.as_str(),
            resource_url = safe_url,
            %status,
            "style provider returned a non-success status"
        );
        let err = style_load_failed(
            style_id,
            format!("style GET failed for {safe_url}: HTTP status code {status}"),
        );
        // Most 4xx responses are deterministic for this URL and may absorb a
        // short burst. 408 and 429 explicitly describe transient conditions
        // and must not poison the profile negative cache.
        return Err(if is_permanent_profile_http_status(status) {
            ProfileFetchError::permanent(err)
        } else {
            ProfileFetchError::transient(err)
        });
    }
    let bytes = read_bounded_body(response, MAX_STYLE_JSON_BYTES, deadline)
        .await
        .map_err(|err| match err {
            BodyReadError::Timeout => ProfileFetchError::caller_deadline(),
            BodyReadError::Transport(_) => ProfileFetchError::transient_load(
                style_id,
                format!("style body read failed for {safe_url}: {err}"),
            ),
            BodyReadError::TooLarge { .. } => {
                ProfileFetchError::permanent_invalid(style_id, err.to_string())
            }
        })?;

    String::from_utf8(bytes).map_err(|err| {
        ProfileFetchError::permanent_invalid(style_id, format!("style JSON is not UTF-8: {err}"))
    })
}

async fn read_style_json_file(
    style_id: &biei_core::types::StyleId,
    path: &std::path::Path,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    use tokio::io::AsyncReadExt;

    let file = tokio::time::timeout_at(deadline, tokio::fs::File::open(path))
        .await
        .map_err(|_| ProfileFetchError::caller_deadline())?
        .map_err(|err| {
            ProfileFetchError::transient_load(
                style_id,
                format!("style file open failed for {}: {err}", path.display()),
            )
        })?;
    let metadata = tokio::time::timeout_at(deadline, file.metadata())
        .await
        .map_err(|_| ProfileFetchError::caller_deadline())?
        .map_err(|err| {
            ProfileFetchError::transient_load(
                style_id,
                format!("style file metadata failed for {}: {err}", path.display()),
            )
        })?;
    if !metadata.is_file() {
        return Err(ProfileFetchError::permanent_invalid(
            style_id,
            format!("style path is not a file: {}", path.display()),
        ));
    }

    // Read at most `MAX_STYLE_JSON_BYTES + 1` from the *same* handle, so a file
    // swapped or grown between the metadata inspection and the read cannot bypass
    // the size bound or force an unbounded allocation.
    let mut bytes = Vec::new();
    tokio::time::timeout_at(
        deadline,
        file.take(MAX_STYLE_JSON_BYTES as u64 + 1)
            .read_to_end(&mut bytes),
    )
    .await
    .map_err(|_| ProfileFetchError::caller_deadline())?
    .map_err(|err| {
        ProfileFetchError::transient_load(
            style_id,
            format!("style file read failed for {}: {err}", path.display()),
        )
    })?;
    if bytes.len() > MAX_STYLE_JSON_BYTES {
        return Err(ProfileFetchError::permanent_invalid(
            style_id,
            format!("style JSON exceeds {MAX_STYLE_JSON_BYTES} bytes"),
        ));
    }

    String::from_utf8(bytes).map_err(|err| {
        ProfileFetchError::permanent_invalid(style_id, format!("style JSON is not UTF-8: {err}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use biei_core::types::StyleId;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::Instant;

    fn temp_style_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "biei_style_read_{}_{}_{tag}.json",
            std::process::id(),
            line!()
        ))
    }

    #[test]
    fn provider_token_is_attached_only_to_the_exact_configured_origin() {
        let style_id = StyleId("test/style".to_string());
        let token = ProviderBearerToken::try_new("public.a+b&c".to_string()).unwrap();
        let origin = url::Url::parse("https://ishikari.test:8443").unwrap();

        let mut same_origin =
            url::Url::parse("https://ishikari.test:8443/styles/test/style.json?encoding=mvt")
                .unwrap();
        attach_provider_token(
            &style_id,
            &mut same_origin,
            Some(&token),
            Some(&origin),
            "style",
        )
        .unwrap_or_else(|error| panic!("same-origin token attachment failed: {}", error.error()));
        assert_eq!(
            same_origin.as_str(),
            "https://ishikari.test:8443/styles/test/style.json?encoding=mvt&access_token=public.a%2Bb%26c"
        );

        let mut other_port =
            url::Url::parse("https://ishikari.test/styles/test/style.json").unwrap();
        attach_provider_token(
            &style_id,
            &mut other_port,
            Some(&token),
            Some(&origin),
            "style",
        )
        .unwrap_or_else(|error| panic!("other-port URL handling failed: {}", error.error()));
        assert!(
            other_port.query().is_none(),
            "host equality alone must not authorize a different origin port"
        );

        let mut other_host =
            url::Url::parse("https://styles.example/styles/test/style.json").unwrap();
        attach_provider_token(
            &style_id,
            &mut other_host,
            Some(&token),
            Some(&origin),
            "style",
        )
        .unwrap_or_else(|error| panic!("other-host URL handling failed: {}", error.error()));
        assert!(
            other_host.query().is_none(),
            "a configured style template must not automatically become an auth target"
        );
    }

    #[test]
    fn provider_token_rejects_a_preexisting_url_credential() {
        let style_id = StyleId("test/style".to_string());
        let token = ProviderBearerToken::try_new("public.new-secret".to_string()).unwrap();
        let origin = url::Url::parse("https://ishikari.test").unwrap();
        let mut url =
            url::Url::parse("https://ishikari.test/style.json?access_token=old-secret").unwrap();

        let error =
            attach_provider_token(&style_id, &mut url, Some(&token), Some(&origin), "style")
                .expect_err("ambiguous provider credentials must be rejected");
        let message = error.error().to_string();
        assert!(!message.contains("new-secret"));
        assert!(!message.contains("old-secret"));
    }

    #[tokio::test]
    async fn profile_request_sends_provider_token_on_the_wire() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind profile fixture");
        let address = listener.local_addr().expect("profile fixture address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept profile request");
            let mut request = vec![0_u8; 4096];
            let read = stream
                .read(&mut request)
                .await
                .expect("read profile request");
            let request_line = String::from_utf8_lossy(&request[..read])
                .lines()
                .next()
                .expect("HTTP request line")
                .to_string();

            let body = r#"{"version":8,"sources":{},"layers":[]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write profile response");
            request_line
        });

        let origin = url::Url::parse(&format!("http://{address}/")).expect("fixture origin");
        let style_url = origin
            .join("styles/test/style.json?encoding=mvt")
            .expect("style URL");
        let policy =
            mmpf_mln_filesource::policy::ResourceUrlPolicy::new(vec![address.ip().to_string()]);
        let client = mmpf_mln_filesource::build_profile_http_client(
            policy.clone(),
            "biei-profile-auth-test",
        )
        .expect("profile HTTP client");
        let style_id = StyleId("test/style".to_string());
        let token = ProviderBearerToken::try_new("public.a+b&c".to_string()).unwrap();

        let json = fetch_style_json_with_auth(
            &client,
            &policy,
            &style_id,
            style_url.as_str(),
            Some(&token),
            Some(&origin),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap_or_else(|error| panic!("authenticated style fetch failed: {}", error.error()));
        assert!(json.contains("\"version\":8"));
        assert_eq!(
            server.await.expect("profile fixture task"),
            "GET /styles/test/style.json?encoding=mvt&access_token=public.a%2Bb%26c HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn reads_valid_style_file() {
        let style_id = StyleId("test/style".to_string());
        let path = temp_style_path("valid");
        let contents = r#"{"version":8,"layers":[]}"#;
        tokio::fs::write(&path, contents).await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(30);
        let read = read_style_json_file(&style_id, &path, deadline).await;
        tokio::fs::remove_file(&path).await.ok();

        match read {
            Ok(text) => assert_eq!(text, contents),
            Err(err) => panic!("valid file should read: {}", err.error()),
        }
    }

    #[tokio::test]
    async fn rejects_style_file_exceeding_bound() {
        let style_id = StyleId("test/style".to_string());
        let path = temp_style_path("oversize");
        // One byte over the bound must be rejected without allocating the whole
        // (potentially unbounded) file.
        let oversized = vec![b'a'; MAX_STYLE_JSON_BYTES + 1];
        tokio::fs::write(&path, &oversized).await.unwrap();

        let deadline = Instant::now() + Duration::from_secs(30);
        let err = read_style_json_file(&style_id, &path, deadline)
            .await
            .expect_err("oversize file is rejected");
        tokio::fs::remove_file(&path).await.ok();

        // Oversize is a permanent (negative-cacheable) invalid-style failure.
        assert!(err.is_negative_cacheable());
        assert!(matches!(
            err.error(),
            ProfilePreparationError::InvalidPreparedContent {
                content: biei_core::types::ProfileContent::Style(_),
                ..
            }
        ));
    }
}
