//! `Renderer` implementation backed by the production MapLibre actor.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use moka::sync::Cache;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};

use crate::renderer::actor::{RenderTaskView, RendererActor, RendererActorConfig, ResolvedStyle};
use crate::renderer::{PreparedProfile, ProfilePreparer, Renderer, StyleAvailabilityError};
use crate::style_catalog::StyleCatalog;
use crate::types::{
    AddLayerSource, InternalTask, RenderOutput, RenderRequest, RendererError, SourceHash, StyleId,
    StyleRevision,
};

pub struct MapLibreRenderer {
    actor: RendererActor,
    config: RendererActorConfig,
    retiring: bool,
}

pub struct MapLibreProfilePreparer {
    style_catalog: Arc<StyleCatalog>,
    http_client: reqwest::Client,
    fetch_permits: Arc<tokio::sync::Semaphore>,
    style_json_cache: Cache<StyleRevision, Arc<str>>,
    style_error_cache: Cache<StyleRevision, RendererError>,
    tileset_json_cache: Cache<String, Arc<str>>,
    inflight_style_loads: Mutex<HashMap<StyleRevision, watch::Sender<StyleLoadSignal>>>,
}

#[derive(Clone)]
enum StyleLoadSignal {
    Pending,
    Ready(Arc<str>),
    Failed(StyleFetchError),
    Aborted,
}

enum CacheLookup {
    Load(watch::Sender<StyleLoadSignal>),
    Wait(watch::Receiver<StyleLoadSignal>),
    Negative(RendererError),
}

struct InFlightStyleLoad<'a> {
    key: StyleRevision,
    inflight: &'a Mutex<HashMap<StyleRevision, watch::Sender<StyleLoadSignal>>>,
    tx: watch::Sender<StyleLoadSignal>,
    completed: bool,
}

impl Drop for InFlightStyleLoad<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        lock_unpoisoned(self.inflight).remove(&self.key);
        self.tx.send_replace(StyleLoadSignal::Aborted);
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

const STYLE_JSON_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const STYLE_JSON_CACHE_IDLE_TTL: Duration = Duration::from_secs(60 * 60);
const TILESET_JSON_CACHE_MAX_BYTES: u64 = 32 * 1024 * 1024;
const TILESET_JSON_CACHE_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
const STYLE_JSON_NEGATIVE_CACHE_MAX_ENTRIES: u64 = 4096;
// Short on purpose: the negative cache only needs to absorb repeated requests
// for the same definitively-bad style within a burst (§7.5.1 spray defense).
// A longer TTL would delay a freshly-registered/fixed style from becoming
// servable. Transient failures (5xx, connection/read errors, timeouts) are not
// cached here at all — see `StyleFetchError`.
const STYLE_JSON_NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(5);

impl MapLibreRenderer {
    pub fn spawn(config: RendererActorConfig) -> Result<Self, RendererError> {
        Ok(Self {
            actor: RendererActor::spawn(config.clone())?,
            config,
            retiring: false,
        })
    }

    #[cfg(test)]
    fn from_actor(actor: RendererActor) -> Self {
        Self {
            actor,
            config: RendererActorConfig {
                worker_id: 0,
                ambient_cache_path: None,
            },
            retiring: false,
        }
    }

    pub fn is_alive(&self) -> bool {
        self.actor.is_alive()
    }

    fn actor(&mut self) -> Result<&RendererActor, RendererError> {
        if self.retiring {
            if self.actor.is_alive() {
                return Err(RendererError::ActorDead);
            }
            self.actor = RendererActor::spawn(self.config.clone())?;
            self.retiring = false;
        } else if !self.actor.is_alive() {
            self.actor = RendererActor::spawn(self.config.clone())?;
        }
        Ok(&self.actor)
    }
}

impl MapLibreProfilePreparer {
    pub fn new(style_catalog: Arc<StyleCatalog>, max_concurrent_fetches: usize) -> Self {
        Self {
            style_catalog,
            http_client: reqwest::Client::new(),
            fetch_permits: Arc::new(tokio::sync::Semaphore::new(max_concurrent_fetches.max(1))),
            style_json_cache: style_json_cache(),
            style_error_cache: style_error_cache(),
            tileset_json_cache: tileset_json_cache(),
            inflight_style_loads: Mutex::new(HashMap::new()),
        }
    }

    async fn resolve_style(
        &self,
        style: &StyleRevision,
        deadline: Instant,
    ) -> Result<PreparedProfile, RendererError> {
        self.resolve_style_fetch(style, deadline)
            .await
            .map_err(|failure| failure.error)
    }

    async fn resolve_style_fetch(
        &self,
        style: &StyleRevision,
        deadline: Instant,
    ) -> Result<PreparedProfile, StyleFetchError> {
        loop {
            match self.lookup_cache(style) {
                Ok(style_json) => {
                    return Ok(PreparedProfile {
                        revision: style.clone(),
                        style_json,
                        addlayer_source: None,
                    });
                }
                Err(CacheLookup::Wait(mut rx)) => {
                    match tokio::time::timeout_at(deadline, wait_for_style_load(&mut rx))
                        .await
                        .map_err(|_| StyleFetchError::transient(RendererError::Timeout))??
                    {
                        Some(style_json) => {
                            return Ok(PreparedProfile {
                                revision: style.clone(),
                                style_json,
                                addlayer_source: None,
                            });
                        }
                        None => continue,
                    }
                }
                Err(CacheLookup::Negative(err)) => return Err(StyleFetchError::permanent(err)),
                Err(CacheLookup::Load(tx)) => {
                    let mut guard = InFlightStyleLoad {
                        key: style.clone(),
                        inflight: &self.inflight_style_loads,
                        tx: tx.clone(),
                        completed: false,
                    };
                    let result = self.fetch_uncached_style(style, deadline).await;
                    self.store_fetch_result(style.clone(), tx, &result);
                    guard.completed = true;
                    return result.map(|style_json| PreparedProfile {
                        revision: style.clone(),
                        style_json,
                        addlayer_source: None,
                    });
                }
            }
        }
    }

    async fn resolve_addlayer_source(
        &self,
        task: &InternalTask,
    ) -> Result<Option<AddLayerSource>, RendererError> {
        let Some(source) = addlayer_source_from_task(task) else {
            return Ok(None);
        };
        let source_json = self
            .resolve_tileset_source_json(&task.style.id, source, task.deadline)
            .await?;
        Ok(Some(AddLayerSource {
            tileset_id: source.tileset_id.clone(),
            json: source_json,
        }))
    }

    async fn resolve_tileset_source_json(
        &self,
        style_id: &StyleId,
        source: &AddLayerSource,
        deadline: Instant,
    ) -> Result<String, RendererError> {
        let tileset_url = source_url_from_addlayer_source(style_id, source)?;
        let tilejson = match self.tileset_json_cache.get(&tileset_url) {
            Some(tilejson) => tilejson,
            None => {
                let _permit = tokio::time::timeout_at(deadline, self.fetch_permits.acquire())
                    .await
                    .map_err(|_| RendererError::Timeout)?
                    .map_err(|_| RendererError::ActorDead)?;
                let fetched = Arc::<str>::from(
                    fetch_tileset_json(&self.http_client, style_id, &tileset_url, deadline).await?,
                );
                self.tileset_json_cache
                    .insert(tileset_url.clone(), fetched.clone());
                fetched
            }
        };
        rewrite_tileset_source_json(style_id, source, &tileset_url, &tilejson)
    }

    fn lookup_cache(&self, revision: &StyleRevision) -> Result<Arc<str>, CacheLookup> {
        if let Some(style_json) = self.style_json_cache.get(revision) {
            return Ok(style_json);
        }
        if let Some(err) = self.style_error_cache.get(revision) {
            return Err(CacheLookup::Negative(err));
        }
        let mut inflight = lock_unpoisoned(&self.inflight_style_loads);
        match inflight.get(revision) {
            Some(tx) => Err(CacheLookup::Wait(tx.subscribe())),
            None => {
                let (tx, _rx) = watch::channel(StyleLoadSignal::Pending);
                inflight.insert(revision.clone(), tx.clone());
                Err(CacheLookup::Load(tx))
            }
        }
    }

    async fn fetch_uncached_style(
        &self,
        style: &StyleRevision,
        deadline: Instant,
    ) -> Result<Arc<str>, StyleFetchError> {
        let _permit = tokio::time::timeout_at(deadline, self.fetch_permits.acquire())
            .await
            .map_err(|_| StyleFetchError::transient(RendererError::Timeout))?
            .map_err(|_| StyleFetchError::transient(RendererError::ActorDead))?;
        let definition = self
            .style_catalog
            .definition_for_revision(style)
            .ok_or_else(|| {
                StyleFetchError::permanent(RendererError::StyleLoadFailed {
                    style_id: style.id.clone(),
                    source: format!(
                        "style definition for version {} is not registered",
                        style.version
                    ),
                })
            })?;
        Ok(Arc::from(
            fetch_style_json(
                &self.http_client,
                &style.id,
                &definition.style_url,
                deadline,
            )
            .await?,
        ))
    }

    fn store_fetch_result(
        &self,
        revision: StyleRevision,
        tx: watch::Sender<StyleLoadSignal>,
        result: &Result<Arc<str>, StyleFetchError>,
    ) {
        match result {
            Ok(style_json) => {
                self.style_json_cache
                    .insert(revision.clone(), style_json.clone());
                lock_unpoisoned(&self.inflight_style_loads).remove(&revision);
                tx.send_replace(StyleLoadSignal::Ready(style_json.clone()));
            }
            Err(failure) => {
                // Only definitive failures are negative-cached; transient ones
                // (5xx, connection/read errors, timeouts) must be retried.
                if failure.negative_cacheable {
                    self.style_error_cache
                        .insert(revision.clone(), failure.error.clone());
                }
                lock_unpoisoned(&self.inflight_style_loads).remove(&revision);
                tx.send_replace(StyleLoadSignal::Failed(failure.clone()));
            }
        }
    }
}

fn style_json_cache() -> Cache<StyleRevision, Arc<str>> {
    Cache::builder()
        .max_capacity(STYLE_JSON_CACHE_MAX_BYTES)
        .time_to_idle(STYLE_JSON_CACHE_IDLE_TTL)
        .weigher(|_key: &StyleRevision, style_json: &Arc<str>| {
            style_json.len().clamp(1, u32::MAX as usize) as u32
        })
        .build()
}

fn tileset_json_cache() -> Cache<String, Arc<str>> {
    Cache::builder()
        .max_capacity(TILESET_JSON_CACHE_MAX_BYTES)
        .time_to_idle(TILESET_JSON_CACHE_IDLE_TTL)
        .weigher(|_key: &String, tilejson: &Arc<str>| {
            tilejson.len().clamp(1, u32::MAX as usize) as u32
        })
        .build()
}

fn style_error_cache() -> Cache<StyleRevision, RendererError> {
    Cache::builder()
        .max_capacity(STYLE_JSON_NEGATIVE_CACHE_MAX_ENTRIES)
        .time_to_live(STYLE_JSON_NEGATIVE_CACHE_TTL)
        .build()
}

async fn wait_for_style_load(
    rx: &mut watch::Receiver<StyleLoadSignal>,
) -> Result<Option<Arc<str>>, StyleFetchError> {
    loop {
        match rx.borrow_and_update().clone() {
            StyleLoadSignal::Pending => {}
            StyleLoadSignal::Ready(style_json) => return Ok(Some(style_json)),
            StyleLoadSignal::Failed(err) => return Err(err),
            StyleLoadSignal::Aborted => return Ok(None),
        }
        rx.changed()
            .await
            .map_err(|_| StyleFetchError::transient(RendererError::ActorDead))?;
    }
}

/// A failed style fetch plus whether it is safe to negative-cache.
///
/// Permanent/content failures (4xx, parse, oversize, bad encoding, unknown
/// style) reproduce on an immediate retry, so caching them briefly is the
/// §7.5.1 spray defense. Transient failures (5xx, connection/read errors,
/// timeouts) may recover at once, so they are never cached — otherwise a
/// one-second upstream blip becomes `STYLE_JSON_NEGATIVE_CACHE_TTL` of forced
/// failures for every request hitting that style.
#[derive(Clone)]
struct StyleFetchError {
    error: RendererError,
    negative_cacheable: bool,
}

impl StyleFetchError {
    fn permanent(error: RendererError) -> Self {
        Self {
            error,
            negative_cacheable: true,
        }
    }

    fn transient(error: RendererError) -> Self {
        Self {
            error,
            negative_cacheable: false,
        }
    }

    fn into_availability_error(self) -> StyleAvailabilityError {
        if self.negative_cacheable {
            StyleAvailabilityError::NotFound(self.error)
        } else {
            StyleAvailabilityError::Unavailable(self.error)
        }
    }
}

#[async_trait]
impl ProfilePreparer for MapLibreProfilePreparer {
    async fn prepare_profile(
        &self,
        task: &InternalTask,
    ) -> Result<Option<PreparedProfile>, RendererError> {
        let mut prepared = self.resolve_style(&task.style, task.deadline).await?;
        prepared.addlayer_source = self.resolve_addlayer_source(task).await?;
        Ok(Some(prepared))
    }

    async fn ensure_style_available(
        &self,
        revision: &StyleRevision,
        deadline: Instant,
    ) -> Result<(), StyleAvailabilityError> {
        // Reuses the cache / single-flight / negative-cache path; the fetched
        // bytes are dropped — we only need to know the provider has the style.
        self.resolve_style_fetch(revision, deadline)
            .await
            .map(|_| ())
            .map_err(StyleFetchError::into_availability_error)
    }
}

#[cfg(test)]
impl MapLibreProfilePreparer {
    fn for_tests(style_catalog: Arc<StyleCatalog>) -> Self {
        Self {
            style_catalog,
            http_client: reqwest::Client::new(),
            fetch_permits: Arc::new(tokio::sync::Semaphore::new(16)),
            style_json_cache: style_json_cache(),
            style_error_cache: style_error_cache(),
            tileset_json_cache: tileset_json_cache(),
            inflight_style_loads: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Renderer for MapLibreRenderer {
    async fn setup_profile(
        &mut self,
        task: &InternalTask,
        prepared: Option<PreparedProfile>,
    ) -> Result<(), RendererError> {
        let prepared = prepared
            .filter(|prepared| prepared.revision == task.style)
            .ok_or_else(|| RendererError::StyleLoadFailed {
                style_id: task.style.id.clone(),
                source: "prepared style JSON is missing or stale".to_string(),
            })?;
        self.actor()?
            .load_profile(
                ResolvedStyle {
                    revision: prepared.revision,
                    style_json: prepared.style_json,
                },
                RenderTaskView::from(task),
            )
            .await
    }

    async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
        // Production v0 relies on maplibre-native's default resource loader.
        Ok(())
    }

    async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError> {
        self.actor()?.render(RenderTaskView::from(task)).await
    }

    fn retire_after_current(&mut self) {
        self.retiring = true;
        self.actor.retire_after_current();
    }
}

const MAX_STYLE_JSON_BYTES: usize = 2 * 1024 * 1024;
const MAX_TILESET_JSON_BYTES: usize = 1024 * 1024;

fn addlayer_source_from_task(task: &InternalTask) -> Option<&AddLayerSource> {
    match &task.request {
        RenderRequest::StaticImage {
            addlayer: Some(addlayer),
            ..
        } => addlayer.source.as_ref(),
        _ => None,
    }
}

fn source_url_from_addlayer_source(
    style_id: &StyleId,
    source: &AddLayerSource,
) -> Result<String, RendererError> {
    let value: serde_json::Value =
        serde_json::from_str(&source.json).map_err(|err| RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("addlayer source JSON parse failed: {err}"),
        })?;
    let url = value
        .as_object()
        .and_then(|obj| obj.get("url"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: "addlayer source JSON is missing TileJSON URL".to_string(),
        })?;
    Ok(url.to_string())
}

async fn fetch_tileset_json(
    client: &reqwest::Client,
    style_id: &StyleId,
    tileset_url: &str,
    deadline: Instant,
) -> Result<String, RendererError> {
    let url = url::Url::parse(tileset_url).map_err(|err| RendererError::StyleLoadFailed {
        style_id: style_id.clone(),
        source: format!("tileset URL parse failed for {tileset_url}: {err}"),
    })?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("unsupported tileset URL scheme: {}", url.scheme()),
        });
    }
    let response = tokio::time::timeout_at(deadline, client.get(url.clone()).send())
        .await
        .map_err(|_| RendererError::Timeout)?
        .map_err(|err| RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset GET failed for {url}: {err}"),
        })?;
    let status = response.status();
    if !status.is_success() {
        return Err(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset GET failed for {url}: HTTP status code {status}"),
        });
    }
    if response
        .content_length()
        .is_some_and(|len| len > MAX_TILESET_JSON_BYTES as u64)
    {
        return Err(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset JSON exceeds {MAX_TILESET_JSON_BYTES} bytes"),
        });
    }
    let bytes = tokio::time::timeout_at(deadline, response.bytes())
        .await
        .map_err(|_| RendererError::Timeout)?
        .map_err(|err| RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset body read failed for {url}: {err}"),
        })?;
    if bytes.len() > MAX_TILESET_JSON_BYTES {
        return Err(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset JSON exceeds {MAX_TILESET_JSON_BYTES} bytes"),
        });
    }
    String::from_utf8(bytes.to_vec()).map_err(|err| RendererError::StyleLoadFailed {
        style_id: style_id.clone(),
        source: format!("tileset JSON is not UTF-8: {err}"),
    })
}

fn rewrite_tileset_source_json(
    style_id: &StyleId,
    source: &AddLayerSource,
    tileset_url: &str,
    tilejson: &str,
) -> Result<String, RendererError> {
    let original: serde_json::Value =
        serde_json::from_str(&source.json).map_err(|err| RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("addlayer source JSON parse failed: {err}"),
        })?;
    let original = original
        .as_object()
        .ok_or_else(|| RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: "addlayer source JSON must be an object".to_string(),
        })?;
    let tilejson_value: serde_json::Value =
        serde_json::from_str(tilejson).map_err(|err| RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset JSON parse failed for {}: {err}", source.tileset_id),
        })?;
    let tilejson_obj =
        tilejson_value
            .as_object()
            .ok_or_else(|| RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("tileset JSON for {} must be an object", source.tileset_id),
            })?;
    let base = url::Url::parse(tileset_url).map_err(|err| RendererError::StyleLoadFailed {
        style_id: style_id.clone(),
        source: format!("tileset URL parse failed for {tileset_url}: {err}"),
    })?;
    let tile_urls = tilejson_obj
        .get("tiles")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset JSON for {} is missing `tiles`", source.tileset_id),
        })?;
    if tile_urls.is_empty() {
        return Err(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset JSON for {} has no tile URLs", source.tileset_id),
        });
    }
    let mut tiles = Vec::with_capacity(tile_urls.len());
    for tile in tile_urls {
        let tile = tile
            .as_str()
            .ok_or_else(|| RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!(
                    "tileset JSON for {} has non-string tile URL",
                    source.tileset_id
                ),
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
        RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset source JSON serialize failed: {err}"),
        }
    })
}

fn resolve_tile_url(
    style_id: &StyleId,
    base: &url::Url,
    tile: &str,
) -> Result<String, RendererError> {
    let protected_tile = protect_tile_template_placeholders(tile);
    let url = match url::Url::parse(&protected_tile) {
        Ok(url) => url,
        Err(_) => base
            .join(&protected_tile)
            .map_err(|err| RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("relative tile URL resolve failed for `{tile}`: {err}"),
            })?,
    };
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("unsupported tile URL scheme: {}", url.scheme()),
        });
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

async fn fetch_style_json(
    client: &reqwest::Client,
    style_id: &StyleId,
    style_url: &str,
    deadline: Instant,
) -> Result<String, StyleFetchError> {
    let json = match url::Url::parse(style_url) {
        Ok(url) if url.scheme() == "http" || url.scheme() == "https" => {
            fetch_http_style_json(client, style_id, url, deadline).await?
        }
        Ok(url) if url.scheme() == "file" => {
            let path = url.to_file_path().map_err(|_| {
                StyleFetchError::permanent(RendererError::StyleLoadFailed {
                    style_id: style_id.clone(),
                    source: format!("style file URL is not a local path: {style_url}"),
                })
            })?;
            read_style_json_file(style_id, &path, deadline).await?
        }
        Ok(url) => {
            return Err(StyleFetchError::permanent(RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("unsupported style URL scheme: {}", url.scheme()),
            }));
        }
        Err(_) => read_style_json_file(style_id, std::path::Path::new(style_url), deadline).await?,
    };

    // TODO: this keeps error taxonomy under biei's control, but MapLibre
    // Native parses the same JSON again in load_style_from_json. Revisit if
    // cold profile setup cost becomes visible in production profiles.
    serde_json::from_str::<serde_json::Value>(&json).map_err(|err| {
        StyleFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style JSON parse failed: {err}"),
        })
    })?;
    Ok(json)
}

async fn fetch_http_style_json(
    client: &reqwest::Client,
    style_id: &crate::types::StyleId,
    style_url: url::Url,
    deadline: Instant,
) -> Result<String, StyleFetchError> {
    let response = tokio::time::timeout_at(deadline, client.get(style_url.clone()).send())
        .await
        .map_err(|_| StyleFetchError::transient(RendererError::Timeout))?
        .map_err(|err| {
            // Connection/DNS/send failure: the upstream may come back at once.
            StyleFetchError::transient(RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("style GET failed for {style_url}: {err}"),
            })
        })?;

    let status = response.status();
    if !status.is_success() {
        let err = RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style GET failed for {style_url}: HTTP status code {status}"),
        };
        // 4xx means the style is definitively unservable (404/410/...); 5xx and
        // anything else non-success is treated as a transient upstream problem.
        return Err(if status.is_client_error() {
            StyleFetchError::permanent(err)
        } else {
            StyleFetchError::transient(err)
        });
    }
    if response
        .content_length()
        .is_some_and(|len| len > MAX_STYLE_JSON_BYTES as u64)
    {
        return Err(StyleFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style JSON exceeds {MAX_STYLE_JSON_BYTES} bytes"),
        }));
    }

    let bytes = tokio::time::timeout_at(deadline, response.bytes())
        .await
        .map_err(|_| StyleFetchError::transient(RendererError::Timeout))?
        .map_err(|err| {
            StyleFetchError::transient(RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("style body read failed for {style_url}: {err}"),
            })
        })?;

    if bytes.len() > MAX_STYLE_JSON_BYTES {
        return Err(StyleFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style JSON exceeds {MAX_STYLE_JSON_BYTES} bytes"),
        }));
    }

    String::from_utf8(bytes.to_vec()).map_err(|err| {
        StyleFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style JSON is not UTF-8: {err}"),
        })
    })
}

async fn read_style_json_file(
    style_id: &crate::types::StyleId,
    path: &std::path::Path,
    deadline: Instant,
) -> Result<String, StyleFetchError> {
    let metadata = tokio::time::timeout_at(deadline, tokio::fs::metadata(path))
        .await
        .map_err(|_| StyleFetchError::transient(RendererError::Timeout))?
        .map_err(|err| {
            StyleFetchError::transient(RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("style file metadata failed for {}: {err}", path.display()),
            })
        })?;
    if !metadata.is_file() {
        return Err(StyleFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style path is not a file: {}", path.display()),
        }));
    }
    if metadata.len() > MAX_STYLE_JSON_BYTES as u64 {
        return Err(StyleFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style JSON exceeds {MAX_STYLE_JSON_BYTES} bytes"),
        }));
    }

    tokio::time::timeout_at(deadline, tokio::fs::read_to_string(path))
        .await
        .map_err(|_| StyleFetchError::transient(RendererError::Timeout))?
        .map_err(|err| {
            StyleFetchError::transient(RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("style file read failed for {}: {err}", path.display()),
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::renderer::actor::BlockingRenderBackend;
    use crate::style_catalog::StyleDefinition;
    use crate::types::{
        AddLayer, AddLayerSource, ImageFormat, PixelRatio, Positioning, RenderRequest, StyleId,
        StyleRevision,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::Instant;

    struct FakeBackend;

    impl BlockingRenderBackend for FakeBackend {
        fn load_profile(
            &mut self,
            style: &ResolvedStyle,
            _task: &RenderTaskView,
        ) -> Result<(), RendererError> {
            if !style.style_json.contains("\"version\"") {
                return Err(RendererError::StyleLoadFailed {
                    style_id: style.revision.id.clone(),
                    source: "style JSON was not fetched".to_string(),
                });
            }
            Ok(())
        }

        fn render(&mut self, task: &RenderTaskView) -> Result<RenderOutput, RendererError> {
            Ok(RenderOutput {
                bytes: bytes::Bytes::copy_from_slice(task.style.id.as_bytes()),
                format: task.output_format,
            })
        }
    }

    fn revision() -> StyleRevision {
        StyleRevision {
            id: StyleId("carto/voyager".to_string()),
            version: 1,
        }
    }

    fn internal_task(style: StyleRevision) -> InternalTask {
        let now = Instant::now();
        InternalTask {
            id: 9,
            request_id: crate::types::RequestId::from_string("maplibre-test"),
            style,
            source: None,
            request: RenderRequest::StaticImage {
                positioning: Positioning::Center {
                    lon: 139.767,
                    lat: 35.681,
                    zoom: 12.0,
                    bearing: 0.0,
                    pitch: 0.0,
                },
                width: 512,
                height: 512,
                overlays: Vec::new(),
                before_layer: None,
                padding: crate::types::Padding::default(),
                addlayer: None,
            },
            pixel_ratio: PixelRatio::X1,
            output_format: ImageFormat::Webp,
            arrived_at: now,
            deadline: now + std::time::Duration::from_secs(1),
            forwarding_hops: 0,
        }
    }

    fn attach_addlayer_source(task: &mut InternalTask, tileset_url: String) {
        if let RenderRequest::StaticImage { addlayer, .. } = &mut task.request {
            *addlayer = Some(AddLayer {
                json: r#"{"id":"rain","type":"fill","source":{"type":"vector","url":"rain"},"source-layer":"layer"}"#.to_string(),
                hash: 1,
                source: Some(AddLayerSource {
                    tileset_id: "rain".to_string(),
                    json: format!(r#"{{"type":"vector","url":"{tileset_url}"}}"#),
                }),
            });
        }
    }

    #[test]
    fn tile_template_resolution_preserves_only_tile_placeholders() {
        let base = url::Url::parse("https://tiles.example.test/a/b/tileset.json").unwrap();
        let resolved = resolve_tile_url(
            &StyleId("style".to_string()),
            &base,
            "tiles/{z}/{x}/{y}%20a.pbf",
        )
        .expect("tile template resolves");

        assert_eq!(
            resolved,
            "https://tiles.example.test/a/b/tiles/{z}/{x}/{y}%20a.pbf"
        );
    }

    fn write_test_style_json(name: &str, body: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "biei-maplibre-test-{name}-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after unix epoch")
                .as_nanos()
        ));
        std::fs::write(&path, body).expect("test style JSON is written");
        path.to_string_lossy().into_owned()
    }

    async fn spawn_counting_style_server(
        status: axum::http::StatusCode,
        body: &'static str,
        delay: std::time::Duration,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let count = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server binds");
        let addr = listener.local_addr().expect("test server has local addr");
        let server_count = count.clone();
        let server = tokio::spawn(async move {
            let app = axum::Router::new().fallback(move || {
                let server_count = server_count.clone();
                async move {
                    server_count.fetch_add(1, Ordering::SeqCst);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    (status, body)
                }
            });
            axum::serve(listener, app).await.expect("test server runs");
        });
        (format!("http://{addr}/style.json"), count, server)
    }

    #[tokio::test]
    async fn renderer_proxies_trait_calls_to_actor() {
        let actor = RendererActor::spawn_with_backend(
            RendererActorConfig {
                worker_id: 8,
                ambient_cache_path: None,
            },
            FakeBackend,
        )
        .expect("actor spawns");
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(
            rev.id.clone(),
            StyleDefinition::new(
                write_test_style_json("valid", r#"{"version":8,"sources":{},"layers":[]}"#),
                rev.version,
            ),
        );
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let mut renderer = MapLibreRenderer::from_actor(actor);

        let task = internal_task(rev);
        let prepared = preparer
            .prepare_profile(&task)
            .await
            .expect("profile prepares");
        renderer
            .setup_profile(&task, prepared)
            .await
            .expect("profile loads");
        renderer.ensure_source(42).await.expect("source no-op");
        let output = renderer.render(&task).await.expect("render succeeds");

        assert_eq!(output.bytes.as_ref(), b"carto/voyager");
        assert_eq!(output.format, ImageFormat::Webp);
        assert!(renderer.is_alive());
    }

    #[tokio::test]
    async fn renderer_requires_prepared_style() {
        let actor = RendererActor::spawn_with_backend(
            RendererActorConfig {
                worker_id: 9,
                ambient_cache_path: None,
            },
            FakeBackend,
        )
        .expect("actor spawns");
        let mut renderer = MapLibreRenderer::from_actor(actor);
        let task = internal_task(revision());
        let err = renderer
            .setup_profile(&task, None)
            .await
            .expect_err("prepared style is required");

        assert!(matches!(err, RendererError::StyleLoadFailed { .. }));
    }

    #[tokio::test]
    async fn profile_preparer_caches_successful_style_json() {
        let (style_url, request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"version":8,"sources":{},"layers":[]}"#,
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let task = internal_task(rev);

        let first = preparer
            .prepare_profile(&task)
            .await
            .expect("first profile prepares")
            .expect("maplibre returns prepared profile");
        let second = preparer
            .prepare_profile(&task)
            .await
            .expect("second profile prepares")
            .expect("maplibre returns prepared profile");

        server.abort();
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&first.style_json, &second.style_json));
    }

    #[tokio::test]
    async fn profile_preparer_resolves_addlayer_tileset_before_worker() {
        let (style_url, _style_request_count, style_server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"version":8,"sources":{},"layers":[]}"#,
            std::time::Duration::ZERO,
        )
        .await;
        let (tileset_url, tileset_request_count, tileset_server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"tiles":["tiles/{z}/{x}/{y}.pbf"],"minzoom":1,"maxzoom":10}"#,
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let mut task = internal_task(rev);
        attach_addlayer_source(&mut task, tileset_url.clone());

        let first = preparer
            .prepare_profile(&task)
            .await
            .expect("profile prepares")
            .expect("prepared profile");
        let second = preparer
            .prepare_profile(&task)
            .await
            .expect("second profile prepares")
            .expect("second prepared profile");

        style_server.abort();
        tileset_server.abort();
        assert_eq!(tileset_request_count.load(Ordering::SeqCst), 1);

        let source = first
            .addlayer_source
            .expect("addlayer source is prepared before worker");
        let value: serde_json::Value =
            serde_json::from_str(&source.json).expect("prepared source JSON parses");
        assert!(
            value.get("url").is_none(),
            "TileJSON URL is not sent to worker"
        );
        assert_eq!(
            value.get("type").and_then(serde_json::Value::as_str),
            Some("vector")
        );
        assert_eq!(
            value.get("minzoom").and_then(serde_json::Value::as_u64),
            Some(1)
        );
        let tile = value
            .get("tiles")
            .and_then(serde_json::Value::as_array)
            .and_then(|tiles| tiles.first())
            .and_then(serde_json::Value::as_str)
            .expect("tiles array contains absolute tile URL");
        assert!(
            tile.starts_with(tileset_url.trim_end_matches("style.json")),
            "relative tile URL was resolved against TileJSON URL: {tile}"
        );
        assert!(
            tile.ends_with("tiles/{z}/{x}/{y}.pbf"),
            "tile URL template placeholders must remain unescaped: {tile}"
        );
        assert_eq!(second.addlayer_source, Some(source));
    }

    #[tokio::test]
    async fn profile_preparer_coalesces_concurrent_style_fetches() {
        let (style_url, request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"version":8,"sources":{},"layers":[]}"#,
            std::time::Duration::from_millis(50),
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let task = internal_task(rev);

        let (first, second) = tokio::join!(
            preparer.prepare_profile(&task),
            preparer.prepare_profile(&task)
        );

        server.abort();
        assert!(first.expect("first profile prepares").is_some());
        assert!(second.expect("second profile prepares").is_some());
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn profile_preparer_negative_caches_style_load_failures() {
        let (style_url, request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::NOT_FOUND,
            "missing style",
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let task = internal_task(rev);

        let first = preparer.prepare_profile(&task).await;
        let second = preparer.prepare_profile(&task).await;

        server.abort();
        assert!(matches!(first, Err(RendererError::StyleLoadFailed { .. })));
        assert!(matches!(second, Err(RendererError::StyleLoadFailed { .. })));
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fetch_style_json_rejects_http_404_before_actor_load() {
        use axum::http::StatusCode;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server binds");
        let addr = listener.local_addr().expect("test server has local addr");
        let server = tokio::spawn(async move {
            let app =
                axum::Router::new().fallback(|| async { (StatusCode::NOT_FOUND, "missing style") });
            axum::serve(listener, app).await.expect("test server runs");
        });

        let err = fetch_style_json(
            &reqwest::Client::new(),
            &revision().id,
            &format!("http://{addr}/missing-style.json"),
            Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .expect_err("404 is classified before MapLibre load");

        server.abort();
        assert!(matches!(err.error, RendererError::StyleLoadFailed { .. }));
        assert!(err.negative_cacheable, "4xx is definitive and cacheable");
    }

    #[tokio::test]
    async fn profile_preparer_does_not_cache_transient_5xx() {
        let (style_url, request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "upstream down",
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let task = internal_task(rev);

        let first = preparer.prepare_profile(&task).await;
        let second = preparer.prepare_profile(&task).await;

        server.abort();
        assert!(matches!(first, Err(RendererError::StyleLoadFailed { .. })));
        assert!(matches!(second, Err(RendererError::StyleLoadFailed { .. })));
        // 5xx is transient: it must NOT be negative-cached, so the second
        // request re-fetches rather than being served the cached failure.
        assert_eq!(request_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ensure_style_available_maps_404_to_not_found() {
        let (style_url, _request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::NOT_FOUND,
            "missing style",
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);

        let err = preparer
            .ensure_style_available(&rev, Instant::now() + std::time::Duration::from_secs(1))
            .await
            .expect_err("404 is a definitive missing style");

        server.abort();
        assert!(matches!(err, StyleAvailabilityError::NotFound(_)));
    }

    #[tokio::test]
    async fn ensure_style_available_maps_5xx_to_unavailable() {
        let (style_url, _request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "upstream down",
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);

        let err = preparer
            .ensure_style_available(&rev, Instant::now() + std::time::Duration::from_secs(1))
            .await
            .expect_err("5xx is a transient availability failure");

        server.abort();
        assert!(matches!(err, StyleAvailabilityError::Unavailable(_)));
    }

    #[tokio::test]
    async fn fetch_style_json_rejects_invalid_json_file() {
        let path = write_test_style_json("invalid", "not-json");

        let err = fetch_style_json(
            &reqwest::Client::new(),
            &revision().id,
            &path,
            Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .expect_err("invalid JSON is classified before MapLibre load");

        assert!(matches!(err.error, RendererError::StyleLoadFailed { .. }));
        assert!(err.negative_cacheable, "parse failure is definitive");
    }
}
