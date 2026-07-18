//! `Renderer` implementation backed by the production MapLibre actor.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use moka::sync::Cache;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};

use crate::renderer::actor::{
    RenderTaskView, RendererActor, RendererActorConfig, RendererActorSupervisor, ResolvedStyle,
};
use crate::renderer::http_fetch::{
    BodyReadError, read_bounded_body, redacted_url, redacted_url_str, reqwest_error_label,
};
use crate::renderer::{
    PreparedProfile, ProfilePreparer, Renderer, RendererOutput, StyleAvailabilityError,
};
use crate::style_catalog::StyleCatalog;
#[cfg(test)]
use crate::types::RenderOutput;
use crate::types::{
    AddLayerSource, InternalTask, RenderRequest, RendererError, SourceHash, StyleId, StyleRevision,
};
use crate::util::lock_unpoisoned;

pub struct MapLibreRenderer {
    actor: RendererActor,
    config: RendererActorConfig,
    supervisor: RendererActorSupervisor,
    retiring: bool,
    slot_available: bool,
}

pub struct MapLibreProfilePreparer {
    style_catalog: Arc<StyleCatalog>,
    http_client: reqwest::Client,
    url_policy: crate::renderer::file_source::policy::ResourceUrlPolicy,
    fetch_permits: Arc<tokio::sync::Semaphore>,
    style_json_cache: Cache<StyleRevision, Arc<str>>,
    style_error_cache: Cache<StyleRevision, RendererError>,
    tileset_json_cache: Cache<String, Arc<str>>,
    tileset_error_cache: Cache<String, RendererError>,
    inflight_style_loads: Mutex<HashMap<StyleRevision, watch::Sender<JsonLoadSignal>>>,
    inflight_tileset_loads: Mutex<HashMap<String, watch::Sender<JsonLoadSignal>>>,
}

#[derive(Clone)]
enum JsonLoadSignal {
    Pending,
    Ready(Arc<str>),
    Failed(ProfileFetchError),
    Aborted,
}

enum CacheLookup {
    Load(watch::Sender<JsonLoadSignal>),
    Wait(watch::Receiver<JsonLoadSignal>),
    Negative(RendererError),
}

struct InFlightJsonLoad<'a, K: Eq + Hash> {
    key: K,
    inflight: &'a Mutex<HashMap<K, watch::Sender<JsonLoadSignal>>>,
    tx: watch::Sender<JsonLoadSignal>,
    completed: bool,
}

impl<K: Eq + Hash> Drop for InFlightJsonLoad<'_, K> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        lock_unpoisoned(self.inflight).remove(&self.key);
        self.tx.send_replace(JsonLoadSignal::Aborted);
    }
}

const STYLE_JSON_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const STYLE_JSON_CACHE_IDLE_TTL: Duration = Duration::from_secs(60 * 60);
const TILESET_JSON_CACHE_MAX_BYTES: u64 = 32 * 1024 * 1024;
const TILESET_JSON_CACHE_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
const JSON_NEGATIVE_CACHE_MAX_ENTRIES: u64 = 4096;
// Short on purpose: the negative cache only needs to absorb repeated requests
// for the same definitively-bad style or TileJSON within a burst (§7.5 spray
// defense). A longer TTL would delay a freshly-registered/fixed resource from
// becoming servable. Transient failures (5xx, connection/read errors,
// timeouts) are not cached here at all — see `ProfileFetchError`.
const JSON_NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(5);

fn is_permanent_profile_http_status(status: reqwest::StatusCode) -> bool {
    status.is_client_error()
        && status != reqwest::StatusCode::REQUEST_TIMEOUT
        && status != reqwest::StatusCode::TOO_MANY_REQUESTS
}

impl MapLibreRenderer {
    pub fn spawn(config: RendererActorConfig) -> Result<Self, RendererError> {
        Self::spawn_supervised(config, RendererActorSupervisor::new(1))
    }

    pub(crate) fn spawn_supervised(
        config: RendererActorConfig,
        supervisor: RendererActorSupervisor,
    ) -> Result<Self, RendererError> {
        Ok(Self {
            actor: RendererActor::spawn_supervised(config.clone(), supervisor.clone())?,
            config,
            supervisor,
            retiring: false,
            slot_available: true,
        })
    }

    #[cfg(test)]
    fn from_actor(actor: RendererActor) -> Self {
        let supervisor = RendererActorSupervisor::new(1);
        Self {
            actor,
            config: RendererActorConfig {
                worker_id: 0,
                ambient_cache_path: None,
            },
            supervisor,
            retiring: false,
            slot_available: true,
        }
    }

    pub fn is_alive(&self) -> bool {
        !self.retiring && self.actor.is_alive()
    }

    fn actor(&mut self) -> Result<&RendererActor, RendererError> {
        if self.retiring {
            self.replace_retiring_actor()?;
        } else if !self.actor.is_alive() {
            self.replace_finished_actor()?;
        }
        Ok(&self.actor)
    }

    fn replace_retiring_actor(&mut self) -> Result<(), RendererError> {
        if !self.actor.try_abandon() {
            let first_exhaustion = self.slot_available;
            if first_exhaustion {
                self.supervisor.record_replacement_exhausted();
            }
            self.supervisor
                .set_slot_available(&mut self.slot_available, false);
            if first_exhaustion {
                tracing::error!(
                    worker_id = self.config.worker_id,
                    orphaned_threads = self.supervisor.snapshot().orphaned_threads,
                    "renderer actor replacement budget exhausted"
                );
            }
            return Err(RendererError::ActorDead);
        }

        // Reserve bounded orphan capacity before creating another native
        // renderer thread. Otherwise an exhausted slot briefly creates and
        // immediately tears down a replacement on every retry.
        let replacement =
            match RendererActor::spawn_supervised(self.config.clone(), self.supervisor.clone()) {
                Ok(actor) => actor,
                Err(err) => {
                    self.supervisor.record_replacement_failed();
                    self.supervisor
                        .set_slot_available(&mut self.slot_available, false);
                    return Err(err);
                }
            };

        self.actor = replacement;
        self.retiring = false;
        self.supervisor
            .set_slot_available(&mut self.slot_available, true);
        self.supervisor.record_replacement_succeeded();
        tracing::warn!(
            worker_id = self.config.worker_id,
            "abandoned timed-out renderer actor and spawned replacement"
        );
        Ok(())
    }

    fn replace_finished_actor(&mut self) -> Result<(), RendererError> {
        match RendererActor::spawn_supervised(self.config.clone(), self.supervisor.clone()) {
            Ok(actor) => {
                self.actor = actor;
                self.supervisor
                    .set_slot_available(&mut self.slot_available, true);
                self.supervisor.record_replacement_succeeded();
                Ok(())
            }
            Err(err) => {
                self.supervisor.record_replacement_failed();
                self.supervisor
                    .set_slot_available(&mut self.slot_available, false);
                Err(err)
            }
        }
    }
}

impl MapLibreProfilePreparer {
    pub fn new(
        style_catalog: Arc<StyleCatalog>,
        max_concurrent_fetches: usize,
        private_hosts: Vec<String>,
    ) -> anyhow::Result<Self> {
        let url_policy =
            crate::renderer::file_source::policy::ResourceUrlPolicy::new(private_hosts);
        Ok(Self {
            style_catalog,
            http_client: crate::renderer::file_source::build_profile_http_client(
                url_policy.clone(),
            )?,
            url_policy,
            fetch_permits: Arc::new(tokio::sync::Semaphore::new(max_concurrent_fetches.max(1))),
            style_json_cache: style_json_cache(),
            style_error_cache: error_cache(),
            tileset_json_cache: tileset_json_cache(),
            tileset_error_cache: error_cache(),
            inflight_style_loads: Mutex::new(HashMap::new()),
            inflight_tileset_loads: Mutex::new(HashMap::new()),
        })
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
    ) -> Result<PreparedProfile, ProfileFetchError> {
        loop {
            match self.lookup_style_cache(style) {
                Ok(style_json) => {
                    return Ok(PreparedProfile {
                        revision: style.clone(),
                        style_json,
                        addlayer_source: None,
                    });
                }
                Err(CacheLookup::Wait(mut rx)) => {
                    match tokio::time::timeout_at(deadline, wait_for_json_load(&mut rx))
                        .await
                        .map_err(|_| ProfileFetchError::transient(RendererError::Timeout))??
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
                Err(CacheLookup::Negative(err)) => return Err(ProfileFetchError::permanent(err)),
                Err(CacheLookup::Load(tx)) => {
                    let mut guard = InFlightJsonLoad {
                        key: style.clone(),
                        inflight: &self.inflight_style_loads,
                        tx: tx.clone(),
                        completed: false,
                    };
                    let result = self.fetch_uncached_style(style, deadline).await;
                    store_json_fetch_result(
                        &self.style_json_cache,
                        &self.style_error_cache,
                        &self.inflight_style_loads,
                        style.clone(),
                        tx,
                        &result,
                    );
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
        let tilejson = self
            .resolve_tileset_json(style_id, &tileset_url, deadline)
            .await?;
        rewrite_tileset_source_json(style_id, source, &tileset_url, &tilejson)
    }

    async fn resolve_tileset_json(
        &self,
        style_id: &StyleId,
        tileset_url: &str,
        deadline: Instant,
    ) -> Result<Arc<str>, RendererError> {
        loop {
            match self.lookup_tileset_cache(tileset_url) {
                Ok(tilejson) => return Ok(tilejson),
                Err(CacheLookup::Wait(mut rx)) => {
                    match tokio::time::timeout_at(deadline, wait_for_json_load(&mut rx))
                        .await
                        .map_err(|_| RendererError::Timeout)?
                        .map_err(|failure| failure.error)?
                    {
                        Some(tilejson) => return Ok(tilejson),
                        None => continue,
                    }
                }
                Err(CacheLookup::Negative(error)) => return Err(error),
                Err(CacheLookup::Load(tx)) => {
                    let mut guard = InFlightJsonLoad {
                        key: tileset_url.to_string(),
                        inflight: &self.inflight_tileset_loads,
                        tx: tx.clone(),
                        completed: false,
                    };
                    let result = self
                        .fetch_uncached_tileset(style_id, tileset_url, deadline)
                        .await;
                    store_json_fetch_result(
                        &self.tileset_json_cache,
                        &self.tileset_error_cache,
                        &self.inflight_tileset_loads,
                        tileset_url.to_string(),
                        tx,
                        &result,
                    );
                    guard.completed = true;
                    return result.map_err(|failure| failure.error);
                }
            }
        }
    }

    fn lookup_style_cache(&self, revision: &StyleRevision) -> Result<Arc<str>, CacheLookup> {
        if let Some(err) = self.style_error_cache.get(revision) {
            return Err(CacheLookup::Negative(err));
        }
        if let Some(style_json) = self.style_json_cache.get(revision) {
            return Ok(style_json);
        }
        let mut inflight = lock_unpoisoned(&self.inflight_style_loads);
        match inflight.get(revision) {
            Some(tx) => Err(CacheLookup::Wait(tx.subscribe())),
            None => {
                let (tx, _rx) = watch::channel(JsonLoadSignal::Pending);
                inflight.insert(revision.clone(), tx.clone());
                Err(CacheLookup::Load(tx))
            }
        }
    }

    fn lookup_tileset_cache(&self, tileset_url: &str) -> Result<Arc<str>, CacheLookup> {
        if let Some(tilejson) = self.tileset_json_cache.get(tileset_url) {
            return Ok(tilejson);
        }
        if let Some(error) = self.tileset_error_cache.get(tileset_url) {
            return Err(CacheLookup::Negative(error));
        }
        let mut inflight = lock_unpoisoned(&self.inflight_tileset_loads);
        match inflight.get(tileset_url) {
            Some(tx) => Err(CacheLookup::Wait(tx.subscribe())),
            None => {
                let (tx, _rx) = watch::channel(JsonLoadSignal::Pending);
                inflight.insert(tileset_url.to_string(), tx.clone());
                Err(CacheLookup::Load(tx))
            }
        }
    }

    async fn fetch_uncached_style(
        &self,
        style: &StyleRevision,
        deadline: Instant,
    ) -> Result<Arc<str>, ProfileFetchError> {
        let _permit = tokio::time::timeout_at(deadline, self.fetch_permits.acquire())
            .await
            .map_err(|_| ProfileFetchError::transient(RendererError::Timeout))?
            .map_err(|_| ProfileFetchError::transient(RendererError::ActorDead))?;
        let definition = self
            .style_catalog
            .definition_for_revision(style)
            .ok_or_else(|| {
                ProfileFetchError::permanent(RendererError::StyleLoadFailed {
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
                &self.url_policy,
                &style.id,
                &definition.style_url,
                deadline,
            )
            .await?,
        ))
    }

    async fn fetch_uncached_tileset(
        &self,
        style_id: &StyleId,
        tileset_url: &str,
        deadline: Instant,
    ) -> Result<Arc<str>, ProfileFetchError> {
        let _permit = tokio::time::timeout_at(deadline, self.fetch_permits.acquire())
            .await
            .map_err(|_| ProfileFetchError::transient(RendererError::Timeout))?
            .map_err(|_| ProfileFetchError::transient(RendererError::ActorDead))?;
        Ok(Arc::from(
            fetch_tileset_json(
                &self.http_client,
                &self.url_policy,
                style_id,
                tileset_url,
                deadline,
            )
            .await?,
        ))
    }
}

fn store_json_fetch_result<K>(
    cache: &Cache<K, Arc<str>>,
    error_cache: &Cache<K, RendererError>,
    inflight: &Mutex<HashMap<K, watch::Sender<JsonLoadSignal>>>,
    key: K,
    tx: watch::Sender<JsonLoadSignal>,
    result: &Result<Arc<str>, ProfileFetchError>,
) where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    match result {
        Ok(json) => {
            cache.insert(key.clone(), json.clone());
            tx.send_replace(JsonLoadSignal::Ready(json.clone()));
        }
        Err(failure) => {
            // Only definitive failures are negative-cached; transient ones
            // (5xx, connection/read errors, timeouts) must be retried.
            if failure.negative_cacheable {
                error_cache.insert(key.clone(), failure.error.clone());
            }
            tx.send_replace(JsonLoadSignal::Failed(failure.clone()));
        }
    }
    lock_unpoisoned(inflight).remove(&key);
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

fn error_cache<K>() -> Cache<K, RendererError>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    Cache::builder()
        .max_capacity(JSON_NEGATIVE_CACHE_MAX_ENTRIES)
        .time_to_live(JSON_NEGATIVE_CACHE_TTL)
        .build()
}

async fn wait_for_json_load(
    rx: &mut watch::Receiver<JsonLoadSignal>,
) -> Result<Option<Arc<str>>, ProfileFetchError> {
    loop {
        match rx.borrow_and_update().clone() {
            JsonLoadSignal::Pending => {}
            JsonLoadSignal::Ready(style_json) => return Ok(Some(style_json)),
            JsonLoadSignal::Failed(err) => return Err(err),
            JsonLoadSignal::Aborted => return Ok(None),
        }
        rx.changed()
            .await
            .map_err(|_| ProfileFetchError::transient(RendererError::ActorDead))?;
    }
}

/// A failed style or TileJSON fetch plus whether it is safe to negative-cache.
///
/// Permanent/content failures (4xx, parse, oversize, bad encoding, unknown
/// resource) reproduce on an immediate retry, so caching them briefly is the
/// §7.5 spray defense. Transient failures (5xx, connection/read errors,
/// timeouts) may recover at once, so they are never cached — otherwise a
/// one-second upstream blip becomes `JSON_NEGATIVE_CACHE_TTL` of forced
/// failures for every request hitting that style.
#[derive(Clone)]
struct ProfileFetchError {
    error: RendererError,
    negative_cacheable: bool,
}

impl ProfileFetchError {
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
            .map_err(ProfileFetchError::into_availability_error)
    }

    fn mark_style_load_failed(&self, revision: &StyleRevision) {
        // A provider may repair invalid style JSON without changing the lazy
        // template revision. Do not keep feeding MLN the rejected positive
        // cache entry after the short negative-cache window expires.
        self.style_json_cache.invalidate(revision);
        self.style_error_cache.insert(
            revision.clone(),
            RendererError::StyleLoadFailed {
                style_id: revision.id.clone(),
                source: "MapLibre rejected the prepared style".to_string(),
            },
        );
    }
}

#[cfg(test)]
impl MapLibreProfilePreparer {
    fn for_tests(style_catalog: Arc<StyleCatalog>) -> Self {
        Self {
            style_catalog,
            http_client: reqwest::Client::new(),
            url_policy: crate::renderer::file_source::policy::ResourceUrlPolicy::new(vec![
                "127.0.0.1".to_owned(),
                "localhost".to_owned(),
            ]),
            fetch_permits: Arc::new(tokio::sync::Semaphore::new(16)),
            style_json_cache: style_json_cache(),
            style_error_cache: error_cache(),
            tileset_json_cache: tileset_json_cache(),
            tileset_error_cache: error_cache(),
            inflight_style_loads: Mutex::new(HashMap::new()),
            inflight_tileset_loads: Mutex::new(HashMap::new()),
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
        // MapLibre resolves source resources through the process-wide Rust
        // FileSource chain installed by the renderer actor.
        Ok(())
    }

    async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
        self.actor()?.render(RenderTaskView::from(task)).await
    }

    fn retire_after_current(&mut self) {
        self.retiring = true;
        self.actor.retire_after_current();
        // Native renders cannot be preempted safely. Replace the actor now and
        // let the bounded orphan tracker account for the old thread until its
        // native call returns.
        let _ = self.replace_retiring_actor();
    }

    fn repair_if_needed(&mut self) -> Result<bool, RendererError> {
        if self.retiring {
            self.replace_retiring_actor()?;
            Ok(true)
        } else if !self.actor.is_alive() {
            self.replace_finished_actor()?;
            Ok(true)
        } else {
            Ok(false)
        }
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
    url_policy: &crate::renderer::file_source::policy::ResourceUrlPolicy,
    style_id: &StyleId,
    tileset_url: &str,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    let safe_input = redacted_url_str(tileset_url);
    let url = url::Url::parse(tileset_url).map_err(|err| {
        ProfileFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset URL parse failed for {safe_input}: {err}"),
        })
    })?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(ProfileFetchError::permanent(
            RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("unsupported tileset URL scheme: {}", url.scheme()),
            },
        ));
    }
    if !url_policy.permits_url_without_dns(&url) {
        return Err(ProfileFetchError::permanent(
            RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("blocked tileset URL destination: {safe_input}"),
            },
        ));
    }
    let safe_url = redacted_url(&url);
    let response = tokio::time::timeout_at(deadline, client.get(url.clone()).send())
        .await
        .map_err(|_| ProfileFetchError::transient(RendererError::Timeout))?
        .map_err(|err| {
            let error_kind = reqwest_error_label(&err);
            tracing::debug!(
                style_id = style_id.as_str(),
                resource_url = safe_url,
                error_kind,
                "TileJSON request failed"
            );
            ProfileFetchError::transient(RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("tileset GET failed for {safe_url} ({error_kind})"),
            })
        })?;
    let status = response.status();
    if !status.is_success() {
        tracing::debug!(
            style_id = style_id.as_str(),
            resource_url = safe_url,
            %status,
            "TileJSON provider returned a non-success status"
        );
        let error = RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset GET failed for {safe_url}: HTTP status code {status}"),
        };
        return Err(if is_permanent_profile_http_status(status) {
            ProfileFetchError::permanent(error)
        } else {
            ProfileFetchError::transient(error)
        });
    }
    let bytes = read_bounded_body(response, MAX_TILESET_JSON_BYTES, deadline)
        .await
        .map_err(|err| match err {
            BodyReadError::Timeout => ProfileFetchError::transient(RendererError::Timeout),
            BodyReadError::Transport(_) => {
                ProfileFetchError::transient(RendererError::StyleLoadFailed {
                    style_id: style_id.clone(),
                    source: format!("tileset body read failed for {safe_url}: {err}"),
                })
            }
            BodyReadError::TooLarge { .. } => {
                ProfileFetchError::permanent(RendererError::StyleLoadFailed {
                    style_id: style_id.clone(),
                    source: err.to_string(),
                })
            }
        })?;
    let json = String::from_utf8(bytes).map_err(|err| {
        ProfileFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset JSON is not UTF-8: {err}"),
        })
    })?;
    validate_tileset_json(style_id, &json)?;
    Ok(json)
}

fn validate_tileset_json(style_id: &StyleId, json: &str) -> Result<(), ProfileFetchError> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|err| {
        ProfileFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("tileset JSON parse failed: {err}"),
        })
    })?;
    let tiles = value
        .as_object()
        .and_then(|object| object.get("tiles"))
        .and_then(serde_json::Value::as_array)
        .filter(|tiles| !tiles.is_empty())
        .ok_or_else(|| {
            ProfileFetchError::permanent(RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: "tileset JSON must contain a non-empty `tiles` array".to_string(),
            })
        })?;
    if tiles.iter().any(|tile| !tile.is_string()) {
        return Err(ProfileFetchError::permanent(
            RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: "tileset JSON contains a non-string tile URL".to_string(),
            },
        ));
    }
    Ok(())
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
        source: format!(
            "tileset URL parse failed for {}: {err}",
            redacted_url_str(tileset_url)
        ),
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
                source: format!("relative tile URL resolve failed: {err}"),
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
    url_policy: &crate::renderer::file_source::policy::ResourceUrlPolicy,
    style_id: &StyleId,
    style_url: &str,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    let json = match url::Url::parse(style_url) {
        Ok(url) if url.scheme() == "http" || url.scheme() == "https" => {
            fetch_http_style_json(client, url_policy, style_id, url, deadline).await?
        }
        Ok(url) if url.scheme() == "file" => {
            let path = url.to_file_path().map_err(|_| {
                ProfileFetchError::permanent(RendererError::StyleLoadFailed {
                    style_id: style_id.clone(),
                    source: format!("style file URL is not a local path: {style_url}"),
                })
            })?;
            read_style_json_file(style_id, &path, deadline).await?
        }
        Ok(url) => {
            return Err(ProfileFetchError::permanent(
                RendererError::StyleLoadFailed {
                    style_id: style_id.clone(),
                    source: format!("unsupported style URL scheme: {}", url.scheme()),
                },
            ));
        }
        Err(_) => read_style_json_file(style_id, std::path::Path::new(style_url), deadline).await?,
    };

    // TODO: this keeps error taxonomy under biei's control, but MapLibre
    // Native parses the same JSON again in load_style_from_json. Revisit if
    // cold profile setup cost becomes visible in production profiles.
    serde_json::from_str::<serde_json::Value>(&json).map_err(|err| {
        ProfileFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style JSON parse failed: {err}"),
        })
    })?;
    Ok(json)
}

async fn fetch_http_style_json(
    client: &reqwest::Client,
    url_policy: &crate::renderer::file_source::policy::ResourceUrlPolicy,
    style_id: &crate::types::StyleId,
    style_url: url::Url,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    let safe_url = redacted_url(&style_url);
    if !url_policy.permits_url_without_dns(&style_url) {
        return Err(ProfileFetchError::permanent(
            RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("blocked style URL destination: {safe_url}"),
            },
        ));
    }
    let response = tokio::time::timeout_at(deadline, client.get(style_url.clone()).send())
        .await
        .map_err(|_| ProfileFetchError::transient(RendererError::Timeout))?
        .map_err(|err| {
            // Connection/DNS/send failure: the upstream may come back at once.
            let error_kind = reqwest_error_label(&err);
            tracing::debug!(
                style_id = style_id.as_str(),
                resource_url = safe_url,
                error_kind,
                "style request failed"
            );
            ProfileFetchError::transient(RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("style GET failed for {safe_url} ({error_kind})"),
            })
        })?;

    let status = response.status();
    if !status.is_success() {
        tracing::debug!(
            style_id = style_id.as_str(),
            resource_url = safe_url,
            %status,
            "style provider returned a non-success status"
        );
        let err = RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style GET failed for {safe_url}: HTTP status code {status}"),
        };
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
            BodyReadError::Timeout => ProfileFetchError::transient(RendererError::Timeout),
            BodyReadError::Transport(_) => {
                ProfileFetchError::transient(RendererError::StyleLoadFailed {
                    style_id: style_id.clone(),
                    source: format!("style body read failed for {safe_url}: {err}"),
                })
            }
            BodyReadError::TooLarge { .. } => {
                ProfileFetchError::permanent(RendererError::StyleLoadFailed {
                    style_id: style_id.clone(),
                    source: err.to_string(),
                })
            }
        })?;

    String::from_utf8(bytes).map_err(|err| {
        ProfileFetchError::permanent(RendererError::StyleLoadFailed {
            style_id: style_id.clone(),
            source: format!("style JSON is not UTF-8: {err}"),
        })
    })
}

async fn read_style_json_file(
    style_id: &crate::types::StyleId,
    path: &std::path::Path,
    deadline: Instant,
) -> Result<String, ProfileFetchError> {
    let metadata = tokio::time::timeout_at(deadline, tokio::fs::metadata(path))
        .await
        .map_err(|_| ProfileFetchError::transient(RendererError::Timeout))?
        .map_err(|err| {
            ProfileFetchError::transient(RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("style file metadata failed for {}: {err}", path.display()),
            })
        })?;
    if !metadata.is_file() {
        return Err(ProfileFetchError::permanent(
            RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("style path is not a file: {}", path.display()),
            },
        ));
    }
    if metadata.len() > MAX_STYLE_JSON_BYTES as u64 {
        return Err(ProfileFetchError::permanent(
            RendererError::StyleLoadFailed {
                style_id: style_id.clone(),
                source: format!("style JSON exceeds {MAX_STYLE_JSON_BYTES} bytes"),
            },
        ));
    }

    tokio::time::timeout_at(deadline, tokio::fs::read_to_string(path))
        .await
        .map_err(|_| ProfileFetchError::transient(RendererError::Timeout))?
        .map_err(|err| {
            ProfileFetchError::transient(RendererError::StyleLoadFailed {
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

        fn render(&mut self, task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
            Ok(RenderOutput {
                bytes: bytes::Bytes::copy_from_slice(task.style.id.as_bytes()),
                format: task.output_format,
            }
            .into())
        }
    }

    fn revision() -> StyleRevision {
        StyleRevision {
            id: StyleId("carto/voyager".to_string()),
            version: 1,
        }
    }

    #[test]
    fn profile_http_status_only_negative_caches_deterministic_client_errors() {
        assert!(is_permanent_profile_http_status(
            reqwest::StatusCode::NOT_FOUND
        ));
        assert!(is_permanent_profile_http_status(reqwest::StatusCode::GONE));
        assert!(!is_permanent_profile_http_status(
            reqwest::StatusCode::REQUEST_TIMEOUT
        ));
        assert!(!is_permanent_profile_http_status(
            reqwest::StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(!is_permanent_profile_http_status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        ));
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

    fn test_url_policy() -> crate::renderer::file_source::policy::ResourceUrlPolicy {
        crate::renderer::file_source::policy::ResourceUrlPolicy::new(vec![
            "127.0.0.1".to_owned(),
            "localhost".to_owned(),
        ])
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

        assert_eq!(output.output.bytes.as_ref(), b"carto/voyager");
        assert_eq!(output.output.format, ImageFormat::Webp);
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
    async fn autonomous_repair_restores_slot_without_another_render_task() {
        let supervisor = RendererActorSupervisor::new(1);
        let config = RendererActorConfig {
            worker_id: 10,
            ambient_cache_path: None,
        };
        let actor = RendererActor::spawn_with_backend_supervised(
            config.clone(),
            supervisor.clone(),
            FakeBackend,
        )
        .expect("actor spawns");
        actor.retire_after_current();
        tokio::time::timeout(Duration::from_secs(1), async {
            while actor.is_alive() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("idle actor retires");

        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);
        let mut renderer = MapLibreRenderer {
            actor,
            config,
            supervisor: supervisor.clone(),
            retiring: true,
            slot_available,
        };
        assert_eq!(
            supervisor.health(),
            crate::renderer::actor::RendererHealth::InternalUnrecoverable
        );

        assert!(renderer.repair_if_needed().expect("repair succeeds"));
        assert_eq!(
            supervisor.health(),
            crate::renderer::actor::RendererHealth::Full
        );
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
    async fn production_profile_preparer_blocks_unallowlisted_private_style_host() {
        let (style_url, request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"version":8,"sources":{},"layers":[]}"#,
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::new(catalog, 1, Vec::new())
            .expect("build filtered profile client");

        let error = preparer
            .prepare_profile(&internal_task(rev))
            .await
            .expect_err("loopback style host must require an exact allowlist entry");

        server.abort();
        assert!(matches!(error, RendererError::StyleLoadFailed { .. }));
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            0,
            "blocked initial URL must not reach the private server"
        );
    }

    #[tokio::test]
    async fn native_style_rejection_temporarily_suppresses_cached_json() {
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
        let task = internal_task(rev.clone());

        assert!(
            preparer
                .prepare_profile(&task)
                .await
                .expect("style fetch succeeds")
                .is_some()
        );
        preparer.mark_style_load_failed(&rev);
        assert!(
            preparer.style_json_cache.get(&rev).is_none(),
            "MLN rejection invalidates the positive JSON cache"
        );
        let error = preparer
            .prepare_profile(&task)
            .await
            .expect_err("native rejection is temporarily suppressed");

        server.abort();
        assert!(matches!(error, RendererError::StyleLoadFailed { .. }));
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
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
    async fn profile_preparer_coalesces_concurrent_tileset_fetches() {
        let (style_url, _style_request_count, style_server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"version":8,"sources":{},"layers":[]}"#,
            std::time::Duration::ZERO,
        )
        .await;
        let (tileset_url, tileset_request_count, tileset_server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"tiles":["tiles/{z}/{x}/{y}.pbf"]}"#,
            std::time::Duration::from_millis(50),
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let mut task = internal_task(rev);
        attach_addlayer_source(&mut task, tileset_url);

        let (first, second) = tokio::join!(
            preparer.prepare_profile(&task),
            preparer.prepare_profile(&task)
        );

        style_server.abort();
        tileset_server.abort();
        assert!(first.expect("first profile prepares").is_some());
        assert!(second.expect("second profile prepares").is_some());
        assert_eq!(tileset_request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn profile_preparer_negative_caches_tileset_404() {
        let (style_url, _style_request_count, style_server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"version":8,"sources":{},"layers":[]}"#,
            std::time::Duration::ZERO,
        )
        .await;
        let (tileset_url, tileset_request_count, tileset_server) = spawn_counting_style_server(
            axum::http::StatusCode::NOT_FOUND,
            "missing tileset",
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let mut task = internal_task(rev);
        attach_addlayer_source(&mut task, tileset_url);

        let first = preparer.prepare_profile(&task).await;
        let second = preparer.prepare_profile(&task).await;

        style_server.abort();
        tileset_server.abort();
        assert!(matches!(first, Err(RendererError::StyleLoadFailed { .. })));
        assert!(matches!(second, Err(RendererError::StyleLoadFailed { .. })));
        assert_eq!(tileset_request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn profile_preparer_does_not_cache_transient_tileset_5xx() {
        let (style_url, _style_request_count, style_server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"version":8,"sources":{},"layers":[]}"#,
            std::time::Duration::ZERO,
        )
        .await;
        let (tileset_url, tileset_request_count, tileset_server) = spawn_counting_style_server(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "upstream down",
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let mut task = internal_task(rev);
        attach_addlayer_source(&mut task, tileset_url);

        let first = preparer.prepare_profile(&task).await;
        let second = preparer.prepare_profile(&task).await;

        style_server.abort();
        tileset_server.abort();
        assert!(matches!(first, Err(RendererError::StyleLoadFailed { .. })));
        assert!(matches!(second, Err(RendererError::StyleLoadFailed { .. })));
        assert_eq!(tileset_request_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn tileset_fetch_error_redacts_query_credentials() {
        let (tileset_url, _request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::NOT_FOUND,
            "missing tileset",
            std::time::Duration::ZERO,
        )
        .await;
        let secret = "do-not-log-this-token";
        let policy = test_url_policy();
        let error = fetch_tileset_json(
            &reqwest::Client::new(),
            &policy,
            &revision().id,
            &format!("{tileset_url}?access_token={secret}"),
            Instant::now() + std::time::Duration::from_secs(1),
        )
        .await
        .expect_err("404 returns a classified fetch failure");

        server.abort();
        assert!(!error.error.to_string().contains(secret));
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

        let policy = test_url_policy();
        let err = fetch_style_json(
            &reqwest::Client::new(),
            &policy,
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

        let policy = test_url_policy();
        let err = fetch_style_json(
            &reqwest::Client::new(),
            &policy,
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
