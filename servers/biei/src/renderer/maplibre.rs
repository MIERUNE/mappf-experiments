//! `Renderer` implementation backed by the production MapLibre actor.

use async_trait::async_trait;

use crate::renderer::actor::{
    RenderTaskView, RendererActor, RendererActorConfig, RendererActorSupervisor, ResolvedStyle,
};
use crate::renderer::{PreparedProfile, Renderer, RendererOutput};
#[cfg(test)]
use biei_core::types::RenderOutput;
use biei_core::types::{InternalTask, RendererError, SourceHash};

mod profile;
mod profile_fetch;

pub(crate) use profile::MapLibreProfilePreparer;
#[cfg(test)]
use profile::is_permanent_profile_http_status;

#[cfg(test)]
use profile_fetch::{fetch_style_json, fetch_tileset_json, resolve_tile_url};

pub(crate) struct MapLibreRenderer {
    actor: RendererActor,
    config: RendererActorConfig,
    supervisor: RendererActorSupervisor,
    retiring: bool,
    slot_available: bool,
}

impl MapLibreRenderer {
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

    #[cfg(test)]
    fn is_alive(&self) -> bool {
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
        self.spawn_and_install_replacement()?;
        self.retiring = false;
        tracing::warn!(
            worker_id = self.config.worker_id,
            "abandoned timed-out renderer actor and spawned replacement"
        );
        Ok(())
    }

    fn replace_finished_actor(&mut self) -> Result<(), RendererError> {
        self.spawn_and_install_replacement()
    }

    /// Spawns and installs a replacement actor while keeping slot health and
    /// replacement metrics synchronized for every replacement path.
    fn spawn_and_install_replacement(&mut self) -> Result<(), RendererError> {
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

// Style and TileJSON transport/policy live in a separate module so the
// renderer actor adapter remains independent of profile resource I/O.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::renderer::{ProfilePreparer, StyleAvailabilityError, actor::BlockingRenderBackend};
    use biei_core::style_catalog::{StyleCatalog, StyleDefinition};
    use biei_core::types::{
        AddLayer, AddLayerSource, ImageFormat, PixelRatio, Positioning, ProfileContent,
        ProfilePreparationError, RenderRequest, StyleId, StyleRevision,
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
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
            request_id: biei_core::types::RequestId::from_string("maplibre-test"),
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
                padding: biei_core::types::Padding::default(),
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

    fn test_url_policy() -> mmpf_mln_filesource::policy::ResourceUrlPolicy {
        mmpf_mln_filesource::policy::ResourceUrlPolicy::new(vec![
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

    async fn wait_for_request_count(count: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while count.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("test server observes request");
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
        assert!(matches!(
            error,
            ProfilePreparationError::InvalidPreparedContent {
                content: ProfileContent::Style(_),
                ..
            }
        ));
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
            !preparer.has_cached_style(&rev),
            "MLN rejection invalidates the positive JSON cache"
        );
        let error = preparer
            .prepare_profile(&task)
            .await
            .expect_err("native rejection is temporarily suppressed");

        server.abort();
        assert!(matches!(
            error,
            ProfilePreparationError::StyleUnavailable { .. }
        ));
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
    async fn tileset_follower_retries_after_leader_exhausts_its_deadline() {
        let style_path = write_test_style_json(
            "tileset-deadline",
            r#"{"version":8,"sources":{},"layers":[]}"#,
        );
        let (tileset_url, request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"tiles":["tiles/{z}/{x}/{y}.pbf"]}"#,
            Duration::from_millis(250),
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(
            rev.id.clone(),
            StyleDefinition::new(style_path, rev.version),
        );
        let preparer = Arc::new(MapLibreProfilePreparer::for_tests(catalog));
        let warm_task = internal_task(rev.clone());
        assert!(
            preparer
                .prepare_profile(&warm_task)
                .await
                .expect("style cache warms")
                .is_some()
        );

        let mut short_budget = internal_task(rev.clone());
        attach_addlayer_source(&mut short_budget, tileset_url.clone());
        short_budget.deadline = Instant::now() + Duration::from_millis(100);
        let mut long_budget = internal_task(rev);
        attach_addlayer_source(&mut long_budget, tileset_url);
        long_budget.deadline = Instant::now() + Duration::from_secs(2);

        let leader_preparer = Arc::clone(&preparer);
        let leader =
            tokio::spawn(async move { leader_preparer.prepare_profile(&short_budget).await });
        wait_for_request_count(&request_count, 1).await;
        let follower = preparer.prepare_profile(&long_budget).await;
        let leader = leader.await.expect("leader task joins");

        server.abort();
        assert!(matches!(
            leader,
            Err(ProfilePreparationError::CallerDeadlineExceeded)
        ));
        assert!(follower.expect("follower retries").is_some());
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            2,
            "the follower must retry TileJSON under its own deadline"
        );
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
        // A failed addlayer *source* is reported as a source failure, not a
        // style failure (item 31), while still being negative-cached once.
        assert!(matches!(
            first,
            Err(ProfilePreparationError::SourceUnavailable { .. })
        ));
        assert!(matches!(
            second,
            Err(ProfilePreparationError::SourceUnavailable { .. })
        ));
        assert_eq!(tileset_request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn profile_preparer_negative_caches_invalid_tileset_as_source_content() {
        let (style_url, _style_request_count, style_server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"version":8,"sources":{},"layers":[]}"#,
            std::time::Duration::ZERO,
        )
        .await;
        let (tileset_url, tileset_request_count, tileset_server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            "not-json",
            std::time::Duration::ZERO,
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = MapLibreProfilePreparer::for_tests(catalog);
        let mut task = internal_task(rev);
        attach_addlayer_source(&mut task, tileset_url);
        let source_hash = 1;

        let first = preparer.prepare_profile(&task).await;
        let second = preparer.prepare_profile(&task).await;

        style_server.abort();
        tileset_server.abort();
        assert!(matches!(
            first,
            Err(ProfilePreparationError::InvalidPreparedContent {
                content: ProfileContent::Source(hash),
                ..
            }) if hash == source_hash
        ));
        assert!(matches!(
            second,
            Err(ProfilePreparationError::InvalidPreparedContent {
                content: ProfileContent::Source(hash),
                ..
            }) if hash == source_hash
        ));
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
        // Transient tileset 5xx: a source failure (not style), not negative-cached.
        assert!(matches!(
            first,
            Err(ProfilePreparationError::SourceUnavailable { .. })
        ));
        assert!(matches!(
            second,
            Err(ProfilePreparationError::SourceUnavailable { .. })
        ));
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
        assert!(!error.error().to_string().contains(secret));
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
    async fn style_follower_retries_after_leader_exhausts_its_deadline() {
        let (style_url, request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::OK,
            r#"{"version":8,"sources":{},"layers":[]}"#,
            Duration::from_millis(250),
        )
        .await;
        let rev = revision();
        let catalog = Arc::new(StyleCatalog::new());
        catalog.upsert_definition(rev.id.clone(), StyleDefinition::new(style_url, rev.version));
        let preparer = Arc::new(MapLibreProfilePreparer::for_tests(catalog));
        let mut short_budget = internal_task(rev.clone());
        short_budget.deadline = Instant::now() + Duration::from_millis(100);
        let mut long_budget = internal_task(rev);
        long_budget.deadline = Instant::now() + Duration::from_secs(2);

        let leader_preparer = Arc::clone(&preparer);
        let leader =
            tokio::spawn(async move { leader_preparer.prepare_profile(&short_budget).await });
        wait_for_request_count(&request_count, 1).await;
        let follower = preparer.prepare_profile(&long_budget).await;
        let leader = leader.await.expect("leader task joins");

        server.abort();
        assert!(matches!(
            leader,
            Err(ProfilePreparationError::CallerDeadlineExceeded)
        ));
        assert!(follower.expect("follower retries").is_some());
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            2,
            "the follower must elect a new leader instead of inheriting the timeout"
        );
    }

    #[tokio::test]
    async fn profile_preparer_negative_caches_style_load_failures() {
        let (style_url, request_count, server) = spawn_counting_style_server(
            axum::http::StatusCode::NOT_FOUND,
            "missing style",
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
        let third = preparer.prepare_profile(&task).await;

        server.abort();
        assert!(matches!(
            first,
            Err(ProfilePreparationError::StyleUnavailable { .. })
        ));
        assert!(matches!(
            second,
            Err(ProfilePreparationError::StyleUnavailable { .. })
        ));
        assert!(matches!(
            third,
            Err(ProfilePreparationError::StyleUnavailable { .. })
        ));
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            1,
            "the leader shares the 404 and the negative cache serves later calls"
        );
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
        assert!(matches!(
            err.error(),
            ProfilePreparationError::StyleUnavailable { .. }
        ));
        assert!(
            err.is_negative_cacheable(),
            "4xx is definitive and cacheable"
        );
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
        assert!(matches!(
            first,
            Err(ProfilePreparationError::StyleUnavailable { .. })
        ));
        assert!(matches!(
            second,
            Err(ProfilePreparationError::StyleUnavailable { .. })
        ));
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

        assert!(matches!(
            err.error(),
            ProfilePreparationError::InvalidPreparedContent {
                content: ProfileContent::Style(_),
                ..
            }
        ));
        assert!(err.is_negative_cacheable(), "parse failure is definitive");
    }
}
