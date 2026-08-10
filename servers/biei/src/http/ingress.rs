//! URL parsing for the static image / tile API ingress.
//!
//! This module deliberately stops before axum. It converts an already matched
//! request path into an `InternalTask`, so the grammar and validation are
//! testable without binding sockets.
//!
//! This is not a resource loader. Fetching style.json dependencies such as
//! tiles, glyphs, and sprites remains delegated to maplibre-native's default
//! resource loader in production v0.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::http::HeaderMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::auth::DeliveryAuth;
use crate::drain::{DrainController, DrainPermit};
use crate::http::addlayer::parse_addlayer_from_query;
use crate::http::error::IngressError;
use crate::http::path::resolve_style_id;
use crate::http::preview::{PREVIEW_STYLE_CHECK_TIMEOUT, build_preview_response_for_style};
use crate::http::query::{parse_before_layer_from_query, parse_padding_from_query};
use crate::http::response::{
    IngressResponse, PublicResponsePolicy, response_from_ingress_error, response_from_outcome,
};
use crate::http::static_image::parse_static_path;
use crate::http::tile::parse_tile_path;

use biei_core::node::Node;
use biei_core::style_catalog::StyleCatalog;
use biei_core::types::{
    CredentialCachePartition, InternalTask, NamespaceSet, ProviderBearerToken, RenderAuthorization,
    RequestId, StyleId, TaskId,
};

#[derive(Debug)]
enum ParsedPublicPath<'a> {
    Preview { style_id: StyleId },
    Render(ParsedRenderPath<'a>),
}

impl<'a> ParsedPublicPath<'a> {
    fn parse(path: &'a str) -> Result<Self, IngressError> {
        let Some(path) = path.strip_prefix("/styles/") else {
            return Err(crate::http::error::invalid(
                "public path must start with `/styles/`",
            ));
        };
        if path.is_empty() || path.starts_with('/') || path.ends_with('/') {
            return Err(crate::http::error::invalid(
                "public path must not contain repeated or trailing `/` characters",
            ));
        }
        let parts: Vec<_> = path.split('/').collect();
        if parts.iter().any(|part| part.is_empty()) {
            return Err(crate::http::error::invalid(
                "public path must not contain empty segments",
            ));
        }

        if let [namespace, style_id, "preview"] = parts.as_slice() {
            return Ok(Self::Preview {
                style_id: resolve_style_id(&[namespace, style_id])?,
            });
        }

        ParsedRenderPath::from_parts(parts).map(Self::Render)
    }

    fn static_style_id(&self) -> Option<&StyleId> {
        match self {
            Self::Render(ParsedRenderPath {
                style_id,
                kind: ParsedRenderKind::Static { .. },
                ..
            }) => Some(style_id),
            Self::Preview { .. }
            | Self::Render(ParsedRenderPath {
                kind: ParsedRenderKind::Tile,
                ..
            }) => None,
        }
    }
}

#[derive(Debug)]
struct ParsedRenderPath<'a> {
    parts: Vec<&'a str>,
    style_id: StyleId,
    kind: ParsedRenderKind,
}

#[derive(Debug, Clone, Copy)]
enum ParsedRenderKind {
    Tile,
    Static { static_index: usize },
}

impl<'a> ParsedRenderPath<'a> {
    fn from_parts(parts: Vec<&'a str>) -> Result<Self, IngressError> {
        let (style_id, kind) = match parts.as_slice() {
            [namespace, style_id, "static", ..] => (
                resolve_style_id(&[namespace, style_id])?,
                ParsedRenderKind::Static { static_index: 2 },
            ),
            [namespace, style_id, "tiles", _, _, _] => (
                resolve_style_id(&[namespace, style_id])?,
                ParsedRenderKind::Tile,
            ),
            _ => {
                return Err(crate::http::error::invalid(
                    "render path must be /styles/{namespace}/{style_id}/static/... or /styles/{namespace}/{style_id}/tiles/{z}/{x}/{y}{@scale}.{format}",
                ));
            }
        };
        Ok(Self {
            parts,
            style_id,
            kind,
        })
    }

    fn response_policy(&self) -> PublicResponsePolicy {
        match self.kind {
            ParsedRenderKind::Tile => PublicResponsePolicy::Tile,
            ParsedRenderKind::Static { .. } => PublicResponsePolicy::Static,
        }
    }
}

#[derive(Clone)]
pub(crate) struct HttpIngress {
    node: Node,
    catalog: Arc<StyleCatalog>,
    tileset_url_template: Arc<str>,
    sla_budget: Duration,
    next_task_id: Arc<AtomicU64>,
    drain: Option<DrainController>,
    concurrency: Option<Arc<Semaphore>>,
    renderer_supervisor: crate::renderer::actor::RendererActorSupervisor,
    auth: Option<DeliveryAuth>,
}

impl HttpIngress {
    pub(crate) fn with_drain_and_limit(
        node: Node,
        catalog: Arc<StyleCatalog>,
        tileset_url_template: Arc<str>,
        sla_budget: Duration,
        drain: DrainController,
        concurrency_limit: usize,
        renderer_supervisor: crate::renderer::actor::RendererActorSupervisor,
    ) -> Self {
        Self {
            node,
            catalog,
            tileset_url_template,
            sla_budget,
            next_task_id: Arc::new(AtomicU64::new(1)),
            drain: Some(drain),
            concurrency: Some(Arc::new(Semaphore::new(concurrency_limit.max(1)))),
            renderer_supervisor,
            auth: None,
        }
    }

    pub(crate) fn with_auth(mut self, auth: Option<DeliveryAuth>) -> Self {
        self.auth = auth;
        self
    }

    pub(crate) fn drain_controller(&self) -> Option<DrainController> {
        self.drain.clone()
    }

    pub(crate) fn node(&self) -> Node {
        self.node.clone()
    }

    pub(crate) fn style_catalog(&self) -> Arc<StyleCatalog> {
        Arc::clone(&self.catalog)
    }

    pub(crate) fn renderer_supervisor(&self) -> crate::renderer::actor::RendererActorSupervisor {
        self.renderer_supervisor.clone()
    }

    #[cfg(test)]
    pub(crate) async fn handle_path(&self, path: &str, now: Instant) -> IngressResponse {
        self.handle_public_path_with_request_id(path, None, &HeaderMap::new(), None, now)
            .await
    }

    /// Acquires the concurrency and drain admission guards for a request. On
    /// rejection returns the ready-to-send 503 `IngressResponse`; on success
    /// returns the guards, which the caller must hold for the request's
    /// lifetime (dropping them releases the slot).
    fn acquire_admission(
        &self,
        request_id: &RequestId,
    ) -> Result<(Option<OwnedSemaphorePermit>, Option<DrainPermit>), IngressResponse> {
        // Degraded shedding is not decided here: that would drop cache hits too.
        // The node gates it after the output-cache lookup and preserves the
        // typed rejection cause for response classification.
        let concurrency_permit = match &self.concurrency {
            Some(limit) => match limit.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return Err(IngressResponse::json(503, "ingress_busy", "")
                        .with_retry_after("1")
                        .with_request_id(request_id));
                }
            },
            None => None,
        };
        let drain_permit = match &self.drain {
            Some(drain) => match drain.try_acquire() {
                Some(permit) => Some(permit),
                None => {
                    return Err(IngressResponse::json(503, "service_draining", "")
                        .with_retry_after("2")
                        .with_request_id(request_id));
                }
            },
            None => None,
        };
        Ok((concurrency_permit, drain_permit))
    }

    pub(crate) async fn handle_public_path_with_request_id(
        &self,
        path: &str,
        query: Option<&str>,
        headers: &HeaderMap,
        request_id: Option<RequestId>,
        now: Instant,
    ) -> IngressResponse {
        let request_id = request_id.unwrap_or_default();
        let parsed = match ParsedPublicPath::parse(path) {
            Ok(parsed) => parsed,
            Err(err) => return response_from_ingress_error(err).with_request_id(&request_id),
        };

        let authorization =
            if let (Some(auth), Some(style_id)) = (&self.auth, parsed.static_style_id()) {
                match auth
                    .authorize_static(headers, query, style_id.namespace())
                    .await
                {
                    Ok(authorized) => {
                        let readable_namespaces =
                            NamespaceSet::try_from_shared(authorized.shared_readable_namespaces())
                                .expect("mmpf-auth returns a validated bounded namespace set");
                        tracing::debug!(
                            principal_id = authorized.principal_id,
                            registry_id = authorized.registry_id,
                            namespace = style_id.namespace(),
                            "authorized static render"
                        );
                        Some(RenderAuthorization {
                            readable_namespaces,
                            cache_partition: CredentialCachePartition::from_digest(
                                authorized.cache_partition(),
                            ),
                            provider_bearer_token: authorized.backend_bearer_token().map(|token| {
                                ProviderBearerToken::try_new(token.to_string())
                                    .expect("mmpf-auth returns a validated bounded credential")
                            }),
                        })
                    }
                    Err(failure) => {
                        return crate::auth::failure_response(failure).with_request_id(&request_id);
                    }
                }
            } else {
                None
            };

        let admission = match self.acquire_admission(&request_id) {
            Ok(guards) => guards,
            Err(response) => return response,
        };

        let parsed = match parsed {
            ParsedPublicPath::Render(parsed) => parsed,
            ParsedPublicPath::Preview { style_id } => {
                let _admission = admission;
                let node = self.node.clone();
                return build_preview_response_for_style(&self.catalog, style_id, |revision| {
                    let node = node.clone();
                    async move {
                        node.ensure_style_available(
                            &revision,
                            Instant::now() + PREVIEW_STYLE_CHECK_TIMEOUT,
                        )
                        .await
                    }
                })
                .await
                .with_request_id(&request_id);
            }
        };

        let response_policy = parsed.response_policy();
        let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let mut task = match parse_path_with_request_id(
            parsed,
            query,
            &self.catalog,
            &self.tileset_url_template,
            task_id,
            request_id.clone(),
            self.sla_budget,
            now,
        ) {
            Ok(task) => task,
            Err(err) => return response_from_ingress_error(err).with_request_id(&request_id),
        };
        task.authorization = authorization;
        let node = self.node.clone();
        match tokio::spawn(async move {
            // Keep ingress/drain admission attached to the non-cancellable
            // render, not to the client connection that may disappear first.
            let _admission = admission;
            node.handle_incoming(task).await
        })
        .await
        {
            Ok(outcome) => response_from_outcome(outcome, response_policy),
            Err(error) => {
                tracing::error!(%error, "ingress render task terminated unexpectedly");
                IngressResponse::json(500, "internal_error", "").with_request_id(&request_id)
            }
        }
    }
}

#[cfg(test)]
const TEST_TILESET_URL_TEMPLATE: &str = "https://tiles.example.test/{tileset_id}/tileset.json";

#[allow(clippy::too_many_arguments)]
fn parse_path_with_request_id(
    parsed: ParsedRenderPath<'_>,
    query: Option<&str>,
    catalog: &StyleCatalog,
    tileset_url_template: &str,
    task_id: TaskId,
    request_id: RequestId,
    sla_budget: Duration,
    now: Instant,
) -> Result<InternalTask, IngressError> {
    let ParsedRenderPath {
        parts,
        style_id,
        kind,
    } = parsed;
    match kind {
        ParsedRenderKind::Static { static_index } => {
            let before_layer = parse_before_layer_from_query(query)?;
            let padding = parse_padding_from_query(query)?;
            let addlayer = parse_addlayer_from_query(query, tileset_url_template)?;
            parse_static_path(
                &parts,
                static_index,
                style_id,
                before_layer,
                padding,
                addlayer,
                catalog,
                task_id,
                request_id,
                sla_budget,
                now,
            )
        }
        ParsedRenderKind::Tile => parse_tile_path(
            &parts, style_id, catalog, task_id, request_id, sla_budget, now,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use biei_core::style_catalog::StyleDefinition;
    use biei_core::types::{RenderRequest, StyleId};

    fn catalog() -> StyleCatalog {
        let catalog = StyleCatalog::new();
        catalog.upsert_definition(
            StyleId("carto/static".to_string()),
            StyleDefinition::new("https://styles.test/static/style.json", 1),
        );
        catalog
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_path_with_request_id(
        path: &str,
        query: Option<&str>,
        catalog: &StyleCatalog,
        task_id: TaskId,
        request_id: RequestId,
        sla_budget: Duration,
        now: Instant,
    ) -> Result<InternalTask, IngressError> {
        let ParsedPublicPath::Render(parsed) = ParsedPublicPath::parse(path)? else {
            return Err(crate::http::error::invalid("expected render path"));
        };
        super::parse_path_with_request_id(
            parsed,
            query,
            catalog,
            TEST_TILESET_URL_TEMPLATE,
            task_id,
            request_id,
            sla_budget,
            now,
        )
    }

    #[test]
    fn parsed_public_path_preserves_endpoint_policy_and_style_identity() {
        let ParsedPublicPath::Render(tile) =
            ParsedPublicPath::parse("/styles/carto/static/tiles/8/227/100.png").expect("tile path")
        else {
            panic!("expected tile render path");
        };
        assert_eq!(tile.style_id.as_str(), "carto/static");
        assert_eq!(tile.response_policy(), PublicResponsePolicy::Tile);

        let ParsedPublicPath::Render(static_image) = ParsedPublicPath::parse(
            "/styles/carto/voyager/static/none/139.767,35.681,11/320x240.png",
        )
        .expect("static path") else {
            panic!("expected static render path");
        };
        assert_eq!(static_image.style_id.as_str(), "carto/voyager");
        assert_eq!(static_image.response_policy(), PublicResponsePolicy::Static);

        let ParsedPublicPath::Preview { style_id } =
            ParsedPublicPath::parse("/styles/carto/voyager/preview").expect("preview path")
        else {
            panic!("expected preview path");
        };
        assert_eq!(style_id.as_str(), "carto/voyager");
    }

    #[test]
    fn style_named_static_can_still_render_tiles() {
        let task = parse_path_with_request_id(
            "/styles/carto/static/tiles/8/227/100.png",
            Some("addlayer=%7Bbad-json"),
            &catalog(),
            42,
            RequestId::from_string("req-static-style"),
            Duration::from_secs(30),
            Instant::now(),
        )
        .expect("tile path with style id `static` parses and ignores static-only query");

        assert_eq!(task.style.id.as_str(), "carto/static");
        assert!(matches!(
            task.request,
            RenderRequest::Tile {
                z: 8,
                x: 227,
                y: 100,
                ..
            }
        ));
    }

    #[test]
    fn deeply_namespaced_style_is_rejected() {
        assert!(
            ParsedPublicPath::parse(
                "/styles/carto/gl/voyager-gl-style/static/none/139.767,35.681,11,0,0/320x240.png"
            )
            .is_err()
        );
    }

    #[test]
    fn maps_ingress_concurrency_limit_to_retryable_503() {
        let response = IngressResponse::json(503, "ingress_busy", "").with_retry_after("1");

        assert_eq!(response.status, 503);
        assert_eq!(response.headers, vec![("Retry-After", "1".to_string())]);
        assert!(
            std::str::from_utf8(&response.body)
                .expect("json body")
                .contains("ingress_busy")
        );
    }

    #[test]
    fn maps_ingress_drain_to_service_draining_label() {
        let response = IngressResponse::json(503, "service_draining", "").with_retry_after("2");

        assert_eq!(response.status, 503);
        assert_eq!(response.headers, vec![("Retry-After", "2".to_string())]);
        assert!(
            std::str::from_utf8(&response.body)
                .expect("json body")
                .contains("service_draining")
        );
    }

    #[tokio::test]
    async fn style_path_parsing_precedes_drain_admission() {
        let options = crate::options::test_options("https://styles.test/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let ingress = runtime.http_ingress(Duration::from_secs(2));
        runtime.drain_controller().begin_draining();

        for path in [
            "/styles/../voyager/tiles/0/0/0.png",
            "/styles/../voyager/preview",
        ] {
            let response = ingress.handle_path(path, Instant::now()).await;
            assert_eq!(response.status, 400, "malformed style path {path}");
            assert!(
                std::str::from_utf8(&response.body)
                    .expect("json body")
                    .contains("invalid_request")
            );
        }

        let response = ingress
            .handle_path("/styles/carto/basic/tiles/0/0/0.png", Instant::now())
            .await;
        assert_eq!(response.status, 503);
        assert!(
            std::str::from_utf8(&response.body)
                .expect("json body")
                .contains("service_draining")
        );
    }

    #[tokio::test]
    async fn optional_auth_protects_static_before_admission_but_not_tiles() {
        let options = crate::options::test_options("https://styles.test/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let catalog = crate::auth::RegistryCatalog::parse("public=memory:///auth/public/")
            .expect("auth catalog");
        let auth = crate::auth::DeliveryAuth::new(catalog, std::iter::empty::<(String, String)>());
        let ingress = runtime.http_ingress_with_auth(Duration::from_secs(2), auth);
        runtime.drain_controller().begin_draining();

        let static_response = ingress
            .handle_path(
                "/styles/carto/basic/static/none/0,0,1,0,0/320x240.png",
                Instant::now(),
            )
            .await;
        assert_eq!(static_response.status, 401);
        assert!(
            std::str::from_utf8(&static_response.body)
                .unwrap()
                .contains("invalid_token")
        );

        let tile_response = ingress
            .handle_path("/styles/carto/basic/tiles/0/0/0.png", Instant::now())
            .await;
        assert_eq!(tile_response.status, 503);
        assert!(
            std::str::from_utf8(&tile_response.body)
                .unwrap()
                .contains("service_draining")
        );
    }

    #[tokio::test]
    async fn anonymous_static_policy_is_explicit_and_invalid_tokens_do_not_fall_back() {
        let options = crate::options::test_options("https://styles.test/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let auth_root = std::env::temp_dir().join(format!(
            "biei-anonymous-auth-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir_all(&auth_root).expect("auth root");
        std::fs::write(
            auth_root.join("current.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema_version": 1,
                "registry_id": "public",
                "revision": 1,
                "anonymous": {
                    "enabled": true,
                    "namespaces": ["carto"],
                    "actions": ["render.static"],
                    "allowed_origins": [],
                    "allow_missing_origin": true
                },
                "credentials": []
            }))
            .expect("snapshot JSON"),
        )
        .expect("snapshot");
        let root_url = url::Url::from_directory_path(&auth_root)
            .expect("auth root URL")
            .to_string();
        let catalog =
            crate::auth::RegistryCatalog::parse(&format!("public={root_url}")).expect("catalog");
        let auth = crate::auth::DeliveryAuth::new_with_anonymous_registry(
            catalog,
            Some("public".to_string()),
            std::iter::empty::<(String, String)>(),
        )
        .expect("anonymous registry selection")
        .expect("enabled auth");
        let ingress = runtime.http_ingress_with_auth(Duration::from_secs(2), Some(auth));
        runtime.drain_controller().begin_draining();

        let anonymous = ingress
            .handle_path(
                "/styles/carto/basic/static/none/0,0,1,0,0/320x240.png",
                Instant::now(),
            )
            .await;
        assert_eq!(
            anonymous.status, 503,
            "the anonymous carto grant must pass auth and reach drain admission"
        );

        let private = ingress
            .handle_path(
                "/styles/private/basic/static/none/0,0,1,0,0/320x240.png",
                Instant::now(),
            )
            .await;
        assert_eq!(private.status, 403);

        let invalid = ingress
            .handle_public_path_with_request_id(
                "/styles/carto/basic/static/none/0,0,1,0,0/320x240.png",
                Some("access_token=public.wrong"),
                &HeaderMap::new(),
                None,
                Instant::now(),
            )
            .await;
        assert_eq!(invalid.status, 401);
        assert!(
            std::str::from_utf8(&invalid.body)
                .unwrap()
                .contains("invalid_token")
        );

        let _ = std::fs::remove_dir_all(auth_root);
    }

    #[tokio::test]
    async fn degraded_renderer_sheds_uncached_render_as_renderer_degraded() {
        let options = crate::options::test_options("https://styles.test/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let supervisor = runtime.renderer_supervisor();
        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);
        let ingress = runtime.http_ingress(Duration::from_secs(2));

        // A valid render path that misses the (empty) output cache: the node
        // sheds the would-be render before starting native work and preserves
        // the typed cause through public response classification.
        let response = ingress
            .handle_path("/styles/carto/basic/tiles/0/0/0.png", Instant::now())
            .await;
        assert_eq!(response.status, 503);
        assert!(
            std::str::from_utf8(&response.body)
                .expect("json body")
                .contains("renderer_degraded"),
            "uncached render on a degraded node is shed as renderer_degraded"
        );
    }

    #[tokio::test]
    async fn degraded_renderer_no_longer_sheds_before_path_processing() {
        let options = crate::options::test_options("https://styles.test/{style_id}/style.json", 1);
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let supervisor = runtime.renderer_supervisor();
        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);
        let ingress = runtime.http_ingress(Duration::from_secs(2));

        // The render-admission gate now runs after path parsing (so exact
        // output-cache hits stay reachable). A malformed path therefore fails
        // parsing with a 4xx rather than being shed with a blanket 503.
        let response = ingress
            .handle_path("/not/a/render/path", Instant::now())
            .await;
        assert_ne!(
            response.status, 503,
            "degraded shedding no longer precedes path processing"
        );
        assert!((400..500).contains(&response.status));
    }

    #[test]
    fn noncanonical_render_paths_name_the_canonical_shape() {
        for path in [
            "/totally/bogus/path",
            "/styles/default/basic/0/0/0.png",
            "/styles/basic/tiles/0/0/0.png",
        ] {
            let error = ParsedPublicPath::parse(path).expect_err(path);
            let message = error.to_string();
            assert!(message.contains("/styles/"), "{path} reported {message:?}");
        }
        ParsedPublicPath::parse("/styles/default/basic/tiles/8/227/100.png")
            .expect("canonical tile path parses");
    }

    #[test]
    fn an_empty_style_id_is_refused_rather_than_joined_to_nothing() {
        let error = crate::http::path::resolve_style_id(&[]).expect_err("empty slice");
        assert!(error.to_string().contains("namespace"), "{error}");
    }
}
