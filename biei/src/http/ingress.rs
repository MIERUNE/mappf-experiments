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

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::drain::{DrainController, DrainPermit};
use crate::http::addlayer::parse_addlayer_from_query;
use crate::http::error::IngressError;
use crate::http::format::parse_scale_format;
use crate::http::preview::{PREVIEW_STYLE_CHECK_TIMEOUT, build_preview_response};
use crate::http::query::{parse_before_layer_from_query, parse_padding_from_query};
use crate::http::response::{IngressResponse, response_from_ingress_error, response_from_outcome};
use crate::http::static_image::parse_static_path;
use crate::http::tile::parse_tile_path;
use crate::node::Node;
use crate::style_catalog::StyleCatalog;
use crate::tileset_catalog::TilesetCatalog;
use crate::types::{InternalTask, RequestId, TaskId};

#[derive(Clone)]
pub struct HttpIngress {
    node: Node,
    catalog: Arc<StyleCatalog>,
    tileset_catalog: Arc<TilesetCatalog>,
    sla_budget: Duration,
    next_task_id: Arc<AtomicU64>,
    drain: Option<DrainController>,
    concurrency: Option<Arc<Semaphore>>,
    renderer_supervisor: crate::renderer::actor::RendererActorSupervisor,
}

impl HttpIngress {
    pub fn with_drain_and_limit(
        node: Node,
        catalog: Arc<StyleCatalog>,
        tileset_catalog: Arc<TilesetCatalog>,
        sla_budget: Duration,
        drain: DrainController,
        concurrency_limit: usize,
        renderer_supervisor: crate::renderer::actor::RendererActorSupervisor,
    ) -> Self {
        // Teach the (shared) node when it may start a native render; it gates
        // renders (not cache hits) inside its output-cache admission path. All
        // node handles share one `NodeInner`, so this covers both paths.
        node.set_render_admission_probe(renderer_supervisor.render_admission_probe());
        Self {
            node,
            catalog,
            tileset_catalog,
            sla_budget,
            next_task_id: Arc::new(AtomicU64::new(1)),
            drain: Some(drain),
            concurrency: Some(Arc::new(Semaphore::new(concurrency_limit.max(1)))),
            renderer_supervisor,
        }
    }

    pub fn drain_controller(&self) -> Option<DrainController> {
        self.drain.clone()
    }

    pub fn node(&self) -> Node {
        self.node.clone()
    }

    pub fn renderer_supervisor(&self) -> crate::renderer::actor::RendererActorSupervisor {
        self.renderer_supervisor.clone()
    }

    #[cfg(test)]
    pub async fn handle_path(&self, path: &str, now: Instant) -> IngressResponse {
        self.handle_path_with_request_id(path, None, None, now)
            .await
    }

    /// Serve the tile-preview HTML page for `/{user}/{style}/preview`(or
    /// single-segment `/{style}/preview`).
    ///
    /// Acquires the concurrency and drain admission guards for a request. On
    /// rejection returns the ready-to-send 503 `IngressResponse`; on success
    /// returns the guards, which the caller must hold for the request's
    /// lifetime (dropping them releases the slot).
    fn acquire_admission(
        &self,
        request_id: &RequestId,
    ) -> Result<(Option<OwnedSemaphorePermit>, Option<DrainPermit>), IngressResponse> {
        // Degraded shedding is not decided here: that would drop cache hits too.
        // The node gates it after the output-cache lookup; a miss with no usable
        // renderer is relabeled to `renderer_degraded` in the handler below.
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

    /// Returns an HTML page that embeds maplibre-gl-js (from CDN) and points it
    /// at biei's own tile endpoint as a raster source. No style.json is needed
    /// because biei serves pre-rendered raster tiles. Unknown style → 404.
    pub async fn serve_preview(
        &self,
        path: &str,
        request_id: Option<RequestId>,
    ) -> IngressResponse {
        let request_id = request_id.unwrap_or_default();
        let _admission = match self.acquire_admission(&request_id) {
            Ok(guards) => guards,
            Err(response) => return response,
        };
        let node = self.node.clone();
        build_preview_response(&self.catalog, path, |revision| {
            let node = node.clone();
            async move {
                node.ensure_style_available(&revision, Instant::now() + PREVIEW_STYLE_CHECK_TIMEOUT)
                    .await
            }
        })
        .await
        .with_request_id(&request_id)
    }

    pub async fn handle_path_with_request_id(
        &self,
        path: &str,
        query: Option<&str>,
        request_id: Option<RequestId>,
        now: Instant,
    ) -> IngressResponse {
        let request_id = request_id.unwrap_or_default();
        let admission = match self.acquire_admission(&request_id) {
            Ok(guards) => guards,
            Err(response) => return response,
        };
        let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        let task = match parse_path_with_request_id(
            path,
            query,
            &self.catalog,
            &self.tileset_catalog,
            task_id,
            request_id.clone(),
            self.sla_budget,
            now,
        ) {
            Ok(task) => task,
            Err(err) => return response_from_ingress_error(err).with_request_id(&request_id),
        };
        let node = self.node.clone();
        match tokio::spawn(async move {
            // Keep ingress/drain admission attached to the non-cancellable
            // render, not to the client connection that may disappear first.
            let _admission = admission;
            node.handle_incoming(task).await
        })
        .await
        {
            Ok(outcome) => {
                // Surface the node's degraded shed (a wire-safe `NoCapacity`)
                // as the distinct `renderer_degraded` 503; a real queue-full on
                // a renderer that can still render keeps `no_capacity`.
                if !self.renderer_supervisor.can_start_render()
                    && matches!(
                        outcome.result,
                        crate::types::TaskResult::Rejected {
                            reason: crate::types::RejectionReason::NoCapacity
                        }
                    )
                {
                    IngressResponse::json(503, "renderer_degraded", "")
                        .with_retry_after("2")
                        .with_request_id(&request_id)
                } else {
                    response_from_outcome(outcome)
                }
            }
            Err(error) => {
                tracing::error!(%error, "ingress render task terminated unexpectedly");
                IngressResponse::json(500, "internal_error", "").with_request_id(&request_id)
            }
        }
    }
}

#[cfg(test)]
fn test_tileset_catalog() -> TilesetCatalog {
    TilesetCatalog::new("https://tiles.example.test/{tileset_id}/tileset.json")
}

#[allow(clippy::too_many_arguments)]
fn parse_path_with_request_id(
    path: &str,
    query: Option<&str>,
    catalog: &StyleCatalog,
    tileset_catalog: &TilesetCatalog,
    task_id: TaskId,
    request_id: RequestId,
    sla_budget: Duration,
    now: Instant,
) -> Result<InternalTask, IngressError> {
    let parts: Vec<_> = path
        .trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    // tile / static のどちらかを segment 構造で判定する。`static` という
    // style id を tile path で使えるよう、literal の有無ではなく文法上の
    // 位置で判定する。static-only query parsing は tile path では行わない。
    if is_static_path(&parts) {
        let before_layer = parse_before_layer_from_query(query)?;
        let padding = parse_padding_from_query(query)?;
        let addlayer = parse_addlayer_from_query(query, tileset_catalog)?;
        parse_static_path(
            &parts,
            before_layer,
            padding,
            addlayer,
            catalog,
            task_id,
            request_id,
            sla_budget,
            now,
        )
    } else {
        parse_tile_path(&parts, catalog, task_id, request_id, sla_budget, now)
    }
}

fn is_static_path(parts: &[&str]) -> bool {
    // Static requests have either two suffix segments (position, size) or
    // three (overlay, position, size). Style ids may contain any number of
    // namespace segments, so classify from the suffix rather than fixed
    // indices. The three-segment form is ambiguous with a tile request whose
    // style id ends in `static`; a valid z/x/y suffix remains a tile.
    let len = parts.len();
    if len >= 4 && parts[len - 3] == "static" {
        return true;
    }
    if len >= 5 && parts[len - 4] == "static" {
        return !looks_like_user_static_tile_path(parts[len - 3], parts[len - 2], parts[len - 1]);
    }
    false
}

fn looks_like_user_static_tile_path(z: &str, x: &str, yfmt: &str) -> bool {
    z.parse::<u8>().is_ok() && x.parse::<u32>().is_ok() && parse_scale_format(yfmt).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::style_catalog::StyleDefinition;
    use crate::types::{RenderRequest, StyleId};

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
        super::parse_path_with_request_id(
            path,
            query,
            catalog,
            &test_tileset_catalog(),
            task_id,
            request_id,
            sla_budget,
            now,
        )
    }

    #[test]
    fn style_named_static_can_still_render_tiles() {
        let task = parse_path_with_request_id(
            "/carto/static/8/227/100.png",
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
    fn deeply_namespaced_style_can_render_static_images() {
        let catalog = StyleCatalog::new();
        catalog.upsert_definition(
            StyleId("carto/gl/voyager-gl-style".to_string()),
            StyleDefinition::new("https://styles.test/voyager/style.json", 1),
        );

        let task = parse_path_with_request_id(
            "/carto/gl/voyager-gl-style/static/none/139.767,35.681,11,0,0/320x240.png",
            None,
            &catalog,
            43,
            RequestId::from_string("req-nested-static-style"),
            Duration::from_secs(30),
            Instant::now(),
        )
        .expect("static path with a deeply namespaced style parses");

        assert_eq!(task.style.id.as_str(), "carto/gl/voyager-gl-style");
        assert!(matches!(task.request, RenderRequest::StaticImage { .. }));
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
    async fn degraded_renderer_sheds_uncached_render_as_renderer_degraded() {
        let options = crate::options::Options::try_parse_from([
            "biei",
            "--style-templates",
            "https://styles.test/{style_id}/style.json",
            "--cores",
            "1",
        ])
        .expect("options parse");
        let runtime = crate::runtime::Runtime::spawn_single_node(&options).expect("runtime");
        let supervisor = runtime.renderer_supervisor();
        let mut slot_available = true;
        supervisor.set_slot_available(&mut slot_available, false);
        let ingress = runtime.http_ingress(Duration::from_secs(2));

        // A valid render path that misses the (empty) output cache: the node
        // sheds the would-be render before starting native work, and the
        // ingress relabels the wire-safe `NoCapacity` shed to the distinct
        // `renderer_degraded` 503.
        let response = ingress
            .handle_path("/carto/0/0/0.png", Instant::now())
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
        let options = crate::options::Options::try_parse_from([
            "biei",
            "--style-templates",
            "https://styles.test/{style_id}/style.json",
            "--cores",
            "1",
        ])
        .expect("options parse");
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
}
