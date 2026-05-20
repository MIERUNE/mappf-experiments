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
}

impl HttpIngress {
    pub fn with_drain_and_limit(
        node: Node,
        catalog: Arc<StyleCatalog>,
        tileset_catalog: Arc<TilesetCatalog>,
        sla_budget: Duration,
        drain: DrainController,
        concurrency_limit: usize,
    ) -> Self {
        Self {
            node,
            catalog,
            tileset_catalog,
            sla_budget,
            next_task_id: Arc::new(AtomicU64::new(1)),
            drain: Some(drain),
            concurrency: Some(Arc::new(Semaphore::new(concurrency_limit.max(1)))),
        }
    }

    pub fn drain_controller(&self) -> Option<DrainController> {
        self.drain.clone()
    }

    pub fn node(&self) -> Node {
        self.node.clone()
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
        let _admission = match self.acquire_admission(&request_id) {
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
        response_from_outcome(self.node.handle_incoming(task).await)
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
    match parts {
        [_, "static", _, _] => true,
        [_, "static", z, x, yfmt] if looks_like_user_static_tile_path(z, x, yfmt) => false,
        [_, "static", _, _, _] => true,
        [_, _, "static", _, _] => true,
        [_, _, "static", _, _, _] => true,
        _ => false,
    }
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
}
