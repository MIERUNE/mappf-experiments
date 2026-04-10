//! HTTP handlers and response helpers for tileset endpoints.

mod error;
pub mod mapterhorn;
mod mlt;
mod preview;
mod tile;
mod tilejson;

pub(crate) use error::tileset_error_response;
pub(crate) use preview::{
    namespaced_preview_handler, namespaced_preview_style_handler, preview_handler,
    preview_style_handler, render_preview_html,
};
pub(crate) use tile::{internal_tile_handler, namespaced_tile_handler, tile_handler};
pub(crate) use tilejson::{namespaced_tilejson_handler, tilejson_handler};

/// Joins a namespaced route's `(namespace, tileset_id)` path segments into the
/// flat `namespace/tileset_id` key the `serve_*` helpers expect. One home for
/// the join convention shared by the namespaced tile/tilejson/preview handlers.
fn join_tileset_key(namespace: &str, tileset_id: &str) -> String {
    format!("{namespace}/{tileset_id}")
}
