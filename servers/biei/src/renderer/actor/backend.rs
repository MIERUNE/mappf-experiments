//! MapLibre Native-specific renderer construction and blocking render backend.

use std::path::PathBuf;

use biei_core::types::{PixelRatio, RenderRequest, RendererError, StyleRevision};

use super::addlayer::{AddLayerSourceCache, render_static_with_overlays_and_addlayer};
use super::camera::{auto_padding_for_overlays, padding_to_edge_insets};
use super::encode::encode_image;
use super::{BlockingRenderBackend, RenderTaskView, ResolvedStyle};
use crate::renderer::RendererOutput;
use crate::renderer::overlay::{OverlaySlotPool, build_overlay_geojson, populate_static_slots};

pub(super) struct MapLibreNativeBackend {
    loaded_style: Option<ResolvedStyle>,
    active_renderer: Option<ActiveRenderer>,
    ambient_cache_path: Option<PathBuf>,
}

enum ActiveRenderer {
    Static {
        key: RendererKey,
        loaded_style: Option<StyleRevision>,
        renderer: maplibre_native::ImageRenderer<maplibre_native::Static>,
        /// Pre-allocated overlay slots (style-setup-time fixed). Per-request
        /// overlay rendering only updates each slot's GeoJSON source via
        /// `source_mut(...).set_geojson(...)` and never adds/removes layers,
        /// so per-request expression-compile cost is paid once at style load.
        slots: OverlaySlotPool,
        /// Stable request-local addlayer sources kept on the loaded style.
        /// Request-local layers are removed after each render; unreferenced
        /// sources are harmless and let repeated tilesets avoid add_source.
        addlayer_sources: AddLayerSourceCache,
    },
    Tile {
        key: RendererKey,
        loaded_style: Option<StyleRevision>,
        renderer: maplibre_native::ImageRenderer<maplibre_native::Tile>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RendererKey {
    render_mode: biei_core::types::RenderMode,
    pixel_ratio_bits: u32,
}

impl RendererKey {
    fn new(render_mode: biei_core::types::RenderMode, pixel_ratio: PixelRatio) -> Self {
        Self {
            render_mode,
            pixel_ratio_bits: pixel_ratio.as_f32().to_bits(),
        }
    }

    fn pixel_ratio(self) -> f32 {
        f32::from_bits(self.pixel_ratio_bits)
    }
}

impl MapLibreNativeBackend {
    pub(super) fn new(ambient_cache_path: Option<PathBuf>) -> Self {
        Self {
            loaded_style: None,
            active_renderer: None,
            ambient_cache_path,
        }
    }

    fn style(&self) -> Result<&ResolvedStyle, RendererError> {
        self.loaded_style.as_ref().ok_or_else(|| {
            RendererError::RenderFailed("style has not been loaded in renderer backend".to_string())
        })
    }

    fn ensure_static_renderer(
        &mut self,
        key: RendererKey,
        size: maplibre_native::Size,
    ) -> Result<
        (
            &mut maplibre_native::ImageRenderer<maplibre_native::Static>,
            &mut OverlaySlotPool,
            &mut AddLayerSourceCache,
        ),
        RendererError,
    > {
        let style = self.style()?.clone();
        // Rebuild when there is no matching static renderer, or when a prior
        // overlay-recovery failure left the slot pool's layer topology corrupt
        // (a re-move cannot recreate a dropped layer, so the whole renderer must
        // be rebuilt before another render).
        let needs_rebuild = match &self.active_renderer {
            Some(ActiveRenderer::Static {
                key: existing,
                slots,
                ..
            }) => *existing != key || slots.needs_rebuild(),
            _ => true,
        };
        if needs_rebuild {
            let mut renderer = build_renderer(key, size, self.ambient_cache_path.as_deref())?
                .build_static_renderer();
            load_style_json(&mut renderer, &style)?;
            // Slot installation is renderer-local setup on an already-loaded
            // style document; a failure here is `SetupFailed`, not
            // `StyleLoadFailed`, so it cannot negative-cache the revision.
            let slots =
                populate_static_slots(&mut renderer).map_err(|err| RendererError::SetupFailed {
                    style_id: style.revision.id.clone(),
                    source: err.to_string(),
                })?;
            self.active_renderer = Some(ActiveRenderer::Static {
                key,
                loaded_style: Some(style.revision.clone()),
                renderer,
                slots,
                addlayer_sources: AddLayerSourceCache::new(),
            });
        }
        let Some(ActiveRenderer::Static {
            loaded_style,
            renderer,
            slots,
            addlayer_sources,
            ..
        }) = self.active_renderer.as_mut()
        else {
            unreachable!("static renderer was inserted")
        };
        if loaded_style.as_ref() != Some(&style.revision) {
            load_style_json(renderer, &style)?;
            *loaded_style = Some(style.revision.clone());
            *slots = populate_static_slots(renderer).map_err(|err| RendererError::SetupFailed {
                style_id: style.revision.id.clone(),
                source: err.to_string(),
            })?;
            *addlayer_sources = AddLayerSourceCache::new();
        }
        renderer.set_map_size(size);
        Ok((renderer, slots, addlayer_sources))
    }

    fn ensure_tile_renderer(
        &mut self,
        key: RendererKey,
        size: maplibre_native::Size,
    ) -> Result<&mut maplibre_native::ImageRenderer<maplibre_native::Tile>, RendererError> {
        let style = self.style()?.clone();
        let needs_rebuild = !matches!(
            self.active_renderer,
            Some(ActiveRenderer::Tile { key: existing, .. }) if existing == key
        );
        if needs_rebuild {
            let mut renderer = build_renderer(key, size, self.ambient_cache_path.as_deref())?
                .build_tile_renderer();
            load_style_json(&mut renderer, &style)?;
            self.active_renderer = Some(ActiveRenderer::Tile {
                key,
                loaded_style: Some(style.revision.clone()),
                renderer,
            });
        }
        let Some(ActiveRenderer::Tile {
            loaded_style,
            renderer,
            ..
        }) = self.active_renderer.as_mut()
        else {
            unreachable!("tile renderer was inserted")
        };
        if loaded_style.as_ref() != Some(&style.revision) {
            load_style_json(renderer, &style)?;
            *loaded_style = Some(style.revision.clone());
        }
        renderer.set_map_size(size);
        Ok(renderer)
    }

    fn ensure_renderer_for_task(&mut self, task: &RenderTaskView) -> Result<(), RendererError> {
        match task.request {
            RenderRequest::Tile { tile_size, .. } => {
                let key = RendererKey::new(biei_core::types::RenderMode::Tile, task.pixel_ratio);
                let size = render_size(tile_size, tile_size)?;
                self.ensure_tile_renderer(key, size)?;
            }
            RenderRequest::StaticImage { width, height, .. } => {
                let key = RendererKey::new(biei_core::types::RenderMode::Static, task.pixel_ratio);
                let size = render_size(width, height)?;
                self.ensure_static_renderer(key, size)?;
            }
        }
        Ok(())
    }

    fn reset_loaded_state(&mut self) {
        self.loaded_style = None;
        match self.active_renderer.as_mut() {
            Some(ActiveRenderer::Static { loaded_style, .. })
            | Some(ActiveRenderer::Tile { loaded_style, .. }) => {
                *loaded_style = None;
            }
            None => {}
        }
    }
}

impl BlockingRenderBackend for MapLibreNativeBackend {
    fn load_profile(
        &mut self,
        style: &ResolvedStyle,
        task: &RenderTaskView,
    ) -> Result<(), RendererError> {
        self.loaded_style = Some(style.clone());
        self.ensure_renderer_for_task(task)
    }

    fn render(&mut self, task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
        let (image, source_setup_duration) = match task.request {
            RenderRequest::Tile { z, x, y, tile_size } => {
                let key = RendererKey::new(biei_core::types::RenderMode::Tile, task.pixel_ratio);
                let size = render_size(tile_size, tile_size)?;
                self.ensure_tile_renderer(key, size)?
                    .render_tile(z, x, y)
                    .map(|image| (image, None))
            }
            RenderRequest::StaticImage {
                positioning:
                    biei_core::types::Positioning::Center {
                        lon,
                        lat,
                        zoom,
                        bearing,
                        pitch,
                    },
                width,
                height,
                ref overlays,
                ref before_layer,
                padding: _,
                ref addlayer,
            } => {
                let key = RendererKey::new(biei_core::types::RenderMode::Static, task.pixel_ratio);
                let size = render_size(width, height)?;
                let (renderer, slots, addlayer_sources) = self.ensure_static_renderer(key, size)?;
                let camera = maplibre_native::CameraUpdate::new()
                    .center(maplibre_native::LatLng { lat, lng: lon })
                    .zoom(zoom)
                    .bearing(f64::from(bearing))
                    .pitch(f64::from(pitch));
                render_static_with_overlays_and_addlayer(
                    renderer,
                    slots,
                    addlayer_sources,
                    &camera,
                    overlays,
                    None,
                    before_layer.as_deref(),
                    addlayer.as_ref(),
                    task.id,
                )
            }
            RenderRequest::StaticImage {
                positioning:
                    biei_core::types::Positioning::Bbox {
                        min_lon,
                        min_lat,
                        max_lon,
                        max_lat,
                    },
                width,
                height,
                ref overlays,
                ref before_layer,
                padding,
                ref addlayer,
            } => {
                let key = RendererKey::new(biei_core::types::RenderMode::Static, task.pixel_ratio);
                let size = render_size(width, height)?;
                let (renderer, slots, addlayer_sources) = self.ensure_static_renderer(key, size)?;
                let bounds = maplibre_native::LatLngBounds {
                    southwest: maplibre_native::LatLng {
                        lat: min_lat,
                        lng: min_lon,
                    },
                    northeast: maplibre_native::LatLng {
                        lat: max_lat,
                        lng: max_lon,
                    },
                };
                let camera = renderer.camera_for_bounds(
                    bounds,
                    Some(padding_to_edge_insets(padding)),
                    0.0,
                    0.0,
                );
                render_static_with_overlays_and_addlayer(
                    renderer,
                    slots,
                    addlayer_sources,
                    &camera,
                    overlays,
                    None,
                    before_layer.as_deref(),
                    addlayer.as_ref(),
                    task.id,
                )
            }
            RenderRequest::StaticImage {
                positioning: biei_core::types::Positioning::Auto,
                width,
                height,
                ref overlays,
                ref before_layer,
                padding,
                ref addlayer,
            } => {
                let key = RendererKey::new(biei_core::types::RenderMode::Static, task.pixel_ratio);
                let size = render_size(width, height)?;
                let (renderer, slots, addlayer_sources) = self.ensure_static_renderer(key, size)?;
                // Build, serialize, and parse the indexed union once. Slot
                // assignment installs this same GeoJSON after mbgl fits the
                // camera to its geometry.
                let overlay_geojson = build_overlay_geojson(overlays)
                    .map_err(|err| RendererError::RenderFailed(err.to_string()))?;
                let auto_padding = padding_to_edge_insets(auto_padding_for_overlays(
                    padding, overlays, width, height,
                ));
                let Some(camera) =
                    renderer.camera_for_geojson(&overlay_geojson, Some(auto_padding), 0.0, 0.0)
                else {
                    return Err(RendererError::RenderFailed(
                        "auto positioning: overlays produced no fittable geometry".to_string(),
                    ));
                };
                render_static_with_overlays_and_addlayer(
                    renderer,
                    slots,
                    addlayer_sources,
                    &camera,
                    overlays,
                    Some(&overlay_geojson),
                    before_layer.as_deref(),
                    addlayer.as_ref(),
                    task.id,
                )
            }
        }
        .map_err(|err| RendererError::RenderFailed(err.to_string()))?;

        Ok(RendererOutput {
            output: encode_image(&image, task.output_format)?,
            source_setup_duration,
        })
    }

    fn error_invalidates_loaded_state(&self, err: &RendererError) -> bool {
        !matches!(err, RendererError::RenderFailed(_))
    }

    fn reset(&mut self) {
        self.reset_loaded_state();
    }
}

fn build_renderer(
    key: RendererKey,
    size: maplibre_native::Size,
    ambient_cache_path: Option<&std::path::Path>,
) -> Result<maplibre_native::ImageRendererBuilder, RendererError> {
    use std::num::NonZeroU32;

    let width = NonZeroU32::new(size.width)
        .ok_or_else(|| RendererError::RenderFailed("render width must be non-zero".to_string()))?;
    let height = NonZeroU32::new(size.height)
        .ok_or_else(|| RendererError::RenderFailed("render height must be non-zero".to_string()))?;

    let mut builder = maplibre_native::ImageRendererBuilder::new()
        .with_size(width, height)
        .with_pixel_ratio(key.pixel_ratio());
    if let Some(path) = ambient_cache_path {
        let resource_options =
            maplibre_native::ResourceOptions::default().with_cache_path(path.to_path_buf());
        builder = builder.with_resource_options(resource_options);
    }
    Ok(builder)
}

fn render_size(width: u16, height: u16) -> Result<maplibre_native::Size, RendererError> {
    if width == 0 || height == 0 {
        return Err(RendererError::RenderFailed(
            "render size must be non-zero".to_string(),
        ));
    }
    Ok(maplibre_native::Size {
        width: u32::from(width),
        height: u32::from(height),
    })
}

fn load_style_json<S>(
    renderer: &mut maplibre_native::ImageRenderer<S>,
    style: &ResolvedStyle,
) -> Result<(), RendererError> {
    renderer
        .load_style_from_json_str(&style.style_json)
        .wait()
        .map_err(|error| RendererError::StyleLoadFailed {
            style_id: style.revision.id.clone(),
            source: error.to_string(),
        })
}
