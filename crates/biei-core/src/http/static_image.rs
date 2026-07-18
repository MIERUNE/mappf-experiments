use std::time::Duration;

use tokio::time::Instant;

use crate::http::error::{IngressError, invalid};
use crate::http::format::parse_size_scale_format;
use crate::http::parse_util::percent_decode_str;
use crate::http::path::{resolve_style, resolve_style_id};
use crate::style_catalog::StyleCatalog;
use crate::types::{
    AddLayer, InternalTask, Padding, PixelRatio, Positioning, RenderRequest, RequestId, Scale,
    StaticOverlay, TaskId,
};

const MAX_STATIC_WIDTH: u16 = 1920;
const MAX_STATIC_HEIGHT: u16 = 1280;
const MAX_STATIC_RGBA_BYTES: u64 = MAX_STATIC_WIDTH as u64 * MAX_STATIC_HEIGHT as u64 * 2 * 2 * 4;

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_static_path(
    parts: &[&str],
    before_layer: Option<String>,
    padding: Option<Padding>,
    addlayer: Option<AddLayer>,
    catalog: &StyleCatalog,
    task_id: TaskId,
    request_id: RequestId,
    sla_budget: Duration,
    now: Instant,
) -> Result<InternalTask, IngressError> {
    let static_index = parts
        .iter()
        .rposition(|part| *part == "static")
        .ok_or_else(|| invalid("static path is missing the static segment"))?;
    let style_id = resolve_style_id(&parts[..static_index])?;
    let (overlays, position, size_format) = match &parts[static_index..] {
        ["static", position, size_format] => (Vec::new(), *position, *size_format),
        ["static", overlay, position, size_format] => {
            let overlays = parse_static_overlays(overlay)?;
            (overlays, *position, *size_format)
        }
        _ => {
            return Err(invalid(
                "static path must be /{style_id}/static/[{overlay}/]{position}/{width}x{height}{@scale}.{format}",
            ));
        }
    };

    let style = resolve_style(catalog, style_id)?;
    let positioning = parse_positioning(position)?;
    let (width, height, scale, output_format) = parse_size_scale_format(size_format)?;
    validate_static_dimensions(width, height, scale)?;
    // `auto` positioning fits the camera to the union of overlay
    // geometries; with zero overlays there is nothing to fit.
    if matches!(positioning, Positioning::Auto) && overlays.is_empty() {
        return Err(invalid(
            "auto positioning requires at least one overlay with geometry",
        ));
    }
    let padding =
        padding.unwrap_or_else(|| default_padding_for_positioning(positioning, width, height));

    Ok(InternalTask {
        id: task_id,
        request_id,
        style: style.revision,
        source: None,
        request: RenderRequest::StaticImage {
            positioning,
            width,
            height,
            overlays,
            before_layer,
            padding,
            addlayer,
        },
        pixel_ratio: PixelRatio::from(scale),
        output_format,
        arrived_at: now,
        deadline: now + sla_budget,
        forwarding_hops: 0,
    })
}

fn default_padding_for_positioning(positioning: Positioning, width: u16, height: u16) -> Padding {
    match positioning {
        Positioning::Auto => Padding {
            top: five_percent_ceil(height),
            right: five_percent_ceil(width),
            bottom: five_percent_ceil(height),
            left: five_percent_ceil(width),
        },
        Positioning::Center { .. } | Positioning::Bbox { .. } => Padding::default(),
    }
}

fn five_percent_ceil(value: u16) -> u16 {
    value.saturating_add(19) / 20
}

fn parse_static_overlays(overlay: &str) -> Result<Vec<StaticOverlay>, IngressError> {
    crate::http::overlay::parse_static_overlays(overlay)
        .map_err(|err| invalid(format!("invalid static overlay: {err}")))
}

// Geographic + camera ranges accepted by ingress. Out-of-range values
// would otherwise propagate into MapLibre Native as an uncaught
// `std::domain_error` and crash the process, since the cxx bridge doesn't
// catch C++ exceptions for these paths.
const POSITION_MIN_LON: f64 = -180.0;
const POSITION_MAX_LON: f64 = 180.0;
const POSITION_MIN_LAT: f64 = -90.0;
const POSITION_MAX_LAT: f64 = 90.0;
const POSITION_MIN_ZOOM: f64 = 0.0;
/// Highest zoom MapLibre Native supports. Above this mbgl throws
/// `std::domain_error` from tile coordinate construction.
const POSITION_MAX_ZOOM: f64 = 24.0;
/// mbgl clamps pitch internally but throws on far-out values from the
/// JSON style parser. Pick a value comfortably above the rendered range
/// (~60°) but below anything that would trip native validation.
const POSITION_MAX_PITCH: f32 = 85.0;

fn parse_positioning(value: &str) -> Result<Positioning, IngressError> {
    let decoded;
    let value = if value.as_bytes().contains(&b'%') {
        decoded = percent_decode_str(value)
            .map_err(|_| invalid("position must be valid percent-encoded UTF-8"))?;
        decoded.as_str()
    } else {
        value
    };

    if value == "auto" {
        return Ok(Positioning::Auto);
    }

    if let Some(bbox) = value.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        return parse_bbox_positioning(bbox);
    }

    let coords: Vec<_> = value.split(',').collect();
    let (lon, lat, zoom, bearing, pitch) = match coords.as_slice() {
        [lon, lat, zoom] | [lon, lat, zoom, "0"] | [lon, lat, zoom, "0", "0"] => (
            parse_f64(lon, "lon")?,
            parse_f64(lat, "lat")?,
            parse_f64(zoom, "zoom")?,
            0.0_f32,
            0.0_f32,
        ),
        [lon, lat, zoom, bearing] => (
            parse_f64(lon, "lon")?,
            parse_f64(lat, "lat")?,
            parse_f64(zoom, "zoom")?,
            parse_f32(bearing, "bearing")?,
            0.0,
        ),
        [lon, lat, zoom, bearing, pitch] => (
            parse_f64(lon, "lon")?,
            parse_f64(lat, "lat")?,
            parse_f64(zoom, "zoom")?,
            parse_f32(bearing, "bearing")?,
            parse_f32(pitch, "pitch")?,
        ),
        _ => {
            return Err(invalid(
                "position must be auto, [min_lon,min_lat,max_lon,max_lat], or lon,lat,zoom[,bearing[,pitch]]",
            ));
        }
    };

    validate_lon(lon, "lon")?;
    validate_lat(lat, "lat")?;
    validate_zoom(zoom)?;
    validate_pitch(pitch)?;
    Ok(Positioning::Center {
        lon,
        lat,
        zoom,
        bearing,
        pitch,
    })
}

fn parse_bbox_positioning(value: &str) -> Result<Positioning, IngressError> {
    let coords: Vec<_> = value.split(',').collect();
    let [min_lon, min_lat, max_lon, max_lat] = coords.as_slice() else {
        return Err(invalid("bbox must be [min_lon,min_lat,max_lon,max_lat]"));
    };
    let min_lon = parse_f64(min_lon, "min_lon")?;
    let min_lat = parse_f64(min_lat, "min_lat")?;
    let max_lon = parse_f64(max_lon, "max_lon")?;
    let max_lat = parse_f64(max_lat, "max_lat")?;
    validate_lon(min_lon, "min_lon")?;
    validate_lon(max_lon, "max_lon")?;
    validate_lat(min_lat, "min_lat")?;
    validate_lat(max_lat, "max_lat")?;
    if min_lon > max_lon {
        return Err(invalid("bbox min_lon must be <= max_lon"));
    }
    if min_lat > max_lat {
        return Err(invalid("bbox min_lat must be <= max_lat"));
    }
    Ok(Positioning::Bbox {
        min_lon,
        min_lat,
        max_lon,
        max_lat,
    })
}

fn validate_lon(value: f64, name: &str) -> Result<(), IngressError> {
    if (POSITION_MIN_LON..=POSITION_MAX_LON).contains(&value) {
        Ok(())
    } else {
        Err(invalid(format!(
            "{name} must be in [{POSITION_MIN_LON}, {POSITION_MAX_LON}]"
        )))
    }
}

fn validate_lat(value: f64, name: &str) -> Result<(), IngressError> {
    if (POSITION_MIN_LAT..=POSITION_MAX_LAT).contains(&value) {
        Ok(())
    } else {
        Err(invalid(format!(
            "{name} must be in [{POSITION_MIN_LAT}, {POSITION_MAX_LAT}]"
        )))
    }
}

fn validate_zoom(value: f64) -> Result<(), IngressError> {
    if (POSITION_MIN_ZOOM..=POSITION_MAX_ZOOM).contains(&value) {
        Ok(())
    } else {
        Err(invalid(format!(
            "zoom must be in [{POSITION_MIN_ZOOM}, {POSITION_MAX_ZOOM}]"
        )))
    }
}

fn validate_pitch(value: f32) -> Result<(), IngressError> {
    if (0.0..=POSITION_MAX_PITCH).contains(&value) {
        Ok(())
    } else {
        Err(invalid(format!(
            "pitch must be in [0, {POSITION_MAX_PITCH}]"
        )))
    }
}

fn validate_static_dimensions(width: u16, height: u16, scale: Scale) -> Result<(), IngressError> {
    if width == 0 || height == 0 {
        return Err(invalid("static width and height must be positive"));
    }
    if width > MAX_STATIC_WIDTH {
        return Err(invalid("static width must be <= 1920"));
    }
    if height > MAX_STATIC_HEIGHT {
        return Err(invalid("static height must be <= 1280"));
    }
    let scale_multiplier = match scale {
        Scale::X1 => 1_u64,
        Scale::X2 => 2,
    };
    let bytes = width as u64 * height as u64 * scale_multiplier * scale_multiplier * 4;
    if bytes > MAX_STATIC_RGBA_BYTES {
        return Err(invalid("static raw RGBA size exceeds limit"));
    }
    Ok(())
}

fn parse_f64(value: &str, name: &str) -> Result<f64, IngressError> {
    value
        .parse::<f64>()
        .map_err(|_| invalid(format!("{name} must be a number")))
}

fn parse_f32(value: &str, name: &str) -> Result<f32, IngressError> {
    value
        .parse::<f32>()
        .map_err(|_| invalid(format!("{name} must be a number")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::style_catalog::StyleDefinition;
    use crate::types::{ImageFormat, StyleId};

    fn catalog() -> StyleCatalog {
        let catalog = StyleCatalog::new();
        catalog.upsert_definition(
            StyleId("voyager-gl-style".to_string()),
            StyleDefinition::new(
                "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
                1,
            ),
        );
        catalog.upsert_definition(
            StyleId("carto/voyager-gl-style".to_string()),
            StyleDefinition::new(
                "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
                1,
            ),
        );
        catalog
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_static_with_options(
        path: &str,
        before_layer: Option<String>,
        padding: Option<Padding>,
        addlayer: Option<AddLayer>,
        task_id: TaskId,
        now: Instant,
    ) -> Result<InternalTask, IngressError> {
        let parts: Vec<_> = path
            .trim_start_matches('/')
            .trim_end_matches('/')
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        parse_static_path(
            &parts,
            before_layer,
            padding,
            addlayer,
            &catalog(),
            task_id,
            RequestId::default(),
            Duration::from_secs(30),
            now,
        )
    }

    fn parse_static(path: &str) -> Result<InternalTask, IngressError> {
        parse_static_with_options(path, None, None, None, 42, Instant::now())
    }

    #[test]
    fn parses_static_path_with_single_segment_style_id() {
        let task =
            parse_static("/voyager-gl-style/static/none/139.767,35.681,12,0,0/512x384@2x.png")
                .expect("static path parses");

        assert_eq!(task.style.id.as_str(), "voyager-gl-style");
        assert_eq!(task.pixel_ratio.to_scale(), Scale::X2);
        assert_eq!(task.output_format, ImageFormat::Png);
        assert!(matches!(
            task.request,
            RenderRequest::StaticImage {
                positioning: Positioning::Center { .. },
                width: 512,
                height: 384,
                ..
            }
        ));
    }

    #[test]
    fn parses_static_without_extension_as_png() {
        let task = parse_static("/voyager-gl-style/static/none/139.767,35.681,12,0,0/512x384@2x")
            .expect("static path without extension parses");

        assert_eq!(task.output_format, ImageFormat::Png);
        assert_eq!(task.pixel_ratio.to_scale(), Scale::X2);
    }

    #[test]
    fn parses_static_jpg() {
        let task = parse_static("/voyager-gl-style/static/none/139.767,35.681,12,0,0/512x384.jpg")
            .expect("static jpg path parses");

        assert_eq!(task.output_format, ImageFormat::Jpeg);
        assert_eq!(task.pixel_ratio.to_scale(), Scale::X1);
    }

    #[test]
    fn parses_static_path_with_bbox_positioning() {
        let task =
            parse_static("/voyager-gl-style/static/none/[139.7,35.6,139.9,35.8]/512x384.png")
                .expect("static bbox path parses");

        assert_eq!(
            task.request,
            RenderRequest::StaticImage {
                positioning: Positioning::Bbox {
                    min_lon: 139.7,
                    min_lat: 35.6,
                    max_lon: 139.9,
                    max_lat: 35.8,
                },
                width: 512,
                height: 384,
                overlays: Vec::new(),
                before_layer: None,
                padding: Padding::default(),
                addlayer: None,
            }
        );
    }

    #[test]
    fn parses_static_bbox_without_overlay_segment() {
        let task = parse_static("/voyager-gl-style/static/[139.7,35.6,139.9,35.8]/512x384.png")
            .expect("static bbox path without overlay parses");

        assert_eq!(
            task.request,
            RenderRequest::StaticImage {
                positioning: Positioning::Bbox {
                    min_lon: 139.7,
                    min_lat: 35.6,
                    max_lon: 139.9,
                    max_lat: 35.8,
                },
                width: 512,
                height: 384,
                overlays: Vec::new(),
                before_layer: None,
                padding: Padding::default(),
                addlayer: None,
            }
        );
    }

    #[test]
    fn parses_static_bbox_with_percent_encoded_brackets() {
        let task = parse_static("/voyager-gl-style/static/%5B139.7,35.6,139.9,35.8%5D/512x384.png")
            .expect("static bbox path with encoded brackets parses");

        assert!(matches!(
            task.request,
            RenderRequest::StaticImage {
                positioning: Positioning::Bbox {
                    min_lon: 139.7,
                    min_lat: 35.6,
                    max_lon: 139.9,
                    max_lat: 35.8,
                },
                ..
            }
        ));
    }

    #[test]
    fn parses_static_center_png_2x() {
        let now = Instant::now();
        let task = parse_static_with_options(
            "/carto/voyager-gl-style/static/139.767,35.681,12,0,0/512x384@2x.png",
            None,
            None,
            None,
            42,
            now,
        )
        .expect("static path parses");

        assert_eq!(task.id, 42);
        assert_eq!(task.style.id.as_str(), "carto/voyager-gl-style");
        assert_eq!(task.pixel_ratio.to_scale(), Scale::X2);
        assert_eq!(task.output_format, ImageFormat::Png);
        assert_eq!(
            task.request,
            RenderRequest::StaticImage {
                positioning: Positioning::Center {
                    lon: 139.767,
                    lat: 35.681,
                    zoom: 12.0,
                    bearing: 0.0,
                    pitch: 0.0,
                },
                width: 512,
                height: 384,
                overlays: Vec::new(),
                before_layer: None,
                padding: Padding::default(),
                addlayer: None,
            }
        );
        assert_eq!(task.arrived_at, now);
        assert_eq!(task.deadline, now + Duration::from_secs(30));
    }

    #[test]
    fn parses_auto_positioning_with_overlay() {
        let task = parse_static(
            "/voyager-gl-style/static/path-2+f44(_p~iF~ps|U_ulLnnqC_mqNvxq%60@)/auto/512x384.png",
        )
        .expect("auto with one overlay parses");
        assert!(matches!(
            task.request,
            RenderRequest::StaticImage {
                positioning: Positioning::Auto,
                padding: Padding {
                    top: 20,
                    right: 26,
                    bottom: 20,
                    left: 26,
                },
                ..
            }
        ));
    }

    #[test]
    fn parses_auto_positioning_with_padding_query() {
        let task = parse_static_with_options(
            "/voyager-gl-style/static/path-2+f44(_p~iF~ps|U_ulLnnqC_mqNvxq%60@)/auto/512x384.png",
            None,
            Some(Padding::all(40)),
            None,
            1,
            Instant::now(),
        )
        .expect("auto+padding parses");
        if let RenderRequest::StaticImage { padding, .. } = task.request {
            assert_eq!(padding, Padding::all(40));
        } else {
            panic!("expected StaticImage");
        }
    }

    #[test]
    fn rejects_auto_positioning_without_overlays() {
        let err = parse_static("/carto/voyager-gl-style/static/none/auto/256x256.webp")
            .expect_err("auto with no overlays must be rejected");
        assert!(err.to_string().contains("auto positioning"));
    }

    #[test]
    fn parses_static_with_none_overlay() {
        let task = parse_static("/carto/voyager-gl-style/static/none/139.7,35.6,12/256x256.webp")
            .expect("static path parses");

        assert_eq!(task.output_format, ImageFormat::Webp);
        assert_eq!(task.pixel_ratio.to_scale(), Scale::X1);
        assert!(matches!(
            task.request,
            RenderRequest::StaticImage {
                positioning: Positioning::Center { .. },
                overlays: ref o,
                ..
            } if o.is_empty()
        ));
    }

    #[test]
    fn parses_static_path_overlay() {
        let task = parse_static(
            "/voyager-gl-style/static/path-5+f44-0.5(_p~iF~ps%7CU)/139.767,35.681,12/256x256.png",
        )
        .expect("path overlay parses");

        let RenderRequest::StaticImage { overlays, .. } = task.request else {
            panic!("expected static image request");
        };
        assert_eq!(overlays.len(), 1);
    }

    #[test]
    fn parses_static_pin_overlay() {
        let task = parse_static("/voyager-gl-style/static/pin-s-a+9ed4bd(139,35)/auto/256x256.png")
            .expect("pin overlay parses");

        let RenderRequest::StaticImage { overlays, .. } = task.request else {
            panic!("expected static image request");
        };
        assert_eq!(overlays.len(), 1);
    }

    #[test]
    fn rejects_unknown_style() {
        let err =
            parse_static("/carto/unknown/static/auto/256x256.png").expect_err("style is unknown");

        assert!(matches!(err, IngressError::UnknownStyle(_)));
    }

    #[test]
    fn rejects_invalid_format_and_scale() {
        let err = parse_static("/carto/voyager-gl-style/static/auto/256x256.gif")
            .expect_err("format is invalid");

        assert!(err.to_string().contains("format"));
    }

    #[test]
    fn rejects_static_dimension_over_limit() {
        let err = parse_static("/carto/voyager-gl-style/static/auto/1921x256.png")
            .expect_err("dimension is too large");

        assert!(err.to_string().contains("<= 1920"));
    }

    #[test]
    fn rejects_static_lat_out_of_range() {
        let err = parse_static("/voyager-gl-style/static/130,140,12,0/512x384.png")
            .expect_err("lat above 90 must be rejected");
        assert!(err.to_string().contains("lat"));
    }

    #[test]
    fn rejects_static_lon_out_of_range() {
        let err = parse_static("/voyager-gl-style/static/250,35,12,0/512x384.png")
            .expect_err("lon above 180 must be rejected");
        assert!(err.to_string().contains("lon"));
    }

    #[test]
    fn rejects_static_zoom_out_of_range() {
        let err = parse_static("/voyager-gl-style/static/139.7,35.6,30/512x384.png")
            .expect_err("zoom above 24 must be rejected");
        assert!(err.to_string().contains("zoom"));
    }

    #[test]
    fn rejects_static_pitch_out_of_range() {
        let err = parse_static("/voyager-gl-style/static/139.7,35.6,12,0,90/512x384.png")
            .expect_err("pitch above 85 must be rejected");
        assert!(err.to_string().contains("pitch"));
    }

    #[test]
    fn rejects_static_bbox_with_lat_out_of_range() {
        let err = parse_static("/voyager-gl-style/static/[139.7,-95,139.9,35.8]/512x384.png")
            .expect_err("bbox min_lat below -90 must be rejected");
        assert!(err.to_string().contains("min_lat"));
    }

    #[test]
    fn rejects_static_bbox_with_swapped_bounds() {
        let err = parse_static("/voyager-gl-style/static/[140.0,35.6,139.7,35.8]/512x384.png")
            .expect_err("bbox min_lon > max_lon must be rejected");
        assert!(err.to_string().contains("min_lon"));
    }

    #[test]
    fn parse_static_threads_before_layer_into_render_request() {
        let task = parse_static_with_options(
            "/voyager-gl-style/static/-122,37,9/512x384.png",
            Some("labels".to_string()),
            None,
            None,
            42,
            Instant::now(),
        )
        .expect("static URL parses");

        match task.request {
            RenderRequest::StaticImage { before_layer, .. } => {
                assert_eq!(before_layer.as_deref(), Some("labels"));
            }
            _ => panic!("expected StaticImage"),
        }
    }

    #[test]
    fn parse_static_defaults_before_layer_to_none_when_query_absent() {
        let task = parse_static("/voyager-gl-style/static/-122,37,9/512x384.png")
            .expect("static URL parses");

        match task.request {
            RenderRequest::StaticImage { before_layer, .. } => assert!(before_layer.is_none()),
            _ => panic!("expected StaticImage"),
        }
    }

    #[test]
    fn parse_static_threads_addlayer_into_render_request() {
        let json = r#"{"id":"my-line","type":"line","source":"composite"}"#;
        let addlayer = AddLayer {
            json: json.to_string(),
            hash: 123,
            source: None,
        };
        let task = parse_static_with_options(
            "/voyager-gl-style/static/none/139.7,35.6,12/256x256.webp",
            None,
            None,
            Some(addlayer),
            1,
            Instant::now(),
        )
        .expect("addlayer threads through static parser");
        if let RenderRequest::StaticImage { addlayer, .. } = task.request {
            let a = addlayer.expect("addlayer present");
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&a.json).unwrap(),
                serde_json::from_str::<serde_json::Value>(json).unwrap()
            );
        } else {
            panic!("expected StaticImage");
        }
    }
}
