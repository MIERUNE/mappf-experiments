use std::time::Duration;

use tokio::time::Instant;

use crate::http::error::{IngressError, invalid};
use crate::http::format::parse_scale_format;
use crate::http::path::{resolve_style, resolve_style_id};
use crate::style_catalog::StyleCatalog;
use crate::types::{InternalTask, PixelRatio, RenderRequest, RequestId, TaskId};

pub(crate) const TILE_SIZE: u16 = 512;

pub(crate) fn parse_tile_path(
    parts: &[&str],
    catalog: &StyleCatalog,
    task_id: TaskId,
    request_id: RequestId,
    sla_budget: Duration,
    now: Instant,
) -> Result<InternalTask, IngressError> {
    let suffix_index = parts
        .len()
        .checked_sub(3)
        .ok_or_else(|| invalid("tile path must be /{style_id}/{z}/{x}/{y}{@scale}.{format}"))?;
    let style_id = resolve_style_id(&parts[..suffix_index])?;
    let [z_str, x_str, yfmt_str] = parts[suffix_index..] else {
        unreachable!("suffix_index leaves exactly three tile segments")
    };
    let style = resolve_style(catalog, style_id)?;
    let z = z_str
        .parse::<u8>()
        .map_err(|_| invalid("tile z must be an integer in 0..=255"))?;
    let x = x_str
        .parse::<u32>()
        .map_err(|_| invalid("tile x must be an integer"))?;
    let (y, scale, output_format) = parse_scale_format(yfmt_str)?;
    validate_tile_coordinate(z, x, y)?;

    Ok(InternalTask {
        id: task_id,
        request_id,
        style: style.revision,
        source: None,
        request: RenderRequest::Tile {
            z,
            x,
            y,
            tile_size: TILE_SIZE,
        },
        pixel_ratio: PixelRatio::from(scale),
        output_format,
        arrived_at: now,
        deadline: now + sla_budget,
        forwarding_hops: 0,
    })
}

fn validate_tile_coordinate(z: u8, x: u32, y: u32) -> Result<(), IngressError> {
    if z >= 32 {
        return Err(invalid("tile z must be less than 32"));
    }
    let limit = 1_u32 << z;
    if x >= limit || y >= limit {
        return Err(invalid("tile x/y are out of range for z"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::style_catalog::StyleDefinition;
    use crate::types::{ImageFormat, Scale, StyleId};

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
        catalog.upsert_definition(
            StyleId("carto/gl/voyager-gl-style".to_string()),
            StyleDefinition::new(
                "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
                1,
            ),
        );
        catalog
    }

    fn parse_tile(path: &str) -> Result<InternalTask, IngressError> {
        let parts: Vec<_> = path
            .trim_start_matches('/')
            .trim_end_matches('/')
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        parse_tile_path(
            &parts,
            &catalog(),
            7,
            RequestId::default(),
            Duration::from_secs(30),
            Instant::now(),
        )
    }

    #[test]
    fn parses_tile_webp_2x() {
        let task = parse_tile("/carto/voyager-gl-style/1/1/0@2x.webp").expect("tile path parses");

        assert_eq!(task.output_format, ImageFormat::Webp);
        assert_eq!(task.pixel_ratio.to_scale(), Scale::X2);
        assert_eq!(
            task.request,
            RenderRequest::Tile {
                z: 1,
                x: 1,
                y: 0,
                tile_size: TILE_SIZE,
            }
        );
    }

    #[test]
    fn parses_tile_without_extension_as_png() {
        let task = parse_tile("/carto/voyager-gl-style/1/1/0@2x")
            .expect("tile path without extension parses");

        assert_eq!(task.output_format, ImageFormat::Png);
        assert_eq!(task.pixel_ratio.to_scale(), Scale::X2);
        assert_eq!(
            task.request,
            RenderRequest::Tile {
                z: 1,
                x: 1,
                y: 0,
                tile_size: TILE_SIZE,
            }
        );
    }

    #[test]
    fn parses_tile_jpeg_alias() {
        let task = parse_tile("/carto/voyager-gl-style/1/1/0.jpeg").expect("tile jpeg path parses");

        assert_eq!(task.output_format, ImageFormat::Jpeg);
        assert_eq!(task.pixel_ratio.to_scale(), Scale::X1);
    }

    #[test]
    fn rejects_tile_coordinates_out_of_range() {
        let err =
            parse_tile("/carto/voyager-gl-style/1/2/0.png").expect_err("x is out of range for z");

        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn parses_tile_with_single_segment_style_id() {
        let task =
            parse_tile("/voyager-gl-style/2/3/1@2x.webp").expect("single-segment style tile path");

        assert_eq!(task.style.id.as_str(), "voyager-gl-style");
        assert_eq!(task.output_format, ImageFormat::Webp);
        assert_eq!(task.pixel_ratio.to_scale(), Scale::X2);
        assert!(matches!(task.request, RenderRequest::Tile { z: 2, .. }));
    }

    #[test]
    fn parses_tile_with_deeply_namespaced_style_id() {
        let task = parse_tile("/carto/gl/voyager-gl-style/2/3/1@2x.webp")
            .expect("deeply namespaced style tile path");

        assert_eq!(task.style.id.as_str(), "carto/gl/voyager-gl-style");
        assert!(matches!(task.request, RenderRequest::Tile { z: 2, .. }));
    }
}
