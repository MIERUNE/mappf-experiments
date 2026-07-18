//! Image encoding for rendered MapLibre RGBA frames.

use crate::types::{ImageFormat, RenderOutput, RendererError};

pub(super) fn encode_image(
    image: &maplibre_native::Image,
    format: ImageFormat,
) -> Result<RenderOutput, RendererError> {
    use image::ImageEncoder;

    let buffer = image.as_image();
    let raw = buffer.as_raw();
    let width = buffer.width();
    let height = buffer.height();
    // 4 byte/pixel(RGBA8)。PNG/WebP の圧縮率は典型 30-60% 程度なので、
    // raw の半分程度を初期容量にすると再 alloc を概ね 1 回以内に抑えられる。
    let mut bytes: Vec<u8> = Vec::with_capacity(raw.len() / 2);

    match format {
        ImageFormat::Png => {
            // `flate2` の backend を `zlib-ng` に揃えてあるので(`crates/biei-core/Cargo.toml`
            // 経由で workspace dep の features = ["zlib-ng"] が有効)、zlib level 6
            // (Default)でも encode 速度は旧 miniz_oxide Fast と同等。一方で
            // 出力サイズは Fast 比 ~25% 削減できる。大規模 CDN deploy では bytes
            // が支配的コストになるので、speed-vs-size のバランスを Default に倒す。
            // Filter は per-tile で内容が変わる map tile / static image だと
            // Sub 固定が無難(Adaptive は heuristic 計算が重い)。
            let encoder = image::codecs::png::PngEncoder::new_with_quality(
                &mut bytes,
                image::codecs::png::CompressionType::Default,
                image::codecs::png::FilterType::Sub,
            );
            encoder
                .write_image(raw, width, height, image::ExtendedColorType::Rgba8)
                .map_err(|err| RendererError::RenderFailed(format!("png encode failed: {err}")))?;
        }
        ImageFormat::Webp => {
            bytes = encode_webp_lossy(raw, width, height);
        }
        ImageFormat::Jpeg => {
            // JPEG has no alpha channel. Blend any non-opaque pixel onto a
            // white background before encoding; normal map renders are opaque.
            let rgb = rgba_to_rgb_on_white(raw);
            let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, 85);
            encoder
                .write_image(&rgb, width, height, image::ExtendedColorType::Rgb8)
                .map_err(|err| RendererError::RenderFailed(format!("jpg encode failed: {err}")))?;
        }
    }

    Ok(RenderOutput {
        bytes: bytes.into(),
        format,
    })
}

fn encode_webp_lossy(raw: &[u8], width: u32, height: u32) -> Vec<u8> {
    const WEBP_QUALITY: f32 = 85.0;

    let encoder = webp::Encoder::from_rgba(raw, width, height);
    let encoded = encoder.encode(WEBP_QUALITY);
    encoded.to_vec()
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn rgba_to_rgb_on_white(raw: &[u8]) -> Vec<u8> {
    let mut rgb = Vec::with_capacity(raw.len() / 4 * 3);
    for px in raw.chunks_exact(4) {
        let alpha = px[3] as u16;
        if alpha == 255 {
            rgb.extend_from_slice(&px[..3]);
        } else if alpha == 0 {
            rgb.extend_from_slice(&[255, 255, 255]);
        } else {
            for channel in &px[..3] {
                let blended = (*channel as u16 * alpha + 255 * (255 - alpha) + 127) / 255;
                rgb.push(blended as u8);
            }
        }
    }
    rgb
}
