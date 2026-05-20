//! Generated pin marker images for static overlays.

use std::collections::HashSet;

use crate::types::{Padding, PinOverlay, PinSize, StaticOverlay};

const PIN_AUTO_TOP_MARGIN_PX: u16 = 3;

pub(super) fn pin_image_ids(overlays: &[StaticOverlay]) -> Vec<String> {
    let mut ids = Vec::new();
    for overlay in overlays {
        if let StaticOverlay::Pin(pin) = overlay {
            let id = pin_image_id(pin);
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
    }
    ids
}

pub(super) fn register_pin_images(
    style: &mut maplibre_native::StyleRef<'_, maplibre_native::Static>,
    overlays: &[StaticOverlay],
) -> Result<(), maplibre_native::StyleError> {
    let ids = pin_image_ids(overlays);
    for id in &ids {
        // Avoid stale duplicate IDs from a failed prior render. `remove_image`
        // is a no-op for unknown IDs.
        style.remove_image(id);
    }
    let mut registered = HashSet::new();
    for pin in overlays.iter().filter_map(|overlay| match overlay {
        StaticOverlay::Pin(pin) => Some(pin),
        _ => None,
    }) {
        let id = pin_image_id(pin);
        // Repeated same-color/label pins in one request share one style image.
        if !registered.insert(id.clone()) {
            continue;
        }
        let image = render_pin_image(pin).map_err(maplibre_native::StyleError::Native)?;
        if let Err(err) = style.add_image(id, &image, 2.0, false) {
            for id in ids {
                style.remove_image(id);
            }
            return Err(err);
        }
    }
    Ok(())
}

pub(super) fn pin_image_id(pin: &PinOverlay) -> String {
    let size = match pin.size {
        PinSize::Small => "s",
        PinSize::Medium => "m",
        PinSize::Large => "l",
    };
    let label = pin.label.as_deref().unwrap_or("dot");
    format!("biei-pin-{size}-{label}-{}-x2", pin.color)
}

pub(crate) fn pin_auto_padding_inset(size: PinSize) -> Padding {
    let (width, height, _) = pin_image_metrics(size);
    Padding {
        // Pin images are registered with pixel_ratio = 2.0 and anchored at
        // the bottom. Use the logical icon height plus a fixed visual margin
        // above it; exact clipping avoidance leaves top-edge pins feeling too
        // tight in auto-fitted views, but the margin itself should not grow
        // with pin size. Horizontal clearance uses the logical
        // half-width plus a small visual margin because the icon is centered
        // on the anchor and edge-aligned pins otherwise feel cramped.
        top: ceil_div_u32(height, 2).saturating_add(PIN_AUTO_TOP_MARGIN_PX),
        right: ceil_div_u32(width, 3),
        bottom: 0,
        left: ceil_div_u32(width, 3),
    }
}

fn pin_image_metrics(size: PinSize) -> (u32, u32, f32) {
    match size {
        PinSize::Small => (48, 56, 16.0),
        PinSize::Medium => (60, 68, 20.0),
        PinSize::Large => (78, 86, 26.0),
    }
}

pub(super) fn pin_icon_offset_y(size: PinSize) -> f32 {
    let (_, height, radius) = pin_image_metrics(size);
    let cy = 3.0 + radius;
    let tip_y = cy + radius * 1.62;
    (height as f32 - tip_y) / 2.0
}

fn ceil_div_u32(value: u32, divisor: u32) -> u16 {
    value.div_ceil(divisor).min(u32::from(u16::MAX)) as u16
}

pub(super) fn render_pin_image(pin: &PinOverlay) -> Result<image::DynamicImage, String> {
    use tiny_skia::{FillRule, Paint, PathBuilder, Pixmap, Stroke, Transform};

    let (width, height, radius) = pin_image_metrics(pin.size);
    let mut pixmap = Pixmap::new(width, height).ok_or("failed to allocate pin pixmap")?;
    let (r, g, b) = parse_hex_rgb(&pin.color)?;
    let (br, bg, bb) = darken_rgb((r, g, b), 0.78);
    let cx = width as f32 / 2.0;
    let top = 3.0;
    let cy = top + radius;
    let tip_y = cy + radius * 1.62;

    for (scale, alpha) in [(0.82, 12), (0.68, 20), (0.54, 28), (0.40, 34)] {
        let Some(shadow) = pin_shadow_path(cx, tip_y, radius * scale) else {
            continue;
        };
        let mut paint = Paint::default();
        paint.set_color_rgba8(0, 0, 0, alpha);
        pixmap.fill_path(
            &shadow,
            &paint,
            FillRule::Winding,
            Transform::identity(),
            None,
        );
    }

    let body = pin_body_path(cx, cy, radius, tip_y).ok_or("failed to build pin path")?;
    let mut fill = Paint::default();
    fill.set_color_rgba8(r, g, b, 255);
    pixmap.fill_path(&body, &fill, FillRule::Winding, Transform::identity(), None);

    let mut stroke_paint = Paint::default();
    stroke_paint.set_color_rgba8(br, bg, bb, 255);
    let stroke = Stroke {
        width: 2.0,
        ..Stroke::default()
    };
    pixmap.stroke_path(&body, &stroke_paint, &stroke, Transform::identity(), None);

    let label = pin.label.as_deref().and_then(|value| value.chars().next());
    if label.is_none()
        && let Some(dot) = PathBuilder::from_circle(cx, cy, radius * 0.35)
    {
        let mut paint = Paint::default();
        paint.set_color_rgba8(255, 255, 255, 245);
        pixmap.fill_path(&dot, &paint, FillRule::Winding, Transform::identity(), None);
        let mut stroke_paint = Paint::default();
        stroke_paint.set_color_rgba8(br, bg, bb, 255);
        let stroke = Stroke {
            width: 1.5,
            ..Stroke::default()
        };
        pixmap.stroke_path(&dot, &stroke_paint, &stroke, Transform::identity(), None);
    }

    let mut rgba = image::RgbaImage::from_raw(width, height, pixmap.take())
        .ok_or("failed to create pin image")?;
    if let Some(label) = label {
        draw_label(
            &mut rgba,
            label,
            cx,
            cy,
            radius,
            label_color_for_pin((r, g, b)),
        );
    }
    Ok(image::DynamicImage::ImageRgba8(rgba))
}

fn pin_body_path(cx: f32, cy: f32, r: f32, tip_y: f32) -> Option<tiny_skia::Path> {
    let d = tip_y - cy;
    if d <= r {
        return None;
    }
    let tangent_y = r * r / d;
    let tangent_x = (r * r - tangent_y * tangent_y).sqrt();
    let left_angle = tangent_y.atan2(-tangent_x);
    let right_angle = tangent_y.atan2(tangent_x);
    let left_tangent = (cx - tangent_x, cy + tangent_y);
    let right_tangent = (cx + tangent_x, cy + tangent_y);
    let tip = (cx, tip_y);
    let tip_rounding = 1.0;
    let left_tip = point_toward(tip, left_tangent, tip_rounding);
    let right_tip = point_toward(tip, right_tangent, tip_rounding);

    let mut pb = tiny_skia::PathBuilder::new();
    pb.move_to(left_tip.0, left_tip.1);
    pb.line_to(left_tangent.0, left_tangent.1);
    append_arc(
        &mut pb,
        cx,
        cy,
        r,
        left_angle,
        right_angle + std::f32::consts::TAU,
    );
    pb.line_to(right_tip.0, right_tip.1);
    append_quadratic_as_cubic(&mut pb, right_tip, tip, left_tip);
    pb.close();
    pb.finish()
}

fn point_toward(from: (f32, f32), to: (f32, f32), distance: f32) -> (f32, f32) {
    let dx = to.0 - from.0;
    let dy = to.1 - from.1;
    let len = (dx * dx + dy * dy).sqrt();
    if len == 0.0 {
        return from;
    }
    (from.0 + dx / len * distance, from.1 + dy / len * distance)
}

fn append_quadratic_as_cubic(
    pb: &mut tiny_skia::PathBuilder,
    p0: (f32, f32),
    p1: (f32, f32),
    p2: (f32, f32),
) {
    pb.cubic_to(
        p0.0 + (p1.0 - p0.0) * 2.0 / 3.0,
        p0.1 + (p1.1 - p0.1) * 2.0 / 3.0,
        p2.0 + (p1.0 - p2.0) * 2.0 / 3.0,
        p2.1 + (p1.1 - p2.1) * 2.0 / 3.0,
        p2.0,
        p2.1,
    );
}

fn append_arc(
    pb: &mut tiny_skia::PathBuilder,
    cx: f32,
    cy: f32,
    r: f32,
    start_angle: f32,
    end_angle: f32,
) {
    let mut angle = start_angle;
    while angle < end_angle {
        let next = (angle + std::f32::consts::FRAC_PI_2).min(end_angle);
        let k = 4.0 / 3.0 * ((next - angle) / 4.0).tan();
        let p0 = point_on_circle(cx, cy, r, angle);
        let p3 = point_on_circle(cx, cy, r, next);
        let d0 = circle_tangent(r, angle);
        let d1 = circle_tangent(r, next);
        pb.cubic_to(
            p0.0 + d0.0 * k,
            p0.1 + d0.1 * k,
            p3.0 - d1.0 * k,
            p3.1 - d1.1 * k,
            p3.0,
            p3.1,
        );
        angle = next;
    }
}

fn point_on_circle(cx: f32, cy: f32, r: f32, angle: f32) -> (f32, f32) {
    (cx + r * angle.cos(), cy + r * angle.sin())
}

fn circle_tangent(r: f32, angle: f32) -> (f32, f32) {
    (-r * angle.sin(), r * angle.cos())
}

fn pin_shadow_path(cx: f32, cy: f32, r: f32) -> Option<tiny_skia::Path> {
    let mut pb = tiny_skia::PathBuilder::new();
    pb.move_to(cx - r, cy);
    pb.cubic_to(
        cx - r,
        cy - r * 0.26,
        cx - r * 0.45,
        cy - r * 0.42,
        cx,
        cy - r * 0.42,
    );
    pb.cubic_to(
        cx + r * 0.45,
        cy - r * 0.42,
        cx + r,
        cy - r * 0.26,
        cx + r,
        cy,
    );
    pb.cubic_to(
        cx + r,
        cy + r * 0.26,
        cx + r * 0.45,
        cy + r * 0.42,
        cx,
        cy + r * 0.42,
    );
    pb.cubic_to(
        cx - r * 0.45,
        cy + r * 0.42,
        cx - r,
        cy + r * 0.26,
        cx - r,
        cy,
    );
    pb.close();
    pb.finish()
}

fn parse_hex_rgb(hex: &str) -> Result<(u8, u8, u8), String> {
    let expanded;
    let hex = if hex.len() == 3 {
        expanded = hex.chars().flat_map(|ch| [ch, ch]).collect::<String>();
        expanded.as_str()
    } else {
        hex
    };
    if hex.len() != 6 {
        return Err(format!("invalid pin color `{hex}`"));
    }
    let value = u32::from_str_radix(hex, 16).map_err(|_| format!("invalid pin color `{hex}`"))?;
    Ok((
        ((value >> 16) & 0xff) as u8,
        ((value >> 8) & 0xff) as u8,
        (value & 0xff) as u8,
    ))
}

fn darken_rgb((r, g, b): (u8, u8, u8), factor: f32) -> (u8, u8, u8) {
    let scale = |v: u8| ((f32::from(v) * factor).round() as u8).min(v);
    (scale(r), scale(g), scale(b))
}

pub(super) fn label_color_for_pin((r, g, b): (u8, u8, u8)) -> [u8; 3] {
    let luminance = 0.299 * f32::from(r) + 0.587 * f32::from(g) + 0.114 * f32::from(b);
    if luminance >= 160.0 {
        [0, 0, 0]
    } else {
        [255, 255, 255]
    }
}

fn draw_label(image: &mut image::RgbaImage, ch: char, cx: f32, cy: f32, radius: f32, rgb: [u8; 3]) {
    use ab_glyph::{Font, FontArc, Glyph, PxScale, point};
    use std::sync::OnceLock;

    static PIN_LABEL_FONT: OnceLock<Option<FontArc>> = OnceLock::new();

    let Some(font) = PIN_LABEL_FONT.get_or_init(load_pin_label_font).as_ref() else {
        return;
    };

    let scale = PxScale::from(radius * 1.28);
    let glyph_id = font.glyph_id(ch.to_ascii_uppercase());
    let origin = Glyph {
        id: glyph_id,
        scale,
        position: point(0.0, 0.0),
    };
    let Some(origin) = font.outline_glyph(origin) else {
        return;
    };
    let bounds = origin.px_bounds();
    let x = cx - (bounds.min.x + bounds.width() / 2.0);
    let y = cy - (bounds.min.y + bounds.height() / 2.0) + 1.0;
    let glyph = Glyph {
        id: glyph_id,
        scale,
        position: point(x, y),
    };
    let Some(outlined) = font.outline_glyph(glyph) else {
        return;
    };

    let bounds = outlined.px_bounds();
    outlined.draw(|x, y, coverage| {
        let px = bounds.min.x.floor() as i32 + x as i32;
        let py = bounds.min.y.floor() as i32 + y as i32;
        if px < 0 || py < 0 {
            return;
        }
        let (px, py) = (px as u32, py as u32);
        if px >= image.width() || py >= image.height() {
            return;
        }
        blend_pixel(image.get_pixel_mut(px, py), rgb, coverage);
    });
}

fn load_pin_label_font() -> Option<ab_glyph::FontArc> {
    let env_path = std::env::var_os("BIEI_PIN_LABEL_FONT").map(std::path::PathBuf::from);
    let candidates = env_path.into_iter().chain([
        "/Library/Fonts/NotoSans-Bold.ttf".into(),
        "/Library/Fonts/Noto Sans Bold.ttf".into(),
        "/System/Library/Fonts/Supplemental/NotoSans-Bold.ttf".into(),
        "/System/Library/Fonts/Supplemental/Noto Sans Bold.ttf".into(),
        "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf".into(),
        "/usr/share/fonts/opentype/noto/NotoSans-Bold.ttf".into(),
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf".into(),
        "/Library/Fonts/Arial Unicode.ttf".into(),
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf".into(),
        "/System/Library/Fonts/SFNS.ttf".into(),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf".into(),
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf".into(),
    ]);

    for path in candidates {
        let Ok(bytes) = std::fs::read(path) else {
            continue;
        };
        if let Ok(font) = ab_glyph::FontArc::try_from_vec(bytes) {
            return Some(font);
        }
    }
    None
}

fn blend_pixel(pixel: &mut image::Rgba<u8>, rgb: [u8; 3], alpha: f32) {
    let src_a = alpha.clamp(0.0, 1.0);
    let dst_a = f32::from(pixel[3]) / 255.0;
    let out_a = src_a + dst_a * (1.0 - src_a);
    if out_a <= f32::EPSILON {
        *pixel = image::Rgba([0, 0, 0, 0]);
        return;
    }
    for channel in 0..3 {
        let src = f32::from(rgb[channel]) / 255.0;
        let dst = f32::from(pixel[channel]) / 255.0;
        let out = (src * src_a + dst * dst_a * (1.0 - src_a)) / out_a;
        pixel[channel] = (out * 255.0).round() as u8;
    }
    pixel[3] = (out_a * 255.0).round() as u8;
}
