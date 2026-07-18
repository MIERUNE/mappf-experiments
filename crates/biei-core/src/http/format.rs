use crate::http::error::{IngressError, invalid};
use crate::types::{ImageFormat, Scale};

pub(crate) fn parse_size_scale_format(
    value: &str,
) -> Result<(u16, u16, Scale, ImageFormat), IngressError> {
    let (size, scale, format) = parse_suffix(value)?;
    let Some((width, height)) = size.split_once('x') else {
        return Err(invalid("static size must be {width}x{height}"));
    };
    let width = width
        .parse::<u16>()
        .map_err(|_| invalid("static width must be an integer"))?;
    let height = height
        .parse::<u16>()
        .map_err(|_| invalid("static height must be an integer"))?;
    Ok((width, height, scale, format))
}

pub(crate) fn parse_scale_format(value: &str) -> Result<(u32, Scale, ImageFormat), IngressError> {
    let (number, scale, format) = parse_suffix(value)?;
    let number = number
        .parse::<u32>()
        .map_err(|_| invalid("tile y must be an integer"))?;
    Ok((number, scale, format))
}

fn parse_suffix(value: &str) -> Result<(&str, Scale, ImageFormat), IngressError> {
    let (body, output_format) = match value.rsplit_once('.') {
        Some((body, "png")) => (body, ImageFormat::Png),
        Some((body, "webp")) => (body, ImageFormat::Webp),
        Some((body, "jpg" | "jpeg")) => (body, ImageFormat::Jpeg),
        Some((_body, _extension)) => return Err(invalid("format must be png, webp, or jpg")),
        None => (value, ImageFormat::Png),
    };
    let (body, scale) = if let Some(body) = body.strip_suffix("@2x") {
        (body, Scale::X2)
    } else if body.contains('@') {
        return Err(invalid("scale must be omitted or @2x"));
    } else {
        (body, Scale::X1)
    };
    Ok((body, scale, output_format))
}
