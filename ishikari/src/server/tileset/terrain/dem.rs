//! Terrarium DEM decoding and 3x3 neighborhood access.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use image::ImageFormat;

use crate::pmtiles::TileData;

const MIN_VALID_ELEVATION: f32 = -12_000.0;
const MAX_VALID_ELEVATION: f32 = 9_000.0;
/// Maximum accepted DEM source-tile edge (px). Real tiles are 256/512.
const MAX_DEM_TILE_DIM: u32 = 2048;

#[derive(Debug)]
pub(crate) struct DemTile {
    width: usize,
    height: usize,
    elevations: Vec<f32>,
}

impl DemTile {
    fn get(&self, x: usize, y: usize) -> f32 {
        self.elevations[y * self.width + x]
    }

    /// Approximate heap footprint, for cache weighing.
    pub(crate) fn byte_size(&self) -> usize {
        self.elevations.len() * std::mem::size_of::<f32>() + std::mem::size_of::<Self>()
    }
}

/// A center DEM tile plus its eight neighbors in row-major order. Tiles are
/// shared `Arc`s so decoded DEMs can live in a cross-product, cross-request
/// cache (neighboring derived tiles reuse six of the nine).
#[derive(Debug)]
pub(super) struct DemNeighborhood {
    tiles: [Option<Arc<DemTile>>; 9],
    width: usize,
    height: usize,
}

impl DemNeighborhood {
    pub(super) fn from_tiles(tiles: [Option<Arc<DemTile>>; 9]) -> Result<Self> {
        let center = tiles[4]
            .as_ref()
            .context("center Mapterhorn DEM tile is missing")?;
        let (width, height) = (center.width, center.height);
        if width < 3 || height < 3 {
            bail!("DEM tile is too small: {width}x{height}");
        }
        for tile in tiles.iter().flatten() {
            if tile.width != width || tile.height != height {
                bail!(
                    "DEM neighborhood dimensions differ: expected {width}x{height}, got {}x{}",
                    tile.width,
                    tile.height
                );
            }
        }
        Ok(Self {
            tiles,
            width,
            height,
        })
    }

    pub(super) fn width(&self) -> usize {
        self.width
    }

    pub(super) fn height(&self) -> usize {
        self.height
    }

    /// Reads through a one-tile border. A missing sparse detail neighbor falls
    /// back to the center edge; the requested center tile itself is mandatory.
    pub(super) fn get(&self, x: i32, y: i32) -> f32 {
        let width = self.width as i32;
        let height = self.height as i32;
        let (column, local_x) = tile_axis(x, width);
        let (row, local_y) = tile_axis(y, height);
        let index = ((row + 1) * 3 + column + 1) as usize;
        if let Some(tile) = &self.tiles[index] {
            return tile.get(local_x as usize, local_y as usize);
        }

        let center = self.tiles[4].as_ref().expect("center checked in decode");
        center.get(
            x.clamp(0, width - 1) as usize,
            y.clamp(0, height - 1) as usize,
        )
    }

    /// Converts pixel-center samples to a grid-point elevation by averaging the
    /// four adjacent samples, matching maplibre-contour's seam-safe input.
    pub(super) fn grid_elevation(&self, x: i32, y: i32) -> f32 {
        let values = [
            self.get(x - 1, y - 1),
            self.get(x, y - 1),
            self.get(x - 1, y),
            self.get(x, y),
        ];
        let mut sum = 0.0;
        let mut count = 0;
        for value in values {
            if value.is_finite() {
                sum += value;
                count += 1;
            }
        }
        if count == 0 {
            f32::NAN
        } else {
            sum / count as f32
        }
    }
}

fn tile_axis(value: i32, size: i32) -> (i32, i32) {
    if value < 0 {
        (-1, value + size)
    } else if value >= size {
        (1, value - size)
    } else {
        (0, value)
    }
}

pub(super) fn decode_terrarium(tile: TileData) -> Result<DemTile> {
    if tile.content_encoding.is_some() {
        bail!(
            "compressed Mapterhorn image payload is not supported: {:?}",
            tile.content_encoding
        );
    }
    // Cap decode dimensions so a crafted WebP declaring huge dimensions cannot
    // expand a tiny payload into a multi-gigabyte buffer. Real DEM source tiles
    // are 256/512 px; the bound leaves generous headroom.
    let mut reader = image::ImageReader::new(std::io::Cursor::new(tile.bytes.as_ref()));
    reader.set_format(ImageFormat::WebP);
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_DEM_TILE_DIM);
    limits.max_image_height = Some(MAX_DEM_TILE_DIM);
    reader.limits(limits);
    let image = reader
        .decode()
        .context("decode Mapterhorn WebP")?
        .into_rgb8();
    let (width, height) = image.dimensions();
    let elevations = image
        .pixels()
        .map(|pixel| {
            let [r, g, b] = pixel.0;
            let elevation = f32::from(r) * 256.0 + f32::from(g) + f32::from(b) / 256.0 - 32_768.0;
            if (MIN_VALID_ELEVATION..=MAX_VALID_ELEVATION).contains(&elevation) {
                elevation
            } else {
                f32::NAN
            }
        })
        .collect();
    Ok(DemTile {
        width: width as usize,
        height: height as usize,
        elevations,
    })
}

#[cfg(test)]
impl DemNeighborhood {
    /// Builds a synthetic neighborhood from a global elevation function over
    /// center-tile pixel coordinates (neighbors continue the same field).
    pub(super) fn synthetic(size: usize, elevation: impl Fn(i32, i32) -> f32) -> Self {
        let tile = |col: i32, row: i32| DemTile {
            width: size,
            height: size,
            elevations: (0..size * size)
                .map(|i| {
                    let x = col * size as i32 + (i % size) as i32;
                    let y = row * size as i32 + (i / size) as i32;
                    elevation(x, y)
                })
                .collect(),
        };
        let mut tiles: [Option<Arc<DemTile>>; 9] = std::array::from_fn(|_| None);
        for row in -1_i32..=1 {
            for col in -1_i32..=1 {
                tiles[((row + 1) * 3 + col + 1) as usize] = Some(Arc::new(tile(col, row)));
            }
        }
        Self {
            tiles,
            width: size,
            height: size,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use image::{DynamicImage, ImageBuffer, ImageFormat, Rgb};
    use std::io::Cursor;

    use super::*;

    fn webp_tile(rgb: [u8; 3], width: u32, height: u32) -> TileData {
        let image = DynamicImage::ImageRgb8(ImageBuffer::from_pixel(width, height, Rgb(rgb)));
        let mut bytes = Cursor::new(Vec::new());
        image.write_to(&mut bytes, ImageFormat::WebP).unwrap();
        TileData {
            bytes: Bytes::from(bytes.into_inner()),
            content_type: "image/webp",
            content_encoding: None,
        }
    }

    #[test]
    fn decodes_terrarium_elevation() {
        // 128*256 + 10 + 128/256 - 32768 = 10.5m.
        let tile = decode_terrarium(webp_tile([128, 10, 128], 3, 3)).unwrap();
        assert_eq!(tile.get(1, 1), 10.5);
    }

    #[test]
    fn reads_neighbor_and_falls_back_for_missing_neighbor() {
        let mut tiles: [Option<Arc<DemTile>>; 9] = std::array::from_fn(|_| None);
        tiles[4] = Some(Arc::new(
            decode_terrarium(webp_tile([128, 0, 0], 3, 3)).unwrap(),
        ));
        tiles[5] = Some(Arc::new(
            decode_terrarium(webp_tile([128, 1, 0], 3, 3)).unwrap(),
        ));
        let neighborhood = DemNeighborhood::from_tiles(tiles).unwrap();
        assert_eq!(neighborhood.get(3, 1), 1.0);
        assert_eq!(neighborhood.get(-1, 1), 0.0);
    }
}
