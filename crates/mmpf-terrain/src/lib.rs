//! Terrain tile generation primitives.
//!
//! This crate decodes Terrarium DEM tiles, assembles seam-aware 3x3 DEM
//! neighborhoods, and generates contour and hillshade products as MVT vector
//! tiles. Fetching, caching, and serving are left to callers. Experimental
//! raster hillshade encoders (WebP/JPEG) live behind the `raster-encode`
//! feature; the default build is vector-only and pulls no image *encoder*.

pub mod contours;
pub mod dem;
pub mod hillshade;
mod topology;
