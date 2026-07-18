//! Terrain tile generation primitives.
//!
//! This crate decodes Terrarium DEM tiles, assembles seam-aware 3x3 DEM
//! neighborhoods, and generates contour or hillshade products. Fetching,
//! caching, compression, and serving are intentionally left to callers.

pub mod contours;
pub mod dem;
pub mod hillshade;
mod topology;
