pub mod adapter;
pub(crate) mod addlayer;
pub(crate) mod error;
pub(crate) mod format;
pub mod ingress;
pub mod internal;
pub(crate) mod overlay;
pub(crate) mod parse_util;
pub(crate) mod path;
pub(crate) mod preview;
pub(crate) mod query;
pub mod response;
pub(crate) mod static_image;
pub(crate) mod tile;

pub(crate) const REQUEST_ID_HEADER: &str = "x-request-id";
