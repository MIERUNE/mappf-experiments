//! HTTP ingress parser/domain errors.

use std::fmt;

use crate::types::StyleId;

#[derive(Debug, Clone, PartialEq)]
pub enum IngressError {
    InvalidRequest(String),
    UnknownStyle(StyleId),
}

impl fmt::Display for IngressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(detail) => write!(f, "invalid request: {detail}"),
            Self::UnknownStyle(style_id) => write!(f, "unknown style: {}", style_id.as_str()),
        }
    }
}

impl std::error::Error for IngressError {}

pub(crate) fn invalid(detail: impl Into<String>) -> IngressError {
    IngressError::InvalidRequest(detail.into())
}
