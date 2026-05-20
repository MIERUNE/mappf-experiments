//! `Transport` trait — inter-node task forwarding awaiting a wire-safe response.

use async_trait::async_trait;

use crate::types::NodeId;
use crate::wire::{ForwardRequest, ForwardResponse};

#[derive(Debug)]
pub enum ForwardError {
    Retryable(String),
    Fatal(String),
}

impl std::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ForwardError::Retryable(s) => write!(f, "retryable forward error: {s}"),
            ForwardError::Fatal(s) => write!(f, "fatal forward error: {s}"),
        }
    }
}

impl std::error::Error for ForwardError {}

/// Forwards a task from one node to another and awaits the receiver's
/// `ForwardResponse`. In production this is HTTP internal forward; in
/// simulation it routes through `sim::channel_transport::ChannelTransport`
/// (mpsc + hop_latency sleep + direct call into the target node).
#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(
        &self,
        target: NodeId,
        fwd: ForwardRequest,
    ) -> Result<ForwardResponse, ForwardError>;
}
