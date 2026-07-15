//! In-memory chitchat transport used by the real-cache simulator.

use std::{net::SocketAddr, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use chitchat::{
    ChitchatMessage,
    transport::{ChannelTransport, Socket as ChitchatSocket, Transport as ChitchatTransport},
};

/// Runs production chitchat over an in-process network with virtual hop delay.
#[derive(Clone)]
pub(crate) struct SimGossipTransport {
    inner: ChannelTransport,
    hop_latency: Duration,
}

impl SimGossipTransport {
    pub(crate) fn new(hop_latency: Duration) -> Self {
        Self {
            inner: ChannelTransport::default(),
            hop_latency,
        }
    }
}

#[async_trait]
impl ChitchatTransport for SimGossipTransport {
    async fn open(&self, listen_addr: SocketAddr) -> Result<Box<dyn ChitchatSocket>> {
        let inner = self.inner.open(listen_addr).await?;
        Ok(Box::new(SimGossipSocket {
            inner,
            hop_latency: self.hop_latency,
        }))
    }
}

struct SimGossipSocket {
    inner: Box<dyn ChitchatSocket>,
    hop_latency: Duration,
}

#[async_trait]
impl ChitchatSocket for SimGossipSocket {
    async fn send(&mut self, to: SocketAddr, message: ChitchatMessage) -> Result<()> {
        if !self.hop_latency.is_zero() {
            tokio::time::sleep(self.hop_latency).await;
        }
        self.inner.send(to, message).await
    }

    async fn recv(&mut self) -> Result<(SocketAddr, ChitchatMessage)> {
        self.inner.recv().await
    }
}
