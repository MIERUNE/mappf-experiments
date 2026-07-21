//! Distributed tile-renderer core: render scheduling, cluster routing, and node
//! orchestration shared by the Biei server (`servers/biei`) and simulator.

#![deny(unreachable_pub)]

pub mod config;
pub(crate) mod dispatcher;
pub mod gossip;
pub(crate) mod hrw;
pub mod internal_transport;
pub mod metrics;
pub mod node;
mod render_cache;
pub mod renderer;
pub mod style_catalog;
pub mod types;
pub mod wire;
pub(crate) mod worker;
pub(crate) mod worker_pool;
