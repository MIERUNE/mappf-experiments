//! Simulation-only module. Everything here is for running the simulator —
//! task generation, sweep harness, stub renderer, in-process gossip/transport
//! impls. Production code at the crate root should not depend on anything in
//! here.

pub mod calibrated_costs;
pub mod calibration;
pub mod calibration_runner;
pub mod channel_transport;
pub mod chitchat_bus;
pub mod churn;
pub mod config;
pub mod harness;
pub mod metrics;
pub mod report;
pub mod scenarios;
pub mod stub_renderer;
pub mod sweep;
pub mod visualization;
pub mod workload;

pub use channel_transport::ChannelTransport;
pub use chitchat_bus::ChitchatGossipBus;
pub use harness::{Simulation, SimulationOptions};
pub use report::RunReport;
pub use stub_renderer::StubRenderer;
