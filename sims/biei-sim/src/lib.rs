//! Simulation-only module. Everything here is for running the simulator —
//! task generation, reporting, stub rendering, and in-process gossip/transport
//! implementations. Production code at the crate root should not depend on
//! anything in here.

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
pub mod stub_renderer;
pub mod visualization;
pub mod workload;

pub use harness::{Simulation, SimulationOptions};
