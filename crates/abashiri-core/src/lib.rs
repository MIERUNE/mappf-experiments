//! Abashiri management-plane core: bounded resource identities, durable
//! mutation journaling, and conditional object-storage operations.

#![deny(unreachable_pub)]

pub mod auth;
pub mod catalog;
pub mod mutation;
pub mod storage;
mod store_policy;
pub mod style;
