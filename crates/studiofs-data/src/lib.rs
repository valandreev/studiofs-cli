#![forbid(unsafe_code)]
//! StudioFS Data Layer
//!
//! Handles reading/writing data blocks to S3-compatible storage via OpenDAL.
//! Logical blocks are 16MiB; cloud objects are stored as four 4MiB fragments.

pub mod block;
pub mod object;
pub mod store;

pub use store::{DataStore, DataStoreConfig, IoProfile};
