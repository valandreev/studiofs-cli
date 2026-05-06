#![forbid(unsafe_code)]
#![allow(missing_docs)]
//! StudioFS Data Layer
//!
//! Handles reading/writing StudioFS payload objects to S3-compatible storage via OpenDAL.
//! Logical payloads are stored as fixed-size cloud fragments where needed.

pub mod object;
pub mod store;

pub use store::{DataStore, DataStoreConfig, IoProfile};
