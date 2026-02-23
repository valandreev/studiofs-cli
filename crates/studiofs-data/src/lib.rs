//! StudioFS Data Layer
//!
//! Handles reading/writing data blocks to S3-compatible storage via OpenDAL.
//! Blocks are 4MiB units that correspond to slices within chunks.

pub mod block;
pub mod object;
pub mod store;

pub use store::{DataStore, DataStoreConfig, IoProfile};
