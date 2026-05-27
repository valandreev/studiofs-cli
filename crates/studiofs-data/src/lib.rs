#![forbid(unsafe_code)]
//! StudioFS Data Layer
//!
//! Handles reading/writing StudioFS payload objects to S3-compatible storage via OpenDAL.
//! Logical payloads are stored as fixed-size cloud fragments where needed.

pub mod crypto;
pub mod envelope;
pub mod object;
pub mod store;

pub use store::{
    DataStore, DataStoreConfig, DataStoreCryptoConfig, IoProfile, normalize_s3_endpoint_transport,
    validate_s3_endpoint_transport,
};
