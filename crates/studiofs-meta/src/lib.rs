//! StudioFS Metadata Engine
//!
//! Provides metadata storage for the virtual filesystem using redb as the embedded KV store.
//! All filesystem structure (inodes, directory entries, chunk/slice mappings) is managed here.

pub mod engine;
pub mod redb_engine;
pub mod types;

pub use engine::{MetaEngine, MetaError, MetaResult};
pub use redb_engine::RedbMetaEngine;
pub use types::*;
