//! StudioFS Metadata Engine
//!
//! Provides metadata storage for the virtual filesystem using redb as the embedded KV store.
//! All filesystem structure (inodes, directory entries, chunk/slice mappings) is managed here.

pub mod engine;
pub mod events;
pub mod redb_engine;
pub mod schema;
pub mod tonbo_engine;
pub mod types;

use std::path::Path;
use std::sync::Arc;

pub use engine::{MetaEngine, MetaError, MetaResult};
pub use events::MetaEvent;
pub use redb_engine::RedbMetaEngine;
pub use tonbo_engine::TonboMetaEngine;
pub use types::*;

/// Open metadata engine based on format settings.
pub fn open_meta_engine(path: impl AsRef<Path>) -> MetaResult<Arc<dyn MetaEngine>> {
    let redb = RedbMetaEngine::open(path.as_ref())?;
    match redb.get_format_info() {
        Ok(info) if info.metadata.backend == MetadataBackend::Tonbo => {
            // Redb holds an exclusive process-local open lock. Release the probe handle
            // before opening Tonbo (which wraps its own RedbMetaEngine).
            drop(redb);
            let tonbo = TonboMetaEngine::open(path)?;
            Ok(Arc::new(tonbo))
        }
        _ => Ok(Arc::new(redb)),
    }
}
