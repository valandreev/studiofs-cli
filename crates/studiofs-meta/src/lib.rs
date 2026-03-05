//! StudioFS Metadata Engine
//!
//! Provides metadata storage for the virtual filesystem using redb as the embedded KV store.
//! All filesystem structure (inodes, directory entries, chunk/slice mappings) is managed here.

pub mod engine;
pub mod events;
pub mod pg_engine;
pub mod redb_engine;
pub mod schema;
pub mod types;

use std::path::Path;
use std::sync::Arc;

pub use engine::{BatchFlushEntry, BatchFlushResult, MetaEngine, MetaError, MetaResult};
pub use events::MetaEvent;
pub use pg_engine::PgMetaEngine;
pub use redb_engine::RedbMetaEngine;
pub use types::*;

/// Open metadata engine based on format settings.
pub fn open_meta_engine(path: impl AsRef<Path>) -> MetaResult<Arc<dyn MetaEngine>> {
    let path = path.as_ref();
    let redb = RedbMetaEngine::open(path)?;
    match redb.get_format_info() {
        Ok(info) if info.metadata.backend == MetadataBackend::Postgres => {
            // Redb holds an exclusive process-local open lock. Release probe handle first.
            drop(redb);
            let pg = PgMetaEngine::open(path)?;
            Ok(pg)
        }
        _ => Ok(Arc::new(redb)),
    }
}
