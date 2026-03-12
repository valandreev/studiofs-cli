//! StudioFS Metadata Engine
//!
//! Provides metadata storage for the virtual filesystem using redb as the embedded KV store.
//! All filesystem structure (inodes, directory entries, chunk/slice mappings) is managed here.

pub mod engine;
pub mod events;
pub mod pg_dsn;
pub mod pg_engine;
pub mod redb_engine;
pub mod schema;
pub mod types;

use std::path::Path;
use std::sync::Arc;

pub use engine::{BatchFlushEntry, BatchFlushResult, MetaEngine, MetaError, MetaResult};
pub use events::MetaEvent;
pub use pg_dsn::{
    STUDIOFS_METADATA_DB, StudioPgDsns, ensure_studiofs_database, resolve_studiofs_pg_dsns,
};
pub use pg_engine::PgMetaEngine;
pub use redb_engine::RedbMetaEngine;
pub use types::*;

fn ensure_supported_format_version(info: &FormatInfo) -> MetaResult<()> {
    if info.format_version != CURRENT_FORMAT_VERSION {
        return Err(MetaError::UnsupportedFormatVersion {
            found: info.format_version,
            expected: CURRENT_FORMAT_VERSION,
        });
    }
    Ok(())
}

/// Open metadata engine based on format settings.
pub fn open_meta_engine(path: impl AsRef<Path>) -> MetaResult<Arc<dyn MetaEngine>> {
    let path = path.as_ref();
    let redb = RedbMetaEngine::open(path)?;
    match redb.get_format_info() {
        Ok(info) if info.metadata.backend == MetadataBackend::Postgres => {
            ensure_supported_format_version(&info)?;
            // Redb holds an exclusive process-local open lock. Release probe handle first.
            drop(redb);
            let pg = PgMetaEngine::open(path)?;
            ensure_supported_format_version(&pg.get_format_info()?)?;
            Ok(pg)
        }
        Ok(info) => {
            ensure_supported_format_version(&info)?;
            Ok(Arc::new(redb))
        }
        Err(_) => Ok(Arc::new(redb)),
    }
}
