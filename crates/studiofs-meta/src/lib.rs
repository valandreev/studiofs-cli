//! StudioFS Metadata Engine
//!
//! Provides metadata storage for the virtual filesystem using redb as the embedded KV store.
//! All filesystem structure (inodes, directory entries, chunk/slice mappings) is managed here.

pub mod async_engine;
pub mod engine;
pub mod events;
pub mod pg_dsn;
pub mod pg_engine;
pub mod redb_engine;
pub mod schema;
pub mod types;

use std::path::Path;
use std::sync::Arc;

pub use async_engine::{AsyncMetaEngine, SyncMetaEngineAdapter, execute_sync_meta};
pub use engine::{
    BatchFlushEntry, BatchFlushResult, MetaAcl, MetaBlocks, MetaEngine, MetaEngineCore, MetaError,
    MetaFormat, MetaFsOps, MetaGc, MetaInodes, MetaNamespace, MetaRecovery, MetaReplication,
    MetaResult, MetaSnapshots, MetaSymlink, MetaXattr,
};
pub use events::MetaEvent;
pub use pg_dsn::{
    STUDIOFS_METADATA_DB, StudioPgDsns, ensure_studiofs_database, resolve_studiofs_pg_dsns,
};
pub use pg_engine::{PgMetaEngine, PgOpenOptions};
pub use redb_engine::RedbMetaEngine;
pub use types::*;

#[derive(Debug, Clone, Copy)]
pub struct MetaOpenOptions {
    pub background_poll: bool,
}

impl Default for MetaOpenOptions {
    fn default() -> Self {
        Self {
            background_poll: true,
        }
    }
}

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
    open_meta_engine_with_options(path, MetaOpenOptions::default())
}

/// Open metadata engine with runtime behavior overrides.
pub fn open_meta_engine_with_options(
    path: impl AsRef<Path>,
    options: MetaOpenOptions,
) -> MetaResult<Arc<dyn MetaEngine>> {
    let path = path.as_ref();
    match RedbMetaEngine::read_format_info_readonly(path) {
        Ok(info) if info.metadata.backend == MetadataBackend::Postgres => {
            ensure_supported_format_version(&info)?;
            let pg = PgMetaEngine::open_with_options(
                path,
                PgOpenOptions {
                    background_poll: options.background_poll,
                },
            )?;
            ensure_supported_format_version(&pg.get_format_info()?)?;
            Ok(pg)
        }
        probe_result => {
            let redb = RedbMetaEngine::open(path)?;
            match probe_result {
                Ok(info) => {
                    ensure_supported_format_version(&info)?;
                    Ok(Arc::new(redb))
                }
                Err(_) => Ok(Arc::new(redb)),
            }
        }
    }
}
